import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional

import aiohttp
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.concurrency import iterate_in_threadpool

from backend.server.chat_memory import ChatMemoryService
from backend.server.constants import NODE_STATUS_AVAILABLE
from parallax_utils.logging_config import get_logger
from parallax_utils.request_metrics import get_request_metrics

logger = get_logger(__name__)

AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=20 * 60 * 60)


@dataclass
class ActiveRequestRecord:
    request_id: str
    request_data: Dict
    received_ts: int
    conversation_id: Optional[str] = None
    routing_table: Optional[list[str]] = None
    first_hop: Optional[str] = None
    emitted_text: str = ""
    emitted_any_bytes: bool = False
    status: str = "pending"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    last_error: Optional[str] = None


class RequestHandler:
    """HTTP request forwarder with scheduler-aware routing and retry logic.

    Behavior for routing resolution:
    - routing_table is None: scheduler has not decided yet -> treat as error for this attempt
    - routing_table is []: all pipelines are full now -> retry up to max attempts
    - routing_table is non-empty: forward to first hop
    """

    MAX_FORWARD_RETRY = 10
    MAX_ROUTING_RETRY = 20
    FORWARD_DELAY_SEC = 10
    RETRY_DELAY_SEC = 5

    def __init__(self):
        self.scheduler_manage = None
        self.stubs = {}
        self.chat_memory = ChatMemoryService()
        self.active_requests: dict[str, ActiveRequestRecord] = {}
        self.summary_tasks: dict[str, asyncio.Task] = {}

    def set_scheduler_manage(self, scheduler_manage):
        self.scheduler_manage = scheduler_manage

    def get_stub(self, node_id):
        if node_id not in self.stubs:
            self.stubs[node_id] = self.scheduler_manage.completion_handler.get_stub(node_id)
        return self.stubs[node_id]

    def _register_active_request(
        self,
        request_id: str,
        request_data: Dict,
        received_ts: int,
        conversation_id: Optional[str],
    ) -> ActiveRequestRecord:
        record = ActiveRequestRecord(
            request_id=str(request_id),
            request_data=dict(request_data),
            received_ts=received_ts,
            conversation_id=conversation_id,
        )
        self.active_requests[str(request_id)] = record
        return record

    def _mark_request_routed(self, request_id: str, routing_table: list[str]) -> None:
        record = self.active_requests.get(str(request_id))
        if record is None:
            return
        record.routing_table = list(routing_table)
        record.first_hop = routing_table[0] if routing_table else None
        record.status = 'routed'
        record.updated_at = time.time()

    def _append_request_output(self, request_id: str, text: str, *, emitted_any_bytes: bool = False) -> None:
        record = self.active_requests.get(str(request_id))
        if record is None:
            return
        if text:
            record.emitted_text += text
        if emitted_any_bytes:
            record.emitted_any_bytes = True
        if record.status != 'completed':
            record.status = 'streaming'
        record.updated_at = time.time()

    def _fail_active_request(self, request_id: str, error: Exception | str) -> None:
        record = self.active_requests.get(str(request_id))
        if record is None:
            return
        record.status = 'failed'
        record.last_error = str(error)
        record.updated_at = time.time()

    def _complete_active_request(self, request_id: str) -> None:
        record = self.active_requests.get(str(request_id))
        if record is None:
            return
        record.status = 'completed'
        record.updated_at = time.time()
        self.active_requests.pop(str(request_id), None)

    def _schedule_summary_refresh(self, conversation_id: Optional[str], model_name: Optional[str]) -> None:
        if not conversation_id or not self.chat_memory.model_summary_enabled:
            return

        existing = self.summary_tasks.get(conversation_id)
        if existing is not None and not existing.done():
            existing.cancel()

        task = asyncio.create_task(self._refresh_conversation_summary(conversation_id, model_name))
        self.summary_tasks[conversation_id] = task

        def _cleanup(done_task: asyncio.Task):
            self.summary_tasks.pop(conversation_id, None)
            try:
                done_task.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning('Conversation summary refresh failed for %s: %s', conversation_id, exc, exc_info=True)

        task.add_done_callback(_cleanup)

    async def _refresh_conversation_summary(self, conversation_id: str, model_name: Optional[str]) -> None:
        await asyncio.sleep(0.25)
        messages = self.chat_memory.build_summary_generation_messages(conversation_id)
        if not messages:
            self.chat_memory.store_summary(conversation_id, '', source='none')
            return

        summary_request = {
            'model': model_name or 'default',
            'messages': messages,
            'stream': False,
            'max_tokens': self.chat_memory.model_summary_max_tokens,
            'sampling_params': {
                'temperature': 0.1,
                'top_k': 1,
            },
            '_disable_chat_memory': True,
            '_summary_request': True,
        }
        summary_request_id = f'summary-{uuid.uuid4()}'
        response = await self._forward_request(summary_request, summary_request_id, int(time.time()))
        if isinstance(response, Response) and response.status_code == 200:
            try:
                payload = response.body.decode()
            except Exception:
                payload = ''
            summary_text = self._extract_nonstream_content(payload)
            cleaned_summary = self.chat_memory.store_summary(conversation_id, summary_text, source='model')
            if cleaned_summary:
                logger.info('Stored model-written summary for conversation %s', conversation_id)
                return

        fallback_summary = self.chat_memory.refresh_summary(conversation_id)
        if fallback_summary:
            logger.info('Stored heuristic fallback summary for conversation %s after model summary failure', conversation_id)

    async def _resolve_routing_table(self, request_id: str, received_ts: int) -> Optional[list[str]]:
        attempts = 0
        last_error: Optional[Exception] = None
        while attempts < self.MAX_ROUTING_RETRY:
            try:
                if self.scheduler_manage is None:
                    raise RuntimeError('Scheduler manager is not initialized')
                if self.scheduler_manage.get_schedule_status() != NODE_STATUS_AVAILABLE:
                    routing_table = None
                else:
                    routing_table = self.scheduler_manage.get_routing_table(request_id, received_ts)
                    logger.debug(
                        'get_routing_table for request %s return: %s (attempt %d)',
                        request_id,
                        routing_table,
                        attempts + 1,
                    )
            except Exception as e:
                last_error = e
                logger.warning('get_routing_table error for %s: %s', request_id, e)
                routing_table = None

            if routing_table and len(routing_table) > 0:
                return routing_table

            attempts += 1
            if attempts < self.MAX_ROUTING_RETRY:
                await asyncio.sleep(self.RETRY_DELAY_SEC)

        if last_error is not None:
            raise RuntimeError(f'Get routing table error: {last_error}') from last_error
        return None

    def _extract_stream_delta_text(self, chunk: bytes) -> str:
        try:
            decoded = chunk.decode('utf-8')
        except Exception:
            return ''
        pieces = []
        for line in decoded.splitlines():
            if not line.startswith('data: '):
                continue
            payload = line[6:].strip()
            if not payload or payload == '[DONE]':
                continue
            try:
                data = json.loads(payload)
            except Exception:
                continue
            for choice in data.get('choices', []) or []:
                delta = choice.get('delta') or {}
                content = delta.get('content')
                if isinstance(content, str) and content:
                    pieces.append(content)
        return ''.join(pieces)

    def _extract_nonstream_content(self, content: str) -> str:
        try:
            payload = json.loads(content)
        except Exception:
            return ''
        choices = payload.get('choices') or []
        if not choices:
            return ''
        message = choices[0].get('message') or {}
        result = message.get('content')
        return result if isinstance(result, str) else ''

    async def _forward_request(self, request_data: Dict, request_id: str, received_ts: int):
        request_data, conversation_id = self.chat_memory.prepare_request(request_data)
        record = self._register_active_request(str(request_id), request_data, received_ts, conversation_id)
        start_time = time.time()
        logger.debug(
            f"Forwarding request {request_id}; stream={request_data.get('stream', False)} conversation_id={conversation_id}"
        )

        # Try to get a success response
        forward_attempts = 0
        while forward_attempts < self.MAX_FORWARD_RETRY:
            try:
                routing_table = await self._resolve_routing_table(str(request_id), received_ts)
            except Exception as e:
                self._fail_active_request(str(request_id), e)
                return JSONResponse(
                    content={"error": "Get routing table error"},
                    status_code=500,
                )

            if not routing_table:
                self._fail_active_request(str(request_id), 'Routing pipelines not ready')
                return JSONResponse(
                    content={"error": "Routing pipelines not ready"},
                    status_code=503,
                )

            request_data["rid"] = str(request_id)
            request_data["routing_table"] = routing_table
            self._mark_request_routed(str(request_id), routing_table)
            stub = self.get_stub(routing_table[0])
            is_stream = request_data.get("stream", False)
            try:
                if is_stream:

                    async def stream_generator():
                        attempts = 0
                        first_token_time = None
                        last_chunk = None
                        last_token_time = None
                        assistant_chunks = []
                        while attempts < self.MAX_FORWARD_RETRY:
                            current_routing_table = request_data["routing_table"]
                            current_stub = self.get_stub(current_routing_table[0])
                            response = current_stub.chat_completion(request_data)
                            try:
                                iterator = iterate_in_threadpool(response)
                                async for chunk in iterator:
                                    last_token_time = time.time()
                                    if first_token_time is None:
                                        first_token_time = last_token_time
                                    if chunk is not None and not chunk.decode("utf-8").startswith(
                                        "data: [DONE]"
                                    ):
                                        last_chunk = chunk
                                        self._append_request_output(
                                            str(request_id), '', emitted_any_bytes=True
                                        )
                                        delta_text = self._extract_stream_delta_text(chunk)
                                        if delta_text:
                                            assistant_chunks.append(delta_text)
                                            self._append_request_output(str(request_id), delta_text)
                                    yield chunk
                                assistant_text = ''.join(assistant_chunks)
                                if assistant_text:
                                    self.chat_memory.save_assistant_message(
                                        conversation_id, assistant_text, str(request_id)
                                    )
                                    self._schedule_summary_refresh(conversation_id, request_data.get('model'))
                                self._complete_active_request(str(request_id))
                                return
                            except Exception as e:
                                record = self.active_requests.get(str(request_id))
                                emitted_any = bool(record and record.emitted_any_bytes)
                                attempts += 1
                                if not emitted_any and attempts < self.MAX_FORWARD_RETRY:
                                    logger.warning(
                                        'Retrying streamed request %s after forwarding error before any output: %s',
                                        request_id,
                                        e,
                                    )
                                    await asyncio.sleep(self.FORWARD_DELAY_SEC)
                                    fresh_routing = await self._resolve_routing_table(
                                        str(request_id), received_ts
                                    )
                                    if not fresh_routing:
                                        raise
                                    request_data["routing_table"] = fresh_routing
                                    self._mark_request_routed(str(request_id), fresh_routing)
                                    continue
                                self._fail_active_request(str(request_id), e)
                                logger.warning(
                                    'Ending streamed request %s after forwarding error: %s',
                                    request_id,
                                    e,
                                )
                                error_chunk = {
                                    'id': str(request_id),
                                    'object': 'chat.completion.error',
                                    'created': time.time(),
                                    'error': {
                                        'message': 'The active node disconnected while serving this request. You can retry from the current conversation.',
                                        'code': 'peer_disconnected',
                                    },
                                }
                                yield f"data: {json.dumps(error_chunk, separators=(',', ':'))}\n\n".encode('utf-8')
                                return
                            finally:
                                if last_chunk is not None:
                                    tps, ttft, input_tokens, output_tokens = get_request_metrics(
                                        last_chunk, start_time, first_token_time, last_token_time
                                    )
                                    if (
                                        tps is not None
                                        and ttft is not None
                                        and input_tokens is not None
                                        and output_tokens is not None
                                    ):
                                        logger.info(
                                            f"Request ID: {request_id} | TPS: {tps:.2f} |  TTFT: {ttft} ms | Output tokens: {output_tokens} | Input tokens: {input_tokens}"
                                        )
                                logger.debug(f"client disconnected for {request_id}")
                                response.cancel()

                    resp = StreamingResponse(
                        stream_generator(),
                        media_type="text/event-stream",
                        headers={
                            "X-Content-Type-Options": "nosniff",
                            "Cache-Control": "no-cache",
                        },
                    )
                    logger.debug(f"Streaming response initiated for {request_id}")
                    return resp
                else:
                    response = stub.chat_completion(request_data)
                    content = (await anext(iterate_in_threadpool(response))).decode()
                    assistant_text = self._extract_nonstream_content(content)
                    if assistant_text:
                        self.chat_memory.save_assistant_message(
                            conversation_id, assistant_text, str(request_id)
                        )
                        self._schedule_summary_refresh(conversation_id, request_data.get('model'))
                        self._append_request_output(str(request_id), assistant_text, emitted_any_bytes=True)
                    self._complete_active_request(str(request_id))
                    logger.debug(f"Non-stream response completed for {request_id}")
                    return Response(content=content, media_type="application/json")
            except Exception as e:
                record = self.active_requests.get(str(request_id))
                emitted_any = bool(record and record.emitted_any_bytes)
                forward_attempts += 1
                if not emitted_any and forward_attempts < self.MAX_FORWARD_RETRY:
                    await asyncio.sleep(self.FORWARD_DELAY_SEC)
                    logger.warning(
                        f"Error in _forward_request before output for {request_id}: {e}. Retry attempts {forward_attempts}"
                    )
                    continue
                self._fail_active_request(str(request_id), e)
                logger.warning(f"Error in _forward_request: {e}. Retry attemps {forward_attempts}")
                break

        return JSONResponse(
            content={"error": "Internal server error"},
            status_code=500,
        )

    async def v1_chat_completions(self, request_data: Dict, request_id: str, received_ts: int):
        return await self._forward_request(request_data, request_id, received_ts)

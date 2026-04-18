import asyncio
from copy import deepcopy
import json
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import aiohttp
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.concurrency import iterate_in_threadpool

from backend.server.chat_memory import ChatMemoryService
from backend.server.constants import NODE_STATUS_AVAILABLE
from backend.server.run_store import RunStore
from backend.server.tool_runtime import ServerToolRuntime
from parallax_utils.logging_config import get_logger
from parallax_utils.request_metrics import get_request_metrics

logger = get_logger(__name__)
INLINE_TOOL_CALL_PATTERN = re.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", re.DOTALL)

AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=20 * 60 * 60)


@dataclass
class ActiveRequestRecord:
    request_id: str
    request_data: Dict
    received_ts: int
    conversation_id: Optional[str] = None
    run_id: Optional[str] = None
    routing_table: Optional[list[str]] = None
    first_hop: Optional[str] = None
    emitted_text: str = ""
    emitted_any_bytes: bool = False
    first_output_recorded: bool = False
    cancel_handle: Any = None
    status: str = "pending"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    last_error: Optional[str] = None


@dataclass
class StoredResponseRecord:
    response_id: str
    payload: Dict[str, Any]
    input_items: list[Any] = field(default_factory=list)
    chat_messages: list[Dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    status: str = "completed"


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
        self.run_store = RunStore()
        self.tool_runtime = ServerToolRuntime()
        self.active_requests: dict[str, ActiveRequestRecord] = {}
        self.summary_tasks: dict[str, asyncio.Task] = {}
        self.responses: dict[str, StoredResponseRecord] = {}

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
        run_id: Optional[str] = None,
    ) -> ActiveRequestRecord:
        record = ActiveRequestRecord(
            request_id=str(request_id),
            request_data=dict(request_data),
            received_ts=received_ts,
            conversation_id=conversation_id,
            run_id=run_id,
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
        if record.run_id:
            self.run_store.update_run(
                record.run_id,
                status='running',
                current_step='Route resolved and request forwarded to an active node',
            )
            self.run_store.append_event(
                record.run_id,
                kind='routing.resolved',
                status='completed',
                title='Routing resolved',
                detail=f"Request forwarded to node {record.first_hop or 'unknown'}.",
            )

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
        if record.run_id and (text or emitted_any_bytes) and not record.first_output_recorded:
            record.first_output_recorded = True
            self.run_store.update_run(
                record.run_id,
                status='running',
                current_step='Generating answer',
            )
            self.run_store.append_event(
                record.run_id,
                kind='model.output',
                status='running',
                title='First output received',
                detail='The model moved from prompt processing into visible output generation.',
            )

    def _fail_active_request(self, request_id: str, error: Exception | str) -> None:
        record = self.active_requests.get(str(request_id))
        if record is None:
            return
        record.status = 'failed'
        record.last_error = str(error)
        record.updated_at = time.time()
        if record.run_id:
            self.run_store.update_run(
                record.run_id,
                status='failed',
                current_step='Request failed before completion',
                summary=str(error),
            )
            self.run_store.append_event(
                record.run_id,
                kind='run.failed',
                status='completed',
                title='Run failed',
                detail=str(error),
            )

    def _complete_active_request(self, request_id: str) -> None:
        record = self.active_requests.get(str(request_id))
        if record is None:
            return
        record.status = 'completed'
        record.updated_at = time.time()
        self.active_requests.pop(str(request_id), None)

    def _set_request_cancel_handle(self, request_id: str, cancel_handle: Any) -> None:
        record = self.active_requests.get(str(request_id))
        if record is None:
            return
        record.cancel_handle = cancel_handle
        record.updated_at = time.time()

    def cancel_run(self, run_id: str) -> Optional[dict[str, Any]]:
        normalized_run_id = str(run_id or '').strip()
        if not normalized_run_id:
            return None
        cancelled_live = False
        for record in list(self.active_requests.values()):
            if str(record.run_id or '').strip() != normalized_run_id:
                continue
            handle = record.cancel_handle
            if handle is not None and hasattr(handle, 'cancel'):
                try:
                    handle.cancel()
                    cancelled_live = True
                except Exception:
                    logger.warning('Failed to cancel active request for run %s', normalized_run_id, exc_info=True)
        detail = self.run_store.cancel_run(
            normalized_run_id,
            summary=(
                'Run was cancelled by an operator.'
                + (' Active execution was interrupted.' if cancelled_live else ' No active execution handle was available to interrupt.')
            ),
        )
        if detail is None:
            return None
        self.run_store.append_event(
            normalized_run_id,
            kind='run.cancelled',
            status='completed',
            title='Run cancelled',
            detail='Operator cancelled the run.'
            + (' Active execution handle was interrupted.' if cancelled_live else ' Cancellation affected persisted state only.'),
        )
        return self.run_store.get_run(normalized_run_id)

    def resume_run(self, run_id: str) -> Optional[dict[str, Any]]:
        normalized_run_id = str(run_id or '').strip()
        if not normalized_run_id:
            return None
        detail = self.run_store.resume_run(normalized_run_id)
        if detail is None:
            return None
        self.run_store.append_event(
            normalized_run_id,
            kind='run.resumed',
            status='completed',
            title='Run marked ready to resume',
            detail='Operator marked the run ready for manual retry or replay.',
        )
        return self.run_store.get_run(normalized_run_id)

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

    @staticmethod
    def _extract_latest_user_prompt(request_data: Dict) -> str:
        messages = request_data.get('messages') or []
        if not isinstance(messages, list):
            return ''
        for message in reversed(messages):
            if not isinstance(message, dict):
                continue
            if str(message.get('role') or '').strip().lower() != 'user':
                continue
            content = message.get('content')
            if isinstance(content, str):
                return content.strip()
        return ''

    @staticmethod
    def _summarize_run_title(prompt: str) -> str:
        text = str(prompt or '').strip()
        if not text:
            return 'Agent run'
        collapsed = ' '.join(text.split())
        return collapsed[:96]

    def _policy_to_run_policy(self, request_data: Dict, policy: Any) -> dict[str, Any]:
        filesystem_access = 'none'
        enabled_tools = {name for name, enabled in dict(getattr(policy, 'tool_enabled', {}) or {}).items() if enabled}
        if 'apply_patch' in enabled_tools or 'exec_command' in enabled_tools:
            filesystem_access = 'workspace-write'
        elif enabled_tools:
            filesystem_access = 'read-only'
        return {
            'routing_mode': 'local_first',
            'remote_provider_used': False,
            'filesystem_access': filesystem_access,
            'network_access': 'allowlisted',
        }

    def _create_run_for_request(
        self,
        *,
        request_id: str,
        request_data: Dict,
        conversation_id: Optional[str],
        policy: Any,
    ) -> Optional[str]:
        if not conversation_id:
            return None
        prompt = self._extract_latest_user_prompt(request_data)
        run = self.run_store.create_run(
            request_id=str(request_id),
            conversation_id=conversation_id,
            title=self._summarize_run_title(prompt),
            model=str(request_data.get('model') or '').strip(),
            requested_by='local-user',
            status='queued',
            current_step='Queued from chat and waiting for route assignment',
            summary='Run created from the current chat request and linked to the conversation timeline.',
            policy=self._policy_to_run_policy(request_data, policy),
        )
        self.run_store.append_event(
            run['id'],
            kind='run.created',
            status='completed',
            title='Run created',
            detail='A persisted run record was created for this chat request.',
        )
        return str(run['id'])

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
        if not isinstance(result, str):
            return ''
        cleaned_content, _ = self._extract_inline_tool_calls_from_text(result)
        return cleaned_content

    def _parse_completion_payload(self, content: str) -> Dict[str, Any]:
        try:
            payload = json.loads(content)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _extract_inline_tool_calls_from_text(self, text: str) -> tuple[str, list[Dict[str, Any]]]:
        content = str(text or '')
        if not content:
            return '', []

        tool_calls: list[Dict[str, Any]] = []

        def _replace(match: re.Match[str]) -> str:
            raw_payload = str(match.group(1) or '').strip()
            if not raw_payload:
                return ''
            try:
                payload = json.loads(raw_payload)
            except Exception:
                return match.group(0)
            if not isinstance(payload, dict):
                return match.group(0)

            name = str(payload.get('name') or '').strip()
            arguments = payload.get('arguments')
            if not name or not isinstance(arguments, dict):
                return match.group(0)

            tool_calls.append(
                {
                    'id': self._new_output_item_id('call'),
                    'type': 'function',
                    'function': {
                        'name': name,
                        'arguments': json.dumps(arguments, ensure_ascii=False),
                    },
                }
            )
            return ''

        cleaned = INLINE_TOOL_CALL_PATTERN.sub(_replace, content).strip()
        return cleaned, tool_calls

    @staticmethod
    def _deepcopy_jsonable(value: Any) -> Any:
        return deepcopy(value)

    @staticmethod
    def _new_response_id() -> str:
        return f"resp_{uuid.uuid4().hex}"

    @staticmethod
    def _new_output_item_id(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex}"

    @staticmethod
    def _normalize_model_role(role: str) -> str:
        normalized = str(role or "").strip().lower()
        if normalized == "developer":
            return "system"
        if normalized in {"system", "user", "assistant", "tool"}:
            return normalized
        return "user"

    @staticmethod
    def _stringify_tool_output(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    def _flatten_response_content(self, content: Any) -> str:
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, dict):
            return self._flatten_response_content([content])
        if not isinstance(content, list):
            return ""

        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                if item:
                    parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "").strip()
            if item_type in {"input_text", "output_text", "text"}:
                text = str(item.get("text") or "").strip()
                if text:
                    parts.append(text)
                continue
            if item_type == "refusal":
                text = str(item.get("refusal") or "").strip()
                if text:
                    parts.append(text)
        return "".join(parts).strip()

    def _normalize_response_input_items(self, input_value: Any) -> list[Any]:
        if input_value is None:
            return []
        if isinstance(input_value, list):
            return list(input_value)
        return [input_value]

    def _response_input_item_to_chat_messages(self, item: Any) -> list[Dict[str, Any]]:
        if isinstance(item, str):
            stripped = item.strip()
            return [{"role": "user", "content": stripped}] if stripped else []

        if not isinstance(item, dict):
            return []

        item_type = str(item.get("type") or "").strip()
        role = item.get("role")

        if item_type == "message" or role is not None:
            normalized_role = self._normalize_model_role(str(role or "user"))
            content = self._flatten_response_content(item.get("content"))
            if not content:
                return []
            return [{"role": normalized_role, "content": content}]

        if item_type == "function_call":
            call_id = str(item.get("call_id") or item.get("id") or "").strip()
            name = str(item.get("name") or "").strip()
            arguments = item.get("arguments")
            argument_text = arguments if isinstance(arguments, str) else self._stringify_tool_output(arguments)
            if not call_id or not name:
                return []
            return [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": argument_text or "{}",
                            },
                        }
                    ],
                }
            ]

        if item_type.endswith("_call_output"):
            call_id = str(item.get("call_id") or item.get("tool_call_id") or "").strip()
            if not call_id:
                return []
            output = item.get("output")
            if output is None and "result" in item:
                output = item.get("result")
            content = self._stringify_tool_output(output)
            status = str(item.get("status") or "").strip().lower()
            if status and status not in {"completed", "success", "ok"}:
                content = json.dumps(
                    {
                        "ok": False,
                        "status": status,
                        "output": output,
                    },
                    ensure_ascii=False,
                )
            return [{"role": "tool", "tool_call_id": call_id, "content": content}]

        if item_type in {"input_text", "text"}:
            text = str(item.get("text") or "").strip()
            return [{"role": "user", "content": text}] if text else []

        return []

    def _response_tool_to_chat_tool(self, tool: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(tool, dict):
            return None
        if str(tool.get("type") or "").strip() != "function":
            return None

        name = str(tool.get("name") or "").strip()
        if not name:
            return None

        function_payload = {
            "name": name,
            "description": str(tool.get("description") or "").strip(),
            "parameters": tool.get("parameters") or {"type": "object", "properties": {}},
        }
        if "strict" in tool:
            function_payload["strict"] = bool(tool.get("strict"))

        return {
            "type": "function",
            "function": function_payload,
        }

    def _responses_tools_to_chat_tools(self, tools: Any) -> list[Dict[str, Any]]:
        converted: list[Dict[str, Any]] = []
        for tool in tools or []:
            mapped = self._response_tool_to_chat_tool(tool)
            if mapped is not None:
                converted.append(mapped)
        return converted

    @staticmethod
    def _response_tool_choice_to_chat_tool_choice(tool_choice: Any) -> Any:
        if isinstance(tool_choice, str):
            return tool_choice
        if not isinstance(tool_choice, dict):
            return None
        if str(tool_choice.get("type") or "").strip() != "function":
            return None
        name = str(tool_choice.get("name") or "").strip()
        if not name:
            return None
        return {
            "type": "function",
            "function": {"name": name},
        }

    def _build_chat_messages_for_response_request(self, request_data: Dict[str, Any]) -> list[Dict[str, Any]]:
        previous_response_id = str(request_data.get("previous_response_id") or "").strip()
        base_messages: list[Dict[str, Any]] = []
        if previous_response_id:
            record = self.responses.get(previous_response_id)
            if record is None:
                raise KeyError(previous_response_id)
            base_messages = self._deepcopy_jsonable(record.chat_messages)

        instructions = str(request_data.get("instructions") or "").strip()
        if instructions:
            system_message = {"role": "system", "content": instructions}
            if base_messages and str(base_messages[0].get("role") or "") == "system":
                base_messages[0] = system_message
            else:
                base_messages.insert(0, system_message)

        input_items = self._normalize_response_input_items(request_data.get("input"))
        for item in input_items:
            base_messages.extend(self._response_input_item_to_chat_messages(item))

        return base_messages

    def _responses_request_to_chat_request(self, request_data: Dict[str, Any]) -> tuple[Dict[str, Any], list[Any], list[Dict[str, Any]]]:
        input_items = self._normalize_response_input_items(request_data.get("input"))
        messages = self._build_chat_messages_for_response_request(request_data)

        chat_request: Dict[str, Any] = {
            "model": request_data.get("model"),
            "messages": messages,
            "stream": False,
            "_disable_chat_memory": True,
        }
        if request_data.get("max_output_tokens") is not None:
            chat_request["max_tokens"] = request_data.get("max_output_tokens")
        if request_data.get("temperature") is not None:
            chat_request["temperature"] = request_data.get("temperature")
        if request_data.get("top_p") is not None:
            chat_request["top_p"] = request_data.get("top_p")
        if request_data.get("stop") is not None:
            chat_request["stop"] = request_data.get("stop")

        tools = self._responses_tools_to_chat_tools(request_data.get("tools") or [])
        if tools:
            chat_request["tools"] = tools

        tool_choice = self._response_tool_choice_to_chat_tool_choice(request_data.get("tool_choice"))
        if tool_choice is not None:
            chat_request["tool_choice"] = tool_choice
        elif isinstance(request_data.get("tool_choice"), str):
            chat_request["tool_choice"] = request_data.get("tool_choice")

        return chat_request, input_items, messages

    def _chat_payload_to_response_usage(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        usage = payload.get("usage")
        if not isinstance(usage, dict):
            return None

        prompt_tokens = int(usage.get("prompt_tokens") or 0)
        completion_tokens = int(usage.get("completion_tokens") or 0)
        total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))
        prompt_details = usage.get("prompt_tokens_details") or {}
        completion_details = usage.get("completion_tokens_details") or {}

        return {
            "input_tokens": prompt_tokens,
            "input_tokens_details": {
                "cached_tokens": int(prompt_details.get("cached_tokens") or 0),
            },
            "output_tokens": completion_tokens,
            "output_tokens_details": {
                "reasoning_tokens": int(completion_details.get("reasoning_tokens") or 0),
            },
            "total_tokens": total_tokens,
        }

    def _chat_payload_to_response_output_items(self, payload: Dict[str, Any]) -> list[Dict[str, Any]]:
        message = self._extract_completion_message(payload)
        output_items: list[Dict[str, Any]] = []

        tool_calls = message.get("tool_calls") or []
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            function = tool_call.get("function") or {}
            call_id = str(tool_call.get("id") or self._new_output_item_id("call")).strip()
            output_items.append(
                {
                    "type": "function_call",
                    "id": self._new_output_item_id("fc"),
                    "call_id": call_id,
                    "name": str(function.get("name") or "").strip(),
                    "arguments": str(function.get("arguments") or "{}"),
                    "status": "completed",
                }
            )

        content = message.get("content")
        if isinstance(content, str) and content.strip():
            output_items.append(
                {
                    "type": "message",
                    "id": self._new_output_item_id("msg"),
                    "status": "completed",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": content,
                            "annotations": [],
                        }
                    ],
                }
            )

        return output_items

    def _chat_payload_to_response_payload(
        self,
        *,
        response_id: str,
        request_data: Dict[str, Any],
        chat_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        created_at = int(chat_payload.get("created") or time.time())
        output_items = self._chat_payload_to_response_output_items(chat_payload)
        reasoning = request_data.get("reasoning") or {}

        payload: Dict[str, Any] = {
            "id": response_id,
            "object": "response",
            "created_at": created_at,
            "status": "completed",
            "completed_at": int(time.time()),
            "error": None,
            "incomplete_details": None,
            "instructions": request_data.get("instructions"),
            "max_output_tokens": request_data.get("max_output_tokens"),
            "model": request_data.get("model") or chat_payload.get("model"),
            "output": output_items,
            "parallel_tool_calls": bool(request_data.get("parallel_tool_calls", True)),
            "previous_response_id": request_data.get("previous_response_id"),
            "reasoning": {
                "effort": reasoning.get("effort"),
                "summary": reasoning.get("summary"),
            },
            "store": bool(request_data.get("store", False)),
            "temperature": request_data.get("temperature", 1.0),
            "text": request_data.get("text") or {"format": {"type": "text"}},
            "tool_choice": request_data.get("tool_choice", "auto"),
            "tools": request_data.get("tools") or [],
            "top_p": request_data.get("top_p", 1.0),
            "truncation": request_data.get("truncation", "disabled"),
            "usage": self._chat_payload_to_response_usage(chat_payload),
            "user": request_data.get("user"),
            "metadata": request_data.get("metadata") or {},
        }
        return payload

    def _build_response_chat_history(
        self,
        *,
        request_messages: list[Dict[str, Any]],
        chat_payload: Dict[str, Any],
    ) -> list[Dict[str, Any]]:
        history = self._deepcopy_jsonable(request_messages)
        assistant_message = self._build_assistant_tool_message(chat_payload)
        if assistant_message.get("content") or assistant_message.get("tool_calls"):
            history.append(assistant_message)
        return history

    def _store_response_record(
        self,
        *,
        response_id: str,
        payload: Dict[str, Any],
        input_items: list[Any],
        chat_messages: list[Dict[str, Any]],
    ) -> None:
        self.responses[response_id] = StoredResponseRecord(
            response_id=response_id,
            payload=self._deepcopy_jsonable(payload),
            input_items=self._deepcopy_jsonable(input_items),
            chat_messages=self._deepcopy_jsonable(chat_messages),
        )

    def _build_responses_sse_from_payload(self, payload: Dict[str, Any]) -> StreamingResponse:
        response_snapshot = self._deepcopy_jsonable(payload)
        response_snapshot["status"] = "in_progress"
        response_snapshot["completed_at"] = None
        response_snapshot["output"] = []
        output_items = list(payload.get("output") or [])

        async def stream_generator():
            yield f"event: response.created\ndata: {json.dumps({'type': 'response.created', 'response': response_snapshot}, separators=(',', ':'))}\n\n".encode("utf-8")
            yield f"event: response.in_progress\ndata: {json.dumps({'type': 'response.in_progress', 'response': response_snapshot}, separators=(',', ':'))}\n\n".encode("utf-8")

            for output_index, item in enumerate(output_items):
                in_progress_item = self._deepcopy_jsonable(item)
                if isinstance(in_progress_item, dict):
                    in_progress_item["status"] = "in_progress"
                    if str(in_progress_item.get("type") or "") == "message":
                        in_progress_item["content"] = []
                yield f"event: response.output_item.added\ndata: {json.dumps({'type': 'response.output_item.added', 'output_index': output_index, 'item': in_progress_item}, separators=(',', ':'))}\n\n".encode("utf-8")

                if str(item.get("type") or "") == "message":
                    content_items = list(item.get("content") or [])
                    for content_index, content_item in enumerate(content_items):
                        if str(content_item.get("type") or "") != "output_text":
                            continue
                        added_part = {
                            "type": "output_text",
                            "text": "",
                            "annotations": list(content_item.get("annotations") or []),
                        }
                        yield f"event: response.content_part.added\ndata: {json.dumps({'type': 'response.content_part.added', 'item_id': item.get('id'), 'output_index': output_index, 'content_index': content_index, 'part': added_part}, separators=(',', ':'))}\n\n".encode('utf-8')
                        text = str(content_item.get("text") or "")
                        if text:
                            yield f"event: response.output_text.delta\ndata: {json.dumps({'type': 'response.output_text.delta', 'item_id': item.get('id'), 'output_index': output_index, 'content_index': content_index, 'delta': text}, separators=(',', ':'))}\n\n".encode('utf-8')
                        yield f"event: response.output_text.done\ndata: {json.dumps({'type': 'response.output_text.done', 'item_id': item.get('id'), 'output_index': output_index, 'content_index': content_index, 'text': text}, separators=(',', ':'))}\n\n".encode('utf-8')
                        yield f"event: response.content_part.done\ndata: {json.dumps({'type': 'response.content_part.done', 'item_id': item.get('id'), 'output_index': output_index, 'content_index': content_index, 'part': content_item}, separators=(',', ':'))}\n\n".encode('utf-8')

                yield f"event: response.output_item.done\ndata: {json.dumps({'type': 'response.output_item.done', 'output_index': output_index, 'item': item}, separators=(',', ':'))}\n\n".encode("utf-8")

            yield f"event: response.completed\ndata: {json.dumps({'type': 'response.completed', 'response': payload}, separators=(',', ':'))}\n\n".encode("utf-8")

        return StreamingResponse(
            stream_generator(),
            media_type="text/event-stream",
            headers={
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "no-cache",
            },
        )

    def get_response(self, response_id: str) -> Optional[Dict[str, Any]]:
        record = self.responses.get(str(response_id or "").strip())
        if record is None:
            return None
        return self._deepcopy_jsonable(record.payload)

    def get_response_input_items(self, response_id: str) -> Optional[Dict[str, Any]]:
        record = self.responses.get(str(response_id or "").strip())
        if record is None:
            return None
        return {
            "object": "list",
            "data": self._deepcopy_jsonable(record.input_items),
        }

    def cancel_response(self, response_id: str) -> Optional[Dict[str, Any]]:
        record = self.responses.get(str(response_id or "").strip())
        if record is None:
            return None
        record.status = "cancelled"
        record.payload["status"] = "cancelled"
        return self._deepcopy_jsonable(record.payload)

    @staticmethod
    def _extract_completion_message(payload: Dict[str, Any]) -> Dict[str, Any]:
        choices = payload.get("choices") or []
        if not choices:
            return {}
        message = choices[0].get("message") or {}
        return message if isinstance(message, dict) else {}

    def _extract_nonstream_tool_calls(self, content: str) -> list[Dict[str, Any]]:
        payload = self._parse_completion_payload(content)
        message = self._extract_completion_message(payload)
        tool_calls = message.get("tool_calls") or []
        normalized_tool_calls = [tool_call for tool_call in tool_calls if isinstance(tool_call, dict)]
        inline_tool_calls: list[Dict[str, Any]] = []
        content_text = message.get("content")
        if isinstance(content_text, str) and content_text:
            _, inline_tool_calls = self._extract_inline_tool_calls_from_text(content_text)
        return [*normalized_tool_calls, *inline_tool_calls]

    def _build_assistant_tool_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        message = self._extract_completion_message(payload)
        content = message.get("content") or ""
        inline_tool_calls: list[Dict[str, Any]] = []
        if isinstance(content, str) and content:
            content, inline_tool_calls = self._extract_inline_tool_calls_from_text(content)
        return {
            "role": "assistant",
            "content": content,
            "tool_calls": [*(message.get("tool_calls") or []), *inline_tool_calls],
        }

    def _build_sse_response_from_payload(
        self, payload: Dict[str, Any], request_id: str, model: Optional[str] = None
    ) -> StreamingResponse:
        response_payload = dict(payload)
        response_payload["id"] = str(request_id)
        response_payload["object"] = "chat.completion"
        if model:
            response_payload["model"] = model
        message = self._extract_completion_message(response_payload)
        choice = ((response_payload.get("choices") or [{}])[0]) if response_payload.get("choices") else {}
        finish_reason = choice.get("finish_reason", "stop")
        prompt_budget = response_payload.get("prompt_budget")
        input_truncation = response_payload.get("input_truncation")
        usage = response_payload.get("usage")
        created = response_payload.get("created", time.time())
        content = message.get("content") or ""

        first_chunk = {
            "id": str(request_id),
            "object": "chat.completion.chunk",
            "model": response_payload.get("model"),
            "created": created,
            "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}],
            "usage": usage,
        }
        if input_truncation is not None:
            first_chunk["input_truncation"] = input_truncation
        if prompt_budget is not None:
            first_chunk["prompt_budget"] = prompt_budget

        content_chunk = {
            "id": str(request_id),
            "object": "chat.completion.chunk",
            "model": response_payload.get("model"),
            "created": created,
            "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
            "usage": usage,
        }
        final_chunk = {
            "id": str(request_id),
            "object": "chat.completion.chunk",
            "model": response_payload.get("model"),
            "created": created,
            "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
            "usage": usage,
        }

        async def stream_generator():
            yield f"data: {json.dumps(first_chunk, separators=(',', ':'))}\n\n".encode("utf-8")
            if content:
                yield f"data: {json.dumps(content_chunk, separators=(',', ':'))}\n\n".encode("utf-8")
            yield f"data: {json.dumps(final_chunk, separators=(',', ':'))}\n\n".encode("utf-8")
            yield b"data: [DONE]\n\n"

        return StreamingResponse(
            stream_generator(),
            media_type="text/event-stream",
            headers={
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "no-cache",
            },
        )

    async def _forward_prepared_request(
        self,
        request_data: Dict,
        request_id: str,
        received_ts: int,
        *,
        conversation_id: Optional[str],
        run_id: Optional[str] = None,
        save_assistant: bool = True,
        finalize_run: bool = False,
    ):
        self._register_active_request(str(request_id), request_data, received_ts, conversation_id, run_id=run_id)
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
                            self._set_request_cancel_handle(str(request_id), response)
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
                                if assistant_text and save_assistant:
                                    self.chat_memory.save_assistant_message(
                                        conversation_id, assistant_text, str(request_id)
                                    )
                                    self._schedule_summary_refresh(conversation_id, request_data.get('model'))
                                if assistant_text:
                                    self._append_request_output(str(request_id), assistant_text, emitted_any_bytes=True)
                                if finalize_run and run_id:
                                    self.run_store.update_run(
                                        run_id,
                                        status='completed',
                                        current_step='Completed',
                                        summary=assistant_text[:800] if assistant_text else 'Run completed successfully.',
                                    )
                                    self.run_store.append_event(
                                        run_id,
                                        kind='run.completed',
                                        status='completed',
                                        title='Run completed',
                                        detail='The streaming request finished successfully.',
                                    )
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
                                self._set_request_cancel_handle(str(request_id), None)
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
                    self._set_request_cancel_handle(str(request_id), response)
                    content = (await anext(iterate_in_threadpool(response))).decode()
                    assistant_text = self._extract_nonstream_content(content)
                    if assistant_text and save_assistant:
                        self.chat_memory.save_assistant_message(
                            conversation_id, assistant_text, str(request_id)
                        )
                        self._schedule_summary_refresh(conversation_id, request_data.get('model'))
                    if assistant_text:
                        self._append_request_output(str(request_id), assistant_text, emitted_any_bytes=True)
                    if finalize_run and run_id:
                        self.run_store.update_run(
                            run_id,
                            status='completed',
                            current_step='Completed',
                            summary=assistant_text[:800] if assistant_text else 'Run completed successfully.',
                        )
                        self.run_store.append_event(
                            run_id,
                            kind='run.completed',
                            status='completed',
                            title='Run completed',
                            detail='The non-streaming request finished successfully.',
                        )
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

    async def _forward_request(self, request_data: Dict, request_id: str, received_ts: int):
        prepared_request, conversation_id = self.chat_memory.prepare_request(request_data)
        policy = self.tool_runtime.resolve_policy(request_data)
        prepared_request = self.tool_runtime.inject_builtin_tools(prepared_request, policy)
        run_id = self._create_run_for_request(
            request_id=str(request_id),
            request_data=prepared_request,
            conversation_id=conversation_id,
            policy=policy,
        )

        if not policy.enabled:
            return await self._forward_prepared_request(
                prepared_request,
                request_id,
                received_ts,
                conversation_id=conversation_id,
                run_id=run_id,
                finalize_run=True,
            )

        original_stream = bool(prepared_request.get("stream", False))
        tool_request = dict(prepared_request)
        tool_request["stream"] = False

        for iteration in range(policy.max_iterations):
            current_request_id = (
                str(request_id) if iteration == policy.max_iterations - 1 and not original_stream else f"{request_id}:tool:{iteration}"
            )
            response = await self._forward_prepared_request(
                tool_request,
                current_request_id,
                received_ts,
                conversation_id=conversation_id,
                run_id=run_id,
                save_assistant=False,
                finalize_run=False,
            )
            if not isinstance(response, Response) or response.status_code != 200:
                return response

            try:
                content = response.body.decode()
            except Exception:
                return response

            tool_calls = self._extract_nonstream_tool_calls(content)
            if not tool_calls or not self.tool_runtime.can_execute(tool_calls, policy):
                payload = self._parse_completion_payload(content)
                assistant_text = self._extract_nonstream_content(content)
                if assistant_text:
                    self.chat_memory.save_assistant_message(
                        conversation_id, assistant_text, str(request_id)
                    )
                    self._schedule_summary_refresh(conversation_id, prepared_request.get("model"))
                if original_stream:
                    if run_id:
                        self.run_store.update_run(
                            run_id,
                            status='completed',
                            current_step='Completed',
                            summary=assistant_text[:800] if assistant_text else 'Run completed successfully.',
                        )
                        self.run_store.append_event(
                            run_id,
                            kind='run.completed',
                            status='completed',
                            title='Run completed',
                            detail='The tool loop produced a final assistant response and closed successfully.',
                        )
                    return self._build_sse_response_from_payload(
                        payload,
                        str(request_id),
                        model=prepared_request.get("model"),
                    )
                if payload:
                    payload["id"] = str(request_id)
                    if run_id:
                        self.run_store.update_run(
                            run_id,
                            status='completed',
                            current_step='Completed',
                            summary=assistant_text[:800] if assistant_text else 'Run completed successfully.',
                        )
                        self.run_store.append_event(
                            run_id,
                            kind='run.completed',
                            status='completed',
                            title='Run completed',
                            detail='The tool loop produced a final assistant response.',
                        )
                    return Response(
                        content=json.dumps(payload, separators=(",", ":")),
                        media_type="application/json",
                    )
                return response

            if run_id:
                self.run_store.increment_tool_count(run_id, len(tool_calls))
                self.run_store.update_run(
                    run_id,
                    status='running',
                    current_step='Executing tool calls',
                    summary='The agent paused model generation to execute tool calls.',
                )
                self.run_store.append_event(
                    run_id,
                    kind='tool.called',
                    status='completed',
                    title='Tool call requested',
                    detail=f'The model requested {len(tool_calls)} tool call(s).',
                    metadata={'tool_names': [str((tool_call.get("function") or {}).get("name") or '') for tool_call in tool_calls]},
                )

            try:
                tool_messages = await self.tool_runtime.execute_tool_calls(tool_calls)
            except Exception as exc:
                logger.warning("Server tool execution failed for %s: %s", request_id, exc, exc_info=True)
                tool_messages = [
                    {
                        "role": "tool",
                        "tool_call_id": str(tool_call.get("id") or ""),
                        "content": json.dumps(
                            {"ok": False, "error": str(exc)},
                            ensure_ascii=False,
                        ),
                    }
                    for tool_call in tool_calls
                ]
                if run_id:
                    self.run_store.append_event(
                        run_id,
                        kind='tool.failed',
                        status='completed',
                        title='Tool execution failed',
                        detail=str(exc),
                    )
            else:
                if run_id:
                    self.run_store.append_event(
                        run_id,
                        kind='tool.completed',
                        status='completed',
                        title='Tool execution completed',
                        detail=f'Executed {len(tool_messages)} tool result message(s).',
                    )

            payload = self._parse_completion_payload(content)
            next_messages = list(tool_request.get("messages") or [])
            next_messages.append(self._build_assistant_tool_message(payload))
            next_messages.extend(tool_messages)
            tool_request = dict(tool_request)
            tool_request["messages"] = next_messages

        if run_id:
            self.run_store.update_run(
                run_id,
                status='failed',
                current_step='Tool loop exceeded maximum iterations',
                summary='The run hit the configured tool-loop iteration cap before producing a final answer.',
            )
            self.run_store.append_event(
                run_id,
                kind='run.failed',
                status='completed',
                title='Tool loop exceeded limit',
                detail='The run exceeded the configured maximum number of tool iterations.',
            )
        return JSONResponse(
            content={"error": "Server tool loop exceeded maximum iterations"},
            status_code=500,
        )

    async def v1_chat_completions(self, request_data: Dict, request_id: str, received_ts: int):
        return await self._forward_request(request_data, request_id, received_ts)

    async def v1_responses(self, request_data: Dict, request_id: str, received_ts: int):
        response_id = self._new_response_id()
        try:
            chat_request, input_items, request_messages = self._responses_request_to_chat_request(request_data)
        except KeyError as exc:
            return JSONResponse(
                content={
                    "error": {
                        "message": f"Unknown previous_response_id: {exc.args[0]}",
                        "type": "invalid_request_error",
                        "param": "previous_response_id",
                        "code": "not_found",
                    }
                },
                status_code=404,
            )

        response = await self._forward_request(chat_request, response_id, received_ts)
        if not isinstance(response, Response) or response.status_code != 200:
            return response

        try:
            content = response.body.decode()
        except Exception:
            return JSONResponse(
                content={
                    "error": {
                        "message": "Failed to decode backend response",
                        "type": "server_error",
                    }
                },
                status_code=500,
            )

        chat_payload = self._parse_completion_payload(content)
        if not chat_payload:
            return JSONResponse(
                content={
                    "error": {
                        "message": "Backend returned an empty response payload",
                        "type": "server_error",
                    }
                },
                status_code=500,
            )

        response_payload = self._chat_payload_to_response_payload(
            response_id=response_id,
            request_data=request_data,
            chat_payload=chat_payload,
        )
        response_history = self._build_response_chat_history(
            request_messages=request_messages,
            chat_payload=chat_payload,
        )
        self._store_response_record(
            response_id=response_id,
            payload=response_payload,
            input_items=input_items,
            chat_messages=response_history,
        )

        if bool(request_data.get("stream")):
            return self._build_responses_sse_from_payload(response_payload)

        return Response(
            content=json.dumps(response_payload, separators=(",", ":")),
            media_type="application/json",
        )

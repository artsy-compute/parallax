import asyncio
import json

from fastapi.responses import Response, StreamingResponse

from backend.server.request_handler import RequestHandler, StoredResponseRecord


def _chat_completion_payload(*, content: str = "", tool_calls: list[dict] | None = None) -> dict:
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 123,
        "model": "gpt-5.4",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": tool_calls or None,
                },
                "finish_reason": "tool_calls" if tool_calls else "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 0},
            "completion_tokens_details": {"reasoning_tokens": 0},
        },
    }


async def _collect_streaming_response(resp: StreamingResponse) -> str:
    chunks: list[str] = []
    async for chunk in resp.body_iterator:
        if isinstance(chunk, bytes):
            chunks.append(chunk.decode("utf-8"))
        else:
            chunks.append(str(chunk))
    return "".join(chunks)


def test_responses_request_maps_previous_response_and_tool_output():
    handler = RequestHandler()
    handler.responses["resp_prev"] = StoredResponseRecord(
        response_id="resp_prev",
        payload={"id": "resp_prev"},
        input_items=[],
        chat_messages=[
            {"role": "system", "content": "You are helpful."},
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "read_file", "arguments": "{\"path\":\"a.txt\"}"},
                    }
                ],
            },
        ],
    )

    request = {
        "model": "gpt-5.4",
        "instructions": "You are helpful.",
        "previous_response_id": "resp_prev",
        "max_output_tokens": 256,
        "tool_choice": "auto",
        "tools": [
            {
                "type": "function",
                "name": "read_file",
                "description": "Read a file",
                "parameters": {"type": "object", "properties": {"path": {"type": "string"}}},
                "strict": True,
            }
        ],
        "input": [
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": {"ok": True, "content": "hello"},
            }
        ],
    }

    chat_request, input_items, messages = handler._responses_request_to_chat_request(request)

    assert input_items[0]["type"] == "function_call_output"
    assert chat_request["model"] == "gpt-5.4"
    assert chat_request["max_tokens"] == 256
    assert chat_request["_disable_chat_memory"] is True
    assert chat_request["tool_choice"] == "auto"
    assert chat_request["tools"][0]["function"]["name"] == "read_file"
    assert messages[-1]["role"] == "tool"
    assert messages[-1]["tool_call_id"] == "call_1"
    assert "hello" in messages[-1]["content"]


def test_responses_request_maps_codex_followup_input_items():
    handler = RequestHandler()

    request = {
        "model": "gpt-5.4",
        "instructions": "You are Codex.",
        "tools": [
            {
                "type": "function",
                "name": "exec_command",
                "description": "Run a shell command.",
                "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}},
            }
        ],
        "input": [
            {
                "type": "message",
                "role": "developer",
                "content": [{"type": "input_text", "text": "Use rg for search."}],
            },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "exec_command",
                "arguments": "{\"cmd\":\"printf tool-ok\"}",
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "tool-ok",
            },
        ],
    }

    chat_request, _, messages = handler._responses_request_to_chat_request(request)

    assert chat_request["tools"][0]["function"]["name"] == "exec_command"
    assert messages[0] == {"role": "system", "content": "You are Codex."}
    assert messages[1] == {"role": "system", "content": "Use rg for search."}
    assert messages[2]["role"] == "assistant"
    assert messages[2]["tool_calls"][0]["id"] == "call_1"
    assert messages[3] == {"role": "tool", "tool_call_id": "call_1", "content": "tool-ok"}


def test_v1_responses_builds_nonstream_payload_and_stores_history():
    handler = RequestHandler()

    async def fake_forward(request_data, request_id, received_ts):
        assert request_data["_disable_chat_memory"] is True
        return Response(
            content=json.dumps(_chat_completion_payload(content="done")),
            media_type="application/json",
        )

    handler._forward_request = fake_forward

    response = asyncio.run(
        handler.v1_responses(
            {
                "model": "gpt-5.4",
                "instructions": "You are helpful.",
                "input": "say hi",
                "store": False,
                "stream": False,
                "text": {"format": {"type": "text"}},
            },
            "req-1",
            0,
        )
    )

    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["object"] == "response"
    assert payload["status"] == "completed"
    assert payload["output"][0]["type"] == "message"
    assert payload["output"][0]["content"][0]["text"] == "done"

    stored = handler.get_response(payload["id"])
    assert stored is not None
    assert stored["id"] == payload["id"]

    input_items = handler.get_response_input_items(payload["id"])
    assert input_items == {"object": "list", "data": ["say hi"]}


def test_v1_responses_maps_function_calls():
    handler = RequestHandler()

    async def fake_forward(request_data, request_id, received_ts):
        return Response(
            content=json.dumps(
                _chat_completion_payload(
                    tool_calls=[
                        {
                            "id": "call_abc123",
                            "type": "function",
                            "function": {
                                "name": "read_file",
                                "arguments": "{\"path\":\"README.md\"}",
                            },
                        }
                    ]
                )
            ),
            media_type="application/json",
        )

    handler._forward_request = fake_forward

    response = asyncio.run(
        handler.v1_responses(
            {
                "model": "gpt-5.4",
                "instructions": "You are helpful.",
                "input": "inspect the repo",
                "stream": False,
            },
            "req-2",
            0,
        )
    )

    payload = json.loads(response.body)
    assert payload["output"][0]["type"] == "function_call"
    assert payload["output"][0]["call_id"] == "call_abc123"
    assert payload["output"][0]["name"] == "read_file"


def test_v1_responses_stream_emits_completed_event():
    handler = RequestHandler()

    async def fake_forward(request_data, request_id, received_ts):
        return Response(
            content=json.dumps(_chat_completion_payload(content="streamed text")),
            media_type="application/json",
        )

    handler._forward_request = fake_forward

    response = asyncio.run(
        handler.v1_responses(
            {
                "model": "gpt-5.4",
                "instructions": "You are helpful.",
                "input": "stream this",
                "stream": True,
            },
            "req-3",
            0,
        )
    )

    assert isinstance(response, StreamingResponse)
    streamed = asyncio.run(_collect_streaming_response(response))
    assert "event: response.created" in streamed
    assert "event: response.output_text.delta" in streamed
    assert "event: response.completed" in streamed


def test_v1_responses_rejects_unknown_previous_response_id():
    handler = RequestHandler()

    response = asyncio.run(
        handler.v1_responses(
            {
                "model": "gpt-5.4",
                "previous_response_id": "resp_missing",
                "input": "continue",
            },
            "req-4",
            0,
        )
    )

    assert response.status_code == 404
    payload = json.loads(response.body)
    assert payload["error"]["param"] == "previous_response_id"

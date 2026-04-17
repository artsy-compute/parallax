import json
import asyncio
from pathlib import Path

import pytest

from backend.server.tool_runtime import ServerToolRuntime


@pytest.fixture()
def tool_runtime(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("PARALLAX_TOOL_FILE_ROOTS", str(tmp_path))
    monkeypatch.setenv("PARALLAX_SERVER_TOOLS_ENABLED", "1")
    return ServerToolRuntime()


def test_inject_builtin_tools(tool_runtime: ServerToolRuntime):
    request = {"messages": [{"role": "user", "content": "hi"}]}
    policy = tool_runtime.resolve_policy(request)
    enriched = tool_runtime.inject_builtin_tools(request, policy)
    tool_names = [tool["function"]["name"] for tool in enriched["tools"]]
    assert "fetch_url" in tool_names
    assert "read_file" in tool_names


def test_read_file_within_allowed_root(tool_runtime: ServerToolRuntime, tmp_path: Path):
    target = tmp_path / "notes.txt"
    target.write_text("one\ntwo\nthree\n", encoding="utf-8")

    payload = tool_runtime._read_file({"path": str(target), "start_line": 2, "end_line": 3})
    assert payload["ok"] is True
    assert payload["content"] == "two\nthree"


def test_read_file_rejects_outside_allowed_root(tool_runtime: ServerToolRuntime, tmp_path: Path):
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("secret", encoding="utf-8")

    with pytest.raises(ValueError):
        tool_runtime._read_file({"path": str(outside)})


def test_extract_response_text_html(tool_runtime: ServerToolRuntime):
    html = """
    <html>
      <head><title>Example</title><style>.x{display:none}</style></head>
      <body><h1>Hello</h1><p>World</p><script>ignored()</script></body>
    </html>
    """
    text = tool_runtime._extract_response_text(html, "text/html; charset=utf-8")
    assert "Hello" in text
    assert "World" in text
    assert "ignored" not in text


def test_execute_tool_calls_read_file(tool_runtime: ServerToolRuntime, tmp_path: Path):
    target = tmp_path / "doc.txt"
    target.write_text("alpha\nbeta\n", encoding="utf-8")

    results = asyncio.run(
        tool_runtime.execute_tool_calls(
            [
                {
                    "id": "call-1",
                    "function": {
                        "name": "read_file",
                        "arguments": json.dumps({"path": str(target)}),
                    },
                }
            ]
        )
    )

    assert len(results) == 1
    assert results[0]["role"] == "tool"
    payload = json.loads(results[0]["content"])
    assert payload["ok"] is True
    assert "alpha" in payload["content"]

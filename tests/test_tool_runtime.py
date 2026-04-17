import asyncio
import json
from pathlib import Path

import pytest

from backend.server.tool_runtime import ServerToolRuntime


@pytest.fixture()
def tool_runtime(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("PARALLAX_TOOL_FILE_ROOTS", str(tmp_path))
    monkeypatch.setenv("PARALLAX_SERVER_TOOLS_ENABLED", "1")
    monkeypatch.delenv("PARALLAX_TOOL_PLUGIN_DIRS", raising=False)
    return ServerToolRuntime()


def test_inject_builtin_tools(tool_runtime: ServerToolRuntime):
    request = {"messages": [{"role": "user", "content": "hi"}]}
    policy = tool_runtime.resolve_policy(request)
    enriched = tool_runtime.inject_builtin_tools(request, policy)
    tool_names = [tool["function"]["name"] for tool in enriched["tools"]]
    assert "fetch_url" in tool_names
    assert "fetch_json" in tool_names
    assert "read_file" in tool_names
    assert "list_files" in tool_names
    assert "search_files" in tool_names
    assert "get_cluster_status" in tool_names
    assert "list_nodes" in tool_names
    assert "list_models" in tool_names
    assert "get_join_command" in tool_names


def test_describe_available_tools(tool_runtime: ServerToolRuntime):
    descriptions = {item["name"]: item for item in tool_runtime.describe_available_tools()}
    assert descriptions["fetch_url"]["plugin_name"] == "web"
    assert descriptions["fetch_json"]["plugin_name"] == "web"
    assert descriptions["read_file"]["plugin_name"] == "files"
    assert descriptions["get_cluster_status"]["plugin_name"] == "parallax"
    assert descriptions["list_files"]["enabled_by_default"] is True


def test_execute_parallax_tools(tool_runtime: ServerToolRuntime):
    tool_runtime.set_context(
        get_cluster_status=lambda: {"ok": True, "type": "cluster_status"},
        list_nodes=lambda: [{"id": "node-1"}],
        list_models=lambda: [{"name": "model-1"}],
        get_join_command=lambda: {"command": "parallax join --scheduler-addr /ip4/127.0.0.1/tcp/1/p2p/peer"},
    )

    results = asyncio.run(
        tool_runtime.execute_tool_calls(
            [
                {"id": "c1", "function": {"name": "get_cluster_status", "arguments": "{}"}},
                {"id": "c2", "function": {"name": "list_nodes", "arguments": "{}"}},
                {"id": "c3", "function": {"name": "list_models", "arguments": "{}"}},
                {"id": "c4", "function": {"name": "get_join_command", "arguments": "{}"}},
            ]
        )
    )
    payloads = [json.loads(item["content"]) for item in results]
    assert payloads[0]["type"] == "cluster_status"
    assert payloads[1]["nodes"][0]["id"] == "node-1"
    assert payloads[2]["models"][0]["name"] == "model-1"
    assert "parallax join" in payloads[3]["join_command"]["command"]


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

    payload = json.loads(results[0]["content"])
    assert payload["ok"] is True
    assert "alpha" in payload["content"]


def test_list_files_and_search_files(tool_runtime: ServerToolRuntime, tmp_path: Path):
    notes = tmp_path / "notes.txt"
    notes.write_text("hello world\nsecond line\n", encoding="utf-8")
    nested = tmp_path / "sub"
    nested.mkdir()
    (nested / "other.txt").write_text("world again\n", encoding="utf-8")

    listed = asyncio.run(
        tool_runtime.execute_tool_calls(
            [
                {
                    "id": "call-1",
                    "function": {
                        "name": "list_files",
                        "arguments": json.dumps({"path": str(tmp_path), "recursive": True}),
                    },
                }
            ]
        )
    )
    list_payload = json.loads(listed[0]["content"])
    assert any(item["name"] == "notes.txt" for item in list_payload["entries"])

    searched = asyncio.run(
        tool_runtime.execute_tool_calls(
            [
                {
                    "id": "call-2",
                    "function": {
                        "name": "search_files",
                        "arguments": json.dumps({"path": str(tmp_path), "pattern": "world"}),
                    },
                }
            ]
        )
    )
    search_payload = json.loads(searched[0]["content"])
    assert any(match["path"].endswith("notes.txt") for match in search_payload["matches"])


def test_read_file_rejects_outside_allowed_root(tool_runtime: ServerToolRuntime, tmp_path: Path):
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("secret", encoding="utf-8")

    with pytest.raises(ValueError):
        asyncio.run(
            tool_runtime.execute_tool_calls(
                [
                    {
                        "id": "call-1",
                        "function": {
                            "name": "read_file",
                            "arguments": json.dumps({"path": str(outside)}),
                        },
                    }
                ]
            )
        )


def test_dynamic_plugin_loading(monkeypatch, tmp_path: Path):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    plugin_file = plugin_dir / "echo_plugin.py"
    plugin_file.write_text(
        """
from backend.server.tools.base import ToolDefinition


class EchoPlugin:
    name = "echo"

    def get_tools(self):
        return [
            ToolDefinition(
                name="echo_text",
                plugin_name=self.name,
                kind="utility",
                description="Echo text back.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "echo_text",
                        "description": "Echo text back.",
                        "parameters": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    },
                },
            )
        ]

    async def execute(self, tool_name, arguments):
        return {"ok": True, "text": str(arguments.get("text") or "")}


def get_plugin():
    return EchoPlugin()
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("PARALLAX_TOOL_FILE_ROOTS", str(tmp_path))
    monkeypatch.setenv("PARALLAX_TOOL_PLUGIN_DIRS", str(plugin_dir))
    runtime = ServerToolRuntime()
    descriptions = {item["name"]: item for item in runtime.describe_available_tools()}
    assert "echo_text" in descriptions

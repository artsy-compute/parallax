from __future__ import annotations

from typing import Any, Callable

from backend.server.tools.base import ToolDefinition


class ParallaxToolsPlugin:
    name = "parallax"

    def __init__(
        self,
        *,
        get_cluster_status: Callable[[], dict[str, Any]],
        list_nodes: Callable[[], list[dict[str, Any]]],
        list_models: Callable[[], list[dict[str, Any]]],
        get_join_command: Callable[[], dict[str, Any]],
    ):
        self._get_cluster_status = get_cluster_status
        self._list_nodes = list_nodes
        self._list_models = list_models
        self._get_join_command = get_join_command

    def get_tools(self) -> list[ToolDefinition]:
        return [
            ToolDefinition(
                name="get_cluster_status",
                plugin_name=self.name,
                kind="parallax",
                description="Return the current Parallax cluster status payload.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "get_cluster_status",
                        "description": "Return the current Parallax cluster status payload.",
                        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                    },
                },
            ),
            ToolDefinition(
                name="list_nodes",
                plugin_name=self.name,
                kind="parallax",
                description="List nodes currently known to the Parallax scheduler.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "list_nodes",
                        "description": "List nodes currently known to the Parallax scheduler.",
                        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                    },
                },
            ),
            ToolDefinition(
                name="list_models",
                plugin_name=self.name,
                kind="parallax",
                description="List models currently available to the Parallax backend.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "list_models",
                        "description": "List models currently available to the Parallax backend.",
                        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                    },
                },
            ),
            ToolDefinition(
                name="get_join_command",
                plugin_name=self.name,
                kind="parallax",
                description="Return the recommended node join command for the current scheduler.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "get_join_command",
                        "description": "Return the recommended node join command for the current scheduler.",
                        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                    },
                },
            ),
        ]

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool_name == "get_cluster_status":
            return self._get_cluster_status()
        if tool_name == "list_nodes":
            return {"ok": True, "nodes": self._list_nodes()}
        if tool_name == "list_models":
            return {"ok": True, "models": self._list_models()}
        if tool_name == "get_join_command":
            return {"ok": True, "join_command": self._get_join_command()}
        raise ValueError(f"Unsupported tool for parallax plugin: {tool_name}")

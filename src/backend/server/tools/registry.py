from __future__ import annotations

from typing import Any

from backend.server.tools.base import ToolDefinition, ToolPlugin


class ToolRegistry:
    def __init__(self):
        self._plugins: dict[str, ToolPlugin] = {}
        self._tools: dict[str, ToolDefinition] = {}
        self._tool_plugins: dict[str, ToolPlugin] = {}

    def register(self, plugin: ToolPlugin) -> None:
        self._plugins[plugin.name] = plugin
        for tool in plugin.get_tools():
            self._tools[tool.name] = tool
            self._tool_plugins[tool.name] = plugin

    def list_tools(self) -> list[ToolDefinition]:
        return [self._tools[name] for name in sorted(self._tools)]

    def has_tool(self, name: str) -> bool:
        return str(name or "").strip() in self._tools

    def get_tool(self, name: str) -> ToolDefinition | None:
        return self._tools.get(str(name or "").strip())

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        normalized = str(tool_name or "").strip()
        plugin = self._tool_plugins.get(normalized)
        if plugin is None:
            raise ValueError(f"Unsupported server tool: {tool_name}")
        return await plugin.execute(normalized, arguments)

    def describe_tools(self) -> list[dict[str, Any]]:
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "kind": tool.kind,
                "plugin_name": tool.plugin_name,
                **dict(tool.metadata or {}),
            }
            for tool in self.list_tools()
        ]


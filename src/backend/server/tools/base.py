from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    schema: dict[str, Any]
    description: str
    kind: str
    plugin_name: str
    metadata: dict[str, Any] = field(default_factory=dict)


class ToolPlugin(Protocol):
    name: str

    def get_tools(self) -> list[ToolDefinition]:
        ...

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        ...


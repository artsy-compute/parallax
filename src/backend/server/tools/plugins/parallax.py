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
        get_nodes_overview: Callable[[], dict[str, Any]],
    ):
        self._get_cluster_status = get_cluster_status
        self._list_nodes = list_nodes
        self._list_models = list_models
        self._get_join_command = get_join_command
        self._get_nodes_overview = get_nodes_overview

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
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string", "description": "Optional node status filter."},
                                "hostname": {"type": "string", "description": "Optional hostname substring filter."},
                                "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                            },
                            "additionalProperties": False,
                        },
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
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "query": {"type": "string", "description": "Optional model name substring filter."},
                                "custom_only": {"type": "boolean", "description": "Return only custom/imported models."},
                                "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                            },
                            "additionalProperties": False,
                        },
                    },
                },
            ),
            ToolDefinition(
                name="get_model_details",
                plugin_name=self.name,
                kind="parallax",
                description="Return details for a specific model available to the Parallax backend.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "get_model_details",
                        "description": "Return details for a specific model available to the Parallax backend.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "model_name": {"type": "string", "description": "Exact model name to inspect."},
                            },
                            "required": ["model_name"],
                            "additionalProperties": False,
                        },
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
            ToolDefinition(
                name="get_nodes_overview",
                plugin_name=self.name,
                kind="parallax",
                description="Return configured host inventory and live node overview for the current cluster.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "get_nodes_overview",
                        "description": "Return configured host inventory and live node overview for the current cluster.",
                        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                    },
                },
            ),
        ]

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool_name == "get_cluster_status":
            cluster_status = self._get_cluster_status()
            data = dict(cluster_status.get("data") or {})
            node_list = list(data.get("node_list") or [])
            return {
                "ok": True,
                "cluster": cluster_status,
                "summary": {
                    "status": data.get("status"),
                    "model_name": data.get("model_name"),
                    "node_count": len(node_list),
                    "need_more_nodes": bool(data.get("need_more_nodes")),
                    "max_running_request": data.get("max_running_request"),
                },
            }
        if tool_name == "list_nodes":
            nodes = list(self._list_nodes())
            status_filter = str(arguments.get("status") or "").strip().lower()
            hostname_filter = str(arguments.get("hostname") or "").strip().lower()
            if status_filter:
                nodes = [node for node in nodes if str(node.get("status") or "").strip().lower() == status_filter]
            if hostname_filter:
                nodes = [node for node in nodes if hostname_filter in str(node.get("hostname") or "").strip().lower()]
            limit = self._bounded_limit(arguments.get("limit"))
            nodes = nodes[:limit]
            return {
                "ok": True,
                "count": len(nodes),
                "nodes": nodes,
            }
        if tool_name == "list_models":
            models = list(self._list_models())
            query = str(arguments.get("query") or "").strip().lower()
            custom_only = bool(arguments.get("custom_only", False))
            if query:
                models = [model for model in models if query in str(model.get("name") or "").lower()]
            if custom_only:
                models = [model for model in models if bool(model.get("custom"))]
            limit = self._bounded_limit(arguments.get("limit"))
            models = models[:limit]
            return {
                "ok": True,
                "count": len(models),
                "models": models,
            }
        if tool_name == "get_model_details":
            model_name = str(arguments.get("model_name") or "").strip()
            if not model_name:
                raise ValueError("get_model_details requires a model_name")
            models = list(self._list_models())
            for model in models:
                if str(model.get("name") or "").strip() == model_name:
                    return {"ok": True, "model": model}
            return {"ok": False, "error": f"Model not found: {model_name}"}
        if tool_name == "get_join_command":
            join_command = self._get_join_command()
            return {"ok": True, "join_command": join_command}
        if tool_name == "get_nodes_overview":
            overview = self._get_nodes_overview()
            hosts = list(overview.get("hosts") or [])
            summary = dict(overview.get("summary") or {})
            return {
                "ok": True,
                "summary": summary,
                "hosts": hosts,
            }
        raise ValueError(f"Unsupported tool for parallax plugin: {tool_name}")

    @staticmethod
    def _bounded_limit(value: Any) -> int:
        if value is None:
            return 100
        try:
            parsed = int(value)
        except Exception:
            return 100
        return max(1, min(200, parsed))

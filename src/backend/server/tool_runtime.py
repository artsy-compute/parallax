from __future__ import annotations

import importlib.util
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

from backend.server.settings_store import SettingsStore
from backend.server.tools.registry import ToolRegistry
from backend.server.tools.plugins.files import FileToolsPlugin
from backend.server.tools.plugins.web import WebToolsPlugin
from parallax_utils.file_util import get_project_root
from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class ServerToolPolicy:
    enabled: bool
    max_iterations: int
    tool_enabled: dict[str, bool] = field(default_factory=dict)

    def is_tool_enabled(self, tool_name: str) -> bool:
        return self.enabled and bool(self.tool_enabled.get(str(tool_name or "").strip(), False))


class ServerToolRuntime:
    def __init__(self, settings_store: SettingsStore | None = None):
        self.settings_store = settings_store
        self.default_enabled = self._env_flag("PARALLAX_SERVER_TOOLS_ENABLED", True)
        self.default_allow_web_fetch = self._env_flag("PARALLAX_SERVER_TOOL_ALLOW_WEB_FETCH", True)
        self.default_allow_file_read = self._env_flag("PARALLAX_SERVER_TOOL_ALLOW_FILE_READ", True)
        self.default_max_iterations = self._env_int("PARALLAX_SERVER_TOOL_MAX_ITERATIONS", 3, minimum=1, maximum=8)
        self.default_max_chars = self._env_int("PARALLAX_SERVER_TOOL_MAX_CHARS", 16000, minimum=512, maximum=50000)
        self.fetch_timeout_sec = float(os.environ.get("PARALLAX_SERVER_TOOL_FETCH_TIMEOUT_SEC", "20"))
        self.allowed_file_roots = self._load_allowed_file_roots()
        self.registry = ToolRegistry()
        self._register_builtin_plugins()
        self._load_local_plugins()

    @staticmethod
    def _env_flag(name: str, default: bool) -> bool:
        raw = os.environ.get(name)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except Exception:
            return default
        return max(minimum, min(maximum, value))

    def _load_allowed_file_roots(self) -> list[Path]:
        raw = os.environ.get("PARALLAX_TOOL_FILE_ROOTS", "").strip()
        roots: list[Path] = []
        if raw:
            for item in raw.split(","):
                path = Path(str(item).strip()).expanduser()
                if path.exists():
                    roots.append(path.resolve())
        if not roots:
            roots.append(Path.cwd().resolve())
        return roots

    def _register_builtin_plugins(self) -> None:
        self.registry.register(
            WebToolsPlugin(
                fetch_timeout_sec=self.fetch_timeout_sec,
                default_max_chars=self.default_max_chars,
            )
        )
        self.registry.register(
            FileToolsPlugin(
                allowed_file_roots=self.allowed_file_roots,
                default_max_chars=self.default_max_chars,
            )
        )

    def _load_local_plugins(self) -> None:
        raw_dirs = os.environ.get("PARALLAX_TOOL_PLUGIN_DIRS", "").strip()
        plugin_dirs = [Path(item).expanduser() for item in raw_dirs.split(":") if str(item).strip()] if raw_dirs else []
        default_dir = get_project_root() / "plugins" / "tools"
        plugin_dirs.append(default_dir)
        seen: set[Path] = set()
        for plugin_dir in plugin_dirs:
            resolved_dir = plugin_dir.resolve()
            if resolved_dir in seen or not resolved_dir.exists() or not resolved_dir.is_dir():
                continue
            seen.add(resolved_dir)
            for path in sorted(resolved_dir.glob("*.py")):
                if path.name.startswith("_"):
                    continue
                self._load_plugin_module(path)

    def _load_plugin_module(self, path: Path) -> None:
        module_name = f"parallax_local_tool_plugin_{path.stem}"
        try:
            spec = importlib.util.spec_from_file_location(module_name, str(path))
            if spec is None or spec.loader is None:
                return
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            plugin_obj = None
            if hasattr(module, "get_plugin"):
                plugin_obj = module.get_plugin()
            elif hasattr(module, "PLUGIN"):
                plugin_obj = getattr(module, "PLUGIN")
            if plugin_obj is None:
                return
            if isinstance(plugin_obj, (list, tuple)):
                for plugin in plugin_obj:
                    self.registry.register(plugin)
            else:
                self.registry.register(plugin_obj)
            logger.info("Loaded local tool plugin module %s", path)
        except Exception as e:
            logger.warning("Failed to load local tool plugin %s: %s", path, e, exc_info=True)

    def _default_tool_enabled_map(self) -> dict[str, bool]:
        enabled_map: dict[str, bool] = {}
        for tool in self.registry.list_tools():
            if tool.name == "fetch_url":
                enabled_map[tool.name] = self.default_allow_web_fetch
            elif tool.name in {"read_file", "list_files", "search_files"}:
                enabled_map[tool.name] = self.default_allow_file_read
            else:
                enabled_map[tool.name] = True
        return enabled_map

    def _settings_tool_config(self) -> dict[str, Any]:
        if self.settings_store is None:
            return {}
        cluster_settings = self.settings_store.get_cluster_settings()
        advanced = dict(cluster_settings.get("advanced") or {})
        raw = advanced.get("server_tools")
        return dict(raw) if isinstance(raw, dict) else {}

    def resolve_policy(self, request_data: Dict[str, Any]) -> ServerToolPolicy:
        tool_enabled = self._default_tool_enabled_map()
        settings_config = self._settings_tool_config()
        enabled = bool(settings_config.get("enabled", self.default_enabled))
        max_iterations = self.default_max_iterations
        try:
            max_iterations = int(settings_config.get("max_iterations", self.default_max_iterations))
        except Exception:
            pass
        for tool_name, config in dict(settings_config.get("tools") or {}).items():
            if isinstance(config, dict) and "enabled" in config:
                tool_enabled[str(tool_name).strip()] = bool(config.get("enabled"))

        raw_config = request_data.get("server_tools")
        config = raw_config if isinstance(raw_config, dict) else {}
        enabled = bool(config.get("enabled", enabled))
        try:
            max_iterations = int(config.get("max_iterations", max_iterations))
        except Exception:
            pass
        max_iterations = max(1, min(8, max_iterations))

        if "allow_web_fetch" in config:
            tool_enabled["fetch_url"] = bool(config.get("allow_web_fetch"))
        if "allow_file_read" in config:
            file_flag = bool(config.get("allow_file_read"))
            for tool_name in ("read_file", "list_files", "search_files"):
                if tool_name in tool_enabled:
                    tool_enabled[tool_name] = file_flag
        for tool_name, tool_config in dict(config.get("tools") or {}).items():
            if isinstance(tool_config, dict) and "enabled" in tool_config:
                tool_enabled[str(tool_name).strip()] = bool(tool_config.get("enabled"))

        return ServerToolPolicy(
            enabled=enabled,
            max_iterations=max_iterations,
            tool_enabled=tool_enabled,
        )

    def describe_available_tools(self) -> list[Dict[str, Any]]:
        policy = self.resolve_policy({})
        return [
            {
                **description,
                "enabled_by_default": policy.is_tool_enabled(description["name"]),
            }
            for description in self.registry.describe_tools()
        ]

    def inject_builtin_tools(self, request_data: Dict[str, Any], policy: ServerToolPolicy) -> Dict[str, Any]:
        if not policy.enabled:
            return dict(request_data)

        merged_request = dict(request_data)
        existing_tools = list(merged_request.get("tools") or [])
        existing_names = {
            str((item or {}).get("function", {}).get("name") or "").strip()
            for item in existing_tools
            if isinstance(item, dict)
        }
        for tool in self.registry.list_tools():
            if not policy.is_tool_enabled(tool.name) or tool.name in existing_names:
                continue
            existing_tools.append(tool.schema)
        if existing_tools:
            merged_request["tools"] = existing_tools
        return merged_request

    def can_execute(self, tool_calls: list[Dict[str, Any]], policy: ServerToolPolicy) -> bool:
        if not policy.enabled or not tool_calls:
            return False
        for tool_call in tool_calls:
            function = tool_call.get("function") or {}
            name = str(function.get("name") or "").strip()
            if not self.registry.has_tool(name) or not policy.is_tool_enabled(name):
                return False
        return True

    async def execute_tool_calls(self, tool_calls: list[Dict[str, Any]]) -> list[Dict[str, str]]:
        results: list[Dict[str, str]] = []
        for tool_call in tool_calls:
            function = tool_call.get("function") or {}
            tool_name = str(function.get("name") or "").strip()
            tool_call_id = str(tool_call.get("id") or "").strip()
            arguments = self._parse_arguments(function.get("arguments"))
            payload = await self.registry.execute(tool_name, arguments)
            results.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": json.dumps(payload, ensure_ascii=False),
                }
            )
        return results

    @staticmethod
    def _parse_arguments(raw_arguments: Any) -> Dict[str, Any]:
        if isinstance(raw_arguments, dict):
            return dict(raw_arguments)
        if isinstance(raw_arguments, str):
            stripped = raw_arguments.strip()
            if not stripped:
                return {}
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                return parsed
        raise ValueError("Tool call arguments must be a JSON object")

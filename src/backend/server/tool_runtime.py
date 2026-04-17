from __future__ import annotations

import json
import os
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp

from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs):
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if self._skip_depth == 0 and tag in {"p", "div", "section", "article", "li", "br", "tr", "h1", "h2", "h3", "h4"}:
            self._parts.append("\n")

    def handle_endtag(self, tag: str):
        if tag in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if self._skip_depth == 0 and tag in {"p", "div", "section", "article", "li", "br", "tr", "h1", "h2", "h3", "h4"}:
            self._parts.append("\n")

    def handle_data(self, data: str):
        if self._skip_depth == 0 and data:
            self._parts.append(data)

    def get_text(self) -> str:
        text = unescape("".join(self._parts))
        lines = [" ".join(line.split()) for line in text.splitlines()]
        return "\n".join(line for line in lines if line)


@dataclass(frozen=True)
class ServerToolPolicy:
    enabled: bool
    allow_web_fetch: bool
    allow_file_read: bool
    max_iterations: int


class ServerToolRuntime:
    FETCH_URL_TOOL = {
        "type": "function",
        "function": {
            "name": "fetch_url",
            "description": "Fetch a web page or text resource over HTTP/HTTPS and return compact text content.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "Absolute HTTP or HTTPS URL to fetch."},
                    "max_chars": {
                        "type": "integer",
                        "description": "Optional maximum characters to return.",
                        "minimum": 256,
                        "maximum": 50000,
                    },
                },
                "required": ["url"],
                "additionalProperties": False,
            },
        },
    }

    READ_FILE_TOOL = {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a local text file from approved server paths and return its contents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute or relative file path."},
                    "start_line": {"type": "integer", "minimum": 1},
                    "end_line": {"type": "integer", "minimum": 1},
                    "max_chars": {
                        "type": "integer",
                        "description": "Optional maximum characters to return.",
                        "minimum": 256,
                        "maximum": 50000,
                    },
                },
                "required": ["path"],
                "additionalProperties": False,
            },
        },
    }

    def __init__(self):
        self.default_enabled = self._env_flag("PARALLAX_SERVER_TOOLS_ENABLED", True)
        self.default_allow_web_fetch = self._env_flag("PARALLAX_SERVER_TOOL_ALLOW_WEB_FETCH", True)
        self.default_allow_file_read = self._env_flag("PARALLAX_SERVER_TOOL_ALLOW_FILE_READ", True)
        self.default_max_iterations = self._env_int("PARALLAX_SERVER_TOOL_MAX_ITERATIONS", 3, minimum=1, maximum=8)
        self.default_max_chars = self._env_int("PARALLAX_SERVER_TOOL_MAX_CHARS", 16000, minimum=512, maximum=50000)
        self.fetch_timeout_sec = float(os.environ.get("PARALLAX_SERVER_TOOL_FETCH_TIMEOUT_SEC", "20"))
        self.allowed_file_roots = self._load_allowed_file_roots()
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

    def resolve_policy(self, request_data: Dict[str, Any]) -> ServerToolPolicy:
        raw_config = request_data.get("server_tools")
        config = raw_config if isinstance(raw_config, dict) else {}
        enabled = bool(config.get("enabled", self.default_enabled))
        allow_web_fetch = bool(config.get("allow_web_fetch", self.default_allow_web_fetch))
        allow_file_read = bool(config.get("allow_file_read", self.default_allow_file_read))
        max_iterations = self.default_max_iterations
        try:
            max_iterations = int(config.get("max_iterations", self.default_max_iterations))
        except Exception:
            pass
        max_iterations = max(1, min(8, max_iterations))
        return ServerToolPolicy(
            enabled=enabled,
            allow_web_fetch=allow_web_fetch,
            allow_file_read=allow_file_read,
            max_iterations=max_iterations,
        )

    def describe_available_tools(self) -> list[Dict[str, Any]]:
        return [
            {
                "name": "fetch_url",
                "description": self.FETCH_URL_TOOL["function"]["description"],
                "enabled_by_default": bool(self.default_enabled and self.default_allow_web_fetch),
                "kind": "web",
            },
            {
                "name": "read_file",
                "description": self.READ_FILE_TOOL["function"]["description"],
                "enabled_by_default": bool(self.default_enabled and self.default_allow_file_read),
                "kind": "local_file",
                "allowed_roots": [str(root) for root in self.allowed_file_roots],
            },
        ]

    def inject_builtin_tools(
        self, request_data: Dict[str, Any], policy: ServerToolPolicy
    ) -> Dict[str, Any]:
        if not policy.enabled:
            return dict(request_data)

        merged_request = dict(request_data)
        existing_tools = list(merged_request.get("tools") or [])
        existing_names = {
            str((item or {}).get("function", {}).get("name") or "").strip()
            for item in existing_tools
            if isinstance(item, dict)
        }

        if policy.allow_web_fetch and "fetch_url" not in existing_names:
            existing_tools.append(self.FETCH_URL_TOOL)
        if policy.allow_file_read and "read_file" not in existing_names:
            existing_tools.append(self.READ_FILE_TOOL)

        if existing_tools:
            merged_request["tools"] = existing_tools
        return merged_request

    def can_execute(self, tool_calls: List[Dict[str, Any]], policy: ServerToolPolicy) -> bool:
        if not policy.enabled or not tool_calls:
            return False
        for tool_call in tool_calls:
            function = tool_call.get("function") or {}
            name = str(function.get("name") or "").strip()
            if name == "fetch_url" and policy.allow_web_fetch:
                continue
            if name == "read_file" and policy.allow_file_read:
                continue
            return False
        return True

    async def execute_tool_calls(self, tool_calls: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        results: list[Dict[str, str]] = []
        for tool_call in tool_calls:
            function = tool_call.get("function") or {}
            tool_name = str(function.get("name") or "").strip()
            tool_call_id = str(tool_call.get("id") or "").strip()
            raw_arguments = function.get("arguments")
            arguments = self._parse_arguments(raw_arguments)
            if tool_name == "fetch_url":
                payload = await self._fetch_url(arguments)
            elif tool_name == "read_file":
                payload = self._read_file(arguments)
            else:
                raise ValueError(f"Unsupported server tool: {tool_name}")
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

    async def _fetch_url(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        url = str(arguments.get("url") or "").strip()
        if not url:
            raise ValueError("fetch_url requires a non-empty url")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("fetch_url only supports http and https URLs")

        max_chars = self._bounded_max_chars(arguments.get("max_chars"))
        timeout = aiohttp.ClientTimeout(total=self.fetch_timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, allow_redirects=True) as response:
                response.raise_for_status()
                content_type = str(response.headers.get("Content-Type") or "").lower()
                raw_text = await response.text(errors="ignore")

        extracted = self._extract_response_text(raw_text, content_type)
        return {
            "ok": True,
            "url": url,
            "content_type": content_type,
            "content": self._truncate_text(extracted, max_chars),
        }

    def _read_file(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        path_value = str(arguments.get("path") or "").strip()
        if not path_value:
            raise ValueError("read_file requires a non-empty path")

        resolved_path = self._resolve_allowed_file(path_value)
        if not resolved_path.exists():
            raise FileNotFoundError(f"File does not exist: {path_value}")
        if not resolved_path.is_file():
            raise ValueError(f"Path is not a file: {path_value}")

        start_line = self._optional_positive_int(arguments.get("start_line"))
        end_line = self._optional_positive_int(arguments.get("end_line"))
        max_chars = self._bounded_max_chars(arguments.get("max_chars"))

        content = resolved_path.read_text(encoding="utf-8", errors="ignore")
        lines = content.splitlines()
        if start_line is not None or end_line is not None:
            start_index = max(0, (start_line or 1) - 1)
            end_index = end_line if end_line is not None else len(lines)
            if end_index < start_index + 1:
                raise ValueError("end_line must be greater than or equal to start_line")
            selected = lines[start_index:end_index]
            content = "\n".join(selected)

        return {
            "ok": True,
            "path": str(resolved_path),
            "content": self._truncate_text(content, max_chars),
        }

    def _resolve_allowed_file(self, path_value: str) -> Path:
        candidate = Path(path_value).expanduser()
        if not candidate.is_absolute():
            candidate = (Path.cwd() / candidate).resolve()
        else:
            candidate = candidate.resolve()

        for root in self.allowed_file_roots:
            try:
                candidate.relative_to(root)
                return candidate
            except ValueError:
                continue
        allowed = ", ".join(str(root) for root in self.allowed_file_roots)
        raise ValueError(f"Path is outside allowed roots: {allowed}")

    def _bounded_max_chars(self, value: Any) -> int:
        if value is None:
            return self.default_max_chars
        try:
            parsed = int(value)
        except Exception:
            return self.default_max_chars
        return max(256, min(50000, parsed))

    @staticmethod
    def _optional_positive_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        parsed = int(value)
        if parsed < 1:
            raise ValueError("Line numbers must be positive integers")
        return parsed

    @staticmethod
    def _truncate_text(text: str, max_chars: int) -> str:
        compact = text.strip()
        if len(compact) <= max_chars:
            return compact
        return compact[: max_chars - 32].rstrip() + "\n...[truncated]"

    @staticmethod
    def _extract_response_text(raw_text: str, content_type: str) -> str:
        if "html" in content_type:
            parser = _HTMLTextExtractor()
            parser.feed(raw_text)
            parser.close()
            text = parser.get_text()
            if text:
                return text
        return raw_text.strip()

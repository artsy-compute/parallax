from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

import aiohttp

from backend.server.tools.base import ToolDefinition


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


class WebToolsPlugin:
    name = "web"

    def __init__(self, *, fetch_timeout_sec: float, default_max_chars: int):
        self.fetch_timeout_sec = fetch_timeout_sec
        self.default_max_chars = default_max_chars

    def get_tools(self) -> list[ToolDefinition]:
        return [
            ToolDefinition(
                name="fetch_url",
                plugin_name=self.name,
                kind="web",
                description="Fetch a web page or text resource over HTTP/HTTPS and return compact text content.",
                schema={
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
                },
            ),
            ToolDefinition(
                name="fetch_json",
                plugin_name=self.name,
                kind="web",
                description="Fetch a JSON resource over HTTP/HTTPS and return the parsed response body.",
                schema={
                    "type": "function",
                    "function": {
                        "name": "fetch_json",
                        "description": "Fetch a JSON resource over HTTP/HTTPS and return the parsed response body.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "url": {"type": "string", "description": "Absolute HTTP or HTTPS URL to fetch."},
                            },
                            "required": ["url"],
                            "additionalProperties": False,
                        },
                    },
                },
            ),
        ]

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool_name == "fetch_url":
            return await self._fetch_url(arguments)
        if tool_name == "fetch_json":
            return await self._fetch_json(arguments)
        raise ValueError(f"Unsupported tool for web plugin: {tool_name}")

    async def _fetch_url(self, arguments: dict[str, Any]) -> dict[str, Any]:
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

    async def _fetch_json(self, arguments: dict[str, Any]) -> dict[str, Any]:
        url = str(arguments.get("url") or "").strip()
        if not url:
            raise ValueError("fetch_json requires a non-empty url")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("fetch_json only supports http and https URLs")

        timeout = aiohttp.ClientTimeout(total=self.fetch_timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, allow_redirects=True) as response:
                response.raise_for_status()
                content_type = str(response.headers.get("Content-Type") or "").lower()
                payload = await response.json(content_type=None)
        return {
            "ok": True,
            "url": url,
            "content_type": content_type,
            "json": payload,
        }

    def _bounded_max_chars(self, value: Any) -> int:
        if value is None:
            return self.default_max_chars
        try:
            parsed = int(value)
        except Exception:
            return self.default_max_chars
        return max(256, min(50000, parsed))

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

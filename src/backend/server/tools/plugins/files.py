from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import Any

from backend.server.tools.base import ToolDefinition


class FileToolsPlugin:
    name = "files"

    def __init__(self, *, allowed_file_roots: list[Path], default_max_chars: int):
        self.allowed_file_roots = allowed_file_roots
        self.default_max_chars = default_max_chars

    def get_tools(self) -> list[ToolDefinition]:
        allowed_roots = [str(root) for root in self.allowed_file_roots]
        return [
            ToolDefinition(
                name="read_file",
                plugin_name=self.name,
                kind="local_file",
                description="Read a local text file from approved server paths and return its contents.",
                metadata={"allowed_roots": allowed_roots},
                schema={
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
                                "max_chars": {"type": "integer", "minimum": 256, "maximum": 50000},
                            },
                            "required": ["path"],
                            "additionalProperties": False,
                        },
                    },
                },
            ),
            ToolDefinition(
                name="list_files",
                plugin_name=self.name,
                kind="local_file",
                description="List files and directories under an approved server path.",
                metadata={"allowed_roots": allowed_roots},
                schema={
                    "type": "function",
                    "function": {
                        "name": "list_files",
                        "description": "List files and directories under an approved server path.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "path": {"type": "string", "description": "Directory path to list."},
                                "recursive": {"type": "boolean"},
                                "max_entries": {"type": "integer", "minimum": 1, "maximum": 1000},
                            },
                            "required": ["path"],
                            "additionalProperties": False,
                        },
                    },
                },
            ),
            ToolDefinition(
                name="search_files",
                plugin_name=self.name,
                kind="local_file",
                description="Search text files under an approved server path and return matching lines.",
                metadata={"allowed_roots": allowed_roots},
                schema={
                    "type": "function",
                    "function": {
                        "name": "search_files",
                        "description": "Search text files under an approved server path and return matching lines.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "path": {"type": "string", "description": "Directory or file path to search."},
                                "pattern": {"type": "string", "description": "Substring to search for."},
                                "glob": {"type": "string", "description": "Optional filename glob filter."},
                                "max_matches": {"type": "integer", "minimum": 1, "maximum": 500},
                            },
                            "required": ["path", "pattern"],
                            "additionalProperties": False,
                        },
                    },
                },
            ),
        ]

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool_name == "read_file":
            return self._read_file(arguments)
        if tool_name == "list_files":
            return self._list_files(arguments)
        if tool_name == "search_files":
            return self._search_files(arguments)
        raise ValueError(f"Unsupported tool for file plugin: {tool_name}")

    def _read_file(self, arguments: dict[str, Any]) -> dict[str, Any]:
        path_value = str(arguments.get("path") or "").strip()
        if not path_value:
            raise ValueError("read_file requires a non-empty path")
        resolved_path = self._resolve_allowed_path(path_value)
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
            content = "\n".join(lines[start_index:end_index])
        return {
            "ok": True,
            "path": str(resolved_path),
            "content": self._truncate_text(content, max_chars),
        }

    def _list_files(self, arguments: dict[str, Any]) -> dict[str, Any]:
        path_value = str(arguments.get("path") or "").strip()
        if not path_value:
            raise ValueError("list_files requires a non-empty path")
        resolved_path = self._resolve_allowed_path(path_value)
        if not resolved_path.exists():
            raise FileNotFoundError(f"Path does not exist: {path_value}")
        if not resolved_path.is_dir():
            raise ValueError(f"Path is not a directory: {path_value}")

        recursive = bool(arguments.get("recursive", False))
        max_entries = self._bounded_entries(arguments.get("max_entries"))
        iterator = resolved_path.rglob("*") if recursive else resolved_path.iterdir()
        entries: list[dict[str, Any]] = []
        for index, child in enumerate(sorted(iterator), start=1):
            if index > max_entries:
                break
            entries.append(
                {
                    "path": str(child),
                    "name": child.name,
                    "type": "directory" if child.is_dir() else "file",
                }
            )
        return {
            "ok": True,
            "path": str(resolved_path),
            "entries": entries,
            "truncated": len(entries) >= max_entries,
        }

    def _search_files(self, arguments: dict[str, Any]) -> dict[str, Any]:
        path_value = str(arguments.get("path") or "").strip()
        pattern = str(arguments.get("pattern") or "").strip()
        if not path_value:
            raise ValueError("search_files requires a non-empty path")
        if not pattern:
            raise ValueError("search_files requires a non-empty pattern")
        glob_pattern = str(arguments.get("glob") or "*").strip() or "*"
        max_matches = self._bounded_matches(arguments.get("max_matches"))

        resolved_path = self._resolve_allowed_path(path_value)
        if not resolved_path.exists():
            raise FileNotFoundError(f"Path does not exist: {path_value}")

        candidates = [resolved_path] if resolved_path.is_file() else [p for p in resolved_path.rglob("*") if p.is_file()]
        matches: list[dict[str, Any]] = []
        lowered_pattern = pattern.lower()
        for candidate in sorted(candidates):
            if not fnmatch.fnmatch(candidate.name, glob_pattern):
                continue
            try:
                lines = candidate.read_text(encoding="utf-8", errors="ignore").splitlines()
            except Exception:
                continue
            for line_number, line in enumerate(lines, start=1):
                if lowered_pattern in line.lower():
                    matches.append(
                        {
                            "path": str(candidate),
                            "line_number": line_number,
                            "line": line.strip(),
                        }
                    )
                    if len(matches) >= max_matches:
                        return {
                            "ok": True,
                            "path": str(resolved_path),
                            "pattern": pattern,
                            "matches": matches,
                            "truncated": True,
                        }
        return {
            "ok": True,
            "path": str(resolved_path),
            "pattern": pattern,
            "matches": matches,
            "truncated": False,
        }

    def _resolve_allowed_path(self, path_value: str) -> Path:
        candidate = Path(path_value).expanduser()
        candidate = ((Path.cwd() / candidate).resolve() if not candidate.is_absolute() else candidate.resolve())
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
    def _bounded_entries(value: Any) -> int:
        if value is None:
            return 200
        try:
            parsed = int(value)
        except Exception:
            return 200
        return max(1, min(1000, parsed))

    @staticmethod
    def _bounded_matches(value: Any) -> int:
        if value is None:
            return 100
        try:
            parsed = int(value)
        except Exception:
            return 100
        return max(1, min(500, parsed))

    @staticmethod
    def _optional_positive_int(value: Any) -> int | None:
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

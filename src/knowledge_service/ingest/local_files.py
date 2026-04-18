from __future__ import annotations

import hashlib
import mimetypes
from dataclasses import dataclass
from pathlib import Path


TEXT_EXTENSIONS = {
    ".c",
    ".cc",
    ".cpp",
    ".css",
    ".go",
    ".h",
    ".hpp",
    ".html",
    ".java",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".py",
    ".rb",
    ".rs",
    ".rst",
    ".sh",
    ".sql",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}

MAX_TEXT_FILE_BYTES = 2_000_000

SKIP_DIR_NAMES = {
    ".cache",
    ".git",
    ".hg",
    ".idea",
    ".next",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "venv",
}


@dataclass(frozen=True)
class ExtractedDocument:
    document_uri: str
    title: str
    mime_type: str
    sha256: str
    byte_size: int
    text: str


def resolve_workspace_path(workspace_root: Path, raw_path: str) -> Path:
    workspace_root = workspace_root.expanduser().resolve()
    candidate = Path(str(raw_path or "").strip()).expanduser()
    if not candidate.is_absolute():
        candidate = (workspace_root / candidate).resolve()
    else:
        candidate = candidate.resolve()
    if workspace_root not in candidate.parents and candidate != workspace_root:
        raise ValueError(f"Path is outside the workspace root: {candidate}")
    return candidate


def _looks_text_like(path: Path, data: bytes) -> bool:
    if path.suffix.lower() in TEXT_EXTENSIONS:
        return True
    if b"\x00" in data[:1024]:
        return False
    return True


def _read_text_file(path: Path) -> ExtractedDocument | None:
    data = path.read_bytes()
    if len(data) > MAX_TEXT_FILE_BYTES:
        return None
    if not _looks_text_like(path, data):
        return None
    text = data.decode("utf-8", errors="ignore").strip()
    if not text:
        return None
    mime_type = mimetypes.guess_type(str(path))[0] or "text/plain"
    return ExtractedDocument(
        document_uri=str(path),
        title=path.name,
        mime_type=mime_type,
        sha256=hashlib.sha256(data).hexdigest(),
        byte_size=len(data),
        text=text,
    )


def extract_local_documents(workspace_root: Path, raw_path: str) -> tuple[Path, list[ExtractedDocument]]:
    resolved_path = resolve_workspace_path(workspace_root, raw_path)
    documents: list[ExtractedDocument] = []
    if resolved_path.is_file():
        extracted = _read_text_file(resolved_path)
        if extracted is not None:
            documents.append(extracted)
        return resolved_path, documents

    if not resolved_path.is_dir():
        raise ValueError(f"Path does not exist: {resolved_path}")

    for path in sorted(resolved_path.rglob("*")):
        if not path.is_file():
            continue
        if any(part in SKIP_DIR_NAMES for part in path.parts):
            continue
        extracted = _read_text_file(path)
        if extracted is not None:
            documents.append(extracted)
    return resolved_path, documents

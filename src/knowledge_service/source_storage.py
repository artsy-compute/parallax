from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

from knowledge_service.ingest.local_files import (
    STRUCTURED_DOCUMENT_EXTENSIONS,
    TEXT_EXTENSIONS,
    ExtractedDocument,
    extract_uploaded_document,
    resolve_structured_document_suffix,
)

if TYPE_CHECKING:
    from knowledge_service.store import WorkspaceContext

_HIDDEN_METADATA_FILENAME = ".parallax-source.json"
_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(value: str, fallback: str = "item") -> str:
    normalized = _SLUG_RE.sub("-", str(value or "").strip().lower()).strip("-")
    return normalized or fallback


def _guess_mime_type(path: Path) -> str:
    return str(mimetypes.guess_type(str(path))[0] or "").strip()


class LocalSourceStorage:
    backend_name = "local"

    def root_dir(self, context: WorkspaceContext) -> Path:
        return context.library_dir

    def resolve_path(self, context: WorkspaceContext, raw_path: str | None = None) -> Path:
        root = self.root_dir(context)
        normalized = str(raw_path or "").strip().replace("\\", "/").strip("/")
        if not normalized:
            return root
        candidate = (root / normalized).resolve()
        if candidate != root and root not in candidate.parents:
            raise ValueError(f"Library path is outside the source library root: {raw_path}")
        return candidate

    def relative_path(self, context: WorkspaceContext, path: Path) -> str:
        root = self.root_dir(context)
        resolved = path.resolve()
        if resolved == root:
            return ""
        return str(resolved.relative_to(root)).replace(os.sep, "/")

    def _is_visible(self, path: Path) -> bool:
        return not path.name.startswith(".")

    def _supports_preview(self, path: Path) -> bool:
        if path.is_dir():
            return False
        mime_type = _guess_mime_type(path)
        return bool(
            path.suffix.lower() in TEXT_EXTENSIONS
            or path.suffix.lower() in STRUCTURED_DOCUMENT_EXTENSIONS
            or resolve_structured_document_suffix(path, mime_type)
        )

    def _supports_ingest(self, path: Path) -> bool:
        return path.is_dir() or self._supports_preview(path)

    def _entry_summary(self, context: WorkspaceContext, path: Path) -> dict[str, Any]:
        stats = path.stat()
        kind = "directory" if path.is_dir() else "file"
        child_count = 0
        if path.is_dir():
            try:
                child_count = sum(1 for item in path.iterdir() if self._is_visible(item))
            except Exception:
                child_count = 0
        mime_type = "" if path.is_dir() else _guess_mime_type(path)
        return {
            "path": self.relative_path(context, path),
            "name": path.name,
            "kind": kind,
            "size": int(stats.st_size or 0) if path.is_file() else None,
            "modified_at": float(stats.st_mtime or 0),
            "mime_type": mime_type,
            "extension": path.suffix.lower(),
            "child_count": child_count if path.is_dir() else None,
            "preview_supported": self._supports_preview(path),
            "ingest_supported": self._supports_ingest(path),
        }

    def _tree_node(self, context: WorkspaceContext, path: Path) -> dict[str, Any]:
        children = [
            self._tree_node(context, child)
            for child in sorted(
                [item for item in path.iterdir() if item.is_dir() and self._is_visible(item)],
                key=lambda item: item.name.lower(),
            )
        ]
        return {
            "id": self.relative_path(context, path) or "__root__",
            "path": self.relative_path(context, path),
            "name": path.name if path != self.root_dir(context) else "Library",
            "children": children,
        }

    def list_directory(self, context: WorkspaceContext, raw_path: str | None = None) -> dict[str, Any]:
        current_path = self.resolve_path(context, raw_path)
        if not current_path.exists():
            raise ValueError(f"Library path does not exist: {raw_path}")
        if not current_path.is_dir():
            raise ValueError(f"Library path is not a directory: {raw_path}")
        items = [
            self._entry_summary(context, item)
            for item in sorted(
                [item for item in current_path.iterdir() if self._is_visible(item)],
                key=lambda item: (item.is_file(), item.name.lower()),
            )
        ]
        parent_path = None
        if current_path != self.root_dir(context):
            parent_path = self.relative_path(context, current_path.parent)
        return {
            "storage_backend": self.backend_name,
            "root_path": "",
            "current_path": self.relative_path(context, current_path),
            "parent_path": parent_path,
            "imports_path": "_imports/url",
            "items": items,
            "tree": self._tree_node(context, self.root_dir(context)),
        }

    def _load_metadata(self, path: Path) -> dict[str, Any] | None:
        metadata_path = path / _HIDDEN_METADATA_FILENAME
        if not metadata_path.exists():
            return None
        try:
            return dict(json.loads(metadata_path.read_text(encoding="utf-8")))
        except Exception:
            return None

    def read_file_preview(
        self,
        context: WorkspaceContext,
        raw_path: str,
        *,
        max_chars: int = 12000,
    ) -> dict[str, Any]:
        file_path = self.resolve_path(context, raw_path)
        if not file_path.exists():
            raise ValueError(f"Library file does not exist: {raw_path}")
        if not file_path.is_file():
            raise ValueError(f"Library path is not a file: {raw_path}")
        raw_bytes = file_path.read_bytes()
        extracted = extract_uploaded_document(
            file_path.name,
            raw_bytes,
            _guess_mime_type(file_path) or None,
        )
        preview_text = ""
        preview_truncated = False
        if extracted is not None:
            preview_text = str(extracted.text or "")
            if len(preview_text) > max_chars:
                preview_text = preview_text[:max_chars].rstrip() + "…"
                preview_truncated = True
        metadata = self._load_metadata(file_path.parent)
        return {
            "item": self._entry_summary(context, file_path),
            "preview_text": preview_text,
            "preview_truncated": preview_truncated,
            "metadata": metadata,
        }

    def _ensure_unique_path(self, path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem or "file"
        suffix = path.suffix
        parent = path.parent
        for index in range(2, 1000):
            candidate = parent / f"{stem}-{index}{suffix}"
            if not candidate.exists():
                return candidate
        timestamp = int(time.time())
        return parent / f"{stem}-{timestamp}{suffix}"

    def write_uploaded_file(
        self,
        context: WorkspaceContext,
        *,
        directory: str | None,
        filename: str,
        data: bytes,
    ) -> dict[str, Any]:
        target_directory = self.resolve_path(context, directory)
        if not target_directory.exists():
            target_directory.mkdir(parents=True, exist_ok=True)
        if not target_directory.is_dir():
            raise ValueError(f"Upload target is not a directory: {directory}")
        normalized_name = Path(str(filename or "").strip() or "uploaded-file").name
        destination = self._ensure_unique_path(target_directory / normalized_name)
        destination.write_bytes(data)
        return {
            "item": self._entry_summary(context, destination),
            "directory": self.relative_path(context, target_directory),
        }

    def _cleanup_metadata_import_dir(self, context: WorkspaceContext, start_directory: Path) -> None:
        root = self.root_dir(context)
        current = start_directory.resolve()
        while current != root and root in current.parents:
            metadata_path = current / _HIDDEN_METADATA_FILENAME
            visible_entries = [item for item in current.iterdir() if self._is_visible(item)]
            if visible_entries:
                break
            if metadata_path.exists():
                metadata_path.unlink(missing_ok=True)
            try:
                next(current.iterdir())
                break
            except StopIteration:
                current.rmdir()
                current = current.parent
                continue
            break

    def delete_file(
        self,
        context: WorkspaceContext,
        raw_path: str,
    ) -> dict[str, Any]:
        file_path = self.resolve_path(context, raw_path)
        if not file_path.exists():
            raise ValueError(f"Library file does not exist: {raw_path}")
        if not file_path.is_file():
            raise ValueError(f"Library path is not a file: {raw_path}")
        deleted_path = self.relative_path(context, file_path)
        parent_path = file_path.parent
        file_path.unlink()
        self._cleanup_metadata_import_dir(context, parent_path)
        current_path = ""
        if parent_path.exists():
            current_path = self.relative_path(context, parent_path)
        elif parent_path.parent.exists() and parent_path.parent != self.root_dir(context).parent:
            current_path = self.relative_path(context, parent_path.parent)
        return {
            "deleted_path": deleted_path,
            "current_path": current_path,
        }

    def _url_import_directory(self, context: WorkspaceContext, url: str) -> Path:
        parsed = urlparse(str(url or "").strip())
        host = _slugify(parsed.netloc or "unknown-host", fallback="unknown-host")
        path_name = Path(unquote(parsed.path or "").strip("/"))
        stem = _slugify(path_name.stem or path_name.name or "source", fallback="source")
        digest = hashlib.sha256(str(url).encode("utf-8")).hexdigest()[:10]
        base = self.root_dir(context) / "_imports" / "url" / host / f"{stem}--{digest}"
        return self._ensure_unique_path(base)

    def store_url_import(
        self,
        context: WorkspaceContext,
        *,
        url: str,
        raw_bytes: bytes,
        content_type: str,
        filename_hint: str,
        extracted_text: str | None = None,
        title: str | None = None,
    ) -> dict[str, Any]:
        target_directory = self._url_import_directory(context, url)
        target_directory.mkdir(parents=True, exist_ok=True)
        metadata: dict[str, Any] = {
            "source_type": "url_import",
            "url": str(url),
            "content_type": str(content_type or ""),
            "imported_at": self._entry_summary(context, target_directory)["modified_at"],
        }
        if extracted_text is not None:
            stored_name = "source.txt"
            stored_path = target_directory / stored_name
            stored_path.write_text(str(extracted_text or "").strip(), encoding="utf-8")
            metadata.update(
                {
                    "stored_kind": "text",
                    "stored_file": stored_name,
                    "title": str(title or "").strip(),
                }
            )
        else:
            normalized_name = Path(str(filename_hint or "").strip() or "downloaded-file").name
            normalized_path = Path(normalized_name)
            normalized_suffix = resolve_structured_document_suffix(normalized_path, content_type or None)
            if normalized_suffix and normalized_path.suffix.lower() != normalized_suffix:
                normalized_path = normalized_path.with_suffix(normalized_suffix)
            if not normalized_path.suffix and content_type == "application/pdf":
                normalized_path = normalized_path.with_suffix(".pdf")
            stored_path = target_directory / (normalized_path.name or "downloaded-file")
            stored_path.write_bytes(raw_bytes)
            metadata.update(
                {
                    "stored_kind": "binary",
                    "stored_file": stored_path.name,
                }
            )
        (target_directory / _HIDDEN_METADATA_FILENAME).write_text(
            json.dumps(metadata, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        return {
            "directory": self._entry_summary(context, target_directory),
            "stored_file": self._entry_summary(context, stored_path),
            "metadata": metadata,
            "current_path": self.relative_path(context, target_directory),
        }

    def collect_documents_for_ingest(
        self,
        context: WorkspaceContext,
        raw_path: str,
    ) -> tuple[Path, list[ExtractedDocument]]:
        resolved_path = self.resolve_path(context, raw_path)
        documents: list[ExtractedDocument] = []
        paths: list[Path]
        if resolved_path.is_file():
            paths = [resolved_path]
        elif resolved_path.is_dir():
            paths = [
                item
                for item in sorted(resolved_path.rglob("*"), key=lambda item: str(item).lower())
                if item.is_file()
            ]
        else:
            raise ValueError(f"Library path does not exist: {raw_path}")

        for path in paths:
            if not self._is_visible(path):
                continue
            raw_bytes = path.read_bytes()
            mime_type = _guess_mime_type(path) or None
            extracted = extract_uploaded_document(path.name, raw_bytes, mime_type)
            if extracted is None:
                continue
            relative_path = self.relative_path(context, path)
            documents.append(
                ExtractedDocument(
                    document_uri=f"library://{relative_path}",
                    title=extracted.title,
                    mime_type=extracted.mime_type,
                    sha256=extracted.sha256,
                    byte_size=extracted.byte_size,
                    text=extracted.text,
                )
            )
        return resolved_path, documents

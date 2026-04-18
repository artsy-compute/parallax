from __future__ import annotations

import hashlib
import io
import mimetypes
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET


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
MAX_BINARY_DOCUMENT_BYTES = 25_000_000
STRUCTURED_DOCUMENT_EXTENSIONS = {
    ".docx",
    ".odt",
    ".ods",
    ".odp",
    ".pdf",
}

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

_DOCX_NAMESPACE = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
_ODF_TEXT_TAGS = {
    "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}p",
    "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}h",
}
_WHITESPACE_RE = re.compile(r"\s+")


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
    if path.suffix.lower() in STRUCTURED_DOCUMENT_EXTENSIONS:
        return True
    if path.suffix.lower() in TEXT_EXTENSIONS:
        return True
    if b"\x00" in data[:1024]:
        return False
    return True


def _normalize_text(value: str) -> str:
    lines = [_WHITESPACE_RE.sub(" ", line).strip() for line in str(value or "").splitlines()]
    return "\n\n".join(line for line in lines if line).strip()


def _extract_docx_text(data: bytes) -> str:
    paragraph_text: list[str] = []
    part_names = (
        "word/document.xml",
        "word/footnotes.xml",
        "word/endnotes.xml",
        "word/comments.xml",
    )
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        header_footer_names = sorted(
            name
            for name in archive.namelist()
            if name.startswith("word/header") or name.startswith("word/footer")
        )
        for part_name in [*part_names, *header_footer_names]:
            if part_name not in archive.namelist():
                continue
            root = ET.fromstring(archive.read(part_name))
            for paragraph in root.findall(".//w:p", _DOCX_NAMESPACE):
                text = "".join(node.text or "" for node in paragraph.findall(".//w:t", _DOCX_NAMESPACE)).strip()
                if text:
                    paragraph_text.append(text)
    return _normalize_text("\n\n".join(paragraph_text))


def _extract_odf_text(data: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if "content.xml" not in archive.namelist():
            return ""
        root = ET.fromstring(archive.read("content.xml"))
        blocks: list[str] = []
        for element in root.iter():
            if element.tag not in _ODF_TEXT_TAGS:
                continue
            text = "".join(element.itertext()).strip()
            if text:
                blocks.append(text)
        if blocks:
            return _normalize_text("\n\n".join(blocks))
        return _normalize_text(" ".join(part.strip() for part in root.itertext() if part.strip()))


def _extract_pdf_text(data: bytes) -> str:
    try:
        from pypdf import PdfReader
    except Exception as error:  # pragma: no cover - dependency availability
        raise ValueError("PDF ingest requires the `pypdf` package") from error
    reader = PdfReader(io.BytesIO(data))
    parts = [page.extract_text() or "" for page in reader.pages]
    return _normalize_text("\n\n".join(parts))


def _extract_structured_document_text(path: Path, data: bytes) -> str:
    suffix = path.suffix.lower()
    if len(data) > MAX_BINARY_DOCUMENT_BYTES:
        return ""
    if suffix == ".docx":
        return _extract_docx_text(data)
    if suffix in {".odt", ".ods", ".odp"}:
        return _extract_odf_text(data)
    if suffix == ".pdf":
        return _extract_pdf_text(data)
    return ""


def _extract_document_from_bytes(
    *,
    path: Path,
    data: bytes,
    mime_type: str | None = None,
    document_uri: str | None = None,
    title: str | None = None,
) -> ExtractedDocument | None:
    suffix = path.suffix.lower()
    if suffix in STRUCTURED_DOCUMENT_EXTENSIONS:
        text = _extract_structured_document_text(path, data)
    else:
        if len(data) > MAX_TEXT_FILE_BYTES:
            return None
        if not _looks_text_like(path, data):
            return None
        text = data.decode("utf-8", errors="ignore")
    normalized_text = _normalize_text(text)
    if not normalized_text:
        return None
    resolved_mime_type = mime_type or mimetypes.guess_type(str(path))[0] or "text/plain"
    return ExtractedDocument(
        document_uri=document_uri or str(path),
        title=title or path.name,
        mime_type=resolved_mime_type,
        sha256=hashlib.sha256(data).hexdigest(),
        byte_size=len(data),
        text=normalized_text,
    )


def _read_text_file(path: Path) -> ExtractedDocument | None:
    data = path.read_bytes()
    return _extract_document_from_bytes(path=path, data=data)


def extract_uploaded_document(filename: str, data: bytes, content_type: str | None = None) -> ExtractedDocument | None:
    normalized_name = Path(str(filename or "").strip() or "uploaded-document")
    return _extract_document_from_bytes(
        path=normalized_name,
        data=data,
        mime_type=str(content_type or "").strip() or None,
        document_uri=f"upload://{normalized_name.name}",
        title=normalized_name.name,
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

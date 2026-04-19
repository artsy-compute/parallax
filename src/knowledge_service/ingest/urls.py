from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from urllib.parse import urlparse

import httpx

from knowledge_service.ingest.local_files import (
    MAX_BINARY_DOCUMENT_BYTES,
    ExtractedDocument,
    extract_uploaded_document,
    resolve_structured_document_suffix,
)

_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>[\s\S]*?</\1>", re.IGNORECASE)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class FetchedUrlContent:
    url: str
    raw_bytes: bytes
    content_type: str
    encoding: str | None
    filename_hint: str


def _html_to_text(content: str) -> tuple[str, str]:
    title_match = _TITLE_RE.search(content or "")
    title = " ".join(unescape(title_match.group(1)).split()).strip() if title_match else ""
    stripped = _SCRIPT_STYLE_RE.sub(" ", content or "")
    stripped = _TAG_RE.sub(" ", stripped)
    normalized = " ".join(unescape(stripped).split()).strip()
    return title, normalized


async def fetch_url_content(url: str, *, timeout_sec: float) -> FetchedUrlContent:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL ingest only supports http and https")

    try:
        async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True, trust_env=False) as client:
            response = await client.get(url)
            response.raise_for_status()
            filename_hint = Path(parsed.path or "").name or parsed.netloc or "downloaded-url"
            return FetchedUrlContent(
                url=str(url),
                raw_bytes=response.content,
                content_type=str(response.headers.get("content-type") or "").split(";")[0].strip(),
                encoding=response.encoding,
                filename_hint=filename_hint,
            )
    except httpx.TimeoutException as error:
        raise ValueError(f"Timed out fetching URL: {url}") from error
    except httpx.HTTPStatusError as error:
        status_code = int(error.response.status_code)
        raise ValueError(f"URL fetch failed with HTTP {status_code}: {url}") from error
    except httpx.RequestError as error:
        raise ValueError(f"Failed to fetch URL {url}: {error}") from error

def extract_fetched_url_document(
    fetched: FetchedUrlContent,
    *,
    max_chars: int,
) -> ExtractedDocument:
    url = str(fetched.url)
    raw_bytes = fetched.raw_bytes
    content_type = fetched.content_type
    filename_hint = fetched.filename_hint
    structured_suffix = resolve_structured_document_suffix(Path(filename_hint), content_type or None)
    if structured_suffix:
        if len(raw_bytes) > MAX_BINARY_DOCUMENT_BYTES:
            raise ValueError(
                f"Downloaded document is too large to ingest from URL: {url}"
            )
        extracted = extract_uploaded_document(
            filename_hint,
            raw_bytes,
            content_type or None,
        )
        if extracted is None:
            raise ValueError(
                f"The document at {url} did not produce readable text. "
                "PDF, DOCX, and OpenDocument text formats are supported. OCR is not enabled."
            )
        return ExtractedDocument(
            document_uri=str(url),
            title=extracted.title or str(url),
            mime_type=extracted.mime_type,
            sha256=hashlib.sha256(raw_bytes).hexdigest(),
            byte_size=len(raw_bytes),
            text=extracted.text,
        )

    truncated_bytes = raw_bytes[:max_chars]
    decoded = truncated_bytes.decode(fetched.encoding or "utf-8", errors="ignore")
    if "html" in content_type or "<html" in decoded.lower():
        title, text = _html_to_text(decoded)
        mime_type = content_type or "text/html"
        if not text:
            raise ValueError(f"No readable text content found at {url}")
        return ExtractedDocument(
            document_uri=str(url),
            title=title or str(url),
            mime_type=mime_type,
            sha256=hashlib.sha256(truncated_bytes).hexdigest(),
            byte_size=len(truncated_bytes),
            text=text,
        )

    extracted = extract_uploaded_document(
        filename_hint,
        truncated_bytes,
        content_type or None,
    )
    if extracted is not None:
        return ExtractedDocument(
            document_uri=str(url),
            title=extracted.title or str(url),
            mime_type=extracted.mime_type,
            sha256=hashlib.sha256(truncated_bytes).hexdigest(),
            byte_size=len(truncated_bytes),
            text=extracted.text,
        )

    text = " ".join(decoded.split()).strip()
    if not text:
        if content_type:
            raise ValueError(
                f"No readable text content found at {url}. Content type {content_type} is not currently supported for URL ingest."
            )
        raise ValueError(f"No readable text content found at {url}")

    return ExtractedDocument(
        document_uri=str(url),
        title=str(url),
        mime_type=content_type or "text/plain",
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        byte_size=len(raw_bytes),
        text=text,
    )


async def extract_url_document(
    url: str,
    *,
    timeout_sec: float,
    max_chars: int,
) -> ExtractedDocument:
    fetched = await fetch_url_content(url, timeout_sec=timeout_sec)
    return extract_fetched_url_document(fetched, max_chars=max_chars)

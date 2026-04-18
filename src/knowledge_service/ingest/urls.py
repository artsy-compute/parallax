from __future__ import annotations

import hashlib
import re
from html import unescape
from urllib.parse import urlparse

import httpx

from knowledge_service.ingest.local_files import ExtractedDocument

_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>[\s\S]*?</\1>", re.IGNORECASE)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")


def _html_to_text(content: str) -> tuple[str, str]:
    title_match = _TITLE_RE.search(content or "")
    title = " ".join(unescape(title_match.group(1)).split()).strip() if title_match else ""
    stripped = _SCRIPT_STYLE_RE.sub(" ", content or "")
    stripped = _TAG_RE.sub(" ", stripped)
    normalized = " ".join(unescape(stripped).split()).strip()
    return title, normalized


async def extract_url_document(
    url: str,
    *,
    timeout_sec: float,
    max_chars: int,
) -> ExtractedDocument:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL ingest only supports http and https")

    async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        raw_bytes = response.content[:max_chars]
        content_type = str(response.headers.get("content-type") or "").split(";")[0].strip()
        decoded = raw_bytes.decode(response.encoding or "utf-8", errors="ignore")

    if "html" in content_type or "<html" in decoded.lower():
        title, text = _html_to_text(decoded)
        mime_type = content_type or "text/html"
    else:
        title = url
        text = " ".join(decoded.split()).strip()
        mime_type = content_type or "text/plain"

    if not text:
        raise ValueError(f"No readable text content found at {url}")

    return ExtractedDocument(
        document_uri=str(url),
        title=title or str(url),
        mime_type=mime_type,
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        byte_size=len(raw_bytes),
        text=text,
    )

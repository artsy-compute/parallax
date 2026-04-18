from __future__ import annotations

import re
from typing import Iterable

_BLANK_LINE_RE = re.compile(r"\n\s*\n+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+")


def estimate_token_count(text: str) -> int:
    return max(1, len(str(text or "").strip()) // 4)


def _normalize_paragraphs(text: str) -> list[str]:
    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    parts = []
    for paragraph in _BLANK_LINE_RE.split(normalized):
        compact = " ".join(paragraph.split()).strip()
        if compact:
            parts.append(compact)
    return parts


def _split_large_piece(text: str, target_chars: int) -> list[str]:
    compact = " ".join(str(text or "").split()).strip()
    if len(compact) <= target_chars:
        return [compact] if compact else []

    sentences = [part.strip() for part in _SENTENCE_RE.split(compact) if part.strip()]
    if len(sentences) <= 1:
        return [
            compact[index : index + target_chars].strip()
            for index in range(0, len(compact), target_chars)
            if compact[index : index + target_chars].strip()
        ]

    chunks: list[str] = []
    current = ""
    for sentence in sentences:
        if not current:
            current = sentence
            continue
        candidate = f"{current} {sentence}".strip()
        if len(candidate) <= target_chars:
            current = candidate
            continue
        chunks.append(current)
        current = sentence
    if current:
        chunks.append(current)
    return chunks


def chunk_text(
    text: str,
    *,
    target_chars: int = 850,
    overlap_chars: int = 140,
) -> list[str]:
    pieces: list[str] = []
    for paragraph in _normalize_paragraphs(text):
        pieces.extend(_split_large_piece(paragraph, target_chars))

    chunks: list[str] = []
    current = ""
    for piece in pieces:
        if not current:
            current = piece
            continue
        candidate = f"{current}\n\n{piece}".strip()
        if len(candidate) <= target_chars:
            current = candidate
            continue
        chunks.append(current)
        overlap = current[-overlap_chars:].strip() if overlap_chars > 0 else ""
        current = f"{overlap}\n\n{piece}".strip() if overlap else piece
    if current:
        chunks.append(current)
    return [chunk for chunk in chunks if chunk.strip()]


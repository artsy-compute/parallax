from __future__ import annotations

import re
from collections.abc import Iterable

_WORD_RE = re.compile(r"[A-Za-z0-9_\-']+")


def fts_query_from_text(query: str) -> str:
    tokens = [
        token for token in _WORD_RE.findall(str(query or "").lower()) if len(token) >= 2
    ]
    if not tokens:
        return ""
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        unique.append(token)
    return " OR ".join(f'"{token}"' for token in unique)


def reciprocal_rank_fusion(
    ranked_lists: Iterable[Iterable[int]],
    *,
    k: int = 60,
) -> list[int]:
    scores: dict[int, float] = {}
    for ranked in ranked_lists:
        for rank, item_id in enumerate(ranked, start=1):
            scores[item_id] = scores.get(item_id, 0.0) + 1.0 / (k + rank)
    return [
        item_id
        for item_id, _ in sorted(scores.items(), key=lambda item: item[1], reverse=True)
    ]

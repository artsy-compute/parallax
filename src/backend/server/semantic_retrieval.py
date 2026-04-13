import math
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional

import numpy as np

from parallax_utils.logging_config import get_logger

try:
    import hnswlib  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    hnswlib = None

_WORD_RE = re.compile(r"[A-Za-z0-9_\-']+")
logger = get_logger(__name__)


@dataclass(frozen=True)
class SemanticSnippet:
    text: str
    score: float


class HashingEmbeddingProvider:
    """A lightweight deterministic embedding provider.

    This is not as strong as a trained embedding model, but it gives us a pluggable
    semantic-like retrieval layer without requiring remote services. It combines word
    and char-trigram hashing and normalizes vectors for cosine similarity.
    """

    def __init__(self, dim: int = 256):
        self.dim = max(64, dim)

    def _tokenize(self, text: str) -> List[str]:
        compact = " ".join((text or "").split()).lower()
        words = [tok for tok in _WORD_RE.findall(compact) if len(tok) >= 2]
        trigrams = [compact[i : i + 3] for i in range(max(0, len(compact) - 2)) if compact[i : i + 3].strip()]
        return words + trigrams

    def embed(self, text: str) -> np.ndarray:
        vec = np.zeros(self.dim, dtype=np.float32)
        for token in self._tokenize(text):
            index = hash(token) % self.dim
            sign = 1.0 if (hash(token + "#") & 1) == 0 else -1.0
            vec[index] += sign
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec /= norm
        return vec

    def embed_many(self, texts: Iterable[str]) -> np.ndarray:
        vectors = [self.embed(text) for text in texts]
        if not vectors:
            return np.zeros((0, self.dim), dtype=np.float32)
        return np.stack(vectors, axis=0)


class SemanticSnippetRetriever:
    def __init__(self, dim: int = 256, recency_weight: float = 0.15):
        self.provider = HashingEmbeddingProvider(dim=dim)
        self.recency_weight = recency_weight
        self.backend_name = 'hnswlib' if hnswlib is not None else 'bruteforce'
        logger.info('Semantic snippet retriever initialized: backend=%s dim=%d recency_weight=%.3f', self.backend_name, dim, recency_weight)

    def retrieve(
        self,
        *,
        query: str,
        snippets: List[str],
        limit: int,
        char_budget: int,
    ) -> List[str]:
        if not query or not snippets:
            return []

        query_vector = self.provider.embed(query)
        if np.linalg.norm(query_vector) <= 0:
            return []

        snippet_vectors = self.provider.embed_many(snippets)
        if snippet_vectors.shape[0] == 0:
            return []

        scores, backend = self._score_vectors(query_vector, snippet_vectors)
        total = len(snippets)
        ranked = []
        for idx, score in enumerate(scores.tolist()):
            recency_boost = ((idx + 1) / max(total, 1)) * self.recency_weight
            ranked.append((score + recency_boost, snippets[idx]))
        ranked.sort(key=lambda item: item[0], reverse=True)

        selected: List[str] = []
        current_chars = 0
        for _, snippet in ranked:
            if snippet in selected:
                continue
            if len(selected) >= limit:
                break
            if current_chars + len(snippet) + 1 > char_budget:
                break
            selected.append(snippet)
            current_chars += len(snippet) + 1
        self.last_backend_used = backend
        return selected

    def _score_vectors(self, query_vector: np.ndarray, snippet_vectors: np.ndarray) -> tuple[np.ndarray, str]:
        if hnswlib is not None and snippet_vectors.shape[0] >= 16:
            try:
                index = hnswlib.Index(space='cosine', dim=snippet_vectors.shape[1])
                index.init_index(max_elements=snippet_vectors.shape[0], ef_construction=100, M=16)
                index.add_items(snippet_vectors, np.arange(snippet_vectors.shape[0]))
                index.set_ef(min(50, snippet_vectors.shape[0]))
                labels, distances = index.knn_query(query_vector, k=snippet_vectors.shape[0])
                ordered = np.zeros(snippet_vectors.shape[0], dtype=np.float32)
                for rank, label in enumerate(labels[0].tolist()):
                    ordered[label] = 1.0 - float(distances[0][rank])
                return ordered, 'hnswlib'
            except Exception:
                logger.warning('Semantic retrieval falling back to brute-force scoring after hnswlib failure', exc_info=True)

        return np.clip(snippet_vectors @ query_vector, -1.0, 1.0), 'bruteforce'

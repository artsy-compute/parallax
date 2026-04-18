from __future__ import annotations

import re
from collections.abc import Iterable

import numpy as np

from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)

_WORD_RE = re.compile(r"[A-Za-z0-9_\-']+")


class HashingEmbeddingProvider:
    def __init__(self, dim: int = 256):
        self.dim = max(64, dim)
        self.name = f"hashing:{self.dim}"

    def _tokenize(self, text: str) -> list[str]:
        compact = " ".join((text or "").split()).lower()
        words = [tok for tok in _WORD_RE.findall(compact) if len(tok) >= 2]
        trigrams = [
            compact[index : index + 3]
            for index in range(max(0, len(compact) - 2))
            if compact[index : index + 3].strip()
        ]
        return words + trigrams

    def embed(self, text: str) -> np.ndarray:
        vec = np.zeros(self.dim, dtype=np.float32)
        for token in self._tokenize(text):
            index = hash(token) % self.dim
            sign = 1.0 if (hash(token + "#") & 1) == 0 else -1.0
            vec[index] += sign
        norm = float(np.linalg.norm(vec))
        if norm > 0:
            vec /= norm
        return vec

    def embed_many(self, texts: Iterable[str]) -> np.ndarray:
        vectors = [self.embed(text) for text in texts]
        if not vectors:
            return np.zeros((0, self.dim), dtype=np.float32)
        return np.stack(vectors, axis=0)


class TransformersEmbeddingProvider:
    def __init__(self, model_name: str):
        self.model_name = str(model_name or "").strip()
        self.name = f"transformers:{self.model_name}"
        self._tokenizer = None
        self._model = None
        self._torch = None
        self.dim = 0

    def _ensure_loaded(self) -> None:
        if self._tokenizer is not None and self._model is not None and self._torch is not None:
            return
        import torch  # type: ignore
        from transformers import AutoModel, AutoTokenizer

        self._torch = torch
        self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self._model = AutoModel.from_pretrained(self.model_name)
        self._model.eval()
        self.dim = int(getattr(self._model.config, "hidden_size", 0) or 0)
        if self.dim <= 0:
            raise RuntimeError(
                f"Embedding model {self.model_name} did not expose a usable hidden_size"
            )

    def embed_many(self, texts: Iterable[str]) -> np.ndarray:
        self._ensure_loaded()
        torch = self._torch
        tokenizer = self._tokenizer
        model = self._model
        items = [str(text or "") for text in texts]
        if not items:
            return np.zeros((0, self.dim), dtype=np.float32)

        encoded = tokenizer(
            items,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt",
        )
        with torch.no_grad():
            outputs = model(**encoded)
        hidden = outputs.last_hidden_state
        mask = encoded["attention_mask"].unsqueeze(-1).expand(hidden.size()).float()
        pooled = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1e-9)
        normalized = torch.nn.functional.normalize(pooled, p=2, dim=1)
        return normalized.detach().cpu().numpy().astype(np.float32)

    def embed(self, text: str) -> np.ndarray:
        vectors = self.embed_many([text])
        if vectors.shape[0] == 0:
            return np.zeros((self.dim,), dtype=np.float32)
        return vectors[0]


class EmbeddingService:
    def __init__(self, model_name: str, fallback_dim: int = 256):
        self._model_name = str(model_name or "").strip()
        self._fallback = HashingEmbeddingProvider(dim=fallback_dim)
        self._active = None
        self._resolution_attempted = False

    @property
    def configured_provider_name(self) -> str:
        if self._model_name:
            return f"transformers:{self._model_name}"
        return self._fallback.name

    def _provider(self):
        if self._active is not None:
            return self._active
        if not self._resolution_attempted and self._model_name:
            self._resolution_attempted = True
            try:
                provider = TransformersEmbeddingProvider(self._model_name)
                provider.embed_many(["warmup"])
                self._active = provider
                logger.info("Knowledge embeddings using transformers model %s", self._model_name)
                return self._active
            except Exception as exc:
                logger.warning(
                    "Falling back to hashing embeddings after transformers load failure for %s: %s",
                    self._model_name,
                    exc,
                    exc_info=True,
                )
        self._active = self._fallback
        return self._active

    @property
    def provider_name(self) -> str:
        return str(self._provider().name)

    @property
    def dim(self) -> int:
        return int(self._provider().dim)

    @property
    def active_provider_name(self) -> str | None:
        return str(self._active.name) if self._active is not None else None

    def embed(self, text: str) -> np.ndarray:
        return self._provider().embed(text)

    def embed_many(self, texts: Iterable[str]) -> np.ndarray:
        return self._provider().embed_many(texts)

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from parallax_utils.file_util import get_project_root


@dataclass(frozen=True)
class KnowledgeServiceConfig:
    host: str
    port: int
    storage_root: Path
    default_workspace_root: Path
    embedding_model_name: str
    hashing_fallback_dim: int
    fetch_timeout_sec: float
    hnsw_ef_search: int
    hnsw_m: int
    max_url_chars: int


def load_config() -> KnowledgeServiceConfig:
    storage_root = Path(
        os.environ.get("PARALLAX_KB_STORAGE_ROOT", "~/.parallax/knowledge")
    ).expanduser()
    workspace_root = Path(
        os.environ.get("PARALLAX_KB_WORKSPACE_ROOT", str(get_project_root()))
    ).expanduser()
    return KnowledgeServiceConfig(
        host=str(os.environ.get("PARALLAX_KB_HOST", "127.0.0.1")).strip() or "127.0.0.1",
        port=max(1, int(os.environ.get("PARALLAX_KB_PORT", "3012"))),
        storage_root=storage_root.resolve(),
        default_workspace_root=workspace_root.resolve(),
        embedding_model_name=str(
            os.environ.get(
                "PARALLAX_KB_EMBEDDING_MODEL",
                "sentence-transformers/all-MiniLM-L6-v2",
            )
        ).strip(),
        hashing_fallback_dim=max(
            64, int(os.environ.get("PARALLAX_KB_HASHING_DIM", "256"))
        ),
        fetch_timeout_sec=max(
            5.0, float(os.environ.get("PARALLAX_KB_FETCH_TIMEOUT_SEC", "20"))
        ),
        hnsw_ef_search=max(8, int(os.environ.get("PARALLAX_KB_HNSW_EF_SEARCH", "64"))),
        hnsw_m=max(8, int(os.environ.get("PARALLAX_KB_HNSW_M", "16"))),
        max_url_chars=max(
            4096, int(os.environ.get("PARALLAX_KB_MAX_URL_CHARS", "200000"))
        ),
    )


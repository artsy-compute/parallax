from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import requests

from knowledge_service.chunking import chunk_text, estimate_token_count
from knowledge_service.config import KnowledgeServiceConfig
from knowledge_service.embedding import EmbeddingService
from knowledge_service.ingest.local_files import ExtractedDocument, extract_local_documents
from knowledge_service.search import fts_query_from_text, reciprocal_rank_fusion
from parallax_utils.logging_config import get_logger

try:
    import hnswlib  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    hnswlib = None

logger = get_logger(__name__)
_SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class WorkspaceContext:
    workspace_root: Path
    workspace_id: str
    workspace_dir: Path
    metadata_path: Path
    vectors_dir: Path
    raw_dir: Path


@dataclass(frozen=True)
class VectorSnapshot:
    backend: str
    provider_name: str
    dim: int
    chunk_ids: list[str]
    matrix: np.ndarray
    index_path: Path


class KnowledgeStore:
    def __init__(self, config: KnowledgeServiceConfig):
        self.config = config
        self.embeddings = EmbeddingService(
            config.embedding_model_name,
            fallback_dim=config.hashing_fallback_dim,
        )
        self._lock = threading.RLock()

    def workspace_context(self, workspace_root: str | Path | None = None) -> WorkspaceContext:
        root = Path(workspace_root or self.config.default_workspace_root).expanduser().resolve()
        workspace_hash = hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:16]
        workspace_dir = (self.config.storage_root / workspace_hash).resolve()
        vectors_dir = workspace_dir / "vectors"
        raw_dir = workspace_dir / "raw" / "normalized_documents"
        workspace_dir.mkdir(parents=True, exist_ok=True)
        vectors_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        return WorkspaceContext(
            workspace_root=root,
            workspace_id=workspace_hash,
            workspace_dir=workspace_dir,
            metadata_path=workspace_dir / "metadata.sqlite3",
            vectors_dir=vectors_dir,
            raw_dir=raw_dir,
        )

    def _connect(self, context: WorkspaceContext) -> sqlite3.Connection:
        connection = sqlite3.connect(str(context.metadata_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        self._ensure_schema(connection)
        return connection

    def _ensure_schema(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                title TEXT NOT NULL,
                canonical_uri TEXT NOT NULL,
                root_path TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                last_error TEXT NOT NULL DEFAULT '',
                document_count INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sources_workspace_uri
                ON sources(workspace_id, canonical_uri);

            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                document_uri TEXT NOT NULL,
                title TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                byte_size INTEGER NOT NULL DEFAULT 0,
                text_length INTEGER NOT NULL DEFAULT 0,
                chunk_count INTEGER NOT NULL DEFAULT 0,
                content TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id);

            CREATE TABLE IF NOT EXISTS chunks (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                text TEXT NOT NULL,
                token_estimate INTEGER NOT NULL DEFAULT 0,
                char_count INTEGER NOT NULL DEFAULT 0,
                vector_row INTEGER NOT NULL DEFAULT -1,
                created_at REAL NOT NULL,
                FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_chunks_document_position
                ON chunks(document_id, position ASC);
            CREATE INDEX IF NOT EXISTS idx_chunks_workspace ON chunks(workspace_id);

            CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
                chunk_id UNINDEXED,
                text
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                progress REAL NOT NULL DEFAULT 0,
                summary TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_workspace_updated
                ON jobs(workspace_id, updated_at DESC);

            CREATE TABLE IF NOT EXISTS pages (
                id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                parent_page_id TEXT,
                source_id TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL,
                slug TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_home INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'ready',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pages_workspace_parent
                ON pages(workspace_id, parent_page_id, sort_order ASC, title ASC);
            """
        )
        connection.commit()

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _new_id(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _normalize_text_snippet(text: str, max_chars: int = 320) -> str:
        compact = " ".join(str(text or "").split()).strip()
        if len(compact) <= max_chars:
            return compact
        return compact[: max_chars - 1].rstrip() + "…"

    def _vector_matrix_path(self, context: WorkspaceContext) -> Path:
        return context.vectors_dir / "chunks.npy"

    def _vector_meta_path(self, context: WorkspaceContext) -> Path:
        return context.vectors_dir / "chunks.meta.json"

    def _vector_index_path(self, context: WorkspaceContext) -> Path:
        return context.vectors_dir / "chunks.hnswlib.bin"

    def _read_vector_meta(self, context: WorkspaceContext) -> dict[str, Any]:
        path = self._vector_meta_path(context)
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to read KB vector metadata from %s", path, exc_info=True)
            return {}

    def _write_vector_meta(
        self,
        context: WorkspaceContext,
        *,
        chunk_ids: list[str],
        dim: int,
        provider_name: str,
        backend: str,
    ) -> None:
        payload = {
            "chunk_ids": chunk_ids,
            "dim": dim,
            "provider_name": provider_name,
            "backend": backend,
            "updated_at": self._now(),
        }
        self._vector_meta_path(context).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_vector_snapshot(self, context: WorkspaceContext) -> VectorSnapshot:
        matrix_path = self._vector_matrix_path(context)
        meta = self._read_vector_meta(context)
        if not matrix_path.exists() or not meta.get("chunk_ids"):
            return VectorSnapshot(
                backend="none",
                provider_name=str(meta.get("provider_name") or ""),
                dim=0,
                chunk_ids=[],
                matrix=np.zeros((0, 0), dtype=np.float32),
                index_path=self._vector_index_path(context),
            )
        matrix = np.load(str(matrix_path))
        if matrix.ndim == 1:
            matrix = matrix.reshape(1, -1)
        return VectorSnapshot(
            backend=str(meta.get("backend") or "bruteforce"),
            provider_name=str(meta.get("provider_name") or ""),
            dim=int(meta.get("dim") or (matrix.shape[1] if matrix.size else 0)),
            chunk_ids=[str(item) for item in list(meta.get("chunk_ids") or [])],
            matrix=matrix.astype(np.float32),
            index_path=self._vector_index_path(context),
        )

    def _row_to_source_summary(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "workspace_id": row["workspace_id"],
            "source_type": row["source_type"],
            "title": row["title"],
            "canonical_uri": row["canonical_uri"],
            "root_path": row["root_path"],
            "status": row["status"],
            "document_count": int(row["document_count"] or 0),
            "last_error": row["last_error"] or "",
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def _row_to_document_summary(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "source_id": row["source_id"],
            "document_uri": row["document_uri"],
            "title": row["title"],
            "mime_type": row["mime_type"],
            "sha256": row["sha256"],
            "byte_size": int(row["byte_size"] or 0),
            "text_length": int(row["text_length"] or 0),
            "chunk_count": int(row["chunk_count"] or 0),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    def _row_to_job(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "workspace_id": row["workspace_id"],
            "job_type": row["job_type"],
            "status": row["status"],
            "progress": float(row["progress"] or 0),
            "summary": row["summary"] or "",
            "error": row["error"] or "",
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
            "completed_at": float(row["completed_at"]) if row["completed_at"] is not None else None,
        }

    def _row_to_page_summary(self, row: sqlite3.Row, *, child_count: int = 0) -> dict[str, Any]:
        return {
            "id": row["id"],
            "workspace_id": row["workspace_id"],
            "parent_page_id": row["parent_page_id"] or None,
            "source_id": row["source_id"] or "",
            "title": row["title"] or "",
            "slug": row["slug"] or "",
            "summary": row["summary"] or "",
            "sort_order": int(row["sort_order"] or 0),
            "is_home": bool(row["is_home"]),
            "status": row["status"] or "ready",
            "child_count": int(child_count or 0),
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
        }

    @staticmethod
    def _slugify(value: str) -> str:
        compact = _SLUG_RE.sub("-", str(value or "").strip().lower()).strip("-")
        return compact or "page"

    @staticmethod
    def _normalize_page_title(value: str, max_chars: int = 88) -> str:
        compact = " ".join(str(value or "").split()).strip()
        if not compact:
            return "Untitled page"
        if len(compact) <= max_chars:
            return compact
        return compact[: max_chars - 1].rstrip() + "…"

    @staticmethod
    def _looks_like_locator_title(value: str) -> bool:
        compact = " ".join(str(value or "").split()).strip().lower()
        if not compact:
            return True
        if compact.startswith(("http://", "https://", "www.")):
            return True
        if "/" in compact and ("." in compact or compact.count("/") >= 2):
            return True
        if compact.endswith((".md", ".txt", ".html", ".htm", ".pdf", ".json", ".py", ".ts", ".tsx", ".js")):
            return True
        return False

    @staticmethod
    def _extract_summary_markdown(content: str) -> str:
        for line in str(content or "").splitlines():
            compact = line.strip()
            if not compact or compact.startswith("#"):
                continue
            return compact[:240]
        return ""

    def _build_generation_settings(
        self,
        advanced: dict[str, Any] | None,
        *,
        cluster_model_name: str,
        backend_base_url: str,
    ) -> dict[str, Any]:
        raw_advanced = dict(advanced or {})
        raw_providers = raw_advanced.get("llm_providers")
        llm_providers = dict(raw_providers) if isinstance(raw_providers, dict) else {}
        generation = raw_advanced.get("knowledge_generation")
        generation = dict(generation) if isinstance(generation, dict) else {}
        provider = str(generation.get("provider") or "local_cluster").strip() or "local_cluster"
        model_override = str(generation.get("model") or "").strip()
        local_provider = dict(llm_providers.get("local_cluster") or {})
        selected_provider = dict(llm_providers.get(provider) or {})
        if provider == "local_cluster":
            model = model_override or str(local_provider.get("default_model") or cluster_model_name or "").strip()
        else:
            model = model_override or str(selected_provider.get("default_model") or "").strip()
        return {
            "provider": provider,
            "model": model,
            "backend_base_url": str(backend_base_url or "").rstrip("/"),
            "base_url": str(selected_provider.get("base_url") or "").strip(),
            "api_key": str(selected_provider.get("api_key") or "").strip(),
            "cluster_model_name": str(cluster_model_name or "").strip(),
            "use_active_cluster": bool(local_provider.get("use_active_cluster", True)),
            "cluster_id": str(local_provider.get("cluster_id") or "").strip(),
        }

    def _call_generation_provider(
        self,
        *,
        settings: dict[str, Any],
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 1600,
    ) -> str:
        provider = str(settings.get("provider") or "local_cluster").strip()
        model = str(settings.get("model") or "").strip()
        if provider == "local_cluster":
            base_url = str(settings.get("backend_base_url") or "").rstrip("/")
            if not base_url:
                raise ValueError("Knowledge generation requires a backend base URL for local cluster generation")
            response = requests.post(
                f"{base_url}/v1/chat/completions",
                json={
                    "model": model or str(settings.get("cluster_model_name") or "").strip(),
                    "stream": False,
                    "conversation_id": f"knowledge-wiki-{uuid.uuid4().hex[:8]}",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "max_tokens": max_tokens,
                },
                timeout=180,
            )
            response.raise_for_status()
            payload = response.json()
            choices = payload.get("choices") or []
            message = choices[0].get("message") if choices else {}
            content = message.get("content") if isinstance(message, dict) else ""
            return str(content or "").strip()

        if provider in {"openai", "compatible"}:
            base_url = str(settings.get("base_url") or "").rstrip("/")
            api_key = str(settings.get("api_key") or "").strip()
            if not base_url or not api_key:
                raise ValueError(f"{provider} generation is missing base URL or API key")
            response = requests.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "max_tokens": max_tokens,
                },
                timeout=180,
            )
            response.raise_for_status()
            payload = response.json()
            choices = payload.get("choices") or []
            message = choices[0].get("message") if choices else {}
            content = message.get("content") if isinstance(message, dict) else ""
            return str(content or "").strip()

        if provider == "anthropic":
            base_url = str(settings.get("base_url") or "").rstrip("/")
            api_key = str(settings.get("api_key") or "").strip()
            if not base_url or not api_key:
                raise ValueError("Anthropic generation is missing base URL or API key")
            response = requests.post(
                f"{base_url}/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": max_tokens,
                    "system": system_prompt,
                    "messages": [
                        {"role": "user", "content": user_prompt},
                    ],
                },
                timeout=180,
            )
            response.raise_for_status()
            payload = response.json()
            blocks = payload.get("content") or []
            parts = [str(block.get("text") or "").strip() for block in blocks if isinstance(block, dict)]
            return "\n\n".join(part for part in parts if part).strip()

        raise ValueError(f"Unsupported knowledge generation provider: {provider}")

    def _collect_source_material(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        source_id: str,
        max_chars: int = 14000,
    ) -> str:
        rows = connection.execute(
            """
            SELECT title, document_uri, content
            FROM documents
            WHERE workspace_id=? AND source_id=?
            ORDER BY updated_at DESC, created_at DESC
            """,
            (workspace_id, source_id),
        ).fetchall()
        parts: list[str] = []
        remaining = max_chars
        for row in rows:
            content = str(row["content"] or "").strip()
            if not content or remaining <= 0:
                continue
            snippet = content[:remaining].strip()
            parts.append(
                "\n".join(
                    [
                        f"Document: {row['title'] or row['document_uri'] or 'Untitled'}",
                        f"URI: {row['document_uri'] or ''}",
                        "Excerpt:",
                        snippet,
                    ]
                )
            )
            remaining -= len(snippet)
        return "\n\n---\n\n".join(parts).strip()

    @staticmethod
    def _wiki_system_prompt() -> str:
        return (
            "You create concise internal wiki pages in markdown using only provided source material. "
            "Write clearly, organize with headings, and avoid mentioning missing context."
        )

    def _infer_page_title(
        self,
        *,
        fallback_title: str,
        source_material: str,
        generation_settings: dict[str, Any],
    ) -> str:
        normalized_fallback = self._normalize_page_title(fallback_title)
        if normalized_fallback and not self._looks_like_locator_title(normalized_fallback):
            return normalized_fallback

        try:
            title = self._call_generation_provider(
                settings=generation_settings,
                system_prompt=(
                    "You produce concise human-readable wiki page titles from source text. "
                    "Return only the title text. Do not return markdown, quotes, URLs, file paths, or explanations."
                ),
                user_prompt="\n\n".join(
                    [
                        "Suggest a concise page title for this material.",
                        "Use 2 to 8 words when possible.",
                        "Avoid raw URLs and file paths.",
                        "Source material:",
                        source_material[:4000],
                    ]
                ),
                max_tokens=32,
            )
            title = self._normalize_page_title(title.strip().strip("#").strip("\"'"))
            if title and not self._looks_like_locator_title(title):
                return title
        except Exception:
            logger.warning("Failed to infer wiki page title from source text", exc_info=True)

        return normalized_fallback

    def _generate_source_page_payload(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        source_row: sqlite3.Row,
        generation_settings: dict[str, Any],
        sort_order: int,
    ) -> dict[str, Any] | None:
        fallback_title = str(source_row["title"] or source_row["canonical_uri"] or "Untitled source")
        source_material = self._collect_source_material(
            connection,
            workspace_id=workspace_id,
            source_id=str(source_row["id"]),
        )
        if not source_material:
            return None
        source_title = self._infer_page_title(
            fallback_title=fallback_title,
            source_material=source_material,
            generation_settings=generation_settings,
        )

        user_prompt = "\n\n".join(
            [
                "Create a wiki page in markdown for this source.",
                f"Preferred title: {source_title}",
                f"Canonical URI: {source_row['canonical_uri'] or ''}",
                "Write explicit markdown headings.",
                "Start with `## Overview`, then `## Key Details`, then `## Important Notes`.",
                "Use bullet lists where useful and keep prose concise.",
                "Do not use the raw URL or file path as the page title if a better topic/title can be inferred from the text.",
                "Source material:",
                source_material,
            ]
        )
        content = self._call_generation_provider(
            settings=generation_settings,
            system_prompt=self._wiki_system_prompt(),
            user_prompt=user_prompt,
            max_tokens=1400,
        )
        summary = self._extract_summary_markdown(content)
        return {
            "title": source_title,
            "slug": self._slugify(source_title),
            "summary": summary,
            "content": content,
            "source_id": str(source_row["id"]),
            "sort_order": int(sort_order),
        }

    def _generate_home_page_payload(
        self,
        child_pages: list[dict[str, Any]],
        *,
        generation_settings: dict[str, Any],
    ) -> dict[str, str]:
        child_summaries = [
            f"- {page['title']}: {page.get('summary') or 'Generated wiki page'}"
            for page in child_pages
        ]
        home_prompt = "\n\n".join(
            [
                "Create a markdown homepage for this knowledge wiki.",
                "Introduce the knowledge base, summarize the main pages, and include a short navigation section.",
                "Available child pages:",
                "\n".join(child_summaries),
            ]
        )
        home_content = self._call_generation_provider(
            settings=generation_settings,
            system_prompt=self._wiki_system_prompt(),
            user_prompt=home_prompt,
            max_tokens=1400,
        )
        return {
            "title": "Knowledge wiki",
            "slug": "knowledge-wiki",
            "summary": self._extract_summary_markdown(home_content),
            "content": home_content,
        }

    def _link_page_references_in_text(
        self,
        content: str,
        *,
        self_page_id: str,
        page_refs: list[dict[str, str]],
    ) -> str:
        ordered_page_refs = sorted(
            [
                page_ref
                for page_ref in page_refs
                if str(page_ref.get("id") or "").strip() and str(page_ref.get("title") or "").strip()
            ],
            key=lambda page_ref: len(str(page_ref.get("title") or "")),
            reverse=True,
        )
        sections = str(content or "").split("```")
        for index, section in enumerate(sections):
            if index % 2 == 1:
                continue
            lines = section.splitlines()
            for line_index, line in enumerate(lines):
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "](" in line:
                    continue
                next_line = line
                for page_ref in ordered_page_refs:
                    page_id = str(page_ref.get("id") or "").strip()
                    title = str(page_ref.get("title") or "").strip()
                    if not page_id or not title or page_id == self_page_id:
                        continue
                    pattern = re.compile(
                        rf"(?<!\[)(?<!\]\()(?<!/knowledge\?page=)(?<![\w/]){re.escape(title)}(?![\w])(?!\]\()"
                    )
                    next_line = pattern.sub(f"[{title}](/knowledge?page={page_id})", next_line)
                lines[line_index] = next_line
            sections[index] = "\n".join(lines)
        return "```".join(sections)

    def _apply_page_cross_links(self, pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        page_refs = [
            {
                "id": str(page.get("id") or ""),
                "title": str(page.get("title") or ""),
            }
            for page in pages
            if str(page.get("id") or "").strip() and str(page.get("title") or "").strip()
        ]
        next_pages: list[dict[str, Any]] = []
        for page in pages:
            next_page = dict(page)
            next_page["content"] = self._link_page_references_in_text(
                str(page.get("content") or ""),
                self_page_id=str(page.get("id") or ""),
                page_refs=page_refs,
            )
            next_pages.append(next_page)
        return next_pages

    def list_pages(self, workspace_root: str | Path | None = None) -> dict[str, Any]:
        context = self.workspace_context(workspace_root)
        with self._lock, self._connect(context) as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM pages
                WHERE workspace_id=?
                ORDER BY is_home DESC, sort_order ASC, title ASC
                """,
                (context.workspace_id,),
            ).fetchall()
            child_counts: dict[str | None, int] = {}
            for row in rows:
                parent_page_id = str(row["parent_page_id"] or "") or None
                child_counts[parent_page_id] = child_counts.get(parent_page_id, 0) + 1
            items = [
                self._row_to_page_summary(row, child_count=child_counts.get(str(row["id"]), 0))
                for row in rows
            ]
            home_page = next((item for item in items if item["is_home"]), None)
            return {
                "home_page_id": home_page["id"] if home_page else (items[0]["id"] if items else None),
                "items": items,
            }

    def get_page(self, workspace_root: str | Path | None, page_id: str) -> dict[str, Any] | None:
        context = self.workspace_context(workspace_root)
        with self._lock, self._connect(context) as connection:
            row = connection.execute(
                """
                SELECT *
                FROM pages
                WHERE workspace_id=? AND id=?
                """,
                (context.workspace_id, str(page_id or "").strip()),
            ).fetchone()
            if row is None:
                return None
            child_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM pages WHERE workspace_id=? AND parent_page_id=?",
                    (context.workspace_id, row["id"]),
                ).fetchone()["count"]
            )
            return {
                **self._row_to_page_summary(row, child_count=child_count),
                "content": row["content"] or "",
            }

    def generate_wiki(
        self,
        workspace_root: str | Path | None,
        *,
        advanced: dict[str, Any] | None,
        cluster_model_name: str,
        backend_base_url: str,
    ) -> dict[str, Any]:
        context = self.workspace_context(workspace_root)
        generation_settings = self._build_generation_settings(
            advanced,
            cluster_model_name=cluster_model_name,
            backend_base_url=backend_base_url,
        )

        with self._lock, self._connect(context) as connection:
            ready_sources = connection.execute(
                """
                SELECT *
                FROM sources
                WHERE workspace_id=? AND status='ready'
                ORDER BY updated_at DESC, created_at DESC
                """,
                (context.workspace_id,),
            ).fetchall()
            if not ready_sources:
                raise ValueError("No ready knowledge sources are available to generate wiki pages")

            job_id = self._create_job(
                connection,
                workspace_id=context.workspace_id,
                job_type="generate_wiki",
                summary="Generating wiki pages from knowledge sources",
            )
            self._update_job(
                connection,
                job_id,
                status="running",
                progress=0.1,
                summary="Collecting source material for wiki generation",
            )
            connection.commit()

            generated_pages: list[dict[str, Any]] = []
            total_sources = max(1, len(ready_sources))

            for index, source_row in enumerate(ready_sources, start=1):
                generated_page = self._generate_source_page_payload(
                    connection,
                    workspace_id=context.workspace_id,
                    source_row=source_row,
                    generation_settings=generation_settings,
                    sort_order=index,
                )
                if generated_page is None:
                    continue
                generated_pages.append(generated_page)
                self._update_job(
                    connection,
                    job_id,
                    progress=0.15 + (0.55 * (index / total_sources)),
                    summary=f"Generated {index}/{total_sources} source pages",
                )
                connection.commit()

            if not generated_pages:
                self._update_job(
                    connection,
                    job_id,
                    status="failed",
                    progress=1.0,
                    summary="Wiki generation failed",
                    error="No readable source material was available for wiki generation",
                    completed=True,
                )
                connection.commit()
                raise ValueError("No readable source material was available for wiki generation")

            home_page = self._generate_home_page_payload(
                generated_pages,
                generation_settings=generation_settings,
            )

            connection.execute("DELETE FROM pages WHERE workspace_id=?", (context.workspace_id,))
            now = self._now()
            home_page_id = self._new_id("page")
            home_page["id"] = home_page_id
            home_page["parent_page_id"] = None
            home_page["source_id"] = ""
            home_page["sort_order"] = 0
            home_page["is_home"] = True
            home_page["status"] = "ready"
            for page in generated_pages:
                page["id"] = self._new_id("page")
                page["parent_page_id"] = home_page_id
                page["is_home"] = False
                page["status"] = "ready"

            linked_pages = self._apply_page_cross_links([home_page, *generated_pages])
            linked_home_page = linked_pages[0]
            linked_child_pages = linked_pages[1:]
            connection.execute(
                """
                INSERT INTO pages (
                    id, workspace_id, parent_page_id, source_id, title, slug, summary,
                    content, sort_order, is_home, status, created_at, updated_at
                )
                VALUES (?, ?, NULL, '', ?, ?, ?, ?, 0, 1, 'ready', ?, ?)
                """,
                (
                    home_page_id,
                    context.workspace_id,
                    linked_home_page["title"],
                    linked_home_page["slug"],
                    linked_home_page["summary"],
                    linked_home_page["content"],
                    now,
                    now,
                ),
            )

            for page in linked_child_pages:
                connection.execute(
                    """
                    INSERT INTO pages (
                        id, workspace_id, parent_page_id, source_id, title, slug, summary,
                        content, sort_order, is_home, status, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'ready', ?, ?)
                    """,
                    (
                        page["id"],
                        context.workspace_id,
                        home_page_id,
                        page["source_id"],
                        page["title"],
                        page["slug"],
                        page["summary"],
                        page["content"],
                        page["sort_order"],
                        now,
                        now,
                    ),
                )

            self._update_job(
                connection,
                job_id,
                status="completed",
                progress=1.0,
                summary=f"Generated wiki homepage and {len(generated_pages)} child pages",
                error="",
                completed=True,
            )
            connection.commit()

        return {
            "home_page_id": home_page_id,
            "pages_created": len(generated_pages) + 1,
            "job": self.get_job(context.workspace_root, job_id),
            "pages": self.list_pages(context.workspace_root),
        }

    def regenerate_page(
        self,
        workspace_root: str | Path | None,
        page_id: str,
        *,
        advanced: dict[str, Any] | None,
        cluster_model_name: str,
        backend_base_url: str,
    ) -> dict[str, Any] | None:
        context = self.workspace_context(workspace_root)
        normalized_page_id = str(page_id or "").strip()
        if not normalized_page_id:
            return None
        generation_settings = self._build_generation_settings(
            advanced,
            cluster_model_name=cluster_model_name,
            backend_base_url=backend_base_url,
        )

        with self._lock, self._connect(context) as connection:
            page_row = connection.execute(
                "SELECT * FROM pages WHERE workspace_id=? AND id=?",
                (context.workspace_id, normalized_page_id),
            ).fetchone()
            if page_row is None:
                return None

            source_id = str(page_row["source_id"] or "").strip()
            if not source_id:
                raise ValueError("Only source-backed wiki pages can be regenerated individually")

            source_row = connection.execute(
                "SELECT * FROM sources WHERE workspace_id=? AND id=? AND status='ready'",
                (context.workspace_id, source_id),
            ).fetchone()
            if source_row is None:
                raise ValueError("The source for this page is no longer ready for regeneration")

            job_id = self._create_job(
                connection,
                workspace_id=context.workspace_id,
                job_type="regenerate_page",
                summary=f"Regenerating page {page_row['title'] or normalized_page_id}",
            )
            self._update_job(
                connection,
                job_id,
                status="running",
                progress=0.15,
                summary=f"Regenerating page {page_row['title'] or normalized_page_id}",
            )
            connection.commit()

            generated_page = self._generate_source_page_payload(
                connection,
                workspace_id=context.workspace_id,
                source_row=source_row,
                generation_settings=generation_settings,
                sort_order=int(page_row["sort_order"] or 0),
            )
            if generated_page is None:
                self._update_job(
                    connection,
                    job_id,
                    status="failed",
                    progress=1.0,
                    summary="Page regeneration failed",
                    error="No readable source material was available for this page",
                    completed=True,
                )
                connection.commit()
                raise ValueError("No readable source material was available for this page")

            now = self._now()
            home_row = connection.execute(
                "SELECT * FROM pages WHERE workspace_id=? AND is_home=1 LIMIT 1",
                (context.workspace_id,),
            ).fetchone()
            home_page_payload: dict[str, Any] | None = None
            if home_row is not None:
                child_rows = connection.execute(
                    """
                    SELECT id, source_id, parent_page_id, title, slug, summary, content, sort_order, is_home, status
                    FROM pages
                    WHERE workspace_id=? AND parent_page_id=?
                    ORDER BY sort_order ASC, title ASC
                    """,
                    (context.workspace_id, home_row["id"]),
                ).fetchall()
                child_pages: list[dict[str, Any]] = []
                for row in child_rows:
                    if str(row["id"]) == normalized_page_id:
                        child_pages.append(
                            {
                                "id": normalized_page_id,
                                "source_id": str(row["source_id"] or ""),
                                "parent_page_id": str(row["parent_page_id"] or "") or None,
                                "title": generated_page["title"],
                                "slug": generated_page["slug"],
                                "summary": generated_page["summary"],
                                "content": generated_page["content"],
                                "sort_order": int(row["sort_order"] or 0),
                                "is_home": bool(row["is_home"]),
                                "status": str(row["status"] or "ready"),
                            }
                        )
                        continue
                    child_pages.append(
                        {
                            "id": str(row["id"]),
                            "source_id": str(row["source_id"] or ""),
                            "parent_page_id": str(row["parent_page_id"] or "") or None,
                            "title": str(row["title"] or ""),
                            "slug": str(row["slug"] or ""),
                            "summary": str(row["summary"] or ""),
                            "content": str(row["content"] or ""),
                            "sort_order": int(row["sort_order"] or 0),
                            "is_home": bool(row["is_home"]),
                            "status": str(row["status"] or "ready"),
                        }
                    )
                home_page = self._generate_home_page_payload(
                    [
                        {
                            "title": str(page["title"] or ""),
                            "summary": str(page["summary"] or ""),
                        }
                        for page in child_pages
                    ],
                    generation_settings=generation_settings,
                )
                home_page_payload = {
                    "id": str(home_row["id"]),
                    "source_id": "",
                    "parent_page_id": None,
                    "title": str(home_row["title"] or home_page["title"] or "Knowledge wiki"),
                    "slug": str(home_row["slug"] or home_page["slug"] or "knowledge-wiki"),
                    "summary": home_page["summary"],
                    "content": home_page["content"],
                    "sort_order": int(home_row["sort_order"] or 0),
                    "is_home": True,
                    "status": str(home_row["status"] or "ready"),
                }
                linked_pages = self._apply_page_cross_links([home_page_payload, *child_pages])
                linked_home_page = linked_pages[0]
                linked_child_pages = {str(page["id"]): page for page in linked_pages[1:]}
                generated_page = dict(linked_child_pages.get(normalized_page_id) or generated_page)
                connection.execute(
                    """
                    UPDATE pages
                    SET summary=?, content=?, updated_at=?
                    WHERE workspace_id=? AND id=?
                    """,
                    (
                        linked_home_page["summary"],
                        linked_home_page["content"],
                        now,
                        context.workspace_id,
                        home_row["id"],
                    ),
                )

            connection.execute(
                """
                UPDATE pages
                SET title=?, slug=?, summary=?, content=?, status='ready', updated_at=?
                WHERE workspace_id=? AND id=?
                """,
                (
                    generated_page["title"],
                    generated_page["slug"],
                    generated_page["summary"],
                    generated_page["content"],
                    now,
                    context.workspace_id,
                    normalized_page_id,
                ),
            )

            self._update_job(
                connection,
                job_id,
                status="completed",
                progress=1.0,
                summary=f"Regenerated page {generated_page['title']}",
                error="",
                completed=True,
            )
            connection.commit()

        return {
            "page": self.get_page(context.workspace_root, normalized_page_id),
            "job": self.get_job(context.workspace_root, job_id),
            "pages": self.list_pages(context.workspace_root),
        }

    def _create_job(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        job_type: str,
        summary: str,
    ) -> str:
        now = self._now()
        job_id = self._new_id("job")
        connection.execute(
            """
            INSERT INTO jobs (
                id, workspace_id, job_type, status, progress, summary, error, created_at, updated_at, completed_at
            )
            VALUES (?, ?, ?, 'queued', 0, ?, '', ?, ?, NULL)
            """,
            (job_id, workspace_id, job_type, summary, now, now),
        )
        return job_id

    def _update_job(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        *,
        status: str | None = None,
        progress: float | None = None,
        summary: str | None = None,
        error: str | None = None,
        completed: bool = False,
    ) -> None:
        now = self._now()
        fields: list[str] = ["updated_at=?"]
        values: list[Any] = [now]
        if status is not None:
            fields.append("status=?")
            values.append(status)
        if progress is not None:
            fields.append("progress=?")
            values.append(max(0.0, min(float(progress), 1.0)))
        if summary is not None:
            fields.append("summary=?")
            values.append(summary)
        if error is not None:
            fields.append("error=?")
            values.append(error)
        if completed:
            fields.append("completed_at=?")
            values.append(now)
        values.append(job_id)
        connection.execute(
            f"UPDATE jobs SET {', '.join(fields)} WHERE id=?",
            values,
        )

    def _upsert_source(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        source_type: str,
        title: str,
        canonical_uri: str,
        root_path: str,
    ) -> str:
        now = self._now()
        row = connection.execute(
            "SELECT id FROM sources WHERE workspace_id=? AND canonical_uri=?",
            (workspace_id, canonical_uri),
        ).fetchone()
        if row is not None:
            source_id = str(row["id"])
            connection.execute(
                """
                UPDATE sources SET
                    source_type=?,
                    title=?,
                    root_path=?,
                    status='queued',
                    last_error='',
                    updated_at=?
                WHERE id=?
                """,
                (source_type, title, root_path, now, source_id),
            )
            return source_id
        source_id = self._new_id("src")
        connection.execute(
            """
            INSERT INTO sources (
                id, workspace_id, source_type, title, canonical_uri, root_path, status,
                last_error, document_count, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'queued', '', 0, ?, ?)
            """,
            (
                source_id,
                workspace_id,
                source_type,
                title,
                canonical_uri,
                root_path,
                now,
                now,
            ),
        )
        return source_id

    def _replace_source_documents(
        self,
        connection: sqlite3.Connection,
        context: WorkspaceContext,
        *,
        source_id: str,
        documents: list[ExtractedDocument],
    ) -> tuple[int, int]:
        chunk_ids = [
            row["id"]
            for row in connection.execute(
                """
                SELECT chunks.id
                FROM chunks
                JOIN documents ON chunks.document_id=documents.id
                WHERE documents.source_id=?
                """,
                (source_id,),
            ).fetchall()
        ]
        if chunk_ids:
            connection.executemany(
                "DELETE FROM chunk_fts WHERE chunk_id=?",
                [(chunk_id,) for chunk_id in chunk_ids],
            )
        connection.execute("DELETE FROM documents WHERE source_id=?", (source_id,))

        document_count = 0
        total_chunks = 0
        now = self._now()

        for document in documents:
            chunks = chunk_text(document.text)
            if not chunks:
                continue
            document_id = self._new_id("doc")
            connection.execute(
                """
                INSERT INTO documents (
                    id, source_id, workspace_id, document_uri, title, mime_type, sha256,
                    byte_size, text_length, chunk_count, content, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document_id,
                    source_id,
                    context.workspace_id,
                    document.document_uri,
                    document.title,
                    document.mime_type,
                    document.sha256,
                    int(document.byte_size),
                    len(document.text),
                    len(chunks),
                    document.text,
                    now,
                    now,
                ),
            )
            (context.raw_dir / f"{document_id}.txt").write_text(
                document.text,
                encoding="utf-8",
            )
            document_count += 1
            for position, chunk in enumerate(chunks):
                chunk_id = self._new_id("chk")
                connection.execute(
                    """
                    INSERT INTO chunks (
                        id, document_id, workspace_id, position, text, token_estimate,
                        char_count, vector_row, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, -1, ?)
                    """,
                    (
                        chunk_id,
                        document_id,
                        context.workspace_id,
                        position,
                        chunk,
                        estimate_token_count(chunk),
                        len(chunk),
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO chunk_fts (chunk_id, text) VALUES (?, ?)",
                    (chunk_id, chunk),
                )
                total_chunks += 1

        connection.execute(
            """
            UPDATE sources SET
                document_count=?,
                updated_at=?
            WHERE id=?
            """,
            (document_count, now, source_id),
        )
        return document_count, total_chunks

    def _set_source_status(
        self,
        connection: sqlite3.Connection,
        source_id: str,
        *,
        status: str,
        title: str | None = None,
        last_error: str | None = None,
    ) -> None:
        now = self._now()
        fields = ["status=?", "updated_at=?"]
        values: list[Any] = [status, now]
        if title is not None:
            fields.append("title=?")
            values.append(title)
        if last_error is not None:
            fields.append("last_error=?")
            values.append(last_error)
        values.append(source_id)
        connection.execute(
            f"UPDATE sources SET {', '.join(fields)} WHERE id=?",
            values,
        )

    def _rebuild_vector_index(self, context: WorkspaceContext) -> dict[str, Any]:
        with self._lock, self._connect(context) as connection:
            rows = connection.execute(
                """
                SELECT id, text
                FROM chunks
                WHERE workspace_id=?
                ORDER BY created_at ASC, id ASC
                """,
                (context.workspace_id,),
            ).fetchall()
            chunk_ids = [str(row["id"]) for row in rows]
            texts = [str(row["text"] or "") for row in rows]

            if not chunk_ids:
                for path in (
                    self._vector_matrix_path(context),
                    self._vector_meta_path(context),
                    self._vector_index_path(context),
                ):
                    if path.exists():
                        path.unlink()
                return {"backend": "none", "provider_name": self.embeddings.active_provider_name or self.embeddings.configured_provider_name, "count": 0}

            vectors = self.embeddings.embed_many(texts)
            if vectors.ndim == 1:
                vectors = vectors.reshape(1, -1)

            np.save(str(self._vector_matrix_path(context)), vectors.astype(np.float32))
            connection.executemany(
                "UPDATE chunks SET vector_row=? WHERE id=?",
                [(index, chunk_id) for index, chunk_id in enumerate(chunk_ids)],
            )

            backend = "bruteforce"
            index_path = self._vector_index_path(context)
            if hnswlib is not None and vectors.shape[0] > 0:
                try:
                    index = hnswlib.Index(space="cosine", dim=int(vectors.shape[1]))
                    index.init_index(
                        max_elements=max(1, int(vectors.shape[0])),
                        ef_construction=max(100, self.config.hnsw_ef_search),
                        M=self.config.hnsw_m,
                    )
                    index.add_items(vectors, np.arange(vectors.shape[0]))
                    index.set_ef(max(self.config.hnsw_ef_search, min(64, int(vectors.shape[0]))))
                    index.save_index(str(index_path))
                    backend = "hnswlib"
                except Exception:
                    logger.warning("Knowledge vector index falling back to brute-force search", exc_info=True)
                    if index_path.exists():
                        index_path.unlink()
            elif index_path.exists():
                index_path.unlink()

            self._write_vector_meta(
                context,
                chunk_ids=chunk_ids,
                dim=int(vectors.shape[1]),
                provider_name=self.embeddings.provider_name,
                backend=backend,
            )
            connection.commit()
            return {
                "backend": backend,
                "provider_name": self.embeddings.provider_name,
                "count": len(chunk_ids),
                "dim": int(vectors.shape[1]),
            }

    def _ingest_documents(
        self,
        *,
        context: WorkspaceContext,
        source_type: str,
        title: str,
        canonical_uri: str,
        root_path: str,
        documents: list[ExtractedDocument],
        job_type: str,
        job_summary: str,
    ) -> dict[str, Any]:
        if not documents:
            raise ValueError("No readable text documents were found to ingest")

        with self._lock, self._connect(context) as connection:
            job_id = self._create_job(
                connection,
                workspace_id=context.workspace_id,
                job_type=job_type,
                summary=job_summary,
            )
            source_id = self._upsert_source(
                connection,
                workspace_id=context.workspace_id,
                source_type=source_type,
                title=title,
                canonical_uri=canonical_uri,
                root_path=root_path,
            )
            self._update_job(
                connection,
                job_id,
                status="running",
                progress=0.25,
                summary=job_summary,
            )
            connection.commit()

            try:
                document_count, chunk_count = self._replace_source_documents(
                    connection,
                    context,
                    source_id=source_id,
                    documents=documents,
                )
                if document_count <= 0 or chunk_count <= 0:
                    raise ValueError("The source did not produce any searchable text chunks")

                self._set_source_status(
                    connection,
                    source_id,
                    status="ready",
                    title=documents[0].title if source_type == "url" and len(documents) == 1 else title,
                    last_error="",
                )
                self._update_job(
                    connection,
                    job_id,
                    progress=0.75,
                    summary=f"Indexed {document_count} documents and {chunk_count} chunks",
                )
                connection.commit()
            except Exception as exc:
                self._set_source_status(
                    connection,
                    source_id,
                    status="failed",
                    last_error=str(exc),
                )
                self._update_job(
                    connection,
                    job_id,
                    status="failed",
                    progress=1.0,
                    summary="Knowledge ingest failed",
                    error=str(exc),
                    completed=True,
                )
                connection.commit()
                raise

        vector_status = self._rebuild_vector_index(context)

        with self._lock, self._connect(context) as connection:
            self._update_job(
                connection,
                job_id,
                status="completed",
                progress=1.0,
                summary=(
                    f"Indexed {document_count} documents and {chunk_count} chunks "
                    f"using {vector_status.get('provider_name') or 'embeddings'} "
                    f"({vector_status.get('backend') or 'none'})"
                ),
                error="",
                completed=True,
            )
            connection.commit()

        return {
            "source": self.get_source(context.workspace_root, source_id),
            "job": self.get_job(context.workspace_root, job_id),
            "vector_status": vector_status,
        }

    def health(self, workspace_root: str | Path | None = None) -> dict[str, Any]:
        context = self.workspace_context(workspace_root)
        with self._lock, self._connect(context) as connection:
            counts = {
                "sources": int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM sources WHERE workspace_id=?",
                        (context.workspace_id,),
                    ).fetchone()["count"]
                ),
                "documents": int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM documents WHERE workspace_id=?",
                        (context.workspace_id,),
                    ).fetchone()["count"]
                ),
                "chunks": int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM chunks WHERE workspace_id=?",
                        (context.workspace_id,),
                    ).fetchone()["count"]
                ),
                "jobs": int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM jobs WHERE workspace_id=?",
                        (context.workspace_id,),
                    ).fetchone()["count"]
                ),
            }
        vector_meta = self._read_vector_meta(context)
        return {
            "ok": True,
            "workspace_id": context.workspace_id,
            "workspace_root": str(context.workspace_root),
            "storage_root": str(context.workspace_dir),
            "embeddings": {
                "configured_provider": self.embeddings.configured_provider_name,
                "active_provider": self.embeddings.active_provider_name or str(vector_meta.get("provider_name") or ""),
            },
            "vector_backend": str(vector_meta.get("backend") or "none"),
            "counts": counts,
        }

    def list_sources(self, workspace_root: str | Path | None = None, limit: int = 100) -> list[dict[str, Any]]:
        context = self.workspace_context(workspace_root)
        limit = max(1, min(int(limit or 100), 500))
        with self._lock, self._connect(context) as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM sources
                WHERE workspace_id=?
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (context.workspace_id, limit),
            ).fetchall()
            return [self._row_to_source_summary(row) for row in rows]

    def get_source(self, workspace_root: str | Path | None, source_id: str) -> dict[str, Any] | None:
        context = self.workspace_context(workspace_root)
        with self._lock, self._connect(context) as connection:
            row = connection.execute(
                "SELECT * FROM sources WHERE workspace_id=? AND id=?",
                (context.workspace_id, str(source_id or "").strip()),
            ).fetchone()
            if row is None:
                return None
            documents = connection.execute(
                """
                SELECT *
                FROM documents
                WHERE workspace_id=? AND source_id=?
                ORDER BY updated_at DESC, created_at DESC
                """,
                (context.workspace_id, row["id"]),
            ).fetchall()
            return {
                **self._row_to_source_summary(row),
                "documents": [self._row_to_document_summary(item) for item in documents],
            }

    def delete_source(self, workspace_root: str | Path | None, source_id: str) -> dict[str, Any] | None:
        context = self.workspace_context(workspace_root)
        normalized_source_id = str(source_id or "").strip()
        if not normalized_source_id:
            return None

        deleted_document_count = 0
        deleted_chunk_count = 0
        job_id: str | None = None

        with self._lock, self._connect(context) as connection:
            source_row = connection.execute(
                "SELECT * FROM sources WHERE workspace_id=? AND id=?",
                (context.workspace_id, normalized_source_id),
            ).fetchone()
            if source_row is None:
                return None

            source_label = str(source_row["title"] or source_row["canonical_uri"] or normalized_source_id)
            job_id = self._create_job(
                connection,
                workspace_id=context.workspace_id,
                job_type="delete_source",
                summary=f"Deleting source {source_label}",
            )
            self._update_job(
                connection,
                job_id,
                status="running",
                progress=0.2,
                summary=f"Deleting source {source_label}",
            )
            connection.commit()

            document_rows = connection.execute(
                """
                SELECT id
                FROM documents
                WHERE workspace_id=? AND source_id=?
                """,
                (context.workspace_id, normalized_source_id),
            ).fetchall()
            document_ids = [str(row["id"]) for row in document_rows]
            deleted_document_count = len(document_ids)

            chunk_rows = connection.execute(
                """
                SELECT chunks.id
                FROM chunks
                JOIN documents ON chunks.document_id=documents.id
                WHERE documents.workspace_id=? AND documents.source_id=?
                """,
                (context.workspace_id, normalized_source_id),
            ).fetchall()
            chunk_ids = [str(row["id"]) for row in chunk_rows]
            deleted_chunk_count = len(chunk_ids)

            if chunk_ids:
                connection.executemany(
                    "DELETE FROM chunk_fts WHERE chunk_id=?",
                    [(chunk_id,) for chunk_id in chunk_ids],
                )

            connection.execute(
                "DELETE FROM sources WHERE workspace_id=? AND id=?",
                (context.workspace_id, normalized_source_id),
            )
            if job_id is not None:
                self._update_job(
                    connection,
                    job_id,
                    progress=0.7,
                    summary=(
                        f"Deleted source {source_label} with "
                        f"{deleted_document_count} documents and {deleted_chunk_count} chunks"
                    ),
                )
            connection.commit()

        for document_id in document_ids:
            raw_path = context.raw_dir / f"{document_id}.txt"
            if raw_path.exists():
                try:
                    raw_path.unlink()
                except Exception:
                    logger.warning("Failed to remove cached knowledge document %s", raw_path, exc_info=True)

        vector_status = self._rebuild_vector_index(context)
        if job_id is not None:
            with self._lock, self._connect(context) as connection:
                self._update_job(
                    connection,
                    job_id,
                    status="completed",
                    progress=1.0,
                    summary=(
                        f"Deleted source {source_label} with "
                        f"{deleted_document_count} documents and {deleted_chunk_count} chunks "
                        f"and rebuilt vectors using {vector_status.get('provider_name') or 'embeddings'} "
                        f"({vector_status.get('backend') or 'none'})"
                    ),
                    error="",
                    completed=True,
                )
                connection.commit()
        return {
            "source_id": normalized_source_id,
            "deleted_documents": deleted_document_count,
            "deleted_chunks": deleted_chunk_count,
            "job": self.get_job(context.workspace_root, job_id) if job_id else None,
            "vector_status": vector_status,
        }

    def list_jobs(self, workspace_root: str | Path | None = None, limit: int = 20) -> list[dict[str, Any]]:
        context = self.workspace_context(workspace_root)
        limit = max(1, min(int(limit or 20), 200))
        with self._lock, self._connect(context) as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM jobs
                WHERE workspace_id=?
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (context.workspace_id, limit),
            ).fetchall()
            return [self._row_to_job(row) for row in rows]

    def get_job(self, workspace_root: str | Path | None, job_id: str) -> dict[str, Any] | None:
        context = self.workspace_context(workspace_root)
        with self._lock, self._connect(context) as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE workspace_id=? AND id=?",
                (context.workspace_id, str(job_id or "").strip()),
            ).fetchone()
            return self._row_to_job(row) if row is not None else None

    def get_document(self, workspace_root: str | Path | None, document_id: str) -> dict[str, Any] | None:
        context = self.workspace_context(workspace_root)
        with self._lock, self._connect(context) as connection:
            row = connection.execute(
                """
                SELECT
                    documents.*,
                    sources.title AS source_title,
                    sources.source_type AS source_type,
                    sources.canonical_uri AS canonical_uri
                FROM documents
                JOIN sources ON documents.source_id=sources.id
                WHERE documents.workspace_id=? AND documents.id=?
                """,
                (context.workspace_id, str(document_id or "").strip()),
            ).fetchone()
            if row is None:
                return None
            chunks = connection.execute(
                """
                SELECT id, position, text, token_estimate, char_count
                FROM chunks
                WHERE document_id=?
                ORDER BY position ASC
                """,
                (row["id"],),
            ).fetchall()
            return {
                **self._row_to_document_summary(row),
                "content": row["content"] or "",
                "source_title": row["source_title"] or "",
                "source_type": row["source_type"] or "",
                "canonical_uri": row["canonical_uri"] or "",
                "chunks": [
                    {
                        "id": chunk["id"],
                        "position": int(chunk["position"] or 0),
                        "text": chunk["text"] or "",
                        "token_estimate": int(chunk["token_estimate"] or 0),
                        "char_count": int(chunk["char_count"] or 0),
                    }
                    for chunk in chunks
                ],
            }

    def ingest_local_source(
        self,
        workspace_root: str | Path | None,
        raw_path: str,
    ) -> dict[str, Any]:
        context = self.workspace_context(workspace_root)
        resolved_path, documents = extract_local_documents(context.workspace_root, raw_path)
        title = resolved_path.name or str(resolved_path)
        return self._ingest_documents(
            context=context,
            source_type="workspace_path",
            title=title,
            canonical_uri=str(resolved_path),
            root_path=str(resolved_path),
            documents=documents,
            job_type="ingest_local",
            job_summary=f"Ingesting local source {resolved_path}",
        )

    def ingest_url_source(
        self,
        workspace_root: str | Path | None,
        url: str,
        document: ExtractedDocument,
    ) -> dict[str, Any]:
        context = self.workspace_context(workspace_root)
        return self._ingest_documents(
            context=context,
            source_type="url",
            title=document.title or url,
            canonical_uri=str(url),
            root_path=str(url),
            documents=[document],
            job_type="ingest_url",
            job_summary=f"Ingesting URL source {url}",
        )

    def search(
        self,
        workspace_root: str | Path | None,
        query: str,
        limit: int = 10,
    ) -> dict[str, Any]:
        normalized_query = " ".join(str(query or "").split()).strip()
        if not normalized_query:
            return {"query": "", "items": [], "total": 0}

        context = self.workspace_context(workspace_root)
        limit = max(1, min(int(limit or 10), 50))
        lexical_limit = max(limit * 4, 12)
        semantic_limit = max(limit * 4, 12)

        with self._lock, self._connect(context) as connection:
            lexical_ids: list[str] = []
            lexical_rank: dict[str, int] = {}
            fts_query = fts_query_from_text(normalized_query)
            if fts_query:
                lexical_rows = connection.execute(
                    """
                    SELECT chunk_id, bm25(chunk_fts) AS rank
                    FROM chunk_fts
                    WHERE chunk_fts MATCH ?
                    ORDER BY bm25(chunk_fts)
                    LIMIT ?
                    """,
                    (fts_query, lexical_limit),
                ).fetchall()
                lexical_ids = [str(row["chunk_id"]) for row in lexical_rows]
                lexical_rank = {
                    chunk_id: rank
                    for rank, chunk_id in enumerate(lexical_ids, start=1)
                }

        semantic_ids = self._semantic_search(context, normalized_query, limit=semantic_limit)
        semantic_rank = {
            chunk_id: rank for rank, chunk_id in enumerate(semantic_ids, start=1)
        }
        fused_ids = reciprocal_rank_fusion([lexical_ids, semantic_ids])[:limit]
        if not fused_ids:
            return {"query": normalized_query, "items": [], "total": 0}

        with self._lock, self._connect(context) as connection:
            placeholders = ",".join("?" for _ in fused_ids)
            rows = connection.execute(
                f"""
                SELECT
                    chunks.id AS chunk_id,
                    chunks.text AS chunk_text,
                    chunks.position AS chunk_position,
                    documents.id AS document_id,
                    documents.title AS document_title,
                    documents.document_uri AS document_uri,
                    sources.id AS source_id,
                    sources.title AS source_title,
                    sources.source_type AS source_type,
                    sources.canonical_uri AS canonical_uri
                FROM chunks
                JOIN documents ON chunks.document_id=documents.id
                JOIN sources ON documents.source_id=sources.id
                WHERE chunks.id IN ({placeholders})
                """,
                fused_ids,
            ).fetchall()

        rows_by_chunk_id = {str(row["chunk_id"]): row for row in rows}
        items: list[dict[str, Any]] = []
        for fused_rank, chunk_id in enumerate(fused_ids, start=1):
            row = rows_by_chunk_id.get(chunk_id)
            if row is None:
                continue
            items.append(
                {
                    "chunk_id": chunk_id,
                    "document_id": row["document_id"],
                    "document_title": row["document_title"] or row["source_title"] or "Untitled document",
                    "document_uri": row["document_uri"] or "",
                    "source_id": row["source_id"],
                    "source_title": row["source_title"] or "",
                    "source_type": row["source_type"] or "",
                    "canonical_uri": row["canonical_uri"] or "",
                    "snippet": self._normalize_text_snippet(row["chunk_text"] or ""),
                    "chunk_position": int(row["chunk_position"] or 0),
                    "fused_rank": fused_rank,
                    "lexical_rank": lexical_rank.get(chunk_id),
                    "semantic_rank": semantic_rank.get(chunk_id),
                }
            )
        return {"query": normalized_query, "items": items, "total": len(items)}

    def _semantic_search(self, context: WorkspaceContext, query: str, limit: int) -> list[str]:
        snapshot = self._load_vector_snapshot(context)
        if snapshot.matrix.shape[0] == 0 or snapshot.matrix.shape[1] <= 0:
            return []

        query_vector = self.embeddings.embed(query).astype(np.float32)
        if query_vector.ndim != 1 or query_vector.shape[0] <= 0:
            return []

        if snapshot.dim != query_vector.shape[0] or (
            snapshot.provider_name
            and snapshot.provider_name != self.embeddings.provider_name
        ):
            self._rebuild_vector_index(context)
            snapshot = self._load_vector_snapshot(context)
            if snapshot.matrix.shape[0] == 0 or snapshot.dim != query_vector.shape[0]:
                return []

        if (
            snapshot.backend == "hnswlib"
            and hnswlib is not None
            and snapshot.index_path.exists()
            and snapshot.matrix.shape[0] > 0
        ):
            try:
                index = hnswlib.Index(space="cosine", dim=snapshot.dim)
                index.load_index(str(snapshot.index_path), max_elements=max(1, len(snapshot.chunk_ids)))
                index.set_ef(max(self.config.hnsw_ef_search, min(64, len(snapshot.chunk_ids))))
                labels, _distances = index.knn_query(
                    query_vector,
                    k=min(limit, len(snapshot.chunk_ids)),
                )
                return [
                    snapshot.chunk_ids[int(label)]
                    for label in labels[0].tolist()
                    if 0 <= int(label) < len(snapshot.chunk_ids)
                ]
            except Exception:
                logger.warning("Knowledge semantic search falling back to brute-force scoring", exc_info=True)

        scores = np.clip(snapshot.matrix @ query_vector, -1.0, 1.0)
        ranked = np.argsort(scores)[::-1][: min(limit, len(snapshot.chunk_ids))]
        return [snapshot.chunk_ids[int(index)] for index in ranked.tolist()]

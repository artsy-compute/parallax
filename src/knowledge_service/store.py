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
from urllib.parse import unquote

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
_PAGE_TYPES = {
    "home",
    "hub",
    "index",
    "log",
    "source_summary",
    "entity",
    "concept",
    "topic",
    "timeline",
    "comparison",
}
_HUB_PAGE_CONFIGS = [
    {"slug": "sources", "title": "Sources", "page_type": "hub", "child_page_type": "source_summary", "sort_order": 100},
    {"slug": "entities", "title": "Entities", "page_type": "hub", "child_page_type": "entity", "sort_order": 200},
    {"slug": "concepts", "title": "Concepts", "page_type": "hub", "child_page_type": "concept", "sort_order": 300},
    {"slug": "topics", "title": "Topics", "page_type": "hub", "child_page_type": "topic", "sort_order": 400},
    {"slug": "timelines", "title": "Timelines", "page_type": "hub", "child_page_type": "timeline", "sort_order": 500},
    {"slug": "comparisons", "title": "Comparisons", "page_type": "hub", "child_page_type": "comparison", "sort_order": 600},
]
_SPECIAL_PAGE_CONFIGS = [
    {"slug": "index", "title": "Index", "page_type": "index", "sort_order": 900},
    {"slug": "log", "title": "Log", "page_type": "log", "sort_order": 910},
]


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
                page_type TEXT NOT NULL DEFAULT 'topic',
                source_ids_json TEXT NOT NULL DEFAULT '[]',
                aliases_json TEXT NOT NULL DEFAULT '[]',
                updated_from_job_id TEXT NOT NULL DEFAULT '',
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
            CREATE INDEX IF NOT EXISTS idx_pages_workspace_slug
                ON pages(workspace_id, slug);

            CREATE TABLE IF NOT EXISTS wiki_log_entries (
                id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                title TEXT NOT NULL,
                source_id TEXT NOT NULL DEFAULT '',
                job_id TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '',
                touched_page_ids_json TEXT NOT NULL DEFAULT '[]',
                created_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_wiki_log_entries_workspace_created
                ON wiki_log_entries(workspace_id, created_at DESC);
            """
        )
        self._ensure_column(connection, "pages", "page_type", "TEXT NOT NULL DEFAULT 'topic'")
        self._ensure_column(connection, "pages", "source_ids_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(connection, "pages", "aliases_json", "TEXT NOT NULL DEFAULT '[]'")
        self._ensure_column(connection, "pages", "updated_from_job_id", "TEXT NOT NULL DEFAULT ''")
        self._backfill_page_metadata(connection)
        connection.commit()

    @staticmethod
    def _has_column(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(str(row["name"]) == column_name for row in rows)

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_sql: str,
    ) -> None:
        if self._has_column(connection, table_name, column_name):
            return
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

    def _backfill_page_metadata(self, connection: sqlite3.Connection) -> None:
        if self._has_column(connection, "pages", "page_type"):
            connection.execute(
                """
                UPDATE pages
                SET page_type='home'
                WHERE is_home=1 AND (page_type='' OR page_type='topic')
                """
            )
            connection.execute(
                """
                UPDATE pages
                SET page_type='source_summary'
                WHERE is_home=0 AND source_id<>'' AND (page_type='' OR page_type='topic')
                """
            )
        if self._has_column(connection, "pages", "source_ids_json"):
            rows = connection.execute(
                "SELECT id, source_id, source_ids_json FROM pages"
            ).fetchall()
            for row in rows:
                page_id = str(row["id"])
                existing_source_ids = self._decode_string_list(row["source_ids_json"])
                fallback_source_id = str(row["source_id"] or "").strip()
                if fallback_source_id and fallback_source_id not in existing_source_ids:
                    existing_source_ids.append(fallback_source_id)
                connection.execute(
                    "UPDATE pages SET source_ids_json=? WHERE id=?",
                    (self._encode_string_list(existing_source_ids), page_id),
                )
        if self._has_column(connection, "pages", "aliases_json"):
            connection.execute(
                """
                UPDATE pages
                SET aliases_json='[]'
                WHERE aliases_json IS NULL OR aliases_json=''
                """
            )
        if self._has_column(connection, "pages", "updated_from_job_id"):
            connection.execute(
                """
                UPDATE pages
                SET updated_from_job_id=''
                WHERE updated_from_job_id IS NULL
                """
            )

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

    @staticmethod
    def _decode_string_list(value: Any) -> list[str]:
        items: list[str] = []
        raw_items: list[Any]
        if isinstance(value, list):
            raw_items = list(value)
        else:
            compact = str(value or "").strip()
            if not compact:
                return []
            try:
                parsed = json.loads(compact)
                raw_items = list(parsed) if isinstance(parsed, list) else [parsed]
            except Exception:
                raw_items = [part.strip() for part in compact.split(",")]
        for item in raw_items:
            text = str(item or "").strip()
            if text and text not in items:
                items.append(text)
        return items

    @staticmethod
    def _encode_string_list(items: list[str]) -> str:
        normalized: list[str] = []
        for item in items:
            text = str(item or "").strip()
            if text and text not in normalized:
                normalized.append(text)
        return json.dumps(normalized, ensure_ascii=True)

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
        source_ids = self._decode_string_list(row["source_ids_json"]) if "source_ids_json" in row.keys() else []
        fallback_source_id = str(row["source_id"] or "").strip()
        if fallback_source_id and fallback_source_id not in source_ids:
            source_ids.append(fallback_source_id)
        return {
            "id": row["id"],
            "workspace_id": row["workspace_id"],
            "parent_page_id": row["parent_page_id"] or None,
            "source_id": row["source_id"] or "",
            "source_ids": source_ids,
            "page_type": row["page_type"] or ("home" if bool(row["is_home"]) else "topic"),
            "aliases": self._decode_string_list(row["aliases_json"]) if "aliases_json" in row.keys() else [],
            "updated_from_job_id": row["updated_from_job_id"] or "",
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

    @staticmethod
    def _normalize_page_key(value: str) -> str:
        return _SLUG_RE.sub("-", " ".join(str(value or "").split()).strip().lower()).strip("-")

    @staticmethod
    def _normalize_page_type(value: str, fallback: str = "topic") -> str:
        normalized = str(value or "").strip().lower()
        if normalized in _PAGE_TYPES:
            return normalized
        return fallback

    @staticmethod
    def _page_parent_slug(page_type: str) -> str | None:
        normalized_type = str(page_type or "").strip()
        for item in _HUB_PAGE_CONFIGS:
            if item["child_page_type"] == normalized_type:
                return str(item["slug"])
        return None

    @staticmethod
    def _page_sort_order(page_type: str, offset: int = 0) -> int:
        base_sort_orders = {
            "source_summary": 1000,
            "entity": 2000,
            "concept": 3000,
            "topic": 4000,
            "timeline": 5000,
            "comparison": 6000,
            "home": 0,
            "hub": 50,
            "index": 9000,
            "log": 9100,
        }
        return int(base_sort_orders.get(str(page_type or "").strip(), 4000) + max(0, int(offset or 0)))

    @staticmethod
    def _format_event_time(value: float) -> str:
        return time.strftime("%Y-%m-%d %H:%M", time.localtime(float(value or 0)))

    @staticmethod
    def _strip_sources_section(content: str) -> str:
        return re.sub(r"\n## Sources\s*\n(?:.|\n)*\Z", "", str(content or "").rstrip(), flags=re.IGNORECASE)

    def _append_sources_section(self, content: str, source_rows: list[sqlite3.Row | dict[str, Any]]) -> str:
        normalized_content = self._strip_sources_section(content)
        items: list[str] = []
        for row in source_rows:
            title = str(row["title"] or row["canonical_uri"] or "Source").strip()
            canonical_uri = str(row["canonical_uri"] or "").strip()
            entry = f"- [{title}]({canonical_uri})" if canonical_uri else f"- {title}"
            if entry not in items:
                items.append(entry)
        if not items:
            return normalized_content.strip()
        return (
            normalized_content.rstrip()
            + "\n\n## Sources\n"
            + "\n".join(items)
        ).strip()

    @staticmethod
    def _extract_json_value(text: str) -> Any:
        compact = str(text or "").strip()
        if not compact:
            raise ValueError("No JSON payload returned")
        fenced_match = re.search(r"```(?:json)?\s*(.+?)\s*```", compact, flags=re.DOTALL | re.IGNORECASE)
        if fenced_match:
            compact = fenced_match.group(1).strip()
        decoder = json.JSONDecoder()
        for index, char in enumerate(compact):
            if char not in "{[":
                continue
            try:
                value, _end = decoder.raw_decode(compact[index:])
                return value
            except Exception:
                continue
        return json.loads(compact)

    def _load_source_rows_by_ids(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        source_ids: list[str],
    ) -> list[sqlite3.Row]:
        normalized_ids = [str(source_id or "").strip() for source_id in source_ids if str(source_id or "").strip()]
        if not normalized_ids:
            return []
        placeholders = ",".join("?" for _ in normalized_ids)
        return connection.execute(
            f"""
            SELECT *
            FROM sources
            WHERE workspace_id=? AND id IN ({placeholders})
            ORDER BY updated_at DESC, created_at DESC
            """,
            [workspace_id, *normalized_ids],
        ).fetchall()

    def _load_maintainable_pages_catalog(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT *
            FROM pages
            WHERE workspace_id=?
              AND page_type NOT IN ('home', 'hub', 'index', 'log')
            ORDER BY updated_at DESC, sort_order ASC, title ASC
            """,
            (workspace_id,),
        ).fetchall()
        return [self._row_to_page_summary(row) for row in rows]

    def _find_existing_page_match(
        self,
        pages: list[dict[str, Any]],
        *,
        title: str,
        aliases: list[str] | None = None,
        page_type: str | None = None,
    ) -> dict[str, Any] | None:
        desired_key = self._normalize_page_key(title)
        alias_keys = {self._normalize_page_key(item) for item in list(aliases or []) if str(item or "").strip()}
        if desired_key:
            alias_keys.add(desired_key)
        normalized_page_type = self._normalize_page_type(page_type or "", fallback="")
        for page in pages:
            if normalized_page_type and page.get("page_type") == "source_summary" and normalized_page_type != "source_summary":
                continue
            page_keys = {
                self._normalize_page_key(str(page.get("title") or "")),
                *[self._normalize_page_key(item) for item in list(page.get("aliases") or [])],
            }
            if alias_keys & page_keys:
                return page
        return None

    def _plan_source_page_updates(
        self,
        *,
        source_row: sqlite3.Row,
        source_material: str,
        existing_pages: list[dict[str, Any]],
        generation_settings: dict[str, Any],
    ) -> dict[str, Any]:
        catalog_lines = [
            f"- {page['title']} | type={page.get('page_type') or 'topic'} | summary={page.get('summary') or ''}"
            for page in existing_pages[:60]
        ]
        user_prompt = "\n\n".join(
            [
                "Plan how this new source should maintain the wiki.",
                "Return strict JSON only. Do not include markdown fences or commentary.",
                "The JSON schema is:",
                json.dumps(
                    {
                        "page_updates": [
                            {
                                "title": "Page title",
                                "page_type": "entity|concept|topic|timeline|comparison",
                                "action": "create|update",
                                "reason": "Why this page should be touched",
                                "summary": "One-line description of the page after this ingest",
                                "aliases": ["Optional alternate titles"],
                            }
                        ],
                        "contradictions": ["Important tension or contradiction introduced by the source"],
                        "open_questions": ["Question that remains unresolved after reading the source"],
                    },
                    ensure_ascii=True,
                ),
                "Rules:",
                "- Choose only pages materially worth creating or updating.",
                "- Prefer 2 to 8 page updates.",
                "- Reuse existing pages when a topic clearly overlaps the current wiki.",
                "- Use page_type values only from entity, concept, topic, timeline, comparison.",
                f"Source title: {source_row['title'] or source_row['canonical_uri'] or 'Untitled source'}",
                f"Source canonical URI: {source_row['canonical_uri'] or ''}",
                "Existing wiki pages:",
                "\n".join(catalog_lines) if catalog_lines else "(none)",
                "Source material:",
                source_material,
            ]
        )
        try:
            raw_plan = self._call_generation_provider(
                settings=generation_settings,
                system_prompt=(
                    "You are a wiki ingest planner. "
                    "Given a new source and the current wiki catalog, decide which durable pages should be created or updated. "
                    "Return strict JSON only."
                ),
                user_prompt=user_prompt,
                max_tokens=1200,
            )
            plan = self._extract_json_value(raw_plan)
        except Exception:
            logger.warning("Failed to generate structured wiki ingest plan; using fallback plan", exc_info=True)
            plan = {}
        raw_updates = list(plan.get("page_updates") or []) if isinstance(plan, dict) else []
        page_updates: list[dict[str, Any]] = []
        seen_titles: set[str] = set()
        for item in raw_updates:
            if not isinstance(item, dict):
                continue
            title = self._normalize_page_title(str(item.get("title") or ""))
            key = self._normalize_page_key(title)
            if not key or key in seen_titles:
                continue
            seen_titles.add(key)
            page_updates.append(
                {
                    "title": title,
                    "page_type": self._normalize_page_type(item.get("page_type"), fallback="topic"),
                    "action": str(item.get("action") or "update").strip().lower() or "update",
                    "reason": str(item.get("reason") or "").strip(),
                    "summary": str(item.get("summary") or "").strip(),
                    "aliases": self._decode_string_list(item.get("aliases")),
                }
            )
        return {
            "page_updates": page_updates,
            "contradictions": self._decode_string_list(plan.get("contradictions") if isinstance(plan, dict) else []),
            "open_questions": self._decode_string_list(plan.get("open_questions") if isinstance(plan, dict) else []),
        }

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
            "You are maintaining a persistent, compounding markdown wiki from curated sources. "
            "This is not a one-off RAG answer. The wiki should accumulate knowledge over time, "
            "sit between raw sources and future questions, and become more useful after each ingest. "
            "Write with the mindset that the wiki is the durable artifact: structured, interlinked, "
            "and meant to be revised as new information arrives. "
            "Using only the provided source material, extract durable knowledge, preserve important nuance, "
            "identify tensions or contradictions when present, and mention relationships to other topics when justified. "
            "Prefer clear markdown with strong headings, concrete details, and useful internal cross-references over shallow summarization. "
            "Do not invent facts, do not mention missing context, and do not describe yourself or the prompt."
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
                "Create the source summary page for this source inside a persistent wiki.",
                f"Preferred title: {source_title}",
                f"Canonical URI: {source_row['canonical_uri'] or ''}",
                "This page should summarize the source itself while also surfacing the entities, concepts, claims, timelines, comparisons, contradictions, and open questions that should propagate into the wider wiki.",
                "Write explicit markdown headings.",
                "Start with `## Overview`, then `## Key Details`, then `## Important Notes`.",
                "Include concise bullets or subsections for important entities, concepts, timelines, claims, comparisons, contradictions, and open questions when supported by the source.",
                "Do not use the raw URL or file path as the page title if a better topic/title can be inferred from the text.",
                "Source material:",
                source_material,
            ]
        )
        content = self._append_sources_section(
            self._call_generation_provider(
                settings=generation_settings,
                system_prompt=self._wiki_system_prompt(),
                user_prompt=user_prompt,
                max_tokens=1600,
            ),
            [source_row],
        )
        summary = self._extract_summary_markdown(content)
        return {
            "title": source_title,
            "slug": self._slugify(source_title),
            "summary": summary,
            "content": content,
            "source_id": str(source_row["id"]),
            "source_ids": [str(source_row["id"])],
            "aliases": [],
            "page_type": "source_summary",
            "sort_order": int(sort_order),
        }

    def _generate_maintained_page_payload(
        self,
        *,
        page_title: str,
        page_type: str,
        page_plan: dict[str, Any],
        source_row: sqlite3.Row,
        source_material: str,
        generation_settings: dict[str, Any],
        existing_page: dict[str, Any] | None,
        existing_page_source_rows: list[sqlite3.Row],
        related_pages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        related_page_lines = [
            f"- {page['title']} | type={page.get('page_type') or 'topic'} | summary={page.get('summary') or ''}"
            for page in related_pages[:12]
            if self._normalize_page_key(page.get("title")) != self._normalize_page_key(page_title)
        ]
        prompt_lines = [
            f"Maintain the markdown wiki page `{page_title}`.",
            f"Page type: {page_type}",
            f"Reason to touch this page now: {page_plan.get('reason') or 'The new source materially affects this topic.'}",
            f"Target page summary: {page_plan.get('summary') or ''}",
            f"New source title: {source_row['title'] or source_row['canonical_uri'] or ''}",
            f"New source canonical URI: {source_row['canonical_uri'] or ''}",
            "Treat this as a persistent knowledge-base page. Preserve useful existing knowledge, integrate the new source carefully, and explicitly note contradictions or unresolved questions when the source introduces them.",
            "Write markdown only. Prefer durable structure over chatty prose.",
            "Use headings such as `## Overview`, `## Key Details`, and `## Important Notes`, then add more sections if the material benefits from them.",
            "Mention related wiki topics naturally when warranted so internal links can be added later.",
            "Do not invent facts.",
        ]
        if existing_page and str(existing_page.get("content") or "").strip():
            prompt_lines.extend(
                [
                    "Existing page content:",
                    str(existing_page.get("content") or ""),
                ]
            )
        if existing_page_source_rows:
            prompt_lines.extend(
                [
                    "Existing page sources:",
                    "\n".join(
                        f"- {row['title'] or row['canonical_uri'] or 'Source'} ({row['canonical_uri'] or ''})"
                        for row in existing_page_source_rows
                    ),
                ]
            )
        if related_page_lines:
            prompt_lines.extend(
                [
                    "Related wiki pages already present:",
                    "\n".join(related_page_lines),
                ]
            )
        prompt_lines.extend(
            [
                "New source material:",
                source_material,
            ]
        )
        content = self._call_generation_provider(
            settings=generation_settings,
            system_prompt=self._wiki_system_prompt(),
            user_prompt="\n\n".join(prompt_lines),
            max_tokens=1800,
        )
        source_ids = self._decode_string_list(existing_page.get("source_ids") if existing_page else [])
        source_id = str(source_row["id"])
        if source_id not in source_ids:
            source_ids.append(source_id)
        content = self._append_sources_section(content, [*existing_page_source_rows, source_row])
        summary = self._extract_summary_markdown(content)
        return {
            "title": page_title,
            "slug": self._slugify(page_title),
            "summary": summary,
            "content": content,
            "source_id": "",
            "source_ids": source_ids,
            "aliases": self._decode_string_list(page_plan.get("aliases")),
            "page_type": page_type,
        }

    def _generate_home_page_payload(
        self,
        all_pages: list[dict[str, Any]],
        *,
        generation_settings: dict[str, Any],
    ) -> dict[str, str]:
        page_summaries = [
            f"- {page['title']} | type={page.get('page_type') or 'topic'} | summary={page.get('summary') or 'Generated wiki page'}"
            for page in all_pages
            if page.get("page_type") not in {"home", "hub", "index", "log"}
        ]
        home_prompt = "\n\n".join(
            [
                "Create or refresh the homepage for this knowledge wiki.",
                "Treat the wiki as a persistent, compounding knowledge artifact rather than a temporary retrieval layer.",
                "The homepage should orient the reader, summarize the strongest themes across pages, and make the wiki feel like a maintained knowledge base.",
                "Include clear navigation, a synthesis of the current collection, and mention where important questions or tensions remain open.",
                "Do not reduce the homepage to a thin introduction; it should be a substantive overview of what the collection currently knows.",
                "Pages currently in the wiki:",
                "\n".join(page_summaries) if page_summaries else "(none)",
            ]
        )
        home_content = self._call_generation_provider(
            settings=generation_settings,
            system_prompt=self._wiki_system_prompt(),
            user_prompt=home_prompt,
            max_tokens=1400,
        )
        return {
            "title": "Knowledge",
            "slug": "knowledge",
            "summary": self._extract_summary_markdown(home_content),
            "content": home_content,
        }

    def _render_hub_page_content(self, hub_title: str, child_pages: list[dict[str, Any]]) -> str:
        lines = [f"# {hub_title}", "", f"Pages in this section: {len(child_pages)}", ""]
        if not child_pages:
            lines.append("No pages yet.")
        else:
            for page in child_pages:
                lines.append(
                    f"- [{page['title']}](/knowledge/{page['id']})"
                    + (f" - {page.get('summary')}" if str(page.get("summary") or "").strip() else "")
                )
        return "\n".join(lines).strip()

    def _render_index_page_content(self, pages: list[dict[str, Any]]) -> str:
        sections: list[str] = ["# Index", "", "A catalog of the current wiki pages grouped by type.", ""]
        page_type_titles = {
            "source_summary": "Sources",
            "entity": "Entities",
            "concept": "Concepts",
            "topic": "Topics",
            "timeline": "Timelines",
            "comparison": "Comparisons",
        }
        for page_type, section_title in page_type_titles.items():
            items = [page for page in pages if page.get("page_type") == page_type]
            if not items:
                continue
            sections.extend([f"## {section_title}", ""])
            for page in items:
                line = f"- [{page['title']}](/knowledge/{page['id']})"
                if str(page.get("summary") or "").strip():
                    line += f" - {page['summary']}"
                sections.append(line)
            sections.append("")
        return "\n".join(section for section in sections if section is not None).strip()

    def _render_log_page_content(self, entries: list[sqlite3.Row]) -> str:
        lines = ["# Log", "", "Chronological record of wiki maintenance activity.", ""]
        if not entries:
            lines.append("No log entries yet.")
            return "\n".join(lines).strip()
        for entry in entries:
            lines.extend(
                [
                    f"## [{self._format_event_time(entry['created_at'])}] {entry['event_type']} | {entry['title']}",
                    str(entry["details"] or "").strip() or "- No details recorded.",
                    "",
                ]
            )
        return "\n".join(lines).strip()

    def _append_log_entry(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        event_type: str,
        title: str,
        source_id: str,
        job_id: str,
        details: str,
        touched_page_ids: list[str],
    ) -> None:
        connection.execute(
            """
            INSERT INTO wiki_log_entries (
                id, workspace_id, event_type, title, source_id, job_id, details, touched_page_ids_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._new_id("log"),
                workspace_id,
                event_type,
                title,
                source_id,
                job_id,
                details.strip(),
                self._encode_string_list(touched_page_ids),
                self._now(),
            ),
        )

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
                        rf"(?<!\[)(?<!\]\()(?<!/knowledge/)(?<![\w/]){re.escape(title)}(?![\w])(?!\]\()"
                    )
                    next_line = pattern.sub(f"[{title}](/knowledge/{page_id})", next_line)
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

    def _load_page_payloads(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT *
            FROM pages
            WHERE workspace_id=?
            ORDER BY is_home DESC, sort_order ASC, title ASC
            """,
            (workspace_id,),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = self._row_to_page_summary(row)
            item["content"] = row["content"] or ""
            items.append(item)
        return items

    def _upsert_page_record(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        payload: dict[str, Any],
        parent_page_id: str | None,
        is_home: bool = False,
        job_id: str = "",
    ) -> dict[str, Any]:
        slug = str(payload.get("slug") or self._slugify(str(payload.get("title") or ""))).strip()
        existing_row = connection.execute(
            """
            SELECT *
            FROM pages
            WHERE workspace_id=? AND slug=?
            ORDER BY is_home DESC, updated_at DESC
            LIMIT 1
            """,
            (workspace_id, slug),
        ).fetchone()
        now = self._now()
        page_type = self._normalize_page_type(
            payload.get("page_type"),
            fallback="home" if is_home else "topic",
        )
        source_ids = self._decode_string_list(payload.get("source_ids"))
        if existing_row is not None:
            existing_source_ids = self._decode_string_list(existing_row["source_ids_json"])
            for source_id in existing_source_ids:
                if source_id not in source_ids:
                    source_ids.append(source_id)
        source_id = str(payload.get("source_id") or "").strip()
        if not source_id and page_type == "source_summary":
            source_id = source_ids[0] if source_ids else ""
        aliases = self._decode_string_list(payload.get("aliases"))
        if existing_row is not None:
            existing_aliases = self._decode_string_list(existing_row["aliases_json"])
            for alias in existing_aliases:
                if alias not in aliases:
                    aliases.append(alias)
        sort_order = int(
            payload.get("sort_order")
            if payload.get("sort_order") is not None
            else (existing_row["sort_order"] if existing_row is not None else self._page_sort_order(page_type))
        )
        title = self._normalize_page_title(str(payload.get("title") or existing_row["title"] if existing_row is not None else "Untitled page"))
        summary = str(payload.get("summary") or "").strip()
        content = str(payload.get("content") or "").strip()
        parent_value = None if is_home else parent_page_id
        if existing_row is None:
            page_id = str(payload.get("id") or self._new_id("page"))
            connection.execute(
                """
                INSERT INTO pages (
                    id, workspace_id, parent_page_id, source_id, page_type, source_ids_json, aliases_json,
                    updated_from_job_id, title, slug, summary, content, sort_order, is_home, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?)
                """,
                (
                    page_id,
                    workspace_id,
                    parent_value,
                    source_id,
                    page_type,
                    self._encode_string_list(source_ids),
                    self._encode_string_list(aliases),
                    str(job_id or ""),
                    title,
                    slug,
                    summary,
                    content,
                    sort_order,
                    1 if is_home else 0,
                    now,
                    now,
                ),
            )
        else:
            page_id = str(existing_row["id"])
            connection.execute(
                """
                UPDATE pages
                SET parent_page_id=?, source_id=?, page_type=?, source_ids_json=?, aliases_json=?,
                    updated_from_job_id=?, title=?, slug=?, summary=?, content=?, sort_order=?, is_home=?,
                    status='ready', updated_at=?
                WHERE id=?
                """,
                (
                    parent_value,
                    source_id,
                    page_type,
                    self._encode_string_list(source_ids),
                    self._encode_string_list(aliases),
                    str(job_id or ""),
                    title,
                    slug,
                    summary,
                    content,
                    sort_order,
                    1 if is_home else 0,
                    now,
                    page_id,
                ),
            )
        row = connection.execute("SELECT * FROM pages WHERE id=?", (page_id,)).fetchone()
        item = self._row_to_page_summary(row)
        item["content"] = row["content"] or ""
        return item

    def _ensure_wiki_scaffold(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        job_id: str,
    ) -> dict[str, Any]:
        home = self._upsert_page_record(
            connection,
            workspace_id=workspace_id,
            payload={
                "title": "Knowledge",
                "slug": "knowledge",
                "summary": "",
                "content": "# Knowledge",
                "page_type": "home",
                "sort_order": 0,
            },
            parent_page_id=None,
            is_home=True,
            job_id=job_id,
        )
        hubs: dict[str, dict[str, Any]] = {}
        for config in _HUB_PAGE_CONFIGS:
            hubs[str(config["slug"])] = self._upsert_page_record(
                connection,
                workspace_id=workspace_id,
                payload={
                    "title": config["title"],
                    "slug": config["slug"],
                    "summary": "",
                    "content": f"# {config['title']}",
                    "page_type": config["page_type"],
                    "sort_order": int(config["sort_order"]),
                },
                parent_page_id=str(home["id"]),
                job_id=job_id,
            )
        special_pages: dict[str, dict[str, Any]] = {}
        for config in _SPECIAL_PAGE_CONFIGS:
            special_pages[str(config["slug"])] = self._upsert_page_record(
                connection,
                workspace_id=workspace_id,
                payload={
                    "title": config["title"],
                    "slug": config["slug"],
                    "summary": "",
                    "content": f"# {config['title']}",
                    "page_type": config["page_type"],
                    "sort_order": int(config["sort_order"]),
                },
                parent_page_id=str(home["id"]),
                job_id=job_id,
            )
        return {"home": home, "hubs": hubs, "special_pages": special_pages}

    def _refresh_special_pages(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        generation_settings: dict[str, Any],
        scaffold: dict[str, Any],
        job_id: str,
    ) -> None:
        all_pages = self._load_page_payloads(connection, workspace_id=workspace_id)
        home_page = dict(scaffold["home"])
        hub_pages = dict(scaffold["hubs"])
        special_pages = dict(scaffold["special_pages"])
        pages_by_type: dict[str, list[dict[str, Any]]] = {}
        for page in all_pages:
            page_type = str(page.get("page_type") or "topic")
            items = pages_by_type.get(page_type) or []
            items.append(page)
            pages_by_type[page_type] = items

        for config in _HUB_PAGE_CONFIGS:
            slug = str(config["slug"])
            child_page_type = str(config["child_page_type"])
            hub_page = hub_pages[slug]
            child_pages = sorted(
                pages_by_type.get(child_page_type) or [],
                key=lambda page: (int(page.get("sort_order") or 0), str(page.get("title") or "").lower()),
            )
            self._upsert_page_record(
                connection,
                workspace_id=workspace_id,
                payload={
                    "id": hub_page["id"],
                    "title": hub_page["title"],
                    "slug": hub_page["slug"],
                    "summary": f"{len(child_pages)} pages",
                    "content": self._render_hub_page_content(hub_page["title"], child_pages),
                    "page_type": "hub",
                    "sort_order": hub_page["sort_order"],
                },
                parent_page_id=str(home_page["id"]),
                job_id=job_id,
            )

        maintainable_pages = [
            page for page in self._load_page_payloads(connection, workspace_id=workspace_id)
            if page.get("page_type") not in {"home", "hub", "index", "log"}
        ]
        index_page = special_pages["index"]
        self._upsert_page_record(
            connection,
            workspace_id=workspace_id,
            payload={
                "id": index_page["id"],
                "title": index_page["title"],
                "slug": index_page["slug"],
                "summary": f"{len(maintainable_pages)} pages indexed",
                "content": self._render_index_page_content(maintainable_pages),
                "page_type": "index",
                "sort_order": index_page["sort_order"],
            },
            parent_page_id=str(home_page["id"]),
            job_id=job_id,
        )
        log_entries = connection.execute(
            """
            SELECT *
            FROM wiki_log_entries
            WHERE workspace_id=?
            ORDER BY created_at DESC
            LIMIT 200
            """,
            (workspace_id,),
        ).fetchall()
        log_page = special_pages["log"]
        self._upsert_page_record(
            connection,
            workspace_id=workspace_id,
            payload={
                "id": log_page["id"],
                "title": log_page["title"],
                "slug": log_page["slug"],
                "summary": f"{len(log_entries)} recent events",
                "content": self._render_log_page_content(log_entries),
                "page_type": "log",
                "sort_order": log_page["sort_order"],
            },
            parent_page_id=str(home_page["id"]),
            job_id=job_id,
        )
        refreshed_pages = self._load_page_payloads(connection, workspace_id=workspace_id)
        home_payload = self._generate_home_page_payload(refreshed_pages, generation_settings=generation_settings)
        self._upsert_page_record(
            connection,
            workspace_id=workspace_id,
            payload={
                "id": home_page["id"],
                "title": home_payload["title"],
                "slug": home_payload["slug"],
                "summary": home_payload["summary"],
                "content": home_payload["content"],
                "page_type": "home",
                "sort_order": 0,
            },
            parent_page_id=None,
            is_home=True,
            job_id=job_id,
        )

    def _relink_all_pages(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
    ) -> None:
        all_pages = self._load_page_payloads(connection, workspace_id=workspace_id)
        linked_pages = self._apply_page_cross_links(all_pages)
        now = self._now()
        for page in linked_pages:
            current = next((item for item in all_pages if str(item["id"]) == str(page["id"])), None)
            if current is None:
                continue
            if str(current.get("content") or "") == str(page.get("content") or ""):
                continue
            connection.execute(
                """
                UPDATE pages
                SET content=?, summary=?, updated_at=?
                WHERE id=?
                """,
                (
                    str(page.get("content") or ""),
                    self._extract_summary_markdown(str(page.get("content") or "")),
                    now,
                    str(page["id"]),
                ),
            )

    @staticmethod
    def _extract_internal_link_targets(content: str) -> list[str]:
        return [
            unquote(match.group(1)).strip()
            for match in re.finditer(r"/knowledge(?:\?page=|/)([^)\s&#/]+)", str(content or ""))
            if unquote(match.group(1)).strip()
        ]

    def _plan_lint_followups(
        self,
        *,
        pages: list[dict[str, Any]],
        heuristic_findings: dict[str, list[str]],
        generation_settings: dict[str, Any],
    ) -> dict[str, Any]:
        page_lines = [
            f"- {page['title']} | type={page.get('page_type') or 'topic'} | summary={page.get('summary') or ''}"
            for page in pages[:60]
        ]
        heuristic_lines = [
            f"{key}: {', '.join(values[:8]) if values else '(none)'}"
            for key, values in heuristic_findings.items()
        ]
        user_prompt = "\n\n".join(
            [
                "Lint this persistent markdown wiki and suggest the highest-value maintenance follow-ups.",
                "Return strict JSON only. Do not include markdown fences or commentary.",
                "The JSON schema is:",
                json.dumps(
                    {
                        "summary": "Two concise sentences about the current wiki health.",
                        "contradiction_candidates": ["Potential contradiction or tension worth checking"],
                        "missing_pages": ["Important page that likely should exist"],
                        "data_gaps": ["Gap that could be closed with more sources or clarification"],
                    },
                    ensure_ascii=True,
                ),
                "Rules:",
                "- Focus on durable maintenance work, not stylistic edits.",
                "- Prefer 0 to 5 items per list.",
                "- Only name pages or gaps that are materially justified by the current wiki state.",
                "Current wiki pages:",
                "\n".join(page_lines) if page_lines else "(none)",
                "Heuristic findings:",
                "\n".join(heuristic_lines) if heuristic_lines else "(none)",
            ]
        )
        try:
            raw_plan = self._call_generation_provider(
                settings=generation_settings,
                system_prompt=(
                    "You are a wiki maintenance reviewer. "
                    "Given a current wiki catalog and heuristic lint findings, identify the most important contradictions, "
                    "missing pages, and data gaps. Return strict JSON only."
                ),
                user_prompt=user_prompt,
                max_tokens=1000,
            )
            plan = self._extract_json_value(raw_plan)
        except Exception:
            logger.warning("Failed to generate wiki lint follow-ups; using heuristic-only report", exc_info=True)
            plan = {}
        return {
            "summary": str(plan.get("summary") or "").strip(),
            "contradiction_candidates": self._decode_string_list(
                plan.get("contradiction_candidates") if isinstance(plan, dict) else []
            ),
            "missing_pages": self._decode_string_list(plan.get("missing_pages") if isinstance(plan, dict) else []),
            "data_gaps": self._decode_string_list(plan.get("data_gaps") if isinstance(plan, dict) else []),
        }

    def _run_wiki_lint(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        generation_settings: dict[str, Any],
    ) -> dict[str, Any]:
        maintainable_pages = [
            page
            for page in self._load_page_payloads(connection, workspace_id=workspace_id)
            if page.get("page_type") not in {"home", "hub", "index", "log"}
        ]
        if not maintainable_pages:
            return {
                "summary": "The wiki has no maintainable pages yet. Ingest sources and generate the wiki before running lint.",
                "orphan_pages": [],
                "stale_pages": [],
                "missing_cross_references": [],
                "broken_links": [],
                "missing_provenance": [],
                "contradiction_candidates": [],
                "missing_pages": [],
                "data_gaps": [],
                "report_markdown": "# Wiki Maintenance Check\n\nThe wiki has no maintainable pages yet.",
            }

        page_by_id = {str(page["id"]): page for page in maintainable_pages}
        inbound_counts = {page_id: 0 for page_id in page_by_id}
        broken_links: list[str] = []
        for page in maintainable_pages:
            seen_targets: set[str] = set()
            for target_id in self._extract_internal_link_targets(str(page.get("content") or "")):
                if target_id in seen_targets:
                    continue
                seen_targets.add(target_id)
                if target_id in inbound_counts:
                    inbound_counts[target_id] += 1
                elif target_id not in broken_links:
                    broken_links.append(target_id)

        orphan_pages = [
            str(page["title"])
            for page in maintainable_pages
            if page.get("page_type") != "source_summary" and inbound_counts.get(str(page["id"]), 0) == 0
        ]

        all_source_ids: list[str] = []
        for page in maintainable_pages:
            for source_id in self._decode_string_list(page.get("source_ids")):
                if source_id not in all_source_ids:
                    all_source_ids.append(source_id)
        source_rows_by_id = {
            str(row["id"]): row
            for row in self._load_source_rows_by_ids(
                connection,
                workspace_id=workspace_id,
                source_ids=all_source_ids,
            )
        }

        stale_pages: list[str] = []
        missing_provenance: list[str] = []
        for page in maintainable_pages:
            source_ids = self._decode_string_list(page.get("source_ids"))
            if not source_ids:
                if page.get("page_type") != "source_summary":
                    missing_provenance.append(str(page["title"]))
                continue
            linked_sources = [source_rows_by_id[source_id] for source_id in source_ids if source_id in source_rows_by_id]
            if not linked_sources:
                missing_provenance.append(f"{page['title']} (missing source records)")
                continue
            latest_source = max(linked_sources, key=lambda row: float(row["updated_at"] or 0))
            if float(latest_source["updated_at"] or 0) > float(page.get("updated_at") or 0) + 5:
                latest_source_title = str(latest_source["title"] or latest_source["canonical_uri"] or latest_source["id"])
                stale_pages.append(f"{page['title']} (newer linked source: {latest_source_title})")

        missing_cross_references: list[str] = []
        for page in maintainable_pages:
            content = str(page.get("content") or "")
            linked_target_ids = set(self._extract_internal_link_targets(content))
            unlinked_titles: list[str] = []
            for other_page in maintainable_pages:
                if str(other_page["id"]) == str(page["id"]):
                    continue
                other_title = str(other_page.get("title") or "").strip()
                if len(other_title) < 4 or str(other_page["id"]) in linked_target_ids:
                    continue
                if re.search(
                    rf"(?<!\[)(?<!\]\()(?<!/knowledge\?page=)(?<![\w/]){re.escape(other_title)}(?![\w])(?!\]\()",
                    content,
                ):
                    unlinked_titles.append(other_title)
            if unlinked_titles:
                missing_cross_references.append(
                    f"{page['title']} -> {', '.join(unlinked_titles[:3])}"
                )

        heuristic_findings = {
            "orphan_pages": orphan_pages,
            "stale_pages": stale_pages,
            "missing_cross_references": missing_cross_references,
            "broken_links": broken_links,
            "missing_provenance": missing_provenance,
        }
        llm_followups = self._plan_lint_followups(
            pages=maintainable_pages,
            heuristic_findings=heuristic_findings,
            generation_settings=generation_settings,
        )
        summary = (
            llm_followups["summary"]
            or f"The wiki currently has {len(orphan_pages)} orphan pages, {len(stale_pages)} stale pages, "
            f"and {len(missing_cross_references)} pages with likely missing cross-references."
        )

        sections = ["# Wiki Maintenance Check", "", "## Summary", "", summary.strip(), ""]
        report_sections = [
            ("Orphan Pages", orphan_pages),
            ("Stale Pages", stale_pages),
            ("Missing Cross-References", missing_cross_references),
            ("Broken Internal Links", broken_links),
            ("Missing Provenance", missing_provenance),
            ("Contradiction Candidates", llm_followups["contradiction_candidates"]),
            ("Missing Pages", llm_followups["missing_pages"]),
            ("Data Gaps", llm_followups["data_gaps"]),
        ]
        for heading, items in report_sections:
            sections.extend([f"## {heading}", ""])
            if items:
                sections.extend([f"- {item}" for item in items])
            else:
                sections.append("- None identified.")
            sections.append("")

        return {
            "summary": summary.strip(),
            "orphan_pages": orphan_pages,
            "stale_pages": stale_pages,
            "missing_cross_references": missing_cross_references,
            "broken_links": broken_links,
            "missing_provenance": missing_provenance,
            "contradiction_candidates": llm_followups["contradiction_candidates"],
            "missing_pages": llm_followups["missing_pages"],
            "data_gaps": llm_followups["data_gaps"],
            "report_markdown": "\n".join(sections).strip(),
        }

    def _maintain_source_in_wiki(
        self,
        connection: sqlite3.Connection,
        *,
        workspace_id: str,
        source_row: sqlite3.Row,
        generation_settings: dict[str, Any],
        scaffold: dict[str, Any],
        job_id: str,
        sort_order: int,
    ) -> dict[str, Any]:
        source_material = self._collect_source_material(
            connection,
            workspace_id=workspace_id,
            source_id=str(source_row["id"]),
        )
        if not source_material:
            raise ValueError("No readable source material was available for wiki maintenance")

        source_page_payload = self._generate_source_page_payload(
            connection,
            workspace_id=workspace_id,
            source_row=source_row,
            generation_settings=generation_settings,
            sort_order=sort_order,
        )
        if source_page_payload is None:
            raise ValueError("Failed to build source summary page")
        source_summary_page = self._upsert_page_record(
            connection,
            workspace_id=workspace_id,
            payload=source_page_payload,
            parent_page_id=str(scaffold["hubs"]["sources"]["id"]),
            job_id=job_id,
        )

        existing_pages = self._load_maintainable_pages_catalog(connection, workspace_id=workspace_id)
        planner_output = self._plan_source_page_updates(
            source_row=source_row,
            source_material=source_material,
            existing_pages=existing_pages,
            generation_settings=generation_settings,
        )
        touched_pages: list[dict[str, Any]] = [source_summary_page]
        for page_plan in planner_output["page_updates"]:
            existing_page = self._find_existing_page_match(
                existing_pages,
                title=str(page_plan.get("title") or ""),
                aliases=self._decode_string_list(page_plan.get("aliases")),
                page_type=str(page_plan.get("page_type") or "topic"),
            )
            existing_page_source_rows = self._load_source_rows_by_ids(
                connection,
                workspace_id=workspace_id,
                source_ids=self._decode_string_list(existing_page.get("source_ids")) if existing_page else [],
            )
            page_type = self._normalize_page_type(page_plan.get("page_type"), fallback="topic")
            maintained_payload = self._generate_maintained_page_payload(
                page_title=self._normalize_page_title(str(page_plan.get("title") or "")),
                page_type=page_type,
                page_plan=page_plan,
                source_row=source_row,
                source_material=source_material,
                generation_settings=generation_settings,
                existing_page=existing_page,
                existing_page_source_rows=existing_page_source_rows,
                related_pages=existing_pages,
            )
            parent_slug = self._page_parent_slug(page_type)
            parent_page_id = str(scaffold["hubs"][parent_slug]["id"]) if parent_slug and parent_slug in scaffold["hubs"] else str(scaffold["home"]["id"])
            maintained_page = self._upsert_page_record(
                connection,
                workspace_id=workspace_id,
                payload={
                    **maintained_payload,
                    "sort_order": self._page_sort_order(page_type, len(touched_pages)),
                },
                parent_page_id=parent_page_id,
                job_id=job_id,
            )
            touched_pages.append(maintained_page)
            existing_pages = [
                page for page in existing_pages
                if self._normalize_page_key(str(page.get("title") or "")) != self._normalize_page_key(maintained_page["title"])
            ]
            existing_pages.append(maintained_page)

        contradiction_lines = [f"- {item}" for item in planner_output["contradictions"]]
        question_lines = [f"- {item}" for item in planner_output["open_questions"]]
        log_lines = [
            f"- Source: [{source_row['title'] or source_row['canonical_uri'] or 'Source'}]({source_row['canonical_uri'] or ''})",
            "- Pages touched:",
            *[
                f"  - [{page['title']}](/knowledge/{page['id']})"
                for page in touched_pages
            ],
        ]
        if contradiction_lines:
            log_lines.extend(["- Contradictions or tensions:", *[f"  {line}" for line in contradiction_lines]])
        if question_lines:
            log_lines.extend(["- Open questions:", *[f"  {line}" for line in question_lines]])
        self._append_log_entry(
            connection,
            workspace_id=workspace_id,
            event_type="ingest",
            title=str(source_row["title"] or source_row["canonical_uri"] or "Source"),
            source_id=str(source_row["id"]),
            job_id=job_id,
            details="\n".join(log_lines),
            touched_page_ids=[str(page["id"]) for page in touched_pages],
        )
        return {
            "source_summary_page_id": str(source_summary_page["id"]),
            "touched_page_ids": [str(page["id"]) for page in touched_pages],
            "contradictions": planner_output["contradictions"],
            "open_questions": planner_output["open_questions"],
        }

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

    def delete_pages(self, workspace_root: str | Path | None = None) -> dict[str, Any]:
        context = self.workspace_context(workspace_root)
        with self._lock, self._connect(context) as connection:
            deleted_page_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM pages WHERE workspace_id=?",
                    (context.workspace_id,),
                ).fetchone()["count"]
            )
            deleted_log_entry_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM wiki_log_entries WHERE workspace_id=?",
                    (context.workspace_id,),
                ).fetchone()["count"]
            )
            job_id = self._create_job(
                connection,
                workspace_id=context.workspace_id,
                job_type="delete_wiki_pages",
                summary="Deleting generated wiki pages",
            )
            self._update_job(
                connection,
                job_id,
                status="running",
                progress=0.2,
                summary="Deleting generated wiki pages and maintenance log entries",
            )
            connection.execute(
                "DELETE FROM wiki_log_entries WHERE workspace_id=?",
                (context.workspace_id,),
            )
            connection.execute(
                "DELETE FROM pages WHERE workspace_id=?",
                (context.workspace_id,),
            )
            self._update_job(
                connection,
                job_id,
                status="completed",
                progress=1.0,
                summary=(
                    f"Deleted {deleted_page_count} wiki pages and "
                    f"{deleted_log_entry_count} wiki log entries"
                ),
                error="",
                completed=True,
            )
            connection.commit()

        return {
            "deleted_pages": deleted_page_count,
            "deleted_log_entries": deleted_log_entry_count,
            "job": self.get_job(context.workspace_root, job_id),
            "pages": self.list_pages(context.workspace_root),
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

            scaffold = self._ensure_wiki_scaffold(
                connection,
                workspace_id=context.workspace_id,
                job_id=job_id,
            )
            total_sources = max(1, len(ready_sources))
            last_source_summary_page_id = str(scaffold["home"]["id"])

            for index, source_row in enumerate(ready_sources, start=1):
                maintenance_result = self._maintain_source_in_wiki(
                    connection,
                    workspace_id=context.workspace_id,
                    source_row=source_row,
                    generation_settings=generation_settings,
                    scaffold=scaffold,
                    job_id=job_id,
                    sort_order=index,
                )
                last_source_summary_page_id = str(
                    maintenance_result.get("source_summary_page_id") or last_source_summary_page_id
                )
                self._update_job(
                    connection,
                    job_id,
                    progress=0.15 + (0.55 * (index / total_sources)),
                    summary=f"Maintained wiki from {index}/{total_sources} ready sources",
                )
                connection.commit()

            maintainable_pages = [
                page for page in self._load_page_payloads(connection, workspace_id=context.workspace_id)
                if page.get("page_type") not in {"home", "hub", "index", "log"}
            ]
            if not maintainable_pages:
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

            self._refresh_special_pages(
                connection,
                workspace_id=context.workspace_id,
                generation_settings=generation_settings,
                scaffold=scaffold,
                job_id=job_id,
            )
            self._relink_all_pages(connection, workspace_id=context.workspace_id)
            connection.commit()

            self._update_job(
                connection,
                job_id,
                status="completed",
                progress=1.0,
                summary=f"Maintained wiki from {len(ready_sources)} ready sources and refreshed index, log, and home pages",
                error="",
                completed=True,
            )
            connection.commit()

        pages_payload = self.list_pages(context.workspace_root)
        return {
            "home_page_id": pages_payload.get("home_page_id"),
            "pages_created": len(pages_payload.get("items") or []),
            "job": self.get_job(context.workspace_root, job_id),
            "pages": pages_payload,
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
            scaffold = self._ensure_wiki_scaffold(
                connection,
                workspace_id=context.workspace_id,
                job_id=job_id,
            )
            maintenance_result = self._maintain_source_in_wiki(
                connection,
                workspace_id=context.workspace_id,
                source_row=source_row,
                generation_settings=generation_settings,
                scaffold=scaffold,
                job_id=job_id,
                sort_order=int(page_row["sort_order"] or 0) or 1,
            )
            self._refresh_special_pages(
                connection,
                workspace_id=context.workspace_id,
                generation_settings=generation_settings,
                scaffold=scaffold,
                job_id=job_id,
            )
            self._relink_all_pages(connection, workspace_id=context.workspace_id)
            connection.commit()

            self._update_job(
                connection,
                job_id,
                status="completed",
                progress=1.0,
                summary=f"Regenerated source-backed wiki content for {source_row['title'] or normalized_page_id}",
                error="",
                completed=True,
            )
            connection.commit()

        selected_page_id = str(maintenance_result.get("source_summary_page_id") or normalized_page_id)
        return {
            "page": self.get_page(context.workspace_root, selected_page_id),
            "job": self.get_job(context.workspace_root, job_id),
            "pages": self.list_pages(context.workspace_root),
        }

    def lint_wiki(
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
            job_id = self._create_job(
                connection,
                workspace_id=context.workspace_id,
                job_type="lint_wiki",
                summary="Running wiki maintenance checks",
            )
            self._update_job(
                connection,
                job_id,
                status="running",
                progress=0.1,
                summary="Scanning wiki pages for maintenance issues",
            )
            connection.commit()

            scaffold = self._ensure_wiki_scaffold(
                connection,
                workspace_id=context.workspace_id,
                job_id=job_id,
            )
            lint_report = self._run_wiki_lint(
                connection,
                workspace_id=context.workspace_id,
                generation_settings=generation_settings,
            )
            self._append_log_entry(
                connection,
                workspace_id=context.workspace_id,
                event_type="lint",
                title="Wiki maintenance check",
                source_id="",
                job_id=job_id,
                details=str(lint_report.get("report_markdown") or "").strip(),
                touched_page_ids=[],
            )
            self._update_job(
                connection,
                job_id,
                progress=0.8,
                summary="Refreshing wiki log after maintenance checks",
            )
            self._refresh_special_pages(
                connection,
                workspace_id=context.workspace_id,
                generation_settings=generation_settings,
                scaffold=scaffold,
                job_id=job_id,
            )
            self._relink_all_pages(connection, workspace_id=context.workspace_id)
            connection.commit()

            self._update_job(
                connection,
                job_id,
                status="completed",
                progress=1.0,
                summary=(
                    f"Wiki lint completed: {len(lint_report.get('orphan_pages') or [])} orphan pages, "
                    f"{len(lint_report.get('stale_pages') or [])} stale pages, "
                    f"{len(lint_report.get('missing_cross_references') or [])} missing cross-reference candidates"
                ),
                error="",
                completed=True,
            )
            connection.commit()

        return {
            "report_markdown": str(lint_report.get("report_markdown") or ""),
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

    def ingest_uploaded_source(
        self,
        workspace_root: str | Path | None,
        filename: str,
        document: ExtractedDocument,
    ) -> dict[str, Any]:
        context = self.workspace_context(workspace_root)
        normalized_filename = Path(str(filename or document.title or "uploaded-document")).name
        canonical_uri = f"upload://{normalized_filename}"
        return self._ingest_documents(
            context=context,
            source_type="uploaded_file",
            title=normalized_filename,
            canonical_uri=canonical_uri,
            root_path=canonical_uri,
            documents=[document],
            job_type="ingest_upload",
            job_summary=f"Ingesting uploaded source {normalized_filename}",
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

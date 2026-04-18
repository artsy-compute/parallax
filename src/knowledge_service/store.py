from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

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

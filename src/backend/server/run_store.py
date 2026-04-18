import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional


class RunStore:
    def __init__(self, db_path: str | None = None):
        if db_path is None:
            db_path = os.environ.get('PARALLAX_RUNS_DB', '/tmp/parallax_runs.sqlite3')
        self.db_path = str(Path(db_path))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    conversation_id TEXT,
                    request_id TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL DEFAULT '',
                    agent_name TEXT NOT NULL DEFAULT 'Workspace Agent',
                    status TEXT NOT NULL DEFAULT 'queued',
                    priority TEXT NOT NULL DEFAULT 'medium',
                    risk_level TEXT NOT NULL DEFAULT 'guarded',
                    requested_by TEXT NOT NULL DEFAULT 'local-user',
                    started_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    duration_ms INTEGER NOT NULL DEFAULT 0,
                    current_step TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    model TEXT NOT NULL DEFAULT '',
                    tool_count INTEGER NOT NULL DEFAULT 0,
                    approval_count INTEGER NOT NULL DEFAULT 0,
                    artifacts_json TEXT NOT NULL DEFAULT '[]',
                    policy_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_runs_conversation_updated_at
                    ON runs(conversation_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_runs_updated_at
                    ON runs(updated_at DESC);
                CREATE TABLE IF NOT EXISTS run_events (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    title TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    position INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_run_events_run_position
                    ON run_events(run_id, position ASC, timestamp ASC);
                CREATE TABLE IF NOT EXISTS run_approvals (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    requested_by TEXT NOT NULL DEFAULT 'system',
                    requested_at REAL NOT NULL,
                    resolved_at REAL,
                    decided_by TEXT NOT NULL DEFAULT '',
                    decision_note TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_run_approvals_run_requested_at
                    ON run_approvals(run_id, requested_at DESC);
                """
            )
            conn.commit()

    @staticmethod
    def _json_load(text: str, fallback: Any) -> Any:
        try:
            return json.loads(str(text or ''))
        except Exception:
            return fallback

    @staticmethod
    def _now() -> float:
        return time.time()

    @classmethod
    def _row_to_summary(cls, row: sqlite3.Row) -> dict[str, Any]:
        started_at = float(row['started_at'] or 0)
        updated_at = float(row['updated_at'] or started_at)
        duration_ms = int(row['duration_ms'] or max(0, int((updated_at - started_at) * 1000)))
        return {
            'id': row['id'],
            'request_id': row['request_id'] or '',
            'title': row['title'] or 'Untitled run',
            'agent_name': row['agent_name'] or 'Workspace Agent',
            'status': row['status'] or 'queued',
            'priority': row['priority'] or 'medium',
            'risk_level': row['risk_level'] or 'guarded',
            'requested_by': row['requested_by'] or 'local-user',
            'started_at': started_at,
            'updated_at': updated_at,
            'duration_ms': duration_ms,
            'current_step': row['current_step'] or '',
            'summary': row['summary'] or '',
            'conversation_id': row['conversation_id'] or '',
            'model': row['model'] or '',
            'tool_count': int(row['tool_count'] or 0),
            'approval_count': int(row['approval_count'] or 0),
        }

    @classmethod
    def _row_to_detail(cls, row: sqlite3.Row, events: list[dict[str, Any]]) -> dict[str, Any]:
        detail = cls._row_to_summary(row)
        detail['artifacts'] = cls._json_load(row['artifacts_json'], [])
        detail['policy'] = cls._json_load(row['policy_json'], {})
        detail['events'] = events
        return detail

    def _load_events(self, conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT id, kind, status, timestamp, title, detail, metadata_json
            FROM run_events
            WHERE run_id=?
            ORDER BY position ASC, timestamp ASC
            """,
            (str(run_id or '').strip(),),
        ).fetchall()
        return [
            {
                'id': row['id'],
                'kind': row['kind'],
                'status': row['status'],
                'timestamp': float(row['timestamp'] or 0),
                'title': row['title'],
                'detail': row['detail'] or '',
                'metadata': self._json_load(row['metadata_json'], {}),
            }
            for row in rows
        ]

    def _load_approvals(self, conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT id, title, detail, status, requested_by, requested_at, resolved_at, decided_by, decision_note
            FROM run_approvals
            WHERE run_id=?
            ORDER BY requested_at DESC
            """,
            (str(run_id or '').strip(),),
        ).fetchall()
        return [
            {
                'id': row['id'],
                'title': row['title'],
                'detail': row['detail'] or '',
                'status': row['status'] or 'pending',
                'requested_by': row['requested_by'] or 'system',
                'requested_at': float(row['requested_at'] or 0),
                'resolved_at': float(row['resolved_at']) if row['resolved_at'] is not None else None,
                'decided_by': row['decided_by'] or '',
                'decision_note': row['decision_note'] or '',
            }
            for row in rows
        ]

    def _pending_approval_count(self, conn: sqlite3.Connection, run_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM run_approvals WHERE run_id=? AND status='pending'",
            (str(run_id or '').strip(),),
        ).fetchone()
        return int(row['count'] or 0) if row is not None else 0

    def create_run(
        self,
        *,
        request_id: str,
        conversation_id: str | None,
        title: str,
        model: str,
        requested_by: str = 'local-user',
        agent_name: str = 'Workspace Agent',
        priority: str = 'medium',
        risk_level: str = 'guarded',
        status: str = 'queued',
        current_step: str = 'Queued',
        summary: str = '',
        policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        now = self._now()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    id, conversation_id, request_id, title, agent_name, status, priority,
                    risk_level, requested_by, started_at, updated_at, duration_ms,
                    current_step, summary, model, tool_count, approval_count,
                    artifacts_json, policy_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '[]', ?)
                """,
                (
                    run_id,
                    str(conversation_id or '').strip(),
                    str(request_id or '').strip(),
                    str(title or '').strip() or 'Untitled run',
                    str(agent_name or '').strip() or 'Workspace Agent',
                    str(status or '').strip() or 'queued',
                    str(priority or '').strip() or 'medium',
                    str(risk_level or '').strip() or 'guarded',
                    str(requested_by or '').strip() or 'local-user',
                    now,
                    now,
                    0,
                    str(current_step or '').strip(),
                    str(summary or '').strip(),
                    str(model or '').strip(),
                    json.dumps(policy or {}, ensure_ascii=False),
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            detail = self._row_to_detail(row, [])
            detail['approvals'] = []
            return detail

    def append_event(
        self,
        run_id: str,
        *,
        kind: str,
        status: str,
        title: str,
        detail: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = self._now()
        with self._lock, self._connect() as conn:
            next_position_row = conn.execute(
                "SELECT COALESCE(MAX(position), -1) + 1 AS next_position FROM run_events WHERE run_id=?",
                (str(run_id or '').strip(),),
            ).fetchone()
            next_position = int(next_position_row['next_position'] or 0)
            conn.execute(
                """
                INSERT INTO run_events (
                    id, run_id, kind, status, timestamp, title, detail, metadata_json, position
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"evt_{uuid.uuid4().hex[:12]}",
                    str(run_id or '').strip(),
                    str(kind or '').strip(),
                    str(status or '').strip(),
                    now,
                    str(title or '').strip(),
                    str(detail or '').strip(),
                    json.dumps(metadata or {}, ensure_ascii=False),
                    next_position,
                ),
            )
            conn.commit()

    def update_run(
        self,
        run_id: str,
        *,
        status: Optional[str] = None,
        current_step: Optional[str] = None,
        summary: Optional[str] = None,
        tool_count: Optional[int] = None,
        approval_count: Optional[int] = None,
        policy: Optional[dict[str, Any]] = None,
        artifacts: Optional[list[dict[str, Any]]] = None,
    ) -> Optional[dict[str, Any]]:
        normalized_run_id = str(run_id or '').strip()
        if not normalized_run_id:
            return None
        now = self._now()
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id=?", (normalized_run_id,)).fetchone()
            if row is None:
                return None
            started_at = float(row['started_at'] or now)
            conn.execute(
                """
                UPDATE runs SET
                    status=?,
                    current_step=?,
                    summary=?,
                    tool_count=?,
                    approval_count=?,
                    policy_json=?,
                    artifacts_json=?,
                    updated_at=?,
                    duration_ms=?
                WHERE id=?
                """,
                (
                    str(status or row['status'] or 'queued'),
                    str(current_step if current_step is not None else row['current_step'] or ''),
                    str(summary if summary is not None else row['summary'] or ''),
                    int(tool_count if tool_count is not None else row['tool_count'] or 0),
                    int(approval_count if approval_count is not None else row['approval_count'] or 0),
                    json.dumps(policy if policy is not None else self._json_load(row['policy_json'], {}), ensure_ascii=False),
                    json.dumps(artifacts if artifacts is not None else self._json_load(row['artifacts_json'], []), ensure_ascii=False),
                    now,
                    max(0, int((now - started_at) * 1000)),
                    normalized_run_id,
                ),
            )
            conn.commit()
            updated = conn.execute("SELECT * FROM runs WHERE id=?", (normalized_run_id,)).fetchone()
            detail = self._row_to_detail(updated, self._load_events(conn, normalized_run_id))
            detail['approvals'] = self._load_approvals(conn, normalized_run_id)
            return detail

    def increment_tool_count(self, run_id: str, delta: int = 1) -> Optional[dict[str, Any]]:
        normalized_run_id = str(run_id or '').strip()
        if not normalized_run_id:
            return None
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT tool_count FROM runs WHERE id=?", (normalized_run_id,)).fetchone()
            if row is None:
                return None
            next_tool_count = max(0, int(row['tool_count'] or 0) + int(delta or 0))
        return self.update_run(normalized_run_id, tool_count=next_tool_count)

    def list_approvals(self, run_id: str) -> list[dict[str, Any]]:
        normalized_run_id = str(run_id or '').strip()
        if not normalized_run_id:
            return []
        with self._lock, self._connect() as conn:
            return self._load_approvals(conn, normalized_run_id)

    def create_approval(
        self,
        run_id: str,
        *,
        title: str,
        detail: str,
        requested_by: str = 'local-user',
    ) -> Optional[dict[str, Any]]:
        normalized_run_id = str(run_id or '').strip()
        if not normalized_run_id:
            return None
        now = self._now()
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id=?", (normalized_run_id,)).fetchone()
            if row is None:
                return None
            approval_id = f"appr_{uuid.uuid4().hex[:12]}"
            conn.execute(
                """
                INSERT INTO run_approvals (
                    id, run_id, title, detail, status, requested_by, requested_at, resolved_at, decided_by, decision_note
                )
                VALUES (?, ?, ?, ?, 'pending', ?, ?, NULL, '', '')
                """,
                (
                    approval_id,
                    normalized_run_id,
                    str(title or '').strip() or 'Approval required',
                    str(detail or '').strip(),
                    str(requested_by or '').strip() or 'local-user',
                    now,
                ),
            )
            pending_count = self._pending_approval_count(conn, normalized_run_id)
            started_at = float(row['started_at'] or now)
            conn.execute(
                """
                UPDATE runs SET
                    status='waiting_for_approval',
                    current_step=?,
                    approval_count=?,
                    updated_at=?,
                    duration_ms=?
                WHERE id=?
                """,
                (
                    str(title or '').strip() or 'Approval required',
                    pending_count,
                    now,
                    max(0, int((now - started_at) * 1000)),
                    normalized_run_id,
                ),
            )
            conn.commit()
            updated = conn.execute("SELECT * FROM runs WHERE id=?", (normalized_run_id,)).fetchone()
            detail_payload = self._row_to_detail(updated, self._load_events(conn, normalized_run_id))
            detail_payload['approvals'] = self._load_approvals(conn, normalized_run_id)
            return detail_payload

    def resolve_approval(
        self,
        approval_id: str,
        *,
        decision: str,
        decided_by: str = 'local-user',
        decision_note: str = '',
    ) -> Optional[dict[str, Any]]:
        normalized_approval_id = str(approval_id or '').strip()
        normalized_decision = str(decision or '').strip().lower()
        if normalized_approval_id == '' or normalized_decision not in {'approved', 'rejected'}:
            return None
        now = self._now()
        with self._lock, self._connect() as conn:
            approval_row = conn.execute(
                "SELECT id, run_id, status FROM run_approvals WHERE id=?",
                (normalized_approval_id,),
            ).fetchone()
            if approval_row is None:
                return None
            conn.execute(
                """
                UPDATE run_approvals SET
                    status=?,
                    resolved_at=?,
                    decided_by=?,
                    decision_note=?
                WHERE id=?
                """,
                (
                    normalized_decision,
                    now,
                    str(decided_by or '').strip() or 'local-user',
                    str(decision_note or '').strip(),
                    normalized_approval_id,
                ),
            )
            run_id = str(approval_row['run_id'] or '').strip()
            run_row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if run_row is None:
                conn.commit()
                return None
            pending_count = self._pending_approval_count(conn, run_id)
            started_at = float(run_row['started_at'] or now)
            next_status = run_row['status']
            next_step = run_row['current_step']
            if pending_count == 0:
                if normalized_decision == 'approved':
                    next_status = 'paused'
                    next_step = 'Approval granted; ready to resume'
                else:
                    next_status = 'cancelled'
                    next_step = 'Approval rejected'
            conn.execute(
                """
                UPDATE runs SET
                    status=?,
                    current_step=?,
                    approval_count=?,
                    updated_at=?,
                    duration_ms=?
                WHERE id=?
                """,
                (
                    next_status,
                    next_step,
                    pending_count,
                    now,
                    max(0, int((now - started_at) * 1000)),
                    run_id,
                ),
            )
            conn.commit()
            updated = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            detail_payload = self._row_to_detail(updated, self._load_events(conn, run_id))
            detail_payload['approvals'] = self._load_approvals(conn, run_id)
            return detail_payload

    def cancel_run(self, run_id: str, *, summary: str = '') -> Optional[dict[str, Any]]:
        return self.update_run(
            run_id,
            status='cancelled',
            current_step='Cancelled',
            summary=summary or 'Run was cancelled by an operator.',
        )

    def resume_run(self, run_id: str, *, summary: str = '') -> Optional[dict[str, Any]]:
        return self.update_run(
            run_id,
            status='queued',
            current_step='Queued for manual retry or resume',
            summary=summary or 'Run was marked ready for resume by an operator.',
        )

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        normalized_run_id = str(run_id or '').strip()
        if not normalized_run_id:
            return None
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id=?", (normalized_run_id,)).fetchone()
            if row is None:
                return None
            detail = self._row_to_detail(row, self._load_events(conn, normalized_run_id))
            detail['approvals'] = self._load_approvals(conn, normalized_run_id)
            return detail

    def get_latest_run_for_conversation(self, conversation_id: str) -> Optional[dict[str, Any]]:
        normalized_conversation_id = str(conversation_id or '').strip()
        if not normalized_conversation_id:
            return None
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM runs
                WHERE conversation_id=?
                ORDER BY updated_at DESC, started_at DESC
                LIMIT 1
                """,
                (normalized_conversation_id,),
            ).fetchone()
            if row is None:
                return None
            detail = self._row_to_detail(row, self._load_events(conn, row['id']))
            detail['approvals'] = self._load_approvals(conn, row['id'])
            return detail

    def list_runs_for_conversation(self, conversation_id: str, limit: int = 100) -> list[dict[str, Any]]:
        normalized_conversation_id = str(conversation_id or '').strip()
        if not normalized_conversation_id:
            return []
        limit = max(1, min(int(limit or 100), 500))
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM runs
                WHERE conversation_id=?
                ORDER BY updated_at DESC, started_at DESC
                LIMIT ?
                """,
                (normalized_conversation_id, limit),
            ).fetchall()
            return [self._row_to_summary(row) for row in rows]

    def list_runs(self, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        limit = max(1, min(int(limit or 100), 500))
        offset = max(0, int(offset or 0))
        with self._lock, self._connect() as conn:
            total_row = conn.execute("SELECT COUNT(*) AS count FROM runs").fetchone()
            counts = {
                'total': int(total_row['count'] or 0),
                'active': int(
                    conn.execute(
                        "SELECT COUNT(*) AS count FROM runs WHERE status IN ('queued', 'running', 'paused', 'waiting_for_approval')"
                    ).fetchone()['count']
                    or 0
                ),
                'waiting_for_approval': int(
                    conn.execute(
                        "SELECT COUNT(*) AS count FROM runs WHERE status='waiting_for_approval'"
                    ).fetchone()['count']
                    or 0
                ),
                'completed': int(
                    conn.execute(
                        "SELECT COUNT(*) AS count FROM runs WHERE status='completed'"
                    ).fetchone()['count']
                    or 0
                ),
            }
            rows = conn.execute(
                """
                SELECT * FROM runs
                ORDER BY updated_at DESC, started_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            items = [self._row_to_summary(row) for row in rows]
            return {
                'counts': counts,
                'items': items,
            }

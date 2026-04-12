import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)

_WORD_RE = re.compile(r"[A-Za-z0-9_\-']+")


class ChatMemoryService:
    """SQLite-backed long-conversation memory for the chat app.

    This is an intentionally simple first implementation:
    - full raw history is stored on disk
    - a compact rolling summary of older turns is maintained
    - lightweight lexical retrieval pulls older relevant snippets
    - the live prompt is rebuilt from summary + retrieved snippets + recent turns

    It is approximate and favors operational simplicity over perfect retrieval quality.
    """

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = os.environ.get('PARALLAX_CHAT_MEMORY_DB', '/tmp/parallax_chat_memory.sqlite3')
        self.db_path = str(Path(db_path))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.recent_message_count = int(os.environ.get('PARALLAX_CHAT_MEMORY_RECENT_MESSAGES', '8'))
        self.summary_message_limit = int(os.environ.get('PARALLAX_CHAT_MEMORY_SUMMARY_MESSAGES', '24'))
        self.summary_char_budget = int(os.environ.get('PARALLAX_CHAT_MEMORY_SUMMARY_CHARS', '3000'))
        self.retrieval_limit = int(os.environ.get('PARALLAX_CHAT_MEMORY_RETRIEVAL_LIMIT', '4'))
        self.retrieval_char_budget = int(os.environ.get('PARALLAX_CHAT_MEMORY_RETRIEVAL_CHARS', '1800'))
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
                CREATE TABLE IF NOT EXISTS conversations (
                    conversation_id TEXT PRIMARY KEY,
                    summary_text TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id TEXT NOT NULL,
                    client_message_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id),
                    UNIQUE(conversation_id, client_message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_conversation_created_at
                    ON messages(conversation_id, created_at, id);
                """
            )

    def _tokenize(self, text: str) -> List[str]:
        return [tok.lower() for tok in _WORD_RE.findall(text or '') if len(tok) >= 3]

    def _compact_text(self, text: str, max_chars: int = 180) -> str:
        text = ' '.join((text or '').split())
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 1].rstrip() + '...'

    def ensure_conversation(self, conversation_id: str) -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO conversations (conversation_id, summary_text, created_at, updated_at)
                VALUES (?, '', ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET updated_at=excluded.updated_at
                """,
                (conversation_id, now, now),
            )
            conn.commit()

    def save_client_messages(self, conversation_id: str, messages: List[Dict]) -> None:
        if not conversation_id or not messages:
            return
        self.ensure_conversation(conversation_id)
        now = time.time()
        rows = []
        for idx, msg in enumerate(messages):
            role = (msg.get('role') or '').strip()
            content = (msg.get('content') or '').strip()
            if role not in {'user', 'assistant', 'system'} or not content:
                continue
            client_message_id = str(msg.get('id') or f'{role}:{idx}:{hash(content)}')
            created_at = float(msg.get('createdAt') or now + idx * 1e-6)
            rows.append((conversation_id, client_message_id, role, content, created_at))
        if not rows:
            return
        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO messages (
                    conversation_id, client_message_id, role, content, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.execute(
                'UPDATE conversations SET updated_at=? WHERE conversation_id=?',
                (time.time(), conversation_id),
            )
            conn.commit()

    def save_assistant_message(self, conversation_id: Optional[str], content: str, request_id: str) -> None:
        if not conversation_id:
            return
        content = (content or '').strip()
        if not content:
            return
        self.ensure_conversation(conversation_id)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO messages (
                    conversation_id, client_message_id, role, content, created_at
                ) VALUES (?, ?, 'assistant', ?, ?)
                """,
                (conversation_id, str(request_id), content, time.time()),
            )
            conn.execute(
                'UPDATE conversations SET updated_at=? WHERE conversation_id=?',
                (time.time(), conversation_id),
            )
            conn.commit()
        self.refresh_summary(conversation_id)

    def _load_messages(self, conversation_id: str) -> List[sqlite3.Row]:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                SELECT client_message_id, role, content, created_at
                FROM messages
                WHERE conversation_id=?
                ORDER BY created_at ASC, id ASC
                """,
                (conversation_id,),
            )
            return list(cur.fetchall())

    def refresh_summary(self, conversation_id: str) -> str:
        rows = self._load_messages(conversation_id)
        older_rows = rows[:-self.recent_message_count] if len(rows) > self.recent_message_count else []
        if not older_rows:
            summary = ''
        else:
            selected = older_rows[-self.summary_message_limit :]
            lines = []
            current_chars = 0
            for row in selected:
                prefix = 'User' if row['role'] == 'user' else 'Assistant' if row['role'] == 'assistant' else 'System'
                line = f'- {prefix}: {self._compact_text(row["content"], 200)}'
                if current_chars + len(line) + 1 > self.summary_char_budget:
                    break
                lines.append(line)
                current_chars += len(line) + 1
            summary = 'Approximate summary of earlier conversation:\n' + '\n'.join(lines)
        with self._lock, self._connect() as conn:
            conn.execute(
                'UPDATE conversations SET summary_text=?, updated_at=? WHERE conversation_id=?',
                (summary, time.time(), conversation_id),
            )
            conn.commit()
        return summary

    def _load_summary(self, conversation_id: str) -> str:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                'SELECT summary_text FROM conversations WHERE conversation_id=?',
                (conversation_id,),
            )
            row = cur.fetchone()
            return row['summary_text'] if row and row['summary_text'] else ''

    def _retrieve_relevant_snippets(self, conversation_id: str, query: str) -> List[str]:
        query_tokens = set(self._tokenize(query))
        if not query_tokens:
            return []
        rows = self._load_messages(conversation_id)
        if len(rows) <= self.recent_message_count:
            return []
        older_rows = rows[:-self.recent_message_count]
        scored: List[Tuple[float, str]] = []
        total = len(older_rows)
        for idx, row in enumerate(older_rows):
            tokens = set(self._tokenize(row['content']))
            if not tokens:
                continue
            overlap = len(query_tokens & tokens)
            if overlap <= 0:
                continue
            recency_boost = (idx + 1) / max(total, 1)
            score = overlap + 0.25 * recency_boost
            prefix = 'User' if row['role'] == 'user' else 'Assistant' if row['role'] == 'assistant' else 'System'
            snippet = f'{prefix}: {self._compact_text(row["content"], 220)}'
            scored.append((score, snippet))
        scored.sort(key=lambda item: item[0], reverse=True)
        snippets: List[str] = []
        current_chars = 0
        for _, snippet in scored:
            if snippet in snippets:
                continue
            if len(snippets) >= self.retrieval_limit:
                break
            if current_chars + len(snippet) + 1 > self.retrieval_char_budget:
                break
            snippets.append(snippet)
            current_chars += len(snippet) + 1
        return snippets

    def list_conversations(self, limit: int = 50) -> List[Dict]:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                SELECT c.conversation_id, c.summary_text, c.created_at, c.updated_at,
                       COUNT(m.id) AS message_count,
                       COALESCE(
                         (
                           SELECT content FROM messages m2
                           WHERE m2.conversation_id = c.conversation_id
                           ORDER BY m2.created_at DESC, m2.id DESC
                           LIMIT 1
                         ),
                         ''
                       ) AS last_message
                FROM conversations c
                LEFT JOIN messages m ON m.conversation_id = c.conversation_id
                GROUP BY c.conversation_id, c.summary_text, c.created_at, c.updated_at
                ORDER BY c.updated_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = list(cur.fetchall())
        conversations = []
        for row in rows:
            last_message = self._compact_text(row['last_message'] or '', 120)
            title = last_message or f"Conversation {row['conversation_id'][:8]}"
            conversations.append(
                {
                    'conversation_id': row['conversation_id'],
                    'title': title,
                    'summary': self._compact_text(row['summary_text'] or '', 180),
                    'message_count': int(row['message_count'] or 0),
                    'created_at': float(row['created_at']),
                    'updated_at': float(row['updated_at']),
                    'last_message': last_message,
                }
            )
        return conversations

    def get_conversation(self, conversation_id: str) -> Dict:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                'SELECT conversation_id, summary_text, created_at, updated_at FROM conversations WHERE conversation_id=?',
                (conversation_id,),
            )
            conversation = cur.fetchone()
            if conversation is None:
                return {'conversation_id': conversation_id, 'messages': [], 'summary_text': ''}
            cur = conn.execute(
                """
                SELECT client_message_id, role, content, created_at
                FROM messages
                WHERE conversation_id=?
                ORDER BY created_at ASC, id ASC
                """,
                (conversation_id,),
            )
            rows = list(cur.fetchall())
        return {
            'conversation_id': conversation['conversation_id'],
            'summary_text': conversation['summary_text'] or '',
            'created_at': float(conversation['created_at']),
            'updated_at': float(conversation['updated_at']),
            'messages': [
                {
                    'id': row['client_message_id'],
                    'role': row['role'],
                    'content': row['content'],
                    'created_at': float(row['created_at']),
                }
                for row in rows
            ],
        }

    def prepare_request(self, request_data: Dict) -> Tuple[Dict, Optional[str]]:
        conversation_id = request_data.get('conversation_id')
        messages = request_data.get('messages') or []
        sanitized = dict(request_data)
        if not conversation_id or not isinstance(messages, list):
            sanitized['messages'] = [
                {'role': m.get('role'), 'content': m.get('content')}
                for m in messages
                if m.get('role') and m.get('content')
            ]
            return sanitized, None

        self.save_client_messages(conversation_id, messages)
        summary = self._load_summary(conversation_id) or self.refresh_summary(conversation_id)
        latest_user = ''
        for msg in reversed(messages):
            if msg.get('role') == 'user' and msg.get('content'):
                latest_user = str(msg.get('content'))
                break
        retrieved = self._retrieve_relevant_snippets(conversation_id, latest_user)
        recent_messages = messages[-self.recent_message_count :]

        memory_sections = []
        if summary:
            memory_sections.append(summary)
        if retrieved:
            memory_sections.append(
                'Relevant earlier snippets from long-term memory:\n' + '\n'.join(f'- {s}' for s in retrieved)
            )

        model_messages: List[Dict[str, str]] = []
        if memory_sections:
            model_messages.append(
                {
                    'role': 'system',
                    'content': (
                        'Use the following memory only as approximate prior context. '
                        'Prefer the recent verbatim turns if there is any conflict.\n\n'
                        + '\n\n'.join(memory_sections)
                    ),
                }
            )

        model_messages.extend(
            {'role': m.get('role'), 'content': m.get('content')}
            for m in recent_messages
            if m.get('role') and m.get('content')
        )

        sanitized['messages'] = model_messages
        return sanitized, conversation_id

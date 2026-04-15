import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from backend.server.semantic_retrieval import SemanticSnippetRetriever
from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)

_WORD_RE = re.compile(r"[A-Za-z0-9_\-']+")
_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


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
        self.default_max_sequence_tokens = int(os.environ.get('PARALLAX_CHAT_MEMORY_MAX_SEQUENCE_TOKENS', '7168'))
        self.default_response_reserve_tokens = int(os.environ.get('PARALLAX_CHAT_MEMORY_RESPONSE_RESERVE_TOKENS', '2048'))
        self.prompt_overhead_tokens = int(os.environ.get('PARALLAX_CHAT_MEMORY_PROMPT_OVERHEAD_TOKENS', '96'))
        self.max_memory_fraction = float(os.environ.get('PARALLAX_CHAT_MEMORY_MAX_MEMORY_FRACTION', '0.35'))
        self.min_recent_message_count = int(os.environ.get('PARALLAX_CHAT_MEMORY_MIN_RECENT_MESSAGES', '2'))
        self.adaptive_max_tokens_enabled = os.environ.get('PARALLAX_CHAT_MEMORY_ADAPTIVE_MAX_TOKENS', '1').lower() not in {'0', 'false', 'no'}
        self.min_response_reserve_tokens = int(os.environ.get('PARALLAX_CHAT_MEMORY_MIN_RESPONSE_RESERVE_TOKENS', '256'))
        self.model_summary_enabled = os.environ.get('PARALLAX_CHAT_MEMORY_MODEL_SUMMARY', '1').lower() not in {'0', 'false', 'no'}
        self.model_summary_max_tokens = int(os.environ.get('PARALLAX_CHAT_MEMORY_MODEL_SUMMARY_MAX_TOKENS', '256'))
        self.summary_source_char_budget = int(os.environ.get('PARALLAX_CHAT_MEMORY_SUMMARY_SOURCE_CHARS', '6000'))
        self.semantic_retrieval_enabled = os.environ.get('PARALLAX_CHAT_MEMORY_SEMANTIC_RETRIEVAL', '1').lower() not in {'0', 'false', 'no'}
        self.semantic_retrieval_dim = int(os.environ.get('PARALLAX_CHAT_MEMORY_SEMANTIC_DIM', '256'))
        self.semantic_retrieval_recency_weight = float(os.environ.get('PARALLAX_CHAT_MEMORY_SEMANTIC_RECENCY_WEIGHT', '0.15'))
        self.semantic_retriever = SemanticSnippetRetriever(
            dim=self.semantic_retrieval_dim,
            recency_weight=self.semantic_retrieval_recency_weight,
        )
        logger.info(
            'Chat memory retrieval config: semantic_enabled=%s semantic_dim=%d semantic_backend=%s lexical_fallback=%s',
            self.semantic_retrieval_enabled,
            self.semantic_retrieval_dim,
            getattr(self.semantic_retriever, 'backend_name', 'unknown'),
            True,
        )
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
                    summary_source TEXT NOT NULL DEFAULT 'none',
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
            columns = {row['name'] for row in conn.execute("PRAGMA table_info(conversations)").fetchall()}
            if 'summary_source' not in columns:
                conn.execute("ALTER TABLE conversations ADD COLUMN summary_source TEXT NOT NULL DEFAULT 'none'")
            conn.commit()

    def _tokenize(self, text: str) -> List[str]:
        return [tok.lower() for tok in _WORD_RE.findall(text or '') if len(tok) >= 3]

    def _compact_text(self, text: str, max_chars: int = 180) -> str:
        text = ' '.join((text or '').split())
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 1].rstrip() + '...'

    def _clean_history_text(self, text: str, max_chars: int = 180) -> str:
        text = _THINK_RE.sub('', text or '')
        text = re.sub(r'<[^>]+>', ' ', text)
        text = ' '.join(text.split())
        return self._compact_text(text, max_chars)

    def _estimate_text_tokens(self, text: str) -> int:
        compact = ' '.join((text or '').split())
        if not compact:
            return 0
        return max(1, (len(compact) + 3) // 4)

    def _estimate_message_tokens(self, message: Dict) -> int:
        role = str(message.get('role') or '')
        content = str(message.get('content') or '')
        overhead = 8 + (len(role) // 8)
        return overhead + self._estimate_text_tokens(content)

    def _truncate_text_to_budget(
        self,
        text: str,
        budget_tokens: int,
        *,
        preserve_tail: bool = False,
    ) -> str:
        compact = ' '.join((text or '').split())
        if budget_tokens <= 0 or not compact:
            return ''
        if self._estimate_text_tokens(compact) <= budget_tokens:
            return compact

        marker = ' ...[content omitted to fit context]... '
        max_chars = max(16, budget_tokens * 4)
        if len(compact) <= max_chars:
            return compact
        if max_chars <= len(marker) + 8:
            return compact[-max_chars:] if preserve_tail else compact[:max_chars]

        if preserve_tail:
            tail_chars = max_chars - len(marker)
            return marker.strip() + compact[-tail_chars:]

        head_chars = max_chars // 3
        tail_chars = max_chars - len(marker) - head_chars
        if tail_chars <= 0:
            return compact[:max_chars]
        return compact[:head_chars].rstrip() + marker + compact[-tail_chars:].lstrip()

    def _sanitize_messages(self, messages: List[Dict]) -> List[Dict[str, str]]:
        return [
            {'role': str(m.get('role')), 'content': str(m.get('content')).strip()}
            for m in messages
            if m.get('role') and str(m.get('content') or '').strip()
        ]

    def _fit_recent_messages(self, messages: List[Dict], budget_tokens: int) -> List[Dict[str, str]]:
        sanitized_messages = self._sanitize_messages(messages)
        if not sanitized_messages:
            return []

        kept: List[Dict[str, str]] = []
        used_tokens = 0
        min_keep = min(self.min_recent_message_count, len(sanitized_messages))

        for reverse_index, message in enumerate(reversed(sanitized_messages)):
            remaining_messages = len(sanitized_messages) - reverse_index
            must_keep = remaining_messages <= min_keep
            message_tokens = self._estimate_message_tokens(message)
            available = max(0, budget_tokens - used_tokens)

            if message_tokens <= available:
                kept.append(message)
                used_tokens += message_tokens
                continue

            if must_keep or not kept:
                content_budget = max(8, available - 8)
                trimmed_content = self._truncate_text_to_budget(
                    message['content'],
                    content_budget,
                    preserve_tail=message['role'] == 'user',
                )
                if trimmed_content:
                    trimmed_message = {'role': message['role'], 'content': trimmed_content}
                    kept.append(trimmed_message)
                    used_tokens += self._estimate_message_tokens(trimmed_message)
                continue

        kept.reverse()
        return kept

    def _estimate_memory_message_tokens(self, summary: str, retrieved: List[str], budget_tokens: int) -> int:
        sections = self._build_memory_sections(summary, retrieved, budget_tokens)
        if not sections:
            return 0
        memory_message = {
            'role': 'system',
            'content': (
                'Use the following memory only as approximate prior context. '
                'Prefer the recent verbatim turns if there is any conflict.\n\n'
                + '\n\n'.join(sections)
            ),
        }
        return self._estimate_message_tokens(memory_message)

    def _compute_adaptive_response_reserve(
        self,
        *,
        requested_output_tokens: int,
        max_sequence_tokens: int,
        recent_messages: List[Dict],
        summary: str,
        retrieved: List[str],
    ) -> Tuple[int, Dict[str, int]]:
        minimum_output_tokens = max(64, self.min_response_reserve_tokens)
        requested_output_tokens = max(64, requested_output_tokens)
        adjusted_output_tokens = requested_output_tokens
        metadata = {
            'requested_output_tokens': requested_output_tokens,
            'adjusted_output_tokens': requested_output_tokens,
            'output_tokens_reduced': 0,
            'adapted_output_budget': 0,
        }
        if not self.adaptive_max_tokens_enabled or requested_output_tokens <= minimum_output_tokens:
            return requested_output_tokens, metadata

        sanitized_recent_messages = self._sanitize_messages(recent_messages)
        if not sanitized_recent_messages:
            return requested_output_tokens, metadata

        full_recent_tokens = sum(self._estimate_message_tokens(message) for message in sanitized_recent_messages)
        minimum_input_budget = max(256, max_sequence_tokens - minimum_output_tokens - self.prompt_overhead_tokens)
        desired_memory_tokens = min(
            self._estimate_memory_message_tokens(summary, retrieved, minimum_input_budget),
            max(0, int(minimum_input_budget * self.max_memory_fraction)),
        )
        desired_input_tokens = full_recent_tokens + desired_memory_tokens
        requested_input_budget = max(256, max_sequence_tokens - requested_output_tokens - self.prompt_overhead_tokens)

        if desired_input_tokens <= requested_input_budget:
            return requested_output_tokens, metadata

        extra_input_needed = desired_input_tokens - requested_input_budget
        adjusted_output_tokens = max(minimum_output_tokens, requested_output_tokens - extra_input_needed)
        if adjusted_output_tokens >= requested_output_tokens:
            return requested_output_tokens, metadata

        metadata['adjusted_output_tokens'] = adjusted_output_tokens
        metadata['output_tokens_reduced'] = requested_output_tokens - adjusted_output_tokens
        metadata['adapted_output_budget'] = 1
        return adjusted_output_tokens, metadata

    def _build_memory_sections(
        self,
        summary: str,
        retrieved: List[str],
        budget_tokens: int,
    ) -> List[str]:
        if budget_tokens <= 0:
            return []

        sections: List[str] = []
        used_tokens = 0

        if summary:
            remaining = max(0, budget_tokens - used_tokens)
            if remaining > 0:
                trimmed_summary = self._truncate_text_to_budget(summary, remaining)
                if trimmed_summary:
                    sections.append(trimmed_summary)
                    used_tokens += self._estimate_text_tokens(trimmed_summary)

        if retrieved:
            remaining = max(0, budget_tokens - used_tokens)
            if remaining > 12:
                snippets: List[str] = []
                header = 'Relevant earlier snippets from long-term memory:'
                snippet_budget = max(0, remaining - self._estimate_text_tokens(header))
                current = 0
                for snippet in retrieved:
                    trimmed = self._truncate_text_to_budget(snippet, snippet_budget - current)
                    if not trimmed:
                        break
                    snippet_tokens = self._estimate_text_tokens(trimmed)
                    if current + snippet_tokens > snippet_budget:
                        break
                    snippets.append(f'- {trimmed}')
                    current += snippet_tokens
                if snippets:
                    sections.append(header + '\n' + '\n'.join(snippets))

        return sections

    def ensure_conversation(self, conversation_id: str) -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO conversations (conversation_id, summary_text, summary_source, created_at, updated_at)
                VALUES (?, '', 'none', ?, ?)
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

    def _older_summary_rows(self, conversation_id: str) -> List[sqlite3.Row]:
        rows = self._load_messages(conversation_id)
        older_rows = rows[:-self.recent_message_count] if len(rows) > self.recent_message_count else []
        return older_rows[-self.summary_message_limit :]

    def _build_heuristic_summary_from_rows(self, rows: List[sqlite3.Row]) -> str:
        if not rows:
            return ''
        lines = []
        current_chars = 0
        for row in rows:
            prefix = 'User' if row['role'] == 'user' else 'Assistant' if row['role'] == 'assistant' else 'System'
            line = f'- {prefix}: {self._compact_text(row["content"], 200)}'
            if current_chars + len(line) + 1 > self.summary_char_budget:
                break
            lines.append(line)
            current_chars += len(line) + 1
        if not lines:
            return ''
        return 'Approximate summary of earlier conversation:\n' + '\n'.join(lines)

    def store_summary(self, conversation_id: str, summary: str, source: str = 'heuristic') -> str:
        cleaned = self._clean_history_text(summary or '', self.summary_char_budget)
        with self._lock, self._connect() as conn:
            conn.execute(
                'UPDATE conversations SET summary_text=?, summary_source=?, updated_at=? WHERE conversation_id=?',
                (cleaned, 'model' if source == 'model' else 'heuristic', time.time(), conversation_id),
            )
            conn.commit()
        return cleaned

    def refresh_summary(self, conversation_id: str) -> str:
        summary = self._build_heuristic_summary_from_rows(self._older_summary_rows(conversation_id))
        return self.store_summary(conversation_id, summary, source='heuristic' if summary else 'none')

    def build_summary_generation_messages(self, conversation_id: str) -> Optional[List[Dict[str, str]]]:
        rows = self._older_summary_rows(conversation_id)
        if not rows:
            return None

        transcript_lines = []
        current_chars = 0
        for row in rows:
            prefix = 'User' if row['role'] == 'user' else 'Assistant' if row['role'] == 'assistant' else 'System'
            line = f'{prefix}: {self._compact_text(row["content"], 500)}'
            if current_chars + len(line) + 1 > self.summary_source_char_budget:
                break
            transcript_lines.append(line)
            current_chars += len(line) + 1

        if not transcript_lines:
            return None

        existing_summary = self._load_summary(conversation_id)
        prompt_parts = []
        if existing_summary:
            prompt_parts.append('Existing summary (update or replace it if needed):\n' + existing_summary)
        prompt_parts.append('Conversation excerpt to summarize:\n' + '\n'.join(transcript_lines))

        return [
            {
                'role': 'system',
                'content': (
                    'Write a compact working-memory summary of earlier conversation turns. '
                    'Preserve stable user preferences, facts, constraints, unresolved tasks, and decisions. '
                    'Use concise bullet points. Do not mention every turn. Do not include XML or markdown headings.'
                ),
            },
            {
                'role': 'user',
                'content': '\n\n'.join(prompt_parts),
            },
        ]

    def _load_summary(self, conversation_id: str) -> str:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                'SELECT summary_text FROM conversations WHERE conversation_id=?',
                (conversation_id,),
            )
            row = cur.fetchone()
            return row['summary_text'] if row and row['summary_text'] else ''

    def _load_summary_source(self, conversation_id: str) -> str:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                'SELECT summary_source FROM conversations WHERE conversation_id=?',
                (conversation_id,),
            )
            row = cur.fetchone()
            return row['summary_source'] if row and row['summary_source'] else 'none'

    def _retrieve_relevant_snippets_lexical(self, conversation_id: str, query: str) -> List[str]:
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

    def _retrieve_relevant_snippets(self, conversation_id: str, query: str) -> List[str]:
        rows = self._load_messages(conversation_id)
        if len(rows) <= self.recent_message_count:
            return []
        older_rows = rows[:-self.recent_message_count]
        candidate_snippets = []
        for row in older_rows:
            prefix = 'User' if row['role'] == 'user' else 'Assistant' if row['role'] == 'assistant' else 'System'
            candidate_snippets.append(f'{prefix}: {self._compact_text(row["content"], 220)}')

        if self.semantic_retrieval_enabled:
            semantic = self.semantic_retriever.retrieve(
                query=query,
                snippets=candidate_snippets,
                limit=self.retrieval_limit,
                char_budget=self.retrieval_char_budget,
            )
            if semantic:
                logger.info(
                    'Chat memory retrieval mode=semantic backend=%s snippets=%d conversation_id=%s',
                    getattr(self.semantic_retriever, 'last_backend_used', getattr(self.semantic_retriever, 'backend_name', 'unknown')),
                    len(semantic),
                    conversation_id,
                )
                return semantic
            logger.info('Chat memory retrieval mode=semantic_empty fallback=lexical conversation_id=%s', conversation_id)

        lexical = self._retrieve_relevant_snippets_lexical(conversation_id, query)
        if lexical:
            logger.info('Chat memory retrieval mode=lexical snippets=%d conversation_id=%s', len(lexical), conversation_id)
        else:
            logger.info('Chat memory retrieval mode=none conversation_id=%s', conversation_id)
        return lexical

    def list_conversations(self, limit: int = 50) -> List[Dict]:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                SELECT c.conversation_id, c.summary_text, c.summary_source, c.created_at, c.updated_at,
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
                GROUP BY c.conversation_id, c.summary_text, c.summary_source, c.created_at, c.updated_at
                ORDER BY c.updated_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = list(cur.fetchall())
        conversations = []
        for row in rows:
            with self._lock, self._connect() as conn:
                first_user_cur = conn.execute(
                    """
                    SELECT content FROM messages
                    WHERE conversation_id=? AND role='user'
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    """,
                    (row['conversation_id'],),
                )
                first_user_row = first_user_cur.fetchone()
            first_user_message = self._clean_history_text(
                first_user_row['content'] if first_user_row else '',
                80,
            )
            last_message = self._clean_history_text(row['last_message'] or '', 120)
            summary = self._clean_history_text(row['summary_text'] or '', 180)
            title = first_user_message or last_message or f"Conversation {row['conversation_id'][:8]}"
            conversations.append(
                {
                    'conversation_id': row['conversation_id'],
                    'title': title,
                    'summary': summary,
                    'summary_source': row['summary_source'] or 'none',
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
                'SELECT conversation_id, summary_text, summary_source, created_at, updated_at FROM conversations WHERE conversation_id=?',
                (conversation_id,),
            )
            conversation = cur.fetchone()
            if conversation is None:
                return {'conversation_id': conversation_id, 'messages': [], 'summary_text': '', 'summary_source': 'none'}
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
            'summary_source': conversation['summary_source'] or 'none',
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

    def delete_conversation(self, conversation_id: str) -> bool:
        if not conversation_id:
            return False
        with self._lock, self._connect() as conn:
            cur = conn.execute('SELECT 1 FROM conversations WHERE conversation_id=?', (conversation_id,))
            exists = cur.fetchone() is not None
            if not exists:
                return False
            conn.execute('DELETE FROM messages WHERE conversation_id=?', (conversation_id,))
            conn.execute('DELETE FROM conversations WHERE conversation_id=?', (conversation_id,))
            conn.commit()
        return True

    def delete_all_conversations(self) -> int:
        with self._lock, self._connect() as conn:
            cur = conn.execute('SELECT COUNT(*) AS count FROM conversations')
            row = cur.fetchone()
            deleted = int(row['count'] or 0) if row is not None else 0
            conn.execute('DELETE FROM messages')
            conn.execute('DELETE FROM conversations')
            conn.commit()
        return deleted

    def prepare_request(self, request_data: Dict) -> Tuple[Dict, Optional[str]]:
        sanitized = dict(request_data)
        disable_chat_memory = bool(sanitized.pop('_disable_chat_memory', False))
        sanitized.pop('_summary_request', None)
        conversation_id = sanitized.get('conversation_id')
        messages = sanitized.get('messages') or []
        if disable_chat_memory or not conversation_id or not isinstance(messages, list):
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

        max_sequence_tokens = int(request_data.get('max_sequence_length') or self.default_max_sequence_tokens)
        requested_output_tokens = int(request_data.get('max_tokens') or self.default_response_reserve_tokens)
        response_reserve_tokens, adaptive_output_budget = self._compute_adaptive_response_reserve(
            requested_output_tokens=requested_output_tokens,
            max_sequence_tokens=max_sequence_tokens,
            recent_messages=recent_messages,
            summary=summary,
            retrieved=retrieved,
        )
        input_budget_tokens = max(256, max_sequence_tokens - response_reserve_tokens - self.prompt_overhead_tokens)
        memory_budget_tokens = max(0, int(input_budget_tokens * self.max_memory_fraction))
        recent_budget_tokens = max(128, input_budget_tokens - memory_budget_tokens)

        fitted_recent_messages = self._fit_recent_messages(recent_messages, recent_budget_tokens)
        recent_used_tokens = sum(self._estimate_message_tokens(message) for message in fitted_recent_messages)
        remaining_for_memory = max(0, input_budget_tokens - recent_used_tokens)
        fitted_memory_sections = self._build_memory_sections(summary, retrieved, remaining_for_memory)
        summary_tokens = 0
        snippet_tokens = 0
        for section in fitted_memory_sections:
            section_tokens = self._estimate_text_tokens(section)
            if section.startswith('Relevant earlier snippets from long-term memory:'):
                snippet_tokens += section_tokens
            else:
                summary_tokens += section_tokens

        model_messages: List[Dict[str, str]] = []
        if fitted_memory_sections:
            memory_message = {
                'role': 'system',
                'content': (
                    'Use the following memory only as approximate prior context. '
                    'Prefer the recent verbatim turns if there is any conflict.\n\n'
                    + '\n\n'.join(fitted_memory_sections)
                ),
            }
            if self._estimate_message_tokens(memory_message) > remaining_for_memory:
                memory_content_budget = max(32, remaining_for_memory - 16)
                memory_message['content'] = self._truncate_text_to_budget(memory_message['content'], memory_content_budget)
            if memory_message['content']:
                model_messages.append(memory_message)

        model_messages.extend(fitted_recent_messages)

        estimated_input_tokens = sum(self._estimate_message_tokens(message) for message in model_messages)
        prompt_budget = {
            'input_budget_tokens': input_budget_tokens,
            'reserved_output_tokens': response_reserve_tokens,
            'estimated_input_tokens': estimated_input_tokens,
            'recent_messages_count': len(fitted_recent_messages),
            'memory_sections_count': len(fitted_memory_sections),
            'memory_budget_tokens': remaining_for_memory,
            'recent_turn_tokens': recent_used_tokens,
            'summary_tokens': summary_tokens,
            'snippet_tokens': snippet_tokens,
            'requested_output_tokens': adaptive_output_budget['requested_output_tokens'],
            'adjusted_output_tokens': adaptive_output_budget['adjusted_output_tokens'],
            'output_tokens_reduced': adaptive_output_budget['output_tokens_reduced'],
            'adapted_output_budget': adaptive_output_budget['adapted_output_budget'],
        }
        if estimated_input_tokens > input_budget_tokens:
            logger.warning(
                'Chat memory budgeting still exceeded target input budget for conversation %s: estimated_input_tokens=%d budget=%d',
                conversation_id,
                estimated_input_tokens,
                input_budget_tokens,
            )
        else:
            logger.info(
                'Chat memory prompt budget for %s: input_budget=%d reserved_output=%d requested_output=%d estimated_input=%d recent_messages=%d memory_sections=%d adapted_output=%s',
                conversation_id,
                input_budget_tokens,
                response_reserve_tokens,
                requested_output_tokens,
                estimated_input_tokens,
                len(fitted_recent_messages),
                len(fitted_memory_sections),
                bool(adaptive_output_budget['adapted_output_budget']),
            )

        sanitized['messages'] = model_messages
        sanitized['max_tokens'] = response_reserve_tokens
        sanitized['_prompt_budget'] = prompt_budget
        return sanitized, conversation_id

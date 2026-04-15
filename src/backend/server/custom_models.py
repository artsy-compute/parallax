import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from backend.server.static_config import estimate_vram_gb_required, get_model_info_with_try_catch
from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)

SUPPORTED_MODEL_TYPES = {
    'qwen2',
    'qwen3',
    'qwen3_moe',
    'qwen3_next',
    'llama',
    'deepseek_v2',
    'deepseek_v3',
    'deepseek_v32',
    'gpt_oss',
    'step3p5',
    'minimax',
    'glm4_moe',
}


class CustomModelStore:
    def __init__(self, db_path: str | None = None):
        if db_path is None:
            db_path = os.environ.get('PARALLAX_CUSTOM_MODELS_DB', '/tmp/parallax_custom_models.sqlite3')
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
                CREATE TABLE IF NOT EXISTS custom_models (
                    id TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_value TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    validation_status TEXT NOT NULL DEFAULT 'pending',
                    validation_message TEXT NOT NULL DEFAULT '',
                    detected_model_type TEXT NOT NULL DEFAULT '',
                    supports_sharding INTEGER NOT NULL DEFAULT 0,
                    vram_gb INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(source_type, source_value)
                );
                """
            )
            conn.commit()

    @staticmethod
    def _normalize_source_type(source_type: str) -> str:
        value = str(source_type or '').strip().lower()
        if value not in {'huggingface', 'local_path'}:
            raise ValueError('source_type must be one of: huggingface, local_path')
        return value

    @staticmethod
    def _normalize_source_value(source_value: str) -> str:
        value = str(source_value or '').strip()
        if not value:
            raise ValueError('source_value is required')
        return value

    @staticmethod
    def _default_display_name(source_type: str, source_value: str) -> str:
        if source_type == 'local_path':
            return Path(source_value).name or source_value
        return source_value

    def _load_config_only(self, source_type: str, source_value: str) -> dict[str, Any]:
        if source_type == 'local_path':
            model_path = Path(source_value).expanduser()
            if not model_path.exists():
                raise FileNotFoundError(f'Local model path does not exist: {model_path}')
            config_path = model_path / 'config.json'
            if not config_path.exists():
                raise FileNotFoundError(f'Missing config.json in local model path: {model_path}')
            return json.loads(config_path.read_text())

        from huggingface_hub import hf_hub_download  # type: ignore

        config_file = hf_hub_download(repo_id=source_value, filename='config.json')
        return json.loads(Path(config_file).read_text())

    def _validate_model(self, source_type: str, source_value: str) -> dict[str, Any]:
        config = self._load_config_only(source_type, source_value)
        model_type = str(config.get('model_type') or '').strip()
        model_name = source_value
        model_info = get_model_info_with_try_catch(model_name)
        vram_gb = int(estimate_vram_gb_required(model_info)) if model_info is not None else 0
        if model_type in SUPPORTED_MODEL_TYPES:
            validation_status = 'verified'
            validation_message = f'Detected supported architecture: {model_type}'
        elif config:
            validation_status = 'config_only'
            validation_message = f'Config detected for model_type={model_type or "unknown"}; runtime compatibility is not guaranteed'
        else:
            validation_status = 'invalid'
            validation_message = 'Unable to read model config'

        return {
            'validation_status': validation_status,
            'validation_message': validation_message,
            'detected_model_type': model_type,
            'supports_sharding': 1 if validation_status == 'verified' else 0,
            'vram_gb': vram_gb,
            'metadata_json': json.dumps({'config': {'model_type': model_type}}, ensure_ascii=False),
        }

    def list_models(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, source_type, source_value, display_name, enabled, validation_status,
                       validation_message, detected_model_type, supports_sharding, vram_gb,
                       metadata_json, created_at, updated_at
                FROM custom_models
                ORDER BY updated_at DESC, created_at DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def list_model_entries(self) -> list[dict[str, Any]]:
        return [
            {
                'name': row['source_value'],
                'display_name': row['display_name'],
                'vram_gb': int(row['vram_gb'] or 0),
                'custom': True,
                'source_type': row['source_type'],
                'validation_status': row['validation_status'],
                'validation_message': row['validation_message'],
                'supports_sharding': bool(row['supports_sharding']),
            }
            for row in self.list_models()
            if bool(row.get('enabled'))
        ]

    def search_huggingface_models(self, query: str, limit: int = 8) -> list[dict[str, Any]]:
        normalized_query = str(query or '').strip()
        if not normalized_query:
            raise ValueError('query is required')
        normalized_limit = max(1, min(int(limit or 8), 20))
        raw_limit = min(max(normalized_limit * 5, 20), 100)

        model_ids: list[str] = []
        try:
            from huggingface_hub import HfApi  # type: ignore

            api = HfApi()
            for item in api.list_models(search=normalized_query, limit=raw_limit):
                model_id = str(getattr(item, 'id', '') or '').strip()
                if model_id:
                    model_ids.append(model_id)
        except Exception as e:
            logger.debug('Falling back to HF REST search for %r: %s', normalized_query, e)
            url = (
                "https://huggingface.co/api/models?"
                + urllib.parse.urlencode({"search": normalized_query, "limit": raw_limit})
            )
            with urllib.request.urlopen(url, timeout=10) as response:
                payload = json.loads(response.read().decode('utf-8'))
            model_ids.extend(
                str(item.get('id') or '').strip()
                for item in payload
                if str(item.get('id') or '').strip()
            )

        seen: set[str] = set()
        results: list[dict[str, Any]] = []
        for model_id in model_ids:
            if model_id in seen:
                continue
            seen.add(model_id)
            try:
                validation = self._validate_model('huggingface', model_id)
                if validation['validation_status'] != 'verified':
                    continue
                results.append(
                    {
                        'source_type': 'huggingface',
                        'source_value': model_id,
                        'display_name': model_id,
                        'validation_status': validation['validation_status'],
                        'validation_message': validation['validation_message'],
                        'detected_model_type': validation['detected_model_type'],
                        'supports_sharding': bool(validation['supports_sharding']),
                        'vram_gb': int(validation['vram_gb'] or 0),
                    }
                )
                if len(results) >= normalized_limit:
                    break
            except Exception:
                continue
        return results

    @staticmethod
    def _model_id(source_type: str, source_value: str) -> str:
        return f"custom-{uuid.uuid5(uuid.NAMESPACE_URL, f'{source_type}:{source_value}')}"

    def add_model(self, *, source_type: str, source_value: str, display_name: str = '') -> dict[str, Any]:
        normalized_type = self._normalize_source_type(source_type)
        normalized_value = self._normalize_source_value(source_value)
        resolved_display_name = str(display_name or '').strip() or self._default_display_name(normalized_type, normalized_value)
        validation = self._validate_model(normalized_type, normalized_value)
        now = time.time()
        model_id = self._model_id(normalized_type, normalized_value)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO custom_models (
                    id, source_type, source_value, display_name, enabled,
                    validation_status, validation_message, detected_model_type,
                    supports_sharding, vram_gb, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_type, source_value) DO UPDATE SET
                    display_name=excluded.display_name,
                    enabled=1,
                    validation_status=excluded.validation_status,
                    validation_message=excluded.validation_message,
                    detected_model_type=excluded.detected_model_type,
                    supports_sharding=excluded.supports_sharding,
                    vram_gb=excluded.vram_gb,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    model_id,
                    normalized_type,
                    normalized_value,
                    resolved_display_name,
                    validation['validation_status'],
                    validation['validation_message'],
                    validation['detected_model_type'],
                    validation['supports_sharding'],
                    validation['vram_gb'],
                    validation['metadata_json'],
                    now,
                    now,
                ),
            )
            conn.commit()

        for item in self.list_models():
            if item['source_type'] == normalized_type and item['source_value'] == normalized_value:
                return item
        raise RuntimeError('Failed to persist custom model')

    def delete_model(self, model_id: str) -> bool:
        with self._lock, self._connect() as conn:
            cur = conn.execute("DELETE FROM custom_models WHERE id = ?", (str(model_id or '').strip(),))
            conn.commit()
            return cur.rowcount > 0

import json
import os
import sqlite3
import tarfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
import zipfile
from pathlib import Path
import re
from typing import Any

from backend.server.static_config import estimate_vram_gb_required, get_model_info_with_try_catch
from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)
HF_SEARCH_CACHE_TTL_SEC = 60 * 60

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


def _discover_supported_architectures() -> set[str]:
    architectures: set[str] = set()
    models_dir = Path(__file__).resolve().parents[2] / 'parallax' / 'models'
    for path in sorted(models_dir.glob('*.py')):
        if path.name == '__init__.py':
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except Exception:
            logger.warning('Failed to read model definition file %s', path, exc_info=True)
            continue
        match = re.search(r'return\s+"([^"]+)"', text)
        if match:
            architectures.add(str(match.group(1)).strip())
    return architectures


SUPPORTED_ARCHITECTURES = _discover_supported_architectures()


class CustomModelStore:
    def __init__(self, db_path: str | None = None, allowed_local_roots: dict[str, str] | None = None):
        if db_path is None:
            db_path = os.environ.get('PARALLAX_CUSTOM_MODELS_DB', '/tmp/parallax_custom_models.sqlite3')
        self.db_path = str(Path(db_path))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._allowed_local_roots: dict[str, Path] = {}
        self._init_db()
        self.configure_allowed_local_roots(allowed_local_roots or {})

    def configure_allowed_local_roots(self, roots: dict[str, str] | None) -> None:
        normalized: dict[str, Path] = {}
        for raw_key, raw_path in (roots or {}).items():
            key = str(raw_key or '').strip()
            path_text = str(raw_path or '').strip()
            if not key or not path_text:
                continue
            normalized[key] = Path(path_text).expanduser().resolve()
        self._allowed_local_roots = normalized

    def list_allowed_local_roots(self) -> list[dict[str, str]]:
        return [
            {
                'id': key,
                'label': key.replace('_', ' ').replace('-', ' ').title(),
                'path': str(path),
            }
            for key, path in sorted(self._allowed_local_roots.items())
        ]

    def list_allowed_local_model_options(self) -> list[dict[str, str]]:
        options: list[dict[str, str]] = []
        for root in self.list_allowed_local_roots():
            root_id = str(root.get('id') or '').strip()
            root_label = str(root.get('label') or root_id).strip() or root_id
            root_path = self._allowed_local_roots.get(root_id)
            if not root_id or root_path is None or not root_path.exists():
                continue
            try:
                seen_dirs: set[Path] = set()
                config_paths: list[Path] = []
                for current_root, _, filenames in os.walk(root_path, followlinks=True):
                    if 'config.json' not in filenames:
                        continue
                    config_path = (Path(current_root) / 'config.json').resolve()
                    model_dir = config_path.parent
                    if model_dir in seen_dirs:
                        continue
                    seen_dirs.add(model_dir)
                    config_paths.append(config_path)
                config_paths.sort()
            except Exception:
                logger.warning('Failed to scan approved local root %s', root_path, exc_info=True)
                continue
            for config_path in config_paths:
                if not config_path.is_file():
                    continue
                model_dir = config_path.parent.resolve()
                try:
                    relative_path = model_dir.relative_to(root_path).as_posix()
                except ValueError:
                    continue
                if not relative_path or relative_path == '.':
                    continue
                options.append(
                    {
                        'root_id': root_id,
                        'root_label': root_label,
                        'relative_path': relative_path,
                        'source_value': f'{root_id}:{relative_path}',
                        'label': f'{Path(relative_path).name} ({root_label})',
                        'path': str(model_dir),
                    }
                )
        options.sort(key=lambda item: (str(item.get('root_label') or '').lower(), str(item.get('relative_path') or '').lower()))
        return options

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
                CREATE TABLE IF NOT EXISTS hf_search_cache (
                    cache_key TEXT PRIMARY KEY,
                    query TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
                """
            )
            conn.commit()

    @staticmethod
    def _hf_search_cache_key(query: str, limit: int, offset: int) -> str:
        return json.dumps(
            {
                'query': str(query or '').strip().lower(),
                'limit': int(limit or 0),
                'offset': int(offset or 0),
            },
            sort_keys=True,
            ensure_ascii=False,
        )

    def _get_cached_hf_search(self, query: str, limit: int, offset: int) -> dict[str, Any] | None:
        cache_key = self._hf_search_cache_key(query, limit, offset)
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM hf_search_cache WHERE expires_at <= ?", (now,))
            row = conn.execute(
                """
                SELECT response_json
                FROM hf_search_cache
                WHERE cache_key = ? AND expires_at > ?
                """,
                (cache_key, now),
            ).fetchone()
            conn.commit()
        if row is None:
            return None
        try:
            payload = json.loads(str(row['response_json'] or '{}'))
        except Exception:
            logger.warning('Failed to parse cached HF search response for %s', cache_key, exc_info=True)
            return None
        if not isinstance(payload, dict):
            return None
        payload['items'] = payload.get('items') if isinstance(payload.get('items'), list) else []
        payload['next_offset'] = int(payload.get('next_offset') or 0)
        payload['has_more'] = bool(payload.get('has_more'))
        return payload

    def _set_cached_hf_search(self, query: str, limit: int, offset: int, payload: dict[str, Any]) -> None:
        cache_key = self._hf_search_cache_key(query, limit, offset)
        now = time.time()
        expires_at = now + HF_SEARCH_CACHE_TTL_SEC
        response_json = json.dumps(payload or {}, ensure_ascii=False)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO hf_search_cache (cache_key, query, response_json, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    query=excluded.query,
                    response_json=excluded.response_json,
                    created_at=excluded.created_at,
                    expires_at=excluded.expires_at
                """,
                (cache_key, str(query or '').strip(), response_json, now, expires_at),
            )
            conn.commit()

    @staticmethod
    def _normalize_source_type(source_type: str) -> str:
        value = str(source_type or '').strip().lower()
        if value not in {'huggingface', 'local_path', 'scheduler_root', 'url'}:
            raise ValueError('source_type must be one of: huggingface, scheduler_root, url')
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
        if source_type == 'scheduler_root':
            _, _, relative_path = source_value.partition(':')
            return Path(relative_path or source_value).name or source_value
        if source_type == 'url':
            parsed = urllib.parse.urlparse(source_value)
            filename = Path(parsed.path or '').name
            stem = filename
            for suffix in ('.tar.gz', '.tgz', '.tar.bz2', '.tar.xz', '.zip', '.tar'):
                if stem.endswith(suffix):
                    stem = stem[: -len(suffix)]
                    break
            return stem or source_value
        return source_value

    def _normalize_scheduler_root_value(self, source_value: str) -> str:
        raw = str(source_value or '').strip()
        if ':' not in raw:
            raise ValueError('Approved local root models must use root_id:relative_path')
        root_id, _, relative_path = raw.partition(':')
        root_id = str(root_id or '').strip()
        relative_path = str(relative_path or '').strip().strip('/')
        if not root_id or not relative_path:
            raise ValueError('Approved local root models must include both a root and relative path')
        if root_id not in self._allowed_local_roots:
            raise ValueError(f'Unknown approved local root: {root_id}')
        relative = Path(relative_path)
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('Relative path must stay within the approved local root')
        return f'{root_id}:{relative.as_posix()}'

    def _resolve_scheduler_root_path(self, source_value: str) -> Path:
        normalized_value = self._normalize_scheduler_root_value(source_value)
        root_id, _, relative_path = normalized_value.partition(':')
        root = self._allowed_local_roots[root_id]
        candidate = (root / relative_path).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            raise ValueError('Relative path escapes the approved local root')
        return candidate

    @staticmethod
    def _normalize_url_value(source_value: str) -> str:
        value = str(source_value or '').strip()
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme not in {'https', 'http'}:
            raise ValueError('URL source must use http or https')
        if not parsed.netloc:
            raise ValueError('URL source must include a host')
        if not Path(parsed.path or '').name:
            raise ValueError('URL source must point to a downloadable archive file')
        return value

    def _get_import_root(self) -> Path:
        if not self._allowed_local_roots:
            raise ValueError('No approved local model roots are configured')
        first_root = next(iter(self._allowed_local_roots.values()))
        import_root = (first_root / 'imported').resolve()
        import_root.mkdir(parents=True, exist_ok=True)
        return import_root

    def _extract_model_archive(self, archive_path: Path, target_dir: Path) -> None:
        target_dir = target_dir.resolve()
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as zf:
                for member in zf.namelist():
                    candidate = (target_dir / member).resolve()
                    try:
                        candidate.relative_to(target_dir)
                    except ValueError:
                        raise ValueError('Archive contains an invalid path outside the target directory')
                zf.extractall(target_dir)
            return
        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path) as tf:
                for member in tf.getmembers():
                    candidate = (target_dir / member.name).resolve()
                    try:
                        candidate.relative_to(target_dir)
                    except ValueError:
                        raise ValueError('Archive contains an invalid path outside the target directory')
                tf.extractall(target_dir)
            return
        raise ValueError('URL source must point to a .zip or .tar archive')

    def _resolve_imported_model_dir(self, base_dir: Path) -> Path:
        direct_config = base_dir / 'config.json'
        if direct_config.exists():
            return base_dir
        candidates = [path for path in base_dir.iterdir() if path.is_dir()]
        if len(candidates) == 1 and (candidates[0] / 'config.json').exists():
            return candidates[0]
        raise FileNotFoundError(f'Imported archive does not contain a model directory with config.json: {base_dir}')

    def _import_url_model(self, source_value: str) -> Path:
        normalized_url = self._normalize_url_value(source_value)
        import_root = self._get_import_root()
        model_dir = import_root / f'url-{uuid.uuid5(uuid.NAMESPACE_URL, normalized_url)}'
        resolved_model_dir = model_dir.resolve()
        if resolved_model_dir.exists():
            return self._resolve_imported_model_dir(resolved_model_dir)

        resolved_model_dir.mkdir(parents=True, exist_ok=True)
        parsed = urllib.parse.urlparse(normalized_url)
        archive_name = Path(parsed.path or '').name or 'model-archive'
        archive_path = resolved_model_dir / archive_name
        try:
            with urllib.request.urlopen(normalized_url, timeout=60) as response, archive_path.open('wb') as output:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
            self._extract_model_archive(archive_path, resolved_model_dir)
            archive_path.unlink(missing_ok=True)
            return self._resolve_imported_model_dir(resolved_model_dir)
        except Exception:
            try:
                for child in resolved_model_dir.iterdir():
                    if child.is_dir():
                        for nested in sorted(child.rglob('*'), reverse=True):
                            if nested.is_file():
                                nested.unlink(missing_ok=True)
                            elif nested.is_dir():
                                nested.rmdir()
                        child.rmdir()
                    else:
                        child.unlink(missing_ok=True)
                resolved_model_dir.rmdir()
            except Exception:
                logger.warning('Failed to clean up partial URL import directory %s', resolved_model_dir, exc_info=True)
            raise

    def _load_config_only(self, source_type: str, source_value: str) -> dict[str, Any]:
        if source_type == 'url':
            model_path = self._import_url_model(source_value)
            config_path = model_path / 'config.json'
            if not config_path.exists():
                raise FileNotFoundError(f'Missing config.json in imported URL model path: {model_path}')
            return json.loads(config_path.read_text())

        if source_type == 'scheduler_root':
            model_path = self._resolve_scheduler_root_path(source_value)
            if not model_path.exists():
                raise FileNotFoundError(f'Approved local model path does not exist: {model_path}')
            config_path = model_path / 'config.json'
            if not config_path.exists():
                raise FileNotFoundError(f'Missing config.json in approved local model path: {model_path}')
            return json.loads(config_path.read_text())

        if source_type == 'local_path':
            model_path = Path(source_value).expanduser()
            if not model_path.exists():
                raise FileNotFoundError(f'Local model path does not exist: {model_path}')
            config_path = model_path / 'config.json'
            if not config_path.exists():
                raise FileNotFoundError(f'Missing config.json in local model path: {model_path}')
            return json.loads(config_path.read_text())

        try:
            from huggingface_hub import hf_hub_download  # type: ignore

            config_file = hf_hub_download(repo_id=source_value, filename='config.json')
            return json.loads(Path(config_file).read_text())
        except Exception:
            encoded_repo = '/'.join(urllib.parse.quote(part, safe='') for part in str(source_value or '').split('/'))
            config_url = f'https://huggingface.co/{encoded_repo}/resolve/main/config.json'
            with urllib.request.urlopen(config_url, timeout=20) as response:
                return json.loads(response.read().decode('utf-8'))

    def _validate_model(self, source_type: str, source_value: str) -> dict[str, Any]:
        normalized_value = (
            self._normalize_scheduler_root_value(source_value)
            if source_type == 'scheduler_root'
            else self._normalize_url_value(source_value)
            if source_type == 'url'
            else source_value
        )
        config = self._load_config_only(source_type, normalized_value)
        model_type = str(config.get('model_type') or '').strip()
        architectures = [
            str(item).strip() for item in (config.get('architectures') or []) if str(item).strip()
        ]
        unsupported_architectures = [
            architecture for architecture in architectures if architecture not in SUPPORTED_ARCHITECTURES
        ]
        model_name = (
            str(self._resolve_scheduler_root_path(normalized_value))
            if source_type == 'scheduler_root'
            else str(self._import_url_model(normalized_value))
            if source_type == 'url'
            else normalized_value
        )
        model_info = get_model_info_with_try_catch(model_name)
        vram_gb = int(estimate_vram_gb_required(model_info)) if model_info is not None else 0
        if model_type in SUPPORTED_MODEL_TYPES and architectures and not unsupported_architectures:
            validation_status = 'verified'
            validation_message = (
                f'Detected supported model_type={model_type} and architecture={architectures[0]}'
            )
        elif model_type in SUPPORTED_MODEL_TYPES and not architectures:
            validation_status = 'config_only'
            validation_message = (
                f'Detected supported model_type={model_type}, but config.json does not declare architectures; '
                'runtime compatibility is not guaranteed'
            )
        elif model_type in SUPPORTED_MODEL_TYPES and unsupported_architectures:
            validation_status = 'invalid'
            validation_message = (
                f'Unsupported architecture={", ".join(unsupported_architectures)} for current Parallax runtime'
            )
        elif config:
            if architectures:
                validation_status = 'invalid'
                validation_message = (
                    f'Unsupported model_type={model_type or "unknown"} architecture={", ".join(architectures)}'
                )
            else:
                validation_status = 'config_only'
                validation_message = (
                    f'Config detected for model_type={model_type or "unknown"}; '
                    'runtime compatibility is not guaranteed'
                )
        else:
            validation_status = 'invalid'
            validation_message = 'Unable to read model config'

        return {
            'validation_status': validation_status,
            'validation_message': validation_message,
            'detected_model_type': model_type,
            'supports_sharding': 1 if validation_status == 'verified' else 0,
            'vram_gb': vram_gb,
            'metadata_json': json.dumps(
                {
                    'config': {
                        'model_type': model_type,
                        'architectures': architectures,
                    },
                    'runtime_support': {
                        'supported_model_type': model_type in SUPPORTED_MODEL_TYPES,
                        'supported_architectures': [
                            architecture for architecture in architectures if architecture in SUPPORTED_ARCHITECTURES
                        ],
                        'unsupported_architectures': unsupported_architectures,
                    },
                    **(
                        {
                            'scheduler_root': {
                                'root_id': normalized_value.partition(':')[0],
                                'relative_path': normalized_value.partition(':')[2],
                                'resolved_path': str(self._resolve_scheduler_root_path(normalized_value)),
                            }
                        }
                        if source_type == 'scheduler_root'
                        else {
                            'url_import': {
                                'url': normalized_value,
                                'resolved_path': str(self._import_url_model(normalized_value)),
                            }
                        }
                        if source_type == 'url'
                        else {}
                    ),
                },
                ensure_ascii=False,
            ),
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
                'name': self._runtime_model_name_for_row(row),
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

    def _runtime_model_name_for_row(self, row: dict[str, Any]) -> str:
        source_type = str(row.get('source_type') or '')
        source_value = str(row.get('source_value') or '')
        if source_type == 'scheduler_root':
            try:
                return str(self._resolve_scheduler_root_path(source_value))
            except Exception:
                return source_value
        if source_type == 'url':
            try:
                return str(self._import_url_model(source_value))
            except Exception:
                return source_value
        return source_value

    def export_models(self) -> list[dict[str, Any]]:
        return [
            {
                'source_type': str(row.get('source_type') or ''),
                'source_value': str(row.get('source_value') or ''),
                'display_name': str(row.get('display_name') or ''),
            }
            for row in self.list_models()
            if bool(row.get('enabled'))
        ]

    def search_huggingface_models(self, query: str, limit: int = 8, offset: int = 0) -> dict[str, Any]:
        normalized_query = str(query or '').strip()
        if not normalized_query:
            raise ValueError('query is required')
        normalized_limit = max(1, min(int(limit or 8), 20))
        normalized_offset = max(0, int(offset or 0))
        cached = self._get_cached_hf_search(normalized_query, normalized_limit, normalized_offset)
        if cached is not None:
            return cached
        raw_limit = min(max((normalized_offset + normalized_limit) * 8, 20), 250)

        normalized_query_lower = normalized_query.lower()
        query_author = ''
        query_search = normalized_query
        if '/' in normalized_query:
            candidate_author, _, candidate_search = normalized_query.partition('/')
            if str(candidate_author or '').strip() and str(candidate_search or '').strip():
                query_author = str(candidate_author or '').strip()
                query_search = str(candidate_search or '').strip()

        candidate_requests: list[tuple[str, str | None]] = []
        if query_author:
            candidate_requests.append((query_search, query_author))
        elif all(ch not in normalized_query for ch in (' ', '\t', '\n', '/')):
            for author_candidate in (
                normalized_query,
                normalized_query_lower,
                normalized_query.title(),
                normalized_query.upper(),
            ):
                normalized_author_candidate = str(author_candidate or '').strip()
                if normalized_author_candidate:
                    candidate_requests.append((normalized_query, normalized_author_candidate))
        candidate_requests.append((normalized_query, None))

        model_ids: list[str] = []
        seen_candidate_requests: set[tuple[str, str]] = set()

        def extend_model_ids_from_payload(payload: Any) -> None:
            for item in payload or []:
                model_id = str((item or {}).get('id') or '').strip()
                if model_id:
                    model_ids.append(model_id)
                if len(model_ids) >= raw_limit:
                    return

        try:
            from huggingface_hub import HfApi  # type: ignore

            api = HfApi()
            for search_text, author in candidate_requests:
                request_key = (str(search_text or '').strip().lower(), str(author or '').strip().lower())
                if request_key in seen_candidate_requests:
                    continue
                seen_candidate_requests.add(request_key)
                request_kwargs: dict[str, Any] = {
                    'search': search_text,
                    'limit': raw_limit,
                }
                if author:
                    request_kwargs['author'] = author
                for item in api.list_models(**request_kwargs):
                    model_id = str(getattr(item, 'id', '') or '').strip()
                    if model_id:
                        model_ids.append(model_id)
                    if len(model_ids) >= raw_limit:
                        break
                if len(model_ids) >= raw_limit:
                    break
        except Exception as e:
            logger.debug('Falling back to HF REST search for %r: %s', normalized_query, e)
            for search_text, author in candidate_requests:
                request_key = (str(search_text or '').strip().lower(), str(author or '').strip().lower())
                if request_key in seen_candidate_requests:
                    continue
                seen_candidate_requests.add(request_key)
                params = {
                    'search': search_text,
                    'limit': raw_limit,
                }
                if author:
                    params['author'] = author
                url = "https://huggingface.co/api/models?" + urllib.parse.urlencode(params)
                with urllib.request.urlopen(url, timeout=10) as response:
                    payload = json.loads(response.read().decode('utf-8'))
                extend_model_ids_from_payload(payload)
                if len(model_ids) >= raw_limit:
                    break

        seen: set[str] = set()
        validated_results: list[dict[str, Any]] = []
        for model_id in model_ids:
            if model_id in seen:
                continue
            seen.add(model_id)
            try:
                validation = self._validate_model('huggingface', model_id)
                validation_status = str(validation.get('validation_status') or '')
                if validation_status not in {'verified', 'config_only'}:
                    continue
                model_id_lower = model_id.lower()
                repo_name = model_id_lower.split('/', 1)[-1]
                score = 0
                if model_id_lower.startswith(f'{normalized_query_lower}/'):
                    score += 40
                if repo_name.startswith(normalized_query_lower):
                    score += 30
                if normalized_query_lower in repo_name:
                    score += 20
                if normalized_query_lower in model_id_lower:
                    score += 10
                if validation_status == 'verified':
                    score += 100
                validated_results.append(
                    {
                        'source_type': 'huggingface',
                        'source_value': model_id,
                        'display_name': model_id,
                        'validation_status': validation_status,
                        'validation_message': validation['validation_message'],
                        'detected_model_type': validation['detected_model_type'],
                        'supports_sharding': bool(validation['supports_sharding']),
                        'vram_gb': int(validation['vram_gb'] or 0),
                        '_score': score,
                    }
                )
                if len(validated_results) >= max(normalized_offset + normalized_limit, normalized_limit * 3):
                    break
            except Exception:
                continue
        validated_results.sort(
            key=lambda item: (
                -int(item.get('_score') or 0),
                str(item.get('source_value') or '').lower(),
            )
        )
        for item in validated_results:
            item.pop('_score', None)
        page_items = validated_results[normalized_offset: normalized_offset + normalized_limit]
        payload = {
            'items': page_items,
            'next_offset': normalized_offset + len(page_items),
            'has_more': len(validated_results) > normalized_offset + normalized_limit or len(model_ids) >= raw_limit,
        }
        self._set_cached_hf_search(normalized_query, normalized_limit, normalized_offset, payload)
        return payload

    @staticmethod
    def _model_id(source_type: str, source_value: str) -> str:
        return f"custom-{uuid.uuid5(uuid.NAMESPACE_URL, f'{source_type}:{source_value}')}"

    def add_model(self, *, source_type: str, source_value: str, display_name: str = '') -> dict[str, Any]:
        normalized_type = self._normalize_source_type(source_type)
        raw_normalized_value = self._normalize_source_value(source_value)
        normalized_value = (
            self._normalize_scheduler_root_value(raw_normalized_value)
            if normalized_type == 'scheduler_root'
            else self._normalize_url_value(raw_normalized_value)
            if normalized_type == 'url'
            else raw_normalized_value
        )
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

    def replace_models(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_items: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for raw in items or []:
            source_type = self._normalize_source_type(str((raw or {}).get('source_type') or ''))
            source_value = self._normalize_source_value(str((raw or {}).get('source_value') or ''))
            display_name = str((raw or {}).get('display_name') or '').strip()
            key = (source_type, source_value)
            if key in seen:
                continue
            seen.add(key)
            normalized_items.append(
                {
                    'source_type': source_type,
                    'source_value': source_value,
                    'display_name': display_name,
                }
            )

        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM custom_models")
            conn.commit()

        for item in normalized_items:
            self.add_model(
                source_type=item['source_type'],
                source_value=item['source_value'],
                display_name=item['display_name'],
            )
        return self.list_models()

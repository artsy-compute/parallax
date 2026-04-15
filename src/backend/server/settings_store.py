import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)


class SettingsStore:
    DEFAULT_CLUSTER_ID = 'cluster-default'

    def __init__(self, db_path: str | None = None):
        if db_path is None:
            db_path = os.environ.get('PARALLAX_SETTINGS_DB', '/tmp/parallax_settings.sqlite3')
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
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL DEFAULT '{}',
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS managed_node_hosts (
                    id TEXT PRIMARY KEY,
                    ssh_target TEXT NOT NULL UNIQUE,
                    parallax_path TEXT NOT NULL DEFAULT '',
                    hostname_hint TEXT NOT NULL DEFAULT '',
                    position INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            conn.commit()

    @staticmethod
    def _normalize_hostname(value: str) -> str:
        text = (value or '').strip().lower()
        if not text:
            return ''
        if '@' in text:
            text = text.split('@', 1)[1]
        if text.startswith('['):
            closing = text.find(']')
            if closing > 0:
                return text[1:closing].strip().lower()
        if text.count(':') == 1:
            host, _, port = text.partition(':')
            if port.isdigit():
                text = host
        return text.strip().lower()

    @staticmethod
    def _default_cluster_settings() -> dict[str, Any]:
        return {
            'id': SettingsStore.DEFAULT_CLUSTER_ID,
            'name': 'Primary cluster',
            'model_name': '',
            'init_nodes_num': 1,
            'is_local_network': True,
            'network_type': 'local',
            'advanced': {},
        }

    @classmethod
    def _normalize_cluster_settings(cls, value: dict[str, Any] | None, *, fallback_id: str | None = None, fallback_name: str | None = None) -> dict[str, Any]:
        raw = dict(value or {})
        normalized = cls._default_cluster_settings()
        normalized['id'] = str(raw.get('id') or fallback_id or normalized['id']).strip() or cls.DEFAULT_CLUSTER_ID
        normalized['name'] = str(raw.get('name') or fallback_name or normalized['name']).strip() or f"Cluster {normalized['id'][-4:]}"
        normalized['model_name'] = str(raw.get('model_name') or '').strip()
        try:
            normalized['init_nodes_num'] = max(1, int(raw.get('init_nodes_num') or 1))
        except Exception:
            normalized['init_nodes_num'] = 1
        normalized['is_local_network'] = bool(raw.get('is_local_network', True))
        network_type = str(raw.get('network_type') or '').strip().lower()
        if network_type not in {'local', 'remote'}:
            network_type = 'local' if normalized['is_local_network'] else 'remote'
        normalized['network_type'] = network_type
        normalized['is_local_network'] = network_type == 'local'
        advanced = raw.get('advanced')
        normalized['advanced'] = dict(advanced) if isinstance(advanced, dict) else {}
        return normalized

    @classmethod
    def _default_clusters_state(cls) -> dict[str, Any]:
        default_cluster = cls._default_cluster_settings()
        return {
            'active_cluster_id': default_cluster['id'],
            'clusters': [default_cluster],
        }

    @classmethod
    def _normalize_clusters_state(cls, value: dict[str, Any] | None) -> dict[str, Any]:
        raw = dict(value or {})
        raw_clusters = raw.get('clusters')
        clusters: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        if isinstance(raw_clusters, list):
            for index, item in enumerate(raw_clusters, start=1):
                if not isinstance(item, dict):
                    continue
                cluster = cls._normalize_cluster_settings(
                    item,
                    fallback_id=f"cluster-{index}",
                    fallback_name=f"Cluster {index}",
                )
                if cluster['id'] in seen_ids:
                    continue
                seen_ids.add(cluster['id'])
                clusters.append(cluster)
        if not clusters:
            clusters = [cls._default_cluster_settings()]
        active_cluster_id = str(raw.get('active_cluster_id') or '').strip()
        if active_cluster_id not in {item['id'] for item in clusters}:
            active_cluster_id = str(clusters[0]['id'])
        return {
            'active_cluster_id': active_cluster_id,
            'clusters': clusters,
        }

    def _get_json(self, key: str, default: dict[str, Any]) -> dict[str, Any]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM app_settings WHERE key = ?",
                (str(key or '').strip(),),
            ).fetchone()
        if row is None:
            return dict(default)
        try:
            parsed = json.loads(str(row['value_json'] or '{}'))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            logger.warning("Failed to parse stored settings for key %s", key, exc_info=True)
        return dict(default)

    def _set_json(self, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value or {}, ensure_ascii=False)
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO app_settings (key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json=excluded.value_json,
                    updated_at=excluded.updated_at
                """,
                (str(key or '').strip(), payload, now),
            )
            conn.commit()

    def get_cluster_settings(self) -> dict[str, Any]:
        state = self.get_clusters_state()
        active_cluster_id = str(state.get('active_cluster_id') or '')
        for cluster in state.get('clusters') or []:
            if str(cluster.get('id') or '') == active_cluster_id:
                return dict(cluster)
        return dict((state.get('clusters') or [self._default_cluster_settings()])[0])

    def set_cluster_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        state = self.get_clusters_state()
        active_cluster_id = str(state.get('active_cluster_id') or '')
        updated_clusters: list[dict[str, Any]] = []
        next_active_cluster: dict[str, Any] | None = None
        for cluster in state.get('clusters') or []:
            if str(cluster.get('id') or '') == active_cluster_id:
                merged = {**cluster, **dict(settings or {})}
                normalized = self._normalize_cluster_settings(
                    merged,
                    fallback_id=str(cluster.get('id') or ''),
                    fallback_name=str(cluster.get('name') or ''),
                )
                updated_clusters.append(normalized)
                next_active_cluster = normalized
            else:
                updated_clusters.append(dict(cluster))
        if next_active_cluster is None:
            next_active_cluster = self._normalize_cluster_settings(settings)
            updated_clusters = [next_active_cluster, *updated_clusters]
            active_cluster_id = str(next_active_cluster['id'])
        self._save_clusters_state({
            'active_cluster_id': active_cluster_id,
            'clusters': updated_clusters,
        })
        return dict(next_active_cluster)

    def get_clusters_state(self) -> dict[str, Any]:
        legacy_cluster_settings = self._get_json('cluster_settings', self._default_cluster_settings())
        state = self._get_json('clusters_state', {})
        if not state:
            state = {
                'active_cluster_id': str(legacy_cluster_settings.get('id') or self.DEFAULT_CLUSTER_ID),
                'clusters': [legacy_cluster_settings],
            }
        normalized = self._normalize_clusters_state(state)
        self._save_clusters_state(normalized)
        return normalized

    def _save_clusters_state(self, state: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_clusters_state(state)
        self._set_json('clusters_state', normalized)
        active_cluster = next(
            (item for item in normalized['clusters'] if item['id'] == normalized['active_cluster_id']),
            normalized['clusters'][0],
        )
        self._set_json('cluster_settings', active_cluster)
        return normalized

    def replace_clusters_state(self, clusters: list[dict[str, Any]], active_cluster_id: str | None = None) -> dict[str, Any]:
        return self._save_clusters_state({
            'active_cluster_id': str(active_cluster_id or '').strip(),
            'clusters': list(clusters or []),
        })

    def set_active_cluster_id(self, cluster_id: str) -> dict[str, Any]:
        state = self.get_clusters_state()
        target_id = str(cluster_id or '').strip()
        if target_id not in {str(item.get('id') or '') for item in state.get('clusters') or []}:
            target_id = str((state.get('clusters') or [self._default_cluster_settings()])[0]['id'])
        return self._save_clusters_state({
            'active_cluster_id': target_id,
            'clusters': state.get('clusters') or [],
        })

    def list_managed_node_hosts(self, joined_hostnames: set[str] | None = None) -> list[dict[str, Any]]:
        joined_hostnames = {str(item or '').strip().lower() for item in (joined_hostnames or set()) if str(item or '').strip()}
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, ssh_target, parallax_path, hostname_hint, position, created_at, updated_at
                FROM managed_node_hosts
                ORDER BY position ASC, created_at ASC
                """
            ).fetchall()
        hosts: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=1):
            hostname_hint = str(row['hostname_hint'] or '')
            hosts.append(
                {
                    'id': str(row['id'] or ''),
                    'ssh_target': str(row['ssh_target'] or ''),
                    'parallax_path': str(row['parallax_path'] or ''),
                    'hostname_hint': hostname_hint,
                    'line_number': index,
                    'joined': bool(hostname_hint) and hostname_hint in joined_hostnames,
                }
            )
        return hosts

    def replace_managed_node_hosts(self, hosts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_hosts: list[dict[str, Any]] = []
        seen_targets: set[str] = set()
        now = time.time()
        for index, raw_host in enumerate(hosts or [], start=1):
            ssh_target = str((raw_host or {}).get('ssh_target') or '').strip()
            parallax_path = str((raw_host or {}).get('parallax_path') or '').strip()
            if not ssh_target or ssh_target in seen_targets:
                continue
            seen_targets.add(ssh_target)
            normalized_hosts.append(
                {
                    'id': str((raw_host or {}).get('id') or f"host-{uuid.uuid5(uuid.NAMESPACE_URL, ssh_target)}"),
                    'ssh_target': ssh_target,
                    'parallax_path': parallax_path,
                    'hostname_hint': self._normalize_hostname(ssh_target),
                    'position': index,
                    'created_at': now,
                    'updated_at': now,
                }
            )

        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM managed_node_hosts")
            conn.executemany(
                """
                INSERT INTO managed_node_hosts (
                    id, ssh_target, parallax_path, hostname_hint, position, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item['id'],
                        item['ssh_target'],
                        item['parallax_path'],
                        item['hostname_hint'],
                        item['position'],
                        item['created_at'],
                        item['updated_at'],
                    )
                    for item in normalized_hosts
                ],
            )
            conn.commit()
        return self.list_managed_node_hosts()

    def import_nodes_host_file(self, nodes_host_file: str | None) -> list[dict[str, Any]]:
        if not nodes_host_file:
            return self.list_managed_node_hosts()
        path = Path(nodes_host_file).expanduser()
        if not path.exists():
            return self.list_managed_node_hosts()
        loaded_hosts: list[dict[str, Any]] = []
        for raw_line in path.read_text().splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            entry = stripped.split()[0]
            ssh_target, parallax_path = self._parse_inventory_entry(entry)
            if not ssh_target:
                continue
            loaded_hosts.append({'ssh_target': ssh_target, 'parallax_path': parallax_path})
        if not loaded_hosts:
            return self.list_managed_node_hosts()
        logger.info('Imported %d node host(s) from legacy file %s into settings DB', len(loaded_hosts), path)
        return self.replace_managed_node_hosts(loaded_hosts)

    @staticmethod
    def _parse_inventory_entry(raw_value: str) -> tuple[str, str]:
        stripped = (raw_value or '').strip()
        if not stripped:
            return '', ''
        if ':' not in stripped:
            return stripped, ''
        ssh_target, parallax_path = stripped.rsplit(':', 1)
        return ssh_target.strip(), parallax_path.strip()

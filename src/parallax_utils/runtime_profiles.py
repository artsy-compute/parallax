from __future__ import annotations

import json
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from parallax_utils.file_util import get_project_root
from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_RUNTIME_PROFILE = "auto"
PROFILE_DIR = get_project_root() / "profiles" / "runtime"


@dataclass(frozen=True)
class RuntimeProfile:
    requested_name: str
    resolved_name: str
    description: str
    scheduler_heartbeat_timeout_sec: float
    node_heartbeat_interval_sec: float
    node_heartbeat_rpc_timeout_sec: float
    force_rejoin_threshold: int
    force_rejoin_cooldown_sec: float
    local_http_retry_attempts: int
    local_http_retry_delay_sec: float


class RuntimeProfileError(RuntimeError):
    pass


def infer_is_local_network(*, scheduler_addr: Optional[str], relay_servers: Optional[list[str]]) -> bool:
    if relay_servers:
        return False
    if scheduler_addr is None or scheduler_addr == "auto":
        return True
    return str(scheduler_addr).startswith("/")


def detect_runtime_profile_name(*, is_local_network: Optional[bool] = None) -> str:
    if is_local_network is False:
        return "remote"
    system = platform.system().lower()
    if system == "darwin":
        return "mac-local"
    return "local-fast"


def _load_profile_json(profile_name: str) -> dict:
    path = PROFILE_DIR / f"{profile_name}.json"
    if not path.exists():
        raise RuntimeProfileError(
            f"Runtime profile '{profile_name}' not found at {path}. Available profiles: {', '.join(list_runtime_profiles())}"
        )
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        raise RuntimeProfileError(f"Failed to load runtime profile '{profile_name}': {exc}") from exc


def list_runtime_profiles() -> list[str]:
    if not PROFILE_DIR.exists():
        return []
    return sorted(path.stem for path in PROFILE_DIR.glob('*.json'))


def resolve_runtime_profile(
    requested_name: Optional[str] = None,
    *,
    is_local_network: Optional[bool] = None,
) -> RuntimeProfile:
    requested = requested_name or DEFAULT_RUNTIME_PROFILE
    resolved = detect_runtime_profile_name(is_local_network=is_local_network) if requested == DEFAULT_RUNTIME_PROFILE else requested
    data = _load_profile_json(resolved)
    profile = RuntimeProfile(
        requested_name=requested,
        resolved_name=resolved,
        description=data.get('description', ''),
        scheduler_heartbeat_timeout_sec=float(data['scheduler_heartbeat_timeout_sec']),
        node_heartbeat_interval_sec=float(data['node_heartbeat_interval_sec']),
        node_heartbeat_rpc_timeout_sec=float(data['node_heartbeat_rpc_timeout_sec']),
        force_rejoin_threshold=int(data['force_rejoin_threshold']),
        force_rejoin_cooldown_sec=float(data['force_rejoin_cooldown_sec']),
        local_http_retry_attempts=int(data['local_http_retry_attempts']),
        local_http_retry_delay_sec=float(data['local_http_retry_delay_sec']),
    )
    logger.info(
        "Resolved runtime profile: requested=%s resolved=%s is_local_network=%s",
        requested,
        resolved,
        is_local_network,
    )
    return profile

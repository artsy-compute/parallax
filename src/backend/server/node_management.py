import shlex
import subprocess
import time
import json
from typing import Any

from backend.server.node_lifecycle import build_node_lifecycle
from backend.server.static_config import get_node_join_command
from parallax.cli import PUBLIC_INITIAL_PEERS, PUBLIC_RELAY_SERVERS
from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)


class NodeManagementService:
    PROCESS_STATUS_TTL_SEC = 20.0
    ACTION_OVERRIDE_TTL_SEC = 15.0

    def __init__(self, scheduler_manage=None):
        self.scheduler_manage = scheduler_manage
        self._process_status_cache: dict[str, dict[str, Any]] = {}

    def set_scheduler_manage(self, scheduler_manage) -> None:
        self.scheduler_manage = scheduler_manage

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

    def _live_nodes(self) -> list[dict[str, Any]]:
        if self.scheduler_manage is None:
            return []
        try:
            return list(self.scheduler_manage.get_node_list())
        except Exception:
            logger.warning('Failed to read live node list for node management', exc_info=True)
            return []

    def _configured_hosts(self) -> list[dict[str, Any]]:
        if self.scheduler_manage is None:
            return []
        try:
            return list(self.scheduler_manage.get_configured_node_hosts())
        except Exception:
            logger.warning('Failed to read configured node hosts for node management', exc_info=True)
            return []

    def _configured_host_for_target(self, ssh_target: str) -> dict[str, Any] | None:
        target = (ssh_target or '').strip()
        for item in self._configured_hosts():
            if str(item.get('ssh_target') or '').strip() == target:
                return item
        return None

    def _cached_process_status(self, ssh_target: str) -> dict[str, Any] | None:
        target = (ssh_target or '').strip()
        cached = self._process_status_cache.get(target)
        if not cached:
            return None
        checked_at = float(cached.get('checked_at') or 0.0)
        if (time.time() - checked_at) > self.PROCESS_STATUS_TTL_SEC:
            return None
        return cached

    def _recent_action_override(self, ssh_target: str) -> dict[str, Any] | None:
        cached = self._cached_process_status(ssh_target)
        if not cached:
            return None
        source = str(cached.get('source') or '')
        if source not in {'action', 'action_pending'}:
            return None
        checked_at = float(cached.get('checked_at') or 0.0)
        if (time.time() - checked_at) > self.ACTION_OVERRIDE_TTL_SEC:
            return None
        return cached

    def _store_process_status(self, ssh_target: str, status: dict[str, Any]) -> dict[str, Any]:
        payload = {
            'running': bool(status.get('running')),
            'confirmed_running': bool(status.get('confirmed_running', status.get('running'))),
            'pid': str(status.get('pid') or ''),
            'source': str(status.get('source') or 'none'),
            'message': str(status.get('message') or ''),
            'checked_at': float(status.get('checked_at') or time.time()),
        }
        self._process_status_cache[(ssh_target or '').strip()] = payload
        return payload

    def _probe_host_process(self, ssh_target: str, parallax_path: str) -> dict[str, Any]:
        cached = self._cached_process_status(ssh_target)
        if cached is not None:
            return cached

        pid_file, _ = self._node_action_paths(parallax_path)
        quoted_pid = shlex.quote(pid_file)
        remote_command = f"""bash -lc '
set +e
if [ -f {quoted_pid} ]; then
  pid=$(cat {quoted_pid} 2>/dev/null || true)
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "__PARALLAX_NODE_PROCESS__:running:pidfile:$pid"
    exit 0
  fi
  rm -f {quoted_pid}
fi
echo "__PARALLAX_NODE_PROCESS__:stopped"
exit 0
'"""
        result = self._run_ssh_command((ssh_target or '').strip(), remote_command, timeout_sec=8)
        stdout = str(result.get('stdout') or '')
        marker = '__PARALLAX_NODE_PROCESS__:'
        checked_at = time.time()
        if marker in stdout:
            line = next((ln for ln in stdout.splitlines() if ln.startswith(marker)), '')
            status = line[len(marker):].strip()
            if status.startswith('running:'):
                parts = status.split(':', 2)
                source = parts[1] if len(parts) > 1 else 'unknown'
                pid = parts[2] if len(parts) > 2 else ''
                return self._store_process_status(ssh_target, {
                    'running': True,
                    'confirmed_running': True,
                    'pid': pid,
                    'source': source or 'unknown',
                    'message': 'Node process detected via pid file',
                    'checked_at': checked_at,
                })
            return self._store_process_status(ssh_target, {
                'running': False,
                'confirmed_running': False,
                'pid': '',
                'source': 'none',
                'message': 'No remote node process detected',
                'checked_at': checked_at,
            })
        return self._store_process_status(ssh_target, {
            'running': False,
            'confirmed_running': False,
            'pid': '',
            'source': 'probe_error',
            'message': str(result.get('message') or 'SSH process probe failed'),
            'checked_at': checked_at,
        })

    def get_overview(self) -> dict[str, Any]:
        live_nodes = self._live_nodes()
        configured_hosts = self._configured_hosts()
        matched_live_node_keys: set[str] = set()
        live_node_entries: list[dict[str, Any]] = []
        live_by_hostname: dict[str, list[dict[str, Any]]] = {}
        for index, node in enumerate(live_nodes, start=1):
            match_key = str(node.get('node_id') or '').strip() or f"live-{index}:{self._normalize_hostname(str(node.get('hostname') or ''))}"
            entry = {
                'match_key': match_key,
                'node': node,
            }
            live_node_entries.append(entry)
            key = self._normalize_hostname(str(node.get('hostname') or ''))
            if key:
                live_by_hostname.setdefault(key, []).append(entry)

        host_entries: list[dict[str, Any]] = []
        for index, item in enumerate(configured_hosts, start=1):
            hostname_hint = self._normalize_hostname(str(item.get('hostname_hint') or ''))
            ssh_target = str(item.get('ssh_target') or '')
            management_mode = str(item.get('management_mode') or ('ssh_managed' if ssh_target else 'manual')).strip()
            action_override = self._recent_action_override(ssh_target)
            live_match = None
            live_match_key = ''
            for candidate in live_by_hostname.get(hostname_hint, []):
                candidate_key = str(candidate.get('match_key') or '')
                if candidate_key and candidate_key not in matched_live_node_keys:
                    live_match = dict(candidate.get('node') or {})
                    live_match_key = candidate_key
                    matched_live_node_keys.add(candidate_key)
                    break
            if action_override and not bool(action_override.get('running')):
                live_match = None
            can_remote_manage = management_mode == 'ssh_managed' and bool(item.get('ssh_target')) and bool(item.get('parallax_path'))
            process_status = {
                'running': bool(live_match),
                'confirmed_running': bool(live_match),
                'pid': '',
                'source': 'joined' if live_match else 'unknown',
                'message': 'Node is joined to the scheduler' if live_match else 'Remote process status unavailable',
                'checked_at': time.time() if live_match else 0.0,
            }
            if action_override is not None:
                process_status = action_override
            elif can_remote_manage and not live_match:
                process_status = self._probe_host_process(
                    ssh_target,
                    str(item.get('parallax_path') or ''),
                )
            lifecycle = build_node_lifecycle(
                management_mode='ssh_managed' if can_remote_manage else 'manual',
                process_status=process_status,
                scheduler_joined=bool(live_match),
                scheduler_node_id=live_match.get('node_id') if live_match else None,
                runtime_status=live_match.get('status') if live_match else None,
                serving_start_layer=live_match.get('start_layer') if live_match else None,
                serving_end_layer=live_match.get('end_layer') if live_match else None,
                serving_total_layers=live_match.get('total_layers') if live_match else None,
            )
            host_entries.append({
                'id': str(item.get('id') or item.get('ssh_target') or hostname_hint or item.get('line_number') or index),
                'display_name': str(item.get('display_name') or item.get('ssh_target') or item.get('hostname_hint') or 'Configured host'),
                'ssh_target': ssh_target,
                'hostname_hint': str(item.get('hostname_hint') or ''),
                'parallax_path': str(item.get('parallax_path') or ''),
                'management_mode': management_mode,
                'network_scope': str(item.get('network_scope') or 'remote'),
                'inventory_source': 'configured',
                'joined': bool(live_match),
                'ssh_reachable': None,
                'last_ping_ok': None,
                'last_ping_message': '',
                'runtime': {
                    'node_id': live_match.get('node_id') if live_match else None,
                    'status': live_match.get('status') if live_match else 'waiting',
                    'hostname': live_match.get('hostname') if live_match else None,
                    'gpu_name': live_match.get('gpu_name') if live_match else None,
                    'gpu_memory': live_match.get('gpu_memory') if live_match else None,
                    'gpu_num': live_match.get('gpu_num') if live_match else None,
                    'start_layer': live_match.get('start_layer') if live_match else None,
                    'end_layer': live_match.get('end_layer') if live_match else None,
                    'total_layers': live_match.get('total_layers') if live_match else None,
                    'approx_remaining_context': live_match.get('approx_remaining_context') if live_match else None,
                },
                'system': {
                    'cpu_percent': live_match.get('cpu_percent') if live_match else None,
                    'ram_used_gb': live_match.get('ram_used_gb') if live_match else None,
                    'ram_total_gb': live_match.get('ram_total_gb') if live_match else None,
                    'ram_used_percent': live_match.get('ram_used_percent') if live_match else None,
                    'disk_used_gb': live_match.get('disk_used_gb') if live_match else None,
                    'disk_total_gb': live_match.get('disk_total_gb') if live_match else None,
                    'disk_used_percent': live_match.get('disk_used_percent') if live_match else None,
                },
                'host_process': process_status,
                'lifecycle': lifecycle,
                'actions': {
                    'can_ping': bool(item.get('ssh_target')),
                    'can_start': can_remote_manage and not bool(process_status.get('running')),
                    'can_stop': can_remote_manage and bool(process_status.get('confirmed_running')),
                    'can_restart': can_remote_manage and bool(process_status.get('confirmed_running')),
                    'can_tail_logs': can_remote_manage,
                },
                '_live_match': live_match,
                '_live_match_key': live_match_key,
            })

        # Fallback reconciliation: if a managed host was started from the UI but the node reports a
        # machine hostname that differs from the SSH target/alias, bind one unmatched live node back
        # onto that host instead of rendering a duplicate configured + live_only pair.
        unmatched_live_nodes = [
            entry for entry in live_node_entries
            if str(entry.get('match_key') or '') not in matched_live_node_keys
        ]
        fallback_candidates = [
            host for host in host_entries
            if host.get('inventory_source') == 'configured'
            and host.get('_live_match') is None
            and bool((host.get('actions') or {}).get('can_tail_logs'))
            and bool((host.get('host_process') or {}).get('running'))
        ]
        fallback_candidates.sort(
            key=lambda host: float(((host.get('host_process') or {}).get('checked_at')) or 0.0),
            reverse=True,
        )
        for host, live_entry in zip(fallback_candidates, unmatched_live_nodes):
            node_key = str(live_entry.get('match_key') or '')
            node = dict(live_entry.get('node') or {})
            if not node_key or node_key in matched_live_node_keys:
                continue
            matched_live_node_keys.add(node_key)
            process_status = dict(host.get('host_process') or {})
            process_status.update(
                {
                    'running': True,
                    'confirmed_running': bool(process_status.get('confirmed_running', True)),
                    'source': str(process_status.get('source') or 'joined'),
                    'message': str(process_status.get('message') or 'Node process detected on remote host'),
                    'checked_at': float(process_status.get('checked_at') or time.time()),
                }
            )
            host['joined'] = True
            host['runtime'] = {
                'node_id': node.get('node_id'),
                'status': node.get('status'),
                'hostname': node.get('hostname'),
                'gpu_name': node.get('gpu_name'),
                'gpu_memory': node.get('gpu_memory'),
                'gpu_num': node.get('gpu_num'),
                'start_layer': node.get('start_layer'),
                'end_layer': node.get('end_layer'),
                'total_layers': node.get('total_layers'),
                'approx_remaining_context': node.get('approx_remaining_context'),
            }
            host['system'] = {
                'cpu_percent': node.get('cpu_percent'),
                'ram_used_gb': node.get('ram_used_gb'),
                'ram_total_gb': node.get('ram_total_gb'),
                'ram_used_percent': node.get('ram_used_percent'),
                'disk_used_gb': node.get('disk_used_gb'),
                'disk_total_gb': node.get('disk_total_gb'),
                'disk_used_percent': node.get('disk_used_percent'),
            }
            host['host_process'] = process_status
            host['lifecycle'] = build_node_lifecycle(
                management_mode='ssh_managed' if bool((host.get('actions') or {}).get('can_tail_logs')) else 'manual',
                process_status=process_status,
                scheduler_joined=True,
                scheduler_node_id=node.get('node_id'),
                runtime_status=node.get('status'),
                serving_start_layer=node.get('start_layer'),
                serving_end_layer=node.get('end_layer'),
                serving_total_layers=node.get('total_layers'),
            )
            host['_live_match_key'] = node_key

        hosts: list[dict[str, Any]] = []
        for host in host_entries:
            host.pop('_live_match', None)
            host.pop('_live_match_key', None)
            hosts.append(host)

        for index, node in enumerate(live_nodes, start=1):
            node_key = str(node.get('node_id') or '').strip() or f"live-{index}:{self._normalize_hostname(str(node.get('hostname') or ''))}"
            if node_key in matched_live_node_keys:
                continue
            process_status = {
                'running': True,
                'confirmed_running': True,
                'pid': '',
                'source': 'joined',
                'message': 'Node is joined to the scheduler',
                'checked_at': time.time(),
            }
            lifecycle = build_node_lifecycle(
                management_mode='unmanaged',
                process_status=process_status,
                scheduler_joined=True,
                scheduler_node_id=node.get('node_id'),
                runtime_status=node.get('status'),
                serving_start_layer=node.get('start_layer'),
                serving_end_layer=node.get('end_layer'),
                serving_total_layers=node.get('total_layers'),
            )
            hosts.append({
                'id': str(node.get('node_id') or node_key or len(hosts)),
                'display_name': str(node.get('hostname') or node.get('node_id') or 'Live node'),
                'ssh_target': '',
                'hostname_hint': self._normalize_hostname(str(node.get('hostname') or '')),
                'inventory_source': 'live_only',
                'joined': True,
                'ssh_reachable': None,
                'last_ping_ok': None,
                'last_ping_message': '',
                'runtime': {
                    'node_id': node.get('node_id'),
                    'status': node.get('status'),
                    'hostname': node.get('hostname'),
                    'gpu_name': node.get('gpu_name'),
                    'gpu_memory': node.get('gpu_memory'),
                    'gpu_num': node.get('gpu_num'),
                    'start_layer': node.get('start_layer'),
                    'end_layer': node.get('end_layer'),
                    'total_layers': node.get('total_layers'),
                    'approx_remaining_context': node.get('approx_remaining_context'),
                },
                'system': {
                    'cpu_percent': node.get('cpu_percent'),
                    'ram_used_gb': node.get('ram_used_gb'),
                    'ram_total_gb': node.get('ram_total_gb'),
                    'ram_used_percent': node.get('ram_used_percent'),
                    'disk_used_gb': node.get('disk_used_gb'),
                    'disk_total_gb': node.get('disk_total_gb'),
                    'disk_used_percent': node.get('disk_used_percent'),
                },
                'host_process': process_status,
                'lifecycle': lifecycle,
                'actions': {
                    'can_ping': False,
                    'can_start': False,
                    'can_stop': False,
                    'can_restart': False,
                    'can_tail_logs': False,
                },
            })

        summary = {
            'configured_hosts': len(configured_hosts),
            'joined_hosts': sum(1 for host in hosts if host.get('joined')),
            'unjoined_configured_hosts': sum(1 for host in hosts if host.get('inventory_source') == 'configured' and not host.get('joined')),
            'live_only_hosts': sum(1 for host in hosts if host.get('inventory_source') == 'live_only'),
        }
        return {'summary': summary, 'hosts': hosts}

    def _run_ssh_command(self, ssh_target: str, remote_command: str, *, timeout_sec: int = 10) -> dict[str, Any]:
        target = (ssh_target or '').strip()
        if not target:
            return {'ok': False, 'message': 'ssh_target is required', 'ssh_target': target, 'stdout': '', 'stderr': '', 'return_code': None}

        command = [
            'ssh',
            '-o', 'BatchMode=yes',
            '-o', 'ConnectTimeout=5',
            target,
            remote_command,
        ]
        started = time.time()
        try:
            proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout_sec)
        except FileNotFoundError:
            return {'ok': False, 'message': 'ssh command is not available on the scheduler host', 'ssh_target': target, 'stdout': '', 'stderr': '', 'return_code': None}
        except subprocess.TimeoutExpired:
            return {'ok': False, 'message': 'SSH command timed out', 'ssh_target': target, 'stdout': '', 'stderr': '', 'return_code': None}
        except Exception as exc:
            return {'ok': False, 'message': f'SSH command failed: {exc}', 'ssh_target': target, 'stdout': '', 'stderr': '', 'return_code': None}

        latency_ms = int((time.time() - started) * 1000)
        stdout = (proc.stdout or '').strip()
        stderr = (proc.stderr or '').strip()
        ok = proc.returncode == 0
        message = 'SSH command succeeded' if ok else (stderr or stdout or f'ssh exited with code {proc.returncode}')
        return {
            'ok': ok,
            'message': message,
            'ssh_target': target,
            'stdout': stdout,
            'stderr': stderr,
            'latency_ms': latency_ms,
            'return_code': proc.returncode,
        }

    def _require_configured_host(self, ssh_target: str) -> tuple[str, dict[str, Any] | None, str]:
        target = (ssh_target or '').strip()
        if not target:
            return target, None, 'ssh_target is required'
        configured = self._configured_host_for_target(target)
        if configured is None:
            return target, None, 'ssh_target is not present in the configured node inventory'
        parallax_path = str(configured.get('parallax_path') or '').strip()
        if not parallax_path:
            return target, None, 'Configured host is missing PARALLAX_PATH'
        return target, configured, ''

    def _join_command(self) -> tuple[str, str]:
        if self.scheduler_manage is None:
            return '', 'Scheduler is not initialized'
        scheduler_addr = str(self.scheduler_manage.get_join_scheduler_addr() or '').strip()
        is_local_network = self.scheduler_manage.get_is_local_network()
        payload = get_node_join_command(scheduler_addr, is_local_network)
        if isinstance(payload, dict):
            return str(payload.get('command') or '').strip(), ''
        if isinstance(payload, str):
            return payload.strip(), ''
        return '', 'Scheduler is not ready to provide a join command'

    @staticmethod
    def _wrap_remote_login_shell(script: str) -> str:
        quoted_script = shlex.quote(str(script or '').strip())
        return f"""__parallax_shell="${{SHELL:-}}"
if [ -z "$__parallax_shell" ] || [ ! -x "$__parallax_shell" ]; then
  __parallax_shell=$(getent passwd "$(id -un 2>/dev/null || whoami)" 2>/dev/null | awk -F: 'NR==1{{print $7}}')
fi
if [ -z "$__parallax_shell" ] || [ ! -x "$__parallax_shell" ]; then
  __parallax_shell=$(dscl . -read "/Users/$(id -un 2>/dev/null || whoami)" UserShell 2>/dev/null | awk 'NR==1{{print $2}}')
fi
if [ -z "$__parallax_shell" ] || [ ! -x "$__parallax_shell" ]; then
  __parallax_shell=/bin/bash
fi
exec "$__parallax_shell" -lc {quoted_script}"""

    @staticmethod
    def _render_remote_join_command(join_command: str) -> str:
        tokens = shlex.split(str(join_command or '').strip())
        if not tokens:
            return ''
        if len(tokens) < 2 or tokens[0] != 'parallax' or tokens[1] != 'join':
            return shlex.join(tokens)

        passthrough = list(tokens[2:])
        use_relay = False
        filtered_args: list[str] = []
        i = 0
        while i < len(passthrough):
            token = passthrough[i]
            if token in {'-r', '--use-relay'}:
                use_relay = True
                i += 1
                continue
            filtered_args.append(token)
            i += 1

        launch_tokens = ['./venv/bin/python', 'src/parallax/launch.py', *filtered_args]
        if use_relay:
            launch_tokens.extend([
                '--relay-servers',
                *PUBLIC_RELAY_SERVERS,
                '--initial-peers',
                *PUBLIC_INITIAL_PEERS,
            ])
        return shlex.join(launch_tokens)

    @staticmethod
    def _node_action_paths(parallax_path: str) -> tuple[str, str]:
        base = parallax_path.rstrip('/')
        return f'{base}/logs/parallax-node.pid', f'{base}/logs/parallax-node-manager.log'

    def _run_node_action(self, ssh_target: str, action: str) -> dict[str, Any]:
        target, configured, error = self._require_configured_host(ssh_target)
        if configured is None:
            return {'ok': False, 'message': error, 'ssh_target': target, 'action': action}

        parallax_path = str(configured.get('parallax_path') or '').strip()
        pid_file, manager_log = self._node_action_paths(parallax_path)
        quoted_path = shlex.quote(parallax_path)
        quoted_pid = shlex.quote(pid_file)
        quoted_manager_log = shlex.quote(manager_log)

        if action == 'start':
            join_command, join_error = self._join_command()
            remote_join_command = self._render_remote_join_command(join_command)
            if not join_command or not remote_join_command:
                return {'ok': False, 'message': join_error or 'Scheduler is not ready to provide a join command', 'ssh_target': target, 'action': action}
            remote_command = self._wrap_remote_login_shell(f"""
set -e
cd {quoted_path}
mkdir -p logs
launch_shell="${{SHELL:-}}"
if [ -z "$launch_shell" ] || [ ! -x "$launch_shell" ]; then
  launch_shell=/bin/bash
fi
{{
  echo "=== PARALLAX NODE START ENV ==="
  echo "timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date)"
  echo "user=$(id -un 2>/dev/null || whoami)"
  echo "shell=${{SHELL:-unset}}"
  echo "launch_shell=$launch_shell"
  echo "pwd=$(pwd)"
  echo "path=$PATH"
  echo "which_python=$(command -v python 2>/dev/null || true)"
  echo "which_python3=$(command -v python3 2>/dev/null || true)"
  echo "which_parallax=$(command -v parallax 2>/dev/null || true)"
  ls -ld venv venv/bin venv/bin/activate venv/bin/parallax venv/bin/python venv/bin/python3 2>/dev/null || true
}} >> {quoted_manager_log} 2>&1
if [ ! -f venv/bin/activate ]; then
  echo "Missing venv/bin/activate" >&2
  exit 2
fi
if [ ! -x venv/bin/parallax ]; then
  echo "Missing executable venv/bin/parallax" >&2
  exit 2
fi
source venv/bin/activate
if [ "${{VIRTUAL_ENV:-}}" != "$(pwd)/venv" ] && [ "${{VIRTUAL_ENV:-}}" != "$PWD/venv" ]; then
  echo "Failed to activate expected venv: ${{VIRTUAL_ENV:-unset}}" >&2
  exit 2
fi
if [ "$(command -v python3 2>/dev/null || true)" != "$(pwd)/venv/bin/python3" ] && [ "$(command -v python 2>/dev/null || true)" != "$(pwd)/venv/bin/python" ]; then
  echo "Activated python is not from venv" >&2
  exit 2
fi
{{
  echo "=== PARALLAX NODE START ENV AFTER ACTIVATE ==="
  echo "virtual_env=${{VIRTUAL_ENV:-unset}}"
  echo "which_python=$(command -v python 2>/dev/null || true)"
  echo "which_python3=$(command -v python3 2>/dev/null || true)"
  echo "which_parallax=$(command -v parallax 2>/dev/null || true)"
  echo "remote_join_command={remote_join_command}"
}} >> {quoted_manager_log} 2>&1
if [ -f {quoted_pid} ]; then
  pid=$(cat {quoted_pid} 2>/dev/null || true)
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "__PARALLAX_NODE_ACTION__:already_running:$pid"
    exit 0
  fi
  rm -f {quoted_pid}
fi
if command -v setsid >/dev/null 2>&1; then
  nohup setsid "$launch_shell" -lc "cd {quoted_path} && source venv/bin/activate && exec {remote_join_command}" >> {quoted_manager_log} 2>&1 < /dev/null &
else
  nohup "$launch_shell" -lc "cd {quoted_path} && source venv/bin/activate && exec {remote_join_command}" >> {quoted_manager_log} 2>&1 < /dev/null &
fi
pid=$!
sleep 2
if ! kill -0 "$pid" 2>/dev/null; then
  echo "__PARALLAX_NODE_ACTION__:start_failed"
  tail -n 40 {quoted_manager_log} 2>/dev/null || true
  exit 1
fi
echo "$pid" > {quoted_pid}
echo "__PARALLAX_NODE_ACTION__:started:$pid"
""")
            result = self._run_ssh_command(target, remote_command, timeout_sec=20)
        elif action in ('stop', 'restart'):
            join_command, join_error = self._join_command() if action == 'restart' else ('', '')
            remote_join_command = self._render_remote_join_command(join_command) if action == 'restart' else ''
            if action == 'restart' and (not join_command or not remote_join_command):
                return {'ok': False, 'message': join_error or 'Scheduler is not ready to provide a join command', 'ssh_target': target, 'action': action}
            finish_block = ""
            if action == 'restart':
                finish_block = f"""
if [ ! -f venv/bin/activate ]; then
  echo "Missing venv/bin/activate" >&2
  exit 2
fi
if [ ! -x venv/bin/parallax ]; then
  echo "Missing executable venv/bin/parallax" >&2
  exit 2
fi
source venv/bin/activate
if [ "${{VIRTUAL_ENV:-}}" != "$(pwd)/venv" ] && [ "${{VIRTUAL_ENV:-}}" != "$PWD/venv" ]; then
  echo "Failed to activate expected venv: ${{VIRTUAL_ENV:-unset}}" >&2
  exit 2
fi
if [ "$(command -v python3 2>/dev/null || true)" != "$(pwd)/venv/bin/python3" ] && [ "$(command -v python 2>/dev/null || true)" != "$(pwd)/venv/bin/python" ]; then
  echo "Activated python is not from venv" >&2
  exit 2
fi
launch_shell="${{SHELL:-}}"
if [ -z "$launch_shell" ] || [ ! -x "$launch_shell" ]; then
  launch_shell=/bin/bash
fi
if command -v setsid >/dev/null 2>&1; then
  nohup setsid "$launch_shell" -lc "cd {quoted_path} && source venv/bin/activate && exec {remote_join_command}" >> {quoted_manager_log} 2>&1 < /dev/null &
else
  nohup "$launch_shell" -lc "cd {quoted_path} && source venv/bin/activate && exec {remote_join_command}" >> {quoted_manager_log} 2>&1 < /dev/null &
fi
new_pid=$!
sleep 2
if ! kill -0 "$new_pid" 2>/dev/null; then
  echo "__PARALLAX_NODE_ACTION__:restart_failed"
  tail -n 40 {quoted_manager_log} 2>/dev/null || true
  exit 1
fi
echo "$new_pid" > {quoted_pid}
echo "__PARALLAX_NODE_ACTION__:restarted:$new_pid"
"""
            else:
                finish_block = """
if [ -n "$stopped_pid" ]; then
  echo "__PARALLAX_NODE_ACTION__:stopped:$stopped_pid"
else
  echo "__PARALLAX_NODE_ACTION__:not_running"
fi
"""
            remote_command = self._wrap_remote_login_shell(f"""
set -e
cd {quoted_path}
mkdir -p logs
stopped_pid=""
if [ -f {quoted_pid} ]; then
  pid=$(cat {quoted_pid} 2>/dev/null || true)
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM -- -"$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL -- -"$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
    fi
    stopped_pid="$pid"
  fi
  rm -f {quoted_pid}
fi
{finish_block}
""")
            result = self._run_ssh_command(target, remote_command, timeout_sec=25)
        else:
            return {'ok': False, 'message': f'Unsupported node action: {action}', 'ssh_target': target, 'action': action}

        stdout = str(result.get('stdout') or '')
        marker = '__PARALLAX_NODE_ACTION__:'
        if marker in stdout:
            line = next((ln for ln in stdout.splitlines() if ln.startswith(marker)), '')
            status = line[len(marker):].strip() if line else ''
            if status.startswith('started:'):
                result['message'] = f"Node start requested (pid {status.split(':', 1)[1]})"
            elif status.startswith('restarted:'):
                result['message'] = f"Node restarted (pid {status.split(':', 1)[1]})"
            elif status.startswith('stopped:'):
                result['message'] = f"Node stopped ({status.split(':', 1)[1]})"
            elif status == 'not_running':
                result['message'] = 'Node is not running'
            elif status.startswith('already_running:'):
                result['message'] = f"Node is already running (pid {status.split(':', 1)[1]})"
            elif status == 'start_failed':
                result['ok'] = False
                result['message'] = stderr or stdout or 'Node failed to stay running after launch'
            elif status == 'restart_failed':
                result['ok'] = False
                result['message'] = stderr or stdout or 'Node failed to stay running after restart'
        checked_at = time.time()
        if action == 'start':
            if '__PARALLAX_NODE_ACTION__:started:' in stdout or '__PARALLAX_NODE_ACTION__:already_running:' in stdout:
                pid = ''
                for prefix in ('__PARALLAX_NODE_ACTION__:started:', '__PARALLAX_NODE_ACTION__:already_running:'):
                    if prefix in stdout:
                        line = next((ln for ln in stdout.splitlines() if ln.startswith(prefix)), '')
                        pid = line.split(':', 2)[-1].strip() if line else ''
                        break
                self._store_process_status(target, {
                    'running': True,
                    'pid': pid,
                    'confirmed_running': False,
                    'source': 'action_pending',
                    'message': result.get('message') or 'Node start requested',
                    'checked_at': checked_at,
                })
        elif action == 'restart':
            if '__PARALLAX_NODE_ACTION__:restarted:' in stdout:
                line = next((ln for ln in stdout.splitlines() if ln.startswith('__PARALLAX_NODE_ACTION__:restarted:')), '')
                self._store_process_status(target, {
                    'running': True,
                    'pid': line.split(':', 2)[-1].strip() if line else '',
                    'confirmed_running': False,
                    'source': 'action_pending',
                    'message': result.get('message') or 'Node restarted',
                    'checked_at': checked_at,
                })
        elif action == 'stop':
            if '__PARALLAX_NODE_ACTION__:stopped:' in stdout or '__PARALLAX_NODE_ACTION__:not_running' in stdout:
                self._store_process_status(target, {
                    'running': False,
                    'pid': '',
                    'confirmed_running': False,
                    'source': 'action',
                    'message': result.get('message') or 'Node stopped',
                    'checked_at': checked_at,
                })
        result['action'] = action
        return result

    def ping_host(self, ssh_target: str) -> dict[str, Any]:
        target = (ssh_target or '').strip()
        if not target:
            return {'ok': False, 'message': 'ssh_target is required', 'ssh_target': target}

        result = self._run_ssh_command(target, 'exit', timeout_sec=8)
        if result.get('ok'):
            result['message'] = 'SSH reachable'
        elif result.get('message') == 'SSH command timed out':
            result['message'] = 'SSH ping timed out'
        elif result.get('message', '').startswith('SSH command failed:'):
            result['message'] = result['message'].replace('SSH command failed:', 'SSH ping failed:', 1).strip()
        return result

    def probe_candidate_host(self, ssh_target: str, parallax_path: str) -> dict[str, Any]:
        target = (ssh_target or '').strip()
        path = (parallax_path or '').strip()
        if not target:
            return {'ok': False, 'message': 'ssh_target is required', 'ssh_target': target, 'parallax_path': path}
        if not path:
            return {'ok': False, 'message': 'PARALLAX_PATH is required', 'ssh_target': target, 'parallax_path': path}

        quoted_path = shlex.quote(path)
        remote_command = self._wrap_remote_login_shell(f"""
set +e
os_name="$(uname -s 2>/dev/null || echo unknown)"
remote_user="$(id -un 2>/dev/null || whoami 2>/dev/null || echo unknown)"
remote_host="$(hostname 2>/dev/null || echo unknown)"
if [ -f /proc/version ] && grep -qi microsoft /proc/version 2>/dev/null; then
  os_name="WSL"
fi
path_exists=0
has_venv_activate=0
has_parallax_bin=0
if [ -d {quoted_path} ]; then
  path_exists=1
fi
if [ -f {quoted_path}/venv/bin/activate ]; then
  has_venv_activate=1
fi
if [ -x {quoted_path}/venv/bin/parallax ]; then
  has_parallax_bin=1
fi
PROBE_OS="$os_name" \
PROBE_USER="$remote_user" \
PROBE_HOST="$remote_host" \
PROBE_PATH_EXISTS="$path_exists" \
PROBE_HAS_VENV_ACTIVATE="$has_venv_activate" \
PROBE_HAS_PARALLAX_BIN="$has_parallax_bin" \
python3 -c 'import json, os; print("__PARALLAX_NODE_PROBE__:" + json.dumps({{"os": os.environ.get("PROBE_OS", ""), "user": os.environ.get("PROBE_USER", ""), "host": os.environ.get("PROBE_HOST", ""), "path_exists": os.environ.get("PROBE_PATH_EXISTS", ""), "has_venv_activate": os.environ.get("PROBE_HAS_VENV_ACTIVATE", ""), "has_parallax_bin": os.environ.get("PROBE_HAS_PARALLAX_BIN", "")}}))'
exit 0
""")
        result = self._run_ssh_command(target, remote_command, timeout_sec=10)
        payload = {
            'ok': False,
            'message': str(result.get('message') or ''),
            'ssh_target': target,
            'parallax_path': path,
            'ssh_reachable': bool(result.get('ok')),
            'stdout': str(result.get('stdout') or ''),
            'stderr': str(result.get('stderr') or ''),
            'return_code': result.get('return_code'),
            'os_name': '',
            'remote_user': '',
            'remote_host': '',
            'path_exists': False,
            'has_venv_activate': False,
            'has_parallax_bin': False,
            'notes': [],
        }
        if not result.get('ok'):
            logger.info(
                'Node probe failed before remote inspection for ssh_target=%s parallax_path=%s return_code=%s stderr=%r stdout=%r',
                target,
                path,
                result.get('return_code'),
                result.get('stderr'),
                result.get('stdout'),
            )
            return payload

        marker = '__PARALLAX_NODE_PROBE__:'
        stdout = str(result.get('stdout') or '')
        line = next((ln for ln in stdout.splitlines() if ln.startswith(marker)), '')
        parsed: dict[str, str] = {}
        if line:
            try:
                raw_payload = json.loads(line[len(marker):].strip())
                if isinstance(raw_payload, dict):
                    parsed = {str(key).strip(): str(value).strip() for key, value in raw_payload.items()}
            except Exception:
                logger.warning('Failed to parse node probe marker for ssh_target=%s stdout=%r', target, stdout, exc_info=True)
        os_name = str(parsed.get('os') or '')
        remote_user = str(parsed.get('user') or '')
        remote_host = str(parsed.get('host') or '')
        path_exists = parsed.get('path_exists') == '1'
        has_venv_activate = parsed.get('has_venv_activate') == '1'
        has_parallax_bin = parsed.get('has_parallax_bin') == '1'

        notes: list[str] = []
        if os_name == 'Darwin':
            notes.append('On macOS, if node discovery fails after launch, allow the SSH-launched process to access the local network in Privacy & Security.')
        elif os_name == 'WSL':
            notes.append('If this node must join over the local network from WSL, use Mirrored networking instead of NAT so the scheduler stays reachable.')

        if not path_exists:
            message = 'SSH reachable, but PARALLAX_PATH does not exist on the remote host'
        elif not has_venv_activate:
            message = 'SSH reachable, but PARALLAX_PATH is missing venv/bin/activate'
        elif not has_parallax_bin:
            message = 'SSH reachable, but PARALLAX_PATH is missing executable venv/bin/parallax'
        else:
            message = 'SSH reachable and PARALLAX_PATH looks ready'

        payload.update({
            'ok': path_exists and has_venv_activate and has_parallax_bin,
            'message': message,
            'os_name': os_name,
            'remote_user': remote_user,
            'remote_host': remote_host,
            'path_exists': path_exists,
            'has_venv_activate': has_venv_activate,
            'has_parallax_bin': has_parallax_bin,
            'notes': notes,
        })
        logger.info(
            'Node probe result ssh_target=%s remote_session=%s@%s parallax_path=%s ok=%s path_exists=%s has_venv_activate=%s has_parallax_bin=%s return_code=%s stderr=%r stdout=%r',
            target,
            remote_user or 'unknown',
            remote_host or 'unknown',
            path,
            payload.get('ok'),
            path_exists,
            has_venv_activate,
            has_parallax_bin,
            payload.get('return_code'),
            payload.get('stderr'),
            payload.get('stdout'),
        )
        return payload

    def start_host(self, ssh_target: str) -> dict[str, Any]:
        return self._run_node_action(ssh_target, 'start')

    def stop_host(self, ssh_target: str) -> dict[str, Any]:
        return self._run_node_action(ssh_target, 'stop')

    def restart_host(self, ssh_target: str) -> dict[str, Any]:
        return self._run_node_action(ssh_target, 'restart')

    def tail_logs(self, ssh_target: str, lines: int = 200) -> dict[str, Any]:
        target = (ssh_target or '').strip()
        if not target:
            return {'ok': False, 'message': 'ssh_target is required', 'ssh_target': target, 'content': '', 'source': ''}

        safe_lines = max(20, min(int(lines or 200), 1000))
        configured = self._configured_host_for_target(target) or {}
        parallax_path = str(configured.get('parallax_path') or '').strip()
        preferred_logs_glob = f'"{parallax_path.rstrip("/")}/logs/*.log"' if parallax_path else ''
        remote_command = f"""bash -lc '
LINES={safe_lines}
for path in \
  "$PARALLAX_NODE_LOG_PATH" \
  {preferred_logs_glob} \
  /tmp/parallax-node.log \
  /tmp/parallax.log \
  /tmp/parallax/*.log \
  /var/tmp/parallax-node.log \
  /var/tmp/parallax.log \
  "$HOME/.parallax/parallax-node.log" \
  "$HOME/.parallax/parallax.log"; do
  if [ -n "$path" ] && ls $path >/dev/null 2>&1; then
    found=$(ls -t $path 2>/dev/null | head -n 1)
    echo "__PARALLAX_LOG_SOURCE__:$found"
    tail -n "$LINES" "$found"
    exit 0
  fi
done
if command -v journalctl >/dev/null 2>&1; then
  echo "__PARALLAX_LOG_SOURCE__:journalctl --user --no-pager -n $LINES"
  journalctl --user --no-pager -n "$LINES" 2>/dev/null && exit 0
fi
echo "No Parallax log file found on remote host. Checked common log paths and journalctl."
exit 2
'"""
        result = self._run_ssh_command(target, remote_command, timeout_sec=15)
        stdout = str(result.get('stdout') or '')
        source = ''
        content = stdout
        marker = '__PARALLAX_LOG_SOURCE__:'
        if stdout.startswith(marker):
            first_line, _, remainder = stdout.partition('\n')
            source = first_line[len(marker):].strip()
            content = remainder
        return {
            'ok': bool(result.get('ok')),
            'message': result.get('message') or ('Log tail fetched' if result.get('ok') else 'Failed to fetch logs'),
            'ssh_target': target,
            'source': source,
            'content': content,
            'stderr': result.get('stderr') or '',
            'latency_ms': result.get('latency_ms'),
            'return_code': result.get('return_code'),
        }

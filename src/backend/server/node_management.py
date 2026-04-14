import subprocess
import time
from typing import Any

from parallax_utils.logging_config import get_logger

logger = get_logger(__name__)


class NodeManagementService:
    def __init__(self, scheduler_manage=None):
        self.scheduler_manage = scheduler_manage

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

    def get_overview(self) -> dict[str, Any]:
        live_nodes = self._live_nodes()
        configured_hosts = self._configured_hosts()
        matched_live_node_ids: set[str] = set()
        live_by_hostname: dict[str, list[dict[str, Any]]] = {}
        for node in live_nodes:
            key = self._normalize_hostname(str(node.get('hostname') or ''))
            if key:
                live_by_hostname.setdefault(key, []).append(node)

        hosts: list[dict[str, Any]] = []
        for item in configured_hosts:
            hostname_hint = self._normalize_hostname(str(item.get('hostname_hint') or ''))
            live_match = None
            for candidate in live_by_hostname.get(hostname_hint, []):
                node_id = str(candidate.get('node_id') or '')
                if node_id and node_id not in matched_live_node_ids:
                    live_match = candidate
                    matched_live_node_ids.add(node_id)
                    break
            hosts.append({
                'id': str(item.get('ssh_target') or hostname_hint or item.get('line_number') or len(hosts)),
                'display_name': str(item.get('ssh_target') or item.get('hostname_hint') or 'Configured host'),
                'ssh_target': str(item.get('ssh_target') or ''),
                'hostname_hint': str(item.get('hostname_hint') or ''),
                'parallax_path': str(item.get('parallax_path') or ''),
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
                    'cpu': None,
                    'ram': None,
                    'disk': None,
                },
                'actions': {
                    'can_ping': bool(item.get('ssh_target')),
                    'can_start': False,
                    'can_stop': False,
                    'can_restart': False,
                    'can_tail_logs': bool(item.get('ssh_target')) and bool(item.get('parallax_path')),
                },
            })

        for node in live_nodes:
            node_id = str(node.get('node_id') or '')
            if node_id and node_id in matched_live_node_ids:
                continue
            hosts.append({
                'id': node_id or str(len(hosts)),
                'display_name': str(node.get('hostname') or node_id or 'Live node'),
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
                    'cpu': None,
                    'ram': None,
                    'disk': None,
                },
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

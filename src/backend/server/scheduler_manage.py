import json
import threading
import time
from pathlib import Path
from typing import List

from lattica import Lattica

from backend.server.constants import NODE_STATUS_AVAILABLE, NODE_STATUS_WAITING
from backend.server.node_lifecycle import build_node_lifecycle
from backend.server.rpc_connection_handler import RPCConnectionHandler
from backend.server.static_config import get_model_info, get_node_join_command
from parallax.cli import PUBLIC_INITIAL_PEERS, PUBLIC_RELAY_SERVERS
from parallax.p2p.server import TransformerConnectionHandler
from parallax_utils.logging_config import get_logger
from parallax_utils.runtime_profiles import DEFAULT_RUNTIME_PROFILE, resolve_runtime_profile
from scheduling.node import RequestSignal
from scheduling.scheduler import Scheduler

logger = get_logger(__name__)

SCHEDULER_STATE_PATH = Path("/tmp/parallax_scheduler_state.json")


class SchedulerManage:
    """
    Coordinates the in-process scheduler and the P2P RPC layer.

    This manager owns the `Scheduler` instance and the Lattica P2P node,
    wiring RPC calls from workers to scheduler events.
    """

    def __init__(
        self,
        initial_peers: List[str] = [],
        relay_servers: List[str] = [],
        dht_prefix: str = "gradient",
        host_maddrs: List[str] = [],
        announce_maddrs: List[str] = [],
        http_port: int = 3001,
        scheduler_host: str = "",
        use_hfcache: bool = False,
        enable_weight_refit: bool = False,
        weight_refit_mode: str = "disk",
        profile: str = DEFAULT_RUNTIME_PROFILE,
        scheduler_heartbeat_timeout_sec: float | None = None,
        nodes_host_file: str | None = None,
    ):
        """Initialize the manager with networking bootstrap parameters."""
        self.initial_peers = initial_peers
        self.relay_servers = relay_servers
        self.dht_prefix = dht_prefix
        self.host_maddrs = host_maddrs
        self.announce_maddrs = announce_maddrs
        self.http_port = http_port
        self.scheduler_host = scheduler_host
        self.use_hfcache = use_hfcache
        self.enable_weight_refit = enable_weight_refit
        self.weight_refit_mode = weight_refit_mode
        self.profile = profile or DEFAULT_RUNTIME_PROFILE
        self.scheduler_heartbeat_timeout_sec = scheduler_heartbeat_timeout_sec
        self.nodes_host_file = nodes_host_file
        self.configured_node_hosts = self._load_nodes_host_file(nodes_host_file)
        self.model_name = None
        self.init_nodes_num = None
        self.scheduler = None
        self.node_id = f"{dht_prefix}_announce"
        self.lattica = None
        self.stubs = {}
        self.is_local_network = False


    @staticmethod
    def _parse_inventory_entry(raw_value: str) -> tuple[str, str]:
        stripped = (raw_value or '').strip()
        if not stripped:
            return '', ''
        if ':' not in stripped:
            return stripped, ''
        ssh_target, parallax_path = stripped.rsplit(':', 1)
        return ssh_target.strip(), parallax_path.strip()

    @staticmethod
    def _normalize_inventory_hostname(target: str) -> str:
        value = (target or '').strip()
        if not value:
            return ''
        if '@' in value:
            value = value.split('@', 1)[1]
        if value.startswith('['):
            closing = value.find(']')
            if closing > 0:
                return value[1:closing].strip().lower()
        if value.count(':') == 1:
            host, _, port = value.partition(':')
            if port.isdigit():
                value = host
        return value.strip().lower()

    def _load_nodes_host_file(self, nodes_host_file: str | None) -> list[dict]:
        if not nodes_host_file:
            return []
        path = Path(nodes_host_file).expanduser()
        if not path.exists():
            logger.warning('Nodes host file does not exist: %s', path)
            return []

        configured_hosts: list[dict] = []
        seen_targets: set[str] = set()
        try:
            for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
                stripped = raw_line.strip()
                if not stripped or stripped.startswith('#'):
                    continue
                entry = stripped.split()[0]
                ssh_target, parallax_path = self._parse_inventory_entry(entry)
                if not ssh_target:
                    continue
                if ssh_target in seen_targets:
                    continue
                seen_targets.add(ssh_target)
                configured_hosts.append(
                    {
                        'ssh_target': ssh_target,
                        'parallax_path': parallax_path,
                        'hostname_hint': self._normalize_inventory_hostname(ssh_target),
                        'line_number': line_number,
                    }
                )
        except Exception as exc:
            logger.warning('Failed to load nodes host file %s: %s', path, exc)
            return []

        logger.info('Loaded %d configured node host(s) from %s', len(configured_hosts), path)
        return configured_hosts

    def get_configured_node_hosts(self) -> list[dict]:
        live_hostnames = {
            str(node.hardware.hostname or '').strip().lower()
            for node in (self.scheduler.node_manager.nodes if self.scheduler else [])
            if getattr(node.hardware, 'hostname', None)
        }
        return [
            {
                'ssh_target': host['ssh_target'],
                'hostname_hint': host['hostname_hint'],
                'line_number': host['line_number'],
                'parallax_path': host.get('parallax_path', ''),
                'joined': bool(host['hostname_hint']) and host['hostname_hint'] in live_hostnames,
            }
            for host in self.configured_node_hosts
        ]

    def set_configured_node_hosts(self, hosts: list[dict]) -> list[dict]:
        if not self.nodes_host_file:
            raise ValueError("Scheduler was not started with --nodes-host-file; configured node inventory cannot be persisted")

        path = Path(self.nodes_host_file).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)

        normalized_hosts: list[dict] = []
        seen_targets: set[str] = set()
        for index, raw_host in enumerate(hosts or [], start=1):
            ssh_target = str((raw_host or {}).get('ssh_target') or '').strip()
            parallax_path = str((raw_host or {}).get('parallax_path') or '').strip()
            if not ssh_target:
                continue
            if ssh_target in seen_targets:
                continue
            seen_targets.add(ssh_target)
            normalized_hosts.append(
                {
                    'ssh_target': ssh_target,
                    'parallax_path': parallax_path,
                    'hostname_hint': self._normalize_inventory_hostname(ssh_target),
                    'line_number': index,
                }
            )

        lines = [
            f"{item['ssh_target']}:{item['parallax_path']}" if item.get('parallax_path') else item['ssh_target']
            for item in normalized_hosts
        ]
        path.write_text('\n'.join(lines) + ('\n' if lines else ''))
        self.configured_node_hosts = normalized_hosts
        logger.info('Persisted %d configured node host(s) to %s', len(normalized_hosts), path)
        return self.get_configured_node_hosts()

    def persist_runtime_config(self, model_name, init_nodes_num, is_local_network) -> None:
        data = {
            "model_name": model_name,
            "init_nodes_num": init_nodes_num,
            "is_local_network": is_local_network,
        }
        SCHEDULER_STATE_PATH.write_text(json.dumps(data))

    def load_runtime_config(self):
        if not SCHEDULER_STATE_PATH.exists():
            return None
        try:
            return json.loads(SCHEDULER_STATE_PATH.read_text())
        except Exception as e:
            logger.warning(f"Failed to load persisted scheduler config: {e}")
            return None

    def run(self, model_name, init_nodes_num, is_local_network=True):
        """
        Start the scheduler and the P2P service for RPC handling.
        If Lattica is already running, it will be reused.
        Nodes will automatically rejoin via their heartbeat (node_update) mechanism.
        """
        logger.debug(
            f"SchedulerManage starting: model_name={model_name}, init_nodes_num={init_nodes_num}"
        )
        self.is_local_network = is_local_network
        if not is_local_network and not self.initial_peers and not self.relay_servers:
            logger.debug("Using public relay servers")
            self.initial_peers = PUBLIC_INITIAL_PEERS
            self.relay_servers = PUBLIC_RELAY_SERVERS

        self.persist_runtime_config(model_name, init_nodes_num, is_local_network)
        self._start_scheduler(model_name, init_nodes_num)
        self._start_lattica()
        self.completion_handler = TransformerConnectionHandler(
            lattica=self.lattica,
            recv_from_peer_addr="",
            send_to_peer_addr="",
            block_start_index=0,
            block_end_index=1,
        )

    def is_running(self):
        """
        Returns True if the scheduler is running, False otherwise.
        """
        return self.scheduler is not None

    def stop(self):
        """
        Stop the scheduler only. Lattica will remain running.
        """
        logger.info("Stopping scheduler...")

        # Stop scheduler if running
        if self.scheduler is not None:
            logger.debug("Stopping scheduler...")
            self.scheduler._stop_event.set()
            # Wait a bit for threads to finish
            time.sleep(0.1)
            self.scheduler = None
            logger.debug("Scheduler stopped")

        # Note: We don't close Lattica here to allow model switching without restarting P2P

        logger.info("Scheduler stopped")

    def get_model_name(self):
        return self.model_name

    def get_init_nodes_num(self):
        return self.init_nodes_num

    def get_is_local_network(self):
        return self.is_local_network

    def get_peer_id(self):
        if self.lattica is None:
            return None
        return self.lattica.peer_id()

    @staticmethod
    def _append_peer_id_to_maddr(maddr: str, peer_id: str | None) -> str | None:
        value = (maddr or "").strip()
        if not value:
            return None
        if not peer_id:
            return value
        if '/p2p/' in value:
            return value
        return f"{value.rstrip('/')}/p2p/{peer_id}"

    @staticmethod
    def _is_usable_join_maddr(value: str) -> bool:
        text = (value or '').strip().lower()
        if not text:
            return False
        return not (
            text.startswith('/ip4/127.')
            or text.startswith('/ip4/0.0.0.0/')
            or text.startswith('/ip6/::1/')
            or text.startswith('/ip6/::/')
            or '/dns4/localhost/' in text
            or '/tcp/0/' in text
            or text.endswith('/tcp/0')
            or '/udp/0/' in text
            or text.endswith('/udp/0')
        )

    def get_join_scheduler_addr(self):
        peer_id = self.get_peer_id()
        if peer_id is None:
            return None

        if self.lattica is not None:
            try:
                for addr in self.lattica.get_visible_maddrs() or []:
                    candidate = self._append_peer_id_to_maddr(addr, peer_id)
                    if candidate and self._is_usable_join_maddr(candidate):
                        return candidate
            except Exception as e:
                logger.debug('Failed to read visible scheduler maddrs: %s', e)
            try:
                for addr in self.lattica.get_peer_addresses(peer_id) or []:
                    candidate = self._append_peer_id_to_maddr(addr, peer_id)
                    if candidate and self._is_usable_join_maddr(candidate):
                        return candidate
            except Exception as e:
                logger.debug('Failed to read scheduler peer addresses: %s', e)
            try:
                peer_info = self.lattica.get_peer_info(peer_id)
                if peer_info is not None:
                    for attr in ('addresses', 'addrs', 'maddrs'):
                        values = getattr(peer_info, attr, None)
                        if not values:
                            continue
                        for addr in values:
                            candidate = self._append_peer_id_to_maddr(str(addr), peer_id)
                            if candidate and self._is_usable_join_maddr(candidate):
                                return candidate
            except Exception as e:
                logger.debug('Failed to read scheduler peer info addresses: %s', e)

        for addr in self.announce_maddrs:
            candidate = self._append_peer_id_to_maddr(addr, peer_id)
            if candidate and self._is_usable_join_maddr(candidate):
                return candidate

        host = (self.scheduler_host or '').strip()
        if host and host not in {'0.0.0.0', '::', 'localhost'}:
            prefix = f'/ip4/{host}' if host.replace('.', '').isdigit() and host.count('.') == 3 else f'/dns4/{host}'
            for addr in self.host_maddrs:
                value = (addr or '').strip()
                if not value:
                    continue
                if value.startswith('/ip4/0.0.0.0/'):
                    suffix = value[len('/ip4/0.0.0.0'): ]
                    candidate = self._append_peer_id_to_maddr(f'{prefix}{suffix}', peer_id)
                    if candidate and self._is_usable_join_maddr(candidate):
                        return candidate
                elif value.startswith('/ip6/::/'):
                    suffix = value[len('/ip6/::'): ]
                    candidate = self._append_peer_id_to_maddr(f'{prefix}{suffix}', peer_id)
                    if candidate and self._is_usable_join_maddr(candidate):
                        return candidate
                else:
                    candidate = self._append_peer_id_to_maddr(value, peer_id)
                    if candidate and self._is_usable_join_maddr(candidate):
                        return candidate

        return peer_id

    def weight_refit(self, request_data):
        """
        Trigger weight refit on every nodes.
        """
        if self.scheduler is None:
            return False
        self.scheduler.refit_request = request_data
        self.scheduler.refit_set = set()
        return True

    def get_last_refit_time(self):
        return self.scheduler.update_last_refit_time()

    def need_more_nodes(self):
        return self.scheduler.need_more_nodes() if self.scheduler else False

    def supports_manual_topology_rebalance(self) -> bool:
        return self.scheduler.supports_manual_topology_rebalance() if self.scheduler else False

    def get_topology_change_advisory(self) -> dict:
        if self.scheduler is None:
            return {
                "show": False,
                "message": "",
                "can_rebalance": False,
                "standby_nodes": 0,
            }

        standby_nodes = self.scheduler.node_manager.num_standby_nodes
        active_nodes = self.scheduler.node_manager.num_active_nodes
        show = self.scheduler.has_full_pipeline() and standby_nodes > 0
        can_rebalance = show and self.supports_manual_topology_rebalance()
        if not show:
            message = ""
        elif standby_nodes == 1:
            message = (
                "Cluster topology changed. Current allocation is still serving traffic. "
                "Rebalance to use the newly available standby node."
            )
        else:
            message = (
                f"Cluster topology changed. Current allocation is still serving traffic. "
                f"Rebalance to use {standby_nodes} standby nodes."
            )
        return {
            "show": show,
            "message": message,
            "can_rebalance": can_rebalance,
            "standby_nodes": standby_nodes,
            "active_nodes": active_nodes,
        }

    def request_topology_rebalance(self) -> tuple[bool, str]:
        if self.scheduler is None:
            return False, "Scheduler is not initialized"
        advisory = self.get_topology_change_advisory()
        if not advisory["show"]:
            return False, "No topology change requires a manual rebalance"
        if not advisory["can_rebalance"]:
            return False, "Current topology uses retained/manual allocations and cannot be manually rebalanced"
        self.scheduler.enqueue_rebalance("operator_requested_topology_change")
        return True, "Topology rebalance requested"

    def get_cluster_status(self):
        return {
            "type": "cluster_status",
            "data": {
                "status": self.get_schedule_status(),
                "model_name": self.model_name,
                "init_nodes_num": self.init_nodes_num,
                "node_join_command": get_node_join_command(
                    self.get_join_scheduler_addr(), self.is_local_network
                ),
                "node_list": self.get_node_list(),
                "configured_node_hosts": self.get_configured_node_hosts(),
                "need_more_nodes": self.need_more_nodes(),
                "topology_change_advisory": self.get_topology_change_advisory(),
                "max_running_request": (
                    self.scheduler.report_pipeline_capacity()[1] if self.scheduler else 0
                ),
            },
        }

    def get_node_list(self):
        if self.scheduler is None:
            return []

        return [self.build_node_info(node) for node in self.scheduler.node_manager.nodes]

    def build_node_info(self, node):
        process_status = {
            "running": True,
            "confirmed_running": True,
            "pid": "",
            "source": "heartbeat",
            "message": "Node is joined to the scheduler",
            "checked_at": time.time(),
        }
        return {
            "node_id": node.node_id,
            "hostname": node.hardware.hostname,
            "status": NODE_STATUS_AVAILABLE if node.is_active else NODE_STATUS_WAITING,
            "gpu_num": node.hardware.num_gpus,
            "gpu_name": node.hardware.gpu_name,
            "gpu_memory": node.hardware.memory_gb,
            "start_layer": node.start_layer,
            "end_layer": node.end_layer,
            "total_layers": node.model_info.num_layers if node.model_info is not None else None,
            "approx_remaining_context": node.approx_remaining_context,
            "cpu_percent": node.cpu_percent,
            "ram_used_gb": node.ram_used_gb,
            "ram_total_gb": node.ram_total_gb,
            "ram_used_percent": node.ram_used_percent,
            "disk_used_gb": node.disk_used_gb,
            "disk_total_gb": node.disk_total_gb,
            "disk_used_percent": node.disk_used_percent,
            "lifecycle": build_node_lifecycle(
                management_mode="scheduler_observed",
                process_status=process_status,
                scheduler_joined=True,
                scheduler_node_id=node.node_id,
                runtime_status=NODE_STATUS_AVAILABLE if node.is_active else NODE_STATUS_WAITING,
                serving_start_layer=node.start_layer,
                serving_end_layer=node.end_layer,
                serving_total_layers=node.model_info.num_layers if node.model_info is not None else None,
            ),
        }

    def _start_scheduler(self, model_name, init_nodes_num):
        """
        Create the scheduler and start its background run loop.
        If scheduler already exists, it will be stopped and recreated.
        Nodes will automatically rejoin via their heartbeat (node_update) mechanism.
        """
        # Stop existing scheduler if running
        if self.scheduler is not None:
            logger.info("Scheduler already running, stopping it first for re-initialization")
            self.stop()

        self.model_name = model_name
        self.init_nodes_num = init_nodes_num

        runtime_profile = resolve_runtime_profile(self.profile, is_local_network=self.is_local_network)
        heartbeat_timeout = (
            self.scheduler_heartbeat_timeout_sec
            if self.scheduler_heartbeat_timeout_sec is not None
            else runtime_profile.scheduler_heartbeat_timeout_sec
        )
        logger.info(
            "Using runtime profile for scheduler: requested=%s resolved=%s heartbeat_timeout=%.1fs",
            self.profile,
            runtime_profile.resolved_name,
            heartbeat_timeout,
        )

        model_info = get_model_info(model_name, self.use_hfcache)
        self.scheduler = Scheduler(
            model_info,
            [],
            min_nodes_bootstrapping=init_nodes_num,
            enable_weight_refit=self.enable_weight_refit,
            weight_refit_mode=self.weight_refit_mode,
            heartbeat_timeout=heartbeat_timeout,
        )

        # Run the scheduler's event/dispatch loops in background so the process
        # can continue to serve RPCs and HTTP traffic.
        threading.Thread(
            target=self.scheduler.run,
            kwargs={"poll_interval": 0.05},
            name="SchedulerMain",
            daemon=True,
        ).start()
        logger.debug("Scheduler background thread started (poll_interval=0.05)")
        logger.info("Nodes will automatically rejoin via heartbeat (node_update) mechanism")

    def _start_lattica(self):
        """
        Initialize and start the Lattica P2P node used for RPCs.
        If Lattica already exists, it will be reused (no restart), but connection_handler will be updated.
        """
        # Reuse existing Lattica if running
        if self.lattica is not None:
            logger.debug("Lattica already running, reusing existing instance")
            # Update connection handler with new scheduler if it exists
            if hasattr(self, "connection_handler") and self.connection_handler is not None:
                self.connection_handler.scheduler = self.scheduler
                logger.debug("Updated connection handler with new scheduler")
            else:
                # Create connection handler if it doesn't exist
                self.connection_handler = RPCConnectionHandler(
                    lattica=self.lattica,
                    scheduler=self.scheduler,
                    http_port=self.http_port,
                )
                logger.debug("Created connection handler with existing Lattica")
            return

        logger.debug(
            f"Starting Lattica with host_maddrs={self.host_maddrs}, mdns=False, dht_prefix={self.dht_prefix}"
        )
        self.lattica = Lattica.builder().with_listen_addrs(self.host_maddrs).with_key_path(".")

        if len(self.relay_servers) > 0:
            logger.info(f"Using relay servers: {self.relay_servers}")
            self.lattica.with_relay_servers(self.relay_servers).with_dcutr(True).with_protocol("")

        if len(self.announce_maddrs) > 0:
            logger.info(f"Using announce maddrs: {self.announce_maddrs}")
            self.lattica.with_external_addrs(self.announce_maddrs)

        if len(self.initial_peers) > 0:
            logger.info(f"Using initial peers: {self.initial_peers}")
            self.lattica.with_bootstraps(self.initial_peers)

        self.lattica.build()
        logger.debug("Lattica node built")

        if len(self.relay_servers) > 0:
            try:
                is_symmetric_nat = self.lattica.is_symmetric_nat()
                if is_symmetric_nat is None:
                    logger.warning("Failed to get is symmetric NAT, skip")
                elif is_symmetric_nat:
                    logger.error(
                        "Your network NAT type is symmetric, relay does not work on this type of NAT, see https://en.wikipedia.org/wiki/Network_address_translation"
                    )
                    exit(1)
            except Exception as e:
                logger.exception(f"Error in is symmetric NAT: {e}")

        store_success = False
        for _ in range(10):
            try:
                if self.lattica.store(
                    "scheduler_peer_id",
                    self.lattica.peer_id(),
                    expiration_time=time.time() + 365 * 24 * 60 * 60,
                ):
                    logger.info(f"Stored scheduler peer id: {self.lattica.peer_id()}")
                    store_success = True
                    break
                logger.warning("Failed to store scheduler peer id, waiting for 10 seconds")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Failed to store scheduler peer id: {e}, waiting for 10 seconds")
                time.sleep(10)

        if not store_success:
            logger.error("Failed to store scheduler peer id, after 10 times")
            exit(1)

        self.connection_handler = RPCConnectionHandler(
            lattica=self.lattica,
            scheduler=self.scheduler,
            http_port=self.http_port,
        )
        logger.debug("RPCConnectionHandler initialized")

    def get_routing_table(self, request_id, received_ts):
        """Block briefly until the scheduler assigns a routing path for the request.

        Distinguish three states via `RequestSignal.routing_table`:
        - None: not yet decided, keep waiting up to timeout
        - []: decided but no capacity (pipelines full), return immediately
        - [..]: valid routing path, return immediately
        """
        logger.debug(f"Routing table requested for request_id={request_id}")
        request = RequestSignal(request_id, received_ts)
        self.scheduler.receive_request(request)

        # Wait up to 5 seconds, but return immediately if the routing table is set (including an empty list)
        start_time = time.time()
        while request.routing_table is None and (time.time() - start_time) < 5.0:
            time.sleep(0.05)

        # Return the routing_table
        if request.routing_table is None:
            logger.debug(
                f"Routing table not ready after {(time.time() - start_time):.2f}s for request_id={request_id}"
            )
        else:
            logger.debug(
                f"Routing table resolved for request_id={request_id}: {request.routing_table}"
            )
        return request.routing_table

    def get_schedule_status(self):
        """
        Return whether a full pipeline has been allocated across joined nodes.
        """
        if self.scheduler is None:
            logger.debug("SchedulerManage status queried: waiting (scheduler not initialized)")
            return NODE_STATUS_WAITING

        # todo rebalance status
        status = (
            NODE_STATUS_AVAILABLE if self.scheduler.has_full_pipeline() else NODE_STATUS_WAITING
        )
        logger.debug(f"SchedulerManage status queried: {status}")
        return status

    def get_call_url_by_node_id(self, node_id):
        """
        Lookup the HTTP endpoint for a given node id managed by the RPC layer.
        """
        url = self.connection_handler.get_call_url_by_node_id(node_id)
        logger.debug(f"Lookup call_url for node_id={node_id} -> {url}")
        return url

import time

from lattica import ConnectionHandler, Lattica, rpc_method, rpc_stream, rpc_stream_iter

from parallax_utils.logging_config import get_logger
from scheduling.node import Node, NodeHardwareInfo
from scheduling.scheduler import Scheduler

logger = get_logger(__name__)

import json

import httpx


class RPCConnectionHandler(ConnectionHandler):
    """
    Handles RPC requests from clients, forwarding them to the appropriate TransformerBackend.
    Inherits from hivemind's ConnectionHandler.
    """

    def __init__(
        self,
        lattica: Lattica,
        scheduler: Scheduler,
        http_port: int,
    ):
        # Initialize the base class
        super().__init__(lattica)
        self.scheduler = scheduler
        self.http_port = http_port

    def _expected_model_names(self) -> set[str]:
        model_info = getattr(self.scheduler, 'model_info', None)
        names = set()
        for attr in ('model_name', 'mlx_model_name'):
            value = getattr(model_info, attr, None)
            if value:
                names.add(str(value))
        return names

    def _should_recover_layer_allocation(self, message: dict) -> bool:
        if not (message.get('recovery_layer_allocation') and message.get('start_layer') is not None and message.get('end_layer') is not None):
            return False

        start_layer = int(message.get('start_layer'))
        end_layer = int(message.get('end_layer'))
        total_layers = int(getattr(self.scheduler, 'num_layers', 0) or 0)
        if start_layer < 0 or end_layer <= start_layer or (total_layers > 0 and end_layer > total_layers):
            logger.info(
                'Ignoring retained layer allocation [%s, %s) for node %s because it is invalid for current scheduler total_layers=%s',
                start_layer,
                end_layer,
                message.get('node_id'),
                total_layers,
            )
            return False

        message_model_name = str(message.get('model_name') or '').strip()
        expected_model_names = self._expected_model_names()
        if expected_model_names and message_model_name and message_model_name not in expected_model_names:
            logger.info(
                'Ignoring retained layer allocation [%s, %s) for node %s because heartbeat model %s does not match current scheduler model(s) %s',
                start_layer,
                end_layer,
                message.get('node_id'),
                message_model_name,
                sorted(expected_model_names),
            )
            return False

        return True

    @rpc_stream
    def node_join(self, message):
        # node = {
        #     "node_id": "lattica peer id",
        #     "hardware": {
        #         "node_id": "lattica peer id",
        #         "tflops_fp16": 100,
        #         "memory_gb": 100,
        #         "memory_bandwidth_gbps": 100,
        #     },
        #     "kvcache_mem_ratio": 0.3,
        #     "param_mem_ratio": 0.5,
        #     "max_concurrent_requests": 16,
        #     "max_sequence_length": 1024,
        # }
        logger.info(f"receive node_join request: {message}")
        try:
            node = self.build_node(message)
            if self._should_recover_layer_allocation(message):
                node.manual_layer_assignment = True
                logger.info(
                    "Applying retained layer allocation during node_join recovery for %s: [%s, %s)",
                    node.node_id,
                    message.get("start_layer"),
                    message.get("end_layer"),
                )
            self.scheduler.enqueue_join(node)

            response = self.wait_layer_allocation(node.node_id, wait_seconds=300)
            logger.debug(f"node_join response: {response}")
            return response
        except Exception as e:
            logger.exception(f"node_join error: {e}")
            return {}

    @rpc_method
    def node_leave(self, message):
        logger.debug(f"receive node_leave request: {message}")
        try:
            node = self.build_node(message)
            self.scheduler.enqueue_leave(node.node_id)
            return {}
        except Exception as e:
            logger.exception(f"node_leave error: {e}")
            return {}

    @rpc_method
    def node_update(self, message):
        """
        Returns a Tuple[Dict, Dict] where
        first dict contains layer allocation result and
        second dict records weight refit information.
        """
        logger.debug(f"receive node_update request: {message}")
        try:
            node = self.build_node(message)
            # Check if node exists in scheduler
            if self.scheduler.get_node(node.node_id) is None:
                # Node not found, automatically join it (e.g., after scheduler restart)
                if self._should_recover_layer_allocation(message):
                    node.manual_layer_assignment = True
                    logger.info(
                        "Node %s not found in scheduler; recovering retained allocation [%s, %s) via node_update",
                        node.node_id,
                        message.get("start_layer"),
                        message.get("end_layer"),
                    )
                else:
                    logger.info(
                        "Node %s not found in scheduler, auto-joining via node_update and waiting for allocation rebuild",
                        node.node_id,
                    )
                self.scheduler.enqueue_join(node)
                # Leave margin below the node-side RPC timeout so restart recovery does not
                # surface as a transport timeout when allocation rebuild is still converging.
                layer_allocation = self.wait_layer_allocation(node.node_id, wait_seconds=25)
                return layer_allocation, {}

            # Node exists, update its info
            self.scheduler.enqueue_node_update(
                node.node_id,
                current_requests=node.current_requests,
                layer_latency_ms=node.layer_latency_ms,
                approx_remaining_context=node.approx_remaining_context,
                new_rtt_to_nodes=node.rtt_to_nodes,
                is_active=node.is_active,
                last_refit_time=node.last_refit_time,
                cpu_percent=node.cpu_percent,
                ram_used_gb=node.ram_used_gb,
                ram_total_gb=node.ram_total_gb,
                ram_used_percent=node.ram_used_percent,
                disk_used_gb=node.disk_used_gb,
                disk_total_gb=node.disk_total_gb,
                disk_used_percent=node.disk_used_percent,
            )
            # Return current layer allocation to node. If scheduler is still rebuilding after restart,
            # wait briefly so heartbeats can converge into a full pipeline before responding with {}.
            layer_allocation = self.get_layer_allocation(node.node_id)
            if not layer_allocation and not self.scheduler._bootstrapped_event.is_set():
                logger.info(
                    "Node %s is joined but has no allocation yet; waiting for bootstrap recovery",
                    node.node_id,
                )
                layer_allocation = self.wait_layer_allocation(node.node_id, wait_seconds=15)
            refit_request = {}
            if self.scheduler.refit_request:
                if node.node_id not in self.scheduler.refit_set and node.is_active:
                    refit_request = self.scheduler.refit_request
                    self.scheduler.refit_set.add(node.node_id)
            return layer_allocation, refit_request
        except Exception as e:
            logger.exception(f"node_update error: {e}")
            return {}, {}

    @rpc_stream_iter
    def chat_completion(
        self,
        request,
    ):
        """Handle chat completion request"""
        logger.debug(f"Chat completion request: {request}, type: {type(request)}")
        try:
            with httpx.Client(timeout=10 * 60, proxy=None, trust_env=False) as client:
                if request.get("stream", False):
                    with client.stream(
                        "POST",
                        f"http://localhost:{self.http_port}/v1/chat/completions",
                        json=request,
                    ) as response:
                        for chunk in response.iter_bytes():
                            if chunk:
                                yield chunk
                else:
                    response = client.post(
                        f"http://localhost:{self.http_port}/v1/chat/completions", json=request
                    ).json()
                    yield json.dumps(response).encode()
        except Exception as e:
            logger.exception(f"Error in chat completion: {e}")
            yield b"internal server error"

    @rpc_stream_iter
    def cluster_status(self):
        try:
            with httpx.Client(timeout=10 * 60, proxy=None, trust_env=False) as client:
                with client.stream(
                    "GET", f"http://localhost:{self.http_port}/cluster/status"
                ) as response:
                    for chunk in response.iter_bytes():
                        if chunk:
                            yield chunk
        except Exception as e:
            logger.exception(f"Error in cluster status: {e}")
            yield json.dumps({"error": "internal server error"}).encode()

    def wait_layer_allocation(self, current_node_id, wait_seconds):
        start_time = time.time()
        while True:
            layer_allocation = self.get_layer_allocation(current_node_id)
            if layer_allocation:
                return layer_allocation
            if time.time() - start_time > wait_seconds:
                return {}
            time.sleep(0.5)

    def get_layer_allocation(self, current_node_id):
        list_node_allocations = self.scheduler.list_node_allocations()
        for node_id, start_layer, end_layer in list_node_allocations:
            if current_node_id == node_id:
                node = self.scheduler.get_node(node_id)
                if node:
                    return {
                        "node_id": node_id,
                        "model_name": (
                            node.model_info.model_name
                            if node.hardware.device != "mlx"
                            else node.model_info.mlx_model_name
                        ),
                        "start_layer": start_layer,
                        "end_layer": end_layer,
                        "tp_size": node.hardware.num_gpus,
                        "enable_weight_refit": self.scheduler.enable_weight_refit,
                        "weight_refit_mode": self.scheduler.weight_refit_mode,
                    }
        return {}

    def build_node(self, node_json: dict):
        node = Node(
            node_id=node_json.get("node_id"),
            hardware=self.build_hardware(node_json.get("hardware")),
            model_info=self.scheduler.model_info,
            kvcache_mem_ratio=node_json.get("kvcache_mem_ratio"),
            param_mem_ratio=node_json.get("param_mem_ratio"),
            max_concurrent_requests=node_json.get("max_concurrent_requests"),
            max_sequence_length=node_json.get("max_sequence_length"),
            is_active=node_json.get("is_active", True),
            manual_layer_assignment=node_json.get("manual_layer_assignment", False),
            last_refit_time=node_json.get("last_refit_time", 0.0),
        )
        if self._should_recover_layer_allocation(node_json):
            node.start_layer = node_json.get("start_layer")
            node.end_layer = node_json.get("end_layer")
        if node_json.get("current_requests", None) is not None:
            node.current_requests = node_json.get("current_requests")
        if node_json.get("layer_latency_ms", None) is not None:
            node.avg_layer_latency_ms = node_json.get("layer_latency_ms")
        if node_json.get("approx_remaining_context", None) is not None:
            node.approx_remaining_context = node_json.get("approx_remaining_context")
        if node_json.get("rtt_to_nodes", None) is not None:
            node.rtt_to_nodes = node_json.get("rtt_to_nodes")
        for field in (
            'cpu_percent',
            'ram_used_gb',
            'ram_total_gb',
            'ram_used_percent',
            'disk_used_gb',
            'disk_total_gb',
            'disk_used_percent',
        ):
            if node_json.get(field, None) is not None:
                setattr(node, field, node_json.get(field))
        return node

    def build_hardware(self, hardware_json):
        node_id = hardware_json.get("node_id")
        hostname = hardware_json.get("hostname", "")
        num_gpus = hardware_json.get("num_gpus")
        tflops_fp16 = hardware_json.get("tflops_fp16")
        gpu_name = hardware_json.get("gpu_name")
        memory_gb = hardware_json.get("memory_gb")
        memory_bandwidth_gbps = hardware_json.get("memory_bandwidth_gbps")
        device = hardware_json.get("device")
        return NodeHardwareInfo(
            node_id=node_id,
            num_gpus=num_gpus,
            tflops_fp16=tflops_fp16,
            gpu_name=gpu_name,
            memory_gb=memory_gb,
            memory_bandwidth_gbps=memory_bandwidth_gbps,
            device=device,
            hostname=hostname,
        )

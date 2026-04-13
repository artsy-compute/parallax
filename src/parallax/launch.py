"""
Launch the Parallax server.

This script is used to launch the Parallax server.
It will start the following services:
    1.Executor each tp_rank as a subprocess.
    2.HTTP server as a subprocess.
    3.P2P server as a subprocess.

Example command:
python src/parallax/launch.py \
    --model-path Qwen/Qwen3-0.6B \
    --max-num-tokens-per-batch 16384 \
    --max-batch-size 128 \
    --start-layer 0 \
    --end-layer 28
"""

import argparse
import multiprocessing
import os
import signal
import sys
import tempfile
import time

from parallax.p2p.server import ServerState, launch_p2p_server_process, stop_p2p_server
from parallax.server.executor.factory import run_executor_process, stop_executor_process
from parallax.server.http_server import launch_http_server, stop_http_server
from parallax.server.server_args import parse_args
from parallax.utils.shared_state import SharedState
from parallax.utils.utils import fetch_model_from_hf, initialize_nccl_port
from parallax_utils.ascii_anime import display_parallax_join
from parallax_utils.logging_config import get_logger, set_log_level
from parallax_utils.runtime_profiles import infer_is_local_network, resolve_runtime_profile
from parallax_utils.version_check import check_latest_release

logger = get_logger("parallax.launch")


def _update_args_from_shared_state(args, shared_state: SharedState, force_update: bool):
    """Update args with layer allocation from shared state"""
    model_info = shared_state.get_model_info()
    args.start_layer = model_info["block_start_index"]
    args.end_layer = model_info["block_end_index"]
    if args.model_path is not None and force_update == False:
        # Use local model path first
        pass
    elif model_info["model_name"]:
        # Update model_path if provided
        args.model_path = model_info["model_name"]
        logger.debug(f"Updated model_path to: {args.model_path}")
    else:
        assert False, "Neither scheduler nor worker provides a valid model path!"
    # Update tp_size if provided, otherwise keep current value
    args.tp_size = model_info["tp_size"] or args.tp_size
    # Update weight refit switch
    args.enable_weight_refit = model_info["enable_weight_refit"] or args.enable_weight_refit
    args.weight_refit_mode = model_info["weight_refit_mode"] or args.weight_refit_mode


def _stop_executor_processes(executor_subprocs):
    """Stop all executor processes"""
    for executor_process in executor_subprocs:
        if executor_process.is_alive():
            logger.debug(f"Terminating executor process {executor_process.pid}")
            stop_executor_process(executor_process)


def _wait_executors_check_layer_change(shared_state: SharedState, executor_subprocs):
    """Wait for executor processes and check if layer allocation changed.

    Returns:
        True if layer allocation changed (need to reload executors),
        False if all executors exited normally.
    """
    while any(proc.is_alive() for proc in executor_subprocs):
        for proc in executor_subprocs:
            if proc.is_alive():
                proc.join(timeout=1.0)  # Check every second

        if shared_state.get_layer_allocation_changed():
            return True

    # Check race condition: layer allocation changed after all processes exited
    return shared_state.get_layer_allocation_changed()


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)

    p2p_server_process = None
    http_server_process = None
    executor_subprocs = []
    # Shared state for layer allocation info (used when P2P server is in subprocess)
    shared_state = SharedState.create()
    shared_state.set_status(ServerState.JOINING.value)

    conn_main = None
    conn_refit = None
    conn_tp_0 = []
    conn_tp_i = []
    cleanup_state = {"started": False}

    def _cleanup_processes():
        if cleanup_state["started"]:
            return
        cleanup_state["started"] = True

        logger.debug("Shutting down all processes...")
        try:
            shared_state.set_status(ServerState.OFFLINE.value)
        except Exception:
            pass

        _stop_executor_processes(executor_subprocs)

        if p2p_server_process is not None:
            stop_p2p_server(p2p_server_process)

        if http_server_process is not None:
            stop_http_server(http_server_process)

        for conn in [conn_main, conn_refit, *conn_tp_0, *conn_tp_i]:
            if conn is None:
                continue
            try:
                conn.close()
            except Exception:
                pass

        try:
            shared_state.shutdown()
        except Exception:
            pass

        logger.debug("All processes shut down.")

    def _handle_signal(signum, frame):
        logger.warning("Received signal %s, shutting down node process tree...", signum)
        _cleanup_processes()
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        args = parse_args()
        set_log_level(args.log_level)
        logger.debug(f"args: {args}")

        runtime_profile = resolve_runtime_profile(
            args.profile,
            is_local_network=infer_is_local_network(
                scheduler_addr=args.scheduler_addr,
                relay_servers=args.relay_servers,
            ),
        )
        if args.heartbeat_interval_sec is None:
            args.heartbeat_interval_sec = runtime_profile.node_heartbeat_interval_sec
        if args.heartbeat_rpc_timeout_sec is None:
            args.heartbeat_rpc_timeout_sec = runtime_profile.node_heartbeat_rpc_timeout_sec
        if args.force_rejoin_threshold is None:
            args.force_rejoin_threshold = runtime_profile.force_rejoin_threshold
        if args.force_rejoin_cooldown_sec is None:
            args.force_rejoin_cooldown_sec = runtime_profile.force_rejoin_cooldown_sec
        if args.local_http_retry_attempts is None:
            args.local_http_retry_attempts = runtime_profile.local_http_retry_attempts
        if args.local_http_retry_delay_sec is None:
            args.local_http_retry_delay_sec = runtime_profile.local_http_retry_delay_sec
        logger.info(
            "Using runtime profile for node: requested=%s resolved=%s heartbeat_interval=%.1fs heartbeat_rpc_timeout=%.1fs force_rejoin_threshold=%d",
            args.profile,
            runtime_profile.resolved_name,
            args.heartbeat_interval_sec,
            args.heartbeat_rpc_timeout_sec,
            args.force_rejoin_threshold,
        )
        args.recv_from_peer_addr = f"ipc://{tempfile.NamedTemporaryFile().name}"
        args.send_to_peer_addr = f"ipc://{tempfile.NamedTemporaryFile().name}"
        args.executor_input_ipc = f"ipc://{tempfile.NamedTemporaryFile().name}"
        args.executor_output_ipc = f"ipc://{tempfile.NamedTemporaryFile().name}"
        if args.nccl_port is None:
            args.nccl_port = initialize_nccl_port()

        # Silence tokenizer warnings
        os.environ["TOKENIZERS_PARALLELISM"] = "false"

        logger.debug(f"executor_input_addr: {args.executor_input_ipc}")
        logger.debug(f"executor_output_addr: {args.executor_output_ipc}")
        logger.debug(f"nccl_port: {args.nccl_port}")

        # Pipe for subprocess communication
        conn_main, conn_refit = multiprocessing.Pipe()

        if args.scheduler_addr is None:
            if args.log_level != "DEBUG":
                display_parallax_join(args.model_path)
            check_latest_release()

            config = fetch_model_from_hf(args.model_path, local_files_only=args.use_hfcache)
            if args.start_layer is None:
                args.start_layer = 0
            if args.end_layer is None:
                args.end_layer = config.get("num_hidden_layers")

            # only launch http server on head node
            if args.start_layer == 0:
                http_server_process = launch_http_server(args)
            # Launch P2P server as subprocess
            if not (args.start_layer == 0 and args.end_layer == config.get("num_hidden_layers")):
                p2p_server_process = launch_p2p_server_process(
                    initial_peers=args.initial_peers,
                    scheduler_addr=args.scheduler_addr,
                    relay_servers=args.relay_servers,
                    pp_start_layer=args.start_layer,
                    pp_end_layer=args.end_layer,
                    hidden_layers=config.get("num_hidden_layers"),
                    tp_size=args.tp_size,
                    dp_size=args.dp_size,
                    tcp_port=args.tcp_port,
                    udp_port=args.udp_port,
                    dht_prefix=args.dht_prefix,
                    announce_maddrs=args.announce_maddrs,
                    http_port=args.port,
                    notify_url=args.notify_url,
                    recv_from_peer_addr=args.recv_from_peer_addr,
                    send_to_peer_addr=args.send_to_peer_addr,
                    model_name=args.model_path,
                    max_batch_size=args.max_batch_size,
                    max_sequence_length=args.max_sequence_length,
                    param_mem_ratio=args.param_mem_ratio,
                    kvcache_mem_ratio=args.kvcache_mem_ratio,
                    shared_state=shared_state.dict,
                    log_level=args.log_level,
                    conn=conn_main,
                    heartbeat_interval_sec=args.heartbeat_interval_sec,
                    heartbeat_rpc_timeout_sec=args.heartbeat_rpc_timeout_sec,
                    heartbeat_force_rejoin_threshold=args.force_rejoin_threshold,
                    heartbeat_force_rejoin_cooldown_sec=args.force_rejoin_cooldown_sec,
                    local_http_retry_attempts=args.local_http_retry_attempts,
                    local_http_retry_delay_sec=args.local_http_retry_delay_sec,
                )

            # Build connectors for tp communication
            conn_tp_0 = [conn_refit]
            conn_tp_i = []
            for i in range(1, args.tp_size):
                conn1, conn2 = multiprocessing.Pipe()
                conn_tp_0.append(conn1)
                conn_tp_i.append(conn2)
            # Launch all executor processes (including tp_rank=0)
            for tp_rank in range(args.tp_size):
                args_copy = argparse.Namespace(**vars(args))
                args_copy.tp_rank = tp_rank
                proc = multiprocessing.Process(
                    target=run_executor_process,
                    args=(
                        args_copy,
                        shared_state.dict,  # Pass dict to subprocess
                        conn_tp_0 if tp_rank == 0 else [conn_tp_i[tp_rank - 1]],
                    ),
                )
                proc.start()
                executor_subprocs.append(proc)

            time.sleep(2)  # Give executors time to start
            shared_state.set_status(ServerState.READY.value)

            # Wait for all executor processes
            for proc in executor_subprocs:
                proc.join()
        else:
            # Launch P2P server as subprocess (with scheduler)
            # Pass dict to subprocess (multiprocessing requires serializable objects)
            p2p_server_process = launch_p2p_server_process(
                initial_peers=args.initial_peers,
                scheduler_addr=args.scheduler_addr,
                relay_servers=args.relay_servers,
                pp_start_layer=args.start_layer,
                pp_end_layer=args.end_layer,
                hidden_layers=None,
                tp_size=args.tp_size,
                dp_size=args.dp_size,
                tcp_port=args.tcp_port,
                udp_port=args.udp_port,
                dht_prefix=args.dht_prefix,
                announce_maddrs=args.announce_maddrs,
                http_port=args.port,
                notify_url=args.notify_url,
                recv_from_peer_addr=args.recv_from_peer_addr,
                send_to_peer_addr=args.send_to_peer_addr,
                model_name=args.model_path,
                max_batch_size=args.max_batch_size,
                max_sequence_length=args.max_sequence_length,
                param_mem_ratio=args.param_mem_ratio,
                kvcache_mem_ratio=args.kvcache_mem_ratio,
                shared_state=shared_state.dict,  # Pass dict to subprocess
                log_level=args.log_level,
                conn=conn_main,
                heartbeat_interval_sec=args.heartbeat_interval_sec,
                heartbeat_rpc_timeout_sec=args.heartbeat_rpc_timeout_sec,
                heartbeat_force_rejoin_threshold=args.force_rejoin_threshold,
                heartbeat_force_rejoin_cooldown_sec=args.force_rejoin_cooldown_sec,
                local_http_retry_attempts=args.local_http_retry_attempts,
                local_http_retry_delay_sec=args.local_http_retry_delay_sec,
            )

            # Wait for layer allocation from scheduler (via shared state)
            logger.debug("Waiting for layer allocation from scheduler...")
            max_wait_time = 300  # 5 minutes
            wait_start = time.time()
            while True:
                model_info = shared_state.get_model_info()
                if (
                    model_info["block_start_index"] is not None
                    and model_info["block_end_index"] is not None
                    and model_info["model_name"] is not None
                ):
                    break
                if time.time() - wait_start > max_wait_time:
                    logger.error("Timeout waiting for layer allocation from scheduler")
                    raise RuntimeError("Failed to get layer allocation from scheduler")
                time.sleep(1)

            # Get layer allocation from shared state
            _update_args_from_shared_state(args, shared_state, force_update=False)

            logger.debug(
                f"Start Executor with start_layer: {args.start_layer}, end_layer: {args.end_layer}, "
                f"model: {args.model_path}"
            )

            if args.log_level != "DEBUG":
                display_parallax_join(args.model_path)
            check_latest_release()

            # Main execution loop with layer reallocation support
            while True:
                try:
                    # only launch http server on head node
                    if args.start_layer == 0:
                        http_server_process = launch_http_server(args)

                    # Build connectors for tp communication
                    conn_tp_0 = [conn_refit]
                    conn_tp_i = []
                    for i in range(1, args.tp_size):
                        conn1, conn2 = multiprocessing.Pipe()
                        conn_tp_0.append(conn1)
                        conn_tp_i.append(conn2)
                    # Launch all executor processes (including tp_rank=0)
                    executor_subprocs = []
                    for tp_rank in range(args.tp_size):
                        args_copy = argparse.Namespace(**vars(args))
                        args_copy.tp_rank = tp_rank
                        proc = multiprocessing.Process(
                            target=run_executor_process,
                            args=(
                                args_copy,
                                shared_state.dict,  # Pass dict to subprocess
                                conn_tp_0 if tp_rank == 0 else [conn_tp_i[tp_rank - 1]],
                            ),
                        )
                        proc.start()
                        executor_subprocs.append(proc)

                    # Wait for executors and restart if layer allocation changes
                    if _wait_executors_check_layer_change(shared_state, executor_subprocs):
                        logger.warning("Layer allocation changed! Stopping executors to reload...")
                        # Reset flag and set status to INITIALIZING
                        shared_state.update(
                            _layer_allocation_changed=False,
                            status=ServerState.INITIALIZING.value,
                        )
                        _stop_executor_processes(executor_subprocs)
                        if http_server_process is not None:
                            stop_http_server(http_server_process)
                        _update_args_from_shared_state(args, shared_state, force_update=True)
                        logger.info(
                            f"Reloading executor with layers [{args.start_layer}, {args.end_layer})"
                        )
                        continue

                    # All processes exited normally
                    break
                except KeyboardInterrupt:
                    logger.debug("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    logger.exception(f"Executor error: {e}")
                    # Shutdown all executor processes on error
                    for proc in executor_subprocs:
                        if proc.is_alive():
                            stop_executor_process(proc)
                    raise
    except KeyboardInterrupt:
        logger.debug("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.exception(e)
    finally:
        _cleanup_processes()

#!/usr/bin/env python3
"""
Parallax CLI - Command line interface for Parallax distributed LLM serving.

This module provides the main CLI entry point for Parallax, supporting
commands like 'run' and 'join' that mirror the functionality of the
bash scripts.
"""

import argparse
import base64
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import time

import requests

from parallax_utils.file_util import get_project_root
from parallax_utils.logging_config import get_logger
from parallax_utils.version_check import get_current_version

logger = get_logger("parallax.cli")

PUBLIC_INITIAL_PEERS = [
    "/dns4/bootstrap-lattica.gradient.network/udp/18080/quic-v1/p2p/12D3KooWJHXvu8TWkFn6hmSwaxdCLy4ZzFwr4u5mvF9Fe2rMmFXb",
    "/dns4/bootstrap-lattica.gradient.network/tcp/18080/p2p/12D3KooWJHXvu8TWkFn6hmSwaxdCLy4ZzFwr4u5mvF9Fe2rMmFXb",
    "/dns4/bootstrap-lattica-us.gradient.network/udp/18080/quic-v1/p2p/12D3KooWFD8NoyHfmVxLVCocvXJBjwgE9RZ2bgm2p5WAWQax4FoQ",
    "/dns4/bootstrap-lattica-us.gradient.network/tcp/18080/p2p/12D3KooWFD8NoyHfmVxLVCocvXJBjwgE9RZ2bgm2p5WAWQax4FoQ",
    "/dns4/bootstrap-lattica-eu.gradient.network/udp/18080/quic-v1/p2p/12D3KooWCNuEF4ro95VA4Lgq4NvjdWfJFoTcvWsBA7Z6VkBByPtN",
    "/dns4/bootstrap-lattica-eu.gradient.network/tcp/18080/p2p/12D3KooWCNuEF4ro95VA4Lgq4NvjdWfJFoTcvWsBA7Z6VkBByPtN",
]

def _looks_like_peer_id(value: str | None) -> bool:
    text = str(value or '').strip()
    return text.startswith('12D3Koo') or text.startswith('Qm')


def _should_use_public_relay(scheduler_addr: str | None, use_relay: bool) -> bool:
    if use_relay:
        return True
    text = str(scheduler_addr or '').strip()
    if not text or text == 'auto':
        return False
    if text.startswith('/'):
        return False
    if _looks_like_peer_id(text):
        return False
    return True


PUBLIC_RELAY_SERVERS = [
    "/dns4/relay-lattica.gradient.network/udp/18080/quic-v1/p2p/12D3KooWDaqDAsFupYvffBDxjHHuWmEAJE4sMDCXiuZiB8aG8rjf",
    "/dns4/relay-lattica.gradient.network/tcp/18080/p2p/12D3KooWDaqDAsFupYvffBDxjHHuWmEAJE4sMDCXiuZiB8aG8rjf",
    "/dns4/relay-lattica-us.gradient.network/udp/18080/quic-v1/p2p/12D3KooWHMXi6SCfaQzLcFt6Th545EgRt4JNzxqmDeLs1PgGm3LU",
    "/dns4/relay-lattica-us.gradient.network/tcp/18080/p2p/12D3KooWHMXi6SCfaQzLcFt6Th545EgRt4JNzxqmDeLs1PgGm3LU",
    "/dns4/relay-lattica-eu.gradient.network/udp/18080/quic-v1/p2p/12D3KooWRAuR7rMNA7Yd4S1vgKS6akiJfQoRNNexTtzWxYPiWfG5",
    "/dns4/relay-lattica-eu.gradient.network/tcp/18080/p2p/12D3KooWRAuR7rMNA7Yd4S1vgKS6akiJfQoRNNexTtzWxYPiWfG5",
]


def check_python_version():
    """Check if Python version is 3.11 or higher."""
    if sys.version_info < (3, 11) or sys.version_info >= (3, 14):
        logger.info(
            f"Error: Python 3.11 or higher and less than 3.14 is required. Current version is {sys.version_info.major}.{sys.version_info.minor}."
        )
        sys.exit(1)


def _flag_present(args_list: list[str], flag_names: list[str]) -> bool:
    """Return True if any of the given flags is present in args_list.

    Supports forms: "--flag value", "--flag=value", "-f value", "-f=value".
    """
    if not args_list:
        return False
    flags_set = set(flag_names)
    for i, token in enumerate(args_list):
        if token in flags_set:
            return True
        for flag in flags_set:
            if token.startswith(flag + "="):
                return True
    return False


def _find_flag_value(args_list: list[str], flag_names: list[str]) -> str | None:
    """Find the value for the first matching flag in args_list, if present.

    Returns the associated value for forms: "--flag value" or "--flag=value" or
    "-f value" or "-f=value". Returns None if not found or value is missing.
    """
    if not args_list:
        return None
    flags_set = set(flag_names)
    for i, token in enumerate(args_list):
        if token in flags_set:
            # expect value in next token if exists and is not another flag
            if i + 1 < len(args_list) and not args_list[i + 1].startswith("-"):
                return args_list[i + 1]
            return None
        for flag in flags_set:
            prefix = flag + "="
            if token.startswith(prefix):
                return token[len(prefix) :]
    return None


def _resolve_scheduler_addr_from_local_backend() -> str | None:
    """Try to fetch a concrete scheduler address from a local backend."""
    for url in (
        "http://127.0.0.1:3001/node/join/command",
        "http://localhost:3001/node/join/command",
    ):
        try:
            response = requests.get(url, timeout=2)
            response.raise_for_status()
            payload = response.json()
            command = str(((payload.get("data") or {}).get("command") or "")).strip()
            if not command:
                continue
            resolved = _find_flag_value(command.split(), ["--scheduler-addr", "-s"])
            if resolved:
                logger.info("Resolved scheduler address from local backend: %s", resolved)
                return resolved
        except Exception as e:
            logger.debug("Failed to resolve scheduler address from %s: %s", url, e)
    return None


def _is_local_port_available(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, int(port)))
            return True
    except OSError:
        return False


def _pick_available_local_port(host: str, preferred_port: int) -> int:
    if preferred_port > 0 and _is_local_port_available(host, preferred_port):
        return preferred_port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def _execute_with_graceful_shutdown(cmd: list[str], env: dict[str, str] | None = None) -> None:
    """Execute a command in a subprocess and handle graceful shutdown on Ctrl-C.

    This centralizes the common Popen + signal handling logic shared by
    run_command and join_command.
    """
    logger.info(f"Running command: {' '.join(cmd)}")

    sub_process = None
    try:
        # Start in a new session so we can signal the entire process group
        sub_process = subprocess.Popen(cmd, env=env, start_new_session=True)
        # Wait for the subprocess to finish
        return_code = sub_process.wait()
        if return_code != 0:
            logger.error(f"Command failed with exit code {return_code}")
            sys.exit(return_code)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")

        # If another Ctrl-C arrives during cleanup, force-kill the whole group immediately
        def _force_kill_handler(signum, frame):
            try:
                os.killpg(sub_process.pid, signal.SIGKILL)
            except Exception:
                try:
                    sub_process.kill()
                except Exception:
                    pass
            os._exit(130)

        try:
            signal.signal(signal.SIGINT, _force_kill_handler)
        except Exception:
            pass

        if sub_process is not None:
            try:
                logger.info("Terminating subprocess group...")
                # Gracefully terminate the entire process group
                try:
                    os.killpg(sub_process.pid, signal.SIGINT)
                except Exception:
                    # Fall back to signaling just the child process
                    sub_process.send_signal(signal.SIGINT)

                logger.info("Waiting for subprocess to exit...")
                # Wait for the subprocess to exit gracefully
                try:
                    sub_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.info("SIGINT timeout; sending SIGTERM to process group...")
                    try:
                        os.killpg(sub_process.pid, signal.SIGTERM)
                    except Exception:
                        sub_process.terminate()
                    try:
                        sub_process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        logger.info("SIGTERM timeout; forcing SIGKILL on process group...")
                        try:
                            os.killpg(sub_process.pid, signal.SIGKILL)
                        except Exception:
                            sub_process.kill()
                        sub_process.wait()
                logger.info("Subprocess exited.")
            except Exception as e:
                logger.error(f"Failed to terminate subprocess: {e}")
        else:
            logger.info("Subprocess not found, skipping shutdown...")
        sys.exit(0)


def _terminate_process_group(sub_process: subprocess.Popen | None, label: str) -> None:
    if sub_process is None:
        return
    try:
        logger.info("Terminating %s...", label)
        try:
            os.killpg(sub_process.pid, signal.SIGINT)
        except Exception:
            sub_process.send_signal(signal.SIGINT)
        try:
            sub_process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            logger.info("%s SIGINT timeout; sending SIGTERM...", label)
        try:
            os.killpg(sub_process.pid, signal.SIGTERM)
        except Exception:
            sub_process.terminate()
        try:
            sub_process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            logger.info("%s SIGTERM timeout; forcing SIGKILL...", label)
        try:
            os.killpg(sub_process.pid, signal.SIGKILL)
        except Exception:
            sub_process.kill()
        sub_process.wait()
    except Exception as e:
        logger.error("Failed to terminate %s: %s", label, e)


def _execute_dev_processes(
    backend_cmd: list[str],
    frontend_cmd: list[str],
    *,
    backend_env: dict[str, str] | None = None,
    frontend_env: dict[str, str] | None = None,
    frontend_cwd: str | None = None,
) -> None:
    logger.info("Running backend command: %s", " ".join(backend_cmd))
    logger.info("Running frontend command: %s", " ".join(frontend_cmd))

    backend_process = None
    frontend_process = None
    try:
        backend_process = subprocess.Popen(
            backend_cmd,
            env=backend_env,
            start_new_session=True,
        )
        frontend_process = subprocess.Popen(
            frontend_cmd,
            env=frontend_env,
            cwd=frontend_cwd,
            start_new_session=True,
        )
        while True:
            backend_rc = backend_process.poll()
            frontend_rc = frontend_process.poll()
            if backend_rc is not None:
                _terminate_process_group(frontend_process, "frontend dev server")
                if backend_rc != 0:
                    logger.error("Backend exited with code %s", backend_rc)
                sys.exit(backend_rc)
            if frontend_rc is not None:
                _terminate_process_group(backend_process, "backend server")
                if frontend_rc != 0:
                    logger.error("Frontend dev server exited with code %s", frontend_rc)
                sys.exit(frontend_rc)
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down dev-run...")
        _terminate_process_group(frontend_process, "frontend dev server")
        _terminate_process_group(backend_process, "backend server")
        sys.exit(0)




def _default_log_file(kind: str) -> str:
    project_root = get_project_root()
    logs_dir = Path(project_root) / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    if kind == 'run':
        filename = 'parallax-scheduler.log'
    else:
        filename = f'parallax-node-{int(time.time())}-{os.getpid()}.log'
    return str(logs_dir / filename)

def _get_relay_params():
    return [
        "--relay-servers",
        *PUBLIC_RELAY_SERVERS,
        "--initial-peers",
        *PUBLIC_INITIAL_PEERS,
    ]


def build_frontend_command(args=None, passthrough_args: list[str] | None = None):
    """Build the frontend bundle in src/frontend."""
    check_python_version()

    project_root = get_project_root()
    frontend_dir = project_root / "src" / "frontend"
    package_json = frontend_dir / "package.json"

    if not package_json.exists():
        logger.info(f"Error: frontend package.json not found at {package_json}")
        sys.exit(1)

    cmd = ["npm", "run", "build"]
    logger.info(f"Building frontend in {frontend_dir}")
    logger.info(f"Running command: {' '.join(cmd)}")

    try:
        subprocess.run(cmd, cwd=frontend_dir, check=True)
        logger.info("Frontend build completed.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Frontend build failed with exit code {e.returncode}")
        sys.exit(e.returncode)


def run_command(args, passthrough_args: list[str] | None = None):
    """Run the scheduler (equivalent to scripts/start.sh)."""
    if not args.skip_upload:
        update_package_info()

    check_python_version()

    project_root = get_project_root()
    backend_main = project_root / "src" / "backend" / "main.py"

    if not backend_main.exists():
        logger.info(f"Error: Backend main.py not found at {backend_main}")
        sys.exit(1)

    # Build the command to run the backend main.py
    passthrough_args = passthrough_args or []
    cmd = [sys.executable, str(backend_main)]
    log_file = args.log_file or _find_flag_value(passthrough_args, ["--log-file"]) or _default_log_file("run")
    env = os.environ.copy()
    env["PARALLAX_LOG_FILE"] = log_file
    if not _flag_present(passthrough_args, ["--port"]):
        cmd.extend(["--port", "3001"])

    # Add optional arguments if provided
    if args.model_name:
        cmd.extend(["--model-name", args.model_name])
    if args.init_nodes_num:
        cmd.extend(["--init-nodes-num", str(args.init_nodes_num)])
    if args.use_relay:
        cmd.extend(_get_relay_params())
        logger.info(
            "Using public relay server to help nodes and the scheduler establish a connection (remote mode). Your IP address will be reported to the relay server to help establish the connection."
        )

    if args.profile:
        cmd.extend(["--profile", args.profile])
    if not _flag_present(passthrough_args, ["--log-file"]):
        cmd.extend(["--log-file", log_file])
    if args.nodes_host_file:
        cmd.extend(["--nodes-host-file", args.nodes_host_file])

    # Append any passthrough args (unrecognized by this CLI) directly to the command
    if passthrough_args:
        cmd.extend(passthrough_args)

    _execute_with_graceful_shutdown(cmd, env=env)


def dev_run_command(args, passthrough_args: list[str] | None = None):
    """Run the scheduler backend together with the Vite frontend dev server."""
    if not args.skip_upload:
        update_package_info()

    check_python_version()

    if shutil.which("npm") is None:
        logger.error("Error: npm is required for dev-run but was not found on PATH.")
        sys.exit(1)

    project_root = get_project_root()
    backend_main = project_root / "src" / "backend" / "main.py"
    frontend_dir = project_root / "src" / "frontend"
    package_json = frontend_dir / "package.json"

    if not backend_main.exists():
        logger.info(f"Error: Backend main.py not found at {backend_main}")
        sys.exit(1)
    if not package_json.exists():
        logger.info(f"Error: frontend package.json not found at {package_json}")
        sys.exit(1)

    passthrough_args = passthrough_args or []
    requested_backend_port = int(_find_flag_value(passthrough_args, ["--port"]) or "3001")
    backend_host = _find_flag_value(passthrough_args, ["--host"]) or "127.0.0.1"
    requested_frontend_port = int(args.frontend_port or 5173)
    frontend_host = str(args.frontend_host or "127.0.0.1")
    explicit_backend_port = _flag_present(passthrough_args, ["--port"])
    explicit_frontend_port = int(args.frontend_port or 5173) != 5173

    if explicit_backend_port:
        if not _is_local_port_available(backend_host, requested_backend_port):
            logger.error(
                "Error: backend dev-run port %s is already in use on %s. Stop the other process or pass a different --port.",
                requested_backend_port,
                backend_host,
            )
            sys.exit(1)
        backend_port = requested_backend_port
    else:
        backend_port = _pick_available_local_port(backend_host, requested_backend_port)
        if backend_port != requested_backend_port:
            logger.info(
                "Backend port %s is in use on %s; dev-run selected free port %s instead.",
                requested_backend_port,
                backend_host,
                backend_port,
            )

    if explicit_frontend_port:
        if not _is_local_port_available(frontend_host, requested_frontend_port):
            logger.error(
                "Error: frontend dev-run port %s is already in use on %s. Stop the other process or pass a different --frontend-port.",
                requested_frontend_port,
                frontend_host,
            )
            sys.exit(1)
        frontend_port = requested_frontend_port
    else:
        frontend_port = _pick_available_local_port(frontend_host, requested_frontend_port)
        if frontend_port != requested_frontend_port:
            logger.info(
                "Frontend port %s is in use on %s; dev-run selected free port %s instead.",
                requested_frontend_port,
                frontend_host,
                frontend_port,
            )

    backend_cmd = [sys.executable, str(backend_main)]
    log_file = args.log_file or _find_flag_value(passthrough_args, ["--log-file"]) or _default_log_file("run")
    backend_env = os.environ.copy()
    backend_env["PARALLAX_LOG_FILE"] = log_file

    if not _flag_present(passthrough_args, ["--port"]):
        backend_cmd.extend(["--port", str(backend_port)])
    if not _flag_present(passthrough_args, ["--host"]):
        backend_cmd.extend(["--host", backend_host])
    if args.model_name:
        backend_cmd.extend(["--model-name", args.model_name])
    if args.init_nodes_num:
        backend_cmd.extend(["--init-nodes-num", str(args.init_nodes_num)])
    if args.use_relay:
        backend_cmd.extend(_get_relay_params())
        logger.info(
            "Using public relay server to help nodes and the scheduler establish a connection (remote mode). Your IP address will be reported to the relay server to help establish the connection."
        )
    if args.profile:
        backend_cmd.extend(["--profile", args.profile])
    if not _flag_present(passthrough_args, ["--log-file"]):
        backend_cmd.extend(["--log-file", log_file])
    if args.nodes_host_file:
        backend_cmd.extend(["--nodes-host-file", args.nodes_host_file])
    if passthrough_args:
        backend_cmd.extend(passthrough_args)

    frontend_cmd = ["npm", "run", "dev", "--", "--host", frontend_host, "--port", str(frontend_port)]
    frontend_env = os.environ.copy()
    frontend_env["PARALLAX_BACKEND_HOST"] = backend_host
    frontend_env["PARALLAX_BACKEND_PORT"] = str(backend_port)
    backend_env["PARALLAX_FRONTEND_DEV_SERVER_URL"] = f"http://{frontend_host}:{frontend_port}"

    logger.info("Frontend dev server URL: http://%s:%s", frontend_host, frontend_port)
    logger.info("Backend dev server URL: http://%s:%s", backend_host, backend_port)
    logger.info(
        "dev-run is active. Open http://%s:%s/ and the backend will redirect to the live Vite frontend.",
        backend_host,
        backend_port,
    )
    _execute_dev_processes(
        backend_cmd,
        frontend_cmd,
        backend_env=backend_env,
        frontend_env=frontend_env,
        frontend_cwd=str(frontend_dir),
    )


def join_command(args, passthrough_args: list[str] | None = None):
    """Join a distributed cluster (equivalent to scripts/join.sh)."""
    if not args.skip_upload:
        update_package_info()

    check_python_version()

    project_root = get_project_root()
    launch_script = project_root / "src" / "parallax" / "launch.py"

    if not launch_script.exists():
        logger.info(f"Error: Launch script not found at {launch_script}")
        sys.exit(1)

    if str(args.scheduler_addr or "").strip() == "auto":
        resolved_scheduler_addr = _resolve_scheduler_addr_from_local_backend()
        if resolved_scheduler_addr:
            args.scheduler_addr = resolved_scheduler_addr

    # Set environment variable for the subprocess
    env = os.environ.copy()
    env["SGLANG_ENABLE_JIT_DEEPGEMM"] = "0"
    log_file = args.log_file or _find_flag_value(passthrough_args, ["--log-file"]) or _default_log_file("join")
    env["PARALLAX_LOG_FILE"] = log_file

    # Build the command to run the launch.py script
    passthrough_args = passthrough_args or []

    cmd = [sys.executable, str(launch_script)]
    if not _flag_present(passthrough_args, ["--max-num-tokens-per-batch"]):
        cmd.extend(["--max-num-tokens-per-batch", "4096"])
    if not _flag_present(passthrough_args, ["--max-sequence-length"]):
        cmd.extend(["--max-sequence-length", "7168"])
    if not _flag_present(passthrough_args, ["--max-batch-size"]):
        cmd.extend(["--max-batch-size", "8"])
    if not _flag_present(passthrough_args, ["--kv-block-size"]):
        cmd.extend(["--kv-block-size", "32"])

    # The scheduler address is now taken directly from the parsed arguments.
    cmd.extend(["--scheduler-addr", args.scheduler_addr])

    # Relay logic based on effective scheduler address
    if _should_use_public_relay(args.scheduler_addr, args.use_relay):
        cmd.extend(_get_relay_params())
        logger.info(
            "Using public relay server to help nodes and the scheduler establish a connection (remote mode). Your IP address will be reported to the relay server to help establish the connection."
        )

    if args.profile:
        cmd.extend(["--profile", args.profile])
    if not _flag_present(passthrough_args, ["--log-file"]):
        cmd.extend(["--log-file", log_file])

    # Append any passthrough args (unrecognized by this CLI) directly to the command
    if passthrough_args:
        cmd.extend(passthrough_args)

    logger.info(f"Scheduler address: {args.scheduler_addr}")
    _execute_with_graceful_shutdown(cmd, env=env)


def chat_command(args, passthrough_args: list[str] | None = None):
    """Start the Parallax chat server (equivalent to scripts/chat.sh)."""
    check_python_version()

    project_root = get_project_root()
    launch_script = project_root / "src" / "parallax" / "launch_chat.py"

    if not launch_script.exists():
        logger.info(f"Error: Launch chat script not found at {launch_script}")
        sys.exit(1)

    # Build the command to run the launch_chat.py script
    passthrough_args = passthrough_args or []
    cmd = [sys.executable, str(launch_script)]

    cmd.extend(["--scheduler-addr", args.scheduler_addr])

    # Relay logic based on effective scheduler address
    if _should_use_public_relay(args.scheduler_addr, args.use_relay):
        cmd.extend(_get_relay_params())
        logger.info(
            "Using public relay server to help chat client and the scheduler establish a connection (remote mode). Your IP address will be reported to the relay server to help establish the connection."
        )

    # Append any passthrough args (unrecognized by this CLI) directly to the command
    if passthrough_args:
        cmd.extend(passthrough_args)

    logger.info(f"Scheduler address: {args.scheduler_addr}")
    _execute_with_graceful_shutdown(cmd)


def update_package_info():
    """Update package information."""
    version = get_current_version()

    try:
        package_info = load_package_info()
        if package_info is not None and package_info["version"] == version:
            return

        save_package_info({"version": version})
    except Exception:
        pass


def load_package_info():
    """Load package information."""
    try:
        project_root = get_project_root()
        if not (project_root / ".cache" / "tmp_key.txt").exists():
            return None
        with open(project_root / ".cache" / "tmp_key.txt", "r") as f:
            return json.loads(reversible_decode_string(f.read()))
    except Exception:
        return None


def save_package_info(usage_info: dict):
    """Save package information."""
    project_root = get_project_root()
    os.makedirs(project_root / ".cache", exist_ok=True)
    with open(project_root / ".cache" / "tmp_key.txt", "w") as f:
        f.write(reversible_encode_string(json.dumps(usage_info)))

    upload_package_info(usage_info)


def upload_package_info(usage_info: dict):
    post_url = "https://chatbe-dev.gradient.network/api/v1/parallax/upload"
    headers = {
        "Content-Type": "application/json",
    }
    try:
        requests.post(post_url, headers=headers, json=usage_info, timeout=5)
        return
    except Exception:
        return


def reversible_encode_string(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("utf-8")


def reversible_decode_string(encoded: str) -> str:
    return base64.urlsafe_b64decode(encoded.encode("utf-8")).decode("utf-8")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Parallax - A fully decentralized inference engine developed by Gradient Network",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  parallax run                                                          # Start scheduler with frontend
  parallax run -m {model-name} -n {number-of-worker-nodes}              # Start scheduler without frontend
  parallax run -m Qwen/Qwen3-0.6B -n 2                                  # example
  parallax dev-run                                                      # Start backend + Vite frontend with live reload
  parallax join                                                         # Join cluster in local network
  parallax join -s {scheduler-address}                                  # Join cluster in public network
  parallax join -s 12D3KooWLX7MWuzi1Txa5LyZS4eTQ2tPaJijheH8faHggB9SxnBu # example
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add 'run' command parser
    run_parser = subparsers.add_parser(
        "run", help="Start the Parallax scheduler (equivalent to scripts/start.sh)"
    )
    run_parser.add_argument("-n", "--init-nodes-num", type=int, help="Number of initial nodes")
    run_parser.add_argument("-m", "--model-name", type=str, help="Model name")
    run_parser.add_argument(
        "-r", "--use-relay", action="store_true", help="Use public relay servers"
    )
    run_parser.add_argument(
        "-u", "--skip-upload", action="store_true", help="Skip upload package info"
    )
    run_parser.add_argument(
        "--log-file",
        type=str,
        help="Optional path to write scheduler logs to a file",
    )
    run_parser.add_argument(
        "--build-frontend",
        action="store_true",
        help="Build frontend assets on startup if the local frontend build is stale",
    )
    run_parser.add_argument(
        "--profile",
        default="auto",
        help="Runtime recovery profile name. Defaults to auto-detect.",
    )
    run_parser.add_argument(
        "--nodes-host-file",
        type=str,
        help="Optional file listing SSH-reachable node hosts to track alongside live joined nodes.",
    )

    dev_run_parser = subparsers.add_parser(
        "dev-run", help="Start the scheduler backend together with the Vite frontend dev server"
    )
    dev_run_parser.add_argument("-n", "--init-nodes-num", type=int, help="Number of initial nodes")
    dev_run_parser.add_argument("-m", "--model-name", type=str, help="Model name")
    dev_run_parser.add_argument(
        "-r", "--use-relay", action="store_true", help="Use public relay servers"
    )
    dev_run_parser.add_argument(
        "-u", "--skip-upload", action="store_true", help="Skip upload package info"
    )
    dev_run_parser.add_argument(
        "--log-file",
        type=str,
        help="Optional path to write scheduler logs to a file",
    )
    dev_run_parser.add_argument(
        "--profile",
        default="auto",
        help="Runtime recovery profile name. Defaults to auto-detect.",
    )
    dev_run_parser.add_argument(
        "--nodes-host-file",
        type=str,
        help="Optional file listing SSH-reachable node hosts to track alongside live joined nodes.",
    )
    dev_run_parser.add_argument(
        "--frontend-port",
        type=int,
        default=5173,
        help="Port for the Vite frontend dev server.",
    )
    dev_run_parser.add_argument(
        "--frontend-host",
        type=str,
        default="127.0.0.1",
        help="Host for the Vite frontend dev server.",
    )

    # Add 'join' command parser
    join_parser = subparsers.add_parser(
        "join", help="Join a distributed cluster (equivalent to scripts/join.sh)"
    )
    join_parser.add_argument(
        "-s",
        "--scheduler-addr",
        default="auto",
        type=str,
        help="Scheduler address (required)",
    )
    join_parser.add_argument(
        "-r", "--use-relay", action="store_true", help="Use public relay servers"
    )
    join_parser.add_argument(
        "-u", "--skip-upload", action="store_true", help="Skip upload package info"
    )
    join_parser.add_argument(
        "--log-file",
        type=str,
        help="Optional path to write node logs to a file",
    )
    join_parser.add_argument(
        "--profile",
        default="auto",
        help="Runtime recovery profile name. Defaults to auto-detect.",
    )

    build_frontend_parser = subparsers.add_parser(
        "build-frontend",
        aliases=["build:frontend"],
        help="Build the frontend bundle in src/frontend",
    )

    # Add 'chat' command parser
    chat_parser = subparsers.add_parser(
        "chat", help="Start the Parallax chat server (equivalent to scripts/chat.sh)"
    )
    chat_parser.add_argument(
        "-s",
        "--scheduler-addr",
        default="auto",
        type=str,
        help="Scheduler address (required)",
    )
    chat_parser.add_argument(
        "-r", "--use-relay", action="store_true", help="Use public relay servers"
    )

    # Accept unknown args and pass them through to the underlying python command
    args, passthrough_args = parser.parse_known_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "run":
        run_command(args, passthrough_args)
    elif args.command == "dev-run":
        dev_run_command(args, passthrough_args)
    elif args.command == "join":
        join_command(args, passthrough_args)
    elif args.command == "chat":
        chat_command(args, passthrough_args)
    elif args.command in {"build-frontend", "build:frontend"}:
        build_frontend_command(args, passthrough_args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

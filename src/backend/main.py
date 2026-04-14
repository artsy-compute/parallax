import asyncio
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.server.node_management import NodeManagementService
from backend.server.request_handler import RequestHandler
from backend.server.scheduler_manage import SchedulerManage
from backend.server.server_args import parse_args
from backend.server.static_config import (
    get_model_list,
    get_node_join_command,
    init_model_info_dict_cache,
)
from parallax_utils.ascii_anime import display_parallax_run
from parallax_utils.file_util import get_project_root
from parallax_utils.logging_config import get_logger, set_log_file, set_log_level
from parallax_utils.version_check import check_latest_release

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = get_logger(__name__)

scheduler_manage = None
node_management = None
request_handler = RequestHandler()

FRONTEND_DIR = get_project_root() / "src" / "frontend"
FRONTEND_SRC_DIR = FRONTEND_DIR / "src"
FRONTEND_DIST_DIR = FRONTEND_DIR / "dist"
FRONTEND_INDEX_PATH = FRONTEND_DIST_DIR / "index.html"
_FRONTEND_BUILD_STATUS = {
    "stale": False,
    "reason": "",
    "checked_at": 0.0,
    "dist_mtime": 0.0,
    "latest_source_mtime": 0.0,
}


def _frontend_source_paths() -> list[Path]:
    candidates = [
        FRONTEND_DIR / "package.json",
        FRONTEND_DIR / "package-lock.json",
        FRONTEND_DIR / "tsconfig.json",
        FRONTEND_DIR / "vite.config.ts",
        FRONTEND_DIR / "vite.config.js",
    ]
    for base in (FRONTEND_DIR / "src", FRONTEND_DIR / "public"):
        if base.exists():
            candidates.extend(path for path in base.rglob("*") if path.is_file())
    return [path for path in candidates if path.exists() and path.is_file()]


def check_frontend_build_status() -> dict:
    dist_exists = FRONTEND_INDEX_PATH.exists()
    dist_mtime = FRONTEND_INDEX_PATH.stat().st_mtime if dist_exists else 0.0
    source_paths = _frontend_source_paths()
    latest_source_mtime = max((path.stat().st_mtime for path in source_paths), default=0.0)

    stale = not dist_exists or latest_source_mtime > dist_mtime
    if not dist_exists:
        reason = f"missing build artifact: {FRONTEND_INDEX_PATH}"
    elif latest_source_mtime > dist_mtime:
        reason = "frontend source files are newer than dist/index.html"
    else:
        reason = ""

    _FRONTEND_BUILD_STATUS.update(
        {
            "stale": stale,
            "reason": reason,
            "checked_at": time.time(),
            "dist_mtime": dist_mtime,
            "latest_source_mtime": latest_source_mtime,
        }
    )
    return dict(_FRONTEND_BUILD_STATUS)


def ensure_frontend_build(build_frontend: bool = False) -> None:
    status = check_frontend_build_status()
    if not status["stale"]:
        return

    auto_build = build_frontend or os.environ.get("PARALLAX_AUTO_BUILD_FRONTEND", "").lower() in {"1", "true", "yes", "on"}
    if auto_build:
        logger.warning("Frontend build is stale; running `npm run build` in %s", FRONTEND_DIR)
        subprocess.run(["npm", "run", "build"], cwd=FRONTEND_DIR, check=True)
        status = check_frontend_build_status()
        if status["stale"]:
            logger.warning("Frontend build still appears stale after rebuild: %s", status["reason"])
        else:
            logger.info("Frontend build completed and is up to date.")
        return

    logger.warning(
        "Frontend build is stale: %s. Rebuild with `cd %s && npm run build` or set PARALLAX_AUTO_BUILD_FRONTEND=1.",
        status["reason"],
        FRONTEND_DIR,
    )


@app.post("/weight/refit")
async def weight_refit(raw_request: Request):
    request_data = await raw_request.json()
    status = scheduler_manage.weight_refit(request_data)
    if status:
        return JSONResponse(
            content={
                "type": "weight_refit",
                "data": None,
            },
            status_code=200,
        )
    else:
        return JSONResponse(
            content={
                "type": "weight_refit",
                "data": "Sever not ready",
            },
            status_code=500,
        )


@app.get("/weight/refit/timestamp")
async def weight_refit_timstamp():
    last_refit_time = scheduler_manage.get_last_refit_time()

    return JSONResponse(
        content={
            "latest_timestamp": last_refit_time,
        },
        status_code=200,
    )


@app.get("/model/list")
async def model_list():
    return JSONResponse(
        content={
            "type": "model_list",
            "data": get_model_list(),
        },
        status_code=200,
    )


@app.post("/scheduler/init")
async def scheduler_init(raw_request: Request):
    request_data = await raw_request.json()
    model_name = request_data.get("model_name")
    init_nodes_num = request_data.get("init_nodes_num")
    is_local_network = request_data.get("is_local_network")

    # Validate required parameters
    if model_name is None:
        return JSONResponse(
            content={
                "type": "scheduler_init",
                "error": "model_name is required",
            },
            status_code=400,
        )
    if init_nodes_num is None:
        return JSONResponse(
            content={
                "type": "scheduler_init",
                "error": "init_nodes_num is required",
            },
            status_code=400,
        )

    try:
        # If scheduler is already running, stop it first
        if scheduler_manage.is_running():
            logger.info(f"Stopping existing scheduler to switch to model: {model_name}")
            scheduler_manage.stop()

        # Start scheduler with new model
        logger.info(
            f"Initializing scheduler with model: {model_name}, init_nodes_num: {init_nodes_num}"
        )
        scheduler_manage.run(model_name, init_nodes_num, is_local_network)

        return JSONResponse(
            content={
                "type": "scheduler_init",
                "data": {
                    "model_name": model_name,
                    "init_nodes_num": init_nodes_num,
                    "is_local_network": is_local_network,
                },
            },
            status_code=200,
        )
    except Exception as e:
        logger.exception(f"Error initializing scheduler: {e}")
        return JSONResponse(
            content={
                "type": "scheduler_init",
                "error": str(e),
            },
            status_code=500,
        )


@app.get("/node/join/command")
async def node_join_command():
    peer_id = scheduler_manage.get_peer_id()
    is_local_network = scheduler_manage.get_is_local_network()

    return JSONResponse(
        content={
            "type": "node_join_command",
            "data": get_node_join_command(peer_id, is_local_network),
        },
        status_code=200,
    )


@app.get("/cluster/status")
async def cluster_status():
    async def stream_cluster_status():
        while True:
            yield json.dumps(scheduler_manage.get_cluster_status(), ensure_ascii=False) + "\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        stream_cluster_status(),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@app.get("/cluster/status_json")
async def cluster_status_json() -> JSONResponse:
    if scheduler_manage is None:
        return JSONResponse(content={"error": "Scheduler is not initialized"}, status_code=503)
    return JSONResponse(content=scheduler_manage.get_cluster_status(), status_code=200)


@app.get("/nodes/overview")
async def nodes_overview() -> JSONResponse:
    if node_management is None:
        return JSONResponse(
            content={"type": "nodes_overview", "data": {"summary": {}, "hosts": []}},
            status_code=503,
        )
    return JSONResponse(
        content={"type": "nodes_overview", "data": node_management.get_overview()},
        status_code=200,
    )


@app.post("/nodes/ping")
async def nodes_ping(raw_request: Request) -> JSONResponse:
    if node_management is None:
        return JSONResponse(
            content={"type": "node_ping", "data": {"ok": False, "message": "Node management is not initialized"}},
            status_code=503,
        )
    request_data = await raw_request.json()
    result = node_management.ping_host(str(request_data.get("ssh_target") or ""))
    return JSONResponse(
        content={"type": "node_ping", "data": result},
        status_code=200 if result.get("ok") else 409,
    )


@app.post("/nodes/logs")
async def nodes_logs(raw_request: Request) -> JSONResponse:
    if node_management is None:
        return JSONResponse(
            content={"type": "node_logs", "data": {"ok": False, "message": "Node management is not initialized", "content": ""}},
            status_code=503,
        )
    request_data = await raw_request.json()
    result = node_management.tail_logs(
        str(request_data.get("ssh_target") or ""),
        int(request_data.get("lines") or 200),
    )
    return JSONResponse(
        content={"type": "node_logs", "data": result},
        status_code=200 if result.get("ok") else 409,
    )



@app.post("/nodes/start")
async def nodes_start(raw_request: Request) -> JSONResponse:
    if node_management is None:
        return JSONResponse(
            content={"type": "node_start", "data": {"ok": False, "message": "Node management is not initialized"}},
            status_code=503,
        )
    request_data = await raw_request.json()
    result = node_management.start_host(str(request_data.get("ssh_target") or ""))
    return JSONResponse(
        content={"type": "node_start", "data": result},
        status_code=200 if result.get("ok") else 409,
    )


@app.post("/nodes/stop")
async def nodes_stop(raw_request: Request) -> JSONResponse:
    if node_management is None:
        return JSONResponse(
            content={"type": "node_stop", "data": {"ok": False, "message": "Node management is not initialized"}},
            status_code=503,
        )
    request_data = await raw_request.json()
    result = node_management.stop_host(str(request_data.get("ssh_target") or ""))
    return JSONResponse(
        content={"type": "node_stop", "data": result},
        status_code=200 if result.get("ok") else 409,
    )


@app.post("/nodes/restart")
async def nodes_restart(raw_request: Request) -> JSONResponse:
    if node_management is None:
        return JSONResponse(
            content={"type": "node_restart", "data": {"ok": False, "message": "Node management is not initialized"}},
            status_code=503,
        )
    request_data = await raw_request.json()
    result = node_management.restart_host(str(request_data.get("ssh_target") or ""))
    return JSONResponse(
        content={"type": "node_restart", "data": result},
        status_code=200 if result.get("ok") else 409,
    )


@app.post("/cluster/rebalance")
async def cluster_rebalance() -> JSONResponse:
    if scheduler_manage is None:
        return JSONResponse(
            content={"type": "cluster_rebalance", "data": {"ok": False, "message": "Scheduler is not initialized"}},
            status_code=503,
        )

    ok, message = scheduler_manage.request_topology_rebalance()
    return JSONResponse(
        content={"type": "cluster_rebalance", "data": {"ok": ok, "message": message}},
        status_code=200 if ok else 409,
    )


@app.post("/v1/chat/completions")
async def openai_v1_chat_completions(raw_request: Request):
    request_data = await raw_request.json()
    request_id = uuid.uuid4()
    received_ts = time.time()
    return await request_handler.v1_chat_completions(request_data, request_id, received_ts)


@app.get("/chat/history")
async def chat_history_list(limit: int = 50):
    return JSONResponse(
        content={
            "type": "chat_history_list",
            "data": request_handler.chat_memory.list_conversations(limit=limit),
        },
        status_code=200,
    )


@app.get("/chat/history/{conversation_id}")
async def chat_history_detail(conversation_id: str):
    return JSONResponse(
        content={
            "type": "chat_history_detail",
            "data": request_handler.chat_memory.get_conversation(conversation_id),
        },
        status_code=200,
    )


@app.delete("/chat/history/{conversation_id}")
async def chat_history_delete(conversation_id: str):
    deleted = request_handler.chat_memory.delete_conversation(conversation_id)
    return JSONResponse(
        content={
            "type": "chat_history_delete",
            "data": {"deleted": deleted, "conversation_id": conversation_id},
        },
        status_code=200 if deleted else 404,
    )


@app.get("/frontend/build_status")
async def frontend_build_status():
    return JSONResponse(
        content={
            "type": "frontend_build_status",
            "data": check_frontend_build_status(),
        },
        status_code=200,
    )


# Disable caching for index.html
@app.get("/")
async def serve_index():
    status = check_frontend_build_status()
    response = FileResponse(str(FRONTEND_INDEX_PATH))
    # Disable cache
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["X-Parallax-Frontend-Stale"] = "1" if status["stale"] else "0"
    if status["reason"]:
        response.headers["X-Parallax-Frontend-Stale-Reason"] = status["reason"]
    return response


# mount the frontend
app.mount(
    "/",
    StaticFiles(directory=str(FRONTEND_DIST_DIR), html=True),
    name="static",
)

if __name__ == "__main__":
    args = parse_args()
    set_log_file(args.log_file)
    set_log_level(args.log_level)
    logger.info(f"args: {args}")

    ensure_frontend_build(args.build_frontend)

    if args.model_name is None:
        init_model_info_dict_cache(args.use_hfcache)

    if args.log_level != "DEBUG":
        display_parallax_run()

    check_latest_release()

    scheduler_manage = SchedulerManage(
        initial_peers=args.initial_peers,
        relay_servers=args.relay_servers,
        dht_prefix=args.dht_prefix,
        host_maddrs=[
            f"/ip4/0.0.0.0/tcp/{args.tcp_port}",
            f"/ip4/0.0.0.0/udp/{args.udp_port}/quic-v1",
        ],
        announce_maddrs=args.announce_maddrs,
        http_port=args.port,
        use_hfcache=args.use_hfcache,
        enable_weight_refit=args.enable_weight_refit,
        weight_refit_mode=args.weight_refit_mode,
        profile=args.profile,
        scheduler_heartbeat_timeout_sec=args.scheduler_heartbeat_timeout_sec,
        nodes_host_file=args.nodes_host_file,
    )

    node_management = NodeManagementService(scheduler_manage)
    request_handler.set_scheduler_manage(scheduler_manage)

    model_name = args.model_name
    init_nodes_num = args.init_nodes_num
    is_local_network = args.is_local_network

    if model_name is None or init_nodes_num is None:
        saved_runtime = scheduler_manage.load_runtime_config()
        if saved_runtime is not None:
            model_name = saved_runtime.get("model_name")
            init_nodes_num = saved_runtime.get("init_nodes_num")
            is_local_network = saved_runtime.get("is_local_network", is_local_network)
            logger.info(
                "Restored persisted scheduler config: model_name=%s init_nodes_num=%s is_local_network=%s",
                model_name,
                init_nodes_num,
                is_local_network,
            )

    if model_name is not None and init_nodes_num is not None:
        scheduler_manage.run(model_name, init_nodes_num, is_local_network)

    host = args.host
    port = args.port

    uvicorn.run(app, host=host, port=port, log_level="info", loop="uvloop")

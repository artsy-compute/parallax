import asyncio
import gzip
import io
import json
import os
import subprocess
import time
import uuid
import zlib
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.server.custom_models import CustomModelStore
from backend.server.knowledge_client import KnowledgeServiceClient, KnowledgeServiceError
from backend.server.node_management import NodeManagementService
from backend.server.request_handler import RequestHandler
from backend.server.scheduler_manage import SchedulerManage
from backend.server.tool_runtime import ServerToolRuntime
from backend.server.settings_store import SettingsStore
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
custom_model_store = CustomModelStore()
settings_store = SettingsStore()
request_handler = RequestHandler()
tool_runtime = ServerToolRuntime(settings_store=settings_store)
request_handler.tool_runtime = tool_runtime
knowledge_client = KnowledgeServiceClient()


def _content_encoding_has_token(value: str | None, token: str) -> bool:
    if not value:
        return False
    return any(entry.strip().lower() == token for entry in value.split(","))


async def _read_json_request(raw_request: Request) -> dict:
    body = await raw_request.body()
    content_encoding = raw_request.headers.get("content-encoding")

    try:
        if _content_encoding_has_token(content_encoding, "zstd"):
            try:
                import zstandard as zstd
            except ImportError as exc:
                raise HTTPException(
                    status_code=415,
                    detail=(
                        "Received Content-Encoding: zstd, but Python package "
                        "`zstandard` is not installed"
                    ),
                ) from exc
            with zstd.ZstdDecompressor().stream_reader(io.BytesIO(body)) as reader:
                body = reader.read()
        elif _content_encoding_has_token(content_encoding, "gzip"):
            body = gzip.decompress(body)
        elif _content_encoding_has_token(content_encoding, "deflate"):
            body = zlib.decompress(body)
        elif content_encoding not in (None, "", "identity"):
            raise HTTPException(
                status_code=415,
                detail=f"Unsupported Content-Encoding: {content_encoding}",
            )

        return json.loads(body.decode("utf-8"))
    except HTTPException:
        raise
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Request body is not valid UTF-8 JSON after decoding: {exc}",
        ) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {exc}") from exc
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Failed to decode request body with Content-Encoding "
                f"{content_encoding!r}: {exc}"
            ),
        ) from exc


def _merged_model_list() -> list[dict]:
    static_models = list(get_model_list())
    custom_models = custom_model_store.list_model_entries()
    merged_models: list[dict] = []
    seen_names: set[str] = set()
    for item in [*static_models, *custom_models]:
        name = str(item.get("name") or "").strip()
        if not name or name in seen_names:
            continue
        seen_names.add(name)
        merged_models.append(item)
    return merged_models


def _serialize_openai_model(item: dict) -> dict:
    name = str(item.get("name") or "").strip()
    return {
        "id": name,
        "object": "model",
        "created": int(item.get("created") or 0),
        "owned_by": "parallax",
        "metadata": {
            key: value
            for key, value in item.items()
            if key not in {"name", "created"}
        },
    }


tool_runtime.set_context(
    get_cluster_status=lambda: (
        scheduler_manage.get_cluster_status()
        if scheduler_manage is not None
        else {"ok": False, "error": "Scheduler is not initialized"}
    ),
    list_nodes=lambda: (
        scheduler_manage.get_node_list()
        if scheduler_manage is not None
        else []
    ),
    list_models=lambda: _merged_model_list(),
    get_join_command=lambda: get_node_join_command(
        scheduler_manage.get_join_scheduler_addr() if scheduler_manage is not None else None,
        scheduler_manage.get_is_local_network() if scheduler_manage is not None else True,
    ),
    get_nodes_overview=lambda: (
        node_management.get_overview()
        if node_management is not None
        else {"summary": {}, "hosts": []}
    ),
)

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
FRONTEND_DEV_SERVER_URL = os.environ.get("PARALLAX_FRONTEND_DEV_SERVER_URL", "").strip()


@app.middleware("http")
async def log_openai_api_requests(request: Request, call_next):
    if not str(request.url.path or "").startswith("/v1/"):
        return await call_next(request)

    started_at = time.time()
    client_host = request.client.host if request.client else "unknown"
    logger.info(
        "OpenAI API request started method=%s path=%s query=%s client=%s",
        request.method,
        request.url.path,
        request.url.query,
        client_host,
    )
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = int((time.time() - started_at) * 1000)
        logger.exception(
            "OpenAI API request failed method=%s path=%s client=%s elapsed_ms=%d",
            request.method,
            request.url.path,
            client_host,
            elapsed_ms,
        )
        raise

    elapsed_ms = int((time.time() - started_at) * 1000)
    logger.info(
        "OpenAI API request completed method=%s path=%s client=%s status=%s elapsed_ms=%d",
        request.method,
        request.url.path,
        client_host,
        response.status_code,
        elapsed_ms,
    )
    return response
PERSISTED_BIND_OVERRIDE = os.environ.get("PARALLAX_USE_PERSISTED_BIND_SETTINGS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _parse_custom_model_roots(values: list[str] | None) -> dict[str, str]:
    roots: dict[str, str] = {}
    for raw in values or []:
        item = str(raw or "").strip()
        if not item or "=" not in item:
            continue
        root_id, _, path = item.partition("=")
        root_id = str(root_id or "").strip()
        path = str(path or "").strip()
        if root_id and path:
            roots[root_id] = path
    return roots


def _custom_model_roots_from_env() -> dict[str, str]:
    raw = os.environ.get("PARALLAX_CUSTOM_MODEL_ROOTS", "").strip()
    if not raw:
        return {}
    return _parse_custom_model_roots([item for item in raw.split(",") if str(item).strip()])


def _default_custom_model_roots() -> dict[str, str]:
    default_root = (Path.cwd() / "custom-model-root").resolve()
    default_root.mkdir(parents=True, exist_ok=True)
    return {
        "custom-model-root": str(default_root),
    }


def _frontend_dev_server_redirect_target(path: str = "/") -> str | None:
    base = FRONTEND_DEV_SERVER_URL.rstrip("/")
    if not base:
        return None
    normalized_path = path if str(path or "").startswith("/") else f"/{path}"
    return f"{base}{normalized_path}"


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
    merged_models = _merged_model_list()
    return JSONResponse(
        content={
            "type": "model_list",
            "data": merged_models,
        },
        status_code=200,
    )


@app.get("/v1/models")
async def openai_v1_models():
    merged_models = _merged_model_list()
    return JSONResponse(
        content={
            "object": "list",
            "data": [_serialize_openai_model(item) for item in merged_models],
        },
        status_code=200,
    )


@app.get("/v1/models/{model_id:path}")
async def openai_v1_model_detail(model_id: str):
    normalized_model_id = str(model_id or "").strip()
    for item in _merged_model_list():
        if str(item.get("name") or "").strip() == normalized_model_id:
            return JSONResponse(content=_serialize_openai_model(item), status_code=200)
    return JSONResponse(
        content={
            "error": {
                "message": f"Model not found: {normalized_model_id}",
                "type": "invalid_request_error",
                "param": "model",
                "code": "not_found",
            }
        },
        status_code=404,
    )


@app.get("/model/custom")
async def custom_model_list():
    return JSONResponse(
        content={
            "type": "custom_model_list",
            "data": custom_model_store.list_models(),
        },
        status_code=200,
    )


@app.get("/model/custom/sources")
async def custom_model_sources():
    return JSONResponse(
        content={
            "type": "custom_model_sources",
            "data": {
                "supported_source_types": ["huggingface", "scheduler_root", "url"],
                "allowed_local_roots": custom_model_store.list_allowed_local_roots(),
                "allowed_local_model_options": custom_model_store.list_allowed_local_model_options(),
            },
        },
        status_code=200,
    )


@app.post("/model/custom")
async def custom_model_add(raw_request: Request):
    request_data = await raw_request.json()
    try:
        record = custom_model_store.add_model(
            source_type=str(request_data.get("source_type") or ""),
            source_value=str(request_data.get("source_value") or ""),
            display_name=str(request_data.get("display_name") or ""),
        )
    except ValueError as e:
        return JSONResponse(
            content={
                "type": "custom_model_add",
                "error": str(e),
            },
            status_code=400,
        )
    except Exception as e:
        logger.exception("Failed to add custom model: %s", e)
        return JSONResponse(
            content={
                "type": "custom_model_add",
                "error": str(e),
            },
            status_code=500,
        )
    return JSONResponse(
        content={
            "type": "custom_model_add",
            "data": record,
        },
        status_code=200,
    )


@app.get("/model/custom/search")
async def custom_model_search(query: str = "", limit: int = 8, offset: int = 0):
    try:
        results = custom_model_store.search_huggingface_models(query=query, limit=limit, offset=offset)
    except ValueError as e:
        return JSONResponse(
            content={
                "type": "custom_model_search",
                "error": str(e),
            },
            status_code=400,
        )
    except Exception as e:
        logger.exception("Failed to search Hugging Face models: %s", e)
        return JSONResponse(
            content={
                "type": "custom_model_search",
                "error": str(e),
            },
            status_code=500,
        )
    return JSONResponse(
        content={
            "type": "custom_model_search",
            "data": results,
        },
        status_code=200,
    )


@app.delete("/model/custom/{model_id}")
async def custom_model_delete(model_id: str):
    deleted = custom_model_store.delete_model(model_id)
    return JSONResponse(
        content={
            "type": "custom_model_delete",
            "data": {
                "deleted": deleted,
                "model_id": model_id,
            },
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


@app.get("/settings")
async def app_settings():
    cluster_settings = settings_store.get_cluster_settings()
    clusters_state = settings_store.get_clusters_state()
    return JSONResponse(
        content={
            "type": "app_settings",
            "data": {
                "cluster_settings": cluster_settings,
                "clusters": clusters_state.get("clusters") or [],
                "active_cluster_id": clusters_state.get("active_cluster_id") or "",
                "available_tools": tool_runtime.describe_available_tools(),
            },
        },
        status_code=200,
    )


@app.put("/settings")
async def app_settings_update(raw_request: Request):
    request_data = await raw_request.json()
    clusters_payload = request_data.get("clusters")
    active_cluster_id = str(request_data.get("active_cluster_id") or "").strip()
    if isinstance(clusters_payload, list):
        clusters_state = settings_store.replace_clusters_state(
            list(clusters_payload or []),
            active_cluster_id=active_cluster_id or None,
        )
    elif active_cluster_id:
        clusters_state = settings_store.set_active_cluster_id(active_cluster_id)
    else:
        clusters_state = settings_store.get_clusters_state()
    if isinstance(request_data.get("cluster_settings"), dict):
        cluster_settings = settings_store.set_cluster_settings(dict(request_data.get("cluster_settings") or {}))
        clusters_state = settings_store.get_clusters_state()
    else:
        cluster_settings = settings_store.get_cluster_settings()
    return JSONResponse(
        content={
            "type": "app_settings_update",
            "data": {
                "cluster_settings": cluster_settings,
                "clusters": clusters_state.get("clusters") or [],
                "active_cluster_id": clusters_state.get("active_cluster_id") or "",
                "available_tools": tool_runtime.describe_available_tools(),
            },
        },
        status_code=200,
    )


@app.get("/settings/export")
async def app_settings_export():
    cluster_settings = settings_store.get_cluster_settings()
    clusters_state = settings_store.get_clusters_state()
    runtime_advanced = dict(cluster_settings.get("advanced") or {})
    if scheduler_manage is not None:
        runtime_advanced.update(
            {
                "profile": scheduler_manage.profile,
                "scheduler_host": scheduler_manage.scheduler_host,
                "http_port": scheduler_manage.http_port,
                "tcp_port": scheduler_manage.tcp_port,
                "udp_port": scheduler_manage.udp_port,
                "announce_maddrs": list(scheduler_manage.announce_maddrs),
                "nodes_host_file": scheduler_manage.nodes_host_file,
            }
        )
    cluster_settings["advanced"] = runtime_advanced
    return JSONResponse(
        content={
            "type": "settings_export",
            "data": {
                "schema_version": 3,
                "cluster_settings": cluster_settings,
                "clusters": clusters_state.get("clusters") or [],
                "active_cluster_id": clusters_state.get("active_cluster_id") or "",
                "managed_node_hosts": settings_store.list_managed_node_hosts(),
                "custom_models": custom_model_store.export_models(),
            },
        },
        status_code=200,
    )


@app.post("/settings/import")
async def app_settings_import(raw_request: Request):
    request_data = await raw_request.json()
    try:
        if isinstance(request_data.get("clusters"), list):
            clusters_state = settings_store.replace_clusters_state(
                list(request_data.get("clusters") or []),
                active_cluster_id=str(request_data.get("active_cluster_id") or "").strip() or None,
            )
            cluster_settings = settings_store.get_cluster_settings()
        else:
            cluster_settings = settings_store.set_cluster_settings(dict(request_data.get("cluster_settings") or {}))
            clusters_state = settings_store.get_clusters_state()
        managed_node_hosts = settings_store.replace_managed_node_hosts(list(request_data.get("managed_node_hosts") or []))
        custom_models = custom_model_store.replace_models(list(request_data.get("custom_models") or []))
        if scheduler_manage is not None:
            scheduler_manage.configured_node_hosts = settings_store.list_managed_node_hosts()
    except ValueError as e:
        return JSONResponse(
            content={"type": "settings_import", "error": str(e)},
            status_code=400,
        )
    except Exception as e:
        logger.exception("Failed to import settings bundle: %s", e)
        return JSONResponse(
            content={"type": "settings_import", "error": str(e)},
            status_code=500,
        )
    return JSONResponse(
        content={
            "type": "settings_import",
            "data": {
                "cluster_settings": cluster_settings,
                "clusters": clusters_state.get("clusters") or [],
                "active_cluster_id": clusters_state.get("active_cluster_id") or "",
                "managed_node_hosts": managed_node_hosts,
                "custom_models": custom_models,
            },
        },
        status_code=200,
    )


@app.get("/node/join/command")
async def node_join_command():
    scheduler_addr = scheduler_manage.get_join_scheduler_addr()
    is_local_network = scheduler_manage.get_is_local_network()

    return JSONResponse(
        content={
            "type": "node_join_command",
            "data": get_node_join_command(scheduler_addr, is_local_network),
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


@app.get("/nodes/inventory")
async def nodes_inventory() -> JSONResponse:
    joined_hostnames: set[str] = set()
    if scheduler_manage is not None and scheduler_manage.scheduler is not None:
        joined_hostnames = {
            str(node.hardware.hostname or '').strip().lower()
            for node in scheduler_manage.scheduler.node_manager.nodes
            if getattr(node.hardware, 'hostname', None)
        }
    return JSONResponse(
        content={"type": "nodes_inventory", "data": {"hosts": settings_store.list_managed_node_hosts(joined_hostnames)}},
        status_code=200,
    )


@app.put("/nodes/inventory")
async def nodes_inventory_update(raw_request: Request) -> JSONResponse:
    request_data = await raw_request.json()
    try:
        hosts = settings_store.replace_managed_node_hosts(list(request_data.get("hosts") or []))
        if scheduler_manage is not None:
            scheduler_manage.configured_node_hosts = settings_store.list_managed_node_hosts()
    except Exception as e:
        logger.exception("Failed to update configured node inventory: %s", e)
        return JSONResponse(
            content={"type": "nodes_inventory_update", "data": {"ok": False, "message": str(e), "hosts": []}},
            status_code=500,
        )
    return JSONResponse(
        content={"type": "nodes_inventory_update", "data": {"ok": True, "message": "Configured node inventory updated", "hosts": hosts}},
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


@app.post("/nodes/probe")
async def nodes_probe(raw_request: Request) -> JSONResponse:
    if node_management is None:
        return JSONResponse(
            content={"type": "node_probe", "data": {"ok": False, "message": "Node management is not initialized"}},
            status_code=503,
        )
    request_data = await raw_request.json()
    result = node_management.probe_candidate_host(
        str(request_data.get("ssh_target") or ""),
        str(request_data.get("parallax_path") or ""),
    )
    return JSONResponse(
        content={"type": "node_probe", "data": result},
        status_code=200 if result.get("ssh_reachable") else 409,
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
    request_data = await _read_json_request(raw_request)
    logger.info(
        "Handling /v1/chat/completions model=%s stream=%s conversation_id=%s content_encoding=%s",
        str(request_data.get("model") or ""),
        bool(request_data.get("stream", False)),
        str(request_data.get("conversation_id") or ""),
        str(raw_request.headers.get("content-encoding") or ""),
    )
    request_id = uuid.uuid4()
    received_ts = time.time()
    return await request_handler.v1_chat_completions(request_data, request_id, received_ts)


@app.post("/v1/responses")
async def openai_v1_responses(raw_request: Request):
    request_data = await _read_json_request(raw_request)
    logger.info(
        "Handling /v1/responses model=%s stream=%s previous_response_id=%s content_encoding=%s",
        str(request_data.get("model") or ""),
        bool(request_data.get("stream", False)),
        str(request_data.get("previous_response_id") or ""),
        str(raw_request.headers.get("content-encoding") or ""),
    )
    request_id = uuid.uuid4()
    received_ts = time.time()
    return await request_handler.v1_responses(request_data, request_id, received_ts)


@app.get("/v1/responses/{response_id}")
async def openai_v1_response_detail(response_id: str):
    payload = request_handler.get_response(response_id)
    if payload is None:
        return JSONResponse(
            content={
                "error": {
                    "message": f"Response not found: {response_id}",
                    "type": "invalid_request_error",
                    "param": "response_id",
                    "code": "not_found",
                }
            },
            status_code=404,
        )
    return JSONResponse(content=payload, status_code=200)


@app.get("/v1/responses/{response_id}/input_items")
async def openai_v1_response_input_items(response_id: str):
    payload = request_handler.get_response_input_items(response_id)
    if payload is None:
        return JSONResponse(
            content={
                "error": {
                    "message": f"Response not found: {response_id}",
                    "type": "invalid_request_error",
                    "param": "response_id",
                    "code": "not_found",
                }
            },
            status_code=404,
        )
    return JSONResponse(content=payload, status_code=200)


@app.post("/v1/responses/{response_id}/cancel")
async def openai_v1_response_cancel(response_id: str):
    payload = request_handler.cancel_response(response_id)
    if payload is None:
        return JSONResponse(
            content={
                "error": {
                    "message": f"Response not found: {response_id}",
                    "type": "invalid_request_error",
                    "param": "response_id",
                    "code": "not_found",
                }
            },
            status_code=404,
        )
    return JSONResponse(content=payload, status_code=200)


@app.get("/chat/history")
async def chat_history_list(limit: int = 20, offset: int = 0):
    return JSONResponse(
        content={
            "type": "chat_history_list",
            "data": request_handler.chat_memory.list_conversations(limit=limit, offset=offset),
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


@app.delete("/chat/history")
async def chat_history_delete_all():
    deleted = request_handler.chat_memory.delete_all_conversations()
    return JSONResponse(
        content={
            "type": "chat_history_delete_all",
            "data": {"deleted": deleted},
        },
        status_code=200,
    )


def _knowledge_error_response(message_type: str, error: KnowledgeServiceError) -> JSONResponse:
    payload: dict[str, object] = {"type": message_type, "error": str(error)}
    if message_type == "knowledge_health":
        payload["data"] = {"ok": False, "error": str(error)}
    return JSONResponse(
        content=payload,
        status_code=max(400, min(int(error.status_code or 500), 599)),
    )


@app.get("/knowledge/health")
async def knowledge_health() -> JSONResponse:
    try:
        payload = await knowledge_client.health()
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_health", error)
    return JSONResponse(
        content={"type": "knowledge_health", "data": payload},
        status_code=200,
    )


@app.get("/knowledge/sources")
async def knowledge_sources(limit: int = 100) -> JSONResponse:
    try:
        payload = await knowledge_client.list_sources(limit=limit)
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_sources", error)
    return JSONResponse(
        content={"type": "knowledge_sources", "data": {"items": payload}},
        status_code=200,
    )


@app.get("/knowledge/sources/{source_id}")
async def knowledge_source_detail(source_id: str) -> JSONResponse:
    try:
        payload = await knowledge_client.get_source(source_id)
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_source_detail", error)
    return JSONResponse(
        content={"type": "knowledge_source_detail", "data": payload},
        status_code=200,
    )


@app.post("/knowledge/sources/local")
async def knowledge_source_local(raw_request: Request) -> JSONResponse:
    request_data = await _read_json_request(raw_request)
    try:
        payload = await knowledge_client.ingest_local_source(str(request_data.get("path") or ""))
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_source_create", error)
    return JSONResponse(
        content={"type": "knowledge_source_create", "data": payload},
        status_code=200,
    )


@app.post("/knowledge/sources/url")
async def knowledge_source_url(raw_request: Request) -> JSONResponse:
    request_data = await _read_json_request(raw_request)
    try:
        payload = await knowledge_client.ingest_url_source(str(request_data.get("url") or ""))
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_source_create", error)
    return JSONResponse(
        content={"type": "knowledge_source_create", "data": payload},
        status_code=200,
    )


@app.get("/knowledge/search")
async def knowledge_search(q: str, limit: int = 10) -> JSONResponse:
    try:
        payload = await knowledge_client.search(q, limit=limit)
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_search", error)
    return JSONResponse(
        content={"type": "knowledge_search", "data": payload},
        status_code=200,
    )


@app.get("/knowledge/documents/{document_id}")
async def knowledge_document_detail(document_id: str) -> JSONResponse:
    try:
        payload = await knowledge_client.get_document(document_id)
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_document_detail", error)
    return JSONResponse(
        content={"type": "knowledge_document_detail", "data": payload},
        status_code=200,
    )


@app.get("/knowledge/jobs")
async def knowledge_jobs(limit: int = 20) -> JSONResponse:
    try:
        payload = await knowledge_client.list_jobs(limit=limit)
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_jobs", error)
    return JSONResponse(
        content={"type": "knowledge_jobs", "data": {"items": payload}},
        status_code=200,
    )


@app.get("/knowledge/jobs/{job_id}")
async def knowledge_job_detail(job_id: str) -> JSONResponse:
    try:
        payload = await knowledge_client.get_job(job_id)
    except KnowledgeServiceError as error:
        return _knowledge_error_response("knowledge_job_detail", error)
    return JSONResponse(
        content={"type": "knowledge_job_detail", "data": payload},
        status_code=200,
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
    redirect_target = _frontend_dev_server_redirect_target("/")
    if redirect_target:
        return RedirectResponse(url=redirect_target, status_code=307)
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


@app.get("/chat.html")
async def serve_chat_html():
    redirect_target = _frontend_dev_server_redirect_target("/chat.html")
    if redirect_target:
        return RedirectResponse(url=redirect_target, status_code=307)
    return FileResponse(str(FRONTEND_DIST_DIR / "chat.html"))


@app.websocket("/{path:path}")
async def reject_unhandled_websocket(path: str, websocket: WebSocket):
    logger.info("Rejecting unsupported websocket path=/%s", path)
    await websocket.close(code=1003, reason="WebSocket not supported on this endpoint")


# mount the frontend
app.mount(
    "/",
    StaticFiles(directory=str(FRONTEND_DIST_DIR), html=True),
    name="static",
)

if __name__ == "__main__":
    args = parse_args()
    configured_custom_model_roots = {
        **_custom_model_roots_from_env(),
        **_parse_custom_model_roots(getattr(args, "custom_model_root", []) or []),
    }
    custom_model_store.configure_allowed_local_roots(
        configured_custom_model_roots or _default_custom_model_roots()
    )
    stored_cluster_settings = settings_store.get_cluster_settings()
    stored_advanced = dict(stored_cluster_settings.get("advanced") or {})
    if PERSISTED_BIND_OVERRIDE and args.host == "localhost" and stored_advanced.get("scheduler_host"):
        args.host = str(stored_advanced.get("scheduler_host"))
    if PERSISTED_BIND_OVERRIDE and args.port == 3001 and stored_advanced.get("http_port") is not None:
        args.port = int(stored_advanced.get("http_port"))
    if args.tcp_port == 0 and stored_advanced.get("tcp_port") is not None:
        args.tcp_port = int(stored_advanced.get("tcp_port"))
    if args.udp_port == 0 and stored_advanced.get("udp_port") is not None:
        args.udp_port = int(stored_advanced.get("udp_port"))
    if not args.announce_maddrs and isinstance(stored_advanced.get("announce_maddrs"), list):
        args.announce_maddrs = [str(item) for item in stored_advanced.get("announce_maddrs") or [] if str(item)]
    if args.profile == "auto" and stored_advanced.get("profile"):
        args.profile = str(stored_advanced.get("profile"))
    if args.nodes_host_file is None and stored_advanced.get("nodes_host_file"):
        args.nodes_host_file = str(stored_advanced.get("nodes_host_file"))
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
        scheduler_host=args.host,
        use_hfcache=args.use_hfcache,
        enable_weight_refit=args.enable_weight_refit,
        weight_refit_mode=args.weight_refit_mode,
        profile=args.profile,
        scheduler_heartbeat_timeout_sec=args.scheduler_heartbeat_timeout_sec,
        nodes_host_file=args.nodes_host_file,
        settings_store=settings_store,
        tcp_port=args.tcp_port,
        udp_port=args.udp_port,
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

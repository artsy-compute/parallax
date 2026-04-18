from __future__ import annotations

from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from knowledge_service.config import KnowledgeServiceConfig, load_config
from knowledge_service.ingest.urls import extract_url_document
from knowledge_service.models import LocalSourceCreateRequest, UrlSourceCreateRequest
from knowledge_service.store import KnowledgeStore


def create_app(config: KnowledgeServiceConfig | None = None) -> FastAPI:
    config = config or load_config()
    store = KnowledgeStore(config)
    app = FastAPI(title="Parallax Knowledge Service")
    app.state.config = config
    app.state.store = store

    def _service_store() -> KnowledgeStore:
        return app.state.store

    def _as_http_exception(error: Exception) -> HTTPException:
        if isinstance(error, HTTPException):
            return error
        if isinstance(error, ValueError):
            return HTTPException(status_code=400, detail=str(error))
        return HTTPException(status_code=500, detail=str(error))

    @app.get("/health")
    async def health(workspace_root: str | None = None) -> dict[str, Any]:
        return await run_in_threadpool(_service_store().health, workspace_root)

    @app.get("/sources")
    async def list_sources(
        workspace_root: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict[str, Any]:
        items = await run_in_threadpool(_service_store().list_sources, workspace_root, limit)
        return {"items": items}

    @app.get("/sources/{source_id}")
    async def source_detail(
        source_id: str,
        workspace_root: str | None = None,
    ) -> dict[str, Any]:
        item = await run_in_threadpool(_service_store().get_source, workspace_root, source_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Source not found: {source_id}")
        return item

    @app.delete("/sources/{source_id}")
    async def delete_source(
        source_id: str,
        workspace_root: str | None = None,
    ) -> dict[str, Any]:
        item = await run_in_threadpool(_service_store().delete_source, workspace_root, source_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Source not found: {source_id}")
        return item

    @app.post("/sources/local")
    async def ingest_local_source(request: LocalSourceCreateRequest) -> dict[str, Any]:
        try:
            return await run_in_threadpool(
                _service_store().ingest_local_source,
                request.workspace_root,
                request.path,
            )
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.post("/sources/url")
    async def ingest_url_source(request: UrlSourceCreateRequest) -> dict[str, Any]:
        try:
            extracted = await extract_url_document(
                request.url,
                timeout_sec=config.fetch_timeout_sec,
                max_chars=config.max_url_chars,
            )
            return await run_in_threadpool(
                _service_store().ingest_url_source,
                request.workspace_root,
                request.url,
                extracted,
            )
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.get("/search")
    async def search(
        q: str = Query(min_length=1),
        workspace_root: str | None = None,
        limit: int = Query(default=10, ge=1, le=50),
    ) -> dict[str, Any]:
        return await run_in_threadpool(_service_store().search, workspace_root, q, limit)

    @app.get("/documents/{document_id}")
    async def document_detail(
        document_id: str,
        workspace_root: str | None = None,
    ) -> dict[str, Any]:
        item = await run_in_threadpool(_service_store().get_document, workspace_root, document_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Document not found: {document_id}")
        return item

    @app.get("/jobs")
    async def jobs(
        workspace_root: str | None = None,
        limit: int = Query(default=20, ge=1, le=200),
    ) -> dict[str, Any]:
        items = await run_in_threadpool(_service_store().list_jobs, workspace_root, limit)
        return {"items": items}

    @app.get("/jobs/{job_id}")
    async def job_detail(job_id: str, workspace_root: str | None = None) -> dict[str, Any]:
        item = await run_in_threadpool(_service_store().get_job, workspace_root, job_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        return item

    return app


app = create_app()


def main() -> None:
    config = load_config()
    uvicorn.run(
        "knowledge_service.app:app",
        host=config.host,
        port=config.port,
        log_level="info",
        loop="uvloop",
    )


if __name__ == "__main__":
    main()

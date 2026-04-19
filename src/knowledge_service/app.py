from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Allow running the knowledge service directly from a source checkout without
# requiring PYTHONPATH=src.
_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.concurrency import run_in_threadpool

from knowledge_service.config import KnowledgeServiceConfig, load_config
from knowledge_service.ingest.local_files import extract_uploaded_document, resolve_structured_document_suffix
from knowledge_service.ingest.urls import extract_fetched_url_document, extract_url_document, fetch_url_content
from knowledge_service.models import LibraryPathRequest, LibraryUrlImportRequest, LocalSourceCreateRequest, UrlSourceCreateRequest
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

    @app.post("/sources/upload")
    async def ingest_uploaded_source(
        file: UploadFile = File(...),
        workspace_root: str | None = Form(default=None),
    ) -> dict[str, Any]:
        try:
            file_bytes = await file.read()
            extracted = extract_uploaded_document(
                file.filename or "uploaded-document",
                file_bytes,
                file.content_type,
            )
            if extracted is None:
                raise ValueError(
                    "The uploaded file did not produce readable text. "
                    "PDF, DOCX, and OpenDocument text formats are supported. OCR is not enabled."
                )
            return await run_in_threadpool(
                _service_store().ingest_uploaded_source,
                workspace_root,
                file.filename or extracted.title,
                extracted,
            )
        except Exception as error:
            raise _as_http_exception(error) from error
        finally:
            await file.close()

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

    @app.get("/library")
    async def library(
        workspace_root: str | None = None,
        path: str | None = None,
    ) -> dict[str, Any]:
        try:
            return await run_in_threadpool(_service_store().list_library, workspace_root, path)
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.get("/library/file")
    async def library_file(
        path: str = Query(min_length=1),
        workspace_root: str | None = None,
    ) -> dict[str, Any]:
        try:
            return await run_in_threadpool(_service_store().get_library_file, workspace_root, path)
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.delete("/library/file")
    async def delete_library_file(
        path: str = Query(min_length=1),
        workspace_root: str | None = None,
    ) -> dict[str, Any]:
        try:
            return await run_in_threadpool(_service_store().delete_library_file, workspace_root, path)
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.post("/library/file/delete")
    async def delete_library_file_post(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await run_in_threadpool(
                _service_store().delete_library_file,
                payload.get("workspace_root"),
                str(payload.get("path") or ""),
            )
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.post("/library/upload")
    async def library_upload(
        file: UploadFile = File(...),
        directory: str | None = Form(default=None),
        workspace_root: str | None = Form(default=None),
    ) -> dict[str, Any]:
        try:
            return await run_in_threadpool(
                _service_store().upload_library_file,
                workspace_root,
                directory=directory,
                filename=file.filename or "uploaded-file",
                data=await file.read(),
            )
        except Exception as error:
            raise _as_http_exception(error) from error
        finally:
            await file.close()

    @app.post("/library/url")
    async def library_url(request: LibraryUrlImportRequest) -> dict[str, Any]:
        try:
            fetched = await fetch_url_content(
                request.url,
                timeout_sec=config.fetch_timeout_sec,
            )
            extracted_text: str | None = None
            title: str | None = None
            structured_suffix = resolve_structured_document_suffix(
                Path(fetched.filename_hint),
                fetched.content_type or None,
            )
            if not structured_suffix:
                extracted = extract_fetched_url_document(
                    fetched,
                    max_chars=config.max_url_chars,
                )
                extracted_text = extracted.text
                title = extracted.title
            return await run_in_threadpool(
                _service_store().import_url_to_library,
                request.workspace_root,
                url=request.url,
                raw_bytes=fetched.raw_bytes,
                content_type=fetched.content_type,
                filename_hint=fetched.filename_hint,
                extracted_text=extracted_text,
                title=title,
            )
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.post("/library/ingest")
    async def library_ingest(request: LibraryPathRequest) -> dict[str, Any]:
        try:
            return await run_in_threadpool(
                _service_store().ingest_library_path,
                request.workspace_root,
                request.path,
            )
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.get("/pages")
    async def pages(workspace_root: str | None = None) -> dict[str, Any]:
        return await run_in_threadpool(_service_store().list_pages, workspace_root)

    @app.delete("/pages")
    async def delete_pages(workspace_root: str | None = None) -> dict[str, Any]:
        return await run_in_threadpool(_service_store().delete_pages, workspace_root)

    @app.get("/pages/{page_id}")
    async def page_detail(page_id: str, workspace_root: str | None = None) -> dict[str, Any]:
        item = await run_in_threadpool(_service_store().get_page, workspace_root, page_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Page not found: {page_id}")
        return item

    @app.post("/pages/generate")
    async def generate_pages(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await run_in_threadpool(
                _service_store().generate_wiki,
                payload.get("workspace_root"),
                advanced=dict(payload.get("advanced") or {}),
                cluster_model_name=str(payload.get("cluster_model_name") or ""),
                backend_base_url=str(payload.get("backend_base_url") or ""),
            )
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.post("/wiki/lint")
    async def lint_wiki(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await run_in_threadpool(
                _service_store().lint_wiki,
                payload.get("workspace_root"),
                advanced=dict(payload.get("advanced") or {}),
                cluster_model_name=str(payload.get("cluster_model_name") or ""),
                backend_base_url=str(payload.get("backend_base_url") or ""),
            )
        except Exception as error:
            raise _as_http_exception(error) from error

    @app.post("/pages/{page_id}/generate")
    async def regenerate_page(page_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            item = await run_in_threadpool(
                _service_store().regenerate_page,
                payload.get("workspace_root"),
                page_id,
                advanced=dict(payload.get("advanced") or {}),
                cluster_model_name=str(payload.get("cluster_model_name") or ""),
                backend_base_url=str(payload.get("backend_base_url") or ""),
            )
            if item is None:
                raise HTTPException(status_code=404, detail=f"Page not found: {page_id}")
            return item
        except Exception as error:
            raise _as_http_exception(error) from error

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

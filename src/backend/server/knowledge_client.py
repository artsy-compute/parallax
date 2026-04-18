from __future__ import annotations

import os
from typing import Any

import httpx

from parallax_utils.file_util import get_project_root


class KnowledgeServiceError(Exception):
    def __init__(self, message: str, *, status_code: int = 500):
        super().__init__(message)
        self.status_code = int(status_code)


class KnowledgeServiceClient:
    def __init__(self) -> None:
        self._workspace_root = str(get_project_root())
        explicit_base_url = str(os.environ.get("PARALLAX_KB_URL", "")).strip()
        if explicit_base_url:
            self.base_url = explicit_base_url.rstrip("/")
        else:
            host = str(os.environ.get("PARALLAX_KB_HOST", "127.0.0.1")).strip() or "127.0.0.1"
            port = max(1, int(os.environ.get("PARALLAX_KB_PORT", "3012")))
            self.base_url = f"http://{host}:{port}"
        self.timeout = max(5.0, float(os.environ.get("PARALLAX_KB_TIMEOUT_SEC", "30")))

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                trust_env=False,
            ) as client:
                response = await client.request(
                    method,
                    f"{self.base_url}{path}",
                    params=params,
                    json=json_body,
                )
        except httpx.RequestError as error:
            raise KnowledgeServiceError(
                f"Knowledge service unavailable at {self.base_url}: {error}",
                status_code=503,
            ) from error

        try:
            payload = response.json()
        except Exception:
            payload = {"detail": response.text or f"HTTP {response.status_code}"}

        if response.status_code >= 400:
            if isinstance(payload, dict):
                message = str(payload.get("detail") or payload.get("error") or payload)
            else:
                message = str(payload)
            raise KnowledgeServiceError(message, status_code=response.status_code)
        return payload

    def _workspace_params(self) -> dict[str, Any]:
        return {"workspace_root": self._workspace_root}

    async def health(self) -> dict[str, Any]:
        return await self._request("GET", "/health", params=self._workspace_params())

    async def list_sources(self, limit: int = 100) -> list[dict[str, Any]]:
        payload = await self._request(
            "GET",
            "/sources",
            params={**self._workspace_params(), "limit": limit},
        )
        return list(payload.get("items") or [])

    async def get_source(self, source_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/sources/{source_id}",
            params=self._workspace_params(),
        )

    async def delete_source(self, source_id: str) -> dict[str, Any]:
        return await self._request(
            "DELETE",
            f"/sources/{source_id}",
            params=self._workspace_params(),
        )

    async def ingest_local_source(self, path: str) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/sources/local",
            json_body={**self._workspace_params(), "path": path},
        )

    async def ingest_url_source(self, url: str) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/sources/url",
            json_body={**self._workspace_params(), "url": url},
        )

    async def ingest_uploaded_source(
        self,
        filename: str,
        data: bytes,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                trust_env=False,
            ) as client:
                response = await client.post(
                    f"{self.base_url}/sources/upload",
                    data=self._workspace_params(),
                    files={
                        "file": (
                            str(filename or "uploaded-document"),
                            data,
                            str(content_type or "application/octet-stream"),
                        )
                    },
                )
        except httpx.RequestError as error:
            raise KnowledgeServiceError(
                f"Knowledge service unavailable at {self.base_url}: {error}",
                status_code=503,
            ) from error

        try:
            payload = response.json()
        except Exception:
            payload = {"detail": response.text or f"HTTP {response.status_code}"}

        if response.status_code >= 400:
            if isinstance(payload, dict):
                message = str(payload.get("detail") or payload.get("error") or payload)
            else:
                message = str(payload)
            raise KnowledgeServiceError(message, status_code=response.status_code)
        return payload

    async def search(self, query: str, limit: int = 10) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/search",
            params={**self._workspace_params(), "q": query, "limit": limit},
        )

    async def get_document(self, document_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/documents/{document_id}",
            params=self._workspace_params(),
        )

    async def list_pages(self) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/pages",
            params=self._workspace_params(),
        )

    async def get_page(self, page_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/pages/{page_id}",
            params=self._workspace_params(),
        )

    async def generate_pages(
        self,
        *,
        advanced: dict[str, Any],
        cluster_model_name: str,
        backend_base_url: str,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/pages/generate",
            json_body={
                **self._workspace_params(),
                "advanced": advanced,
                "cluster_model_name": cluster_model_name,
                "backend_base_url": backend_base_url,
            },
        )

    async def regenerate_page(
        self,
        page_id: str,
        *,
        advanced: dict[str, Any],
        cluster_model_name: str,
        backend_base_url: str,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/pages/{page_id}/generate",
            json_body={
                **self._workspace_params(),
                "advanced": advanced,
                "cluster_model_name": cluster_model_name,
                "backend_base_url": backend_base_url,
            },
        )

    async def list_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        payload = await self._request(
            "GET",
            "/jobs",
            params={**self._workspace_params(), "limit": limit},
        )
        return list(payload.get("items") or [])

    async def get_job(self, job_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/jobs/{job_id}",
            params=self._workspace_params(),
        )

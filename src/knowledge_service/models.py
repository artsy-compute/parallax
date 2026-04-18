from __future__ import annotations

from pydantic import BaseModel, Field


class LocalSourceCreateRequest(BaseModel):
    path: str = Field(min_length=1)
    workspace_root: str | None = None


class UrlSourceCreateRequest(BaseModel):
    url: str = Field(min_length=1)
    workspace_root: str | None = None


class WorkspaceScopedRequest(BaseModel):
    workspace_root: str | None = None


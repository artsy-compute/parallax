> Approved V1 design used for the first implementation slice on `feat/knowledge-base`.

# Knowledge Service Design

## Purpose

This document defines the first implementation slice for adding a knowledge system to Parallax.

The target product shape is:

- an explicit, user-driven knowledge base
- backed by embeddings and retrieval from day one
- running as a separate service/process
- integrated into Parallax through backend APIs and shared UI components
- inspired by the `llmwiki` concept, but starting with RAG before compiled wiki pages

This document is the confirmation checkpoint before code implementation begins.

## Product Direction

Parallax will gain a new `Knowledge` slice that turns the app into:

- a local-first intelligent knowledge base
- a retrieval layer over workspace files and fetched URLs
- a future base for LLM-generated wiki pages with citations

The first slice is intentionally smaller than a full `llmwiki` clone.

Phase 1 is:

- ingest local workspace files
- ingest arbitrary URLs
- embed and index chunked content
- search and inspect results from a dedicated `Knowledge` page
- keep chat usage explicit rather than automatic

Phase 2 and later may add:

- knowledge-aware chat/tool integration
- LLM-generated wiki pages
- source refresh and page regeneration jobs
- graph/relationship views

## Confirmed Decisions

### Service Boundary

- The knowledge system lives under `src/knowledge_service`
- It runs as its own FastAPI service on its own port
- It communicates with Parallax through API calls, not in-process imports

Recommended default environment:

- `PARALLAX_KB_HOST=127.0.0.1`
- `PARALLAX_KB_PORT=3012`

### Repository Layout

- Same repo
- Separate service directory
- Shared frontend components and theme
- Separate runtime process

This preserves:

- fast local iteration
- reuse of existing UI primitives
- a real service boundary for future product separation

### Storage

- User-global storage, not project-local
- Knowledge data root under:
  - `~/.parallax/knowledge`

To avoid collisions between different local workspaces, data should be namespaced by workspace id:

- `~/.parallax/knowledge/<workspace_hash>/...`

The workspace hash should be derived from a normalized workspace root path.

### Retrieval Backend

V1 should not use Qdrant or Postgres.

V1 backend:

- metadata: `SQLite`
- lexical search: `SQLite FTS5`
- vector index: `hnswlib`
- vectors persisted to local files

Rationale:

- zero extra infrastructure
- fits local-first single-user operation
- fastest implementation path
- clean enough to swap later

Qdrant is the likely future upgrade path if a dedicated vector database becomes necessary.

Postgres/pgvector is explicitly out of scope for V1.

### Embeddings

Embeddings are included from day one.

Recommended V1 default:

- local Hugging Face embedding model
- default model:
  - `sentence-transformers/all-MiniLM-L6-v2`

Fallback behavior:

- if the embedding model cannot be loaded, the service may temporarily fall back to the existing deterministic hashing approach already used in Parallax semantic retrieval, but that fallback is not the primary path

### Frontend/API Boundary

The browser should not talk to the KB service directly.

Preferred boundary:

- Browser UI
  -> Parallax backend
  -> Knowledge service

Rationale:

- one client-facing API surface
- cleaner auth and configuration boundary
- simpler frontend integration
- easier future replacement of KB internals

### Knowledge UI Scope

V1 UI is a minimal `Knowledge` page.

Included:

- search box
- source list
- local workspace ingest
- URL ingest
- search results with citations/snippets
- disabled upload-documents mockup

Deferred:

- full wiki page browser
- page editing
- graph view
- automatic KB use in chat

### Chat Behavior

Knowledge usage stays explicit in V1.

That means:

- no automatic KB retrieval in chat
- no silent KB routing
- explicit KB use later through:
  - a knowledge page
  - explicit tools/actions
  - future `search_knowledge` integration

### Authoring Model

For now, knowledge pages remain LLM-generated only.

User editing is deferred.

However, in V1, compiled wiki pages themselves are also deferred until the RAG/search core is working.

## Non-Goals For V1

The following are intentionally out of scope:

- user-editable wiki pages
- uploaded-doc parsing as a real ingest path
- multi-tenant knowledge collections
- browser-to-KB direct calls
- Qdrant/Postgres deployment
- auto-consulting KB from chat
- fully autonomous wiki compilation and refresh loops

## High-Level Architecture

```text
Browser
  -> Parallax frontend
  -> Parallax backend
  -> Knowledge service
      -> SQLite metadata + FTS5
      -> hnswlib vector index
      -> local embedding model
      -> user-global knowledge store
```

## Planned Repository Layout

```text
src/
  backend/
    server/
      knowledge_client.py
  knowledge_service/
    __init__.py
    app.py
    config.py
    models.py
    store.py
    embedding.py
    chunking.py
    search.py
    ingest/
      local_files.py
      urls.py
  frontend/
    src/
      pages/
        knowledge.tsx
      services/
        api.ts
```

This preserves clean ownership:

- `knowledge_service`: internal KB implementation
- `backend`: KB proxy/client integration
- `frontend`: Knowledge page and reused UI shell

## Data Model

### Primary Entities

#### `sources`

Represents an ingest origin.

Fields:

- `id`
- `workspace_id`
- `source_type`
  - `workspace_path`
  - `url`
- `title`
- `canonical_uri`
- `root_path`
- `status`
  - `queued`
  - `ready`
  - `failed`
- `created_at`
- `updated_at`
- `last_error`

#### `documents`

Represents a resolved document extracted from a source.

Fields:

- `id`
- `source_id`
- `workspace_id`
- `document_uri`
- `title`
- `mime_type`
- `sha256`
- `byte_size`
- `text_length`
- `chunk_count`
- `created_at`
- `updated_at`

#### `chunks`

Represents a retrieval unit.

Fields:

- `id`
- `document_id`
- `workspace_id`
- `position`
- `text`
- `token_estimate`
- `char_count`
- `vector_row`
- `created_at`

#### `jobs`

Tracks ingest/index work.

Fields:

- `id`
- `workspace_id`
- `job_type`
  - `ingest_local`
  - `ingest_url`
  - `reindex`
- `status`
  - `queued`
  - `running`
  - `completed`
  - `failed`
- `progress`
- `summary`
- `error`
- `created_at`
- `updated_at`
- `completed_at`

### Deferred Entities

Planned later, not V1:

- `pages`
- `page_links`
- `page_refresh_jobs`

## Storage Layout

Per workspace:

```text
~/.parallax/knowledge/<workspace_hash>/
  metadata.sqlite3
  vectors/
    chunks.hnswlib.bin
    chunks.meta.json
  cache/
    fetched_urls/
  raw/
    normalized_documents/
```

### Why This Layout

- user-global persistence
- no dependency on the current git repo remaining in place
- clean separation between metadata, vector index, and cached artifacts
- future migration path to external vector stores

## Retrieval Design

V1 uses hybrid retrieval:

1. lexical recall from `SQLite FTS5`
2. semantic recall from embedding vectors in `hnswlib`
3. result fusion
4. top-k snippet return with source citations

Recommended fusion:

- Reciprocal Rank Fusion or a simple weighted merge

Returned search result shape:

- source title
- source/document URI
- snippet text
- lexical/semantic scores
- chunk id
- document id

## Chunking Strategy

V1 chunking should be simple and predictable:

- paragraph-aware splitting first
- hard cap by characters/tokens second
- overlap between chunks

Suggested defaults:

- target chunk size: 700-900 characters
- overlap: 120-180 characters

Chunking quality matters more than cleverness in V1.

## Ingest Flows

### Local Workspace Files

Input:

- explicit path from the user

Rules:

- must resolve under approved workspace roots
- skip obviously binary files
- skip giant artifacts/build outputs by default
- respect include/exclude defaults

Initial supported text-like formats:

- `.md`
- `.txt`
- `.py`
- `.ts`
- `.tsx`
- `.js`
- `.jsx`
- `.json`
- `.toml`
- `.yaml`
- `.yml`
- `.rst`
- source files generally treated as text

### Arbitrary URLs

Input:

- explicit URL from the user

Rules:

- only `http` and `https`
- fetch content
- normalize into readable text
- keep source URL as canonical citation target

V1 parser expectations:

- basic HTML/text extraction
- good enough for readable article text
- not a full browser-grade extraction pipeline yet

### Uploaded Docs

V1 UI should show:

- disabled upload button/area
- clear `Coming soon` label

There should be no fake ingest persistence for uploads in V1.

## Internal Knowledge Service API

These are service-local APIs, not necessarily the exact browser-facing endpoints.

### Health

- `GET /health`

### Sources

- `GET /sources`
- `POST /sources/local`
- `POST /sources/url`
- `GET /sources/{source_id}`

### Search

- `GET /search?q=...`

### Documents

- `GET /documents/{document_id}`

### Jobs

- `GET /jobs`
- `GET /jobs/{job_id}`

## Parallax Backend Proxy API

Parallax should expose a stable app-facing surface that proxies the KB service.

Suggested shape:

- `GET /knowledge/health`
- `GET /knowledge/sources`
- `POST /knowledge/sources/local`
- `POST /knowledge/sources/url`
- `GET /knowledge/search`
- `GET /knowledge/documents/{document_id}`
- `GET /knowledge/jobs`

The frontend should only call these Parallax endpoints.

## Frontend Design

### New Page

Add a top-level `Knowledge` page using existing layout primitives.

Suggested route:

- `/#/knowledge`

### Minimal Page Layout

#### Header

- title: `Knowledge`
- short description
- status hint if KB service unavailable

#### Ingest Section

- local path ingest form
- URL ingest form
- disabled upload-doc card/button

#### Search Section

- search input
- result count
- result list with source and snippet

#### Sources Section

- recent sources list
- status chips
- source type chips

### Shared UI Reuse

Use existing:

- `DrawerLayout`
- `Paper`
- `Stack`
- `Button`
- `Chip`
- existing typography/theme tokens

This is a new product slice, but not a separate frontend visual system.

## Dev And Runtime Model

### Runtime

- Parallax app runs as one service
- KB runs as a separate FastAPI service

### Development

Short term:

- separate terminal/process for KB service

Later:

- one top-level dev entrypoint should start both

Desired future developer experience:

- single command to run frontend + backend + KB service together

## Why Not Qdrant Or Postgres In V1

### Why Not Qdrant Yet

Qdrant is strong, but adds:

- another dependency/service
- more operational work
- less local-first simplicity

For this first slice, it slows shipping more than it helps.

### Why Not Postgres Yet

Postgres would force:

- DB setup where none is otherwise required
- vector/FTS tuning work
- more operational overhead than the problem currently justifies

The KB slice does not yet need relational-heavy multi-tenant app infrastructure.

## Future `llmwiki`-Style Extension

Once V1 search and ingest are solid, the next layer is:

- LLM-generated knowledge pages
- page regeneration from sources
- explicit page citations
- source-to-page traceability

That should build on top of this retrieval core rather than replacing it.

Planned future additions:

- `pages` table
- `generate page from source set` jobs
- `refresh page` action
- page detail UI
- `search_knowledge` and `read_knowledge_page` tools

## Implementation Plan

### Step 1

- add `src/knowledge_service`
- add config, storage root, SQLite schema, health endpoint

### Step 2

- implement local embedding model loading
- implement vector store and FTS-backed metadata store

### Step 3

- implement local file ingest
- implement URL ingest
- implement chunking and indexing

### Step 4

- add Parallax backend KB client/proxy endpoints

### Step 5

- add minimal `Knowledge` page in frontend
- add route and nav entry
- add disabled upload mockup

### Step 6

- document explicit future chat/tool integration
- defer automatic use and wiki compilation until after this slice is stable

## Confirmation Checklist

The implementation should proceed only if the following remain true:

- separate KB service under `src/knowledge_service`
- browser calls Parallax backend, not KB directly
- user-global storage under `~/.parallax/knowledge/<workspace_hash>/`
- V1 uses `SQLite + FTS5 + hnswlib`
- embeddings are enabled from day one
- ingest supports workspace files and URLs
- uploads are UI-only mockup for now
- knowledge usage in chat remains explicit
- V1 is RAG-first, not wiki-first

# Custom Model Source Spec

## Status

- Proposed product and implementation spec
- Covers safe custom model source configuration, distribution, and cache policy
- Complements:
  - `docs/settings_page_spec.md`
  - `docs/cluster_and_node_assignment_spec.md`

## Goal

Support custom model sources without exposing arbitrary absolute-path entry in the UI.

The system should:

- make source semantics explicit
- define who validates a source
- define who downloads model bytes
- support cluster-wide use of custom models
- provide future visibility into cached model state on nodes

## Supported Source Types

### 1. Hugging Face

- user provides repo id
- scheduler validates metadata and `config.json`
- nodes download model data directly from Hugging Face

### 2. Approved Local Root

- scheduler starts with one or more approved local model roots
- UI exposes a root dropdown plus relative path entry
- source is scheduler-local, not arbitrary filesystem access
- scheduler syncs required model data to assigned managed nodes

### 3. URL

- future source type
- user provides trusted remote URL
- scheduler validates URL policy and metadata first
- nodes download directly from that URL
- not enabled until runtime fetch/caching policy exists

## Rejected Source Types

- arbitrary absolute local path from the web UI
- arbitrary filesystem browsing outside approved roots
- unsafe URL schemes such as:
  - `file:`
  - `scp:`
  - `ssh:`
  - `javascript:`

## Scheduler Startup Configuration

The scheduler may define approved roots with:

- `--custom-model-root root_id=/absolute/path`
- repeated as needed

Environment alternative:

- `PARALLAX_CUSTOM_MODEL_ROOTS=root_id=/path,other=/path`

If none are provided, the default approved root is:

- `<working-directory>/custom-model-root`

Requirements:

- this directory should be created automatically at startup
- this directory should be gitignored

## UI Model

In `Settings > Custom Models`, source dropdown order should be:

1. `Hugging Face`
2. `Approved local root`
3. `URL` when implemented

### Hugging Face UI

- repo id input
- autocomplete/search
- optional display name

### Approved Local Root UI

- root dropdown
- relative path input
- optional display name

### URL UI

- URL field
- optional display name
- optional checksum later

## Stored Representation

Store structured source definitions, not ambiguous raw local paths.

Examples:

- `{ type: "huggingface", source_value: "Qwen/Qwen3-8B" }`
- `{ type: "scheduler_root", source_value: "shared:qwen/Qwen3-8B" }`
- future:
  - `{ type: "url", source_value: "https://..." }`

For approved local roots:

- store `root_id:relative_path`
- do not store arbitrary absolute path as user input

## Validation

Scheduler validates every custom model before listing it in model selection.

Validation includes:

- read `config.json`
- detect model architecture
- estimate VRAM if possible
- determine status:
  - `verified`
  - `config_only`
  - `invalid`

### Approved Local Root Validation

- resolve path under approved root only
- reject:
  - missing root id
  - missing relative path
  - absolute paths
  - `..` traversal
  - root escape via path resolution

### URL Validation

- allow `https` by default
- optionally allow `http` in local dev later
- scheduler must validate URL policy before accepting it

## Runtime Semantics

### Hugging Face

- treated as a remote source
- nodes download directly

### Approved Local Root

- treated as a scheduler-local source
- scheduler syncs or distributes required model data to assigned managed nodes
- nodes must not assume the same local path exists on them

### URL

- treated as a remote source
- nodes download directly
- only after runtime support exists

## Transport Policy

- remote sources:
  - `Hugging Face`
  - future `URL`
  - nodes download directly
- scheduler-local sources:
  - `Approved local root`
  - scheduler syncs to nodes

## Initial Sync Strategy

Use `rsync` first for approved local roots.

Why:

- incremental transfer
- mature tooling
- simpler than building custom artifact serving immediately
- appropriate for scheduler-to-managed-node sync

Initial behavior:

- on cluster activation, scheduler syncs the selected approved-root model to assigned managed nodes
- on node join, scheduler checks and syncs if needed
- unmanaged/manual nodes are not assumed to support scheduler-initiated sync

## Cache Policy

Nodes keep a model cache.

Do not:

- mirror the entire scheduler root by default
- auto-delete old models immediately on model switch

Do:

- sync only models required by the active cluster
- track cache state separately from runtime state
- add explicit cleanup later

## Cache Inspection

Operators need visibility into cached models on node machines.

Per node, expose:

- cached models
- source type
- local cache path
- size on disk
- last used time
- active/in-use flag
- cache status:
  - `ready`
  - `syncing`
  - `missing`
  - `stale`
  - `unknown`

Managed nodes:

- scheduler may inspect cache via SSH or equivalent management path

Unmanaged nodes:

- cache state should be self-reported if possible
- otherwise show `unknown`

## Cleanup Policy

First version:

- no automatic destructive cleanup
- provide manual action:
  - `Prune unused models`

Later:

- GC policy may consider:
  - active cluster references
  - last-used time
  - pinned models
  - disk pressure thresholds

## Security Rules

- no arbitrary absolute-path input from UI
- approved roots only for scheduler-local sources
- canonicalize and validate all resolved paths
- do not leak sensitive filesystem details in user-facing errors
- URL support must enforce scheme restrictions and source policy
- scheduler is the trust boundary for source validation

## Cluster Interaction

- custom models are shared across clusters
- a cluster selects one model from the shared catalog
- assigned nodes determine where that model must be available
- cache and sync behavior follow the active cluster and assigned-node set

## Phased Delivery

1. `Hugging Face` plus `Approved local root` in UI
2. approved-root validation and structured storage
3. scheduler-to-node sync for approved local roots using `rsync`
4. node cache inspection UI
5. manual prune flow
6. `URL` source after runtime fetch policy exists

## Short Rule

- remote sources: nodes download directly
- scheduler-local sources: scheduler syncs to nodes
- UI selects structured sources, never arbitrary absolute paths

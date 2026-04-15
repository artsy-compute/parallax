# Cluster And Node Assignment Spec

## Status

- Proposed product spec
- Intended to guide the next round of Settings and runtime UX changes
- Supersedes the older assumptions in `docs/settings_page_spec.md` where they conflict

## Purpose

This document defines the intended long-term model for:

- node inventory
- cluster profiles
- node assignment to clusters
- managed vs unmanaged nodes
- capacity validation while configuring a cluster

The goal is to replace the weak mental model of "startup node target only" with a stronger model of "global node inventory plus explicit cluster assignment".

## Core Concepts

### Nodes

- `Nodes` is the global inventory of machines Parallax may use.
- A node may be:
  - local
  - remote
  - managed via SSH
  - manual / unmanaged
  - online or offline
- Node inventory is global, not owned by any single cluster.

### Clusters

- A `Cluster` is a saved configuration profile.
- A cluster selects:
  - model
  - assigned nodes
  - network/runtime settings
  - optional advanced settings
- Multiple cluster profiles may exist in Settings.
- Only one cluster is active at runtime at a time in the current product.

### Custom Models

- `Custom Models` is a shared model library.
- Custom models are not owned by a single cluster.
- Any cluster may select from the shared built-in plus custom model catalog.

## Node Ownership And Usage Policy

- A node that is not assigned to a cluster must not be used for that cluster's LLM shards.
- Only assigned nodes are eligible to serve that cluster.
- If some assigned nodes are offline, the cluster may become degraded or unavailable.
- Parallax must not silently borrow unassigned nodes to satisfy cluster capacity.

This policy is the main reason to move away from a numeric-only startup target as the primary control.

## Node Types

### Managed Node

A managed node has enough information for Parallax to lifecycle-manage it.

Capabilities:

- start
- stop
- restart
- fetch logs
- basic host-level diagnostics

Required fields:

- `ssh_target`
- optional `PARALLAX_PATH`

### Unmanaged Node

An unmanaged node does not have SSH lifecycle control from Parallax.

Capabilities:

- may join the cluster
- may be observed after join
- may serve model shards if assigned to the active cluster

Limitations:

- Parallax cannot start it
- Parallax cannot restart it
- Parallax cannot tail logs through SSH controls
- recovery is manual

Expected operational policy:

- unmanaged remote nodes are expected to stay live on their own
- if they go down, the operator is responsible for bringing them back

## Network Scope

- `Local` vs `Remote` is a property of node/network placement.
- `Managed` vs `Unmanaged` is a property of control capability.
- These are separate dimensions and should not be conflated.

Examples:

- local + managed
- remote + managed
- remote + unmanaged

## Settings Information Architecture

### Nodes

The `Nodes` section is the inventory of available machines.

It should contain:

- all known nodes
- local and remote nodes
- managed and unmanaged nodes
- hardware and status metadata where available
- lifecycle/operations for managed nodes

Each node entry should support:

- display name
- hostname hint
- network scope
- management mode
- linked-cluster summary
- optional hardware summary
- optional notes/tags

Managed node fields:

- `SSH target`
- `PARALLAX_PATH`

Unmanaged node fields:

- no SSH target required
- no SSH-only controls shown
- clear manual-management label

### Node-To-Cluster Visibility

The `Nodes` inventory should show whether a node is already linked to one or more saved clusters.

At minimum, each node row should expose:

- not assigned to any cluster
- assigned to one cluster
- assigned to multiple clusters

Recommended display:

- linked cluster names
- linked cluster count
- a concise overlap indicator when more than one cluster references the node

This is important for planning because a user should be able to see, from the inventory view, whether a node is already part of another cluster definition.

### Cluster

The `Cluster` section is the configuration of one saved cluster profile.

It should contain:

- cluster profile selection / CRUD
- cluster name
- model selection
- assigned nodes
- capacity summary
- network/runtime settings
- join/recovery helpers

### Custom Models

The `Custom Models` section is a shared library.

It should contain:

- built-in plus custom catalog context
- Hugging Face add flow
- local-path add flow
- validation badges and preflight metadata

## Replace Numeric-First Capacity With Assignment-First Capacity

Current UI uses a numeric concept such as:

- `Startup node target`

That concept is not sufficient by itself because it does not answer:

- which nodes are intended for the cluster
- whether those nodes have enough combined VRAM
- whether those nodes are managed or manual
- whether those nodes overlap with other saved clusters

### Recommended Model

Primary control:

- `Assigned nodes`

Derived values:

- assigned node count
- available assigned node count
- total assigned VRAM
- currently available assigned VRAM

Optional secondary control:

- `Use all assigned nodes`
- or `Minimum startup nodes`

But explicit node assignment should be primary.

## Capacity Validation

While editing a cluster, the UI should continuously show whether the assigned nodes are sufficient for the selected model.

Minimum useful summary:

- `Assigned nodes: X`
- `Available now: Y`
- `Total assigned VRAM: N GB`
- `Available VRAM now: M GB`
- `Model requirement: R GB`
- `Capacity status`

Capacity states:

- `Ready`
- `Insufficient`
- `Enough assigned, but not enough online`
- `Unknown` when model metadata is incomplete

If insufficient, show the shortfall:

- `Need about 12 GB more VRAM`

## Cluster Availability Rules

- A selected cluster should remain selected even if it is unavailable.
- Creating a new chat must not silently switch the selected cluster.
- If the selected cluster has no available assigned nodes:
  - keep it selected
  - disable send
  - show a clear recovery/start/switch message

This keeps cluster choice explicit and predictable.

## Runtime Constraints

Current runtime assumption:

- one active scheduler / cluster at a time

Implications:

- multiple saved clusters may reference the same nodes
- node overlap across saved clusters is allowed
- overlap is an explicit user choice, not something Parallax should hide
- only the active cluster actually uses them at runtime
- switching cluster may trigger model reload, weight movement, node rejoin, and temporary downtime

This is not true concurrent multi-cluster serving.

### Overlap Policy

- Double-binding a node across multiple saved clusters is allowed.
- Parallax should not block overlap by default.
- The UI should make overlap visible so the user understands the tradeoff.
- The final decision to keep or remove overlap belongs to the user.

Recommended UX:

- show linked clusters in `Nodes`
- show assigned-node overlap in `Cluster`
- warn when a cluster includes nodes already linked elsewhere, but do not force resolution

## Import And Export

Settings import/export should remain JSON-based and replace the saved configuration instead of merging.

The bundle should include:

- cluster profiles
- active cluster id
- node inventory
- custom models
- advanced runtime settings

Future node assignment data should also be part of the same bundle.

## UI Requirements

### Nodes Section

- Show node type clearly:
  - `Managed`
  - `Manual`
- Show network scope clearly:
  - `Local`
  - `Remote`
- Show cluster linkage clearly:
  - `Unassigned`
  - `Linked to Cluster A`
  - `Linked to 2 clusters`
- For unmanaged nodes, show a concise warning:
  - `Parallax cannot restart this node`

### Cluster Section

- Show assigned nodes as an explicit list or multi-select
- Show capacity summary near the assigned-node editor
- Show model VRAM requirement near the capacity summary
- Show degraded/unavailable state if assigned nodes are offline
- Show when an assigned node is also used by another saved cluster, without blocking the selection

### Chat Top Nav

- Cluster selection remains explicit
- If a cluster is unavailable, it may be greyed or marked degraded
- Switching cluster is a user action, not an automatic fallback

## Migration Guidance

The likely implementation path is:

1. Keep the existing global node inventory in Settings-backed SQLite
2. Add node metadata for:
   - `management_mode`
   - `network_scope`
3. Add per-cluster assigned-node lists
4. Convert capacity UI from numeric-only target to assignment-first
5. Add live capacity validation based on assigned nodes and model VRAM

## Open Questions

- Should unmanaged nodes be creatable before they have ever joined, or only after first discovery?
- Should assignment be a checklist, a searchable multi-select, or a two-pane picker?
- Should the UI preserve a secondary `minimum startup nodes` control once explicit assignment exists?
- Should overlap warnings be soft badges only, or include a confirmation step for large-model clusters?

## Short Product Rule

- `Nodes` defines supply
- `Cluster` defines allowed supply plus workload
- `Custom Models` defines the shared model catalog
- unmanaged remote nodes are usable, but Parallax expects them to remain live on their own

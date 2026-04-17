> AI-generated draft. Verify against the current code before relying on this document for implementation details.

# Architecture

## Purpose

This document describes the current runtime architecture of Parallax as implemented in the repository today. It is intended for contributors who need to understand how the main subsystems fit together before making changes.

The focus here is system structure and request flow, not end-user setup. For installation and operator workflows, see:

- [Installation](./user_guide/install.md)
- [Quick Start](./user_guide/quick_start.md)

## High-Level View

Parallax is a distributed LLM serving system with two distinct backend layers:

- a cluster/control backend that manages cluster state, settings, routing, and the web app
- a node-local inference backend that serves model requests on each participating node

At a high level, the system looks like this:

```text
Browser UI
  -> frontend React app
  -> backend FastAPI app (cluster control plane)
  -> scheduler/routing layer
  -> first node in selected pipeline
  -> node-local HTTP server
  -> local executor process
  -> next node(s) over P2P when the model is sharded
```

## Main Code Areas

### `src/frontend`

The browser UI built with React, Vite, and MUI.

Main responsibilities:

- cluster setup and settings UI
- nodes inventory and status views
- chat interface
- calling the cluster/backend HTTP APIs

Important entrypoints:

- `src/frontend/src/main.tsx`
- `src/frontend/src/App.tsx`
- `src/frontend/src/router/main.tsx`

### `src/backend`

The cluster-facing backend and web application server.

Main responsibilities:

- serve the frontend build
- expose cluster control APIs
- store settings and custom model configuration
- coordinate scheduler lifecycle
- accept chat/completion requests from the frontend
- resolve routes and forward requests to the appropriate node

Important entrypoints:

- `src/backend/main.py`
- `src/backend/server/request_handler.py`
- `src/backend/server/scheduler_manage.py`

This layer is the main control plane of a Parallax cluster.

### `src/scheduling`

The cluster-level scheduling subsystem.

Main responsibilities:

- assign contiguous layer ranges to nodes
- rebalance on joins, leaves, or health changes
- compute request routes across assigned nodes
- model cluster capacity and node performance

Important entrypoints:

- `src/scheduling/scheduler.py`
- `src/scheduling/layer_allocation.py`
- `src/scheduling/request_routing.py`

This package decides which nodes should host which parts of the model and which path a request should take through the cluster.

### `src/parallax`

The node runtime and inference stack.

Main responsibilities:

- node startup and CLI commands
- local model-serving HTTP API
- executor creation and lifecycle
- batching, KV cache management, and request scheduling inside a node
- P2P transport between nodes

Important entrypoints:

- `src/parallax/cli.py`
- `src/parallax/launch.py`
- `src/parallax/server/http_server.py`
- `src/parallax/server/scheduler.py`
- `src/parallax/p2p/server.py`

This layer is the data plane for actual inference.

### `src/parallax_extensions`

Native extensions and lower-level kernels used for performance-sensitive operations.

This area includes:

- C++ bindings
- Metal kernels
- custom attention/cache-related operations

## Two Different Scheduler Concepts

The repository contains two different scheduler layers, and they serve different purposes:

### Cluster scheduler: `src/scheduling`

This scheduler operates across nodes.

It decides:

- which nodes are in the active serving set
- which layer ranges are assigned to each node
- which node path should handle a request

### Node-local scheduler: `src/parallax/server/scheduler.py`

This scheduler operates inside a single node runtime.

It decides:

- which local requests are admitted
- how prefill and decode work are batched
- how local KV cache residency is managed

A useful mental model is:

- `src/scheduling` chooses the path through the cluster
- `src/parallax/server/scheduler.py` chooses the next batch on one node

## Runtime Roles

Parallax commonly runs in three different roles:

### Scheduler node

Usually started via `parallax run`.

Responsibilities:

- starts the cluster/control backend
- hosts the web UI
- creates and owns the cluster scheduler
- accepts cluster-level chat requests
- tracks nodes joining the cluster

Depending on configuration, the scheduler machine may also serve model layers itself.

### Worker node

Usually started via `parallax join`.

Responsibilities:

- joins the cluster through P2P
- receives assigned model layer ranges
- starts the node-local inference runtime
- serves one shard of the model pipeline

### Chat-only client node

Usually started via `parallax chat`.

Responsibilities:

- runs the chat UI/backend without acting as a model-serving worker
- forwards requests to the scheduler

## Process Model

Parallax is not a single-process server. The node runtime uses multiple subprocesses with explicit transport boundaries.

### Cluster/control process

`src/backend/main.py` runs a FastAPI application that:

- serves the frontend
- exposes cluster management APIs
- manages scheduler state
- forwards user requests into the serving cluster

### Node runtime processes

`src/parallax/launch.py` starts the node-serving stack. Depending on node role and model layout, it may start:

- one HTTP server process
- one P2P server process
- one or more executor subprocesses

Shared state and IPC are used between these processes.

### Intra-node communication

Within a node, the HTTP server and executors communicate over ZMQ IPC endpoints.

The node-local HTTP server:

- receives OpenAI-style requests
- tokenizes and tracks streaming state
- sends work to executors over IPC
- streams results back to the caller

### Inter-node communication

Between nodes, Parallax uses the P2P layer built around Lattica.

This layer is responsible for:

- node discovery and addressing
- RPC-style coordination with the scheduler
- forwarding model-state/request payloads between pipeline stages

## Request Flow

This section describes the most common path for a chat request initiated from the web UI.

### 1. Frontend request

The React frontend sends a chat/completion request to the cluster backend.

Relevant code:

- `src/frontend/src/services`
- `src/frontend/src/pages/chat.tsx`

### 2. Backend receives and prepares the request

The cluster backend receives the request in `src/backend/main.py` and delegates request forwarding to `RequestHandler`.

`RequestHandler` is responsible for:

- preparing request metadata
- integrating chat memory/summary behavior
- tracking active requests
- retrying when no route is currently available

Relevant code:

- `src/backend/server/request_handler.py`

### 3. Cluster route resolution

`RequestHandler` asks `SchedulerManage` for a routing table.

`SchedulerManage` bridges:

- the cluster-facing backend
- the cluster scheduler in `src/scheduling`
- the P2P/RPC layer used to reach nodes

Relevant code:

- `src/backend/server/scheduler_manage.py`
- `src/scheduling/scheduler.py`

### 4. Forward to the first node

Once a route is selected, the backend forwards the request to the first hop in the chosen pipeline.

If the model is not sharded, the route may effectively be a single node. If it is sharded, the request enters a multi-hop pipeline.

### 5. Node-local HTTP handling

On the selected node, `src/parallax/server/http_server.py` accepts the request.

This layer is responsible for:

- request bookkeeping
- streaming token assembly
- tokenizer and detokenizer handling
- abort handling
- IPC communication with executor processes

### 6. Local batching and execution

Inside the node runtime, work is passed to the local executor stack.

Main pieces:

- `src/parallax/server/scheduler.py`: local batching and admission
- `src/parallax/server/cache_manager.py`: KV cache tracking
- `src/parallax/server/executor/factory.py`: backend selection
- executor implementations in `src/parallax/server/executor`

Backend selection depends on device/runtime:

- CUDA: `sglang` or `vllm`
- Apple Silicon / Metal: `mlx`

### 7. Inter-node pipeline forwarding

If the node hosts only part of the model, intermediate request state is forwarded to the next node over the P2P transport.

Relevant code:

- `src/parallax/p2p/server.py`
- `src/parallax/p2p/message_util.py`
- `src/parallax/p2p/proto/forward.proto`

This continues until the last stage produces output tokens.

### 8. Response streaming back to the caller

The output is streamed back through the node runtime to the cluster backend and then back to the frontend client.

For streaming requests, the backend preserves the streaming response path rather than waiting for a full final payload.

## Model Execution Backends

Parallax supports different execution engines depending on hardware and configuration.

Selection happens in `src/parallax/server/executor/factory.py`.

### `SGLExecutor`

Used on CUDA systems when `gpu_backend == "sglang"`.

### `VLLMExecutor`

Used on CUDA systems when `gpu_backend == "vllm"`.

### `MLXExecutor`

Used on Apple Silicon / Metal systems.

These executors share a common role:

- load the assigned model shard
- run forward passes for prefill/decode
- cooperate with local scheduling and cache management

## State And Persistence

Several kinds of state exist in the system:

### Cluster settings state

Managed by the backend settings stores in `src/backend/server`.

Examples:

- cluster settings
- managed node inventory
- custom models

### Runtime shared state

Used inside node runtime processes to coordinate:

- server status
- model info
- layer allocation changes
- process lifecycle

Relevant code:

- `src/parallax/utils/shared_state.py`

### Request state

Request lifecycle state is tracked separately at different layers:

- cluster/backend active request tracking in `src/backend/server/request_handler.py`
- node-local HTTP request tracking in `src/parallax/server/http_server.py`
- executor/local request objects in `src/parallax/server/request.py`

## Boundaries And Responsibilities

When changing the codebase, it helps to preserve these boundaries:

- `src/frontend` should remain a UI client, not a source of scheduling logic
- `src/backend` should own cluster coordination and user-facing APIs
- `src/scheduling` should remain focused on cluster-level allocation and routing
- `src/parallax/server` should remain focused on node-local serving, batching, and executor integration
- `src/parallax/p2p` should remain the transport layer between distributed runtime components

Crossing these boundaries is sometimes necessary, but it usually increases coupling.

## Typical Startup Paths

### `parallax run`

Typical effects:

- start cluster/backend server
- initialize scheduler management
- serve the frontend UI
- optionally serve local model layers on the same machine

### `parallax join`

Typical effects:

- start node runtime
- join the cluster through scheduler/P2P mechanisms
- receive layer assignment
- start local serving subprocesses

### `parallax chat`

Typical effects:

- run a chat-focused UI/backend client
- connect to an existing scheduler

## Where To Read Next

For more detail by subsystem:

- cluster scheduler internals: `src/scheduling/README.md`
- operator workflow: `docs/user_guide/quick_start.md`
- cluster/node product model: `docs/cluster_and_node_assignment_spec.md`

For code entry:

- cluster backend: `src/backend/main.py`
- request forwarding: `src/backend/server/request_handler.py`
- scheduler bridge: `src/backend/server/scheduler_manage.py`
- node launcher: `src/parallax/launch.py`
- node HTTP server: `src/parallax/server/http_server.py`
- executor selection: `src/parallax/server/executor/factory.py`

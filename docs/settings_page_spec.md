# Settings Page Spec

> Status: partially implemented and partially outdated.
>
> This document is still useful for the original move into a dedicated Settings page, but it no longer reflects the current direction for cluster profiles, shared custom models, SQLite-backed settings, or future node-to-cluster assignment policy.
>
> Current source-of-truth follow-on spec:
> - [Cluster And Node Assignment Spec](./cluster_and_node_assignment_spec.md)

This document captures the planned UX direction for moving from scattered setup/configuration surfaces to a unified dedicated Settings page.

## Goals

- Replace the current split between `/#/setup` and the Cluster Settings modal with a single full-page configuration workspace.
- Keep `/#/nodes` focused on runtime node operations.
- Keep the `Reconnect your nodes` modal as a contextual recovery flow.
- Hide chat conversation history while the user is in Settings.

## Route Structure

- `/#/settings`
- Optional section anchors:
  - `/#/settings/general`
  - `/#/settings/models`
  - `/#/settings/cluster`
  - `/#/settings/nodes`
  - `/#/settings/chat`
  - `/#/settings/advanced`
  - `/#/settings/about`

## Page Behavior

- Settings is a full-page admin/configuration workspace.
- Chat conversation history/sidebar should not appear while viewing Settings.
- Settings should favor stable, structured navigation over transient modal workflows.

## Information Architecture

### General

- Model selection
- Network mode: local vs remote
- Initial node count
- Current scheduler state summary
- Quick status:
  - scheduler running/stopped
  - selected model
  - joined node count

### Models

- Built-in model selector
- Custom model management
- Hugging Face autocomplete add flow
- Local path add flow
- Validation badges such as `Verified`
- Preflight information:
  - architecture
  - estimated VRAM
  - shardability

### Cluster

- Join command display
- Resolved scheduler address
- Copy reconnect commands
- Topology advisory
- Rebalance action
- Scheduler address warnings:
  - bare peer-id-only join path
  - invalid or unusable resolved address
- Cluster recovery helpers

### Nodes

- Configured node inventory
- SSH targets
- Hostname hints
- `PARALLAX_PATH`
- Add/edit/remove configured hosts
- Link to `/#/nodes` for full runtime operations:
  - start/stop/restart
  - logs
  - health

### Chat

- History retention settings
- Memory/summarization options
- Clear chat history controls
- Conversation storage controls

### Advanced

- Scheduler ports
- Announce addresses
- Host binding
- Relay/network behavior
- Cache/model storage paths
- Log level / diagnostics
- Debug toggles
- Clear labels for restart-required settings

### About

- Version/build info
- Update status
- Documentation links
- Diagnostics/export info
- Environment summary

## What Moves Into Settings

From `/#/setup`:

- Model selection
- Initial node count
- Network mode
- Cluster initialization flow

From the Cluster Settings modal:

- Model section
- Custom models
- Live nodes summary
- Add nodes / join command

## What Stays Outside Settings

- Chat conversation history sidebar
- Full node runtime operations page
- Contextual reconnect modal

## Reconnect Modal

Keep the `Reconnect your nodes` modal.

Purpose:

- Interruption/recovery workflow
- Quick “run this join command and confirm nodes are back” path
- Easy entry point into reconnecting without navigating a full settings surface

It should link back to:

- `Settings > Cluster`
- `/#/nodes`

## UX Guidance

- Use a left settings navigation with category sections.
- Use a main content panel on the right.
- Allow section-specific save/apply actions where needed.
- Distinguish clearly between:
  - applies immediately
  - requires scheduler restart
  - requires node reconnect
- Provide copy buttons for commands and resolved addresses.
- Use status chips for:
  - scheduler running
  - cluster healthy/degraded
  - model verified/custom

## App Structure After This Change

- Main chat route: live conversation UI
- `/#/settings`: unified configuration
- `/#/nodes`: operational node management
- `Reconnect your nodes`: contextual recovery modal

## Summary

The intended split is:

- Settings = configure
- Nodes = operate
- Reconnect modal = recover

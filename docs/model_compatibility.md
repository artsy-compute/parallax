# Model Compatibility

## Purpose

This document explains what it means for a model to be compatible with Parallax, especially for distributed inference.

This is not the same as:

- being available on Hugging Face
- exposing an OpenAI-compatible API
- having a broadly supported `model_type`

For Parallax, compatibility depends on whether the runtime can load, shard, and execute the model architecture used by the selected backend.

## Short Version

A model is suitable for distributed inference in Parallax only when:

1. Parallax can read the model metadata and configuration.
2. The model architecture is supported by the runtime backend.
3. The runtime knows how to shard that architecture into layer ranges.
4. The scheduler can route requests across those layer shards.

If any of those conditions fail, the model should be treated as unsupported for distributed Parallax inference.

## Compatibility Layers

### 1. Source Compatibility

Parallax can import models from sources such as:

- Hugging Face
- approved local roots
- scheduler-local imported archives

This only means Parallax can locate the model files and inspect metadata.

It does not mean the model is runnable.

### 2. Config Compatibility

Parallax validates model metadata from `config.json`.

The most important fields are:

- `model_type`
- `architectures`

These are used during custom model validation in:

- `src/backend/server/custom_models.py`

### 3. Runtime Compatibility

Runtime compatibility means the serving backend can actually construct and execute the model.

For MLX-based distributed inference, this is the most important requirement:

- the model architecture declared in `config.json["architectures"]` must match a Parallax runtime implementation in `src/parallax/models`

If it does not, model loading fails at runtime.

### 4. Distributed Compatibility

Distributed compatibility means the model can participate in Parallax’s pipeline-parallel serving model.

That requires shard-aware loading and execution, not just local inference.

In practice, this means the runtime must know how to split the model into:

- first shard: embeddings and first block range
- middle shard(s): decoder block ranges
- last shard: final block range plus final norm and LM head

Parallax’s distributed inference uses contiguous layer-range sharding plus routing across those shards.

## What Parallax Uses For Distribution

Parallax distributes inference primarily through pipeline parallelism across nodes.

That means:

- each node hosts a contiguous range of decoder layers
- requests are forwarded through nodes in order
- the model must support shard-aware loading

Relevant implementation:

- `src/parallax/server/model.py`
- `src/parallax/server/shard_loader.py`
- `src/scheduling`

## Supported Architectures

For the MLX distributed path in the current codebase, supported architectures are those implemented in:

- `src/parallax/models`

At the time of writing, these include architecture handlers such as:

- `DeepseekV2ForCausalLM`
- `DeepseekV3ForCausalLM`
- `DeepseekV32ForCausalLM`
- `Glm4MoeForCausalLM`
- `GptOssForCausalLM`
- `LlamaForCausalLM`
- `MiniMaxM2ForCausalLM`
- `Qwen2ForCausalLM`
- `Qwen3ForCausalLM`
- `Qwen3MoeForCausalLM`
- `Qwen3NextForCausalLM`
- `Step3p5ForCausalLM`

If a model declares an architecture outside that set, it should be considered unsupported for the current MLX shard-loading path.

## Why `model_type` Alone Is Not Enough

Some models may share a familiar `model_type` but declare a different architecture in `architectures`.

This matters because Parallax runtime support is architecture-specific.

Example:

- a model may report a broadly familiar `model_type`
- but declare `architectures: ["DFlashDraftModel"]`
- if Parallax does not implement `DFlashDraftModel`, that model is not supported for distributed inference

This is why compatibility checks must consider both:

- `model_type`
- `architectures`

## Validation States

Custom model validation uses statuses such as:

- `verified`
- `config_only`
- `invalid`

Recommended interpretation:

### `verified`

Use when:

- model metadata is readable
- architecture is supported by the runtime
- Parallax can reasonably treat the model as shard-compatible

### `config_only`

Use when:

- metadata is readable
- but compatibility cannot be confidently guaranteed

Example:

- `architectures` missing from `config.json`

This status should be treated as experimental, not production-safe.

### `invalid`

Use when:

- `config.json` cannot be read
- `model_type` is unsupported
- the declared architecture is unsupported by the runtime

This status should block distributed use.

## Recommended Product Policy

For distributed Parallax inference, the recommended policy is:

- reject unsupported architectures as early as possible
- do not wait until `parallax join` or runtime model loading to discover incompatibility

In practice:

- a model with unsupported `architectures` should fail validation during import or search
- it should not be presented as shard-compatible
- it should not be selectable for distributed cluster use

## Backend Notes

### MLX

This backend is strict in the current repository because shard loading is implemented through explicit architecture adapters.

If the architecture is unsupported, MLX shard loading fails.

### Other Backends

CUDA backends may have different behavior depending on runtime support in `sglang` or `vllm`, but Parallax still requires compatibility with its serving assumptions.

For product safety, it is best to use the same early validation principle across backends whenever runtime incompatibility is known in advance.

## Practical Checklist

Before allowing a model for distributed inference, verify:

1. `config.json` is present and readable.
2. `model_type` is in the supported set.
3. `architectures` is present.
4. Every declared architecture is implemented by the Parallax runtime.
5. The model is marked `verified`.

If any of the above fail, treat the model as unsupported for distributed Parallax inference.

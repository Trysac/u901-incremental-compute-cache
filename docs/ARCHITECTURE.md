# Architecture

## Overview

`incremental_compute_cache` implements a dynamic, pull-based incremental computation graph (inspired by build systems like Salsa and Adapton) tailored for Python functions using only the standard library.

## Core Components

1. **Canonical Type-Tagged Encoding (`_to_tagged`, `_from_tagged`)**
   - Supports `None`, `bool`, `int`, `float` (encoded via `float.hex()` so `-0.0` and `0.0` remain distinct, plus `NaN` and `+/-Inf`), `str`, `bytes` (base64), `list`, `tuple`, and `dict` with `str` keys.
   - Distinguishes `bool` from `int` (`True != 1`) and `tuple` from `list`.
   - Enforces a maximum nesting depth of 64.
   - Used for both argument fingerprinting and result persistence so `pickle` is never invoked.

2. **Code Fingerprinting (`_fingerprint_function`)**
   - Hashes `co_code`, `co_names`, `co_varnames`, `co_freevars`, `co_cellvars`, argument counts, flags, default arguments, `sys.version_info[:2]`, and `co_consts` (recursively fingerprinting nested code objects).

3. **Call-Graph Verification & Early Cutoff (`Graph`)**
   - A node invocation computes `key = sha256(name, code_fp, arg_fp)`.
   - `contextvars.ContextVar` maintains the active call stack to detect cycles (`code='cycle'`), enforce `max_depth` (`code='depth_exceeded'`), and record child dependencies in the caller's frame.
   - On a cache lookup, if an `Entry` exists for `key`, each recorded dependency `(dep_node, dep_args, dep_result_hash)` is recursively evaluated through the graph in order. If every dependency produces a result with the same SHA-256 hash, the parent entry hits (`early cutoff`) without re-executing the parent function.

4. **LRU Stores (`MemoryStore`, `SqliteStore`)**
   - Both stores maintain a strictly monotonic integer access counter incremented on `put` and `touch`.
   - When `max_entries` is reached, the entry with the lowest access counter is evicted and an `Event(kind='evicted', node=..., key_prefix=...)` is emitted.

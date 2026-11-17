# incremental-compute-cache

Memoised function graph with a persistent cache invalidated by code and input hashes, recomputing only the dependents of what changed.

## Problem

Data scripts and notebooks recompute expensive steps after every edit, or cache them by hand and serve stale results when the code changes. Plain memoisation ignores code changes and dependencies between functions. This library records which cached call used which other calls, fingerprints code and arguments, and recomputes only what a change can affect, with early cutoff when a recomputed value is unchanged.

## Installation

Install via pip:

```bash
pip install incremental-compute-cache
```

## Usage

```python
from incremental_compute_cache import Graph

g = Graph()


@g.node
def b(x: int) -> int:
    return x + 1


@g.node
def a(x: int) -> int:
    return b(x) * 2


assert a(3) == 8
```

## Public API

- `Graph(store=None, *, max_depth=100, max_value_bytes=16_777_216, on_event=None)`
- `MemoryStore(max_entries=10_000)`
- `SqliteStore(path, *, max_entries=100_000)`
- `Store` (Protocol): `get(key)`, `put(key, entry)`, `touch(key)`
- `Event(kind, node, key_prefix)` where `kind` is `'hit' | 'miss' | 'not_stored' | 'evicted'`
- `CacheError(code)` with `code` in `'unhashable_argument' | 'unhashable_value' | 'cycle' | 'depth_exceeded' | 'corrupt_store'`

### Options & Limits

- **Allowed types**: `None`, `bool`, `int`, `float`, `str`, `bytes`, `list`, `tuple`, `dict` (str keys).
- **Serialization**: Type-tagged JSON (no `pickle`).
- **Cycle Detection**: Prevents recursive calls with identical arguments.
- **Depth**: Capped at `max_depth`.
- **Persistence**: `SqliteStore` handles one writer process per file.

## Why not `functools.cache` or `joblib.Memory`?

They ignore dependencies between functions and do not cut off recomputation when an upstream value is unchanged.

## Security

See [docs/SECURITY.md](docs/SECURITY.md).

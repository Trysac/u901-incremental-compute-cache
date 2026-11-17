# Testing

## Test Coverage

- `tests/test_<module>.py` files exist for all source modules.
- `tests/test_graph.py`: Exercises node registration, dependency tracking, call cycles, and re-registration logic.
- `tests/test_stores.py`: Exercises `MemoryStore` and `SqliteStore` LRU eviction and access counter monotonicity.
- `tests/test_encoding.py`: Verifies type-tagged JSON round-tripping for supported types (`None`, `bool`, `int`, `float`, `bytes`, `dict`, `list`, `tuple`).

## Execution

```bash
uv run pytest --cov=incremental_compute_cache
```

## Policy

- Branch coverage must remain at or above 80%.
- Tests are deterministic: no network, no wall clock.

# Security Policy & Threat Model

## Threat Model

### Defended

- **Arbitrary Code Execution**: The store uses canonical type-tagged JSON instead of `pickle`. Malformed files or unknown tags immediately raise `CacheError('corrupt_store')`.
- **Resource Exhaustion**: Call stack depth is capped (default 100), nesting depth for values is capped (64), individual serialized values are limited to `max_value_bytes`, and stores are bounded by `max_entries` with LRU eviction.
- **Cycles**: Cyclic calls are detected via the active call stack (`CacheError('cycle')`).
- **Data Leakage**: `Event` payloads never contain raw arguments or results, only metadata and a 12-char hex key prefix.

### Not Defended

- **Concurrent Writers**: `SqliteStore` assumes one writer process per database file.
- **Global Mutation**: Mutations to external global objects are not tracked; fingerprints rely solely on bytecode and constants.

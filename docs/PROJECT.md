# Project record

## Objective

Memoised function graph with a persistent cache invalidated by code and input hashes, recomputing only the dependents of what changed.

## Scope

- Public API, typed and tested.
- Dynamic dependency tracking via `contextvars`.
- Code fingerprinting (hash of bytecode and recursively fingerprinted constants).
- Early cutoff mechanism.
- Bounded memory and disk stores (LRU).

## Out-of-scope

- Distributed caches.
- Async functions.
- Concurrency across multiple writer processes for `SqliteStore`.
- Tracking mutations to external global objects.

## Effort metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 7768861 / 87256 |
| Hours (human) | pending measurement |
| Cost (USD) | 3.5364 |

## Factory run u901 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-10 | generation | google/gemini-4-argon | yes | 5356 | 16336 | 0 | 0 | 0.3481 |
| 2026-11-10 | fix-1 | google/gemini-3.8-flash | no | 358905 | 1477 | 0 | 800609 | 0.3348 |
| 2026-11-10 | fix-2 | google/gemini-3.8-flash | no | 23564 | 27 | 0 | 12137 | 0.0187 |
| 2026-11-10 | fix-3 | google/gemini-3.8-flash | no | 122876 | 312 | 0 | 73096 | 0.0988 |
| 2026-11-10 | fix-4 | google/gemini-3.8-flash | no | 605353 | 3296 | 0 | 1270138 | 0.5616 |
| 2026-11-10 | generation | google/gemini-4-argon | yes | 5356 | 16942 | 0 | 0 | 0.3603 |
| 2026-11-10 | generation | google/gemini-4-argon | yes | 5356 | 16837 | 0 | 0 | 0.3582 |
| 2026-11-10 | generation | google/gemini-4-argon | yes | 5356 | 18131 | 0 | 0 | 0.3840 |
| 2026-11-10 | fix-1 | google/gemini-3.8-flash | no | 219818 | 1727 | 0 | 173510 | 0.1844 |
| 2026-11-10 | fix-2 | google/gemini-3.8-flash | no | 201226 | 2364 | 0 | 82373 | 0.1660 |
| 2026-11-10 | fix-3 | google/gemini-3.8-flash | no | 105053 | 300 | 0 | 153950 | 0.0915 |
| 2026-11-10 | fix-4 | google/gemini-3.8-flash | no | 69114 | 208 | 0 | 121419 | 0.0617 |
| 2026-11-17 | fix-1 | google/gemini-3.8-flash | no | 220794 | 6306 | 0 | 1509075 | 0.3024 |
| 2026-11-17 | fix-2 | google/gemini-3.8-flash | no | 170175 | 1641 | 0 | 1333713 | 0.2338 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 0 | 0 | 0 | 0 | 0.0000 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 120539 | 1352 | 0 | 0 | 0.0322 |

Total: $3.5364. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).

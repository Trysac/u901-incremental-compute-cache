# Pull-based dependency verification with type-tagged JSON serialization

Status: accepted (generated, pending human review)

## Context

Standard Python memoisation libraries either ignore upstream code changes and cross-function calls (functools.cache) or rely on pickle without fine-grained dependency cutoff (joblib.Memory). We need a persistent incremental computation cache that detects bytecode edits, cuts off downstream recomputation when intermediate outputs are unchanged, and is safe against untrusted cache files.

## Decision

Track dynamic dependency calls using a contextvars stack recording (node, arg_fp, encoded_args, result_hash). Key cache entries by sha256(name, code_fp, arg_fp) and verify stored entries by re-evaluating recorded dependencies in order, cutting off recomputation whenever result hashes match. Encode all arguments and results in a deterministic type-tagged JSON format instead of pickle, and manage store capacity via a monotonic integer access counter.

## Consequences

Zero runtime dependencies outside the Python standard library; safe against code execution from tampered SQLite files; supports dynamic conditional dependencies and early cutoff; closures over mutable external globals and multi-process concurrent writes to the same SQLite file remain out of scope.

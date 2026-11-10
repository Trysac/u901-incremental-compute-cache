# incremental-compute-cache

[![PyPI version](https://img.shields.io/pypi/v/trysac-incremental-compute-cache)](https://pypi.org/project/trysac-incremental-compute-cache/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-incremental-compute-cache)](LICENSE)
[![CI](https://github.com/Trysac/u901-incremental-compute-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u901-incremental-compute-cache/actions/workflows/ci.yml)

Memoised function graph with a persistent cache invalidated by code and input hashes, recomputing only the dependents of what changed

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-incremental-compute-cache
# or
uv add trysac-incremental-compute-cache
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import incremental_compute_cache

# normalize() trims surrounding whitespace and lower-cases the text.
print(incremental_compute_cache.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    incremental_compute_cache.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez

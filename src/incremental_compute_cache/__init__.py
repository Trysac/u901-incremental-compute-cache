"""Memoised function graph with persistent cache invalidated by code and input hashes."""

from __future__ import annotations

import base64
import contextvars
import hashlib
import json
import logging
import math
import os
import sqlite3
import sys
import types
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeVar, overload

__all__ = [
    "CacheError",
    "CacheErrorCode",
    "DepRecord",
    "Entry",
    "Event",
    "EventKind",
    "Graph",
    "MemoryStore",
    "SqliteStore",
    "Store",
    "__version__",
]

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

CacheErrorCode = Literal[
    "unhashable_argument",
    "unhashable_value",
    "cycle",
    "depth_exceeded",
    "corrupt_store",
]

EventKind = Literal["hit", "miss", "not_stored", "evicted"]

_MAX_VALUE_NESTING_DEPTH = 64
_MAX_ERROR_MSG_LEN = 200


class CacheError(Exception):
    """Base error raised by incremental_compute_cache with a machine-readable code."""

    def __init__(self, code: CacheErrorCode, message: str = "") -> None:
        self.code: CacheErrorCode = code
        base_msg = f"[{code}] {message}" if message else f"[{code}]"
        if len(base_msg) > _MAX_ERROR_MSG_LEN:
            base_msg = base_msg[: _MAX_ERROR_MSG_LEN - 3] + "..."
        super().__init__(base_msg)


@dataclass(frozen=True)
class Event:
    """Diagnostic cache lifecycle event emitted by Graph and Stores.

    Payloads never contain raw argument or result values.
    """

    kind: EventKind
    node: str
    key_prefix: str


@dataclass(frozen=True)
class DepRecord:
    """Recorded downstream dependency invocation and the hash of its encoded result."""

    node: str
    arg_fp: str
    args_encoded: bytes
    kwargs_encoded: bytes
    result_hash: str


@dataclass(frozen=True)
class Entry:
    """A cached call result together with its ordered dependency records."""

    node: str
    value_encoded: bytes
    value_hash: str
    deps: tuple[DepRecord, ...]


class Store(Protocol):
    """Protocol for cache backends storing computed node entries."""

    def get(self, key: str) -> Entry | None:
        """Retrieve the cached entry for ``key`` without updating LRU order, or None."""

    def put(self, key: str, entry: Entry) -> None:
        """Insert or replace the cached entry for ``key`` and mark it most recently used."""

    def touch(self, key: str) -> None:
        """Mark ``key`` as most recently used."""


def _encode_float(val: float) -> str:
    if math.isnan(val):
        return "nan"
    if math.isinf(val):
        return "+inf" if val > 0 else "-inf"
    return val.hex()


def _decode_float(raw: str) -> float:
    if raw == "nan":
        return float("nan")
    if raw == "+inf":
        return float("inf")
    if raw == "-inf":
        return float("-inf")
    return float.fromhex(raw)


def _to_tagged(val: Any, depth: int, error_code: CacheErrorCode) -> Any:
    if depth > _MAX_VALUE_NESTING_DEPTH:
        raise CacheError(
            error_code,
            f"Nesting depth exceeds maximum of {_MAX_VALUE_NESTING_DEPTH}",
        )
    t = type(val)
    if val is None:
        return ["n"]
    if t is bool:
        return ["b", val]
    if t is int:
        return ["i", val]
    if t is float:
        return ["f", _encode_float(val)]
    if t is str:
        return ["s", val]
    if t is bytes:
        return ["y", base64.b64encode(val).decode("ascii")]
    if t is list:
        return ["l", [_to_tagged(item, depth + 1, error_code) for item in val]]
    if t is tuple:
        return ["t", [_to_tagged(item, depth + 1, error_code) for item in val]]
    if t is dict:
        pairs: list[list[Any]] = []
        for k in val:
            if type(k) is not str:
                raise CacheError(
                    error_code,
                    f"Dict keys must be exact str, got {type(k).__name__}",
                )
        for k in sorted(val.keys()):
            pairs.append([k, _to_tagged(val[k], depth + 1, error_code)])
        return ["d", pairs]
    raise CacheError(error_code, f"Unsupported value type: {t.__name__}")


def _from_tagged(tagged: Any, depth: int = 0) -> Any:
    if depth > _MAX_VALUE_NESTING_DEPTH:
        raise CacheError("corrupt_store", "Encoded value exceeds maximum nesting depth")
    if not isinstance(tagged, list) or len(tagged) == 0:
        raise CacheError("corrupt_store", "Malformed tagged structure")
    tag = tagged[0]
    if tag == "n":
        if len(tagged) != 1:
            raise CacheError("corrupt_store", "Invalid null tag length")
        return None
    if len(tagged) != 2:
        raise CacheError("corrupt_store", "Invalid tagged tuple length")
    payload = tagged[1]
    if tag == "b":
        if type(payload) is not bool:
            raise CacheError("corrupt_store", "Invalid bool payload")
        return payload
    if tag == "i":
        if type(payload) is not int:
            raise CacheError("corrupt_store", "Invalid int payload")
        return payload
    if tag == "f":
        if type(payload) is not str:
            raise CacheError("corrupt_store", "Invalid float payload")
        try:
            return _decode_float(payload)
        except ValueError as exc:
            raise CacheError("corrupt_store", "Invalid float hex string") from exc
    if tag == "s":
        if type(payload) is not str:
            raise CacheError("corrupt_store", "Invalid str payload")
        return payload
    if tag == "y":
        if type(payload) is not str:
            raise CacheError("corrupt_store", "Invalid bytes payload")
        try:
            return base64.b64decode(payload.encode("ascii"), validate=True)
        except Exception as exc:
            raise CacheError("corrupt_store", "Invalid base64 payload") from exc
    if tag == "l":
        if type(payload) is not list:
            raise CacheError("corrupt_store", "Invalid list payload")
        return [_from_tagged(item, depth + 1) for item in payload]
    if tag == "t":
        if type(payload) is not list:
            raise CacheError("corrupt_store", "Invalid tuple payload")
        return tuple(_from_tagged(item, depth + 1) for item in payload)
    if tag == "d":
        if type(payload) is not list:
            raise CacheError("corrupt_store", "Invalid dict payload")
        result: dict[str, Any] = {}
        for pair in payload:
            if type(pair) is not list or len(pair) != 2 or type(pair[0]) is not str:
                raise CacheError("corrupt_store", "Invalid dict entry in payload")
            result[pair[0]] = _from_tagged(pair[1], depth + 1)
        return result
    raise CacheError("corrupt_store", f"Unknown type tag: {tag!r}")


def _encode_value(val: Any, error_code: CacheErrorCode, initial_depth: int = 0) -> bytes:
    tagged = _to_tagged(val, initial_depth, error_code)
    return json.dumps(tagged, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _decode_value(data: bytes) -> Any:
    try:
        text = data.decode("utf-8")
        tagged = json.loads(text)
    except Exception as exc:
        raise CacheError("corrupt_store", "Invalid JSON in store") from exc
    return _from_tagged(tagged, 0)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fingerprint_const(const: Any, depth: int = 0) -> Any:
    if depth > _MAX_VALUE_NESTING_DEPTH:
        return ["depth_cap"]
    if isinstance(const, types.CodeType):
        return ["code", _fingerprint_code_obj(const, depth + 1)]
    t = type(const)
    if const is None or const is Ellipsis:
        return ["singleton", str(const)]
    if t in (bool, int, float, str, bytes):
        return _to_tagged(const, depth, "unhashable_argument")
    if t is complex:
        return ["complex", _encode_float(const.real), _encode_float(const.imag)]
    if t is tuple:
        return ["t", [_fingerprint_const(c, depth + 1) for c in const]]
    if t is list:
        return ["l", [_fingerprint_const(c, depth + 1) for c in const]]
    if t is frozenset:
        items = [_fingerprint_const(c, depth + 1) for c in const]
        serialized = sorted(json.dumps(i, separators=(",", ":")) for i in items)
        return ["frozenset", serialized]
    return ["type", t.__qualname__]


def _fingerprint_code_obj(code: types.CodeType, depth: int = 0) -> str:
    consts_fp = [_fingerprint_const(c, depth + 1) for c in code.co_consts]
    payload = {
        "co_code": base64.b64encode(code.co_code).decode("ascii"),
        "co_names": list(code.co_names),
        "co_consts": consts_fp,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    version_bytes = f"{sys.version_info[0]}.{sys.version_info[1]}".encode()
    return _sha256_bytes(raw + b":" + version_bytes)


def _fingerprint_function(fn: Callable[..., Any]) -> str:
    code = getattr(fn, "__code__", None)
    if not isinstance(code, types.CodeType):
        raise TypeError("Node target must be a Python function with a __code__ attribute")
    defaults = getattr(fn, "__defaults__", None)
    kwdefaults = getattr(fn, "__kwdefaults__", None)
    combined = {
        "code": _fingerprint_code_obj(code),
        "defaults": _fingerprint_const(defaults) if defaults is not None else None,
        "kwdefaults": (
            [[k, _fingerprint_const(v)] for k, v in sorted(kwdefaults.items())]
            if kwdefaults is not None
            else None
        ),
    }
    raw = json.dumps(combined, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _sha256_bytes(raw)


def _encode_call_arguments(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[str, bytes, bytes]:
    # Verify each individual argument starts at depth 0 up to _MAX_VALUE_NESTING_DEPTH
    tagged_args = [_to_tagged(arg, 0, "unhashable_argument") for arg in args]
    for k in kwargs:
        if type(k) is not str:
            raise CacheError("unhashable_argument", "Keyword argument names must be str")
    tagged_kwargs = [
        [k, _to_tagged(kwargs[k], 0, "unhashable_argument")] for k in sorted(kwargs.keys())
    ]
    args_encoded = json.dumps(["t", tagged_args], separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
    kwargs_encoded = json.dumps(
        ["d", tagged_kwargs], separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    combined = b"args:" + args_encoded + b"|kwargs:" + kwargs_encoded
    return _sha256_bytes(combined), args_encoded, kwargs_encoded


def _compute_node_key(name: str, code_fp: str, arg_fp: str) -> str:
    payload = json.dumps([name, code_fp, arg_fp], separators=(",", ":")).encode("utf-8")
    return _sha256_bytes(payload)


class MemoryStore:
    """In-memory LRU store bounded by ``max_entries`` using a monotonic access counter."""

    def __init__(self, max_entries: int = 10_000) -> None:
        if max_entries < 0:
            raise ValueError("max_entries must be non-negative")
        self._max_entries = max_entries
        self._access_counter: int = 0
        self._entries: OrderedDict[str, tuple[Entry, int]] = OrderedDict()
        self._eviction_Sink: list[Callable[[Event], None]] = []

    def _register_eviction_listener(self, cb: Callable[[Event], None]) -> None:
        if cb not in self._eviction_Sink:
            self._eviction_Sink.append(cb)

    def _emit_evicted(self, key: str, node: str) -> None:
        ev = Event(kind="evicted", node=node, key_prefix=key[:12])
        for cb in list(self._eviction_Sink):
            cb(ev)

    def get(self, key: str) -> Entry | None:
        """Return the stored Entry for ``key`` or None if absent."""
        item = self._entries.get(key)
        if item is None:
            return None
        return item[0]

    def touch(self, key: str) -> None:
        """Update the monotonic access counter for ``key`` and move it to most-recently-used."""
        item = self._entries.get(key)
        if item is not None:
            self._access_counter += 1
            self._entries[key] = (item[0], self._access_counter)
            self._entries.move_to_end(key)

    def put(self, key: str, entry: Entry) -> None:
        """Store ``entry`` under ``key``, evicting the least-recently-used entry if full."""
        if self._max_entries == 0:
            self._emit_evicted(key, entry.node)
            return
        self._access_counter += 1
        if key in self._entries:
            self._entries[key] = (entry, self._access_counter)
            self._entries.move_to_end(key)
            return
        while len(self._entries) >= self._max_entries:
            evicted_key, (evicted_entry, _) = self._entries.popitem(last=False)
            self._emit_evicted(evicted_key, evicted_entry.node)
        self._entries[key] = (entry, self._access_counter)


class SqliteStore:
    """Persistent SQLite LRU store using safe tagged-JSON encoding.

    Keeps entries in a single table ``cache_entries(key TEXT PRIMARY KEY, value BLOB,
    deps TEXT, access INTEGER)`` and evicts by a monotonic access counter.
    """

    def __init__(self, path: str | os.PathLike[str], *, max_entries: int = 100_000) -> None:
        if max_entries < 0:
            raise ValueError("max_entries must be non-negative")
        self._path = os.fspath(path)
        self._max_entries = max_entries
        self._eviction_listeners: list[Callable[[Event], None]] = []
        self._conn = sqlite3.connect(self._path)
        self._init_schema()

    def _init_schema(self) -> None:
        try:
            with self._conn:
                self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS cache_entries ("
                    "key TEXT PRIMARY KEY, "
                    "value BLOB NOT NULL, "
                    "deps TEXT NOT NULL, "
                    "access INTEGER NOT NULL"
                    ")"
                )
            row = self._conn.execute("SELECT MAX(access) FROM cache_entries").fetchone()
            max_acc = row[0] if row is not None and row[0] is not None else 0
            if not isinstance(max_acc, int):
                raise CacheError("corrupt_store", "Non-integer access counter in SQLite store")
            self._access_counter: int = max_acc
        except sqlite3.DatabaseError as exc:
            raise CacheError(
                "corrupt_store", "SQLite database error during initialization"
            ) from exc

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()

    def _register_eviction_listener(self, cb: Callable[[Event], None]) -> None:
        if cb not in self._eviction_listeners:
            self._eviction_listeners.append(cb)

    def _emit_evicted(self, key: str, node: str) -> None:
        ev = Event(kind="evicted", node=node, key_prefix=key[:12])
        for cb in list(self._eviction_listeners):
            cb(ev)

    def _next_access(self) -> int:
        self._access_counter += 1
        return self._access_counter

    def _parse_row(self, value_blob: Any, deps_text: Any) -> Entry:
        if not isinstance(value_blob, (bytes, bytearray, memoryview)):
            raise CacheError("corrupt_store", "Column 'value' is not a BLOB")
        value_bytes = bytes(value_blob)
        # Validate that value_bytes is a valid tagged JSON value
        _decode_value(value_bytes)
        if not isinstance(deps_text, str):
            raise CacheError("corrupt_store", "Column 'deps' is not TEXT")
        try:
            meta = json.loads(deps_text)
        except Exception as exc:
            raise CacheError("corrupt_store", "Column 'deps' is not valid JSON") from exc

        if isinstance(meta, dict):
            node_name = meta.get("node")
            raw_deps = meta.get("deps")
            val_hash = meta.get("value_hash")
        else:
            raise CacheError("corrupt_store", "Column 'deps' root must be a JSON object")

        if type(node_name) is not str or type(raw_deps) is not list or type(val_hash) is not str:
            raise CacheError("corrupt_store", "Malformed metadata in 'deps' column")

        expected_hash = _sha256_bytes(value_bytes)
        if val_hash != expected_hash:
            raise CacheError("corrupt_store", "Value hash mismatch in store row")

        deps_list: list[DepRecord] = []
        for item in raw_deps:
            if type(item) is not dict:
                raise CacheError("corrupt_store", "Dependency item must be an object")
            d_node = item.get("node")
            d_arg_fp = item.get("arg_fp")
            d_args_b64 = item.get("args")
            d_kwargs_b64 = item.get("kwargs")
            d_res_hash = item.get("result_hash")
            if (
                type(d_node) is not str
                or type(d_arg_fp) is not str
                or type(d_args_b64) is not str
                or type(d_kwargs_b64) is not str
                or type(d_res_hash) is not str
            ):
                raise CacheError("corrupt_store", "Malformed dependency fields in store row")
            try:
                args_enc = base64.b64decode(d_args_b64.encode("ascii"), validate=True)
                kwargs_enc = base64.b64decode(d_kwargs_b64.encode("ascii"), validate=True)
            except Exception as exc:
                raise CacheError("corrupt_store", "Invalid base64 in dependency arguments") from exc
            # Validate encoded args and kwargs decode properly
            _decode_value(args_enc)
            _decode_value(kwargs_enc)
            deps_list.append(
                DepRecord(
                    node=d_node,
                    arg_fp=d_arg_fp,
                    args_encoded=args_enc,
                    kwargs_encoded=kwargs_enc,
                    result_hash=d_res_hash,
                )
            )
        return Entry(
            node=node_name,
            value_encoded=value_bytes,
            value_hash=val_hash,
            deps=tuple(deps_list),
        )

    def get(self, key: str) -> Entry | None:
        """Retrieve and validate the entry for ``key``; raise corrupt_store if invalid."""
        try:
            row = self._conn.execute(
                "SELECT value, deps, access FROM cache_entries WHERE key = ?",
                (key,),
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise CacheError("corrupt_store", "SQLite error reading entry") from exc
        if row is None:
            return None
        value_blob, deps_text, access_val = row
        if type(access_val) is not int:
            raise CacheError("corrupt_store", "Column 'access' is not an INTEGER")
        return self._parse_row(value_blob, deps_text)

    def touch(self, key: str) -> None:
        """Increment the monotonic access counter for ``key``."""
        acc = self._next_access()
        try:
            with self._conn:
                self._conn.execute(
                    "UPDATE cache_entries SET access = ? WHERE key = ?",
                    (acc, key),
                )
        except sqlite3.DatabaseError as exc:
            raise CacheError("corrupt_store", "SQLite error updating access counter") from exc

    def put(self, key: str, entry: Entry) -> None:
        """Insert or replace ``entry`` in SQLite, evicting least-recently-used rows if needed."""
        deps_payload = {
            "node": entry.node,
            "value_hash": entry.value_hash,
            "deps": [
                {
                    "node": d.node,
                    "arg_fp": d.arg_fp,
                    "args": base64.b64encode(d.args_encoded).decode("ascii"),
                    "kwargs": base64.b64encode(d.kwargs_encoded).decode("ascii"),
                    "result_hash": d.result_hash,
                }
                for d in entry.deps
            ],
        }
        deps_json = json.dumps(deps_payload, separators=(",", ":"), ensure_ascii=True)
        acc = self._next_access()
        if self._max_entries == 0:
            self._emit_evicted(key, entry.node)
            return
        evicted: list[tuple[str, str]] = []
        try:
            with self._conn:
                exists = self._conn.execute(
                    "SELECT 1 FROM cache_entries WHERE key = ?", (key,)
                ).fetchone()
                if exists is None:
                    count_row = self._conn.execute("SELECT COUNT(*) FROM cache_entries").fetchone()
                    current_count = count_row[0] if count_row is not None else 0
                    to_evict = (current_count + 1) - self._max_entries
                    if to_evict > 0:
                        evict_sql = (
                            "SELECT key, value, deps FROM cache_entries ORDER BY access ASC LIMIT ?"
                        )
                        rows = self._conn.execute(evict_sql, (to_evict,)).fetchall()
                        for ev_key, ev_val, ev_deps in rows:
                            ev_node = "unknown"
                            try:
                                parsed = self._parse_row(ev_val, ev_deps)
                                ev_node = parsed.node
                            except CacheError:
                                pass
                            self._conn.execute("DELETE FROM cache_entries WHERE key = ?", (ev_key,))
                            evicted.append((str(ev_key), ev_node))
                upsert_sql = (
                    "INSERT OR REPLACE INTO cache_entries "
                    "(key, value, deps, access) VALUES (?, ?, ?, ?)"
                )
                self._conn.execute(
                    upsert_sql,
                    (key, entry.value_encoded, deps_json, acc),
                )
        except sqlite3.DatabaseError as exc:
            raise CacheError("corrupt_store", "SQLite error writing entry") from exc
        for ev_key, ev_node in evicted:
            self._emit_evicted(ev_key, ev_node)


@dataclass
class _ActiveFrame:
    graph_id: int
    node: str
    arg_fp: str
    deps: list[DepRecord]


_call_stack_var: contextvars.ContextVar[tuple[_ActiveFrame, ...]] = contextvars.ContextVar(
    "incremental_compute_cache_call_stack",
    default=(),
)

F = TypeVar("F", bound=Callable[..., Any])


@dataclass
class _NodeRegistration:
    name: str
    fn: Callable[..., Any]
    code_fp: str


class Graph:
    """Incremental computation graph that memoises node calls by code and argument fingerprints."""

    def __init__(
        self,
        store: Store | None = None,
        *,
        max_depth: int = 100,
        max_value_bytes: int = 16_777_216,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        if max_depth < 1:
            raise ValueError("max_depth must be at least 1")
        if max_value_bytes < 0:
            raise ValueError("max_value_bytes must be non-negative")
        self._store: Store = store if store is not None else MemoryStore()
        self._max_depth = max_depth
        self._max_value_bytes = max_value_bytes
        self._on_event = on_event
        self._nodes: dict[str, _NodeRegistration] = {}
        self._wrappers: dict[str, Callable[..., Any]] = {}
        if isinstance(self._store, (MemoryStore, SqliteStore)):
            self._store._register_eviction_listener(self._forward_event)

    def _forward_event(self, event: Event) -> None:
        if self._on_event is not None:
            self._on_event(event)

    def _emit(self, kind: EventKind, node: str, key: str) -> None:
        ev = Event(kind=kind, node=node, key_prefix=key[:12])
        logger.debug("cache event: kind=%s node=%s key_prefix=%s", ev.kind, ev.node, ev.key_prefix)
        if self._on_event is not None:
            self._on_event(ev)

    @overload
    def node(self, fn: F, *, name: str | None = None) -> F: ...

    @overload
    def node(self, fn: None = None, *, name: str | None = None) -> Callable[[F], F]: ...

    def node(
        self,
        fn: F | None = None,
        *,
        name: str | None = None,
    ) -> F | Callable[[F], F]:
        """Register a function as a node in the incremental compute graph.

        Can be used as ``@g.node`` or ``@g.node(name='custom')``. Re-registering an
        existing name updates the target implementation in place so all previously
        created wrapper references invoke the latest registration.
        """

        def decorator(target: F) -> F:
            node_name = name if name is not None else target.__name__
            code_fp = _fingerprint_function(target)
            self._nodes[node_name] = _NodeRegistration(
                name=node_name,
                fn=target,
                code_fp=code_fp,
            )

            # Look up or create the single wrapper for node_name
            existing_wrapper = self._wrappers.get(node_name)
            if existing_wrapper is not None:
                wrapper = existing_wrapper
            else:

                def wrapper(*args: Any, **kwargs: Any) -> Any:
                    return self._invoke_by_name(node_name, args, kwargs)

                self._wrappers[node_name] = wrapper

            wrapper.__name__ = getattr(target, "__name__", node_name)
            wrapper.__qualname__ = getattr(target, "__qualname__", node_name)
            wrapper.__doc__ = getattr(target, "__doc__", None)
            return wrapper  # type: ignore[return-value]

        if fn is not None:
            return decorator(fn)
        return decorator

    def _invoke_by_name(
        self,
        node_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        val, _, _, _ = self._evaluate_node(node_name, args, kwargs)
        return val

    def _evaluate_node(
        self,
        node_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> tuple[Any, bytes, str, str]:
        reg = self._nodes.get(node_name)
        if reg is None:
            raise KeyError(f"Unregistered node: {node_name!r}")

        arg_fp, args_encoded, kwargs_encoded = _encode_call_arguments(args, kwargs)
        stack = _call_stack_var.get()
        my_graph_id = id(self)

        # Check for cycle on the current graph's call stack
        for frame in stack:
            if frame.graph_id == my_graph_id and frame.node == node_name and frame.arg_fp == arg_fp:
                raise CacheError("cycle", f"Cycle detected calling node {node_name!r}")

        # Count depth on the current graph
        current_depth = sum(1 for frame in stack if frame.graph_id == my_graph_id) + 1
        if current_depth > self._max_depth:
            raise CacheError(
                "depth_exceeded",
                f"Call stack depth {current_depth} exceeds max_depth {self._max_depth}",
            )

        key = _compute_node_key(node_name, reg.code_fp, arg_fp)
        frame = _ActiveFrame(graph_id=my_graph_id, node=node_name, arg_fp=arg_fp, deps=[])
        token = _call_stack_var.set((*stack, frame))
        try:
            cached_entry = self._store.get(key)
            if cached_entry is not None and self._verify_dependencies(cached_entry):
                self._store.touch(key)
                self._emit("hit", node_name, key)
                val = _decode_value(cached_entry.value_encoded)
                val_encoded = cached_entry.value_encoded
                val_hash = cached_entry.value_hash
            else:
                self._emit("miss", node_name, key)
                frame.deps.clear()
                # Execute the current registration of the node
                current_reg = self._nodes[node_name]
                val = current_reg.fn(*args, **kwargs)
                val_encoded = _encode_value(val, "unhashable_value", 0)
                val_hash = _sha256_bytes(val_encoded)
                if len(val_encoded) > self._max_value_bytes:
                    self._emit("not_stored", node_name, key)
                else:
                    new_entry = Entry(
                        node=node_name,
                        value_encoded=val_encoded,
                        value_hash=val_hash,
                        deps=tuple(frame.deps),
                    )
                    self._store.put(key, new_entry)
        finally:
            _call_stack_var.reset(token)

        # Record dependency in parent frame if called within the same Graph
        if stack and stack[-1].graph_id == my_graph_id:
            stack[-1].deps.append(
                DepRecord(
                    node=node_name,
                    arg_fp=arg_fp,
                    args_encoded=args_encoded,
                    kwargs_encoded=kwargs_encoded,
                    result_hash=val_hash,
                )
            )

        return val, val_encoded, val_hash, arg_fp

    def _verify_dependencies(self, entry: Entry) -> bool:
        for dep in entry.deps:
            if dep.node not in self._nodes:
                return False
            dep_args: tuple[Any, ...] = _decode_value(dep.args_encoded)
            dep_kwargs: dict[str, Any] = _decode_value(dep.kwargs_encoded)
            _, _, actual_hash, _ = self._evaluate_node(dep.node, dep_args, dep_kwargs)
            if actual_hash != dep.result_hash:
                return False
        return True

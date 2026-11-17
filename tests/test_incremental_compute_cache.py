"""Comprehensive test suite for incremental_compute_cache."""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from incremental_compute_cache import (
    CacheError,
    Entry,
    Event,
    Graph,
    MemoryStore,
    SqliteStore,
)


def test_basic_dag_and_re_registration_and_early_cutoff() -> None:
    """Acceptance 1, 2, 3: DAG memoisation, selective recomputation on code change, early cutoff."""
    g = Graph()
    counts = {"a": 0, "b": 0, "c": 0}

    @g.node
    def b(x: int) -> int:
        counts["b"] += 1
        return x + 1

    @g.node
    def c(x: int) -> int:
        counts["c"] += 1
        return x * 2

    @g.node
    def a(x: int) -> int:
        counts["a"] += 1
        return b(x) + c(x)

    assert a(3) == 10
    assert counts == {"a": 1, "b": 1, "c": 1}

    # Second call returns 10 with no new calls
    assert a(3) == 10
    assert counts == {"a": 1, "b": 1, "c": 1}

    # Acceptance 2: Re-registering c under name='c' with body x * 3 makes a(3) == 13
    # with counts c=2, a=2 and b still 1 (only the dependents of c are recomputed)
    @g.node(name="c")
    def c_v2(x: int) -> int:
        counts["c"] += 1
        return x * 3

    assert a(3) == 13
    assert counts == {"a": 2, "b": 1, "c": 2}
    # Old wrapper reference also dispatches to the new registration
    assert c(3) == 9
    assert counts == {"a": 2, "b": 1, "c": 2}

    # Acceptance 3: Early cutoff: re-registering b under name='b' with body 1 + x
    # makes a(3) recompute b (b=2) but not a (a stays 2 from previous step; relative to
    # fresh state it stays 1) because b(3) still returns 4.
    g2 = Graph()
    counts2 = {"a": 0, "b": 0, "c": 0}

    @g2.node(name="b")
    def b1(x: int) -> int:
        counts2["b"] += 1
        return x + 1

    @g2.node(name="c")
    def c1(x: int) -> int:
        counts2["c"] += 1
        return x * 2

    @g2.node(name="a")
    def a1(x: int) -> int:
        counts2["a"] += 1
        return b1(x) + c1(x)

    assert a1(3) == 10
    assert counts2 == {"a": 1, "b": 1, "c": 1}

    @g2.node(name="b")
    def b2(x: int) -> int:
        counts2["b"] += 1
        return 1 + x

    assert a1(3) == 10
    assert counts2 == {"a": 1, "b": 2, "c": 1}


def test_sqlite_store_persistence_across_graphs(tmp_path: Path) -> None:
    """Acceptance 4: SqliteStore persists cached results across distinct Graph instances."""
    db_path = tmp_path / "cache.db"
    store1 = SqliteStore(db_path)
    g1 = Graph(store=store1)
    counts = {"a": 0, "b": 0, "c": 0}

    @g1.node(name="b")
    def b1(x: int) -> int:
        counts["b"] += 1
        return x + 1

    @g1.node(name="c")
    def c1(x: int) -> int:
        counts["c"] += 1
        return x * 2

    @g1.node(name="a")
    def a1(x: int) -> int:
        counts["a"] += 1
        return b1(x) + c1(x)

    assert a1(3) == 10
    assert counts == {"a": 1, "b": 1, "c": 1}
    store1.close()

    store2 = SqliteStore(db_path)
    g2 = Graph(store=store2)

    @g2.node(name="b")
    def b2(x: int) -> int:
        counts["b"] += 1
        return x + 1

    @g2.node(name="c")
    def c2(x: int) -> int:
        counts["c"] += 1
        return x * 2

    @g2.node(name="a")
    def a2(x: int) -> int:
        counts["a"] += 1
        return b2(x) + c2(x)

    assert a2(3) == 10
    assert counts == {"a": 1, "b": 1, "c": 1}
    store2.close()


def test_errors_unhashable_cycle_and_depth() -> None:
    """Acceptance 6: unhashable_argument, cycle, and depth_exceeded."""
    g = Graph(max_depth=100)

    @g.node
    def a(x: Any) -> Any:
        return x

    with pytest.raises(CacheError) as exc_info:
        a(object())
    assert exc_info.value.code == "unhashable_argument"

    @g.node
    def unhashable_ret(x: int) -> Any:
        return object()

    with pytest.raises(CacheError) as exc_info:
        unhashable_ret(1)
    assert exc_info.value.code == "unhashable_value"

    @g.node
    def self_cycle(x: int) -> int:
        return self_cycle(x)

    with pytest.raises(CacheError) as exc_info:
        self_cycle(5)
    assert exc_info.value.code == "cycle"

    @g.node
    def countdown(n: int) -> int:
        if n <= 0:
            return 0
        return countdown(n - 1)

    assert countdown(99) == 0
    with pytest.raises(CacheError) as exc_info:
        countdown(100)
    assert exc_info.value.code == "depth_exceeded"


def test_max_value_bytes_and_eviction_events() -> None:
    """Acceptance 7: max_value_bytes emits not_stored; MemoryStore(max_entries=2) emits evicted."""
    events: list[Event] = []
    g = Graph(max_value_bytes=100, on_event=events.append)
    runs = 0

    @g.node
    def big(n: int) -> str:
        nonlocal runs
        runs += 1
        return "x" * 200

    assert big(1) == "x" * 200
    assert runs == 1
    assert any(e.kind == "not_stored" and e.node == "big" for e in events)

    # Runs again on the next call because it was not stored
    assert big(1) == "x" * 200
    assert runs == 2

    # Eviction with MemoryStore(max_entries=2)
    ev_events: list[Event] = []
    store = MemoryStore(max_entries=2)
    g2 = Graph(store=store, on_event=ev_events.append)
    call_log: list[int] = []

    @g2.node
    def echo(x: int) -> int:
        call_log.append(x)
        return x + 10

    assert echo(1) == 11
    assert echo(2) == 12
    # Touch echo(1) so echo(2) becomes the least recently used
    assert echo(1) == 11
    assert call_log == [1, 2]

    # Third distinct call evicts echo(2)
    assert echo(3) == 13
    evicted = [e for e in ev_events if e.kind == "evicted"]
    assert len(evicted) == 1
    assert evicted[0].node == "echo"
    assert len(evicted[0].key_prefix) == 12

    # echo(1) is still cached; echo(2) was evicted and recomputes
    assert echo(1) == 11
    assert call_log == [1, 2, 3]
    assert echo(2) == 12
    assert call_log == [1, 2, 3, 2]


def test_exceptions_not_cached_and_event_privacy() -> None:
    """Acceptance 8: Exceptions propagate without caching; Events never leak values."""
    events: list[Event] = []
    g = Graph(on_event=events.append)
    attempts = 0

    @g.node
    def flaky(secret: str) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("transient failure")
        return f"result:{secret}"

    with pytest.raises(ValueError, match="transient failure"):
        flaky("SENSITIVE_ARG_VALUE")
    assert attempts == 1

    # Second call runs again and succeeds
    assert flaky("SENSITIVE_ARG_VALUE") == "result:SENSITIVE_ARG_VALUE"
    assert attempts == 2

    # Third call hits cache
    assert flaky("SENSITIVE_ARG_VALUE") == "result:SENSITIVE_ARG_VALUE"
    assert attempts == 2

    for ev in events:
        assert "SENSITIVE_ARG_VALUE" not in repr(ev)
        assert len(ev.key_prefix) == 12
        int(ev.key_prefix, 16)  # valid hex


def test_edge_cases_types_and_fingerprints() -> None:
    """Edge cases: kwargs order, -0.0 vs 0.0, NaN, bool vs int, tuple vs list, dependency removal."""
    g = Graph()
    calls = 0

    @g.node
    def fn(a: Any, b: Any = None, **kw: Any) -> Any:
        nonlocal calls
        calls += 1
        return (a, b, kw)

    # kwargs order does not affect fingerprint
    res1 = fn(1, x=10, y=20)
    res2 = fn(1, y=20, x=10)
    assert res1 == res2
    assert calls == 1

    # -0.0 vs 0.0 differ
    fn(0.0)
    assert calls == 2
    fn(-0.0)
    assert calls == 3
    fn(-0.0)
    assert calls == 3

    # NaN is allowed and roundtrips
    nan_res = fn(float("nan"))[0]
    assert math.isnan(nan_res)
    assert calls == 4
    nan_res2 = fn(float("nan"))[0]
    assert math.isnan(nan_res2)
    assert calls == 4

    # Infinity roundtrips
    assert fn(float("inf"))[0] == float("inf")
    assert fn(float("-inf"))[0] == float("-inf")

    # bool and int are distinct (True != 1 in keys)
    calls_bi = 0

    @g.node
    def bi(val: Any) -> Any:
        nonlocal calls_bi
        calls_bi += 1
        return [val, (True, 1), {"k": b"bytes"}, None]

    r_true = bi(True)
    r_one = bi(1)
    assert calls_bi == 2
    assert r_true[0] is True and type(r_true[0]) is bool
    assert r_one[0] == 1 and type(r_one[0]) is int

    # Cached retrieval preserves exact types (tuple vs list, bool vs int, bytes)
    r_true_cached = bi(True)
    assert calls_bi == 2
    assert type(r_true_cached[0]) is bool
    assert type(r_true_cached[1]) is tuple
    assert type(r_true_cached[2]["k"]) is bytes

    # tuple vs list arguments are distinct
    bi([1, 2])
    assert calls_bi == 3
    bi((1, 2))
    assert calls_bi == 4

    # Same node called with two arguments inside a parent node
    leaf_calls = 0

    @g.node
    def leaf(x: int) -> int:
        nonlocal leaf_calls
        leaf_calls += 1
        return x * 10

    @g.node
    def pair_caller(x: int) -> int:
        return leaf(x) + leaf(x + 1)

    assert pair_caller(2) == 50
    assert leaf_calls == 2
    assert pair_caller(2) == 50
    assert leaf_calls == 2

    # Dependency removed by a code change
    @g.node(name="pair_caller")
    def pair_caller_v2(x: int) -> int:
        return x * 100

    assert pair_caller(2) == 200

    # Now changing leaf does not cause pair_caller_v2 to recompute
    p_calls = 0

    @g.node(name="pair_caller")
    def pair_caller_v3(x: int) -> int:
        nonlocal p_calls
        p_calls += 1
        return x * 100

    assert pair_caller(2) == 200
    assert p_calls == 1

    @g.node(name="leaf")
    def leaf_v2(x: int) -> int:
        return x * 999

    assert pair_caller(2) == 200
    assert p_calls == 1


def test_nesting_depth_and_invalid_dict_keys() -> None:
    """Nesting depth limit of 64 and dict non-str key rejection."""
    g = Graph()

    @g.node
    def identity(x: Any) -> Any:
        return x

    # Dict with non-str key in argument
    with pytest.raises(CacheError) as exc_info:
        identity({1: "a"})
    assert exc_info.value.code == "unhashable_argument"

    # Build structure of depth 64 (allowed) and 65 (rejected)
    val_64: Any = "leaf"
    for _ in range(64):
        val_64 = [val_64]
    assert identity(val_64) == val_64

    val_65: Any = [val_64]
    with pytest.raises(CacheError) as exc_info:
        identity(val_65)
    assert exc_info.value.code == "unhashable_argument"

    @g.node
    def make_deep_result(n: int) -> Any:
        res: Any = 0
        for _ in range(n):
            res = [res]
        return res

    assert make_deep_result(64) is not None
    with pytest.raises(CacheError) as exc_info:
        make_deep_result(65)
    assert exc_info.value.code == "unhashable_value"


def test_sqlite_eviction_and_corruption(tmp_path: Path) -> None:
    """SqliteStore LRU eviction and corrupt_store detection."""
    db_path = tmp_path / "evict.db"
    events: list[Event] = []
    store = SqliteStore(db_path, max_entries=2)
    g = Graph(store=store, on_event=events.append)

    @g.node
    def sq(x: int) -> int:
        return x * x

    assert sq(1) == 1
    assert sq(2) == 4
    assert sq(1) == 1  # touches sq(1) so sq(2) is LRU
    assert sq(3) == 9  # evicts sq(2)
    assert any(e.kind == "evicted" and e.node == "sq" for e in events)

    # Corrupt the deps column in SQLite
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute("UPDATE cache_entries SET deps = 'not-json'")
    conn.close()

    with pytest.raises(CacheError) as exc_info:
        sq(1)
    assert exc_info.value.code == "corrupt_store"
    store.close()

    # Corrupt value blob hash mismatch
    db_path2 = tmp_path / "corrupt_val.db"
    store2 = SqliteStore(db_path2)
    g2 = Graph(store=store2)

    @g2.node
    def f(x: int) -> int:
        return x + 1

    assert f(10) == 11
    conn2 = sqlite3.connect(db_path2)
    with conn2:
        conn2.execute("UPDATE cache_entries SET value = ?", (b'["i",999]',))
    conn2.close()

    with pytest.raises(CacheError) as exc_info:
        f(10)
    assert exc_info.value.code == "corrupt_store"
    store2.close()


def test_nested_functions_and_constants_fingerprinting() -> None:
    """Nested functions, lambdas, default arguments, complex/frozenset constants."""
    g = Graph()
    calls = 0

    @g.node(name="compute")
    def compute_v1(x: int, offset: int = 5, *, scale: int = 2) -> int:
        nonlocal calls
        calls += 1
        helper = lambda y: y + 1  # noqa: E731
        _ = (1 + 2j, frozenset({1, 2}), ...)
        return helper(x) * scale + offset

    assert compute_v1(3) == 13
    assert calls == 1
    assert compute_v1(3) == 13
    assert calls == 1

    # Changing inner lambda or default value changes fingerprint
    @g.node(name="compute")
    def compute_v2(x: int, offset: int = 6, *, scale: int = 2) -> int:
        nonlocal calls
        calls += 1
        helper = lambda y: y + 1  # noqa: E731
        return helper(x) * scale + offset

    assert compute_v2(3) == 14
    assert calls == 2

    # Changing only a default value (identical body) invalidates the cache
    @g.node(name="compute")
    def compute_v3(x: int, offset: int = 7, *, scale: int = 2) -> int:
        nonlocal calls
        calls += 1
        helper = lambda y: y + 1  # noqa: E731
        return helper(x) * scale + offset

    assert compute_v3(3) == 15
    assert calls == 3

    # Changing only a keyword-only default value invalidates the cache
    @g.node(name="compute")
    def compute_v4(x: int, offset: int = 7, *, scale: int = 3) -> int:
        nonlocal calls
        calls += 1
        helper = lambda y: y + 1  # noqa: E731
        return helper(x) * scale + offset

    assert compute_v4(3) == 19
    assert calls == 4


def test_store_edge_validations(tmp_path: Path) -> None:
    """Validation of constructor parameters and zero-capacity stores."""
    with pytest.raises(ValueError):
        MemoryStore(max_entries=-1)
    with pytest.raises(ValueError):
        SqliteStore(tmp_path / "neg.db", max_entries=-1)
    with pytest.raises(ValueError):
        Graph(max_depth=0)
    with pytest.raises(ValueError):
        Graph(max_value_bytes=-1)

    # max_entries=0 immediately evicts on put
    events: list[Event] = []
    m0 = MemoryStore(max_entries=0)
    s0 = SqliteStore(tmp_path / "zero.db", max_entries=0)
    for st_backend in (m0, s0):
        g = Graph(store=st_backend, on_event=events.append)

        @g.node
        def ident(x: int) -> int:
            return x

        assert ident(1) == 1
    assert sum(1 for e in events if e.kind == "evicted") == 2
    s0.close()

    # Updating an existing key in MemoryStore
    m_upd = MemoryStore(max_entries=5)
    dummy_entry = Entry(node="n", value_encoded=b'["i",1]', value_hash="h", deps=())
    m_upd.put("k1", dummy_entry)
    m_upd.put("k1", dummy_entry)
    assert m_upd.get("k1") == dummy_entry


def _make_dag_node(
    idx: int,
    c0: int,
    c1: int,
    d1: int,
    d2: int,
    op_kind: int,
    node_refs: dict[int, Any],
) -> Any:
    # We create distinct bytecode per (c0, c1, d1, d2, op_kind) by compiling a function
    # so its __code__ reflects the exact operation and constants.
    if idx == 0:
        src = f"def node_0(x):\n    return (x * {c0}) + {c1}\n"
    elif op_kind == 0:
        src = f"def node_{idx}(x):\n    return n_{d1}(x) + {c0}\n"
    elif op_kind == 1:
        src = f"def node_{idx}(x):\n    return n_{d1}(x) + n_{d2}(x) + {c1}\n"
    else:
        # Conditional branch where only one dependency is called depending on x
        src = (
            f"def node_{idx}(x):\n"
            f"    return (n_{d1}(x) + {c0}) if (x % 2 == 0) else (n_{d2}(x) - {c1})\n"
        )
    ns: dict[str, Any] = {f"n_{k}": v for k, v in node_refs.items()}
    exec(src, ns)  # noqa: S102
    return ns[f"node_{idx}"]


@settings(max_examples=200, derandomize=True)
@given(
    num_nodes=st.integers(min_value=1, max_value=8),
    initial_specs=st.lists(
        st.tuples(
            st.integers(min_value=-5, max_value=5),
            st.integers(min_value=-5, max_value=5),
            st.integers(min_value=0, max_value=7),
            st.integers(min_value=0, max_value=7),
            st.integers(min_value=0, max_value=2),
        ),
        min_size=8,
        max_size=8,
    ),
    actions=st.lists(
        st.tuples(
            st.booleans(),  # True = call node, False = mutate node code
            st.integers(min_value=0, max_value=7),  # target node index
            st.integers(min_value=-4, max_value=4),  # arg x or new c0
            st.integers(min_value=-4, max_value=4),  # new c1
            st.integers(min_value=0, max_value=7),  # new d1
            st.integers(min_value=0, max_value=7),  # new d2
            st.integers(min_value=0, max_value=2),  # new op_kind
        ),
        min_size=1,
        max_size=20,
    ),
)
def test_property_random_dag_matches_fresh_graph(
    num_nodes: int,
    initial_specs: list[tuple[int, int, int, int, int]],
    actions: list[tuple[bool, int, int, int, int, int, int]],
) -> None:
    """Acceptance 5: Property (hypothesis) verifying cached DAG results match a fresh Graph."""
    cached_graph = Graph()
    current_specs: list[tuple[int, int, int, int, int]] = []
    cached_wrappers: dict[int, Any] = {}

    for idx in range(num_nodes):
        c0, c1, raw_d1, raw_d2, op_kind = initial_specs[idx]
        d1 = raw_d1 % idx if idx > 0 else 0
        d2 = raw_d2 % idx if idx > 0 else 0
        current_specs.append((c0, c1, d1, d2, op_kind))
        fn = _make_dag_node(idx, c0, c1, d1, d2, op_kind, cached_wrappers)
        cached_wrappers[idx] = cached_graph.node(fn, name=f"node_{idx}")

    for is_call, raw_idx, val_a, val_b, raw_d1, raw_d2, op_kind in actions:
        idx = raw_idx % num_nodes
        if is_call:
            arg_x = val_a
            cached_res = cached_wrappers[idx](arg_x)

            # Build a fresh Graph with no shared store and register current specs
            fresh_graph = Graph(store=MemoryStore(max_entries=0))
            fresh_wrappers: dict[int, Any] = {}
            for i in range(num_nodes):
                sc0, sc1, sd1, sd2, sop = current_specs[i]
                ffn = _make_dag_node(i, sc0, sc1, sd1, sd2, sop, fresh_wrappers)
                fresh_wrappers[i] = fresh_graph.node(ffn, name=f"node_{i}")

            fresh_res = fresh_wrappers[idx](arg_x)
            assert cached_res == fresh_res
        else:
            d1 = raw_d1 % idx if idx > 0 else 0
            d2 = raw_d2 % idx if idx > 0 else 0
            current_specs[idx] = (val_a, val_b, d1, d2, op_kind)
            fn = _make_dag_node(idx, val_a, val_b, d1, d2, op_kind, cached_wrappers)
            cached_graph.node(fn, name=f"node_{idx}")

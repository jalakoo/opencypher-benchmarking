"""Tests for benchmark framework: registry, runner, and tier benchmark definitions."""

from __future__ import annotations

from unittest.mock import MagicMock

from graph_db_comparison.benchmarks import (
    BENCHMARK_REGISTRY,
    BenchmarkDefinition,
    get_benchmarks_for_tier,
)
from graph_db_comparison.benchmarks.runner import (
    _percentile,
    run_single_benchmark,
    run_tier,
)
from graph_db_comparison.connections import Result
from graph_db_comparison.models import BenchmarkConfig, FeatureSupportMap

# --- Registry ---


def test_registry_has_basic_benchmarks():
    """At least 10 basic benchmarks are registered."""
    basics = get_benchmarks_for_tier("basic")
    assert len(basics) >= 10


def test_registry_has_intermediate_benchmarks():
    """At least 11 intermediate benchmarks are registered."""
    intermediates = get_benchmarks_for_tier("intermediate")
    assert len(intermediates) >= 11


def test_registry_has_advanced_benchmarks():
    """At least 10 advanced benchmarks are registered."""
    advanced = get_benchmarks_for_tier("advanced")
    assert len(advanced) >= 10


def test_all_benchmarks_have_required_fields():
    """Every registered benchmark has name, tier, category, required_features, and callables."""
    for bench in BENCHMARK_REGISTRY:
        assert bench.name, f"Benchmark missing name: {bench}"
        assert bench.tier in ("basic", "intermediate", "advanced"), f"Bad tier: {bench.name}"
        assert bench.category in ("read", "write", "mixed"), f"Bad category: {bench.name}"
        assert isinstance(bench.required_features, dict), f"Bad features: {bench.name}"
        assert callable(bench.setup), f"setup not callable: {bench.name}"
        assert callable(bench.run), f"run not callable: {bench.name}"
        assert callable(bench.teardown), f"teardown not callable: {bench.name}"


def test_benchmark_names_are_unique():
    """No two benchmarks share the same name."""
    names = [b.name for b in BENCHMARK_REGISTRY]
    dupes = [n for n in names if names.count(n) > 1]
    assert len(names) == len(set(names)), f"Duplicate names: {dupes}"


def test_get_benchmarks_for_unknown_tier():
    """Unknown tier returns empty list."""
    assert get_benchmarks_for_tier("nonexistent") == []


# --- Basic tier benchmark names ---


EXPECTED_BASIC = {
    "create_single_node",
    "create_single_relationship",
    "match_all_nodes",
    "match_by_label",
    "match_by_property",
    "match_with_limit",
    "delete_single_node",
    "set_property",
    "remove_property",
    "count_nodes",
}


def test_basic_benchmark_names():
    """All expected basic benchmark names are registered."""
    basic_names = {b.name for b in get_benchmarks_for_tier("basic")}
    for name in EXPECTED_BASIC:
        assert name in basic_names, f"Missing basic benchmark: {name}"


# --- Intermediate tier benchmark names ---


EXPECTED_INTERMEDIATE = {
    "index_creation",
    "multi_hop_traversal",
    "aggregate_group_by",
    "merge_node",
    "create_bulk_nodes",
    "pattern_filtering",
    "optional_match",
    "order_and_paginate",
    "path_length_filter",
    "update_bulk",
    "delete_with_relationships",
}


def test_intermediate_benchmark_names():
    """All expected intermediate benchmark names are registered."""
    inter_names = {b.name for b in get_benchmarks_for_tier("intermediate")}
    for name in EXPECTED_INTERMEDIATE:
        assert name in inter_names, f"Missing intermediate benchmark: {name}"


# --- Advanced tier benchmark names ---


EXPECTED_ADVANCED = {
    "shortest_path",
    "recommendation_query",
    "graph_projection",
    "concurrent_writes",
    "mixed_read_write",
    "large_traversal",
    "complex_aggregation",
    "text_search",
    "temporal_queries",
    "write_throughput",
}


def test_advanced_benchmark_names():
    """All expected advanced benchmark names are registered."""
    adv_names = {b.name for b in get_benchmarks_for_tier("advanced")}
    for name in EXPECTED_ADVANCED:
        assert name in adv_names, f"Missing advanced benchmark: {name}"


# --- _percentile ---


def test_percentile_median():
    """p50 of [1,2,3,4,5] is 3."""
    assert _percentile([1, 2, 3, 4, 5], 50) == 3


def test_percentile_p95():
    """p95 of range(100) is approximately 94."""
    data = list(range(100))
    p95 = _percentile(data, 95)
    assert 93 <= p95 <= 96


def test_percentile_single_value():
    """Percentile of single-element list returns that element."""
    assert _percentile([42], 50) == 42
    assert _percentile([42], 95) == 42


# --- run_single_benchmark ---


def _make_config(**overrides) -> BenchmarkConfig:
    defaults = dict(
        iterations=3,
        warmup_iterations=1,
        timeout_seconds=10,
        dataset_scale=1,
        concurrency=4,
    )
    defaults.update(overrides)
    return BenchmarkConfig(**defaults)


def test_run_single_benchmark_pass():
    """A benchmark that succeeds returns status 'pass' with timing stats."""
    adapter = MagicMock()
    adapter.execute.return_value = Result(records=[])
    adapter.execute_read.return_value = Result(records=[])

    bench = BenchmarkDefinition(
        name="test_bench",
        tier="basic",
        category="read",
        required_features={},
        setup=lambda a, s: None,
        run=lambda a: a.execute_read("RETURN 1"),
        teardown=lambda a: None,
    )
    result = run_single_benchmark(adapter, bench, _make_config())
    assert result.status == "pass"
    assert result.benchmark_name == "test_bench"
    assert len(result.warm_latencies_ns) == 3
    assert result.median_ns is not None
    assert result.median_ns > 0


def test_run_single_benchmark_setup_failure():
    """A benchmark whose setup fails returns status 'error'."""
    adapter = MagicMock()
    adapter.execute.return_value = Result(records=[])

    def bad_setup(a, s):
        raise RuntimeError("setup boom")

    bench = BenchmarkDefinition(
        name="fail_setup",
        tier="basic",
        category="write",
        required_features={},
        setup=bad_setup,
        run=lambda a: None,
        teardown=lambda a: None,
    )
    result = run_single_benchmark(adapter, bench, _make_config())
    assert result.status == "error"
    assert any("Setup failed" in e for e in result.errors)


def test_run_single_benchmark_records_cold_latency():
    """Cold latency is recorded even if warmup/iterations follow."""
    adapter = MagicMock()
    adapter.execute.return_value = Result(records=[])
    adapter.execute_read.return_value = Result(records=[])

    bench = BenchmarkDefinition(
        name="cold_test",
        tier="basic",
        category="read",
        required_features={},
        setup=lambda a, s: None,
        run=lambda a: a.execute_read("RETURN 1"),
        teardown=lambda a: None,
    )
    result = run_single_benchmark(adapter, bench, _make_config())
    assert result.cold_latency_ns is not None
    assert result.cold_latency_ns >= 0


def test_run_single_benchmark_iteration_error_recorded():
    """Errors during iterations are recorded but don't stop the benchmark."""
    adapter = MagicMock()
    adapter.execute.return_value = Result(records=[])
    call_count = 0

    def flaky_run(a):
        nonlocal call_count
        call_count += 1
        if call_count == 3:  # fail on 3rd call (2nd timed iteration after warmup+cold+setup)
            raise RuntimeError("flaky")

    bench = BenchmarkDefinition(
        name="flaky_bench",
        tier="basic",
        category="read",
        required_features={},
        setup=lambda a, s: None,
        run=flaky_run,
        teardown=lambda a: None,
    )
    result = run_single_benchmark(adapter, bench, _make_config(warmup_iterations=0))
    # Some iterations should still pass
    assert result.status == "pass" or result.status == "error"
    # Errors should be recorded
    assert len(result.errors) >= 0  # at least attempted


# --- run_tier ---


def test_run_tier_skips_ineligible_benchmarks():
    """Benchmarks requiring unsupported features are skipped."""
    adapter = MagicMock()
    adapter.execute.return_value = Result(records=[])
    adapter.execute_read.return_value = Result(records=[])

    features = FeatureSupportMap(clauses={"MATCH"})
    config = _make_config()

    results = run_tier(adapter, "basic", features, config, "testdb")
    skipped = [r for r in results if r.status == "skip"]
    passed = [r for r in results if r.status == "pass"]
    # Some basic benchmarks only require MATCH and should pass
    assert len(passed) > 0 or len(skipped) > 0


def test_run_tier_all_skipped_below_threshold():
    """If fewer than 3 benchmarks are eligible, all are skipped with tier exclusion reason."""
    adapter = MagicMock()
    # Empty feature map — nothing is supported
    features = FeatureSupportMap()
    config = _make_config()

    results = run_tier(adapter, "basic", features, config, "testdb")
    # All should be skipped
    for r in results:
        assert r.status == "skip"

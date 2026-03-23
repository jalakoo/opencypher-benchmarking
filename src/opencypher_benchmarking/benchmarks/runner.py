"""Benchmark runner: orchestrates execution, timing, and statistics."""

from __future__ import annotations

import logging
import math
import statistics
import time
from typing import Any

from opencypher_benchmarking.benchmarks import BenchmarkDefinition, get_benchmarks_for_tier
from opencypher_benchmarking.compatibility import check_benchmark_eligible, check_tier_eligible
from opencypher_benchmarking.models import BenchmarkConfig, BenchmarkResult, FeatureSupportMap

logger = logging.getLogger(__name__)


def _percentile(data: list[int | float], pct: int) -> float:
    """Compute the pct-th percentile of a sorted list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_data[int(k)])
    return sorted_data[f] * (c - k) + sorted_data[c] * (k - f)


def run_single_benchmark(
    adapter: Any,
    bench: BenchmarkDefinition,
    config: BenchmarkConfig,
) -> BenchmarkResult:
    """Execute one benchmark: cold run, setup, warmup, timed iterations, teardown."""
    errors: list[str] = []

    # --- Cold variant ---
    cold_ns: int | None = None
    try:
        start = time.perf_counter_ns()
        bench.run(adapter)
        cold_ns = time.perf_counter_ns() - start
    except Exception as e:
        errors.append(f"Cold run error: {e}")

    # --- Setup ---
    try:
        bench.setup(adapter, config.dataset_scale)
    except Exception as e:
        return BenchmarkResult(
            benchmark_name=bench.name,
            tier=bench.tier,
            category=bench.category,
            database_name="",
            status="error",
            cold_latency_ns=cold_ns,
            errors=[f"Setup failed: {e}"],
        )

    # --- Warmup ---
    for _ in range(config.warmup_iterations):
        try:
            bench.run(adapter)
        except Exception:
            pass

    # --- Timed iterations ---
    warm_latencies: list[int] = []
    for _ in range(config.iterations):
        try:
            start = time.perf_counter_ns()
            bench.run(adapter)
            elapsed = time.perf_counter_ns() - start
            warm_latencies.append(elapsed)
        except Exception as e:
            errors.append(f"Iteration error: {e}")

    # --- Teardown ---
    try:
        bench.teardown(adapter)
    except Exception as e:
        errors.append(f"Teardown error: {e}")

    # --- Stats ---
    if warm_latencies:
        return BenchmarkResult(
            benchmark_name=bench.name,
            tier=bench.tier,
            category=bench.category,
            database_name="",
            status="pass",
            cold_latency_ns=cold_ns,
            warm_latencies_ns=warm_latencies,
            median_ns=statistics.median(warm_latencies),
            mean_ns=statistics.mean(warm_latencies),
            p95_ns=_percentile(warm_latencies, 95),
            p99_ns=_percentile(warm_latencies, 99),
            min_ns=min(warm_latencies),
            max_ns=max(warm_latencies),
            std_dev_ns=statistics.stdev(warm_latencies) if len(warm_latencies) > 1 else 0.0,
            errors=errors,
        )
    else:
        return BenchmarkResult(
            benchmark_name=bench.name,
            tier=bench.tier,
            category=bench.category,
            database_name="",
            status="error",
            cold_latency_ns=cold_ns,
            errors=errors if errors else ["All iterations failed"],
        )


def run_tier(
    adapter: Any,
    tier: str,
    features: FeatureSupportMap,
    config: BenchmarkConfig,
    db_name: str,
) -> list[BenchmarkResult]:
    """Run all eligible benchmarks in a tier."""
    benchmarks = get_benchmarks_for_tier(tier)
    eligible: list[BenchmarkDefinition] = []
    results: list[BenchmarkResult] = []

    for bench in benchmarks:
        ok, reason = check_benchmark_eligible(bench.required_features, features)
        if ok:
            eligible.append(bench)
        else:
            results.append(
                BenchmarkResult(
                    benchmark_name=bench.name,
                    tier=bench.tier,
                    category=bench.category,
                    database_name=db_name,
                    status="skip",
                    skipped_reason=reason,
                )
            )

    if not check_tier_eligible(len(eligible)):
        for bench in eligible:
            results.append(
                BenchmarkResult(
                    benchmark_name=bench.name,
                    tier=bench.tier,
                    category=bench.category,
                    database_name=db_name,
                    status="skip",
                    skipped_reason="Tier excluded: < 3 eligible benchmarks",
                )
            )
        return results

    for bench in eligible:
        logger.info(f"  Running {bench.name}...")
        result = run_single_benchmark(adapter, bench, config)
        result.database_name = db_name
        results.append(result)

    return results

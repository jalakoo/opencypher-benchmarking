"""Benchmark framework: definitions, registry, and tier imports."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class BenchmarkDefinition:
    """A single benchmark test definition."""

    name: str
    tier: str  # "basic" | "intermediate" | "advanced"
    category: str  # "read" | "write" | "mixed"
    required_features: dict[str, set[str]]
    setup: Callable[[Any, int], None]  # (adapter, scale) -> None
    run: Callable[[Any], None]  # (adapter) -> None
    teardown: Callable[[Any], None]  # (adapter) -> None
    description: str = ""


BENCHMARK_REGISTRY: list[BenchmarkDefinition] = []


def register_benchmark(bench: BenchmarkDefinition) -> None:
    """Add a benchmark to the global registry."""
    BENCHMARK_REGISTRY.append(bench)


def get_benchmarks_for_tier(tier: str) -> list[BenchmarkDefinition]:
    """Return all benchmarks for the given tier."""
    return [b for b in BENCHMARK_REGISTRY if b.tier == tier]


# Import tier modules to trigger registration
from graph_db_comparison.benchmarks import advanced, basic, intermediate  # noqa: E402, F401

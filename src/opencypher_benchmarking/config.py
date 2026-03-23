"""Load and validate config.yaml."""

from __future__ import annotations

from pathlib import Path

import yaml

from opencypher_benchmarking.models import AppConfig, BenchmarkConfig, DatabaseConfig, OutputConfig

VALID_ADAPTERS = {"bolt", "falkordb", "falkordblite", "ladybugdb"}
SERVER_ADAPTERS = {"bolt", "falkordb"}
EMBEDDED_ADAPTERS = {"falkordblite", "ladybugdb"}


def load_config(path: str) -> AppConfig:
    """Load config from a YAML file path."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(p.read_text())
    return _parse_raw(raw)


def load_config_from_string(yaml_string: str) -> AppConfig:
    """Load config from a YAML string. Useful for testing."""
    raw = yaml.safe_load(yaml_string)
    return _parse_raw(raw)


def _parse_raw(raw: dict) -> AppConfig:
    """Parse and validate raw YAML dict into AppConfig."""
    if not raw or "databases" not in raw or not raw["databases"]:
        raise ValueError("Config must contain a 'databases' section with at least one entry")

    databases = {}
    for name, db_raw in raw["databases"].items():
        databases[name] = _validate_database(name, db_raw or {})

    if not any(db.enabled for db in databases.values()):
        raise ValueError("At least one database must have enabled: true")

    benchmark = _validate_benchmark(raw.get("benchmark") or {})
    output = _validate_output(raw.get("output") or {})

    return AppConfig(databases=databases, benchmark=benchmark, output=output)


def _validate_database(name: str, raw: dict) -> DatabaseConfig:
    """Validate a single database config entry."""
    adapter = raw.get("adapter")
    if not adapter or adapter not in VALID_ADAPTERS:
        raise ValueError(
            f"Database '{name}': adapter must be one of {VALID_ADAPTERS}, got '{adapter}'"
        )

    enabled = raw.get("enabled", False)

    # Server adapters require host + port
    if adapter in SERVER_ADAPTERS:
        host = raw.get("host")
        if not host:
            raise ValueError(f"Database '{name}': host is required for '{adapter}' adapter")
        port = raw.get("port")
        if port is None:
            raise ValueError(f"Database '{name}': port is required for '{adapter}' adapter")
        if not isinstance(port, int) or port < 1 or port > 65535:
            raise ValueError(f"Database '{name}': port must be an integer 1-65535, got '{port}'")
    else:
        host = raw.get("host")
        port = raw.get("port")

    # Embedded adapters require db_path
    db_path = raw.get("db_path")
    if adapter in EMBEDDED_ADAPTERS and not db_path:
        raise ValueError(f"Database '{name}': db_path is required for '{adapter}' adapter")

    graph_name = raw.get("graph_name", "benchmark")
    auth = raw.get("auth")

    return DatabaseConfig(
        name=name,
        adapter=adapter,
        enabled=enabled,
        host=host,
        port=port,
        auth=auth,
        db_path=db_path,
        graph_name=graph_name,
    )


def _validate_benchmark(raw: dict) -> BenchmarkConfig:
    """Validate benchmark section."""
    iterations = raw.get("iterations", 5)
    if not isinstance(iterations, int) or iterations < 1:
        raise ValueError(f"benchmark.iterations must be >= 1, got '{iterations}'")

    warmup_iterations = raw.get("warmup_iterations", 2)
    if not isinstance(warmup_iterations, int) or warmup_iterations < 0:
        raise ValueError(f"benchmark.warmup_iterations must be >= 0, got '{warmup_iterations}'")

    timeout_seconds = raw.get("timeout_seconds", 30)
    dataset_scale = raw.get("dataset_scale", 1)

    concurrency = raw.get("concurrency", 8)
    if not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError(f"benchmark.concurrency must be >= 1, got '{concurrency}'")

    return BenchmarkConfig(
        iterations=iterations,
        warmup_iterations=warmup_iterations,
        timeout_seconds=timeout_seconds,
        dataset_scale=dataset_scale,
        concurrency=concurrency,
    )


def _validate_output(raw: dict) -> OutputConfig:
    """Validate output section."""
    return OutputConfig(
        report_path=raw.get("report_path", "./reports/report.html"),
        raw_json_path=raw.get("raw_json_path", "./reports/results.json"),
    )

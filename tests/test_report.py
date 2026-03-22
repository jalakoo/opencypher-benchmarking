"""Tests for HTML report generation and data aggregation."""

from __future__ import annotations

import json

from graph_db_comparison.models import (
    AppConfig,
    BenchmarkConfig,
    BenchmarkResult,
    DatabaseConfig,
    DatabaseReport,
    FeatureSupportMap,
    FullReport,
    OutputConfig,
)
from graph_db_comparison.report.generator import (
    aggregate_report_data,
    generate_html_report,
    generate_json_report,
)

# --- Fixture: synthetic FullReport ---


def _make_report() -> FullReport:
    """Build a synthetic FullReport with 2 databases and mixed results."""
    config = AppConfig(
        databases={
            "neo4j": DatabaseConfig(
                name="neo4j",
                adapter="bolt",
                enabled=True,
                host="localhost",
                port=7687,
                auth={"username": "neo4j", "password": "secret123"},
            ),
            "falkordb_lite": DatabaseConfig(
                name="falkordb_lite",
                adapter="falkordblite",
                enabled=True,
                db_path="/tmp/fdb.db",
                graph_name="benchmark",
            ),
        },
        benchmark=BenchmarkConfig(iterations=5),
        output=OutputConfig(),
    )

    neo4j_compliance = FeatureSupportMap(
        clauses={"MATCH", "CREATE", "MERGE", "DELETE", "SET"},
        functions={"count", "collect"},
        operators={"+", "STARTS WITH"},
        data_types={"Integer", "String"},
        pass_rate=0.87,
    )

    fdb_compliance = FeatureSupportMap(
        clauses={"MATCH", "CREATE"},
        functions={"count"},
        pass_rate=0.65,
    )

    neo4j_results = [
        BenchmarkResult(
            benchmark_name="match_all_nodes",
            tier="basic",
            category="read",
            database_name="neo4j",
            status="pass",
            cold_latency_ns=5_000_000,
            warm_latencies_ns=[1_000_000, 1_100_000, 900_000, 1_050_000, 950_000],
            median_ns=1_000_000,
            mean_ns=1_000_000,
            p95_ns=1_100_000,
            p99_ns=1_100_000,
            min_ns=900_000,
            max_ns=1_100_000,
            std_dev_ns=70710.0,
        ),
        BenchmarkResult(
            benchmark_name="create_single_node",
            tier="basic",
            category="write",
            database_name="neo4j",
            status="pass",
            cold_latency_ns=2_000_000,
            warm_latencies_ns=[500_000, 600_000, 550_000, 520_000, 480_000],
            median_ns=520_000,
            mean_ns=530_000,
            p95_ns=600_000,
            p99_ns=600_000,
            min_ns=480_000,
            max_ns=600_000,
            std_dev_ns=43000.0,
        ),
        BenchmarkResult(
            benchmark_name="merge_node",
            tier="intermediate",
            category="write",
            database_name="neo4j",
            status="skip",
            skipped_reason="Missing clause: MERGE",
        ),
    ]

    fdb_results = [
        BenchmarkResult(
            benchmark_name="match_all_nodes",
            tier="basic",
            category="read",
            database_name="falkordb_lite",
            status="pass",
            cold_latency_ns=3_000_000,
            warm_latencies_ns=[800_000, 850_000, 780_000, 820_000, 810_000],
            median_ns=810_000,
            mean_ns=812_000,
            p95_ns=850_000,
            p99_ns=850_000,
            min_ns=780_000,
            max_ns=850_000,
            std_dev_ns=24000.0,
        ),
        BenchmarkResult(
            benchmark_name="create_single_node",
            tier="basic",
            category="write",
            database_name="falkordb_lite",
            status="pass",
            cold_latency_ns=1_500_000,
            warm_latencies_ns=[400_000, 450_000, 420_000, 410_000, 430_000],
            median_ns=420_000,
            mean_ns=422_000,
            p95_ns=450_000,
            p99_ns=450_000,
            min_ns=400_000,
            max_ns=450_000,
            std_dev_ns=18000.0,
        ),
    ]

    return FullReport(
        timestamp="2026-03-22T12:00:00",
        version="0.1.0",
        config=config,
        databases=[
            DatabaseReport(
                name="neo4j",
                mode="server",
                adapter="bolt",
                compliance=neo4j_compliance,
                results=neo4j_results,
            ),
            DatabaseReport(
                name="falkordb_lite",
                mode="embedded",
                adapter="falkordblite",
                compliance=fdb_compliance,
                results=fdb_results,
            ),
        ],
    )


# --- aggregate_report_data ---


def test_aggregate_produces_tier_tables():
    """Aggregated data contains per-tier tables."""
    report = _make_report()
    data = aggregate_report_data(report)
    assert "tier_tables" in data
    assert "basic" in data["tier_tables"]


def test_aggregate_tier_winner():
    """Tier winner is the database with lowest median across benchmarks."""
    report = _make_report()
    data = aggregate_report_data(report)
    # falkordb_lite has lower medians for both basic benchmarks
    winners = data.get("tier_winners", {})
    if "basic" in winners:
        assert winners["basic"] == "falkordb_lite"


def test_aggregate_radar_chart_data():
    """Radar chart data has normalized 0-100 scores for each database."""
    report = _make_report()
    data = aggregate_report_data(report)
    assert "radar_data" in data
    for db_name, scores in data["radar_data"].items():
        for axis, val in scores.items():
            assert 0 <= val <= 100, f"{db_name}.{axis} = {val} out of range"


def test_aggregate_compliance_matrix():
    """Compliance matrix contains feature support per database."""
    report = _make_report()
    data = aggregate_report_data(report)
    assert "compliance_matrix" in data
    matrix = data["compliance_matrix"]
    assert len(matrix) > 0


def test_aggregate_skipped_benchmarks():
    """Skipped benchmarks are listed."""
    report = _make_report()
    data = aggregate_report_data(report)
    assert "skipped" in data
    skipped = data["skipped"]
    assert any(s["benchmark"] == "merge_node" for s in skipped)


def test_aggregate_warm_vs_cold():
    """Warm vs cold comparison data is populated."""
    report = _make_report()
    data = aggregate_report_data(report)
    assert "warm_vs_cold" in data
    assert len(data["warm_vs_cold"]) > 0


def test_aggregate_databases_tagged():
    """Each database entry has mode tag."""
    report = _make_report()
    data = aggregate_report_data(report)
    assert "databases" in data
    db_map = {d["name"]: d for d in data["databases"]}
    assert db_map["neo4j"]["mode"] == "server"
    assert db_map["falkordb_lite"]["mode"] == "embedded"


# --- generate_html_report ---


def test_html_report_is_valid_html(tmp_path):
    """Generated report is a valid HTML document."""
    report = _make_report()
    out = tmp_path / "report.html"
    generate_html_report(report, str(out))
    html = out.read_text()
    assert html.startswith("<!DOCTYPE html>") or html.startswith("<html")
    assert "</html>" in html


def test_html_report_contains_section_headers(tmp_path):
    """Report contains expected section headings."""
    report = _make_report()
    out = tmp_path / "report.html"
    generate_html_report(report, str(out))
    html = out.read_text()
    assert "Executive Summary" in html
    assert "Compliance Matrix" in html
    assert "Benchmark Results" in html


def test_html_report_contains_server_embedded_tags(tmp_path):
    """Report tags databases as [server] or [embedded]."""
    report = _make_report()
    out = tmp_path / "report.html"
    generate_html_report(report, str(out))
    html = out.read_text()
    assert "[server]" in html or "server" in html.lower()
    assert "[embedded]" in html or "embedded" in html.lower()


def test_html_report_redacts_passwords(tmp_path):
    """Passwords are not present in the generated report."""
    report = _make_report()
    out = tmp_path / "report.html"
    generate_html_report(report, str(out))
    html = out.read_text()
    assert "secret123" not in html


def test_html_report_contains_skipped(tmp_path):
    """Skipped benchmarks appear in the report."""
    report = _make_report()
    out = tmp_path / "report.html"
    generate_html_report(report, str(out))
    html = out.read_text()
    assert "merge_node" in html
    assert "skip" in html.lower() or "Skipped" in html


def test_html_report_creates_parent_dirs(tmp_path):
    """Report generator creates parent directories if needed."""
    report = _make_report()
    out = tmp_path / "nested" / "dir" / "report.html"
    generate_html_report(report, str(out))
    assert out.exists()


# --- generate_json_report ---


def test_json_report_is_valid_json(tmp_path):
    """JSON report is valid JSON."""
    report = _make_report()
    out = tmp_path / "results.json"
    generate_json_report(report, str(out))
    data = json.loads(out.read_text())
    assert data["version"] == "0.1.0"
    assert len(data["databases"]) == 2


def test_json_report_redacts_passwords(tmp_path):
    """JSON report redacts passwords."""
    report = _make_report()
    out = tmp_path / "results.json"
    generate_json_report(report, str(out))
    text = out.read_text()
    assert "secret123" not in text

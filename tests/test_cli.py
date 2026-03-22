"""Tests for CLI argument parsing and main orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from graph_db_comparison.__main__ import build_parser, main, run_benchmarks, run_check
from graph_db_comparison.models import (
    AppConfig,
    BenchmarkConfig,
    DatabaseConfig,
    FeatureSupportMap,
    FullReport,
    OutputConfig,
)

# --- build_parser ---


def test_parser_defaults():
    """Parser returns correct defaults when no args given."""
    parser = build_parser()
    args = parser.parse_args([])
    assert args.config == "./config.yaml"
    assert args.tier is None
    assert args.database is None
    assert args.benchmark is None
    assert args.skip_compliance is False
    assert args.compliance_only is False
    assert args.force_compliance is False
    assert args.compliance_ttl == 86400
    assert args.check is False
    assert args.no_report is False
    assert args.output_dir == "./reports"
    assert args.verbose is False


def test_parser_config_flag():
    """Parser reads -c flag."""
    parser = build_parser()
    args = parser.parse_args(["-c", "custom.yaml"])
    assert args.config == "custom.yaml"


def test_parser_tier_multiple():
    """Parser accumulates multiple -t flags."""
    parser = build_parser()
    args = parser.parse_args(["-t", "basic", "-t", "advanced"])
    assert args.tier == ["basic", "advanced"]


def test_parser_tier_invalid_rejected():
    """Parser rejects invalid tier values."""
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["-t", "mega"])


def test_parser_database_multiple():
    """Parser accumulates multiple -d flags."""
    parser = build_parser()
    args = parser.parse_args(["-d", "neo4j", "-d", "memgraph"])
    assert args.database == ["neo4j", "memgraph"]


def test_parser_benchmark_multiple():
    """Parser accumulates multiple -b flags."""
    parser = build_parser()
    args = parser.parse_args(["-b", "match_all_nodes", "-b", "count_nodes"])
    assert args.benchmark == ["match_all_nodes", "count_nodes"]


def test_parser_boolean_flags():
    """Parser sets boolean flags correctly."""
    parser = build_parser()
    args = parser.parse_args(
        [
            "--skip-compliance",
            "--force-compliance",
            "--compliance-only",
            "--no-report",
            "--check",
            "-v",
        ]
    )
    assert args.skip_compliance is True
    assert args.force_compliance is True
    assert args.compliance_only is True
    assert args.no_report is True
    assert args.check is True
    assert args.verbose is True


def test_parser_compliance_ttl():
    """Parser reads --compliance-ttl as int."""
    parser = build_parser()
    args = parser.parse_args(["--compliance-ttl", "7200"])
    assert args.compliance_ttl == 7200


def test_parser_output_dir():
    """Parser reads --output-dir."""
    parser = build_parser()
    args = parser.parse_args(["--output-dir", "/tmp/out"])
    assert args.output_dir == "/tmp/out"


# --- run_check ---


def _make_simple_config() -> AppConfig:
    return AppConfig(
        databases={
            "db1": DatabaseConfig(
                name="db1",
                adapter="bolt",
                enabled=True,
                host="localhost",
                port=7687,
            ),
            "db2": DatabaseConfig(
                name="db2",
                adapter="bolt",
                enabled=False,
                host="localhost",
                port=7688,
            ),
        },
        benchmark=BenchmarkConfig(),
        output=OutputConfig(),
    )


@patch("graph_db_comparison.__main__.create_adapter")
def test_run_check_reports_reachable(mock_create, capsys):
    """run_check prints reachable for successful connections."""
    mock_adapter = MagicMock()
    mock_adapter.execute.return_value = MagicMock(records=[{"n": 1}])
    mock_create.return_value = mock_adapter

    config = _make_simple_config()
    run_check(config)

    output = capsys.readouterr().out
    assert "db1" in output
    assert "reachable" in output
    # db2 is disabled, should not appear
    assert "db2" not in output


@patch("graph_db_comparison.__main__.create_adapter")
def test_run_check_reports_unreachable(mock_create, capsys):
    """run_check prints error for failed connections."""
    mock_create.side_effect = ConnectionError("refused")

    config = _make_simple_config()
    run_check(config)

    output = capsys.readouterr().out
    assert "db1" in output
    assert "refused" in output


# --- run_benchmarks ---


@patch("graph_db_comparison.__main__.run_tier")
@patch("graph_db_comparison.__main__._run_compliance_with_cache")
@patch("graph_db_comparison.__main__.create_adapter")
def test_run_benchmarks_basic_flow(mock_create, mock_compliance, mock_run_tier):
    """run_benchmarks connects, runs compliance, runs tiers, returns FullReport."""
    mock_adapter = MagicMock()
    mock_adapter.execute.return_value = MagicMock(records=[])
    mock_create.return_value = mock_adapter

    mock_compliance.return_value = FeatureSupportMap(
        clauses={"MATCH"},
        pass_rate=0.5,
    )
    mock_run_tier.return_value = []

    config = _make_simple_config()
    parser = build_parser()
    args = parser.parse_args([])

    report = run_benchmarks(args, config)

    assert isinstance(report, FullReport)
    assert len(report.databases) == 1  # only db1 is enabled
    assert report.databases[0].name == "db1"
    mock_create.assert_called_once()
    mock_compliance.assert_called_once()
    # Should run all 3 tiers
    assert mock_run_tier.call_count == 3


@patch("graph_db_comparison.__main__.run_tier")
@patch("graph_db_comparison.__main__._run_compliance_with_cache")
@patch("graph_db_comparison.__main__.create_adapter")
def test_run_benchmarks_filters_by_database(mock_create, mock_compliance, mock_run_tier):
    """run_benchmarks skips databases not in -d filter."""
    mock_adapter = MagicMock()
    mock_adapter.execute.return_value = MagicMock(records=[])
    mock_create.return_value = mock_adapter
    mock_compliance.return_value = FeatureSupportMap(clauses={"MATCH"}, pass_rate=1.0)
    mock_run_tier.return_value = []

    config = AppConfig(
        databases={
            "neo4j": DatabaseConfig(
                name="neo4j",
                adapter="bolt",
                enabled=True,
                host="localhost",
                port=7687,
            ),
            "memgraph": DatabaseConfig(
                name="memgraph",
                adapter="bolt",
                enabled=True,
                host="localhost",
                port=7688,
            ),
        },
        benchmark=BenchmarkConfig(),
        output=OutputConfig(),
    )
    parser = build_parser()
    args = parser.parse_args(["-d", "neo4j"])

    report = run_benchmarks(args, config)
    assert len(report.databases) == 1
    assert report.databases[0].name == "neo4j"


@patch("graph_db_comparison.__main__.run_tier")
@patch("graph_db_comparison.__main__._run_compliance_with_cache")
@patch("graph_db_comparison.__main__.create_adapter")
def test_run_benchmarks_filters_by_tier(mock_create, mock_compliance, mock_run_tier):
    """run_benchmarks only runs tiers specified by -t."""
    mock_adapter = MagicMock()
    mock_adapter.execute.return_value = MagicMock(records=[])
    mock_create.return_value = mock_adapter
    mock_compliance.return_value = FeatureSupportMap(clauses={"MATCH"}, pass_rate=1.0)
    mock_run_tier.return_value = []

    config = _make_simple_config()
    parser = build_parser()
    args = parser.parse_args(["-t", "basic"])

    run_benchmarks(args, config)
    assert mock_run_tier.call_count == 1
    assert mock_run_tier.call_args_list[0][0][1] == "basic"


@patch("graph_db_comparison.__main__.create_adapter")
def test_run_benchmarks_connection_failure_continues(mock_create):
    """Connection failure for one DB doesn't stop others."""
    mock_create.side_effect = ConnectionError("refused")

    config = _make_simple_config()
    parser = build_parser()
    args = parser.parse_args([])

    report = run_benchmarks(args, config)
    assert len(report.databases) == 1
    assert report.databases[0].compliance_error is not None


@patch("graph_db_comparison.__main__._run_compliance_with_cache")
@patch("graph_db_comparison.__main__.create_adapter")
def test_run_benchmarks_skip_compliance(mock_create, mock_compliance):
    """--skip-compliance skips compliance and runs benchmarks with all features assumed."""
    mock_adapter = MagicMock()
    mock_adapter.execute.return_value = MagicMock(records=[])
    mock_create.return_value = mock_adapter

    config = _make_simple_config()
    parser = build_parser()
    args = parser.parse_args(["--skip-compliance"])

    run_benchmarks(args, config)
    mock_compliance.assert_not_called()


@patch("graph_db_comparison.__main__._run_compliance_with_cache")
@patch("graph_db_comparison.__main__.create_adapter")
def test_run_benchmarks_compliance_only(mock_create, mock_compliance):
    """--compliance-only runs compliance but skips benchmarks."""
    mock_adapter = MagicMock()
    mock_adapter.execute.return_value = MagicMock(records=[])
    mock_create.return_value = mock_adapter
    mock_compliance.return_value = FeatureSupportMap(clauses={"MATCH"}, pass_rate=1.0)

    config = _make_simple_config()
    parser = build_parser()
    args = parser.parse_args(["--compliance-only"])

    report = run_benchmarks(args, config)
    assert len(report.databases) == 1
    assert report.databases[0].compliance is not None
    assert report.databases[0].results == []


# --- main entry point ---


@patch("graph_db_comparison.__main__.load_config")
def test_main_check_mode(mock_load, capsys):
    """main() in --check mode loads config and runs connectivity check."""
    mock_load.return_value = _make_simple_config()

    with patch("graph_db_comparison.__main__.run_check") as mock_check:
        with patch("sys.argv", ["graph-db-bench", "--check", "-c", "sample_config.yaml"]):
            main()
        mock_check.assert_called_once()

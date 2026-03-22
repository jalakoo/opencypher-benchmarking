"""Tests for config loading and validation."""

import textwrap

import pytest

from graph_db_comparison.config import load_config, load_config_from_string

# --- Valid config loading ---


def test_load_sample_config():
    """Loading sample_config.yaml returns a valid AppConfig."""
    config = load_config("sample_config.yaml")
    assert len(config.databases) == 6
    assert config.benchmark.iterations == 5
    assert config.output.report_path == "./reports/report.html"


def test_database_names_preserved():
    """Database names from YAML keys are stored in DatabaseConfig.name."""
    config = load_config("sample_config.yaml")
    assert "neo4j" in config.databases
    assert config.databases["neo4j"].name == "neo4j"


def test_bolt_adapter_is_server_mode():
    """Bolt adapter databases are classified as server mode."""
    config = load_config("sample_config.yaml")
    assert config.databases["neo4j"].mode == "server"
    assert config.databases["memgraph"].mode == "server"
    assert config.databases["arcadedb"].mode == "server"


def test_falkordb_adapter_is_server_mode():
    """FalkorDB adapter databases are classified as server mode."""
    config = load_config("sample_config.yaml")
    assert config.databases["falkordb"].mode == "server"


def test_embedded_adapters_are_embedded_mode():
    """Embedded adapter databases are classified as embedded mode."""
    config = load_config("sample_config.yaml")
    assert config.databases["falkordb_lite"].mode == "embedded"
    assert config.databases["ladybugdb"].mode == "embedded"


def test_graph_name_loaded():
    """graph_name is read from config for falkordb adapters."""
    config = load_config("sample_config.yaml")
    assert config.databases["falkordb"].graph_name == "benchmark"
    assert config.databases["falkordb_lite"].graph_name == "benchmark"


def test_graph_name_defaults_to_benchmark():
    """graph_name defaults to 'benchmark' when omitted for falkordb adapters."""
    yaml = textwrap.dedent("""\
        databases:
          fdb:
            adapter: falkordb
            host: localhost
            port: 6379
            auth:
              password: ""
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./report.html
          raw_json_path: ./results.json
    """)
    config = load_config_from_string(yaml)
    assert config.databases["fdb"].graph_name == "benchmark"


def test_ladybugdb_graph_name_defaults():
    """graph_name defaults to 'benchmark' for ladybugdb even though it may not use it."""
    config = load_config("sample_config.yaml")
    assert config.databases["ladybugdb"].graph_name == "benchmark"


def test_bolt_adapter_has_host_and_port():
    """Bolt adapter configs have host and port populated."""
    config = load_config("sample_config.yaml")
    db = config.databases["neo4j"]
    assert db.host == "localhost"
    assert db.port == 7687


def test_embedded_adapter_has_db_path():
    """Embedded adapter configs have db_path populated."""
    config = load_config("sample_config.yaml")
    assert config.databases["falkordb_lite"].db_path == "/tmp/falkordb_bench.db"
    assert config.databases["ladybugdb"].db_path == "/tmp/ladybug_bench"


def test_benchmark_config_values():
    """Benchmark section values are correctly parsed."""
    config = load_config("sample_config.yaml")
    assert config.benchmark.warmup_iterations == 2
    assert config.benchmark.timeout_seconds == 30
    assert config.benchmark.dataset_scale == 1
    assert config.benchmark.concurrency == 8


# --- Validation errors ---


def test_invalid_adapter_raises():
    """Unknown adapter string raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: mongodb
            host: localhost
            port: 27017
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="adapter"):
        load_config_from_string(yaml)


def test_missing_adapter_raises():
    """Missing adapter field raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            host: localhost
            port: 7687
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="adapter"):
        load_config_from_string(yaml)


def test_bolt_missing_port_raises():
    """Bolt adapter without port raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            host: localhost
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="port"):
        load_config_from_string(yaml)


def test_bolt_missing_host_raises():
    """Bolt adapter without host raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            port: 7687
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="host"):
        load_config_from_string(yaml)


def test_port_out_of_range_raises():
    """Port outside 1-65535 raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            host: localhost
            port: 99999
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="port"):
        load_config_from_string(yaml)


def test_port_zero_raises():
    """Port 0 raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            host: localhost
            port: 0
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="port"):
        load_config_from_string(yaml)


def test_embedded_missing_db_path_raises():
    """Embedded adapter without db_path raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: falkordblite
            enabled: true
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="db_path"):
        load_config_from_string(yaml)


def test_no_enabled_databases_raises():
    """Config with no enabled databases raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            host: localhost
            port: 7687
            enabled: false
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="enabled"):
        load_config_from_string(yaml)


def test_iterations_zero_raises():
    """iterations < 1 raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            host: localhost
            port: 7687
            enabled: true
        benchmark:
          iterations: 0
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="iterations"):
        load_config_from_string(yaml)


def test_negative_warmup_raises():
    """warmup_iterations < 0 raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            host: localhost
            port: 7687
            enabled: true
        benchmark:
          iterations: 1
          warmup_iterations: -1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="warmup"):
        load_config_from_string(yaml)


def test_concurrency_zero_raises():
    """concurrency < 1 raises ValueError."""
    yaml = textwrap.dedent("""\
        databases:
          db1:
            adapter: bolt
            host: localhost
            port: 7687
            enabled: true
        benchmark:
          iterations: 1
          concurrency: 0
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="concurrency"):
        load_config_from_string(yaml)


def test_missing_databases_section_raises():
    """Config with no databases section raises ValueError."""
    yaml = textwrap.dedent("""\
        benchmark:
          iterations: 1
        output:
          report_path: ./r.html
          raw_json_path: ./r.json
    """)
    with pytest.raises(ValueError, match="databases"):
        load_config_from_string(yaml)


def test_file_not_found_raises():
    """Loading a nonexistent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")

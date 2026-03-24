"""Tests for database adapter protocol, Result class, and connection factory."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from opencypher_benchmarking.connections import Result, create_adapter
from opencypher_benchmarking.models import DatabaseConfig

# --- Result class ---


def test_result_stores_records():
    """Result holds a list of record dicts."""
    r = Result(records=[{"n": 1}, {"n": 2}])
    assert len(r.records) == 2
    assert r.records[0] == {"n": 1}


def test_result_stores_metadata():
    """Result can hold optional metadata."""
    r = Result(records=[], metadata={"time_ms": 42})
    assert r.metadata == {"time_ms": 42}


def test_result_metadata_defaults_to_none():
    """Result metadata defaults to None when not provided."""
    r = Result(records=[])
    assert r.metadata is None


def test_result_empty_records():
    """Result with no records is valid."""
    r = Result(records=[])
    assert r.records == []


# --- create_adapter factory ---


def test_create_adapter_unknown_adapter_raises():
    """Unknown adapter type raises ValueError."""
    config = DatabaseConfig(name="bad", adapter="mongodb", enabled=True)
    with pytest.raises(ValueError, match="Unknown adapter"):
        create_adapter(config)


@patch("opencypher_benchmarking.connections.BoltAdapter")
def test_create_adapter_bolt(mock_bolt_cls):
    """create_adapter returns BoltAdapter for 'bolt' adapter."""
    config = DatabaseConfig(
        name="neo4j",
        adapter="bolt",
        enabled=True,
        host="localhost",
        port=7687,
        auth={"username": "neo4j", "password": "pass"},
    )
    mock_bolt_cls.return_value = MagicMock()
    result = create_adapter(config)
    mock_bolt_cls.assert_called_once_with(config)
    assert result is mock_bolt_cls.return_value


@patch("opencypher_benchmarking.connections.FalkorDBAdapter")
def test_create_adapter_falkordb(mock_fdb_cls):
    """create_adapter returns FalkorDBAdapter for 'falkordb' adapter."""
    config = DatabaseConfig(
        name="fdb",
        adapter="falkordb",
        enabled=True,
        host="localhost",
        port=6379,
        graph_name="bench",
    )
    mock_fdb_cls.return_value = MagicMock()
    result = create_adapter(config)
    mock_fdb_cls.assert_called_once_with(config)
    assert result is mock_fdb_cls.return_value


@patch("opencypher_benchmarking.connections.FalkorDBLiteAdapter")
def test_create_adapter_falkordblite(mock_fdbl_cls):
    """create_adapter returns FalkorDBLiteAdapter for 'falkordblite' adapter."""
    config = DatabaseConfig(
        name="fdbl",
        adapter="falkordblite",
        enabled=True,
        db_path="/tmp/test.db",
        graph_name="bench",
    )
    mock_fdbl_cls.return_value = MagicMock()
    result = create_adapter(config)
    mock_fdbl_cls.assert_called_once_with(config)
    assert result is mock_fdbl_cls.return_value


@patch("opencypher_benchmarking.connections.LadybugDBAdapter")
def test_create_adapter_ladybugdb(mock_ldb_cls):
    """create_adapter returns LadybugDBAdapter for 'ladybugdb' adapter."""
    config = DatabaseConfig(
        name="ldb",
        adapter="ladybugdb",
        enabled=True,
        db_path="/tmp/test_ldb",
    )
    mock_ldb_cls.return_value = MagicMock()
    result = create_adapter(config)
    mock_ldb_cls.assert_called_once_with(config)
    assert result is mock_ldb_cls.return_value


# --- Adapter interface compliance ---


def _make_bolt_config() -> DatabaseConfig:
    return DatabaseConfig(
        name="neo4j",
        adapter="bolt",
        enabled=True,
        host="localhost",
        port=7687,
        auth={"username": "neo4j", "password": "pass"},
    )


def _make_falkordb_config() -> DatabaseConfig:
    return DatabaseConfig(
        name="fdb",
        adapter="falkordb",
        enabled=True,
        host="localhost",
        port=6379,
        graph_name="bench",
    )


def _make_falkordblite_config() -> DatabaseConfig:
    return DatabaseConfig(
        name="fdbl",
        adapter="falkordblite",
        enabled=True,
        db_path="/tmp/test.db",
        graph_name="bench",
    )


def _make_ladybugdb_config() -> DatabaseConfig:
    return DatabaseConfig(
        name="ldb",
        adapter="ladybugdb",
        enabled=True,
        db_path="/tmp/test_ldb",
    )


@patch("opencypher_benchmarking.connections._import_neo4j")
def test_bolt_adapter_has_protocol_methods(mock_import):
    """BoltAdapter has all required protocol methods."""
    mock_driver = MagicMock()
    mock_import.return_value = mock_driver
    from opencypher_benchmarking.connections import BoltAdapter

    adapter = BoltAdapter(_make_bolt_config())
    assert hasattr(adapter, "execute")
    assert hasattr(adapter, "execute_read")
    assert hasattr(adapter, "execute_in_transaction")
    assert hasattr(adapter, "setup_schema")
    assert hasattr(adapter, "close")
    assert callable(adapter.execute)
    assert callable(adapter.close)


@patch("opencypher_benchmarking.connections._import_falkordb")
def test_falkordb_adapter_has_protocol_methods(mock_import):
    """FalkorDBAdapter has all required protocol methods."""
    mock_db = MagicMock()
    mock_import.return_value = mock_db
    from opencypher_benchmarking.connections import FalkorDBAdapter

    adapter = FalkorDBAdapter(_make_falkordb_config())
    assert hasattr(adapter, "execute")
    assert hasattr(adapter, "execute_read")
    assert hasattr(adapter, "execute_in_transaction")
    assert hasattr(adapter, "setup_schema")
    assert hasattr(adapter, "close")


@patch("opencypher_benchmarking.connections._import_falkordblite")
def test_falkordblite_adapter_has_protocol_methods(mock_import):
    """FalkorDBLiteAdapter has all required protocol methods."""
    mock_db = MagicMock()
    mock_import.return_value = mock_db
    from opencypher_benchmarking.connections import FalkorDBLiteAdapter

    adapter = FalkorDBLiteAdapter(_make_falkordblite_config())
    assert hasattr(adapter, "execute")
    assert hasattr(adapter, "execute_read")
    assert hasattr(adapter, "execute_in_transaction")
    assert hasattr(adapter, "setup_schema")
    assert hasattr(adapter, "close")


@patch("opencypher_benchmarking.connections._import_ladybugdb")
def test_ladybugdb_adapter_has_protocol_methods(mock_import):
    """LadybugDBAdapter has all required protocol methods."""
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_import.return_value = (mock_db, mock_conn)
    from opencypher_benchmarking.connections import LadybugDBAdapter

    adapter = LadybugDBAdapter(_make_ladybugdb_config())
    assert hasattr(adapter, "execute")
    assert hasattr(adapter, "execute_read")
    assert hasattr(adapter, "execute_in_transaction")
    assert hasattr(adapter, "setup_schema")
    assert hasattr(adapter, "close")


# --- BoltAdapter behavior with mocks ---


@patch("opencypher_benchmarking.connections._import_neo4j")
def test_bolt_execute_runs_cypher(mock_import):
    """BoltAdapter.execute() runs a Cypher query through the driver."""
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
    mock_session.run.return_value.data.return_value = [{"n": 1}]
    mock_import.return_value = mock_driver

    from opencypher_benchmarking.connections import BoltAdapter

    adapter = BoltAdapter(_make_bolt_config())
    result = adapter.execute("RETURN 1 AS n")
    assert result.records == [{"n": 1}]


@patch("opencypher_benchmarking.connections._import_neo4j")
def test_bolt_setup_schema_is_noop(mock_import):
    """BoltAdapter.setup_schema() does nothing."""
    mock_import.return_value = MagicMock()
    from opencypher_benchmarking.connections import BoltAdapter

    adapter = BoltAdapter(_make_bolt_config())
    adapter.setup_schema()  # should not raise


@patch("opencypher_benchmarking.connections._import_neo4j")
def test_bolt_close_closes_driver(mock_import):
    """BoltAdapter.close() closes the underlying driver."""
    mock_driver = MagicMock()
    mock_import.return_value = mock_driver
    from opencypher_benchmarking.connections import BoltAdapter

    adapter = BoltAdapter(_make_bolt_config())
    adapter.close()
    mock_driver.close.assert_called_once()


# --- FalkorDBAdapter behavior with mocks ---


@patch("opencypher_benchmarking.connections._import_falkordb")
def test_falkordb_execute_runs_query(mock_import):
    """FalkorDBAdapter.execute() calls graph.query()."""
    mock_db = MagicMock()
    mock_graph = MagicMock()
    mock_db.select_graph.return_value = mock_graph
    mock_graph.query.return_value.result_set = [[1]]
    mock_graph.query.return_value.header = ["n"]
    mock_import.return_value = mock_db

    from opencypher_benchmarking.connections import FalkorDBAdapter

    adapter = FalkorDBAdapter(_make_falkordb_config())
    result = adapter.execute("RETURN 1 AS n")
    mock_graph.query.assert_called_once()
    assert isinstance(result, Result)


@patch("opencypher_benchmarking.connections._import_falkordb")
def test_falkordb_execute_read_uses_ro_query(mock_import):
    """FalkorDBAdapter.execute_read() calls graph.ro_query()."""
    mock_db = MagicMock()
    mock_graph = MagicMock()
    mock_db.select_graph.return_value = mock_graph
    mock_graph.ro_query.return_value.result_set = [[1]]
    mock_graph.ro_query.return_value.header = ["n"]
    mock_import.return_value = mock_db

    from opencypher_benchmarking.connections import FalkorDBAdapter

    adapter = FalkorDBAdapter(_make_falkordb_config())
    result = adapter.execute_read("RETURN 1 AS n")
    mock_graph.ro_query.assert_called_once()
    assert isinstance(result, Result)


@patch("opencypher_benchmarking.connections._import_falkordb")
def test_falkordb_uses_graph_name_from_config(mock_import):
    """FalkorDBAdapter selects graph using graph_name from config."""
    mock_db = MagicMock()
    mock_import.return_value = mock_db

    from opencypher_benchmarking.connections import FalkorDBAdapter

    config = _make_falkordb_config()
    config.graph_name = "my_graph"
    FalkorDBAdapter(config)
    mock_db.select_graph.assert_called_once_with("my_graph")


# --- LadybugDBAdapter behavior with mocks ---


@patch("opencypher_benchmarking.connections._import_ladybugdb")
def test_ladybugdb_setup_schema_creates_tables(mock_import):
    """LadybugDBAdapter.setup_schema() issues CREATE NODE/REL TABLE statements."""
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_import.return_value = (mock_db, mock_conn)

    from opencypher_benchmarking.connections import LadybugDBAdapter

    adapter = LadybugDBAdapter(_make_ladybugdb_config())
    adapter.setup_schema()
    calls = [str(c) for c in mock_conn.execute.call_args_list]
    create_calls = [c for c in calls if "CREATE" in c]
    assert len(create_calls) >= 4


@patch("opencypher_benchmarking.connections._import_ladybugdb")
def test_ladybugdb_schema_includes_tier_prefixed_tables(mock_import):
    """LadybugDBAdapter.setup_schema() creates tables for all benchmark tier prefixes."""
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_import.return_value = (mock_db, mock_conn)

    from opencypher_benchmarking.connections import LadybugDBAdapter

    adapter = LadybugDBAdapter(_make_ladybugdb_config())
    adapter.setup_schema()
    calls = " ".join(str(c) for c in mock_conn.execute.call_args_list)

    # Must have node tables for all tier prefixes
    for prefix in ["_basic", "_inter", "_adv"]:
        assert f"{prefix}_Person" in calls, f"Missing {prefix}_Person node table"

    # Must have company tables for inter and adv
    assert "_inter_Company" in calls
    assert "_adv_Company" in calls

    # Must have rel tables for inter and adv
    assert "_inter_KNOWS" in calls
    assert "_adv_KNOWS" in calls


def test_resolve_ladybugdb_path_appends_filename_to_directory(tmp_path):
    """_resolve_ladybugdb_path appends 'ladybug.db' when path is a directory."""
    from opencypher_benchmarking.connections import _resolve_ladybugdb_path

    result = _resolve_ladybugdb_path(str(tmp_path))
    assert result == str(tmp_path / "ladybug.db")


def test_resolve_ladybugdb_path_appends_for_no_extension():
    """_resolve_ladybugdb_path appends 'ladybug.db' when path has no extension."""
    from opencypher_benchmarking.connections import _resolve_ladybugdb_path

    result = _resolve_ladybugdb_path("/tmp/ladybug_bench")
    assert result == "/tmp/ladybug_bench/ladybug.db"


def test_resolve_ladybugdb_path_preserves_file_path():
    """_resolve_ladybugdb_path keeps path as-is when it has a file extension."""
    from opencypher_benchmarking.connections import _resolve_ladybugdb_path

    result = _resolve_ladybugdb_path("/tmp/my_ladybug.db")
    assert result == "/tmp/my_ladybug.db"

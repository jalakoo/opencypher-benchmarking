"""Database adapter protocol and implementations."""

from __future__ import annotations

import logging
from typing import Any, Protocol

from graph_db_comparison.models import DatabaseConfig

logger = logging.getLogger(__name__)


class Result:
    """Unified query result wrapper."""

    def __init__(
        self,
        records: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ):
        self.records = records
        self.metadata = metadata


class DatabaseAdapter(Protocol):
    """Protocol defining the interface all database adapters must implement."""

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Result: ...

    def execute_read(self, cypher: str, params: dict[str, Any] | None = None) -> Result: ...

    def execute_in_transaction(
        self, queries: list[tuple[str, dict[str, Any] | None]]
    ) -> list[Result]: ...

    def setup_schema(self) -> None: ...

    def close(self) -> None: ...


# --- Import helpers (allow mocking in tests) ---


def _import_neo4j(config: DatabaseConfig):
    """Create and return a neo4j driver instance."""
    from neo4j import GraphDatabase

    uri = f"bolt://{config.host}:{config.port}"
    auth = None
    if config.auth:
        username = config.auth.get("username", "")
        password = config.auth.get("password", "")
        if username or password:
            auth = (username, password)
    return GraphDatabase.driver(uri, auth=auth)


def _import_falkordb(config: DatabaseConfig):
    """Create and return a FalkorDB client instance."""
    from falkordb import FalkorDB

    kwargs: dict[str, Any] = {}
    if config.host:
        kwargs["host"] = config.host
    if config.port:
        kwargs["port"] = config.port
    if config.auth and config.auth.get("password"):
        kwargs["password"] = config.auth["password"]
    return FalkorDB(**kwargs)


def _import_falkordblite(config: DatabaseConfig):
    """Create and return a FalkorDB Lite client instance."""
    from falkordblite import FalkorDB

    return FalkorDB(config.db_path)


def _import_ladybugdb(config: DatabaseConfig):
    """Create and return a LadybugDB database instance."""
    import kuzu

    return kuzu.Database(config.db_path)


# --- Adapter implementations ---


class BoltAdapter:
    """Adapter for Neo4j, Memgraph, and ArcadeDB via Bolt protocol."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._driver = _import_neo4j(config)

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        with self._driver.session() as session:
            result = session.run(cypher, params or {})
            return Result(records=result.data())

    def execute_read(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        with self._driver.session() as session:

            def _read_tx(tx):
                return tx.run(cypher, params or {}).data()

            records = session.execute_read(_read_tx)
            return Result(records=records)

    def execute_in_transaction(
        self, queries: list[tuple[str, dict[str, Any] | None]]
    ) -> list[Result]:
        results = []
        with self._driver.session() as session:
            tx = session.begin_transaction()
            try:
                for cypher, params in queries:
                    result = tx.run(cypher, params or {})
                    results.append(Result(records=result.data()))
                tx.commit()
            except Exception:
                tx.rollback()
                raise
        return results

    def setup_schema(self) -> None:
        pass  # Auto-schema databases don't need this

    def close(self) -> None:
        self._driver.close()


class FalkorDBAdapter:
    """Adapter for FalkorDB via Redis protocol."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._db = _import_falkordb(config)
        self._graph = self._db.select_graph(config.graph_name)

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        result = self._graph.query(cypher, params)
        return _convert_falkordb_result(result)

    def execute_read(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        result = self._graph.ro_query(cypher, params)
        return _convert_falkordb_result(result)

    def execute_in_transaction(
        self, queries: list[tuple[str, dict[str, Any] | None]]
    ) -> list[Result]:
        results = []
        for cypher, params in queries:
            result = self._graph.query(cypher, params)
            results.append(_convert_falkordb_result(result))
        return results

    def setup_schema(self) -> None:
        pass  # Auto-schema

    def close(self) -> None:
        pass  # FalkorDB client has no explicit close


class FalkorDBLiteAdapter:
    """Adapter for FalkorDB Lite (embedded)."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._db = _import_falkordblite(config)
        self._graph = self._db.select_graph(config.graph_name)

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        result = self._graph.query(cypher, params)
        return _convert_falkordb_result(result)

    def execute_read(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        result = self._graph.ro_query(cypher, params)
        return _convert_falkordb_result(result)

    def execute_in_transaction(
        self, queries: list[tuple[str, dict[str, Any] | None]]
    ) -> list[Result]:
        results = []
        for cypher, params in queries:
            result = self._graph.query(cypher, params)
            results.append(_convert_falkordb_result(result))
        return results

    def setup_schema(self) -> None:
        pass  # Auto-schema

    def close(self) -> None:
        if hasattr(self._db, "close"):
            self._db.close()


class LadybugDBAdapter:
    """Adapter for LadybugDB (embedded, formerly Kuzu)."""

    SCHEMA_STATEMENTS = [
        "CREATE NODE TABLE IF NOT EXISTS Person("
        "name STRING, age INT64, city STRING, active BOOLEAN, created STRING, "
        "PRIMARY KEY(name))",
        "CREATE NODE TABLE IF NOT EXISTS Company("
        "name STRING, industry STRING, founded INT64, "
        "PRIMARY KEY(name))",
        "CREATE REL TABLE IF NOT EXISTS KNOWS(FROM Person TO Person)",
        "CREATE REL TABLE IF NOT EXISTS WORKS_AT(FROM Person TO Company)",
    ]

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        db = _import_ladybugdb(config)
        self._conn = db

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        result = self._conn.execute(cypher, parameters=params or {})
        return _convert_ladybugdb_result(result)

    def execute_read(self, cypher: str, params: dict[str, Any] | None = None) -> Result:
        return self.execute(cypher, params)

    def execute_in_transaction(
        self, queries: list[tuple[str, dict[str, Any] | None]]
    ) -> list[Result]:
        results = []
        for cypher, params in queries:
            result = self._conn.execute(cypher, parameters=params or {})
            results.append(_convert_ladybugdb_result(result))
        return results

    def setup_schema(self) -> None:
        for stmt in self.SCHEMA_STATEMENTS:
            try:
                self._conn.execute(stmt)
            except Exception as e:
                logger.debug(f"Schema statement skipped (may already exist): {e}")

    def close(self) -> None:
        if hasattr(self._conn, "close"):
            self._conn.close()


# --- Result converters ---


def _convert_falkordb_result(result: Any) -> Result:
    """Convert a FalkorDB query result to our Result type."""
    records = []
    header = getattr(result, "header", [])
    result_set = getattr(result, "result_set", [])
    for row in result_set:
        record = {}
        for i, col in enumerate(header):
            record[col] = row[i] if i < len(row) else None
        records.append(record)
    return Result(records=records)


def _convert_ladybugdb_result(result: Any) -> Result:
    """Convert a LadybugDB/Kuzu query result to our Result type."""
    records = []
    if result is None:
        return Result(records=[])
    if hasattr(result, "get_as_df"):
        df = result.get_as_df()
        records = df.to_dict("records")
    elif hasattr(result, "has_next"):
        columns = result.get_column_names() if hasattr(result, "get_column_names") else []
        while result.has_next():
            row = result.get_next()
            record = {}
            for i, col in enumerate(columns):
                record[col] = row[i] if i < len(row) else None
            records.append(record)
    return Result(records=records)


# --- Factory ---


def create_adapter(config: DatabaseConfig) -> DatabaseAdapter:
    """Create the appropriate adapter for the given database config."""
    match config.adapter:
        case "bolt":
            return BoltAdapter(config)  # type: ignore[return-value]
        case "falkordb":
            return FalkorDBAdapter(config)  # type: ignore[return-value]
        case "falkordblite":
            return FalkorDBLiteAdapter(config)  # type: ignore[return-value]
        case "ladybugdb":
            return LadybugDBAdapter(config)  # type: ignore[return-value]
        case _:
            raise ValueError(f"Unknown adapter: {config.adapter}")

"""Intermediate tier benchmarks: multi-clause queries, aggregations, indexes."""

from __future__ import annotations

import logging
from typing import Any

from opencypher_benchmarking.benchmarks import BenchmarkDefinition, register_benchmark
from opencypher_benchmarking.data_generation import (
    generate_companies,
    generate_knows_edges,
    generate_persons,
    generate_works_at_edges,
)

logger = logging.getLogger(__name__)

PREFIX = "_inter"


def _setup_social_graph(adapter: Any, scale: int) -> None:
    """Create the social graph dataset for intermediate benchmarks."""
    persons = generate_persons(scale)
    companies = generate_companies(scale)
    knows = generate_knows_edges(len(persons), scale)
    works_at = generate_works_at_edges(len(persons), len(companies), scale)

    # Insert persons in batches
    for i in range(0, len(persons), 100):
        batch = persons[i : i + 100]
        adapter.execute(
            f"UNWIND $batch AS row CREATE (n:{PREFIX}_Person "
            f"{{name: row.name, age: row.age, city: row.city, "
            f"active: row.active, created: row.created}})",
            {"batch": batch},
        )

    # Insert companies in batches
    for i in range(0, len(companies), 50):
        batch = companies[i : i + 50]
        adapter.execute(
            f"UNWIND $batch AS row CREATE (n:{PREFIX}_Company "
            f"{{name: row.name, industry: row.industry, founded: row.founded}})",
            {"batch": batch},
        )

    # Insert KNOWS edges
    for i in range(0, len(knows), 100):
        batch = [{"src": f"person_{a}", "dst": f"person_{b}"} for a, b in knows[i : i + 100]]
        adapter.execute(
            f"UNWIND $batch AS row "
            f"MATCH (a:{PREFIX}_Person {{name: row.src}}), (b:{PREFIX}_Person {{name: row.dst}}) "
            f"CREATE (a)-[:{PREFIX}_KNOWS]->(b)",
            {"batch": batch},
        )

    # Insert WORKS_AT edges
    for i in range(0, len(works_at), 100):
        batch = [{"p": f"person_{p}", "c": f"company_{c}"} for p, c in works_at[i : i + 100]]
        adapter.execute(
            f"UNWIND $batch AS row "
            f"MATCH (a:{PREFIX}_Person {{name: row.p}}), (b:{PREFIX}_Company {{name: row.c}}) "
            f"CREATE (a)-[:{PREFIX}_WORKS_AT]->(b)",
            {"batch": batch},
        )


def _noop_setup(adapter: Any, scale: int) -> None:
    pass


def _cleanup_all(adapter: Any) -> None:
    for label in [f"{PREFIX}_Temp", f"{PREFIX}_Person", f"{PREFIX}_Company"]:
        try:
            adapter.execute(f"MATCH (n:{label}) DETACH DELETE n")
        except Exception:
            pass


# --- 1. index_creation ---


def _setup_for_index(adapter: Any, scale: int) -> None:
    _setup_social_graph(adapter, scale)


def _run_index_creation(adapter: Any) -> None:
    """Try standard CREATE INDEX syntax."""
    for prop in ["name", "age", "city"]:
        try:
            adapter.execute(f"CREATE INDEX IF NOT EXISTS FOR (n:{PREFIX}_Person) ON (n.{prop})")
        except Exception:
            # Some databases use different syntax; log and continue
            logger.debug(f"Standard index creation failed for {prop}, trying alternative")
            try:
                adapter.execute(f"CREATE INDEX ON :{PREFIX}_Person({prop})")
            except Exception:
                logger.debug(f"Alternative index creation also failed for {prop}")


register_benchmark(
    BenchmarkDefinition(
        name="index_creation",
        tier="intermediate",
        category="write",
        required_features={"clauses": {"CREATE"}},
        setup=_setup_for_index,
        run=_run_index_creation,
        teardown=_cleanup_all,
        description="Create standard indexes on Person.name, Person.age, Person.city",
    )
)

# --- 2. multi_hop_traversal ---

register_benchmark(
    BenchmarkDefinition(
        name="multi_hop_traversal",
        tier="intermediate",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_setup_social_graph,
        run=lambda a: a.execute_read(
            f"MATCH (a:{PREFIX}_Person)-[:{PREFIX}_KNOWS]->(b)-[:{PREFIX}_KNOWS]->(c) "
            f"RETURN c LIMIT 100"
        ),
        teardown=_cleanup_all,
        description="3-hop traversal on social graph",
    )
)

# --- 3. aggregate_group_by ---

register_benchmark(
    BenchmarkDefinition(
        name="aggregate_group_by",
        tier="intermediate",
        category="read",
        required_features={"clauses": {"MATCH", "WITH"}, "functions": {"count", "collect"}},
        setup=_setup_social_graph,
        run=lambda a: a.execute_read(
            f"MATCH (n:{PREFIX}_Person) "
            f"WITH n.city AS city, count(n) AS cnt, collect(n.name) AS names "
            f"RETURN city, cnt, names"
        ),
        teardown=_cleanup_all,
        description="Group by city, count and collect names",
    )
)

# --- 4. merge_node ---

register_benchmark(
    BenchmarkDefinition(
        name="merge_node",
        tier="intermediate",
        category="write",
        required_features={"clauses": {"MERGE"}},
        setup=_noop_setup,
        run=lambda a: a.execute(
            f"MERGE (n:{PREFIX}_Person {{name: $name}}) ON CREATE SET n.created = '2025-01-01'",
            {"name": "merge_test_person"},
        ),
        teardown=_cleanup_all,
        description="MERGE with ON CREATE SET",
    )
)

# --- 5. create_bulk_nodes ---

register_benchmark(
    BenchmarkDefinition(
        name="create_bulk_nodes",
        tier="intermediate",
        category="write",
        required_features={"clauses": {"CREATE", "UNWIND"}},
        setup=_noop_setup,
        run=lambda a: a.execute(
            f"UNWIND $batch AS row CREATE (n:{PREFIX}_Temp {{name: row.name, val: row.val}})",
            {"batch": [{"name": f"bulk_{i}", "val": i} for i in range(1000)]},
        ),
        teardown=_cleanup_all,
        description="UNWIND + CREATE to insert 1000 nodes in one query",
    )
)

# --- 6. pattern_filtering ---

register_benchmark(
    BenchmarkDefinition(
        name="pattern_filtering",
        tier="intermediate",
        category="read",
        required_features={"clauses": {"MATCH", "WHERE"}},
        setup=_setup_social_graph,
        run=lambda a: a.execute_read(
            f"MATCH (n:{PREFIX}_Person) "
            f"WHERE n.age > 30 AND n.city = 'NYC' OR n.active = true "
            f"RETURN n LIMIT 100"
        ),
        teardown=_cleanup_all,
        description="Multi-predicate WHERE filtering",
    )
)

# --- 7. optional_match ---

register_benchmark(
    BenchmarkDefinition(
        name="optional_match",
        tier="intermediate",
        category="read",
        required_features={"clauses": {"MATCH", "OPTIONAL MATCH"}},
        setup=_setup_social_graph,
        run=lambda a: a.execute_read(
            f"MATCH (p:{PREFIX}_Person) "
            f"OPTIONAL MATCH (p)-[:{PREFIX}_WORKS_AT]->(c:{PREFIX}_Company) "
            f"RETURN p.name, c.name LIMIT 100"
        ),
        teardown=_cleanup_all,
        description="Left-join style: people and optionally their employers",
    )
)

# --- 8. order_and_paginate ---

register_benchmark(
    BenchmarkDefinition(
        name="order_and_paginate",
        tier="intermediate",
        category="read",
        required_features={"clauses": {"MATCH", "ORDER BY"}},
        setup=_setup_social_graph,
        run=lambda a: a.execute_read(
            f"MATCH (n:{PREFIX}_Person) RETURN n.name, n.age ORDER BY n.age DESC SKIP 10 LIMIT 20"
        ),
        teardown=_cleanup_all,
        description="Paginated query with sorting",
    )
)

# --- 9. path_length_filter ---

register_benchmark(
    BenchmarkDefinition(
        name="path_length_filter",
        tier="intermediate",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_setup_social_graph,
        run=lambda a: a.execute_read(
            f"MATCH p=(a:{PREFIX}_Person)-[:{PREFIX}_KNOWS*1..3]->(b:{PREFIX}_Person) "
            f"WHERE length(p) = 3 RETURN p LIMIT 10"
        ),
        teardown=_cleanup_all,
        description="Variable-length path with length filter",
    )
)

# --- 10. update_bulk ---

register_benchmark(
    BenchmarkDefinition(
        name="update_bulk",
        tier="intermediate",
        category="write",
        required_features={"clauses": {"MATCH", "SET"}},
        setup=_setup_social_graph,
        run=lambda a: a.execute(f"MATCH (n:{PREFIX}_Person) SET n.updated = true"),
        teardown=_cleanup_all,
        description="Update a property on all nodes matching a label",
    )
)

# --- 11. delete_with_relationships ---


def _setup_temp_nodes(adapter: Any, scale: int) -> None:
    for i in range(100):
        adapter.execute(f"CREATE (n:{PREFIX}_Temp {{name: 'temp_{i}'}})")
    # Create some relationships between them
    for i in range(50):
        adapter.execute(
            f"MATCH (a:{PREFIX}_Temp {{name: 'temp_{i}'}}), "
            f"(b:{PREFIX}_Temp {{name: 'temp_{i + 50}'}}) "
            f"CREATE (a)-[:LINKED]->(b)"
        )


register_benchmark(
    BenchmarkDefinition(
        name="delete_with_relationships",
        tier="intermediate",
        category="write",
        required_features={"clauses": {"MATCH", "DELETE"}},
        setup=_setup_temp_nodes,
        run=lambda a: a.execute(f"MATCH (n:{PREFIX}_Temp) DETACH DELETE n"),
        teardown=_cleanup_all,
        description="MATCH (n:Temp) DETACH DELETE n",
    )
)

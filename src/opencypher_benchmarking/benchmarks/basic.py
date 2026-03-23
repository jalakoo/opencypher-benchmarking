"""Basic tier benchmarks: simple single-clause operations, no indexes."""

from __future__ import annotations

from typing import Any

from opencypher_benchmarking.benchmarks import BenchmarkDefinition, register_benchmark
from opencypher_benchmarking.data_generation import generate_persons

PREFIX = "_basic"


def _bulk_create_persons(adapter: Any, scale: int) -> None:
    """Insert Person nodes with basic label prefix."""
    persons = generate_persons(scale)
    for i in range(0, len(persons), 100):
        batch = persons[i : i + 100]
        adapter.execute(
            f"UNWIND $batch AS row CREATE (n:{PREFIX}_Person) SET n = row",
            {"batch": batch},
        )


def _noop_setup(adapter: Any, scale: int) -> None:
    pass


def _cleanup(adapter: Any) -> None:
    adapter.execute(f"MATCH (n:{PREFIX}_Person) DETACH DELETE n")


def _cleanup_rel(adapter: Any) -> None:
    adapter.execute(f"MATCH (n) WHERE n:{PREFIX}_Person OR n:{PREFIX}_Target DETACH DELETE n")


# --- 1. create_single_node ---

register_benchmark(
    BenchmarkDefinition(
        name="create_single_node",
        tier="basic",
        category="write",
        required_features={"clauses": {"CREATE"}},
        setup=_noop_setup,
        run=lambda a: a.execute(
            f"CREATE (n:{PREFIX}_Person {{name: 'bench_node', age: 30, city: 'NYC'}})"
        ),
        teardown=_cleanup,
        description="Create a single labeled node with 3 properties",
    )
)

# --- 2. create_single_relationship ---


def _setup_rel_nodes(adapter: Any, scale: int) -> None:
    adapter.execute(f"CREATE (a:{PREFIX}_Person {{name: 'rel_src'}})")
    adapter.execute(f"CREATE (b:{PREFIX}_Target {{name: 'rel_dst'}})")


register_benchmark(
    BenchmarkDefinition(
        name="create_single_relationship",
        tier="basic",
        category="write",
        required_features={"clauses": {"CREATE", "MATCH"}},
        setup=_setup_rel_nodes,
        run=lambda a: a.execute(
            f"MATCH (a:{PREFIX}_Person {{name: 'rel_src'}}), "
            f"(b:{PREFIX}_Target {{name: 'rel_dst'}}) "
            "CREATE (a)-[:KNOWS]->(b)"
        ),
        teardown=_cleanup_rel,
        description="Create a relationship between two existing nodes",
    )
)

# --- 3. match_all_nodes ---

register_benchmark(
    BenchmarkDefinition(
        name="match_all_nodes",
        tier="basic",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_bulk_create_persons,
        run=lambda a: a.execute_read(f"MATCH (n:{PREFIX}_Person) RETURN n"),
        teardown=_cleanup,
        description="MATCH (n) RETURN n on ~1000 nodes",
    )
)

# --- 4. match_by_label ---

register_benchmark(
    BenchmarkDefinition(
        name="match_by_label",
        tier="basic",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_bulk_create_persons,
        run=lambda a: a.execute_read(f"MATCH (n:{PREFIX}_Person) RETURN n"),
        teardown=_cleanup,
        description="MATCH (n:Person) RETURN n",
    )
)

# --- 5. match_by_property ---

register_benchmark(
    BenchmarkDefinition(
        name="match_by_property",
        tier="basic",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_bulk_create_persons,
        run=lambda a: a.execute_read(
            f"MATCH (n:{PREFIX}_Person {{name: $name}}) RETURN n",
            {"name": "person_42"},
        ),
        teardown=_cleanup,
        description="MATCH (n:Person {name: $name}) RETURN n with parameterized query",
    )
)

# --- 6. match_with_limit ---

register_benchmark(
    BenchmarkDefinition(
        name="match_with_limit",
        tier="basic",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_bulk_create_persons,
        run=lambda a: a.execute_read(f"MATCH (n:{PREFIX}_Person) RETURN n LIMIT 10"),
        teardown=_cleanup,
        description="MATCH (n) RETURN n LIMIT 10",
    )
)

# --- 7. delete_single_node ---


def _setup_delete_node(adapter: Any, scale: int) -> None:
    adapter.execute(f"CREATE (n:{PREFIX}_Person {{name: 'to_delete'}})")


register_benchmark(
    BenchmarkDefinition(
        name="delete_single_node",
        tier="basic",
        category="write",
        required_features={"clauses": {"MATCH", "DELETE"}},
        setup=_setup_delete_node,
        run=lambda a: a.execute(f"MATCH (n:{PREFIX}_Person {{name: 'to_delete'}}) DELETE n"),
        teardown=_cleanup,
        description="Match a node by property, delete it",
    )
)

# --- 8. set_property ---


def _setup_set_prop(adapter: Any, scale: int) -> None:
    adapter.execute(f"CREATE (n:{PREFIX}_Person {{name: 'set_target', age: 25}})")


register_benchmark(
    BenchmarkDefinition(
        name="set_property",
        tier="basic",
        category="write",
        required_features={"clauses": {"MATCH", "SET"}},
        setup=_setup_set_prop,
        run=lambda a: a.execute(f"MATCH (n:{PREFIX}_Person {{name: 'set_target'}}) SET n.age = 99"),
        teardown=_cleanup,
        description="Update a property on an existing node",
    )
)

# --- 9. remove_property ---


def _setup_remove_prop(adapter: Any, scale: int) -> None:
    adapter.execute(f"CREATE (n:{PREFIX}_Person {{name: 'remove_target', age: 25, city: 'NYC'}})")


register_benchmark(
    BenchmarkDefinition(
        name="remove_property",
        tier="basic",
        category="write",
        required_features={"clauses": {"MATCH", "REMOVE"}},
        setup=_setup_remove_prop,
        run=lambda a: a.execute(
            f"MATCH (n:{PREFIX}_Person {{name: 'remove_target'}}) REMOVE n.city"
        ),
        teardown=_cleanup,
        description="Remove a property from an existing node",
    )
)

# --- 10. count_nodes ---

register_benchmark(
    BenchmarkDefinition(
        name="count_nodes",
        tier="basic",
        category="read",
        required_features={"clauses": {"MATCH"}, "functions": {"count"}},
        setup=_bulk_create_persons,
        run=lambda a: a.execute_read(f"MATCH (n:{PREFIX}_Person) RETURN count(n)"),
        teardown=_cleanup,
        description="MATCH (n:Person) RETURN count(n)",
    )
)

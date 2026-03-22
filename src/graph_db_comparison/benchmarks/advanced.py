"""Advanced tier benchmarks: complex queries, concurrency, large-scale operations."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from graph_db_comparison.benchmarks import BenchmarkDefinition, register_benchmark
from graph_db_comparison.data_generation import (
    generate_companies,
    generate_knows_edges,
    generate_persons,
    generate_works_at_edges,
)

logger = logging.getLogger(__name__)

PREFIX = "_adv"


def _setup_large_graph(adapter: Any, scale: int) -> None:
    """Create a large social graph for advanced benchmarks."""
    # Use higher base counts for advanced tier
    large_scale = max(scale * 5, 5)
    persons = generate_persons(large_scale)
    companies = generate_companies(large_scale)
    knows = generate_knows_edges(len(persons), large_scale)
    works_at = generate_works_at_edges(len(persons), len(companies), large_scale)

    for i in range(0, len(persons), 200):
        batch = persons[i : i + 200]
        adapter.execute(
            f"UNWIND $batch AS row CREATE (n:{PREFIX}_Person) SET n = row",
            {"batch": batch},
        )

    for i in range(0, len(companies), 50):
        batch = companies[i : i + 50]
        adapter.execute(
            f"UNWIND $batch AS row CREATE (n:{PREFIX}_Company) SET n = row",
            {"batch": batch},
        )

    for i in range(0, len(knows), 200):
        batch = [{"src": f"person_{a}", "dst": f"person_{b}"} for a, b in knows[i : i + 200]]
        adapter.execute(
            f"UNWIND $batch AS row "
            f"MATCH (a:{PREFIX}_Person {{name: row.src}}), (b:{PREFIX}_Person {{name: row.dst}}) "
            f"CREATE (a)-[:{PREFIX}_KNOWS]->(b)",
            {"batch": batch},
        )

    for i in range(0, len(works_at), 200):
        batch = [{"p": f"person_{p}", "c": f"company_{c}"} for p, c in works_at[i : i + 200]]
        adapter.execute(
            f"UNWIND $batch AS row "
            f"MATCH (a:{PREFIX}_Person {{name: row.p}}), (b:{PREFIX}_Company {{name: row.c}}) "
            f"CREATE (a)-[:{PREFIX}_WORKS_AT]->(b)",
            {"batch": batch},
        )


def _noop_setup(adapter: Any, scale: int) -> None:
    pass


def _cleanup_all(adapter: Any) -> None:
    adapter.execute(
        f"MATCH (n) WHERE n:{PREFIX}_Person OR n:{PREFIX}_Company OR n:{PREFIX}_Temp "
        f"DETACH DELETE n"
    )


# --- 1. shortest_path ---

register_benchmark(
    BenchmarkDefinition(
        name="shortest_path",
        tier="advanced",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_setup_large_graph,
        run=lambda a: a.execute_read(
            f"MATCH (a:{PREFIX}_Person {{name: $src}}), (b:{PREFIX}_Person {{name: $dst}}), "
            f"p = shortestPath((a)-[*..10]-(b)) RETURN p",
            {"src": "person_0", "dst": "person_100"},
        ),
        teardown=_cleanup_all,
        description="shortestPath between two nodes",
    )
)

# --- 2. recommendation_query ---

register_benchmark(
    BenchmarkDefinition(
        name="recommendation_query",
        tier="advanced",
        category="read",
        required_features={
            "clauses": {"MATCH", "WITH", "ORDER BY"},
            "functions": {"count", "collect"},
        },
        setup=_setup_large_graph,
        run=lambda a: a.execute_read(
            f"MATCH (a:{PREFIX}_Person {{name: $name}})-[:{PREFIX}_KNOWS]->(b)"
            f"-[:{PREFIX}_KNOWS]->(c) "
            f"WHERE NOT (a)-[:{PREFIX}_KNOWS]->(c) AND a <> c "
            f"WITH c, count(b) AS mutuals "
            f"ORDER BY mutuals DESC LIMIT 10 "
            f"RETURN c.name, mutuals",
            {"name": "person_0"},
        ),
        teardown=_cleanup_all,
        description="Friend-of-friend recommendations ranked by mutual connections",
    )
)

# --- 3. graph_projection ---

register_benchmark(
    BenchmarkDefinition(
        name="graph_projection",
        tier="advanced",
        category="read",
        required_features={"clauses": {"MATCH", "WITH", "UNWIND"}, "functions": {"collect"}},
        setup=_setup_large_graph,
        run=lambda a: a.execute_read(
            f"MATCH (a:{PREFIX}_Person)-[:{PREFIX}_KNOWS]->(b:{PREFIX}_Person) "
            f"WITH a.name AS src, collect(b.name) AS neighbors "
            f"RETURN src, neighbors LIMIT 100"
        ),
        teardown=_cleanup_all,
        description="Build an adjacency-list projection in Cypher",
    )
)

# --- 4. concurrent_writes ---


def _run_concurrent_writes(adapter: Any) -> None:
    """Run parallel write transactions. Uses adapter's config for concurrency."""
    concurrency = getattr(adapter, "_concurrency", 8)

    def _write_task(task_id: int) -> None:
        adapter.execute(
            f"CREATE (n:{PREFIX}_Temp {{name: $name, task: $task}})",
            {"name": f"concurrent_{task_id}", "task": task_id},
        )

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_write_task, i) for i in range(concurrency * 10)]
        for f in futures:
            f.result()  # raise if any failed


register_benchmark(
    BenchmarkDefinition(
        name="concurrent_writes",
        tier="advanced",
        category="write",
        required_features={"clauses": {"CREATE"}},
        setup=_noop_setup,
        run=_run_concurrent_writes,
        teardown=_cleanup_all,
        description="Parallel write transactions via ThreadPoolExecutor",
    )
)

# --- 5. mixed_read_write ---


def _setup_mixed(adapter: Any, scale: int) -> None:
    adapter.execute(f"CREATE (n:{PREFIX}_Person {{name: 'counter_node', counter: 0}})")


def _run_mixed(adapter: Any) -> None:
    adapter.execute_in_transaction(
        [
            (f"MATCH (n:{PREFIX}_Person {{name: 'counter_node'}}) RETURN n.counter AS c", None),
            (
                f"CREATE (m:{PREFIX}_Temp {{name: 'mixed_child', ts: $ts}})",
                {"ts": str(time.time_ns())},
            ),
            (
                f"MATCH (n:{PREFIX}_Person {{name: 'counter_node'}}) SET n.counter = n.counter + 1",
                None,
            ),
        ]
    )


register_benchmark(
    BenchmarkDefinition(
        name="mixed_read_write",
        tier="advanced",
        category="mixed",
        required_features={"clauses": {"MATCH", "CREATE", "SET"}},
        setup=_setup_mixed,
        run=_run_mixed,
        teardown=_cleanup_all,
        description="Transaction: read, create related node, update counter",
    )
)

# --- 6. large_traversal ---

register_benchmark(
    BenchmarkDefinition(
        name="large_traversal",
        tier="advanced",
        category="read",
        required_features={"clauses": {"MATCH"}},
        setup=_setup_large_graph,
        run=lambda a: a.execute_read(
            f"MATCH p=(a:{PREFIX}_Person)-[:{PREFIX}_KNOWS*1..4]->(b:{PREFIX}_Person) "
            f"RETURN count(p)"
        ),
        teardown=_cleanup_all,
        description="Variable-length path traversal across large graph",
    )
)

# --- 7. complex_aggregation ---

register_benchmark(
    BenchmarkDefinition(
        name="complex_aggregation",
        tier="advanced",
        category="read",
        required_features={
            "clauses": {"MATCH", "WITH", "UNWIND"},
            "functions": {"count", "avg", "sum", "collect"},
        },
        setup=_setup_large_graph,
        run=lambda a: a.execute_read(
            f"MATCH (n:{PREFIX}_Person) "
            f"WITH n.city AS city, count(n) AS cnt, avg(n.age) AS avg_age, "
            f"collect(n.name) AS names "
            f"UNWIND names AS name "
            f"RETURN city, cnt, avg_age, count(name) AS total"
        ),
        teardown=_cleanup_all,
        description="Multi-stage aggregation pipeline",
    )
)

# --- 8. text_search ---

register_benchmark(
    BenchmarkDefinition(
        name="text_search",
        tier="advanced",
        category="read",
        required_features={"clauses": {"MATCH", "WHERE"}, "operators": {"STARTS WITH", "CONTAINS"}},
        setup=_setup_large_graph,
        run=lambda a: a.execute_read(
            f"MATCH (n:{PREFIX}_Person) "
            f"WHERE n.name STARTS WITH 'person_1' OR n.city CONTAINS 'on' "
            f"RETURN n.name, n.city LIMIT 100"
        ),
        teardown=_cleanup_all,
        description="String-predicate filtering on large dataset",
    )
)

# --- 9. temporal_queries ---

register_benchmark(
    BenchmarkDefinition(
        name="temporal_queries",
        tier="advanced",
        category="read",
        required_features={"clauses": {"MATCH", "WHERE", "ORDER BY"}},
        setup=_setup_large_graph,
        run=lambda a: a.execute_read(
            f"MATCH (n:{PREFIX}_Person) "
            f"WHERE n.created >= '2025-06-01' "
            f"RETURN n.name, n.created ORDER BY n.created DESC LIMIT 50"
        ),
        teardown=_cleanup_all,
        description="Filter and sort by temporal properties",
    )
)

# --- 10. write_throughput ---


def _run_write_throughput(adapter: Any) -> None:
    """Sustained sequential writes for ~1 second, measuring ops."""
    end_time = time.perf_counter() + 1.0  # 1 second burst (shorter for unit tests)
    count = 0
    while time.perf_counter() < end_time:
        adapter.execute(
            f"CREATE (n:{PREFIX}_Temp {{name: $name, seq: $seq}})",
            {"name": f"throughput_{count}", "seq": count},
        )
        count += 1


register_benchmark(
    BenchmarkDefinition(
        name="write_throughput",
        tier="advanced",
        category="write",
        required_features={"clauses": {"CREATE"}},
        setup=_noop_setup,
        run=_run_write_throughput,
        teardown=_cleanup_all,
        description="Sustained sequential writes, measure ops/sec",
    )
)

# Graph Database Performance Comparison Tool

A Python-based benchmarking tool that compares six graph databases using openCypher queries across basic, intermediate, and advanced workloads.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration Reference](#configuration-reference)
- [Database Categories](#database-categories)
- [Benchmark Descriptions](#benchmark-descriptions)
- [Test Datasets](#test-datasets)
- [Warm vs Cold Benchmarking](#warm-vs-cold-benchmarking)
- [CLI Reference](#cli-reference)
- [Report Guide](#report-guide)
- [Running Tests](#running-tests)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

This tool benchmarks **Neo4j**, **Memgraph**, **ArcadeDB**, **FalkorDB**, **FalkorDB Lite**, and **LadybugDB** using a uniform set of Cypher queries. It uses the `opencypher-compliance` package to discover which features each database supports, then runs only eligible benchmarks. Results are output as a self-contained HTML report with radar charts, per-tier comparison tables, and warm-vs-cold analysis.

## Features

- **6 databases**: 4 server-mode (Docker) + 2 embedded (in-process)
- **31 benchmarks** across 3 tiers (basic, intermediate, advanced)
- **Compliance gating**: automatically skips benchmarks requiring unsupported features
- **Warm + cold** measurement for every benchmark
- **HTML report**: self-contained with radar chart, sortable tables, bar charts, expandable details
- **JSON export**: raw results for programmatic analysis
- **Compliance caching**: avoids re-running the full compliance suite on every run
- **Resilient**: one database failure doesn't stop the others

## Prerequisites

- Python >= 3.12
- Docker (for server-mode databases)
- pip

## Quick Start

### 1. Start databases with Docker

```bash
docker compose up -d
```

This starts Neo4j, Memgraph, ArcadeDB, and FalkorDB with ports matching the sample config. No volume mounts — containers are ephemeral.

Embedded databases (FalkorDB Lite, LadybugDB) require no Docker — they run in-process as Python packages.

### 2. Configure

Copy and edit the sample config:

```bash
cp sample_config.yaml config.yaml
# Edit config.yaml with your database credentials/ports
```

### 3. Run benchmarks

The easiest way is `run_bench.sh`, which auto-creates a virtual environment and installs all dependencies on first run:

```bash
# Full run (all databases, all tiers)
./run_bench.sh -c config.yaml -v

# Single database, single tier
./run_bench.sh -c config.yaml -d neo4j -t basic -v

# Check connectivity first
./run_bench.sh -c config.yaml --check

# Compliance only (no benchmarks)
./run_bench.sh -c config.yaml --compliance-only

# Regenerate HTML report from existing results (no benchmarks rerun)
./run_bench.sh --from-json reports/results.json
```

All arguments are forwarded to `graph-db-bench`. If you prefer to manage the environment yourself:

```bash
pip install -e ".[all]"       # or: .[server], .[embedded]
graph-db-bench -c config.yaml -v

# Regenerate HTML from existing JSON results
graph-db-bench --from-json reports/results.json
```

### 4. View report

Open `./reports/report.html` in a browser. Raw data is in `./reports/results.json`.

## Configuration Reference

See `sample_config.yaml` for a complete example. Key sections:

```yaml
databases:
  neo4j:
    adapter: bolt          # bolt | falkordb | falkordblite | ladybugdb
    host: localhost
    port: 7687
    auth:
      username: neo4j
      password: neo4j_pass
    enabled: true

  falkordb_lite:
    adapter: falkordblite
    db_path: /tmp/falkordb_bench.db
    graph_name: benchmark  # required by FalkorDB (default: "benchmark")
    enabled: true

benchmark:
  iterations: 5
  warmup_iterations: 2
  timeout_seconds: 30
  dataset_scale: 1         # 1=small (~1K nodes), 10=large (~10K nodes)
  concurrency: 8           # threads for concurrent_writes benchmark

output:
  report_path: ./reports/report.html
  raw_json_path: ./reports/results.json
```

**Validation rules:**
- At least one database must be `enabled: true`
- Server adapters (`bolt`, `falkordb`) require `host` and `port` (1–65535)
- Embedded adapters (`falkordblite`, `ladybugdb`) require `db_path`
- `iterations` >= 1, `warmup_iterations` >= 0, `concurrency` >= 1

## Database Categories

| Database | Mode | Adapter | Protocol |
|---|---|---|---|
| Neo4j | Server (Docker) | `bolt` | Bolt |
| Memgraph | Server (Docker) | `bolt` | Bolt |
| ArcadeDB | Server (Docker) | `bolt` | Bolt |
| FalkorDB | Server (Docker) | `falkordb` | Redis |
| FalkorDB Lite | Embedded | `falkordblite` | In-process |
| LadybugDB | Embedded | `ladybugdb` | In-process |

**Server-mode** databases run in Docker containers. The tool connects over the network.

**Embedded** databases run in-process with zero network overhead. The report tags each database as `[server]` or `[embedded]` so latency comparisons can be interpreted in context.

FalkorDB and FalkorDB Lite share the same query engine but differ in deployment model. The comparison reveals the overhead of server deployment.

## Benchmark Descriptions

### Basic Tier (10 benchmarks)

Simple single-clause operations with no indexes. Tests raw throughput.

| Benchmark | Category | Description |
|---|---|---|
| `create_single_node` | write | Create a labeled node with 3 properties |
| `create_single_relationship` | write | Create a relationship between two nodes |
| `match_all_nodes` | read | Match all nodes (~1000) |
| `match_by_label` | read | Match by label |
| `match_by_property` | read | Match by property (parameterized) |
| `match_with_limit` | read | Match with LIMIT 10 |
| `delete_single_node` | write | Delete a node by property |
| `set_property` | write | Update a property |
| `remove_property` | write | Remove a property |
| `count_nodes` | read | Count nodes with count() |

### Intermediate Tier (11 benchmarks)

Multi-clause queries with indexes, aggregations, and moderate data volumes.

| Benchmark | Category | Description |
|---|---|---|
| `index_creation` | write | Create indexes on Person properties |
| `multi_hop_traversal` | read | 3-hop traversal on social graph |
| `aggregate_group_by` | read | Group by city, count and collect |
| `merge_node` | write | MERGE with ON CREATE SET |
| `create_bulk_nodes` | write | UNWIND + CREATE 1000 nodes |
| `pattern_filtering` | read | Multi-predicate WHERE filtering |
| `optional_match` | read | OPTIONAL MATCH (left join) |
| `order_and_paginate` | read | ORDER BY + SKIP + LIMIT |
| `path_length_filter` | read | Variable-length paths with length filter |
| `update_bulk` | write | Update property on all matching nodes |
| `delete_with_relationships` | write | DETACH DELETE |

### Advanced Tier (10 benchmarks)

Complex queries simulating real-world analytical and transactional patterns.

| Benchmark | Category | Description |
|---|---|---|
| `shortest_path` | read | shortestPath between two nodes |
| `recommendation_query` | read | Friend-of-friend recommendations |
| `graph_projection` | read | Adjacency-list projection |
| `concurrent_writes` | write | Parallel writes via ThreadPoolExecutor |
| `mixed_read_write` | mixed | Read + create + update in one transaction |
| `large_traversal` | read | Variable-length path on large graph |
| `complex_aggregation` | read | Multi-stage aggregation pipeline |
| `text_search` | read | STARTS WITH / CONTAINS filtering |
| `temporal_queries` | read | Filter and sort by date properties |
| `write_throughput` | write | Sustained sequential writes (ops/sec) |

## Test Datasets

Each benchmark generates its own synthetic data via deterministic random seeds, so results are reproducible across runs. Data is generated independently per database — there is no shared pre-loaded dataset. Each benchmark follows a **setup → run → teardown** cycle: setup inserts data, timed iterations execute queries against it, and teardown deletes it. Between tiers, the entire graph is wiped with `MATCH (n) DETACH DELETE n`.

### Node Types

| Node Label | Properties | Count per scale unit |
|---|---|---|
| Person | `name`, `age` (18–80), `city` (8 cities), `active` (bool), `created` (date) | 1,000 |
| Company | `name`, `industry` (6 industries), `founded` (1950–2025) | 50 |

### Relationship Types

| Relationship | From → To | Count per scale unit | Notes |
|---|---|---|---|
| KNOWS | Person → Person | 5,000 | No self-loops, unique edges |
| WORKS_AT | Person → Company | 1,000 | Random pairing |

### Dataset Scale by Tier

The `dataset_scale` config option (default: 1) multiplies all counts. The advanced tier additionally applies a 5x multiplier on top of the configured scale.

| Tier | Scale multiplier | Persons | Companies | KNOWS edges | WORKS_AT edges |
|---|---|---|---|---|---|
| Basic | 1x | 1,000 | — | — | — |
| Intermediate | 1x | 1,000 | 50 | 5,000 | 1,000 |
| Advanced | 5x | 5,000 | 250 | 25,000 | 5,000 |

### Label Prefixes

Each tier uses a unique label prefix to avoid collisions between tiers: `_basic_`, `_inter_`, `_adv_`. For example, basic tier Person nodes are labeled `_basic_Person`.

### Deterministic Generation

All data generators use a fixed random seed (`seed=42`, with offsets per generator). This means every database receives the same logical dataset for each benchmark, enabling fair comparison despite data being inserted independently.

## Warm vs Cold Benchmarking

Every benchmark runs both a **cold** variant (first execution, no cache warmup) and a **warm** variant (after warmup iterations). The report includes a dedicated comparison section showing the cold/warm ratio for each benchmark.

For server-mode databases, "cold" means no prior query activity on the dataset. For embedded databases, the adapter is closed and reopened (clearing in-memory caches while preserving on-disk data).

## CLI Reference

```
Usage: graph-db-bench [OPTIONS]

Options:
  -c, --config PATH          Path to config.yaml [default: ./config.yaml]
  -t, --tier TEXT             Run specific tier(s): basic, intermediate, advanced
  -d, --database TEXT         Run specific database(s) by config name
  -b, --benchmark TEXT        Run specific benchmark(s) by name
  --skip-compliance           Skip compliance check, assume all features supported
  --compliance-only           Only run compliance checks, skip benchmarks
  --force-compliance          Force fresh compliance run, ignoring cache
  --compliance-ttl SECONDS    Compliance cache TTL [default: 86400]
  --check                     Ping databases and report status, then exit
  --from-json PATH            Regenerate HTML report from existing results.json, then exit
  --no-report                 Skip HTML report, output JSON only
  --output-dir PATH           Output directory [default: ./reports]
  -v, --verbose               Verbose logging
  --version                   Show version and exit
  --help                      Show help and exit
```

## Report Guide

The HTML report includes:

1. **Executive Summary** — narrative findings, radar chart, tier winner badges
2. **Compliance Matrix** — feature support per database (checkmarks/crosses)
3. **Warm vs Cold Comparison** — cold/warm latency ratios
4. **Benchmark Results by Tier** — side-by-side tables with bar charts
5. **Read vs Write Breakdown** — separate performance tables
6. **Detailed Results** — expandable per-database iteration data
7. **Skipped Benchmarks** — what was skipped and why
8. **Environment Info** — config (passwords redacted), versions

Tables are sortable by clicking column headers. Databases are tagged `[server]` or `[embedded]` throughout.

## Running Tests

```bash
# Using the test script
./run_tests.sh

# Or manually
pip install -e ".[dev]"
ruff check src/ tests/
ruff format --check src/ tests/
pytest tests/ -v
```

## Troubleshooting

**Port conflicts:** If Docker containers fail to start, check for existing services on ports 7687, 7688, 7689, 6379, 7474, 2480.

**ArcadeDB connection issues:** ArcadeDB requires a pre-created database. The docker-compose.yaml uses `-Darcadedb.server.defaultDatabases=benchmark[root]` to create one automatically. If connecting manually, ensure the Bolt plugin is enabled.

**FalkorDB graph name:** FalkorDB requires a graph name (no server-side default). Set `graph_name` in config (default: `"benchmark"`).

**LadybugDB schema errors:** LadybugDB requires explicit table definitions before inserting data. The adapter's `setup_schema()` handles this automatically.

**Compliance takes too long:** Use `--skip-compliance` for development. Compliance results are cached for 24 hours by default (override with `--compliance-ttl`).

**Embedded database errors:** Ensure `db_path` directories are writable. The tool wipes and recreates embedded DB paths on each run.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests first (TDD)
4. Implement the feature
5. Run `./run_tests.sh` to verify
6. Submit a pull request

## License

MIT License. See [LICENSE](LICENSE) for details.

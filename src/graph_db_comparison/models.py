from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DatabaseConfig:
    name: str
    adapter: str  # "bolt" | "falkordb" | "falkordblite" | "ladybugdb"
    enabled: bool
    host: str | None = None
    port: int | None = None
    auth: dict[str, str] | None = None
    db_path: str | None = None
    graph_name: str = "benchmark"
    mode: str = ""  # "server" | "embedded" — derived from adapter

    def __post_init__(self) -> None:
        if not self.mode:
            self.mode = "server" if self.adapter in ("bolt", "falkordb") else "embedded"


@dataclass
class BenchmarkConfig:
    iterations: int = 5
    warmup_iterations: int = 2
    timeout_seconds: int = 30
    dataset_scale: int = 1
    concurrency: int = 8


@dataclass
class OutputConfig:
    report_path: str = "./reports/report.html"
    raw_json_path: str = "./reports/results.json"


@dataclass
class AppConfig:
    databases: dict[str, DatabaseConfig] = field(default_factory=dict)
    benchmark: BenchmarkConfig = field(default_factory=BenchmarkConfig)
    output: OutputConfig = field(default_factory=OutputConfig)


@dataclass
class FeatureSupportMap:
    clauses: set[str] = field(default_factory=set)
    functions: set[str] = field(default_factory=set)
    operators: set[str] = field(default_factory=set)
    data_types: set[str] = field(default_factory=set)
    pass_rate: float = 0.0


@dataclass
class BenchmarkResult:
    benchmark_name: str
    tier: str  # "basic" | "intermediate" | "advanced"
    category: str  # "read" | "write" | "mixed"
    database_name: str
    status: str  # "pass" | "skip" | "error"
    cold_latency_ns: int | None = None
    warm_latencies_ns: list[int] = field(default_factory=list)
    median_ns: float | None = None
    mean_ns: float | None = None
    p95_ns: float | None = None
    p99_ns: float | None = None
    min_ns: int | None = None
    max_ns: int | None = None
    std_dev_ns: float | None = None
    errors: list[str] = field(default_factory=list)
    skipped_reason: str | None = None


@dataclass
class DatabaseReport:
    name: str
    mode: str  # "server" | "embedded"
    adapter: str
    compliance: FeatureSupportMap | None = None
    compliance_error: str | None = None
    results: list[BenchmarkResult] = field(default_factory=list)


@dataclass
class FullReport:
    timestamp: str
    version: str
    config: AppConfig
    databases: list[DatabaseReport] = field(default_factory=list)

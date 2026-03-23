"""Report generation: data aggregation, HTML rendering, JSON output."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

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

logger = logging.getLogger(__name__)

DB_COLORS = [
    "#4285F4",  # blue
    "#EA4335",  # red
    "#34A853",  # green
    "#FBBC04",  # yellow
    "#9C27B0",  # purple
    "#FF6D00",  # orange
]


def _safe_pass_rate(value: Any) -> float:
    """Convert pass_rate to a 0-100 float, handling both float and string formats."""
    if isinstance(value, str):
        cleaned = value.strip().rstrip("%")
        try:
            rate = float(cleaned)
            # Already in 0-100 range if it came as percentage string
            return min(rate, 100.0)
        except ValueError:
            return 0.0
    try:
        rate = float(value)
        # If 0-1 range, scale to 0-100
        if rate <= 1.0:
            return rate * 100.0
        return min(rate, 100.0)
    except (TypeError, ValueError):
        return 0.0


def aggregate_report_data(report: FullReport) -> dict[str, Any]:
    """Transform FullReport into template-friendly dicts."""
    databases = _build_database_summaries(report)
    tier_tables = _build_tier_tables(report)
    tier_winners = _compute_tier_winners(tier_tables)
    warm_vs_cold = _build_warm_vs_cold(report)
    cold_warm_summary = _build_cold_warm_summary(report)
    skipped = _build_skipped_list(report)
    narrative = _build_narrative(tier_winners, report)
    scorecards = _compute_scorecards(report, tier_winners)

    return {
        "timestamp": report.timestamp,
        "version": report.version,
        "databases": databases,
        "tier_tables": tier_tables,
        "tier_winners": tier_winners,
        "warm_vs_cold": warm_vs_cold,
        "cold_warm_summary": cold_warm_summary,
        "skipped": skipped,
        "narrative": narrative,
        "scorecards": scorecards,
        "duration_seconds": report.duration_seconds,
        "dataset_profile": _build_dataset_profile(report.config.benchmark.dataset_scale),
        "db_colors": {
            db.name: DB_COLORS[i % len(DB_COLORS)] for i, db in enumerate(report.databases)
        },
        "config_redacted": _redact_config(report.config),
        "detail_results": _build_detail_results(report),
    }


def generate_html_report(report: FullReport, output_path: str) -> None:
    """Render the full HTML report to a file."""
    template_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)
    template = env.get_template("template.html")
    data = aggregate_report_data(report)
    html = template.render(**data)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    logger.info(f"HTML report written to {output_path}")


def generate_json_report(report: FullReport, output_path: str) -> None:
    """Write raw results as JSON with passwords redacted."""
    data = _serialize_report(report)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, default=str))
    logger.info(f"JSON report written to {output_path}")


def load_report_from_json(json_path: str) -> FullReport:
    """Load a FullReport from a previously generated results.json."""
    data = json.loads(Path(json_path).read_text())

    # Rebuild config
    db_configs = {}
    for db_name, db_data in data.get("config", {}).get("databases", {}).items():
        db_configs[db_name] = DatabaseConfig(
            name=db_data.get("name", db_name),
            adapter=db_data.get("adapter", "bolt"),
            enabled=db_data.get("enabled", True),
            host=db_data.get("host"),
            port=db_data.get("port"),
            auth=db_data.get("auth"),
            db_path=db_data.get("db_path"),
            graph_name=db_data.get("graph_name", "benchmark"),
            mode=db_data.get("mode", ""),
        )
    bench_cfg = data.get("config", {}).get("benchmark", {})
    out_cfg = data.get("config", {}).get("output", {})
    config = AppConfig(
        databases=db_configs,
        benchmark=BenchmarkConfig(
            iterations=bench_cfg.get("iterations", 5),
            warmup_iterations=bench_cfg.get("warmup_iterations", 2),
            timeout_seconds=bench_cfg.get("timeout_seconds", 30),
            dataset_scale=bench_cfg.get("dataset_scale", 1),
            concurrency=bench_cfg.get("concurrency", 8),
        ),
        output=OutputConfig(
            report_path=out_cfg.get("report_path", "./reports/report.html"),
            raw_json_path=out_cfg.get("raw_json_path", "./reports/results.json"),
        ),
    )

    # Rebuild database reports
    db_reports = []
    for db_data in data.get("databases", []):
        compliance = None
        if db_data.get("compliance"):
            c = db_data["compliance"]
            compliance = FeatureSupportMap(
                clauses=set(c.get("clauses", [])),
                functions=set(c.get("functions", [])),
                operators=set(c.get("operators", [])),
                data_types=set(c.get("data_types", [])),
                pass_rate=c.get("pass_rate", 0.0),
            )

        results = []
        for r in db_data.get("results", []):
            results.append(
                BenchmarkResult(
                    benchmark_name=r["benchmark_name"],
                    tier=r["tier"],
                    category=r["category"],
                    database_name=r["database_name"],
                    status=r["status"],
                    cold_latency_ns=r.get("cold_latency_ns"),
                    warm_latencies_ns=r.get("warm_latencies_ns", []),
                    median_ns=r.get("median_ns"),
                    mean_ns=r.get("mean_ns"),
                    p95_ns=r.get("p95_ns"),
                    p99_ns=r.get("p99_ns"),
                    min_ns=r.get("min_ns"),
                    max_ns=r.get("max_ns"),
                    std_dev_ns=r.get("std_dev_ns"),
                    errors=r.get("errors", []),
                    skipped_reason=r.get("skipped_reason"),
                )
            )

        db_reports.append(
            DatabaseReport(
                name=db_data["name"],
                mode=db_data["mode"],
                adapter=db_data["adapter"],
                compliance=compliance,
                compliance_error=db_data.get("compliance_error"),
                results=results,
            )
        )

    return FullReport(
        timestamp=data["timestamp"],
        version=data["version"],
        config=config,
        databases=db_reports,
        duration_seconds=data.get("duration_seconds"),
    )


# --- Internal helpers ---


def _build_dataset_profile(scale: int) -> dict[str, Any]:
    """Compute dataset sizes per tier based on the configured scale."""
    adv_scale = max(scale * 5, 5)
    return {
        "scale": scale,
        "tiers": {
            "basic": {
                "persons": 1000 * scale,
            },
            "intermediate": {
                "persons": 1000 * scale,
                "companies": 50 * scale,
                "knows_edges": 5000 * scale,
                "works_at_edges": 1000 * scale,
            },
            "advanced": {
                "persons": 1000 * adv_scale,
                "companies": 50 * adv_scale,
                "knows_edges": 5000 * adv_scale,
                "works_at_edges": 1000 * adv_scale,
            },
        },
    }


def _build_database_summaries(report: FullReport) -> list[dict[str, Any]]:
    """Build per-database summary dicts."""
    summaries = []
    for db in report.databases:
        passed = sum(1 for r in db.results if r.status == "pass")
        total = len(db.results)
        summaries.append(
            {
                "name": db.name,
                "mode": db.mode,
                "adapter": db.adapter,
                "benchmarks_passed": passed,
                "benchmarks_total": total,
            }
        )
    return summaries


def _build_tier_tables(report: FullReport) -> dict[str, list[dict[str, Any]]]:
    """Build per-tier comparison tables."""
    tiers: dict[str, dict[str, dict[str, Any]]] = {}

    for db in report.databases:
        for result in db.results:
            tier = result.tier
            if tier not in tiers:
                tiers[tier] = {}
            bench_name = result.benchmark_name
            if bench_name not in tiers[tier]:
                tiers[tier][bench_name] = {"benchmark": bench_name, "category": result.category}
            tiers[tier][bench_name][f"{db.name}_median"] = result.median_ns
            tiers[tier][bench_name][f"{db.name}_p95"] = result.p95_ns
            tiers[tier][bench_name][f"{db.name}_status"] = result.status

    return {tier: list(rows.values()) for tier, rows in tiers.items()}


def _compute_tier_winners(tier_tables: dict[str, list[dict]]) -> dict[str, str]:
    """Find the database with lowest total median per tier."""
    winners = {}
    for tier, rows in tier_tables.items():
        db_totals: dict[str, list[float]] = {}
        for row in rows:
            for key, val in row.items():
                if key.endswith("_median") and val is not None:
                    db_name = key.replace("_median", "")
                    db_totals.setdefault(db_name, []).append(val)
        if db_totals:
            best_db = min(db_totals, key=lambda d: sum(db_totals[d]) / len(db_totals[d]))
            winners[tier] = best_db
    return winners


def _compute_scorecards(report: FullReport, tier_winners: dict[str, str]) -> list[dict[str, Any]]:
    """Compute ranked scorecard data for each database."""
    cards = []
    for db in report.databases:
        passed = sum(1 for r in db.results if r.status == "pass")
        total = len(db.results)
        medians = [r.median_ns for r in db.results if r.status == "pass" and r.median_ns]
        avg_median_ns = sum(medians) / len(medians) if medians else float("inf")

        # Find best tier: the tier where this DB has the lowest average median
        tier_avgs: dict[str, float] = {}
        for r in db.results:
            if r.status == "pass" and r.median_ns:
                tier_avgs.setdefault(r.tier, []).append(r.median_ns)
        best_tier = None
        if tier_avgs:
            best_tier = min(tier_avgs, key=lambda t: sum(tier_avgs[t]) / len(tier_avgs[t]))

        # Check if this DB is a tier winner
        winner_tiers = [t for t, w in tier_winners.items() if w == db.name]

        cards.append(
            {
                "name": db.name,
                "mode": db.mode,
                "avg_median_ms": (
                    round(avg_median_ns / 1_000_000, 2) if avg_median_ns != float("inf") else None
                ),
                "benchmarks_passed": passed,
                "benchmarks_total": total,
                "best_tier": best_tier,
                "winner_tiers": winner_tiers,
                "rank": 0,  # filled below
            }
        )

    # Rank by average median (lower is better)
    cards.sort(key=lambda c: c["avg_median_ms"] if c["avg_median_ms"] is not None else float("inf"))
    for i, card in enumerate(cards):
        card["rank"] = i + 1

    return cards


def _build_warm_vs_cold(report: FullReport) -> list[dict[str, Any]]:
    """Build warm vs cold comparison entries."""
    entries = []
    for db in report.databases:
        for r in db.results:
            if r.status != "pass" or r.cold_latency_ns is None or r.median_ns is None:
                continue
            ratio = r.cold_latency_ns / r.median_ns if r.median_ns > 0 else 0
            entries.append(
                {
                    "database": db.name,
                    "benchmark": r.benchmark_name,
                    "cold_ns": r.cold_latency_ns,
                    "warm_median_ns": r.median_ns,
                    "ratio": round(ratio, 2),
                }
            )
    return entries


def _build_cold_warm_summary(report: FullReport) -> list[dict[str, Any]]:
    """Build one-row-per-database cold/warm summary."""
    summary = []
    for db in report.databases:
        ratios = []
        worst_bench = None
        worst_ratio = 0.0
        for r in db.results:
            if r.status != "pass" or r.cold_latency_ns is None or r.median_ns is None:
                continue
            if r.median_ns <= 0:
                continue
            ratio = r.cold_latency_ns / r.median_ns
            ratios.append(ratio)
            if ratio > worst_ratio:
                worst_ratio = ratio
                worst_bench = r.benchmark_name
        if ratios:
            summary.append(
                {
                    "name": db.name,
                    "avg_ratio": round(sum(ratios) / len(ratios), 2),
                    "max_ratio": round(max(ratios), 2),
                    "worst_benchmark": worst_bench,
                }
            )
    return summary


def _build_skipped_list(report: FullReport) -> list[dict[str, str]]:
    """Build list of skipped benchmarks."""
    skipped = []
    for db in report.databases:
        for r in db.results:
            if r.status == "skip":
                skipped.append(
                    {
                        "database": db.name,
                        "benchmark": r.benchmark_name,
                        "reason": r.skipped_reason or "Unknown",
                    }
                )
    return skipped


def _build_narrative(winners: dict[str, str], report: FullReport) -> str:
    """Generate plain-English executive summary narrative."""
    parts = []

    if winners:
        for tier, db in sorted(winners.items()):
            mode = next((d.mode for d in report.databases if d.name == db), "unknown")
            parts.append(f"{db} [{mode}] led in the {tier} tier")

    embedded = [db.name for db in report.databases if db.mode == "embedded"]
    if embedded:
        parts.append(
            f"Embedded databases ({', '.join(embedded)}) have zero network overhead; "
            f"latency comparisons with server-mode databases should be interpreted in that context"
        )

    return ". ".join(parts) + "." if parts else "No results to summarize."


def _redact_config(config: AppConfig) -> dict[str, Any]:
    """Serialize config with passwords replaced by '***'."""
    data = asdict(config)
    for db_name, db_data in data.get("databases", {}).items():
        if "auth" in db_data and db_data["auth"]:
            for key in db_data["auth"]:
                if "pass" in key.lower():
                    db_data["auth"][key] = "***"
    return data


def _build_detail_results(report: FullReport) -> dict[str, list[dict[str, Any]]]:
    """Build per-database detailed results for expandable sections."""
    details: dict[str, list[dict[str, Any]]] = {}
    for db in report.databases:
        rows = []
        for r in db.results:
            rows.append(
                {
                    "benchmark": r.benchmark_name,
                    "tier": r.tier,
                    "status": r.status,
                    "median_ns": r.median_ns,
                    "min_ns": r.min_ns,
                    "max_ns": r.max_ns,
                    "std_dev_ns": r.std_dev_ns,
                    "iterations": len(r.warm_latencies_ns),
                    "skipped_reason": r.skipped_reason,
                }
            )
        details[db.name] = rows
    return details


def _serialize_report(report: FullReport) -> dict[str, Any]:
    """Serialize the full report to a JSON-safe dict with redacted passwords."""
    data = asdict(report)
    # Redact passwords in config
    for db_name, db_data in data.get("config", {}).get("databases", {}).items():
        if "auth" in db_data and db_data["auth"]:
            for key in db_data["auth"]:
                if "pass" in key.lower():
                    db_data["auth"][key] = "***"
    # Convert sets to sorted lists for JSON
    for db in data.get("databases", []):
        if db.get("compliance"):
            for field in ("clauses", "functions", "operators", "data_types"):
                if field in db["compliance"] and isinstance(db["compliance"][field], set):
                    db["compliance"][field] = sorted(db["compliance"][field])
    return data

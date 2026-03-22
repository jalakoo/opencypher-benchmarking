"""Report generation: data aggregation, HTML rendering, JSON output."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from graph_db_comparison.models import (
    AppConfig,
    FullReport,
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


def aggregate_report_data(report: FullReport) -> dict[str, Any]:
    """Transform FullReport into template-friendly dicts."""
    databases = _build_database_summaries(report)
    tier_tables = _build_tier_tables(report)
    tier_winners = _compute_tier_winners(tier_tables)
    radar_data = _compute_radar_data(report)
    compliance_matrix = _build_compliance_matrix(report)
    warm_vs_cold = _build_warm_vs_cold(report)
    skipped = _build_skipped_list(report)
    read_write = _build_read_write_breakdown(report)
    narrative = _build_narrative(tier_winners, report)

    return {
        "timestamp": report.timestamp,
        "version": report.version,
        "databases": databases,
        "tier_tables": tier_tables,
        "tier_winners": tier_winners,
        "radar_data": radar_data,
        "compliance_matrix": compliance_matrix,
        "warm_vs_cold": warm_vs_cold,
        "skipped": skipped,
        "read_write": read_write,
        "narrative": narrative,
        "db_colors": {
            db.name: DB_COLORS[i % len(DB_COLORS)] for i, db in enumerate(report.databases)
        },
        "config_redacted": _redact_config(report.config),
        "detail_results": _build_detail_results(report),
        "radar_svg": _build_radar_svg(
            radar_data,
            {db.name: DB_COLORS[i % len(DB_COLORS)] for i, db in enumerate(report.databases)},
        ),
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


# --- Internal helpers ---


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
                "compliance_pass_rate": db.compliance.pass_rate if db.compliance else None,
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


def _compute_radar_data(report: FullReport) -> dict[str, dict[str, float]]:
    """Compute normalized 0-100 radar chart scores for each database."""
    raw: dict[str, dict[str, float]] = {}

    for db in report.databases:
        read_medians = [
            r.median_ns
            for r in db.results
            if r.category == "read" and r.status == "pass" and r.median_ns
        ]
        write_medians = [
            r.median_ns
            for r in db.results
            if r.category == "write" and r.status == "pass" and r.median_ns
        ]
        total = len(db.results)
        passed = sum(1 for r in db.results if r.status == "pass")

        raw[db.name] = {
            "read_latency": (
                float(sum(read_medians) / len(read_medians)) if read_medians else float("inf")
            ),
            "write_latency": (
                float(sum(write_medians) / len(write_medians)) if write_medians else float("inf")
            ),
            "compliance": float(db.compliance.pass_rate * 100) if db.compliance else 0.0,
            "coverage": float(passed / total * 100) if total > 0 else 0.0,
        }

    # Normalize: for latency, lower is better (invert); for others, higher is better
    radar: dict[str, dict[str, float]] = {}
    axes = ["read_latency", "write_latency", "compliance", "coverage"]

    for axis in axes:
        values = [
            raw[db][axis]
            for db in raw
            if isinstance(raw[db][axis], (int, float)) and raw[db][axis] != float("inf")
        ]
        if not values:
            for db in raw:
                radar.setdefault(db, {})[axis] = 0
            continue

        min_val = min(values)
        max_val = max(values)

        for db in raw:
            val = raw[db][axis]
            if val == float("inf"):
                radar.setdefault(db, {})[axis] = 0
            elif max_val == min_val:
                radar.setdefault(db, {})[axis] = 100
            elif axis in ("read_latency", "write_latency"):
                # Invert: lower latency = higher score
                radar.setdefault(db, {})[axis] = round(
                    (1 - (val - min_val) / (max_val - min_val)) * 100
                )
            else:
                radar.setdefault(db, {})[axis] = round((val - min_val) / (max_val - min_val) * 100)

    return radar


def _build_compliance_matrix(report: FullReport) -> list[dict[str, Any]]:
    """Build compliance feature matrix rows."""
    all_features: dict[str, dict[str, bool]] = {}

    for db in report.databases:
        if not db.compliance:
            continue
        for clause in db.compliance.clauses:
            all_features.setdefault(f"clause:{clause}", {})[db.name] = True
        for func in db.compliance.functions:
            all_features.setdefault(f"function:{func}", {})[db.name] = True
        for op in db.compliance.operators:
            all_features.setdefault(f"operator:{op}", {})[db.name] = True

    db_names = [db.name for db in report.databases]
    matrix = []
    for feature, support in sorted(all_features.items()):
        row: dict[str, Any] = {"feature": feature}
        for db_name in db_names:
            row[db_name] = support.get(db_name, False)
        matrix.append(row)

    return matrix


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


def _build_read_write_breakdown(
    report: FullReport,
) -> dict[str, list[dict[str, Any]]]:
    """Split results into read and write tables."""
    breakdown: dict[str, list[dict[str, Any]]] = {"read": [], "write": [], "mixed": []}
    for db in report.databases:
        for r in db.results:
            if r.status != "pass":
                continue
            breakdown.setdefault(r.category, []).append(
                {
                    "database": db.name,
                    "mode": db.mode,
                    "benchmark": r.benchmark_name,
                    "median_ns": r.median_ns,
                    "p95_ns": r.p95_ns,
                }
            )
    return breakdown


def _build_narrative(winners: dict[str, str], report: FullReport) -> str:
    """Generate plain-English executive summary narrative."""
    parts = []

    if winners:
        for tier, db in sorted(winners.items()):
            mode = next((d.mode for d in report.databases if d.name == db), "unknown")
            parts.append(f"{db} [{mode}] led in the {tier} tier")

    compliance_leader = None
    best_rate = 0.0
    for db in report.databases:
        if db.compliance and db.compliance.pass_rate > best_rate:
            best_rate = db.compliance.pass_rate
            compliance_leader = db.name
    if compliance_leader:
        parts.append(f"{compliance_leader} had the highest Cypher compliance ({best_rate:.0%})")

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


def _build_radar_svg(radar_data: dict[str, dict[str, float]], colors: dict[str, str]) -> str:
    """Pre-compute radar chart SVG string."""
    if not radar_data:
        return ""

    axes = ["read_latency", "write_latency", "compliance", "coverage"]
    labels = ["Read Perf", "Write Perf", "Compliance", "Coverage"]
    n = len(axes)
    radius = 180

    lines = ['<svg width="500" height="420" viewBox="-260 -210 520 420">']

    # Grid levels
    for level in [20, 40, 60, 80, 100]:
        points = []
        for i in range(n):
            angle = -math.pi / 2 + (2 * math.pi / n) * i
            x = round(level * 1.8 * math.cos(angle), 1)
            y = round(level * 1.8 * math.sin(angle), 1)
            points.append(f"{x},{y}")
        lines.append(
            f'  <polygon points="{" ".join(points)}" fill="none" stroke="#ddd" stroke-width="1"/>'
        )

    # Axis lines and labels
    for i in range(n):
        angle = -math.pi / 2 + (2 * math.pi / n) * i
        x = round(radius * math.cos(angle), 1)
        y = round(radius * math.sin(angle), 1)
        lx = round(200 * math.cos(angle), 1)
        ly = round(200 * math.sin(angle), 1)
        lines.append(f'  <line x1="0" y1="0" x2="{x}" y2="{y}" stroke="#ddd" stroke-width="1"/>')
        lines.append(
            f'  <text x="{lx}" y="{ly}" text-anchor="middle" font-size="12" '
            f'fill="#666">{labels[i]}</text>'
        )

    # Database polygons
    for db_name, scores in radar_data.items():
        color = colors.get(db_name, "#999")
        points = []
        for i, axis in enumerate(axes):
            val = scores.get(axis, 0)
            angle = -math.pi / 2 + (2 * math.pi / n) * i
            x = round(val * 1.8 * math.cos(angle), 1)
            y = round(val * 1.8 * math.sin(angle), 1)
            points.append(f"{x},{y}")
        lines.append(
            f'  <polygon points="{" ".join(points)}" '
            f'fill="{color}" fill-opacity="0.2" stroke="{color}" stroke-width="2"/>'
        )

    # Legend
    for i, db_name in enumerate(radar_data):
        color = colors.get(db_name, "#999")
        ly = 140 + i * 20
        lines.append(f'  <rect x="-240" y="{ly}" width="12" height="12" fill="{color}"/>')
        lines.append(f'  <text x="-224" y="{ly + 11}" font-size="11" fill="#333">{db_name}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


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

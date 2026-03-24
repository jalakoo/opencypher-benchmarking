"""CLI entry point and main orchestration loop."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from opencypher_benchmarking import __version__
from opencypher_benchmarking.benchmarks.runner import run_tier
from opencypher_benchmarking.compatibility import (
    load_cached_compliance,
    run_embedded_compliance,
    run_server_compliance,
    save_compliance_cache,
)
from opencypher_benchmarking.config import load_config
from opencypher_benchmarking.connections import create_adapter
from opencypher_benchmarking.models import (
    AppConfig,
    DatabaseConfig,
    DatabaseReport,
    FeatureSupportMap,
    FullReport,
)
from opencypher_benchmarking.report.generator import (
    generate_html_report,
    generate_json_report,
    load_report_from_json,
)

logger = logging.getLogger("opencypher_benchmarking")

ALL_TIERS = ["basic", "intermediate", "advanced"]

# Adapters that require explicit schema (CREATE NODE TABLE) before any data ops.
# The generic compliance suite can't run against these because it creates arbitrary
# labels without schema. Use a curated feature map instead.
SCHEMA_REQUIRED_ADAPTERS = {"ladybugdb"}

# Feature map that assumes all features are supported (for --skip-compliance)
ALL_FEATURES = FeatureSupportMap(
    clauses={
        "MATCH",
        "CREATE",
        "MERGE",
        "DELETE",
        "SET",
        "REMOVE",
        "WITH",
        "UNWIND",
        "WHERE",
        "ORDER BY",
        "OPTIONAL MATCH",
    },
    functions={"count", "collect", "avg", "sum", "toInteger", "toString"},
    operators={"+", "-", "=", "<>", "STARTS WITH", "CONTAINS"},
    data_types={"Integer", "Float", "String", "Boolean", "List", "Map"},
    pass_rate=1.0,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="ocb",
        description="Graph database performance comparison tool",
    )
    parser.add_argument("-c", "--config", default="./config.yaml", help="Path to config.yaml")
    parser.add_argument(
        "-t",
        "--tier",
        action="append",
        choices=["basic", "intermediate", "advanced"],
        help="Run only specific tier(s). Can be repeated.",
    )
    parser.add_argument(
        "-d",
        "--database",
        action="append",
        help="Run only specific database(s) by name. Can be repeated.",
    )
    parser.add_argument(
        "-b",
        "--benchmark",
        action="append",
        help="Run only specific benchmark(s) by name. Can be repeated.",
    )
    parser.add_argument(
        "--skip-compliance",
        action="store_true",
        help="Skip compliance check, assume all features supported.",
    )
    parser.add_argument(
        "--compliance-only",
        action="store_true",
        help="Only run compliance checks, skip benchmarks.",
    )
    parser.add_argument(
        "--force-compliance",
        action="store_true",
        help="Force fresh compliance run, ignoring cache.",
    )
    parser.add_argument(
        "--compliance-ttl",
        type=int,
        default=86400,
        help="Compliance cache TTL in seconds (default: 86400).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Ping all enabled databases and report status, then exit.",
    )
    parser.add_argument(
        "--from-json",
        help="Regenerate HTML report from an existing results.json file, then exit.",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge new results into existing results.json in the output directory.",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip HTML report, output JSON only.",
    )
    parser.add_argument(
        "--output-dir",
        default="./reports",
        help="Output directory for reports.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging.")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser


def run_check(config: AppConfig) -> None:
    """Ping all enabled databases and report connectivity status."""
    for db_name, db_config in config.databases.items():
        if not db_config.enabled:
            continue
        try:
            adapter = create_adapter(db_config)
            adapter.setup_schema()
            adapter.execute("RETURN 1 AS n")
            print(f"  OK {db_name} ({db_config.mode}) -- reachable")
            adapter.close()
        except Exception as e:
            print(f"  FAIL {db_name} ({db_config.mode}) -- {e}")


def _run_compliance_with_cache(
    db_config: DatabaseConfig,
    adapter: Any,
    args: argparse.Namespace,
) -> FeatureSupportMap | None:
    """Run compliance with caching support."""
    # Check cache first (unless --force-compliance)
    if not args.force_compliance:
        cached = load_cached_compliance(db_config, ttl_seconds=args.compliance_ttl)
        if cached is not None:
            logger.info(f"  Using cached compliance for {db_config.name}")
            return cached

    # Run compliance
    logger.info(f"  Running compliance for {db_config.name}...")
    features: FeatureSupportMap | None = None
    if db_config.mode == "server":
        features = run_server_compliance(db_config)
    else:
        features = run_embedded_compliance(adapter, db_name=db_config.name)

    # Cache results
    if features is not None:
        save_compliance_cache(db_config, features)

    return features


def run_benchmarks(args: argparse.Namespace, config: AppConfig) -> FullReport:
    """Main benchmark execution loop."""
    database_reports: list[DatabaseReport] = []

    for db_name, db_config in config.databases.items():
        if not db_config.enabled:
            continue
        if args.database and db_name not in args.database:
            continue

        logger.info(f"=== {db_name} ({db_config.mode}) ===")

        # 1. Connect
        try:
            adapter = create_adapter(db_config)
            adapter.setup_schema()
            adapter.execute("RETURN 1")
        except Exception as e:
            logger.error(f"Failed to connect to {db_name}: {e}")
            database_reports.append(
                DatabaseReport(
                    name=db_name,
                    mode=db_config.mode,
                    adapter=db_config.adapter,
                    compliance_error=str(e),
                )
            )
            continue

        # 2. Compliance
        features: FeatureSupportMap | None = None
        compliance_error: str | None = None
        if db_config.adapter in SCHEMA_REQUIRED_ADAPTERS:
            # Schema-required databases can't run the generic compliance suite
            # (it creates arbitrary labels without pre-declaring tables).
            features = ALL_FEATURES
            logger.info(f"  Using curated features for {db_name} (schema-required adapter)")
        elif not args.skip_compliance:
            features = _run_compliance_with_cache(db_config, adapter, args)
            if features is None:
                compliance_error = "Compliance check failed"
        else:
            features = ALL_FEATURES

        # 3. Compliance-only mode: skip benchmarks
        if args.compliance_only:
            adapter.close()
            database_reports.append(
                DatabaseReport(
                    name=db_name,
                    mode=db_config.mode,
                    adapter=db_config.adapter,
                    compliance=features,
                    compliance_error=compliance_error,
                )
            )
            continue

        # 4. Run benchmarks
        all_results = []
        tiers = args.tier or ALL_TIERS
        for tier in tiers:
            tier_features = features or ALL_FEATURES
            tier_results = run_tier(adapter, tier, tier_features, config.benchmark, db_name)
            all_results.extend(tier_results)
            # Between-tier cleanup
            try:
                adapter.execute("MATCH (n) DETACH DELETE n")
            except Exception:
                pass

        adapter.close()
        database_reports.append(
            DatabaseReport(
                name=db_name,
                mode=db_config.mode,
                adapter=db_config.adapter,
                compliance=features,
                compliance_error=compliance_error,
                results=all_results,
            )
        )

    return FullReport(
        timestamp=datetime.now().isoformat(),
        version=__version__,
        config=config,
        databases=database_reports,
    )


def merge_reports(existing: FullReport, new: FullReport) -> FullReport:
    """Merge new database results into an existing report.

    Databases in *new* replace same-named databases in *existing*,
    but only when the new run produced results. Failed runs (connection
    errors with no benchmark results) do not overwrite prior good data.
    Databases only in *existing* are preserved as-is.
    """
    merged = {db.name: db for db in existing.databases}
    for db in new.databases:
        if db.results or db.name not in merged:
            merged[db.name] = db
        else:
            logger.info(f"  Keeping prior results for {db.name} (new run had no results)")

    return FullReport(
        timestamp=new.timestamp,
        version=new.version,
        config=existing.config,
        databases=list(merged.values()),
        duration_seconds=new.duration_seconds,
    )


def main() -> None:
    """CLI entry point."""
    args = build_parser().parse_args()

    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Regenerate HTML from existing JSON
    if args.from_json:
        report = load_report_from_json(args.from_json)
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        html_path = output_dir / "report.html"
        generate_html_report(report, str(html_path))
        logger.info(f"HTML report: {html_path}")
        return

    # Load config
    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    # Check mode
    if args.check:
        run_check(config)
        return

    # Run benchmarks (or compliance-only)
    bench_start = time.monotonic()
    report = run_benchmarks(args, config)
    report.duration_seconds = round(time.monotonic() - bench_start, 1)

    # Merge with existing results if --merge
    if args.merge:
        json_path = Path(args.output_dir) / "results.json"
        try:
            existing_report = load_report_from_json(str(json_path))
            report = merge_reports(existing_report, report)
            logger.info(f"Merged with existing {json_path}")
        except FileNotFoundError:
            logger.warning(f"No existing {json_path} found, writing fresh report.")

    # Generate output
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "results.json"
    generate_json_report(report, str(json_path))
    logger.info(f"JSON report: {json_path}")

    if not args.no_report:
        html_path = output_dir / "report.html"
        generate_html_report(report, str(html_path))
        logger.info(f"HTML report: {html_path}")

    # Summary
    total_passed = sum(sum(1 for r in db.results if r.status == "pass") for db in report.databases)
    total_skipped = sum(sum(1 for r in db.results if r.status == "skip") for db in report.databases)
    total_errors = sum(sum(1 for r in db.results if r.status == "error") for db in report.databases)
    print(
        f"\nDone. {len(report.databases)} database(s), "
        f"{total_passed} passed, {total_skipped} skipped, {total_errors} errors."
    )


if __name__ == "__main__":
    main()

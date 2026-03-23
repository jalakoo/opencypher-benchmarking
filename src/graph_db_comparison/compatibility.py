"""Compatibility discovery: opencypher-compliance integration, caching, eligibility."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

from graph_db_comparison.connections import DatabaseAdapter, Result
from graph_db_comparison.models import DatabaseConfig, FeatureSupportMap

logger = logging.getLogger(__name__)

CACHE_DIR = Path(".cache")


# --- Server-mode compliance (via opencypher-compliance package) ---


def run_server_compliance(config: DatabaseConfig) -> FeatureSupportMap | None:
    """Run opencypher-compliance against a server-mode database."""
    try:
        from opencypher_compliance import run_compliance

        adapter_name = config.adapter if config.adapter == "bolt" else "falkordb"
        auth: dict[str, str] = {}
        if config.auth:
            auth = {
                "username": config.auth.get("username", ""),
                "password": config.auth.get("password", ""),
            }
        results = run_compliance(
            config={
                "database": {
                    "adapter": adapter_name,
                    "host": config.host,
                    "port": config.port,
                    "auth": auth,
                }
            }
        )
        return _build_feature_map(results)
    except Exception as e:
        logger.error(f"Compliance failed for {config.name}: {e}")
        return None


# --- Embedded manual compliance runner ---


def run_embedded_compliance(
    adapter: DatabaseAdapter, db_name: str = ""
) -> FeatureSupportMap | None:
    """Run compliance manually against an embedded database using load_catalog()."""
    try:
        from opencypher_compliance.catalog import load_catalog

        tests = load_catalog()
        passed: list[dict] = []
        failed: list[dict] = []

        for test in tests:
            try:
                # 1. Run setup queries
                for setup_q in test.get("setup", []):
                    adapter.execute(setup_q)
                # 2. Execute the test query
                result = adapter.execute(test["query"])
                # 3. Validate
                if _validate_result(result, test):
                    passed.append(test)
                else:
                    failed.append(test)
            except Exception:
                failed.append(test)
            finally:
                # 4. Run teardown
                for td_q in test.get("teardown", []):
                    try:
                        adapter.execute(td_q)
                    except Exception:
                        pass

        return _build_feature_map_from_tests(passed, failed, tests)
    except Exception as e:
        logger.error(f"Embedded compliance failed for {db_name}: {e}")
        return None


# --- Helpers ---


def _parse_pass_rate(value: Any) -> float:
    """Parse pass_rate from opencypher-compliance, handling both float and string formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().rstrip("%")
        try:
            rate = float(cleaned)
            # If it was "87.5%" convert from percentage to 0-1 range
            if rate > 1.0:
                return rate / 100.0
            return rate
        except ValueError:
            return 0.0
    return 0.0


# --- Feature map builders ---


def _build_feature_map(results: dict[str, Any]) -> FeatureSupportMap:
    """Convert opencypher-compliance results dict into FeatureSupportMap.

    The package returns:
      - results["results"]: list of {"element": "MATCH", "type": "clause", "result": "pass", ...}
      - results["metadata"]["pass_rate"]: float or string like "100.00%"
    """
    clauses: set[str] = set()
    functions: set[str] = set()
    operators: set[str] = set()
    data_types: set[str] = set()

    # The package uses "results" key (not "tests"), "result" (not "status"),
    # "element" (not "feature"), and "type" (not "category")
    test_list = results.get("results", results.get("tests", []))
    for test in test_list:
        status = test.get("result", test.get("status", ""))
        if status != "pass":
            continue
        feature = test.get("element", test.get("feature", ""))
        test_type = test.get("type", test.get("category", ""))
        match test_type:
            case "clause":
                clauses.add(feature)
            case "function":
                functions.add(feature)
            case "operator":
                operators.add(feature)
            case "data_type":
                data_types.add(feature)

    return FeatureSupportMap(
        clauses=clauses,
        functions=functions,
        operators=operators,
        data_types=data_types,
        pass_rate=_parse_pass_rate(results.get("metadata", {}).get("pass_rate", 0.0)),
    )


def _build_feature_map_from_tests(
    passed: list[dict], failed: list[dict], all_tests: list[dict]
) -> FeatureSupportMap:
    """Build FeatureSupportMap from passed/failed test lists (for manual runner)."""
    clauses: set[str] = set()
    functions: set[str] = set()
    operators: set[str] = set()
    data_types: set[str] = set()

    for test in passed:
        feature = test.get("element", test.get("feature", ""))
        test_type = test.get("type", test.get("category", ""))
        match test_type:
            case "clause":
                clauses.add(feature)
            case "function":
                functions.add(feature)
            case "operator":
                operators.add(feature)
            case "data_type":
                data_types.add(feature)

    total = len(all_tests)
    pass_rate = len(passed) / total if total > 0 else 0.0

    return FeatureSupportMap(
        clauses=clauses,
        functions=functions,
        operators=operators,
        data_types=data_types,
        pass_rate=pass_rate,
    )


# --- Eligibility checks ---


def check_benchmark_eligible(
    benchmark_required: dict[str, set[str]], features: FeatureSupportMap
) -> tuple[bool, str | None]:
    """Check if all required features for a benchmark are supported."""
    for req in benchmark_required.get("clauses", set()):
        if req not in features.clauses:
            return False, f"Missing clause: {req}"
    for req in benchmark_required.get("functions", set()):
        if req not in features.functions:
            return False, f"Missing function: {req}"
    for req in benchmark_required.get("operators", set()):
        if req not in features.operators:
            return False, f"Missing operator: {req}"
    for req in benchmark_required.get("data_types", set()):
        if req not in features.data_types:
            return False, f"Missing data type: {req}"
    return True, None


def check_tier_eligible(eligible_count: int) -> bool:
    """Tier requires >= 3 eligible benchmarks."""
    return eligible_count >= 3


# --- Result validation (for manual compliance runner) ---


def _validate_result(result: Result, test: dict) -> bool:
    """Check a query result against test expectations."""
    # If test expects an error, but we got here without one, it's a failure
    if test.get("expect_error"):
        return False

    # Check expected_rows
    if "expected_rows" in test:
        expected = test["expected_rows"]
        if result.records != expected:
            return False

    # Check expected_columns
    if "expected_columns" in test:
        expected_cols = test["expected_columns"]
        if result.records:
            actual_cols = list(result.records[0].keys())
            if sorted(actual_cols) != sorted(expected_cols):
                return False
        else:
            # No records — can't verify columns
            if expected_cols:
                return False

    # Check expected_contains
    if "expected_contains" in test:
        for expected_row in test["expected_contains"]:
            if expected_row not in result.records:
                return False

    return True


# --- Compliance caching ---


def _cache_key(config: DatabaseConfig) -> str:
    """Generate a short hash from the database config for cache file naming."""
    raw = f"{config.adapter}:{config.host}:{config.port}:{config.db_path}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _cache_path(config: DatabaseConfig) -> Path:
    """Return the cache file path for a given database config."""
    return CACHE_DIR / f"compliance_{config.name}_{_cache_key(config)}.json"


def load_cached_compliance(config: DatabaseConfig, ttl_seconds: int) -> FeatureSupportMap | None:
    """Load compliance results from cache if not expired."""
    path = _cache_path(config)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if time.time() - data["timestamp"] > ttl_seconds:
            return None
        feat = data["features"]
        return FeatureSupportMap(
            clauses=set(feat["clauses"]),
            functions=set(feat["functions"]),
            operators=set(feat["operators"]),
            data_types=set(feat["data_types"]),
            pass_rate=feat["pass_rate"],
        )
    except Exception as e:
        logger.warning(f"Failed to load compliance cache for {config.name}: {e}")
        return None


def save_compliance_cache(config: DatabaseConfig, features: FeatureSupportMap) -> None:
    """Save compliance results to cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(config)
    path.write_text(
        json.dumps(
            {
                "timestamp": time.time(),
                "features": {
                    "clauses": sorted(features.clauses),
                    "functions": sorted(features.functions),
                    "operators": sorted(features.operators),
                    "data_types": sorted(features.data_types),
                    "pass_rate": features.pass_rate,
                },
            },
            indent=2,
        )
    )

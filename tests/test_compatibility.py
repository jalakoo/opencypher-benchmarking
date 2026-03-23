"""Tests for compatibility discovery: feature map building, eligibility, caching, validation."""

from __future__ import annotations

import json
import time

import pytest

from graph_db_comparison.compatibility import (
    _build_feature_map,
    _build_feature_map_from_tests,
    _parse_pass_rate,
    _validate_result,
    check_benchmark_eligible,
    check_tier_eligible,
    load_cached_compliance,
    save_compliance_cache,
)
from graph_db_comparison.connections import Result
from graph_db_comparison.models import DatabaseConfig, FeatureSupportMap

# --- _build_feature_map from opencypher-compliance results ---


def test_build_feature_map_extracts_clauses():
    """Passing clause tests are extracted into the clauses set."""
    results = {
        "metadata": {"pass_rate": 0.5, "total": 2, "passed": 1},
        "tests": [
            {"name": "T1", "category": "clause", "feature": "MATCH", "status": "pass"},
            {"name": "T2", "category": "clause", "feature": "CREATE", "status": "fail"},
        ],
    }
    fm = _build_feature_map(results)
    assert "MATCH" in fm.clauses
    assert "CREATE" not in fm.clauses


def test_build_feature_map_extracts_functions():
    """Passing function tests are extracted into the functions set."""
    results = {
        "metadata": {"pass_rate": 1.0, "total": 1, "passed": 1},
        "tests": [
            {"name": "T1", "category": "function", "feature": "count", "status": "pass"},
        ],
    }
    fm = _build_feature_map(results)
    assert "count" in fm.functions


def test_build_feature_map_extracts_operators():
    """Passing operator tests are extracted into the operators set."""
    results = {
        "metadata": {"pass_rate": 1.0, "total": 1, "passed": 1},
        "tests": [
            {"name": "T1", "category": "operator", "feature": "STARTS WITH", "status": "pass"},
        ],
    }
    fm = _build_feature_map(results)
    assert "STARTS WITH" in fm.operators


def test_build_feature_map_extracts_data_types():
    """Passing data_type tests are extracted into the data_types set."""
    results = {
        "metadata": {"pass_rate": 1.0, "total": 1, "passed": 1},
        "tests": [
            {"name": "T1", "category": "data_type", "feature": "Integer", "status": "pass"},
        ],
    }
    fm = _build_feature_map(results)
    assert "Integer" in fm.data_types


def test_build_feature_map_preserves_pass_rate():
    """pass_rate from metadata is stored in FeatureSupportMap."""
    results = {
        "metadata": {"pass_rate": 0.87, "total": 100, "passed": 87},
        "tests": [],
    }
    fm = _build_feature_map(results)
    assert fm.pass_rate == 0.87


def test_build_feature_map_ignores_failed_tests():
    """Failed tests do not appear in any feature set."""
    results = {
        "metadata": {"pass_rate": 0.0, "total": 4, "passed": 0},
        "tests": [
            {"name": "T1", "category": "clause", "feature": "MATCH", "status": "fail"},
            {"name": "T2", "category": "function", "feature": "count", "status": "fail"},
            {"name": "T3", "category": "operator", "feature": "+", "status": "fail"},
            {"name": "T4", "category": "data_type", "feature": "String", "status": "fail"},
        ],
    }
    fm = _build_feature_map(results)
    assert len(fm.clauses) == 0
    assert len(fm.functions) == 0
    assert len(fm.operators) == 0
    assert len(fm.data_types) == 0


# --- _build_feature_map with actual opencypher-compliance format ---


def test_build_feature_map_actual_package_format():
    """Handle the actual opencypher-compliance result format (element/type/result keys)."""
    results = {
        "metadata": {"pass_rate": "100.00%"},
        "results": [
            {"element": "MATCH", "type": "clause", "result": "pass", "duration_ms": 13},
            {"element": "CREATE", "type": "clause", "result": "pass", "duration_ms": 8},
            {"element": "count", "type": "function", "result": "pass", "duration_ms": 5},
            {"element": "STARTS WITH", "type": "operator", "result": "pass", "duration_ms": 3},
            {"element": "Integer", "type": "data_type", "result": "fail", "duration_ms": 2},
        ],
    }
    fm = _build_feature_map(results)
    assert "MATCH" in fm.clauses
    assert "CREATE" in fm.clauses
    assert "count" in fm.functions
    assert "STARTS WITH" in fm.operators
    assert "Integer" not in fm.data_types  # failed
    assert fm.pass_rate == pytest.approx(1.0)


# --- _build_feature_map_from_tests (for embedded manual runner) ---


def test_build_feature_map_from_tests_passed_only():
    """Feature map built from passed/failed test lists."""
    passed = [
        {"name": "T1", "category": "clause", "feature": "MATCH"},
        {"name": "T2", "category": "function", "feature": "count"},
    ]
    failed = [
        {"name": "T3", "category": "clause", "feature": "CREATE"},
    ]
    all_tests = passed + failed
    fm = _build_feature_map_from_tests(passed, failed, all_tests)
    assert "MATCH" in fm.clauses
    assert "count" in fm.functions
    assert "CREATE" not in fm.clauses
    assert fm.pass_rate == pytest.approx(2 / 3, rel=1e-2)


# --- check_benchmark_eligible ---


def test_benchmark_eligible_all_features_present():
    """Benchmark is eligible when all required features are supported."""
    features = FeatureSupportMap(
        clauses={"MATCH", "CREATE"},
        functions={"count"},
    )
    required = {"clauses": {"MATCH", "CREATE"}, "functions": {"count"}}
    eligible, reason = check_benchmark_eligible(required, features)
    assert eligible is True
    assert reason is None


def test_benchmark_eligible_missing_clause():
    """Benchmark is ineligible when a required clause is missing."""
    features = FeatureSupportMap(clauses={"MATCH"})
    required = {"clauses": {"MATCH", "MERGE"}}
    eligible, reason = check_benchmark_eligible(required, features)
    assert eligible is False
    assert "MERGE" in reason


def test_benchmark_eligible_missing_function():
    """Benchmark is ineligible when a required function is missing."""
    features = FeatureSupportMap(clauses={"MATCH"}, functions=set())
    required = {"clauses": {"MATCH"}, "functions": {"collect"}}
    eligible, reason = check_benchmark_eligible(required, features)
    assert eligible is False
    assert "collect" in reason


def test_benchmark_eligible_empty_requirements():
    """Benchmark with no requirements is always eligible."""
    features = FeatureSupportMap()
    eligible, reason = check_benchmark_eligible({}, features)
    assert eligible is True


def test_benchmark_eligible_checks_operators():
    """Benchmark is ineligible when a required operator is missing."""
    features = FeatureSupportMap(operators={"+"})
    required = {"operators": {"CONTAINS"}}
    eligible, reason = check_benchmark_eligible(required, features)
    assert eligible is False
    assert "CONTAINS" in reason


# --- check_tier_eligible ---


def test_tier_eligible_at_threshold():
    """Tier is eligible with exactly 3 benchmarks."""
    assert check_tier_eligible(3) is True


def test_tier_eligible_above_threshold():
    """Tier is eligible with more than 3 benchmarks."""
    assert check_tier_eligible(10) is True


def test_tier_ineligible_below_threshold():
    """Tier is ineligible with fewer than 3 benchmarks."""
    assert check_tier_eligible(2) is False


def test_tier_ineligible_zero():
    """Tier is ineligible with 0 benchmarks."""
    assert check_tier_eligible(0) is False


# --- _validate_result ---


def test_validate_result_pass_with_expected_rows():
    """Validation passes when result records match expected_rows."""
    result = Result(records=[{"n": 1}, {"n": 2}])
    test = {"expected_rows": [{"n": 1}, {"n": 2}]}
    assert _validate_result(result, test) is True


def test_validate_result_fail_with_wrong_rows():
    """Validation fails when result records don't match expected_rows."""
    result = Result(records=[{"n": 99}])
    test = {"expected_rows": [{"n": 1}]}
    assert _validate_result(result, test) is False


def test_validate_result_pass_expect_success():
    """Validation passes when expect_success is True and query didn't error."""
    result = Result(records=[])
    test = {"expect_success": True}
    assert _validate_result(result, test) is True


def test_validate_result_fail_expect_error_but_succeeded():
    """Validation fails when expect_error is set but query succeeded."""
    result = Result(records=[])
    test = {"expect_error": True}
    assert _validate_result(result, test) is False


def test_validate_result_pass_expected_columns():
    """Validation passes when result columns match expected_columns."""
    result = Result(records=[{"name": "Alice", "age": 30}])
    test = {"expected_columns": ["name", "age"]}
    assert _validate_result(result, test) is True


def test_validate_result_fail_wrong_columns():
    """Validation fails when result columns don't match expected_columns."""
    result = Result(records=[{"name": "Alice"}])
    test = {"expected_columns": ["name", "age"]}
    assert _validate_result(result, test) is False


def test_validate_result_pass_expected_contains():
    """Validation passes when result contains expected values."""
    result = Result(records=[{"n": 1}, {"n": 2}, {"n": 3}])
    test = {"expected_contains": [{"n": 2}]}
    assert _validate_result(result, test) is True


def test_validate_result_fail_expected_contains_missing():
    """Validation fails when result doesn't contain expected values."""
    result = Result(records=[{"n": 1}])
    test = {"expected_contains": [{"n": 99}]}
    assert _validate_result(result, test) is False


def test_validate_result_pass_no_expectations():
    """Validation passes when no expectations are set."""
    result = Result(records=[{"x": 1}])
    test = {}
    assert _validate_result(result, test) is True


# --- _parse_pass_rate ---


def test_parse_pass_rate_float():
    """Float value passes through."""
    assert _parse_pass_rate(0.87) == 0.87


def test_parse_pass_rate_int():
    """Int value is converted to float."""
    assert _parse_pass_rate(1) == 1.0


def test_parse_pass_rate_percentage_string():
    """String like '87.50%' is parsed to 0.875."""
    assert _parse_pass_rate("87.50%") == pytest.approx(0.875)


def test_parse_pass_rate_100_percent_string():
    """String '100.00%' is parsed to 1.0."""
    assert _parse_pass_rate("100.00%") == pytest.approx(1.0)


def test_parse_pass_rate_zero_string():
    """String '0%' is parsed to 0.0."""
    assert _parse_pass_rate("0%") == 0.0


def test_parse_pass_rate_garbage_string():
    """Unparseable string returns 0.0."""
    assert _parse_pass_rate("not_a_number") == 0.0


def test_parse_pass_rate_none():
    """None returns 0.0."""
    assert _parse_pass_rate(None) == 0.0


# --- Compliance caching ---


def test_cache_roundtrip(tmp_path, monkeypatch):
    """Saving and loading compliance cache produces the same FeatureSupportMap."""
    monkeypatch.setattr("graph_db_comparison.compatibility.CACHE_DIR", tmp_path)
    config = DatabaseConfig(name="neo4j", adapter="bolt", enabled=True, host="localhost", port=7687)
    features = FeatureSupportMap(
        clauses={"MATCH", "CREATE"},
        functions={"count"},
        operators={"+"},
        data_types={"Integer"},
        pass_rate=0.87,
    )
    save_compliance_cache(config, features)
    loaded = load_cached_compliance(config, ttl_seconds=3600)
    assert loaded is not None
    assert loaded.clauses == features.clauses
    assert loaded.functions == features.functions
    assert loaded.operators == features.operators
    assert loaded.data_types == features.data_types
    assert loaded.pass_rate == features.pass_rate


def test_cache_expired_returns_none(tmp_path, monkeypatch):
    """Expired cache returns None."""
    monkeypatch.setattr("graph_db_comparison.compatibility.CACHE_DIR", tmp_path)
    config = DatabaseConfig(name="neo4j", adapter="bolt", enabled=True, host="localhost", port=7687)
    features = FeatureSupportMap(clauses={"MATCH"}, pass_rate=1.0)
    save_compliance_cache(config, features)

    # Manually backdate the timestamp
    cache_files = list(tmp_path.glob("compliance_*.json"))
    assert len(cache_files) == 1
    data = json.loads(cache_files[0].read_text())
    data["timestamp"] = time.time() - 100000  # way in the past
    cache_files[0].write_text(json.dumps(data))

    loaded = load_cached_compliance(config, ttl_seconds=3600)
    assert loaded is None


def test_cache_missing_returns_none(tmp_path, monkeypatch):
    """Missing cache file returns None."""
    monkeypatch.setattr("graph_db_comparison.compatibility.CACHE_DIR", tmp_path)
    config = DatabaseConfig(name="neo4j", adapter="bolt", enabled=True, host="localhost", port=7687)
    loaded = load_cached_compliance(config, ttl_seconds=3600)
    assert loaded is None


def test_cache_different_configs_different_files(tmp_path, monkeypatch):
    """Different database configs produce different cache files."""
    monkeypatch.setattr("graph_db_comparison.compatibility.CACHE_DIR", tmp_path)
    config1 = DatabaseConfig(
        name="neo4j", adapter="bolt", enabled=True, host="localhost", port=7687
    )
    config2 = DatabaseConfig(
        name="memgraph", adapter="bolt", enabled=True, host="localhost", port=7688
    )
    features = FeatureSupportMap(clauses={"MATCH"}, pass_rate=1.0)
    save_compliance_cache(config1, features)
    save_compliance_cache(config2, features)
    cache_files = list(tmp_path.glob("compliance_*.json"))
    assert len(cache_files) == 2

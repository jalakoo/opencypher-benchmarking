"""Tests for synthetic data generation."""

from __future__ import annotations

from opencypher_benchmarking.data_generation import (
    generate_companies,
    generate_knows_edges,
    generate_persons,
    generate_works_at_edges,
)

# --- generate_persons ---


def test_persons_count_scale_1():
    """generate_persons at scale 1 returns exactly 1000 items."""
    persons = generate_persons(scale=1)
    assert len(persons) == 1000


def test_persons_count_scale_10():
    """generate_persons at scale 10 returns exactly 10000 items."""
    persons = generate_persons(scale=10)
    assert len(persons) == 10000


def test_persons_deterministic():
    """Two calls with same seed produce identical output."""
    a = generate_persons(scale=1)
    b = generate_persons(scale=1)
    assert a == b


def test_persons_different_seed_differs():
    """Different seeds produce different output."""
    a = generate_persons(scale=1, seed=42)
    b = generate_persons(scale=1, seed=99)
    assert a != b


def test_persons_structure():
    """Each person has the required fields with correct types."""
    persons = generate_persons(scale=1)
    p = persons[0]
    assert isinstance(p["name"], str)
    assert isinstance(p["age"], int)
    assert 18 <= p["age"] <= 80
    assert isinstance(p["city"], str)
    assert isinstance(p["active"], bool)
    assert isinstance(p["created"], str)


def test_persons_unique_names():
    """All person names are unique."""
    persons = generate_persons(scale=1)
    names = [p["name"] for p in persons]
    assert len(names) == len(set(names))


# --- generate_companies ---


def test_companies_count_scale_1():
    """generate_companies at scale 1 returns exactly 50 items."""
    companies = generate_companies(scale=1)
    assert len(companies) == 50


def test_companies_count_scale_10():
    """generate_companies at scale 10 returns exactly 500 items."""
    companies = generate_companies(scale=10)
    assert len(companies) == 500


def test_companies_deterministic():
    """Two calls with same seed produce identical output."""
    a = generate_companies(scale=1)
    b = generate_companies(scale=1)
    assert a == b


def test_companies_structure():
    """Each company has the required fields with correct types."""
    companies = generate_companies(scale=1)
    c = companies[0]
    assert isinstance(c["name"], str)
    assert isinstance(c["industry"], str)
    assert isinstance(c["founded"], int)
    assert 1950 <= c["founded"] <= 2025


# --- generate_knows_edges ---


def test_knows_count_scale_1():
    """generate_knows_edges at scale 1 returns exactly 5000 edges."""
    edges = generate_knows_edges(person_count=1000, scale=1)
    assert len(edges) == 5000


def test_knows_no_self_loops():
    """No edge connects a person to themselves."""
    edges = generate_knows_edges(person_count=1000, scale=1)
    for a, b in edges:
        assert a != b


def test_knows_unique_edges():
    """All edges are unique (no duplicates)."""
    edges = generate_knows_edges(person_count=1000, scale=1)
    assert len(edges) == len(set(edges))


def test_knows_deterministic():
    """Two calls with same seed produce identical output."""
    a = generate_knows_edges(person_count=1000, scale=1)
    b = generate_knows_edges(person_count=1000, scale=1)
    assert set(a) == set(b)


def test_knows_valid_indices():
    """All edge indices are within the valid person range."""
    edges = generate_knows_edges(person_count=1000, scale=1)
    for a, b in edges:
        assert 0 <= a < 1000
        assert 0 <= b < 1000


# --- generate_works_at_edges ---


def test_works_at_count_scale_1():
    """generate_works_at_edges at scale 1 returns exactly 1000 edges."""
    edges = generate_works_at_edges(person_count=1000, company_count=50, scale=1)
    assert len(edges) == 1000


def test_works_at_count_scale_10():
    """generate_works_at_edges at scale 10 returns exactly 10000 edges."""
    edges = generate_works_at_edges(person_count=10000, company_count=500, scale=10)
    assert len(edges) == 10000


def test_works_at_deterministic():
    """Two calls with same seed produce identical output."""
    a = generate_works_at_edges(person_count=1000, company_count=50, scale=1)
    b = generate_works_at_edges(person_count=1000, company_count=50, scale=1)
    assert a == b


def test_works_at_valid_indices():
    """All edge indices are within valid ranges."""
    edges = generate_works_at_edges(person_count=1000, company_count=50, scale=1)
    for p, c in edges:
        assert 0 <= p < 1000
        assert 0 <= c < 50

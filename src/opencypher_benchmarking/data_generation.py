"""Synthetic data generation for benchmarks."""

from __future__ import annotations

import random

CITIES = ["NYC", "London", "Tokyo", "Berlin", "Sydney", "Toronto", "Mumbai", "Lagos"]
INDUSTRIES = ["Tech", "Finance", "Healthcare", "Manufacturing", "Retail", "Energy"]


def generate_persons(scale: int, seed: int = 42) -> list[dict]:
    """Generate Person node dicts. ~1000 per scale unit."""
    rng = random.Random(seed)
    count = 1000 * scale
    persons = []
    for i in range(count):
        persons.append(
            {
                "name": f"person_{i}",
                "age": rng.randint(18, 80),
                "city": rng.choice(CITIES),
                "active": rng.random() > 0.3,
                "created": f"2025-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
            }
        )
    return persons


def generate_companies(scale: int, seed: int = 42) -> list[dict]:
    """Generate Company node dicts. ~50 per scale unit."""
    rng = random.Random(seed + 1)
    count = 50 * scale
    companies = []
    for i in range(count):
        companies.append(
            {
                "name": f"company_{i}",
                "industry": rng.choice(INDUSTRIES),
                "founded": rng.randint(1950, 2025),
            }
        )
    return companies


def generate_knows_edges(person_count: int, scale: int, seed: int = 42) -> list[tuple[int, int]]:
    """Generate KNOWS relationship pairs. ~5000 per scale unit. No self-loops, unique edges."""
    rng = random.Random(seed + 2)
    count = 5000 * scale
    edges: set[tuple[int, int]] = set()
    while len(edges) < count:
        a = rng.randint(0, person_count - 1)
        b = rng.randint(0, person_count - 1)
        if a != b:
            edges.add((a, b))
    return list(edges)


def generate_works_at_edges(
    person_count: int, company_count: int, scale: int, seed: int = 42
) -> list[tuple[int, int]]:
    """Generate WORKS_AT relationship pairs. ~1000 per scale unit."""
    rng = random.Random(seed + 3)
    count = 1000 * scale
    edges = []
    for _ in range(count):
        p = rng.randint(0, person_count - 1)
        c = rng.randint(0, company_count - 1)
        edges.append((p, c))
    return edges

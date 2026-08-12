"""Differential tests: batch validation must equal per-row validation.

``CompiledValidationPlan.validate_batch`` takes a fast bulk path and, on
failure, isolates the offending rows instead of re-validating everything.
That is purely an optimisation: the observable result must always be
byte-for-byte what you would get by calling :meth:`validate_row` on each
row in turn.

Per-row validation is the oracle here — it is the definition of correct —
so these tests compare against it directly over randomised dirty data.
"""

from __future__ import annotations

import datetime as dt
import random
from typing import Any

import pytest
from pydantic import BaseModel, Field

from streamval.core.result import ValidationResult
from streamval.schema.coerce import SourceFormat
from streamval.schema.plan import CompiledValidationPlan


class Row(BaseModel):
    id: int
    name: str
    value: float
    active: bool


class Constrained(BaseModel):
    id: int = Field(ge=0)
    name: str = Field(min_length=2)
    score: float = Field(ge=0.0, le=100.0)


class Optionals(BaseModel):
    id: int
    note: str | None = None
    when: dt.datetime | None = None


def _oracle(
    plan: CompiledValidationPlan,
    rows: list[dict[str, Any]],
    start: int,
) -> list[ValidationResult]:
    return [plan.validate_row(start + i, rows[i]) for i in range(len(rows))]


def _same(a: list[ValidationResult], b: list[ValidationResult]) -> None:
    assert len(a) == len(b), f"length {len(a)} != {len(b)}"
    for x, y in zip(a, b, strict=True):
        assert x.row_index == y.row_index, f"row_index {x.row_index} != {y.row_index}"
        assert x.valid == y.valid, f"row {x.row_index}: valid {x.valid} != {y.valid}"
        assert x.raw == y.raw, f"row {x.row_index}: raw differs"
        assert x.data == y.data, f"row {x.row_index}: data differs"
        assert x.errors == y.errors, (
            f"row {x.row_index}: errors differ\n  {x.errors}\n  {y.errors}"
        )


# --- deterministic edge cases ---

_GOOD = {"id": 1, "name": "ok", "value": 1.5, "active": True}
_BAD = {"id": "nope", "name": "x", "value": 1.5, "active": True}
_WORSE = {"id": "nope", "name": None, "value": "zz", "active": "maybe"}


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [_GOOD],
        [_BAD],
        [_GOOD, _BAD],
        [_BAD, _GOOD],
        [_BAD, _BAD],
        [_GOOD] * 50,
        [_BAD] * 50,
        [_GOOD] * 49 + [_BAD],
        [_BAD] + [_GOOD] * 49,
        [_GOOD] * 25 + [_BAD] + [_GOOD] * 24,
        [_GOOD, _WORSE, _GOOD],
        [_WORSE] * 10,
        [{}],
        [{}, _GOOD],
        [{"id": 1}],  # missing required fields
    ],
    ids=lambda r: f"n{len(r)}",
)
def test_batch_matches_per_row(rows: list[dict[str, Any]]) -> None:
    plan = CompiledValidationPlan(Row, SourceFormat.JSONL)
    _same(plan.validate_batch(list(rows), start_index=7), _oracle(plan, rows, 7))


def test_start_index_is_preserved_through_the_split() -> None:
    plan = CompiledValidationPlan(Row, SourceFormat.JSONL)
    rows = [_GOOD, _BAD, _GOOD, _BAD]
    got = plan.validate_batch(rows, start_index=1000)
    assert [r.row_index for r in got] == [1000, 1001, 1002, 1003]
    _same(got, _oracle(plan, rows, 1000))


# --- randomised differential testing ---


def _rand_value(rng: random.Random, kind: str) -> Any:
    """Return a value that is sometimes valid and sometimes not."""
    pool: dict[str, list[Any]] = {
        "int": [0, 1, -5, 999, "12", "nope", None, "", 3.7, [], {"a": 1}, True],
        "str": ["a", "ok", "", None, 5, ["x"], "x" * 200],
        "float": [1.5, 0.0, -2.5, "3.5", "zz", None, "", 10**9, float("nan")],
        "bool": [True, False, "true", "yes", 1, 0, None, "maybe", 2],
    }
    return rng.choice(pool[kind])


@pytest.mark.parametrize("seed", range(40))
def test_random_batches_match_per_row(seed: int) -> None:
    rng = random.Random(seed)
    plan = CompiledValidationPlan(Row, SourceFormat.JSONL)
    n = rng.randint(1, 60)
    rows: list[dict[str, Any]] = []
    for _ in range(n):
        if rng.random() < 0.5:
            rows.append(dict(_GOOD))
            continue
        row = dict(_GOOD)
        for field, kind in (
            ("id", "int"),
            ("name", "str"),
            ("value", "float"),
            ("active", "bool"),
        ):
            if rng.random() < 0.35:
                row[field] = _rand_value(rng, kind)
        if rng.random() < 0.1:
            row.pop(rng.choice(list(row)), None)
        rows.append(row)

    start = rng.randint(0, 10_000)
    _same(
        plan.validate_batch(list(rows), start_index=start),
        _oracle(plan, rows, start),
    )


@pytest.mark.parametrize("seed", range(20))
def test_random_batches_with_constraints(seed: int) -> None:
    """Constraint failures produce different error shapes than type failures."""
    rng = random.Random(seed + 5000)
    plan = CompiledValidationPlan(Constrained, SourceFormat.JSONL)
    rows = [
        {
            "id": rng.choice([0, 5, -1, -99, "x"]),
            "name": rng.choice(["ok", "a", "", "fine"]),
            "score": rng.choice([0.0, 50.0, 100.0, 101.0, -0.5, "z"]),
        }
        for _ in range(rng.randint(1, 40))
    ]
    _same(plan.validate_batch(list(rows), start_index=3), _oracle(plan, rows, 3))


@pytest.mark.parametrize("seed", range(20))
def test_random_batches_with_optionals(seed: int) -> None:
    """None-able fields exercise a different error/loc shape."""
    rng = random.Random(seed + 9000)
    plan = CompiledValidationPlan(Optionals, SourceFormat.JSONL)
    rows = [
        {
            "id": rng.choice([1, 2, "bad", None]),
            "note": rng.choice([None, "hi", 5]),
            "when": rng.choice([None, "2024-01-01T00:00:00", "not-a-date", 7]),
        }
        for _ in range(rng.randint(1, 40))
    ]
    _same(plan.validate_batch(list(rows), start_index=0), _oracle(plan, rows, 0))


@pytest.mark.parametrize("bad_count", [0, 1, 2, 5, 25, 99, 100])
def test_every_density_of_bad_rows(bad_count: int) -> None:
    """From all-good to all-bad, the split must stay faithful."""
    rng = random.Random(bad_count)
    n = 100
    rows = [dict(_GOOD) for _ in range(n)]
    for i in rng.sample(range(n), bad_count):
        rows[i] = dict(_BAD)
    plan = CompiledValidationPlan(Row, SourceFormat.JSONL)
    got = plan.validate_batch(list(rows), start_index=0)
    _same(got, _oracle(plan, rows, 0))
    assert sum(1 for r in got if not r.valid) == bad_count

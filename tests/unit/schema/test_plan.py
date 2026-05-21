"""Tests for streamval.schema.plan."""

from __future__ import annotations

from pydantic import BaseModel

from streamval.schema.coerce import SourceFormat
from streamval.schema.plan import CompiledValidationPlan, get_plan


class _User(BaseModel):
    id: int
    name: str
    score: float = 0.0


def test_validate_row_success_for_csv() -> None:
    plan = CompiledValidationPlan(_User, SourceFormat.CSV)
    res = plan.validate_row(0, {"id": "42", "name": "a", "score": "1.5"})
    assert res.valid is True
    assert res.data is not None
    assert res.data.id == 42
    assert res.data.score == 1.5


def test_validate_row_success_for_jsonl() -> None:
    plan = CompiledValidationPlan(_User, SourceFormat.JSONL)
    res = plan.validate_row(0, {"id": 42, "name": "a"})
    assert res.valid is True
    assert res.data is not None
    assert res.data.id == 42


def test_validate_row_invalid_returns_field_errors() -> None:
    plan = CompiledValidationPlan(_User, SourceFormat.CSV)
    res = plan.validate_row(3, {"id": "not-a-number", "name": "a"})
    assert res.valid is False
    assert res.row_index == 3
    assert any(e.field == "id" for e in res.errors)


def test_validate_row_missing_required_field() -> None:
    plan = CompiledValidationPlan(_User, SourceFormat.CSV)
    res = plan.validate_row(0, {"id": "1"})
    assert res.valid is False
    assert any(e.field == "name" for e in res.errors)


def test_get_plan_caches_per_model_and_format() -> None:
    p1 = get_plan(_User, SourceFormat.CSV)
    p2 = get_plan(_User, SourceFormat.CSV)
    assert p1 is p2

    p3 = get_plan(_User, SourceFormat.JSONL)
    assert p3 is not p1
    assert p3.source_format is SourceFormat.JSONL


def test_plan_field_names_match_model() -> None:
    plan = get_plan(_User, SourceFormat.CSV)
    assert set(plan.field_names) == {"id", "name", "score"}

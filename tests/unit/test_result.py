"""Tests for streamval.core.result."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from streamval.core.result import (
    FieldError,
    StreamValidationError,
    ValidationResult,
)


class _User(BaseModel):
    id: int
    name: str


def test_field_error_is_frozen() -> None:
    err = FieldError(field="id", value="abc", message="bad", error_type="int_parsing")
    with pytest.raises(Exception):
        err.field = "other"  # type: ignore[misc]
    assert "id" in str(err)
    assert "int_parsing" in str(err)


def test_validation_result_success() -> None:
    user = _User(id=1, name="a")
    res = ValidationResult.success(0, {"id": 1, "name": "a"}, user)
    assert res.valid is True
    assert res.data is user
    assert res.errors == []
    assert "valid=True" in str(res)


def test_validation_result_from_pydantic_error() -> None:
    try:
        _User(id="not-an-int", name=123)  # type: ignore[arg-type]
    except ValidationError as exc:
        res = ValidationResult.from_pydantic_error(
            7, {"id": "not-an-int", "name": 123}, exc
        )
    assert res.valid is False
    assert res.row_index == 7
    assert res.data is None
    assert len(res.errors) >= 1
    fields = {e.field for e in res.errors}
    assert "id" in fields
    assert all(e.error_type for e in res.errors)
    assert "valid=False" in str(res)


def test_validation_result_is_frozen() -> None:
    res = ValidationResult.success(0, {}, _User(id=1, name="a"))
    with pytest.raises(Exception):
        res.row_index = 9  # type: ignore[misc]


def test_stream_validation_error_str_lists_top_fields() -> None:
    try:
        _User(id="x", name=1)  # type: ignore[arg-type]
    except ValidationError as exc:
        bad = ValidationResult.from_pydantic_error(0, {}, exc)
    err = StreamValidationError("validation failed", results=[bad, bad, bad])
    text = str(err)
    assert "3 invalid rows" in text
    assert "validation failed" in text


def test_stream_validation_error_no_results() -> None:
    err = StreamValidationError("nothing yet")
    assert str(err) == "nothing yet"
    assert err.results == []
    assert err.stats is None

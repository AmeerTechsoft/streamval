"""Tests for the collect strategy."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from streamval.core.result import StreamValidationError, ValidationResult
from streamval.strategies.collect import CollectHandler


class _M(BaseModel):
    id: int


def _bad(idx: int) -> ValidationResult:
    try:
        _M(id="x")  # type: ignore[arg-type]
    except ValidationError as exc:
        return ValidationResult.from_pydantic_error(idx, {"id": "x"}, exc)
    raise AssertionError


def _good(idx: int) -> ValidationResult:
    return ValidationResult.success(idx, {"id": idx}, _M(id=idx))


async def test_collect_emits_all_rows() -> None:
    h = CollectHandler()
    assert (await h.handle(_good(0))) is not None
    assert (await h.handle(_bad(1))) is not None
    await h.finalize()
    assert h.summary["invalid_count"] == 1
    assert h.invalid_results[0].row_index == 1


async def test_collect_unlimited_never_raises() -> None:
    h = CollectHandler(max_errors=None)
    for i in range(50):
        await h.handle(_bad(i))
    await h.finalize()
    assert h.summary["invalid_count"] == 50


async def test_collect_max_errors_enforced_in_finalize() -> None:
    h = CollectHandler(max_errors=2)
    for i in range(5):
        await h.handle(_bad(i))
    with pytest.raises(StreamValidationError) as ei:
        await h.finalize()
    assert "max_errors=2" in ei.value.message
    assert len(ei.value.results) == 5


async def test_collect_at_limit_does_not_raise() -> None:
    h = CollectHandler(max_errors=3)
    for i in range(3):
        await h.handle(_bad(i))
    await h.finalize()

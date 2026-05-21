"""Tests for the fail_fast strategy."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from streamval.core.result import StreamValidationError, ValidationResult
from streamval.strategies.fail_fast import FailFastHandler


class _M(BaseModel):
    id: int


def _bad(idx: int = 0) -> ValidationResult:
    try:
        _M(id="x")  # type: ignore[arg-type]
    except ValidationError as exc:
        return ValidationResult.from_pydantic_error(idx, {"id": "x"}, exc)
    raise AssertionError


def _good(idx: int = 0) -> ValidationResult:
    return ValidationResult.success(idx, {"id": idx}, _M(id=idx))


async def test_fail_fast_passes_valid_rows() -> None:
    h = FailFastHandler()
    out = await h.handle(_good(1))
    assert out is not None
    assert out.valid is True


async def test_fail_fast_raises_on_first_invalid_row() -> None:
    h = FailFastHandler()
    await h.handle(_good(0))
    with pytest.raises(StreamValidationError) as ei:
        await h.handle(_bad(1))
    assert ei.value.results[0].row_index == 1


async def test_fail_fast_finalize_is_noop() -> None:
    h = FailFastHandler()
    await h.finalize()
    assert h.summary["strategy"] == "fail_fast"

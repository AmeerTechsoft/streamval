"""Tests for the skip strategy."""

from __future__ import annotations

import logging

from pydantic import BaseModel, ValidationError

from streamval.core.result import ValidationResult
from streamval.strategies.skip import SkipHandler


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


async def test_skip_emits_only_valid(caplog) -> None:
    h = SkipHandler()
    caplog.set_level(logging.WARNING, logger="streamval")
    assert (await h.handle(_good(0))) is not None
    assert (await h.handle(_bad(1))) is None
    assert (await h.handle(_good(2))) is not None
    await h.finalize()
    assert h.summary["skipped"] == 1
    msgs = [r.getMessage() for r in caplog.records]
    assert any("Row 1 skipped" in m for m in msgs)


async def test_skip_no_invalid_rows() -> None:
    h = SkipHandler()
    for i in range(5):
        out = await h.handle(_good(i))
        assert out is not None
    await h.finalize()
    assert h.summary["skipped"] == 0

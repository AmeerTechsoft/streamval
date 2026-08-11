"""Tests for the synchronous strategy entry point.

``StrategyHandler.handle_sync`` exists so the per-row hot loop does not
allocate a coroutine for every row. Built-in strategies implement it
directly and advertise ``sync_safe = True``; third-party handlers that
only implement ``handle`` must keep working through the base-class
default.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from streamval.core.result import ValidationResult
from streamval.strategies import build_handler
from streamval.strategies.base import StrategyHandler


class _User(BaseModel):
    id: int


def _ok(row_index: int = 0) -> ValidationResult:
    return ValidationResult.success(row_index, {"id": row_index}, _User(id=row_index))


@pytest.mark.parametrize("name", ["fail_fast", "collect", "skip"])
def test_builtin_handlers_are_sync_safe(name: str) -> None:
    handler = build_handler(name, max_errors=None)  # type: ignore[arg-type]
    assert handler.sync_safe is True
    assert handler.handle_sync(_ok()) is not None


@pytest.mark.parametrize("name", ["fail_fast", "collect", "skip"])
async def test_sync_and_async_paths_agree(name: str) -> None:
    result = _ok(3)
    a = build_handler(name, max_errors=None)  # type: ignore[arg-type]
    b = build_handler(name, max_errors=None)  # type: ignore[arg-type]
    assert a.handle_sync(result) == await b.handle(result)


class _CustomAsyncOnly(StrategyHandler):
    """A third-party handler that only implements ``handle``."""

    def __init__(self) -> None:
        self.seen: list[int] = []

    async def handle(self, result: ValidationResult) -> ValidationResult | None:
        self.seen.append(result.row_index)
        return result

    async def finalize(self) -> None:
        return None

    @property
    def summary(self) -> dict[str, Any]:
        return {"strategy": "custom"}


def test_custom_handler_without_handle_sync_still_works() -> None:
    handler = _CustomAsyncOnly()
    assert handler.sync_safe is False
    assert handler.handle_sync(_ok(5)) is not None
    assert handler.seen == [5]


class _ReallyAwaits(_CustomAsyncOnly):
    async def handle(self, result: ValidationResult) -> ValidationResult | None:
        import asyncio

        await asyncio.sleep(0)
        return result


def test_handler_that_actually_awaits_raises_on_sync_path() -> None:
    with pytest.raises(RuntimeError, match="non-blocking strategy handlers"):
        _ReallyAwaits().handle_sync(_ok())

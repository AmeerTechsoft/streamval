"""``fail_fast`` strategy: raise on the first invalid row."""

from __future__ import annotations

from typing import Any

from streamval.core.result import StreamValidationError, ValidationResult
from streamval.strategies.base import StrategyHandler


class FailFastHandler(StrategyHandler):
    """Raises :class:`StreamValidationError` on the first invalid row.

    Valid rows are forwarded unchanged.
    """

    sync_safe = True

    def __init__(self) -> None:
        self._raised: bool = False

    def handle_sync(self, result: ValidationResult) -> ValidationResult | None:
        if not result.valid:
            self._raised = True
            raise StreamValidationError(
                f"Row {result.row_index} failed validation",
                results=[result],
            )
        return result

    async def handle(self, result: ValidationResult) -> ValidationResult | None:
        return self.handle_sync(result)

    async def finalize(self) -> None:
        return None

    @property
    def summary(self) -> dict[str, Any]:
        return {"strategy": "fail_fast", "raised": self._raised}

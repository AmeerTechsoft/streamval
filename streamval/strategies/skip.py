"""``skip`` strategy: drop invalid rows and log them at WARNING level."""

from __future__ import annotations

import logging
from typing import Any

from streamval.core.result import ValidationResult
from streamval.strategies.base import StrategyHandler

logger = logging.getLogger("streamval")


class SkipHandler(StrategyHandler):
    """Yields valid rows only; logs each skipped row at WARNING level."""

    def __init__(self) -> None:
        self._skipped: int = 0

    async def handle(self, result: ValidationResult) -> ValidationResult | None:
        if not result.valid:
            self._skipped += 1
            logger.warning(
                "Row %d skipped: %d field errors",
                result.row_index,
                len(result.errors),
            )
            return None
        return result

    async def finalize(self) -> None:
        return None

    @property
    def summary(self) -> dict[str, Any]:
        return {"strategy": "skip", "skipped": self._skipped}

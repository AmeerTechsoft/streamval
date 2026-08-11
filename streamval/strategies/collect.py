"""``collect`` strategy: gather every error, optionally cap with ``max_errors``."""

from __future__ import annotations

from typing import Any

from streamval.core.result import StreamValidationError, ValidationResult
from streamval.strategies.base import StrategyHandler


class CollectHandler(StrategyHandler):
    """Forwards every row and accumulates the invalid ones.

    If ``max_errors`` is set and exceeded by the time :meth:`finalize` is
    called, raises :class:`StreamValidationError` with all collected
    invalid results attached.

    Args:
        max_errors: Optional cap on accumulated invalid rows. ``None``
            means unlimited; the caller is expected to inspect stats.
    """

    sync_safe = True

    def __init__(self, max_errors: int | None = None) -> None:
        self._max_errors = max_errors
        self._invalid: list[ValidationResult] = []

    def handle_sync(self, result: ValidationResult) -> ValidationResult | None:
        if not result.valid:
            self._invalid.append(result)
        return result

    async def handle(self, result: ValidationResult) -> ValidationResult | None:
        return self.handle_sync(result)

    async def finalize(self) -> None:
        if self._max_errors is not None and len(self._invalid) > self._max_errors:
            raise StreamValidationError(
                f"max_errors={self._max_errors} exceeded "
                f"({len(self._invalid)} invalid rows)",
                results=self._invalid,
            )

    @property
    def summary(self) -> dict[str, Any]:
        return {
            "strategy": "collect",
            "max_errors": self._max_errors,
            "invalid_count": len(self._invalid),
        }

    @property
    def invalid_results(self) -> list[ValidationResult]:
        """Read-only view of accumulated invalid results."""
        return list(self._invalid)

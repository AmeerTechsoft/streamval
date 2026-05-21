"""Strategy protocol and the ``ErrorStrategy`` literal.

A :class:`StrategyHandler` decides what happens to each
:class:`ValidationResult` produced by the validator: emit it, drop it,
or raise. Three concrete strategies live alongside this module.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Literal

from streamval.core.result import ValidationResult

ErrorStrategy = Literal["fail_fast", "collect", "skip"]
"""Allowed string-name for the built-in error strategies."""


class StrategyHandler(ABC):
    """Abstract base for per-row error-handling strategies.

    Subclasses must implement :meth:`handle` (called per row) and
    :meth:`finalize` (called once after the stream is exhausted), plus
    expose a :attr:`summary` mapping for diagnostics.
    """

    @abstractmethod
    async def handle(self, result: ValidationResult) -> ValidationResult | None:
        """Process a single :class:`ValidationResult`.

        Returns:
            The result to emit downstream, or ``None`` to drop the row.
        """

    @abstractmethod
    async def finalize(self) -> None:
        """Hook called once the stream is exhausted."""

    @property
    @abstractmethod
    def summary(self) -> dict[str, Any]:
        """Diagnostic summary the strategy chooses to expose."""

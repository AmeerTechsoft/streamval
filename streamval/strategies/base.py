"""Strategy protocol and the ``ErrorStrategy`` literal.

A :class:`StrategyHandler` decides what happens to each
:class:`ValidationResult` produced by the validator: emit it, drop it,
or raise. Three concrete strategies live alongside this module.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Literal

from streamval.core.result import ValidationResult

ErrorStrategy = Literal["fail_fast", "collect", "skip"]
"""Allowed string-name for the built-in error strategies."""


class StrategyHandler(ABC):
    """Abstract base for per-row error-handling strategies.

    Subclasses must implement :meth:`handle` (called per row) and
    :meth:`finalize` (called once after the stream is exhausted), plus
    expose a :attr:`summary` mapping for diagnostics.

    Handlers that never actually await anything should also override
    :meth:`handle_sync` and set :attr:`sync_safe` to ``True``. The
    validator then calls the sync method directly on every path, which
    avoids allocating a coroutine object and raising ``StopIteration``
    once per row. All three built-in strategies do this.
    """

    sync_safe: ClassVar[bool] = False
    """``True`` when :meth:`handle_sync` is a real synchronous implementation.

    Leave ``False`` (the default) for handlers that genuinely await; the
    validator will then drive them through the async path.
    """

    @abstractmethod
    async def handle(self, result: ValidationResult) -> ValidationResult | None:
        """Process a single :class:`ValidationResult`.

        Returns:
            The result to emit downstream, or ``None`` to drop the row.
        """

    def handle_sync(self, result: ValidationResult) -> ValidationResult | None:
        """Synchronous per-row entry point.

        The default implementation drives :meth:`handle` by hand, which
        works for any handler that never awaits a real coroutine.
        Subclasses that set :attr:`sync_safe` override this with a plain
        synchronous body so the hot loop allocates no coroutine at all.

        Returns:
            The result to emit downstream, or ``None`` to drop the row.

        Raises:
            RuntimeError: If the handler awaited a real coroutine, which
                the synchronous streaming path cannot service.
        """
        coro = self.handle(result)
        try:
            coro.send(None)
        except StopIteration as exc:
            return exc.value  # type: ignore[no-any-return]
        coro.close()
        raise RuntimeError(
            "Sync streaming requires non-blocking strategy handlers; "
            "the active handler awaited a real coroutine. Use astream_* "
            "instead."
        )

    @abstractmethod
    async def finalize(self) -> None:
        """Hook called once the stream is exhausted."""

    @property
    @abstractmethod
    def summary(self) -> dict[str, Any]:
        """Diagnostic summary the strategy chooses to expose."""

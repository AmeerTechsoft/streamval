"""Error-handling strategies.

Three pluggable strategies decide what happens when a row fails
validation: ``fail_fast`` raises immediately, ``collect`` accumulates
errors, and ``skip`` drops invalid rows.
"""

from __future__ import annotations

from streamval.strategies.base import ErrorStrategy, StrategyHandler
from streamval.strategies.collect import CollectHandler
from streamval.strategies.fail_fast import FailFastHandler
from streamval.strategies.skip import SkipHandler


def build_handler(
    strategy: ErrorStrategy,
    *,
    max_errors: int | None = None,
) -> StrategyHandler:
    """Construct the concrete handler matching a strategy name.

    Args:
        strategy: One of ``"fail_fast"``, ``"collect"``, ``"skip"``.
        max_errors: Forwarded to :class:`CollectHandler`; ignored otherwise.

    Returns:
        A fresh :class:`StrategyHandler` instance.

    Raises:
        ValueError: If ``strategy`` is not one of the allowed names.
    """
    if strategy == "fail_fast":
        return FailFastHandler()
    if strategy == "collect":
        return CollectHandler(max_errors=max_errors)
    if strategy == "skip":
        return SkipHandler()
    raise ValueError(f"Unknown error strategy: {strategy!r}")


__all__ = [
    "CollectHandler",
    "ErrorStrategy",
    "FailFastHandler",
    "SkipHandler",
    "StrategyHandler",
    "build_handler",
]

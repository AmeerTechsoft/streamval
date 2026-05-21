"""Per-format type coercion.

Different source formats give us values with different starting types:

* **CSV** — every value arrives as ``str``. We try to coerce the raw
  string into the target annotation (``int``, ``float``, ``bool``,
  ``datetime``, ``date``) before handing the row to Pydantic, so error
  messages talk about the target type rather than a raw string parse
  failure.
* **JSONL** — values may already be typed (``int``, ``float``,
  ``list``, etc.); only ``datetime``/``date`` strings are pre-coerced.
* **Parquet / Arrow** — values arrive natively typed via pyarrow; we
  only normalise null sentinels.

The :func:`coerce_row` function is the single entry point used by the
schema plan.
"""

from __future__ import annotations

import datetime as _dt
from enum import StrEnum
from typing import Any, get_args, get_origin

from pydantic import BaseModel


class SourceFormat(StrEnum):
    """Format-of-origin tag used to choose a coercion path."""

    CSV = "csv"
    JSONL = "jsonl"
    PARQUET = "parquet"
    ARROW = "arrow"


_TRUTHY = {"true", "t", "yes", "y", "1"}
_FALSY = {"false", "f", "no", "n", "0"}


def coerce_row(
    raw: dict[str, Any],
    model: type[BaseModel],
    fmt: SourceFormat,
) -> dict[str, Any]:
    """Return a new dict with values massaged toward the model's annotations.

    The function never raises on coercion failure — it leaves problematic
    values untouched so Pydantic can produce the canonical error.

    Args:
        raw: The row as produced by an adapter.
        model: The target Pydantic model class.
        fmt: The source format (governs how aggressive coercion is).

    Returns:
        A new dict suitable for ``model.model_validate(...)``.
    """
    fields = model.model_fields
    out: dict[str, Any] = dict(raw)

    for name, info in fields.items():
        if name not in out:
            continue
        value = out[name]
        if value is None:
            continue
        target = info.annotation
        if target is None:
            continue
        out[name] = _coerce_value(value, target, fmt)

    return out


def _coerce_value(value: Any, target: Any, fmt: SourceFormat) -> Any:
    inner_target = _strip_optional(target)

    if fmt is SourceFormat.CSV:
        if isinstance(value, str):
            return _coerce_str(value, inner_target)
        return value

    if fmt is SourceFormat.JSONL:
        if isinstance(value, str) and inner_target in (_dt.datetime, _dt.date):
            return _coerce_str(value, inner_target)
        return value

    return value


def _strip_optional(target: Any) -> Any:
    origin = get_origin(target)
    if origin is None:
        return target
    args = [a for a in get_args(target) if a is not type(None)]
    if len(args) == 1:
        return args[0]
    return target


def _coerce_str(value: str, target: Any) -> Any:
    if target is str:
        return value
    s = value.strip()
    if s == "":
        return value

    if target is int:
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return value
    if target is float:
        try:
            return float(s)
        except ValueError:
            return value
    if target is bool:
        low = s.lower()
        if low in _TRUTHY:
            return True
        if low in _FALSY:
            return False
        return value
    if target is _dt.datetime:
        try:
            return _dt.datetime.fromisoformat(s)
        except ValueError:
            return value
    if target is _dt.date:
        try:
            return _dt.date.fromisoformat(s)
        except ValueError:
            return value
    return value


__all__ = ["SourceFormat", "coerce_row"]

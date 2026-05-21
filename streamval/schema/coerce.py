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
import weakref
from enum import StrEnum
from typing import Any, get_args, get_origin

from pydantic import BaseModel


class SourceFormat(StrEnum):
    """Format-of-origin tag used to choose a coercion path."""

    CSV = "csv"
    JSONL = "jsonl"
    PARQUET = "parquet"
    ARROW = "arrow"
    HTTP_NDJSON = "http_ndjson"


_TRUTHY = {"true", "t", "yes", "y", "1"}
_FALSY = {"false", "f", "no", "n", "0"}

# Per-model cache: model class → {field_name: stripped target type}.
_FIELD_TARGETS: weakref.WeakKeyDictionary[
    type[BaseModel], tuple[tuple[str, Any], ...]
] = weakref.WeakKeyDictionary()


def _field_targets(model: type[BaseModel]) -> tuple[tuple[str, Any], ...]:
    cached = _FIELD_TARGETS.get(model)
    if cached is not None:
        return cached
    items: list[tuple[str, Any]] = []
    for name, info in model.model_fields.items():
        target = info.annotation
        if target is None:
            continue
        items.append((name, _strip_optional(target)))
    out = tuple(items)
    _FIELD_TARGETS[model] = out
    return out


def coerce_row(
    raw: dict[str, Any],
    model: type[BaseModel],
    fmt: SourceFormat,
) -> dict[str, Any]:
    """Return a new dict with values massaged toward the model's annotations.

    The function never raises on coercion failure — it leaves problematic
    values untouched so Pydantic can produce the canonical error.

    Field-target info is cached per model class so the hot loop avoids
    repeated ``typing.get_origin`` calls.

    Args:
        raw: The row as produced by an adapter.
        model: The target Pydantic model class.
        fmt: The source format (governs how aggressive coercion is).

    Returns:
        A new dict suitable for ``model.model_validate(...)``. The
        input dict is returned untouched when no coercion is required
        (PARQUET / ARROW fast path).
    """
    # PARQUET / ARROW: values arrive natively typed via pyarrow. No
    # Python-side coercion is needed; just pass the dict through.
    if fmt is SourceFormat.PARQUET or fmt is SourceFormat.ARROW:
        return raw

    targets = _field_targets(model)

    if fmt is SourceFormat.CSV:
        out: dict[str, Any] = dict(raw)
        for name, target in targets:
            value = out.get(name)
            if value is None or not isinstance(value, str):
                continue
            out[name] = _coerce_str(value, target)
        return out

    # JSONL / HTTP_NDJSON: values arrive JSON-typed; only datetime /
    # date strings need help.
    out = dict(raw)
    for name, target in targets:
        if target is not _dt.datetime and target is not _dt.date:
            continue
        value = out.get(name)
        if isinstance(value, str):
            out[name] = _coerce_str(value, target)
    return out


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

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

import pyarrow as pa
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
        # Copy lazily: when the batch has already been cast column-wise
        # by :func:`cast_csv_batch`, every value here is non-str and
        # nothing changes, so we hand back ``raw`` without allocating.
        out: dict[str, Any] | None = None
        for name, target in targets:
            value = raw.get(name)
            if value is None or not isinstance(value, str):
                continue
            new = _coerce_str(value, target)
            if new is not value:
                if out is None:
                    out = dict(raw)
                out[name] = new
        return raw if out is None else out

    # JSONL / HTTP_NDJSON: values arrive JSON-typed; only datetime /
    # date strings need help.
    out = None
    for name, target in targets:
        if target is not _dt.datetime and target is not _dt.date:
            continue
        value = raw.get(name)
        if isinstance(value, str):
            new = _coerce_str(value, target)
            if new is not value:
                if out is None:
                    out = dict(raw)
                out[name] = new
    return raw if out is None else out


def _arrow_type_for(target: Any) -> pa.DataType | None:
    """Arrow type to cast a string column to, or ``None`` to leave it alone.

    Mirrors the dispatch in :func:`_coerce_str` exactly — the two must
    agree on which targets are handled, or a column would be cast to a
    type the per-row path would never have produced.
    """
    if target is int:
        return pa.int64()
    if target is float:
        return pa.float64()
    if target is bool:
        return pa.bool_()
    if target is _dt.datetime:
        return pa.timestamp("us")
    if target is _dt.date:
        return pa.date32()
    return None


_CAST_MAP: weakref.WeakKeyDictionary[
    type[BaseModel], tuple[tuple[str, pa.DataType], ...]
] = weakref.WeakKeyDictionary()


def _arrow_cast_map(model: type[BaseModel]) -> tuple[tuple[str, pa.DataType], ...]:
    cached = _CAST_MAP.get(model)
    if cached is not None:
        return cached
    items: list[tuple[str, pa.DataType]] = []
    for name, target in _field_targets(model):
        arrow_type = _arrow_type_for(target)
        if arrow_type is not None:
            items.append((name, arrow_type))
    out = tuple(items)
    _CAST_MAP[model] = out
    return out


def cast_csv_batch(
    batch: pa.RecordBatch,
    model: type[BaseModel],
) -> pa.RecordBatch:
    """Vectorised pre-coercion for a string-typed CSV ``RecordBatch``.

    CSV columns arrive as ``Utf8`` (the scan uses
    ``infer_schema_length=0`` so polars never aborts a file over one bad
    cell). Converting them one Python value at a time via
    :func:`_coerce_str` is the dominant cost of the CSV batch path, so
    here we cast whole columns in Arrow instead.

    Each column is cast independently and **safely**:

    * If the cast succeeds, every cell in that column parsed cleanly and
      the Arrow result is identical to what :func:`_coerce_str` would
      have produced value by value. (Arrow is strictly *stricter* than
      ``_coerce_str`` — it rejects padded values like ``" 5 "``, ``bool``
      spellings like ``"yes"``, and ``int``-via-``float`` strings like
      ``"42.0"``. It never accepts something and disagrees.)
    * If any cell fails to parse, the cast raises and the column is left
      as strings. :func:`coerce_row` then handles that column per row
      exactly as before, so a single bad cell degrades one column to the
      old path rather than failing the file.

    Args:
        batch: A CSV ``RecordBatch`` with string-typed columns.
        model: The target Pydantic model, used to pick per-column types.

    Returns:
        A ``RecordBatch`` with castable columns converted. Returns the
        input unchanged when nothing could be cast.
    """
    cast_map = _arrow_cast_map(model)
    if not cast_map:
        return batch

    names = batch.schema.names
    columns: list[pa.Array] | None = None
    for name, arrow_type in cast_map:
        try:
            idx = names.index(name)
        except ValueError:
            continue
        col = batch.column(idx)
        if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            continue
        try:
            cast_col = col.cast(arrow_type, safe=True)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            # At least one cell doesn't parse — leave the column as
            # strings and let the per-row path report it.
            continue
        if columns is None:
            columns = list(batch.columns)
        columns[idx] = cast_col

    if columns is None:
        return batch

    schema = pa.schema(
        [
            batch.schema.field(i).with_type(columns[i].type)
            for i in range(len(columns))
        ]
    )
    return pa.RecordBatch.from_arrays(columns, schema=schema)


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


__all__ = ["SourceFormat", "cast_csv_batch", "coerce_row"]

"""Tests for the vectorised CSV column cast.

The whole optimisation rests on one property: **when an Arrow cast
succeeds, its result is identical to what the per-row ``_coerce_str``
path would have produced.** Arrow is strictly stricter — it rejects
values ``_coerce_str`` accepts — so the fallback is always safe, but a
disagreement on an accepted value would silently corrupt data. These
tests pin that property down.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest
from pydantic import BaseModel

from streamval.schema.coerce import SourceFormat, cast_csv_batch, coerce_row


class Row(BaseModel):
    id: int
    name: str
    value: float
    active: bool


def _batch(**cols: list[str | None]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {k: pa.array(v, type=pa.string()) for k, v in cols.items()}
    )


def test_clean_columns_are_cast_to_native_types() -> None:
    batch = _batch(
        id=["1", "2"],
        name=["a", "b"],
        value=["1.5", "2.5"],
        active=["true", "false"],
    )
    out = cast_csv_batch(batch, Row)
    assert out.schema.field("id").type == pa.int64()
    assert out.schema.field("value").type == pa.float64()
    assert out.schema.field("active").type == pa.bool_()
    # str fields are left alone
    assert out.schema.field("name").type == pa.string()
    assert out.to_pylist() == [
        {"id": 1, "name": "a", "value": 1.5, "active": True},
        {"id": 2, "name": "b", "value": 2.5, "active": False},
    ]


def test_one_bad_cell_degrades_only_its_own_column() -> None:
    batch = _batch(
        id=["1", "oops", "3"],
        name=["a", "b", "c"],
        value=["1.5", "2.5", "3.5"],
        active=["true", "true", "true"],
    )
    out = cast_csv_batch(batch, Row)
    # the dirty column stays string...
    assert out.schema.field("id").type == pa.string()
    # ...while its clean neighbours are still cast
    assert out.schema.field("value").type == pa.float64()
    assert out.schema.field("active").type == pa.bool_()


def test_nulls_survive_the_cast() -> None:
    batch = _batch(
        id=["1", None, "3"],
        name=["a", "b", "c"],
        value=["1.0", "2.0", None],
        active=["true", "false", "true"],
    )
    out = cast_csv_batch(batch, Row).to_pylist()
    assert out[1]["id"] is None
    assert out[2]["value"] is None


def test_missing_and_extra_columns_are_ignored() -> None:
    batch = _batch(id=["1"], unrelated=["x"])
    out = cast_csv_batch(batch, Row)
    assert out.schema.field("id").type == pa.int64()
    assert out.schema.field("unrelated").type == pa.string()


def test_no_castable_fields_returns_input_unchanged() -> None:
    class AllStrings(BaseModel):
        a: str
        b: str

    batch = _batch(a=["1"], b=["2"])
    assert cast_csv_batch(batch, AllStrings) is batch


# --- the core equivalence property ---

_VALUES: dict[str, list[str]] = {
    "int": [
        "1", "42", "-7", "0", " 5 ", "\t6\n", "42.0", "-3.0", "42.7",
        "", "   ", "abc", "9" * 22, "1e3",
    ],
    "float": [
        "1.5", "-0.25", "1e5", "0", " 2.5 ", "\xa03.5\xa0", "", "abc", "inf",
    ],
    "bool": [
        "true", "false", "True", "FALSE", "1", "0", "yes", "no", "Y", "N",
        "t", "f", " yes ", "", "x", "maybe",
    ],
    "datetime": [
        "2024-01-01T00:00:00", "2024-01-01 12:30:00", " 2024-01-01 ",
        "", "abc",
    ],
    "date": [
        "2024-01-01", " 2024-01-01 ", "2024-1-1", "", "abc",
        "2024-01-01T00:00:00",
    ],
}


class _Int(BaseModel):
    v: int


class _Float(BaseModel):
    v: float


class _Bool(BaseModel):
    v: bool


class _Dt(BaseModel):
    v: dt.datetime


class _Date(BaseModel):
    v: dt.date


_MODELS = {
    "int": _Int,
    "float": _Float,
    "bool": _Bool,
    "datetime": _Dt,
    "date": _Date,
}


@pytest.mark.parametrize("kind", sorted(_VALUES))
def test_successful_cast_agrees_with_per_row_coercion(kind: str) -> None:
    """Where Arrow accepts a value, it must produce what _coerce_str does."""
    model = _MODELS[kind]
    for raw in _VALUES[kind]:
        batch = _batch(v=[raw])
        cast = cast_csv_batch(batch, model)
        if cast.schema.field("v").type == pa.string():
            continue  # Arrow declined; the per-row path owns this value
        arrow_value = cast.to_pylist()[0]["v"]
        python_value = coerce_row({"v": raw}, model, SourceFormat.CSV)["v"]
        assert arrow_value == python_value, (
            f"{kind} {raw!r}: arrow gave {arrow_value!r}, "
            f"per-row path gives {python_value!r}"
        )


@pytest.mark.parametrize("kind", sorted(_VALUES))
def test_end_to_end_results_match_with_and_without_cast(kind: str) -> None:
    """A whole mixed column must validate the same either way."""
    model = _MODELS[kind]
    values = _VALUES[kind]
    batch = _batch(v=values)

    via_cast = cast_csv_batch(batch, model).to_pylist()
    via_rows = [
        coerce_row(row, model, SourceFormat.CSV) for row in batch.to_pylist()
    ]

    for i, (a, b) in enumerate(zip(via_cast, via_rows, strict=True)):
        a_v = coerce_row(a, model, SourceFormat.CSV)["v"]
        assert a_v == b["v"], f"{kind}[{i}] {values[i]!r}: {a_v!r} != {b['v']!r}"


# --- the fast path must actually engage, not just be correct ---


@pytest.mark.parametrize(
    ("column", "values", "expected_type", "expected"),
    [
        ("id", ["1", " 2 ", "\t3\n"], pa.int64(), [1, 2, 3]),
        ("id", ["1", "42.0", "-3.0"], pa.int64(), [1, 42, -3]),
        ("value", [" 1.5 ", "2.5", "\xa03.5\xa0"], pa.float64(), [1.5, 2.5, 3.5]),
        ("active", ["yes", "no", "Y"], pa.bool_(), [True, False, True]),
        ("active", ["t", "f", "TRUE"], pa.bool_(), [True, False, True]),
        ("active", [" yes ", "0", "1"], pa.bool_(), [True, False, True]),
    ],
)
def test_variant_representations_stay_on_the_fast_path(
    column: str,
    values: list[str],
    expected_type: pa.DataType,
    expected: list[object],
) -> None:
    """Padded numbers, worded bools and int-as-float must not decline.

    These are what real CSVs are full of; if they fall back to the
    per-row path the vectorised cast buys nothing in production.
    """
    cols: dict[str, list[str | None]] = {
        "id": ["1"] * len(values),
        "name": ["n"] * len(values),
        "value": ["1.0"] * len(values),
        "active": ["true"] * len(values),
    }
    cols[column] = list(values)
    out = cast_csv_batch(_batch(**cols), Row)
    assert out.schema.field(column).type == expected_type, (
        f"{column}={values} declined to the per-row path"
    )
    assert [r[column] for r in out.to_pylist()] == expected


def test_bool_nulls_survive_the_vocabulary_path() -> None:
    batch = _batch(
        id=["1", "2"], name=["a", "b"], value=["1.0", "2.0"], active=["yes", None]
    )
    out = cast_csv_batch(batch, Row)
    assert out.schema.field("active").type == pa.bool_()
    assert [r["active"] for r in out.to_pylist()] == [True, None]


def test_non_integral_float_declines_rather_than_truncating() -> None:
    """`_coerce_str` yields 42 for "42.7"; Arrow must decline, not disagree."""
    batch = _batch(id=["42.7"], name=["a"], value=["1.0"], active=["true"])
    out = cast_csv_batch(batch, Row)
    assert out.schema.field("id").type == pa.string()
    assert coerce_row({"id": "42.7"}, Row, SourceFormat.CSV)["id"] == 42


def test_coerce_row_returns_input_when_nothing_changes() -> None:
    """Already-cast rows must not pay for a dict copy."""
    row = {"id": 1, "name": "a", "value": 1.5, "active": True}
    assert coerce_row(row, Row, SourceFormat.CSV) is row

"""Tests for streamval.schema.coerce."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel

from streamval.schema.coerce import SourceFormat, coerce_row


class _M(BaseModel):
    id: int
    name: str
    value: float
    active: bool
    when: dt.datetime
    optional: int | None = None


def test_csv_coerces_strings_to_target_types() -> None:
    raw = {
        "id": "42",
        "name": "alice",
        "value": "1.5",
        "active": "true",
        "when": "2024-01-02T03:04:05",
        "optional": "7",
    }
    out = coerce_row(raw, _M, SourceFormat.CSV)
    assert out["id"] == 42
    assert out["name"] == "alice"
    assert out["value"] == 1.5
    assert out["active"] is True
    assert out["when"] == dt.datetime(2024, 1, 2, 3, 4, 5)
    assert out["optional"] == 7


def test_csv_bool_falsy_variants() -> None:
    for v in ["false", "FALSE", "no", "0", "F"]:
        out = coerce_row({"id": "1", "active": v}, _M, SourceFormat.CSV)
        assert out["active"] is False


def test_csv_leaves_unparseable_value_alone() -> None:
    out = coerce_row({"id": "abc"}, _M, SourceFormat.CSV)
    assert out["id"] == "abc"


def test_jsonl_keeps_already_typed_values() -> None:
    raw = {"id": 42, "value": 1.5, "active": True}
    out = coerce_row(raw, _M, SourceFormat.JSONL)
    assert out["id"] == 42
    assert out["value"] == 1.5
    assert out["active"] is True


def test_jsonl_coerces_iso_datetime_strings() -> None:
    raw = {"when": "2024-05-21T01:02:03"}
    out = coerce_row(raw, _M, SourceFormat.JSONL)
    assert out["when"] == dt.datetime(2024, 5, 21, 1, 2, 3)


def test_parquet_passes_through() -> None:
    raw = {"id": 1, "value": 2.5, "active": True}
    out = coerce_row(raw, _M, SourceFormat.PARQUET)
    assert out == raw


def test_optional_field_coerced_when_present() -> None:
    out = coerce_row({"id": "1", "optional": "9"}, _M, SourceFormat.CSV)
    assert out["optional"] == 9


def test_missing_field_left_untouched() -> None:
    out = coerce_row({"id": "1"}, _M, SourceFormat.CSV)
    assert "name" not in out

"""Tests for streamval.adapters.csv_adapter."""

from __future__ import annotations

from pathlib import Path

import pytest

from streamval.adapters import csv_adapter


def _write_csv(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "data.csv"
    p.write_text(content, encoding="utf-8", newline="")
    return p


async def test_basic_csv_yields_rows(tmp_path: Path) -> None:
    p = _write_csv(
        tmp_path,
        "id,name,value\n1,alice,1.5\n2,bob,2.5\n3,carol,3.5\n",
    )
    rows = [r async for r in csv_adapter.stream_rows(p)]
    assert rows == [
        {"id": "1", "name": "alice", "value": "1.5"},
        {"id": "2", "name": "bob", "value": "2.5"},
        {"id": "3", "name": "carol", "value": "3.5"},
    ]


async def test_csv_handles_quoted_commas(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, 'id,name\n1,"hello, world"\n2,plain\n')
    rows = [r async for r in csv_adapter.stream_rows(p)]
    assert rows[0]["name"] == "hello, world"
    assert rows[1]["name"] == "plain"


async def test_csv_alternate_delimiter(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a;b\n1;2\n3;4\n")
    rows = [r async for r in csv_adapter.stream_rows(p, delimiter=";")]
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


async def test_csv_no_trailing_newline(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a,b\n1,2\n3,4")
    rows = [r async for r in csv_adapter.stream_rows(p)]
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


async def test_csv_small_chunk_size(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a,b\n1,2\n3,4\n5,6\n")
    rows = [r async for r in csv_adapter.stream_rows(p, chunk_size=4)]
    assert rows == [
        {"a": "1", "b": "2"},
        {"a": "3", "b": "4"},
        {"a": "5", "b": "6"},
    ]


async def test_csv_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        async for _ in csv_adapter.stream_rows(tmp_path / "missing.csv"):
            pass


def test_csv_sync_wrapper(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a,b\n1,2\n3,4\n")
    rows = list(csv_adapter.stream_rows_sync(p))
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

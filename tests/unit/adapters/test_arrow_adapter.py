"""Tests for streamval.adapters.arrow_adapter."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.ipc as ipc
import pytest

from streamval.adapters import arrow_adapter


def _write_feather(tmp_path: Path, data: dict[str, list]) -> Path:
    p = tmp_path / "data.feather"
    feather.write_feather(pa.table(data), p)
    return p


def _write_arrow_stream(tmp_path: Path, data: dict[str, list]) -> Path:
    p = tmp_path / "data.arrow"
    table = pa.table(data)
    with pa.OSFile(str(p), "wb") as sink:
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
    return p


async def test_arrow_feather_file_format(tmp_path: Path) -> None:
    p = _write_feather(tmp_path, {"id": [1, 2, 3], "name": ["a", "b", "c"]})
    rows = [r async for r in arrow_adapter.stream_rows(p)]
    assert rows == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]


async def test_arrow_stream_format(tmp_path: Path) -> None:
    p = _write_arrow_stream(tmp_path, {"x": [10, 20, 30]})
    rows = [r async for r in arrow_adapter.stream_rows(p)]
    assert rows == [{"x": 10}, {"x": 20}, {"x": 30}]


async def test_arrow_column_projection(tmp_path: Path) -> None:
    p = _write_feather(tmp_path, {"a": [1, 2], "b": [3, 4]})
    rows = [r async for r in arrow_adapter.stream_rows(p, columns=["a"])]
    assert rows == [{"a": 1}, {"a": 2}]


async def test_arrow_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        async for _ in arrow_adapter.stream_rows(tmp_path / "missing.arrow"):
            pass


def test_arrow_sync_wrapper(tmp_path: Path) -> None:
    p = _write_feather(tmp_path, {"a": [1, 2]})
    rows = list(arrow_adapter.stream_rows_sync(p))
    assert rows == [{"a": 1}, {"a": 2}]

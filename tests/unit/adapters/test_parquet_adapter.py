"""Tests for streamval.adapters.parquet_adapter."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from streamval.adapters import parquet_adapter


def _write_parquet(tmp_path: Path, data: dict[str, list]) -> Path:
    p = tmp_path / "data.parquet"
    pq.write_table(pa.table(data), p)
    return p


async def test_parquet_basic(tmp_path: Path) -> None:
    p = _write_parquet(
        tmp_path,
        {"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.5, 2.5, 3.5]},
    )
    rows = [r async for r in parquet_adapter.stream_rows(p)]
    assert rows == [
        {"id": 1, "name": "a", "value": 1.5},
        {"id": 2, "name": "b", "value": 2.5},
        {"id": 3, "name": "c", "value": 3.5},
    ]


async def test_parquet_column_projection(tmp_path: Path) -> None:
    p = _write_parquet(tmp_path, {"id": [1, 2], "name": ["a", "b"], "drop": [9, 9]})
    rows = [r async for r in parquet_adapter.stream_rows(p, columns=["id", "name"])]
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


async def test_parquet_small_batch(tmp_path: Path) -> None:
    p = _write_parquet(tmp_path, {"id": list(range(10))})
    rows = [r async for r in parquet_adapter.stream_rows(p, batch_size=2)]
    assert [r["id"] for r in rows] == list(range(10))


async def test_parquet_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        async for _ in parquet_adapter.stream_rows(tmp_path / "nope.parquet"):
            pass


def test_parquet_sync_wrapper(tmp_path: Path) -> None:
    p = _write_parquet(tmp_path, {"id": [1, 2]})
    rows = list(parquet_adapter.stream_rows_sync(p))
    assert rows == [{"id": 1}, {"id": 2}]

"""End-to-end Parquet validation."""

from __future__ import annotations

import tracemalloc
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from streamval.core.validator import StreamValidator


class Row(BaseModel):
    id: int
    name: str
    value: float


def _write_parquet(path: Path, n: int) -> tuple[int, int]:
    """All rows valid (Parquet is strongly typed)."""
    ids = list(range(n))
    names = [f"n{i}" for i in range(n)]
    values = [float(i) for i in range(n)]
    table = pa.table({"id": ids, "name": names, "value": values})
    pq.write_table(table, path)
    return n, 0


def test_parquet_collect(tmp_path: Path) -> None:
    p = tmp_path / "data.parquet"
    valid, _ = _write_parquet(p, 5000)
    v = StreamValidator(Row, on_error="collect", batch_size=500)
    results = list(v.stream_parquet(p))
    assert len(results) == valid
    assert all(r.valid for r in results)
    assert v.stats.rows_total == valid


def test_parquet_memory_bounded(tmp_path: Path) -> None:
    p = tmp_path / "big.parquet"
    _write_parquet(p, 10000)
    tracemalloc.start()
    tracemalloc.reset_peak()
    v = StreamValidator(Row, on_error="skip", batch_size=1000)
    count = sum(1 for _ in v.stream_parquet(p))
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert count == 10000
    assert peak < 50 * 1024 * 1024


async def test_parquet_async(tmp_path: Path) -> None:
    p = tmp_path / "data.parquet"
    _write_parquet(p, 200)
    v = StreamValidator(Row, on_error="collect", batch_size=50)
    rows = [r async for r in v.astream_parquet(p)]
    assert len(rows) == 200

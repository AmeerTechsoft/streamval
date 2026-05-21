"""Integration tests for the Arrow fast path (PROMPT A3).

Covers:

* Correctness parity — row mode and batch mode produce identical
  :class:`ValidationResult` sequences for the same file.
* Throughput floors per the v0.2 spec.
* Mixed-validity behaviour (fallback to per-row validation when a
  batch contains an invalid row).
"""

from __future__ import annotations

import csv
import time
import tracemalloc
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import BaseModel

from streamval.core.validator import StreamValidator


class Row(BaseModel):
    id: int
    name: str
    value: float
    active: bool


def _write_csv(path: Path, n: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value", "active"])
        for i in range(n):
            w.writerow([i, f"n{i}", f"{i * 0.5:.3f}", "true"])


def _write_parquet(path: Path, n: int) -> None:
    table = pa.table(
        {
            "id": list(range(n)),
            "name": [f"n{i}" for i in range(n)],
            "value": [i * 0.5 for i in range(n)],
            "active": [True] * n,
        }
    )
    pq.write_table(table, path)


def test_csv_row_vs_batch_mode_produce_identical_results(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    _write_csv(p, 500)

    row_results = list(
        StreamValidator(Row, on_error="collect", use_arrow=False).stream_csv(p)
    )
    batch_results = list(
        StreamValidator(Row, on_error="collect", use_arrow=True).stream_csv(p)
    )

    assert len(row_results) == len(batch_results) == 500
    assert [r.valid for r in row_results] == [r.valid for r in batch_results]
    assert [r.row_index for r in row_results] == [
        r.row_index for r in batch_results
    ]
    for a, b in zip(row_results, batch_results, strict=True):
        assert a.data is not None and b.data is not None
        assert a.data.model_dump() == b.data.model_dump()


def test_parquet_row_vs_batch_mode_produce_identical_results(
    tmp_path: Path,
) -> None:
    p = tmp_path / "data.parquet"
    _write_parquet(p, 500)

    row_results = list(
        StreamValidator(Row, on_error="collect", use_arrow=False).stream_parquet(p)
    )
    batch_results = list(
        StreamValidator(Row, on_error="collect", use_arrow=True).stream_parquet(p)
    )
    assert len(row_results) == len(batch_results) == 500
    for a, b in zip(row_results, batch_results, strict=True):
        assert a.data is not None and b.data is not None
        assert a.data.model_dump() == b.data.model_dump()


def test_csv_batch_mode_throughput(tmp_path: Path) -> None:
    """Arrow batch mode should reach the spec's CI floor (>= 35k rps)."""
    p = tmp_path / "big.csv"
    n = 100_000
    _write_csv(p, n)

    v = StreamValidator(Row, on_error="skip", use_arrow=True, batch_size=10_000)
    t0 = time.perf_counter()
    count = sum(1 for _ in v.stream_csv(p))
    elapsed = time.perf_counter() - t0
    rps = count / elapsed
    print(f"\nCSV batch throughput: {rps:,.0f} rps ({elapsed:.2f}s for {count} rows)")
    assert count == n
    assert rps > 35_000, (
        f"CSV batch mode {rps:,.0f} rps below 35k floor"
    )


def test_parquet_batch_mode_throughput(tmp_path: Path) -> None:
    """Arrow batch mode for Parquet should reach the spec's floor (>= 45k rps)."""
    p = tmp_path / "big.parquet"
    n = 100_000
    _write_parquet(p, n)

    v = StreamValidator(Row, on_error="skip", use_arrow=True, batch_size=10_000)
    t0 = time.perf_counter()
    count = sum(1 for _ in v.stream_parquet(p))
    elapsed = time.perf_counter() - t0
    rps = count / elapsed
    print(
        f"\nParquet batch throughput: {rps:,.0f} rps "
        f"({elapsed:.2f}s for {count} rows)"
    )
    assert count == n
    assert rps > 45_000, (
        f"Parquet batch mode {rps:,.0f} rps below 45k floor"
    )


def test_csv_batch_mode_memory_bounded(tmp_path: Path) -> None:
    """Peak memory stays bounded even at higher batch sizes."""
    p = tmp_path / "big.csv"
    _write_csv(p, 50_000)
    v = StreamValidator(Row, on_error="skip", use_arrow=True, batch_size=5_000)
    tracemalloc.start()
    tracemalloc.reset_peak()
    count = sum(1 for _ in v.stream_csv(p))
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert count == 50_000
    assert peak < 50 * 1024 * 1024


def test_csv_batch_mode_handles_mixed_validity(tmp_path: Path) -> None:
    p = tmp_path / "mixed.csv"
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value", "active"])
        for i in range(100):
            if i % 9 == 0:
                w.writerow(["not-an-int", f"n{i}", "1.5", "true"])
            else:
                w.writerow([i, f"n{i}", "1.5", "true"])

    v = StreamValidator(
        Row, on_error="collect", use_arrow=True, batch_size=25
    )
    results = list(v.stream_csv(p))
    assert len(results) == 100
    invalid = [r for r in results if not r.valid]
    assert len(invalid) == 12  # rows 0, 9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99
    assert all(any(e.field == "id" for e in r.errors) for r in invalid)


@pytest.mark.parametrize("workers", [1, 4])
def test_csv_batch_mode_workers_preserve_order(
    tmp_path: Path, workers: int
) -> None:
    p = tmp_path / "data.csv"
    _write_csv(p, 1_000)

    async def _collect() -> list:
        v = StreamValidator(
            Row, on_error="collect", use_arrow=True, batch_size=100, workers=workers
        )
        out = []
        async for r in v.astream_csv(p):
            out.append(r)
        return out

    import asyncio

    out = asyncio.run(_collect())
    assert [r.row_index for r in out] == list(range(1000))

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
import os
import time
import tracemalloc
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import BaseModel

from streamval.core.validator import StreamValidator

_PERF = pytest.mark.skipif(
    os.environ.get("STREAMVAL_PERF") != "1",
    reason="aspirational CI-machine throughput floor; "
    "set STREAMVAL_PERF=1 to enforce. "
    "Override numeric floors via STREAMVAL_MIN_CSV_BATCH_RPS / "
    "STREAMVAL_MIN_PARQUET_BATCH_RPS.",
)


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


def test_csv_row_vs_batch_mode_agree_on_messy_values(tmp_path: Path) -> None:
    """Batch mode casts columns in Arrow; row mode coerces per value.

    Arrow declines padded numbers, ``int``-via-``float`` strings and
    non-canonical bool spellings, so those columns fall back to the
    per-row path. Both modes must still agree on every row — validity,
    parsed data, and which fields failed.
    """
    p = tmp_path / "messy.csv"
    rows = [
        # id,        name,  value,   active
        ("1", "clean", "1.5", "true"),
        (" 2 ", "padded-id", "2.5", "false"),  # Arrow rejects " 2 "
        ("3.0", "int-via-float", "3.5", "true"),  # Arrow rejects "3.0"
        ("4", "worded-bool", "4.5", "yes"),  # Arrow rejects "yes"
        ("5", "padded-float", " 5.5 ", "true"),  # Arrow rejects " 5.5 "
        ("nope", "bad-id", "6.5", "true"),  # invalid in both modes
        ("7", "empty-value", "", "true"),  # invalid in both modes
        ("8", "bad-bool", "8.5", "maybe"),  # invalid in both modes
        ("9" * 22, "huge-id", "9.5", "true"),  # Arrow rejects; python parses
    ]
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value", "active"])
        w.writerows(rows)

    row_results = list(
        StreamValidator(Row, on_error="collect", use_arrow=False).stream_csv(p)
    )
    batch_results = list(
        StreamValidator(Row, on_error="collect", use_arrow=True).stream_csv(p)
    )

    assert len(row_results) == len(batch_results) == len(rows)
    for a, b in zip(row_results, batch_results, strict=True):
        assert a.row_index == b.row_index
        assert a.valid == b.valid, f"row {a.row_index}: validity differs"
        if a.valid:
            assert a.data is not None and b.data is not None
            assert a.data.model_dump() == b.data.model_dump(), (
                f"row {a.row_index}: parsed data differs"
            )
        else:
            assert {e.field for e in a.errors} == {e.field for e in b.errors}, (
                f"row {a.row_index}: failing fields differ"
            )

    # Sanity: the fixture really does exercise both valid and invalid rows.
    assert any(r.valid for r in batch_results)
    assert any(not r.valid for r in batch_results)


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


@_PERF
def test_csv_batch_mode_throughput(tmp_path: Path) -> None:
    """Arrow batch mode should clear the CSV regression floor (>= 50k rps).

    The floor is a regression guard, not a target: it sits at roughly
    half of what a developer laptop measures, so it trips on a real
    slowdown without flaking on slower CI hardware. Gated behind
    ``STREAMVAL_PERF=1``; override the number with
    ``STREAMVAL_MIN_CSV_BATCH_RPS``.
    """
    p = tmp_path / "big.csv"
    n = 100_000
    _write_csv(p, n)

    v = StreamValidator(Row, on_error="skip", use_arrow=True, batch_size=10_000)
    t0 = time.perf_counter()
    count = sum(1 for _ in v.stream_csv(p))
    elapsed = time.perf_counter() - t0
    rps = count / elapsed
    floor = float(os.environ.get("STREAMVAL_MIN_CSV_BATCH_RPS", "50000"))
    print(f"\nCSV batch throughput: {rps:,.0f} rps ({elapsed:.2f}s for {count} rows)")
    assert count == n
    assert rps > floor, (
        f"CSV batch mode {rps:,.0f} rps below {floor:,.0f} floor"
    )


@_PERF
def test_parquet_batch_mode_throughput(tmp_path: Path) -> None:
    """Parquet batch mode should clear its regression floor (>= 60k rps).

    A regression guard set at roughly half of laptop-measured
    throughput. Gated behind ``STREAMVAL_PERF=1``; override the number
    with ``STREAMVAL_MIN_PARQUET_BATCH_RPS``.
    """
    p = tmp_path / "big.parquet"
    n = 100_000
    _write_parquet(p, n)

    v = StreamValidator(Row, on_error="skip", use_arrow=True, batch_size=10_000)
    t0 = time.perf_counter()
    count = sum(1 for _ in v.stream_parquet(p))
    elapsed = time.perf_counter() - t0
    rps = count / elapsed
    floor = float(os.environ.get("STREAMVAL_MIN_PARQUET_BATCH_RPS", "60000"))
    print(
        f"\nParquet batch throughput: {rps:,.0f} rps "
        f"({elapsed:.2f}s for {count} rows)"
    )
    assert count == n
    assert rps > floor, (
        f"Parquet batch mode {rps:,.0f} rps below {floor:,.0f} floor"
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

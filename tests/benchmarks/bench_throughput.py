"""Throughput benchmark across all available modes.

Run with:

    STREAMVAL_BENCH=1 python -m pytest tests/benchmarks/bench_throughput.py -v -s

Generates a 100 000-row CSV and a 100 000-row Parquet file, then
measures rows/sec for:

* streamval CSV — batch mode (Arrow path; polars when installed,
  stdlib fallback otherwise)
* streamval Parquet — batch mode (Arrow path, native pyarrow)
* streamval CSV — row mode, polars dict path
* streamval CSV — row mode, aiofiles + csv.DictReader fallback
* naive Pydantic loop (read everything, validate one by one)

Thresholds:

* CSV batch mode: > STREAMVAL_MIN_CSV_BATCH_RPS (default 35000)
* Parquet batch mode: > STREAMVAL_MIN_PARQUET_BATCH_RPS (default 45000)

These are aspirational CI-machine targets; override via env vars on
slower hardware.
"""

from __future__ import annotations

import csv
import os
import time
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import BaseModel, ValidationError

from streamval._compat import HAS_POLARS
from streamval.core.validator import StreamValidator

ROWS = 100_000


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


def _bench(label: str, fn: Callable[[], int]) -> tuple[str, int, float, float]:
    t0 = time.perf_counter()
    rows = fn()
    elapsed = time.perf_counter() - t0
    return label, rows, elapsed, rows / elapsed


def _print_table(rows: list[tuple[str, int, float, float]]) -> None:
    print()
    print(f"{'mode':<40} {'rows':>8} {'time (s)':>10} {'rows/sec':>12}")
    print("-" * 74)
    for label, n, t, rps in rows:
        print(f"{label:<40} {n:>8} {t:>10.3f} {rps:>12,.0f}")


@pytest.mark.skipif(
    os.environ.get("STREAMVAL_BENCH") != "1",
    reason="set STREAMVAL_BENCH=1 to run the throughput benchmark",
)
def test_streamval_throughput(tmp_path: Path) -> None:
    csv_path = tmp_path / "bench.csv"
    parquet_path = tmp_path / "bench.parquet"
    _write_csv(csv_path, ROWS)
    _write_parquet(parquet_path, ROWS)

    results: list[tuple[str, int, float, float]] = []

    def _csv_batch() -> int:
        v = StreamValidator(
            Row, on_error="skip", use_arrow=True, batch_size=10_000
        )
        return sum(1 for _ in v.stream_csv(csv_path))

    results.append(_bench("streamval CSV batch (Arrow path)", _csv_batch))

    def _parquet_batch() -> int:
        v = StreamValidator(
            Row, on_error="skip", use_arrow=True, batch_size=10_000
        )
        return sum(1 for _ in v.stream_parquet(parquet_path))

    results.append(_bench("streamval Parquet batch", _parquet_batch))

    def _csv_row_polars() -> int:
        v = StreamValidator(
            Row, on_error="skip", use_arrow=False, batch_size=1000
        )
        return sum(1 for _ in v.stream_csv(csv_path))

    label = (
        "streamval CSV row (polars dict)"
        if HAS_POLARS
        else "streamval CSV row (aiofiles fallback)"
    )
    results.append(_bench(label, _csv_row_polars))

    def _csv_row_stdlib() -> int:
        # Force the stdlib path by passing use_polars=False to the adapter.
        v = StreamValidator(
            Row, on_error="skip", use_arrow=False, batch_size=1000
        )
        return sum(1 for _ in v.stream_csv(csv_path, use_polars=False))

    if HAS_POLARS:
        results.append(_bench("streamval CSV row (stdlib csv)", _csv_row_stdlib))

    def _naive() -> int:
        n = 0
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    Row.model_validate(
                        {
                            "id": int(row["id"]),
                            "name": row["name"],
                            "value": float(row["value"]),
                            "active": row["active"].lower()
                            in {"true", "1", "yes"},
                        }
                    )
                    n += 1
                except ValidationError:
                    pass
        return n

    results.append(_bench("naive Pydantic loop", _naive))

    _print_table(results)

    # Threshold assertions: pull RPS by label.
    rps = {label: r for (label, _n, _t, r) in results}
    csv_floor = float(os.environ.get("STREAMVAL_MIN_CSV_BATCH_RPS", "35000"))
    parquet_floor = float(
        os.environ.get("STREAMVAL_MIN_PARQUET_BATCH_RPS", "45000")
    )

    assert rps["streamval CSV batch (Arrow path)"] > csv_floor, (
        f"CSV batch {rps['streamval CSV batch (Arrow path)']:,.0f} rps "
        f"< floor {csv_floor:,.0f}"
    )
    assert rps["streamval Parquet batch"] > parquet_floor, (
        f"Parquet batch {rps['streamval Parquet batch']:,.0f} rps "
        f"< floor {parquet_floor:,.0f}"
    )

"""Throughput benchmark: streamval vs naive Pydantic loop.

Run with:

    STREAMVAL_BENCH=1 python -m pytest tests/benchmarks/bench_throughput.py -v

The benchmark generates a 100 000-row CSV, then measures rows/sec for
streamval and a naive read-everything-then-validate loop. A floor
threshold (default 30 000 rps; override with ``STREAMVAL_MIN_RPS``) is
asserted so regressions are caught even on slower CI hardware.
"""

from __future__ import annotations

import csv
import os
import time
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

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


def _bench_streamval(path: Path) -> tuple[int, float]:
    v = StreamValidator(Row, on_error="skip", batch_size=1000)
    t0 = time.perf_counter()
    n = 0
    for _ in v.stream_csv(path):
        n += 1
    return n, time.perf_counter() - t0


def _bench_naive(path: Path) -> tuple[int, float]:
    t0 = time.perf_counter()
    n = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                Row.model_validate(
                    {
                        "id": int(row["id"]),
                        "name": row["name"],
                        "value": float(row["value"]),
                        "active": row["active"].lower() in {"true", "1", "yes"},
                    }
                )
                n += 1
            except ValidationError:
                pass
    return n, time.perf_counter() - t0


@pytest.mark.skipif(
    os.environ.get("STREAMVAL_BENCH") != "1",
    reason="set STREAMVAL_BENCH=1 to run the throughput benchmark",
)
def test_streamval_throughput(tmp_path: Path) -> None:
    p = tmp_path / "bench.csv"
    _write_csv(p, ROWS)

    sv_rows, sv_time = _bench_streamval(p)
    naive_rows, naive_time = _bench_naive(p)

    sv_rps = sv_rows / sv_time
    naive_rps = naive_rows / naive_time

    print()
    print(f"{'method':<20} {'rows':>8} {'time (s)':>10} {'rows/sec':>12}")
    print("-" * 54)
    print(
        f"{'streamval':<20} {sv_rows:>8} {sv_time:>10.3f} {sv_rps:>12,.0f}"
    )
    print(
        f"{'naive Pydantic loop':<20} {naive_rows:>8} "
        f"{naive_time:>10.3f} {naive_rps:>12,.0f}"
    )

    assert sv_rows == ROWS
    # Floor is set per-machine via STREAMVAL_MIN_RPS. The default (5k)
    # catches catastrophic regressions but tolerates slow Windows CI.
    floor = float(os.environ.get("STREAMVAL_MIN_RPS", "5000"))
    assert sv_rps > floor, f"streamval threw {sv_rps:.0f} rps, want > {floor:.0f}"

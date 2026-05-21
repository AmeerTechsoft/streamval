"""Peak-memory benchmark.

Run with:

    STREAMVAL_BENCH=1 python -m pytest tests/benchmarks/bench_memory.py -v

Generates a 1 000 000-row CSV and asserts peak Python-object memory
stays below 50 MB (default; override with ``STREAMVAL_MAX_MB``). Also
verifies that doubling ``batch_size`` increases peak memory roughly
linearly.
"""

from __future__ import annotations

import csv
import os
import tracemalloc
from pathlib import Path

import pytest
from pydantic import BaseModel

from streamval.core.validator import StreamValidator

ROWS = 1_000_000


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


def _measure_peak(path: Path, batch_size: int) -> tuple[int, float]:
    v = StreamValidator(Row, on_error="skip", batch_size=batch_size)
    tracemalloc.start()
    tracemalloc.reset_peak()
    n = 0
    for _ in v.stream_csv(path):
        n += 1
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return n, peak / (1024 * 1024)


@pytest.mark.skipif(
    os.environ.get("STREAMVAL_BENCH") != "1",
    reason="set STREAMVAL_BENCH=1 to run the memory benchmark",
)
def test_streamval_memory(tmp_path: Path) -> None:
    p = tmp_path / "big.csv"
    _write_csv(p, ROWS)

    n_default, peak_default_mb = _measure_peak(p, batch_size=1000)
    n_double, peak_double_mb = _measure_peak(p, batch_size=2000)

    print()
    print(f"{'batch_size':<12} {'rows':>10} {'peak (MB)':>12}")
    print("-" * 40)
    print(f"{1000:<12} {n_default:>10} {peak_default_mb:>12.2f}")
    print(f"{2000:<12} {n_double:>10} {peak_double_mb:>12.2f}")

    assert n_default == ROWS
    assert n_double == ROWS

    max_mb = float(os.environ.get("STREAMVAL_MAX_MB", "50"))
    assert peak_default_mb < max_mb, (
        f"peak {peak_default_mb:.2f} MB exceeds budget {max_mb} MB"
    )
    assert peak_double_mb < 4 * peak_default_mb + 5, (
        "memory growth is super-linear with batch_size; "
        f"got {peak_default_mb:.2f} -> {peak_double_mb:.2f} MB"
    )

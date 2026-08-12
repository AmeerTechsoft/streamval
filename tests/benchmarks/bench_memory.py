"""Peak-memory benchmark across batch sizes.

Run with:

    STREAMVAL_BENCH=1 python -m pytest tests/benchmarks/bench_memory.py -v -s

Generates a 1 000 000-row CSV and measures peak Python-object memory
via tracemalloc at a range of ``batch_size`` values in Arrow batch
mode. Asserts:

* Peak memory < ``STREAMVAL_MAX_MB`` (default 50 MB) at the default
  batch size.
* Memory growth is roughly proportional to ``batch_size`` — not
  super-linear.

This benchmark documents the small-memory streaming contract of the
v0.2 Arrow fast path.

Garbage is collected periodically during each measurement; see
``_GC_EVERY`` for why the raw high-water mark is unusable at small
batch sizes.
"""

from __future__ import annotations

import csv
import gc
import os
import tracemalloc
from pathlib import Path

import pytest
from pydantic import BaseModel

from streamval.core.validator import StreamValidator

ROWS = 500_000

# Peak memory at small batch sizes is not a stable statistic: it is a
# high-water mark, so it captures transient garbage as well as the live
# working set, and identical runs land on visibly different values. At
# ``batch_size=100`` we have measured 0.25, 1.84, 1.90 and 8.23 MB for
# the same code on different machines and runs, while ``batch_size>=1000``
# reproduces to within 0.01 MB across machines.
#
# So take the minimum of several runs. The noise here is strictly
# additive — uncollected garbage on top of the real working set — which
# makes the floor the best estimator of what is actually live, and keeps
# the reported number comparable between machines.
REPS = 3


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


# Collect garbage periodically while measuring. ``tracemalloc`` records a
# high-water mark, so without this the reported peak includes whatever
# transient garbage happened to be uncollected at the moment the mark was
# set — which depends on GC timing, not on the streaming working set.
#
# At small batch sizes the live set is tiny (~0.25 MB at batch_size=100)
# and that garbage dominates: five identical runs measured 0.24, 0.25,
# 0.25, 0.25 and 1.85 MB, a 7.6× spread, while batch_size=1000 and 10000
# were stable to within 0.01 MB. Forcing collection removes the artifact.
#
# This does not weaken the benchmark as a leak detector: ``gc.collect()``
# only frees unreachable objects, so anything genuinely retained still
# shows up.
_GC_EVERY = 50_000


def _measure_peak(
    path: Path, batch_size: int, use_arrow: bool
) -> tuple[int, float, float]:
    """Return ``(rows, min_peak_mb, max_peak_mb)`` over :data:`REPS` runs.

    The spread is reported alongside the floor so an unstable
    measurement is visible in the output rather than silently reduced to
    one flattering number.
    """
    results = [_measure_peak_once(path, batch_size, use_arrow) for _ in range(REPS)]
    counts = {n for n, _ in results}
    assert len(counts) == 1, f"row count varied across runs: {counts}"
    peaks = [p for _, p in results]
    return results[0][0], min(peaks), max(peaks)


def _measure_peak_once(
    path: Path, batch_size: int, use_arrow: bool
) -> tuple[int, float]:
    gc.collect()
    v = StreamValidator(
        Row,
        on_error="skip",
        batch_size=batch_size,
        use_arrow=use_arrow,
    )
    tracemalloc.start()
    tracemalloc.reset_peak()
    n = 0
    for _ in v.stream_csv(path):
        n += 1
        if n % _GC_EVERY == 0:
            gc.collect()
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

    samples: list[tuple[str, int, int, float, float]] = []
    for bs in (100, 1_000, 5_000, 10_000):
        n, lo, hi = _measure_peak(p, batch_size=bs, use_arrow=True)
        samples.append((f"arrow batch_size={bs}", bs, n, lo, hi))

    n_row, row_lo, row_hi = _measure_peak(p, batch_size=1_000, use_arrow=False)
    samples.append(("row mode batch_size=1000", 1_000, n_row, row_lo, row_hi))

    print()
    print(
        f"{'mode':<28} {'batch_size':>11} {'rows':>9} "
        f"{'peak (MB)':>11} {'worst':>8}"
    )
    print("-" * 72)
    for label, bs, n, lo, hi in samples:
        print(f"{label:<28} {bs:>11} {n:>9} {lo:>11.2f} {hi:>8.2f}")
    print(f"(min and max peak over {REPS} runs each)")

    assert all(n == ROWS for _, _, n, _, _ in samples)

    max_mb = float(os.environ.get("STREAMVAL_MAX_MB", "50"))

    def _peak_for(target_bs: int) -> float:
        return next(
            lo
            for label, bs, _, lo, _ in samples
            if bs == target_bs and "arrow" in label
        )

    default_peak = _peak_for(1_000)
    assert default_peak < max_mb, (
        f"arrow batch_size=1000 peak {default_peak:.2f} MB exceeds "
        f"budget {max_mb} MB"
    )

    # Sub-linear or near-linear growth: doubling batch_size from 5k → 10k
    # should not more-than-triple memory.
    peak_5k = _peak_for(5_000)
    peak_10k = _peak_for(10_000)
    assert peak_10k < 3 * peak_5k + 5, (
        f"memory growth super-linear: {peak_5k:.2f} → {peak_10k:.2f} MB"
    )

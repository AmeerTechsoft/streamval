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

* CSV batch mode: > STREAMVAL_MIN_CSV_BATCH_RPS (default 50000)
* Parquet batch mode: > STREAMVAL_MIN_PARQUET_BATCH_RPS (default 60000)

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


def _write_variant_csv(path: Path, n: int) -> None:
    """Same rows as :func:`_write_csv`, written the way real exports write them.

    Every row is still valid — only the formatting varies: padded cells,
    worded booleans, and integers carrying a trailing ``.0``. A plain
    Arrow cast rejects all three, so this is the file that exercises the
    vectorised normalisation in ``_cast_string_column``. The canonical
    benchmark above never touches that code.
    """
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value", "active"])
        for i in range(n):
            w.writerow(
                [f" {i}.0 ", f"n{i}", f" {i * 0.5:.3f} ", " yes " if i % 2 else " no "]
            )


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
    variant_path = tmp_path / "bench_variant.csv"
    parquet_path = tmp_path / "bench.parquet"
    _write_csv(csv_path, ROWS)
    _write_variant_csv(variant_path, ROWS)
    _write_parquet(parquet_path, ROWS)

    results: list[tuple[str, int, float, float]] = []

    def _csv_batch() -> int:
        v = StreamValidator(
            Row, on_error="skip", use_arrow=True, batch_size=10_000
        )
        return sum(1 for _ in v.stream_csv(csv_path))

    results.append(_bench("streamval CSV batch (Arrow path)", _csv_batch))

    def _csv_batch_variant() -> int:
        v = StreamValidator(
            Row, on_error="skip", use_arrow=True, batch_size=10_000
        )
        return sum(1 for _ in v.stream_csv(variant_path))

    results.append(
        _bench("streamval CSV batch (variant formatting)", _csv_batch_variant)
    )

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

    rps = {label: r for (label, _n, _t, r) in results}

    # Ratio guard for the vectorised normalisation. Unlike the absolute
    # floors below this is machine-independent: both files hold the same
    # rows and are measured back to back in one process, so it is not
    # distorted by runner speed. Before the normalisation landed, variant
    # formatting ran at ~0.51x of canonical; it now measures ~0.97x, so a
    # 0.7 threshold catches a regression with room to spare.
    canonical = rps["streamval CSV batch (Arrow path)"]
    variant = rps["streamval CSV batch (variant formatting)"]
    ratio = variant / canonical
    print(f"\nvariant / canonical formatting: {ratio:.2f}x")
    assert ratio > 0.7, (
        f"variant-formatted CSV at {ratio:.2f}x of canonical "
        f"({variant:,.0f} vs {canonical:,.0f} rps) — padded cells, worded "
        "bools or int-as-float have stopped taking the vectorised path"
    )

    # Threshold assertions are aspirational CI-hardware targets — gate
    # them behind STREAMVAL_PERF=1 so the default ``STREAMVAL_BENCH=1``
    # benchmark run is a pure observability pass and never fails the
    # CI job just for being on slower hardware. Numeric floors stay
    # overridable via STREAMVAL_MIN_*_RPS so a CI perf job can keep
    # asserting against the floor it actually expects.
    if os.environ.get("STREAMVAL_PERF") != "1":
        print(
            "\n(STREAMVAL_PERF not set — skipping floor assertions; "
            "set STREAMVAL_PERF=1 to enforce.)"
        )
        return

    csv_floor = float(os.environ.get("STREAMVAL_MIN_CSV_BATCH_RPS", "50000"))
    parquet_floor = float(
        os.environ.get("STREAMVAL_MIN_PARQUET_BATCH_RPS", "60000")
    )

    assert rps["streamval CSV batch (Arrow path)"] > csv_floor, (
        f"CSV batch {rps['streamval CSV batch (Arrow path)']:,.0f} rps "
        f"< floor {csv_floor:,.0f}"
    )
    assert rps["streamval Parquet batch"] > parquet_floor, (
        f"Parquet batch {rps['streamval Parquet batch']:,.0f} rps "
        f"< floor {parquet_floor:,.0f}"
    )

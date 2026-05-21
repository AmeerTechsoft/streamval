"""End-to-end CSV validation."""

from __future__ import annotations

import csv
import tracemalloc
from pathlib import Path

import pytest
from pydantic import BaseModel

from streamval.core.result import StreamValidationError
from streamval.core.validator import StreamValidator, stream_csv


class Row(BaseModel):
    id: int
    name: str
    value: float
    active: bool


def _write_mixed_csv(path: Path, n: int) -> tuple[int, int]:
    """Write n rows; every 7th row is invalid. Returns (valid, invalid)."""
    valid = invalid = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value", "active"])
        for i in range(n):
            if i % 7 == 0:
                w.writerow(["not-an-int", f"n{i}", "1.5", "true"])
                invalid += 1
            else:
                w.writerow([i, f"n{i}", "1.5", "true"])
                valid += 1
    return valid, invalid


def test_csv_collect_strategy_counts_match(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    valid, invalid = _write_mixed_csv(p, 1000)

    v = StreamValidator(Row, on_error="collect", batch_size=100)
    results = list(v.stream_csv(p))

    assert len(results) == valid + invalid
    assert sum(1 for r in results if r.valid) == valid
    assert sum(1 for r in results if not r.valid) == invalid

    s = v.stats
    assert s.rows_total == valid + invalid
    assert s.rows_valid == valid
    assert s.rows_invalid == invalid
    assert s.error_rate == pytest.approx(invalid / (valid + invalid))


def test_csv_skip_strategy_drops_invalid(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    valid, _ = _write_mixed_csv(p, 200)
    v = StreamValidator(Row, on_error="skip", batch_size=50)
    results = list(v.stream_csv(p))
    assert len(results) == valid
    assert all(r.valid for r in results)


def test_csv_fail_fast_raises(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    _write_mixed_csv(p, 50)
    v = StreamValidator(Row, on_error="fail_fast", batch_size=10)
    with pytest.raises(StreamValidationError):
        list(v.stream_csv(p))


def test_csv_module_level_helper(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    valid, _ = _write_mixed_csv(p, 100)
    results = list(stream_csv(p, Row, on_error="skip"))
    assert len(results) == valid


def test_csv_memory_bounded(tmp_path: Path) -> None:
    """Peak memory must stay under 50 MB regardless of file size."""
    p = tmp_path / "big.csv"
    _write_mixed_csv(p, 10000)

    tracemalloc.start()
    tracemalloc.reset_peak()
    v = StreamValidator(Row, on_error="skip", batch_size=1000)
    count = 0
    for _ in v.stream_csv(p):
        count += 1
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert count > 8000
    assert peak < 50 * 1024 * 1024


async def test_csv_async_streaming(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    valid, invalid = _write_mixed_csv(p, 200)
    v = StreamValidator(Row, on_error="collect", batch_size=50)
    out = []
    async for r in v.astream_csv(p):
        out.append(r)
    assert len(out) == valid + invalid


async def test_csv_workers_preserve_order(tmp_path: Path) -> None:
    p = tmp_path / "data.csv"
    _write_mixed_csv(p, 500)
    v = StreamValidator(Row, on_error="collect", batch_size=50, workers=4)
    out = []
    async for r in v.astream_csv(p):
        out.append(r)
    assert [r.row_index for r in out] == list(range(len(out)))

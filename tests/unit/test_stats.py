"""Tests for streamval.core.stats."""

from __future__ import annotations

import time
import tracemalloc

from pydantic import BaseModel, ValidationError

from streamval.core.result import ValidationResult
from streamval.core.stats import StatsAccumulator, StreamStats


class _User(BaseModel):
    id: int
    name: str


def _bad_result(row_index: int, fields: list[str]) -> ValidationResult:
    raw = {"id": "x", "name": 1}
    try:
        _User(**raw)  # type: ignore[arg-type]
    except ValidationError as exc:
        res = ValidationResult.from_pydantic_error(row_index, raw, exc)
    res.errors.clear()
    from streamval.core.result import FieldError

    for f in fields:
        res.errors.append(
            FieldError(field=f, value=None, message="bad", error_type="t")
        )
    return res


def _good_result(row_index: int) -> ValidationResult:
    user = _User(id=row_index, name="a")
    return ValidationResult.success(row_index, {"id": row_index, "name": "a"}, user)


def test_streamstats_error_rate_zero_when_empty() -> None:
    s = StreamStats()
    assert s.error_rate == 0.0


def test_streamstats_error_rate_basic() -> None:
    s = StreamStats(rows_total=10, rows_valid=7, rows_invalid=3)
    assert s.error_rate == 0.3
    text = str(s)
    assert "total=10" in text
    assert "invalid=3" in text


def test_accumulator_records_valid_and_invalid() -> None:
    acc = StatsAccumulator()
    acc.start()
    acc.record(_good_result(0))
    acc.record(_bad_result(1, ["id"]))
    acc.record(_bad_result(2, ["id", "name"]))
    time.sleep(0.005)
    acc.stop()

    stats = acc.to_stats()
    assert stats.rows_total == 3
    assert stats.rows_valid == 1
    assert stats.rows_invalid == 2
    assert stats.errors_by_field == {"id": 2, "name": 1}
    assert stats.duration_seconds > 0
    assert stats.throughput_rps > 0


def test_accumulator_to_stats_before_stop_uses_now() -> None:
    acc = StatsAccumulator()
    acc.start()
    acc.record(_good_result(0))
    stats = acc.to_stats()
    assert stats.rows_total == 1
    acc.stop()


def test_accumulator_repr_contains_counts() -> None:
    acc = StatsAccumulator()
    acc.start()
    acc.record(_good_result(0))
    acc.stop()
    text = repr(acc)
    assert "total=1" in text


def test_accumulator_no_rows() -> None:
    acc = StatsAccumulator()
    acc.start()
    acc.stop()
    stats = acc.to_stats()
    assert stats.rows_total == 0
    assert stats.error_rate == 0.0
    assert stats.throughput_rps == 0.0


def test_record_many_matches_per_row_record() -> None:
    results = [
        _good_result(0),
        _bad_result(1, ["id"]),
        _good_result(2),
        _bad_result(3, ["id", "name"]),
    ]

    one_by_one = StatsAccumulator()
    one_by_one.start()
    for r in results:
        one_by_one.record(r)
    one_by_one.stop()

    batched = StatsAccumulator()
    batched.start()
    batched.record_many(results)
    batched.stop()

    a, b = one_by_one.to_stats(), batched.to_stats()
    assert (a.rows_total, a.rows_valid, a.rows_invalid) == (4, 2, 2)
    assert (b.rows_total, b.rows_valid, b.rows_invalid) == (
        a.rows_total,
        a.rows_valid,
        a.rows_invalid,
    )
    assert b.errors_by_field == a.errors_by_field


def test_record_many_all_valid_shortcut() -> None:
    acc = StatsAccumulator()
    acc.start()
    acc.record_many([_good_result(i) for i in range(5)])
    acc.stop()
    stats = acc.to_stats()
    assert (stats.rows_total, stats.rows_valid, stats.rows_invalid) == (5, 0 + 5, 0)
    assert stats.errors_by_field == {}


def test_tracemalloc_is_off_by_default() -> None:
    """Memory tracking is opt-in: it costs ~4-5x throughput when on."""
    assert not tracemalloc.is_tracing()
    acc = StatsAccumulator()
    acc.start()
    assert not tracemalloc.is_tracing(), (
        "StatsAccumulator must not start tracemalloc unless track_memory=True"
    )
    acc.record(_good_result(0))
    acc.stop()
    assert acc.to_stats().peak_memory_mb == 0.0


def test_track_memory_opt_in_populates_peak() -> None:
    assert not tracemalloc.is_tracing()
    acc = StatsAccumulator(track_memory=True)
    acc.start()
    assert tracemalloc.is_tracing()
    acc.record_many([_good_result(i) for i in range(100)])
    acc.stop()
    assert not tracemalloc.is_tracing(), "accumulator must release the tracer"
    assert acc.to_stats().peak_memory_mb > 0.0


def test_piggybacks_on_externally_started_tracemalloc() -> None:
    """Reading an already-running tracer is free, so still report peak."""
    assert not tracemalloc.is_tracing()
    tracemalloc.start()
    try:
        acc = StatsAccumulator()  # track_memory=False
        acc.start()
        acc.record_many([_good_result(i) for i in range(100)])
        acc.stop()
        assert acc.to_stats().peak_memory_mb > 0.0
        assert tracemalloc.is_tracing(), "must not stop a tracer it doesn't own"
    finally:
        tracemalloc.stop()

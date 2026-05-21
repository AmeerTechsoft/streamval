"""Stream-level statistics.

Defines the immutable :class:`StreamStats` summary and the mutable
:class:`StatsAccumulator` that records each :class:`ValidationResult`
and tracks timing and peak memory.
"""

from __future__ import annotations

import time
import tracemalloc
from dataclasses import dataclass, field

from streamval.core.result import ValidationResult


@dataclass(frozen=True)
class StreamStats:
    """Immutable summary of a streaming validation run.

    Attributes:
        rows_total: Total rows seen.
        rows_valid: Rows that passed validation.
        rows_invalid: Rows that failed validation.
        errors_by_field: Map from field name to count of errors observed.
        throughput_rps: Rows processed per second over the whole run.
        peak_memory_mb: Peak Python-object memory in MB (via tracemalloc).
        duration_seconds: Wall-clock duration of the run.
    """

    rows_total: int = 0
    rows_valid: int = 0
    rows_invalid: int = 0
    errors_by_field: dict[str, int] = field(default_factory=dict)
    throughput_rps: float = 0.0
    peak_memory_mb: float = 0.0
    duration_seconds: float = 0.0

    @property
    def error_rate(self) -> float:
        """Fraction of rows that failed validation (0.0 when no rows)."""
        if self.rows_total == 0:
            return 0.0
        return self.rows_invalid / self.rows_total

    def __str__(self) -> str:
        return (
            f"StreamStats(total={self.rows_total}, valid={self.rows_valid}, "
            f"invalid={self.rows_invalid}, error_rate={self.error_rate:.3f}, "
            f"throughput={self.throughput_rps:.1f} rps, "
            f"peak_mem={self.peak_memory_mb:.2f} MB, "
            f"duration={self.duration_seconds:.3f}s)"
        )


class StatsAccumulator:
    """Mutable accumulator that records per-row results and timing.

    Lifecycle:
        1. Construct an instance.
        2. Call :meth:`start` before consuming rows.
        3. Call :meth:`record` for every :class:`ValidationResult`.
        4. Call :meth:`stop` after the stream is exhausted.
        5. Call :meth:`to_stats` to obtain the immutable :class:`StreamStats`.

    Memory tracking uses ``tracemalloc``. The accumulator only enables
    ``tracemalloc`` if it isn't already active, and disables it again on
    :meth:`stop` to avoid leaking global state.
    """

    def __init__(self) -> None:
        self._rows_total: int = 0
        self._rows_valid: int = 0
        self._rows_invalid: int = 0
        self._errors_by_field: dict[str, int] = {}
        self._start_time: float | None = None
        self._stop_time: float | None = None
        self._peak_bytes: int = 0
        self._owns_tracemalloc: bool = False

    def start(self) -> None:
        """Mark the run start and begin tracking peak memory."""
        self._start_time = time.perf_counter()
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self._owns_tracemalloc = True
        else:
            self._owns_tracemalloc = False
            tracemalloc.reset_peak()

    def record(self, result: ValidationResult) -> None:
        """Record a single :class:`ValidationResult`."""
        self._rows_total += 1
        if result.valid:
            self._rows_valid += 1
        else:
            self._rows_invalid += 1
            for err in result.errors:
                self._errors_by_field[err.field] = (
                    self._errors_by_field.get(err.field, 0) + 1
                )

    def stop(self) -> None:
        """Mark the run end and capture peak memory."""
        self._stop_time = time.perf_counter()
        try:
            _current, peak = tracemalloc.get_traced_memory()
            self._peak_bytes = peak
        except RuntimeError:
            self._peak_bytes = 0
        if self._owns_tracemalloc:
            tracemalloc.stop()
            self._owns_tracemalloc = False

    def to_stats(self) -> StreamStats:
        """Snapshot the accumulator as an immutable :class:`StreamStats`."""
        duration = self._duration()
        throughput = (self._rows_total / duration) if duration > 0 else 0.0
        return StreamStats(
            rows_total=self._rows_total,
            rows_valid=self._rows_valid,
            rows_invalid=self._rows_invalid,
            errors_by_field=dict(self._errors_by_field),
            throughput_rps=throughput,
            peak_memory_mb=self._peak_bytes / (1024 * 1024),
            duration_seconds=duration,
        )

    def _duration(self) -> float:
        if self._start_time is None:
            return 0.0
        end = self._stop_time if self._stop_time is not None else time.perf_counter()
        return max(0.0, end - self._start_time)

    def __repr__(self) -> str:
        return (
            f"StatsAccumulator(total={self._rows_total}, "
            f"valid={self._rows_valid}, invalid={self._rows_invalid})"
        )

    def __str__(self) -> str:
        return self.__repr__()

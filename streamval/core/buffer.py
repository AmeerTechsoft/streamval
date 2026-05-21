"""Bounded-memory batching primitives.

:class:`BatchBuffer` chunks an async row stream into fixed-size lists.
:class:`ParallelBatchProcessor` validates each batch — optionally in a
thread pool — and yields :class:`ValidationResult` objects in input
order. Pydantic v2's core validator is implemented in Rust and is
thread-safe, so dispatching whole batches to worker threads is safe.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from streamval.core.result import ValidationResult
from streamval.schema.plan import CompiledValidationPlan

if TYPE_CHECKING:
    import pyarrow as pa


class BatchBuffer:
    """Wraps an ``AsyncIterator[dict]`` and emits fixed-size batches.

    Args:
        source: Any async iterator of row dicts.
        batch_size: Maximum rows per emitted batch (must be >= 1).

    The buffer never holds more than ``batch_size`` rows in memory at any
    given moment. The final, partial batch is still emitted.
    """

    def __init__(
        self,
        source: AsyncIterator[dict[str, Any]],
        batch_size: int = 1000,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._source = source
        self._batch_size = batch_size

    async def batches(self) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield successive batches as lists of row dicts."""
        batch: list[dict[str, Any]] = []
        async for row in self._source:
            batch.append(row)
            if len(batch) >= self._batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


class ParallelBatchProcessor:
    """Drives a :class:`BatchBuffer` through a :class:`CompiledValidationPlan`.

    With ``workers == 1`` (the default) batches are validated inline on
    the event loop's thread. With ``workers > 1`` each batch is dispatched
    to a :class:`concurrent.futures.ThreadPoolExecutor`; results are
    yielded strictly in input order.

    Args:
        buffer: A :class:`BatchBuffer` to consume.
        plan: The compiled validation plan.
        workers: Thread-pool size; ``1`` disables the pool entirely.
        start_row_index: First row index to assign (default ``0``).
    """

    def __init__(
        self,
        buffer: BatchBuffer,
        plan: CompiledValidationPlan,
        *,
        workers: int = 1,
        start_row_index: int = 0,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")
        self._buffer = buffer
        self._plan = plan
        self._workers = workers
        self._start = start_row_index

    async def stream(self) -> AsyncIterator[ValidationResult]:
        """Yield :class:`ValidationResult` objects in input row order."""
        if self._workers == 1:
            async for result in self._stream_inline():
                yield result
            return

        async for result in self._stream_parallel():
            yield result

    async def _stream_inline(self) -> AsyncIterator[ValidationResult]:
        idx = self._start
        async for batch in self._buffer.batches():
            for row in batch:
                yield self._plan.validate_row(idx, row)
                idx += 1

    async def _stream_parallel(self) -> AsyncIterator[ValidationResult]:
        loop = asyncio.get_running_loop()
        plan = self._plan
        idx = self._start

        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            async for batch in self._buffer.batches():
                start = idx
                idx += len(batch)
                future = loop.run_in_executor(
                    pool, _validate_batch, plan, batch, start
                )
                results = await future
                for result in results:
                    yield result


def _validate_batch(
    plan: CompiledValidationPlan,
    batch: list[dict[str, Any]],
    start_index: int,
) -> list[ValidationResult]:
    out: list[ValidationResult] = []
    for offset, row in enumerate(batch):
        out.append(plan.validate_row(start_index + offset, row))
    return out


class RecordBatchPipeline:
    """Validates a stream of :class:`pyarrow.RecordBatch` directly.

    Each incoming RecordBatch is handed to
    :meth:`CompiledValidationPlan.validate_record_batch`, which uses the
    bulk TypeAdapter path. This is the Arrow fast path that bypasses
    per-row Python dict construction in the adapter loop.

    With ``workers > 1``, individual batches are dispatched to a
    thread pool while results are yielded in input order. Pydantic v2's
    Rust core is thread-safe, so this is safe.
    """

    def __init__(
        self,
        source: AsyncIterator[pa.RecordBatch],
        plan: CompiledValidationPlan,
        *,
        workers: int = 1,
        start_row_index: int = 0,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")
        self._source = source
        self._plan = plan
        self._workers = workers
        self._start = start_row_index

    async def stream(self) -> AsyncIterator[ValidationResult]:
        """Yield :class:`ValidationResult` objects in input row order."""
        if self._workers == 1:
            idx = self._start
            async for batch in self._source:
                results = self._plan.validate_record_batch(
                    batch, start_index=idx
                )
                idx += batch.num_rows
                for r in results:
                    yield r
            return

        loop = asyncio.get_running_loop()
        plan = self._plan
        idx = self._start
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            async for batch in self._source:
                start = idx
                idx += batch.num_rows
                future = loop.run_in_executor(
                    pool, _validate_record_batch, plan, batch, start
                )
                results = await future
                for r in results:
                    yield r


def _validate_record_batch(
    plan: CompiledValidationPlan,
    batch: pa.RecordBatch,
    start_index: int,
) -> list[ValidationResult]:
    return plan.validate_record_batch(batch, start_index=start_index)


__all__ = ["BatchBuffer", "ParallelBatchProcessor", "RecordBatchPipeline"]

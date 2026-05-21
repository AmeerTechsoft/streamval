"""StreamValidator — the main public engine.

Wires a format adapter, a :class:`BatchBuffer`, a
:class:`ParallelBatchProcessor`, a :class:`CompiledValidationPlan`, a
:class:`StrategyHandler`, and a :class:`StatsAccumulator` into a single
async streaming pipeline.

Module-level convenience functions (``stream_csv``, ``astream_csv``, …)
construct a :class:`StreamValidator` per call and deliver an iterator
or async iterator of :class:`ValidationResult` objects.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from streamval.adapters import (
    arrow_adapter,
    csv_adapter,
    jsonl_adapter,
    parquet_adapter,
)
from streamval.core.buffer import (
    BatchBuffer,
    ParallelBatchProcessor,
    RecordBatchPipeline,
)
from streamval.core.result import ValidationResult
from streamval.core.stats import StatsAccumulator, StreamStats
from streamval.schema.coerce import SourceFormat
from streamval.schema.plan import get_plan
from streamval.strategies import StrategyHandler, build_handler
from streamval.strategies.base import ErrorStrategy


class StreamValidator:
    """Validate row streams from CSV, JSONL, Parquet, or Arrow files.

    Args:
        schema: Pydantic ``BaseModel`` subclass describing each row.
        on_error: How to handle invalid rows. One of
            ``"fail_fast"``, ``"collect"`` (default), or ``"skip"``.
        batch_size: Rows per validation batch (default ``1000``).
        max_errors: Cap on collected errors for the ``collect`` strategy.
            ``None`` means unlimited.
        workers: Thread-pool size; ``1`` (default) disables the pool.

    Example:
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     id: int
        ...     name: str
        >>> v = StreamValidator(User, on_error="collect")
        >>> for result in v.stream_csv("users.csv"):
        ...     if not result.valid:
        ...         print(result.errors)
    """

    def __init__(
        self,
        schema: type[BaseModel],
        on_error: ErrorStrategy = "collect",
        batch_size: int = 1000,
        max_errors: int | None = None,
        workers: int = 1,
        use_arrow: bool = True,
    ) -> None:
        self._schema = schema
        self._on_error = on_error
        self._batch_size = batch_size
        self._max_errors = max_errors
        self._workers = workers
        self._use_arrow = use_arrow
        self._handler: StrategyHandler = build_handler(
            on_error, max_errors=max_errors
        )
        self._stats = StatsAccumulator()

    @property
    def use_arrow(self) -> bool:
        """Whether the Arrow batch fast path is active."""
        return self._use_arrow

    @property
    def stats(self) -> StreamStats:
        """Snapshot of the accumulated stats (call after the stream ends)."""
        return self._stats.to_stats()

    @property
    def handler(self) -> StrategyHandler:
        """The active :class:`StrategyHandler` (handy for diagnostics)."""
        return self._handler

    async def _stream(
        self,
        source: AsyncIterator[dict[str, Any]],
        fmt: SourceFormat,
    ) -> AsyncIterator[ValidationResult]:
        plan = get_plan(self._schema, fmt)
        buf = BatchBuffer(source, batch_size=self._batch_size)
        proc = ParallelBatchProcessor(buf, plan, workers=self._workers)

        self._stats.start()
        try:
            async for result in proc.stream():
                self._stats.record(result)
                handled = await self._handler.handle(result)
                if handled is not None:
                    yield handled
            await self._handler.finalize()
        finally:
            self._stats.stop()

    def _stream_sync(
        self,
        rows: Iterator[dict[str, Any]],
        fmt: SourceFormat,
    ) -> Iterator[ValidationResult]:
        """Sync mirror of :meth:`_stream` with no asyncio overhead.

        The built-in strategy handlers' coroutines never actually
        ``await`` anything, so we can drive them by hand via
        :meth:`coroutine.send`. Custom async-only handlers are not
        supported on the sync path; use ``astream_*`` instead.
        """
        plan = get_plan(self._schema, fmt)
        handler = self._handler

        self._stats.start()
        idx = 0
        batch: list[dict[str, Any]] = []
        bs = self._batch_size
        try:
            for row in rows:
                batch.append(row)
                if len(batch) >= bs:
                    yield from self._validate_and_handle_batch(
                        plan, batch, idx, handler
                    )
                    idx += len(batch)
                    batch = []
            if batch:
                yield from self._validate_and_handle_batch(
                    plan, batch, idx, handler
                )
            _run_coro(handler.finalize())
        finally:
            self._stats.stop()

    def _validate_and_handle_batch(
        self,
        plan: Any,
        batch: list[dict[str, Any]],
        start: int,
        handler: StrategyHandler,
    ) -> Iterator[ValidationResult]:
        for offset, row in enumerate(batch):
            result = plan.validate_row(start + offset, row)
            self._stats.record(result)
            handled = _run_coro(handler.handle(result))
            if handled is not None:
                yield handled

    async def _stream_arrow(
        self,
        source: AsyncIterator[Any],
        fmt: SourceFormat,
    ) -> AsyncIterator[ValidationResult]:
        """Arrow fast path: validate :class:`pyarrow.RecordBatch` directly."""
        plan = get_plan(self._schema, fmt)
        pipeline = RecordBatchPipeline(source, plan, workers=self._workers)

        self._stats.start()
        try:
            async for result in pipeline.stream():
                self._stats.record(result)
                handled = await self._handler.handle(result)
                if handled is not None:
                    yield handled
            await self._handler.finalize()
        finally:
            self._stats.stop()

    def _stream_arrow_sync(
        self,
        batches: Iterator[Any],
        fmt: SourceFormat,
    ) -> Iterator[ValidationResult]:
        """Sync mirror of :meth:`_stream_arrow`."""
        plan = get_plan(self._schema, fmt)
        handler = self._handler
        self._stats.start()
        idx = 0
        try:
            for batch in batches:
                results = plan.validate_record_batch(batch, start_index=idx)
                idx += batch.num_rows
                for result in results:
                    self._stats.record(result)
                    handled = _run_coro(handler.handle(result))
                    if handled is not None:
                        yield handled
            _run_coro(handler.finalize())
        finally:
            self._stats.stop()

    async def astream_csv(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> AsyncIterator[ValidationResult]:
        """Validate a CSV file asynchronously."""
        if self._use_arrow:
            kwargs = _arrow_kwargs(adapter_kwargs)
            async for r in self._stream_arrow(
                csv_adapter.stream_record_batches(path, **kwargs),
                SourceFormat.CSV,
            ):
                yield r
            return
        async for r in self._stream(
            csv_adapter.stream_rows(path, **adapter_kwargs), SourceFormat.CSV
        ):
            yield r

    async def astream_jsonl(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> AsyncIterator[ValidationResult]:
        """Validate a JSONL file asynchronously."""
        async for r in self._stream(
            jsonl_adapter.stream_rows(path, **adapter_kwargs), SourceFormat.JSONL
        ):
            yield r

    async def astream_parquet(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> AsyncIterator[ValidationResult]:
        """Validate a Parquet file asynchronously."""
        if self._use_arrow:
            kwargs = _arrow_kwargs(adapter_kwargs, default_batch_size=10_000)
            async for r in self._stream_arrow(
                parquet_adapter.stream_record_batches(path, **kwargs),
                SourceFormat.PARQUET,
            ):
                yield r
            return
        async for r in self._stream(
            parquet_adapter.stream_rows(path, **adapter_kwargs),
            SourceFormat.PARQUET,
        ):
            yield r

    async def astream_arrow(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> AsyncIterator[ValidationResult]:
        """Validate an Arrow IPC / Feather file asynchronously."""
        async for r in self._stream(
            arrow_adapter.stream_rows(path, **adapter_kwargs), SourceFormat.ARROW
        ):
            yield r

    def stream_csv(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> Iterator[ValidationResult]:
        """Streaming sync iterator over a CSV file.

        When :attr:`use_arrow` is ``True`` (the default), validation
        runs against :class:`pyarrow.RecordBatch` objects produced by
        polars (or a stdlib fallback) — no per-row Python dict
        construction in the adapter loop.
        """
        if self._use_arrow:
            kwargs = _arrow_kwargs(adapter_kwargs)
            batches = _stream_csv_record_batches_sync(path, **kwargs)
            return self._stream_arrow_sync(batches, SourceFormat.CSV)
        rows = csv_adapter.stream_rows_sync(path, **adapter_kwargs)
        return self._stream_sync(rows, SourceFormat.CSV)

    def stream_jsonl(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> Iterator[ValidationResult]:
        """Streaming sync iterator over a JSONL file."""
        rows = jsonl_adapter.stream_rows_sync(path, **adapter_kwargs)
        return self._stream_sync(rows, SourceFormat.JSONL)

    def stream_parquet(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> Iterator[ValidationResult]:
        """Streaming sync iterator over a Parquet file."""
        if self._use_arrow:
            kwargs = _arrow_kwargs(adapter_kwargs, default_batch_size=10_000)
            batches = parquet_adapter.stream_record_batches_sync(path, **kwargs)
            return self._stream_arrow_sync(batches, SourceFormat.PARQUET)
        rows = parquet_adapter.stream_rows_sync(path, **adapter_kwargs)
        return self._stream_sync(rows, SourceFormat.PARQUET)

    def stream_arrow(
        self,
        path: str | Path,
        **adapter_kwargs: Any,
    ) -> Iterator[ValidationResult]:
        """Streaming sync iterator over an Arrow IPC / Feather file."""
        rows = arrow_adapter.stream_rows_sync(path, **adapter_kwargs)
        return self._stream_sync(rows, SourceFormat.ARROW)


def _run_coro(coro: Any) -> Any:
    """Drive a coroutine that performs no real awaits.

    All built-in :class:`StrategyHandler` methods fit this shape; the
    helper avoids the cost of spinning up an event loop per row.
    """
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    raise RuntimeError(
        "Sync streaming requires non-blocking strategy handlers; "
        "the active handler awaited a real coroutine."
    )


def stream_csv(
    path: str | Path,
    schema: type[BaseModel],
    **kwargs: Any,
) -> Iterator[ValidationResult]:
    """Convenience: build a :class:`StreamValidator` and validate a CSV."""
    sv_kwargs, adapter_kwargs = _split_kwargs(kwargs)
    sv = StreamValidator(schema, **sv_kwargs)
    return sv.stream_csv(path, **adapter_kwargs)


def stream_jsonl(
    path: str | Path,
    schema: type[BaseModel],
    **kwargs: Any,
) -> Iterator[ValidationResult]:
    """Convenience: build a :class:`StreamValidator` and validate a JSONL file."""
    sv_kwargs, adapter_kwargs = _split_kwargs(kwargs)
    sv = StreamValidator(schema, **sv_kwargs)
    return sv.stream_jsonl(path, **adapter_kwargs)


def stream_parquet(
    path: str | Path,
    schema: type[BaseModel],
    **kwargs: Any,
) -> Iterator[ValidationResult]:
    """Convenience: build a :class:`StreamValidator` and validate Parquet."""
    sv_kwargs, adapter_kwargs = _split_kwargs(kwargs)
    sv = StreamValidator(schema, **sv_kwargs)
    return sv.stream_parquet(path, **adapter_kwargs)


async def astream_csv(
    path: str | Path,
    schema: type[BaseModel],
    **kwargs: Any,
) -> AsyncIterator[ValidationResult]:
    """Async convenience wrapper for CSV."""
    sv_kwargs, adapter_kwargs = _split_kwargs(kwargs)
    sv = StreamValidator(schema, **sv_kwargs)
    async for r in sv.astream_csv(path, **adapter_kwargs):
        yield r


async def astream_jsonl(
    path: str | Path,
    schema: type[BaseModel],
    **kwargs: Any,
) -> AsyncIterator[ValidationResult]:
    """Async convenience wrapper for JSONL."""
    sv_kwargs, adapter_kwargs = _split_kwargs(kwargs)
    sv = StreamValidator(schema, **sv_kwargs)
    async for r in sv.astream_jsonl(path, **adapter_kwargs):
        yield r


async def astream_parquet(
    path: str | Path,
    schema: type[BaseModel],
    **kwargs: Any,
) -> AsyncIterator[ValidationResult]:
    """Async convenience wrapper for Parquet."""
    sv_kwargs, adapter_kwargs = _split_kwargs(kwargs)
    sv = StreamValidator(schema, **sv_kwargs)
    async for r in sv.astream_parquet(path, **adapter_kwargs):
        yield r


_VALIDATOR_KWARGS = {
    "on_error",
    "batch_size",
    "max_errors",
    "workers",
    "use_arrow",
}


def _split_kwargs(
    kwargs: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    sv: dict[str, Any] = {}
    adapter: dict[str, Any] = {}
    for k, v in kwargs.items():
        if k in _VALIDATOR_KWARGS:
            sv[k] = v
        else:
            adapter[k] = v
    return sv, adapter


# Adapter kwargs that mean different things on the row vs batch paths.
# When routing to ``stream_record_batches``, we drop options that don't
# apply (e.g. chunk_size, encoding, use_polars) so callers can pass the
# same kwargs they use for row mode.
_ROW_ONLY_ADAPTER_KWARGS = {"chunk_size", "use_polars", "encoding"}


def _arrow_kwargs(
    kwargs: dict[str, Any],
    *,
    default_batch_size: int | None = None,
) -> dict[str, Any]:
    out = {k: v for k, v in kwargs.items() if k not in _ROW_ONLY_ADAPTER_KWARGS}
    if default_batch_size is not None and "batch_size" not in out:
        out["batch_size"] = default_batch_size
    return out


def _stream_csv_record_batches_sync(
    path: str | Path,
    **kwargs: Any,
) -> Iterator[Any]:
    """Drive :func:`csv_adapter.stream_record_batches` from sync code.

    Uses a private event loop and steps the async generator one batch at
    a time, exactly mirroring how the row-mode sync helpers work.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    agen: AsyncGenerator[Any, None] = csv_adapter.stream_record_batches(
        path, **kwargs
    )
    try:
        while True:
            try:
                yield loop.run_until_complete(agen.__anext__())
            except StopAsyncIteration:
                return
    finally:
        try:
            loop.run_until_complete(agen.aclose())
        except Exception:
            pass
        loop.close()


__all__ = [
    "StreamValidator",
    "astream_csv",
    "astream_jsonl",
    "astream_parquet",
    "stream_csv",
    "stream_jsonl",
    "stream_parquet",
]

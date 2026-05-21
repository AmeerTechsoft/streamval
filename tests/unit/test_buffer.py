"""Tests for streamval.core.buffer."""

from __future__ import annotations

import tracemalloc
from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from streamval.core.buffer import BatchBuffer, ParallelBatchProcessor
from streamval.schema.coerce import SourceFormat
from streamval.schema.plan import CompiledValidationPlan


class _M(BaseModel):
    id: int
    name: str


async def _gen(n: int) -> AsyncIterator[dict]:
    for i in range(n):
        yield {"id": str(i), "name": f"n{i}"}


async def test_batch_buffer_emits_correct_sizes() -> None:
    buf = BatchBuffer(_gen(10), batch_size=4)
    sizes = [len(b) async for b in buf.batches()]
    assert sizes == [4, 4, 2]


async def test_batch_buffer_keeps_final_partial_batch() -> None:
    buf = BatchBuffer(_gen(7), batch_size=3)
    batches = [b async for b in buf.batches()]
    flat = [row for batch in batches for row in batch]
    assert len(flat) == 7
    assert flat[-1] == {"id": "6", "name": "n6"}


async def test_batch_buffer_empty_source() -> None:
    buf = BatchBuffer(_gen(0), batch_size=3)
    batches = [b async for b in buf.batches()]
    assert batches == []


def test_batch_buffer_rejects_bad_batch_size() -> None:
    with pytest.raises(ValueError):
        BatchBuffer(_gen(1), batch_size=0)


async def test_parallel_processor_inline_preserves_order() -> None:
    plan = CompiledValidationPlan(_M, SourceFormat.CSV)
    buf = BatchBuffer(_gen(20), batch_size=5)
    proc = ParallelBatchProcessor(buf, plan, workers=1)
    out = [r async for r in proc.stream()]
    assert [r.row_index for r in out] == list(range(20))
    assert all(r.valid for r in out)
    assert out[0].data is not None
    assert out[0].data.id == 0


async def test_parallel_processor_with_workers_preserves_order() -> None:
    plan = CompiledValidationPlan(_M, SourceFormat.CSV)
    buf = BatchBuffer(_gen(50), batch_size=7)
    proc = ParallelBatchProcessor(buf, plan, workers=4)
    out = [r async for r in proc.stream()]
    assert [r.row_index for r in out] == list(range(50))
    assert all(r.valid for r in out)


async def test_batch_buffer_bounded_memory() -> None:
    n = 5000
    batch_size = 100

    async def big_gen() -> AsyncIterator[dict]:
        for i in range(n):
            yield {"id": str(i), "name": "x" * 50}

    tracemalloc.start()
    tracemalloc.reset_peak()
    buf = BatchBuffer(big_gen(), batch_size=batch_size)
    seen = 0
    async for batch in buf.batches():
        seen += len(batch)
        assert len(batch) <= batch_size
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert seen == n
    # generous bound: peak under 10 MB even though n*row_size > that.
    assert peak < 10 * 1024 * 1024


async def test_parallel_processor_handles_invalid_rows() -> None:
    async def mixed() -> AsyncIterator[dict]:
        for i in range(6):
            yield {"id": "x" if i % 2 else str(i), "name": "n"}

    plan = CompiledValidationPlan(_M, SourceFormat.CSV)
    buf = BatchBuffer(mixed(), batch_size=3)
    proc = ParallelBatchProcessor(buf, plan, workers=2)
    out = [r async for r in proc.stream()]
    assert [r.valid for r in out] == [True, False, True, False, True, False]
    assert [r.row_index for r in out] == list(range(6))

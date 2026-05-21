"""Parquet streaming adapter.

Iterates record batches via :meth:`pyarrow.parquet.ParquetFile.iter_batches`
and yields one dict per row, never loading more than a single batch into
memory at a time.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


async def stream_rows(
    path: str | Path,
    *,
    batch_size: int = 1000,
    columns: list[str] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield one dict per row from a Parquet file.

    Args:
        path: Path to the Parquet file.
        batch_size: Rows per pyarrow record batch.
        columns: Optional projection of column names.

    Yields:
        One ``dict[str, Any]`` per row, with native Python values.

    Raises:
        FileNotFoundError: If the path does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Parquet file not found: {p}")

    pf = pq.ParquetFile(p)
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        cols = batch.to_pydict()
        names = list(cols.keys())
        rows = len(batch)
        for i in range(rows):
            yield {name: cols[name][i] for name in names}
            if i % 256 == 0:
                await asyncio.sleep(0)


def stream_rows_sync(
    path: str | Path,
    *,
    batch_size: int = 1000,
    columns: list[str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Streaming sync iterator over a Parquet file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Parquet file not found: {p}")
    pf = pq.ParquetFile(p)
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        cols = batch.to_pydict()
        names = list(cols.keys())
        for i in range(len(batch)):
            yield {name: cols[name][i] for name in names}

"""Arrow IPC / Feather streaming adapter.

Opens ``.arrow`` and ``.feather`` files via :mod:`pyarrow.ipc`, detecting
file vs. stream format from the magic bytes, and iterates record batches
the same way the Parquet adapter does.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

_FILE_MAGIC = b"ARROW1"


async def stream_rows(
    path: str | Path,
    *,
    columns: list[str] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield one dict per row from an Arrow IPC or Feather file.

    Args:
        path: Path to the file (``.arrow`` or ``.feather``).
        columns: Optional projection of column names.

    Yields:
        One ``dict[str, Any]`` per row.

    Raises:
        FileNotFoundError: If the path does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Arrow file not found: {p}")

    is_file_format = _is_file_format(p)

    with pa.memory_map(str(p), "r") as source:
        reader: ipc.RecordBatchFileReader | ipc.RecordBatchStreamReader
        if is_file_format:
            reader = ipc.open_file(source)
            n = reader.num_record_batches
            for i in range(n):
                batch = reader.get_batch(i)
                async for row in _emit_batch(batch, columns):
                    yield row
        else:
            reader = ipc.open_stream(source)
            for batch in reader:
                async for row in _emit_batch(batch, columns):
                    yield row


def _is_file_format(path: Path) -> bool:
    with open(path, "rb") as f:
        head = f.read(len(_FILE_MAGIC))
    return head == _FILE_MAGIC


async def _emit_batch(
    batch: pa.RecordBatch,
    columns: list[str] | None,
) -> AsyncIterator[dict[str, Any]]:
    cols = batch.to_pydict()
    names = list(cols.keys()) if columns is None else [c for c in columns if c in cols]
    rows = len(batch)
    for i in range(rows):
        yield {name: cols[name][i] for name in names}
        if i % 256 == 0:
            await asyncio.sleep(0)


def stream_rows_sync(
    path: str | Path,
    *,
    columns: list[str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Streaming sync iterator over an Arrow IPC / Feather file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Arrow file not found: {p}")
    is_file_format = _is_file_format(p)
    with pa.memory_map(str(p), "r") as src:
        if is_file_format:
            reader = ipc.open_file(src)
            for i in range(reader.num_record_batches):
                batch = reader.get_batch(i)
                yield from _emit_batch_sync(batch, columns)
        else:
            stream_reader = ipc.open_stream(src)
            for batch in stream_reader:
                yield from _emit_batch_sync(batch, columns)


def _emit_batch_sync(
    batch: pa.RecordBatch,
    columns: list[str] | None,
) -> Iterator[dict[str, Any]]:
    cols = batch.to_pydict()
    names = list(cols.keys()) if columns is None else [c for c in columns if c in cols]
    for i in range(len(batch)):
        yield {name: cols[name][i] for name in names}

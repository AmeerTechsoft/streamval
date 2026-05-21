"""CSV streaming adapter.

Reads a CSV file in chunks via ``aiofiles`` and yields one ``dict[str, str]``
per row. All values are surfaced as strings; type coercion is the schema
layer's job. The header line is read once on entry.
"""

from __future__ import annotations

import csv
import io
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import aiofiles


async def stream_rows(
    path: str | Path,
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    encoding: str = "utf-8",
    chunk_size: int = 65536,
) -> AsyncIterator[dict[str, Any]]:
    """Yield rows from a CSV file as dicts keyed by header name.

    Args:
        path: Path to the CSV file.
        delimiter: Field delimiter (default ``","``).
        quotechar: Quote character (default ``'"'``).
        encoding: File encoding (default ``"utf-8"``).
        chunk_size: Read buffer size in bytes.

    Yields:
        One ``dict[str, str]`` per data row. Header is consumed on entry.

    Raises:
        FileNotFoundError: If the path does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV file not found: {p}")

    async with aiofiles.open(p, encoding=encoding, newline="") as f:
        buffer = ""
        header: list[str] | None = None

        while True:
            chunk = await f.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            lines = buffer.split("\n")
            buffer = lines.pop()

            if header is None and lines:
                reader = csv.reader(
                    io.StringIO(lines[0] + "\n"),
                    delimiter=delimiter,
                    quotechar=quotechar,
                )
                header = next(reader)
                lines = lines[1:]

            if not lines:
                continue

            block = "\n".join(lines) + "\n"
            reader = csv.reader(
                io.StringIO(block),
                delimiter=delimiter,
                quotechar=quotechar,
            )
            for row in reader:
                if not row:
                    continue
                if header is None:
                    header = row
                    continue
                yield _zip_row(header, row)

        if buffer.strip():
            reader = csv.reader(
                io.StringIO(buffer),
                delimiter=delimiter,
                quotechar=quotechar,
            )
            for row in reader:
                if not row:
                    continue
                if header is None:
                    header = row
                    continue
                yield _zip_row(header, row)


def _zip_row(header: list[str], row: list[str]) -> dict[str, str]:
    if len(row) == len(header):
        return dict(zip(header, row, strict=True))
    out: dict[str, str] = {}
    for i, name in enumerate(header):
        out[name] = row[i] if i < len(row) else ""
    return out


def stream_rows_sync(
    path: str | Path,
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    encoding: str = "utf-8",
    chunk_size: int = 65536,  # accepted for parity; ignored on the sync path
) -> Iterator[dict[str, Any]]:
    """Streaming sync iterator over a CSV file.

    Uses :class:`csv.DictReader` directly to avoid the per-row
    asyncio overhead of the async generator path.
    """
    del chunk_size
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV file not found: {p}")
    with p.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter, quotechar=quotechar)
        yield from reader

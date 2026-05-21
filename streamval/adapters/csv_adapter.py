"""CSV streaming adapter.

Two paths, both yielding ``dict[str, Any]`` rows:

* ``_scan_csv_aiofiles`` — pure-stdlib aiofiles + :class:`csv.DictReader`.
  Always available; used when ``polars`` is not installed.
* ``_scan_csv_polars`` — :func:`polars.scan_csv` streaming engine,
  sliced and turned into row dicts via ``DataFrame.to_dicts()`` (a
  Rust-side conversion that's substantially faster than per-row
  ``csv.DictReader``). Used automatically when polars is available.

An additional path, ``_scan_csv_polars_arrow``, scaffolds the
``pyarrow.RecordBatch`` fast path that will be wired up in PROMPT A3.
It is marked with the ``# ARROW_FAST_PATH`` comment for discovery.
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import aiofiles
import pyarrow as pa

from streamval._compat import HAS_POLARS, polars

logger = logging.getLogger("streamval")


async def stream_rows(
    path: str | Path,
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    encoding: str = "utf-8",
    chunk_size: int = 65536,
    batch_size: int = 10_000,
    use_polars: bool | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield rows from a CSV file as ``dict[str, Any]``.

    Args:
        path: Path to the CSV file.
        delimiter: Field delimiter (default ``","``).
        quotechar: Quote character (default ``'"'``).
        encoding: File encoding (default ``"utf-8"``).
        chunk_size: Read buffer size (aiofiles path only).
        batch_size: Polars slice size (polars path only).
        use_polars: Force the polars or aiofiles path. ``None`` (the
            default) picks polars when available.

    Yields:
        One row dict per CSV data row. The header is consumed once on
        entry.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV file not found: {p}")

    take_polars = use_polars if use_polars is not None else HAS_POLARS
    if take_polars and HAS_POLARS:
        async for row in _scan_csv_polars(
            p,
            separator=delimiter,
            quote_char=quotechar,
            batch_size=batch_size,
        ):
            yield row
        return

    async for row in _scan_csv_aiofiles(
        p,
        delimiter=delimiter,
        quotechar=quotechar,
        encoding=encoding,
        chunk_size=chunk_size,
    ):
        yield row


async def _scan_csv_aiofiles(
    path: Path,
    *,
    delimiter: str,
    quotechar: str,
    encoding: str,
    chunk_size: int,
) -> AsyncIterator[dict[str, Any]]:
    """Pure-stdlib async CSV reader (no polars dependency)."""
    async with aiofiles.open(path, encoding=encoding, newline="") as f:
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


async def _scan_csv_polars(
    path: Path,
    *,
    separator: str,
    quote_char: str,
    batch_size: int,
) -> AsyncIterator[dict[str, Any]]:
    """Polars batched CSV scan, sliced into row dicts.

    Uses ``pl.read_csv_batched`` so the file is read in fixed-size
    chunks instead of materialising the whole DataFrame. v0.2.0
    accidentally called ``lazy.collect(engine="streaming")`` which
    in modern polars fully materialises the result and held the
    entire file in memory for the duration of the iteration.

    Uses ``infer_schema_length=0`` so all columns come back as strings —
    that matches the stdlib path and lets ``schema/coerce.py`` own all
    type conversion.
    """
    if not HAS_POLARS or polars is None:
        raise RuntimeError("polars is not installed")  # pragma: no cover

    pl = polars
    reader = pl.read_csv_batched(
        str(path),
        separator=separator,
        quote_char=quote_char,
        batch_size=batch_size,
        infer_schema_length=0,
    )
    while True:
        chunks = reader.next_batches(1)
        if not chunks:
            return
        for chunk in chunks:
            for row in chunk.to_dicts():
                yield row


# ARROW_FAST_PATH — scaffolded for PROMPT A3.
async def _scan_csv_polars_arrow(
    path: Path,
    *,
    separator: str,
    quote_char: str,
    batch_size: int,
    columns: list[str] | None = None,
) -> AsyncIterator[pa.RecordBatch]:
    """Polars → pyarrow ``RecordBatch`` scan (Arrow fast path).

    Not used by the row-mode :func:`stream_rows` entry point. PROMPT A3
    will wire this into :func:`stream_record_batches` and the batch
    validation pipeline. Columns are returned as native typed Arrow
    arrays (all ``Utf8``). We pass ``infer_schema=False`` so Pydantic,
    not polars, owns type coercion — otherwise a single non-integer
    cell in an inferred ``i64`` column aborts the whole file with
    ``polars.ComputeError`` instead of surfacing as a row-level
    Pydantic ``int_parsing`` error.
    """
    if not HAS_POLARS or polars is None:
        raise RuntimeError("polars is not installed")  # pragma: no cover

    pl = polars
    # ``infer_schema_length=0`` reads every column as ``Utf8``. We
    # deliberately do *not* let polars guess the column types: if it
    # guesses ``i64`` and the file contains a single non-integer cell
    # (e.g. ``"not-an-int"``), polars refuses to read the file at all
    # and raises ``ComputeError``. We want those rows to flow through
    # to Pydantic so the user sees a normal per-row ``ValidationResult``
    # with a Pydantic ``int_parsing`` error. Pydantic v2 happily
    # coerces ``"42"`` into ``int`` / ``"3.14"`` into ``float`` / etc.,
    # so the user-visible behaviour is unchanged.
    #
    # We use ``pl.read_csv_batched`` rather than
    # ``pl.scan_csv(...).collect(engine="streaming")``: the latter
    # materialises the whole DataFrame into memory in modern polars,
    # which broke the bounded-memory contract for large files.
    reader = pl.read_csv_batched(
        str(path),
        separator=separator,
        quote_char=quote_char,
        batch_size=batch_size,
        infer_schema_length=0,
        columns=columns,
    )
    while True:
        chunks = reader.next_batches(1)
        if not chunks:
            return
        for chunk in chunks:
            table = chunk.to_arrow()
            for batch in table.to_batches():
                yield batch


async def stream_record_batches(
    path: str | Path,
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    encoding: str = "utf-8",
    batch_size: int = 10_000,
    columns: list[str] | None = None,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """Yield :class:`pyarrow.RecordBatch` objects from a CSV file.

    When polars is installed the Arrow fast path (Rust-side parsing
    direct to Arrow buffers) is used. Otherwise we fall back to
    :class:`csv.DictReader` and synthesise RecordBatches from chunks of
    string-typed rows — slower than the polars path, but still avoids
    per-row Python overhead in the validation pipeline.

    Args:
        path: Path to the CSV file.
        delimiter: Field delimiter.
        quotechar: Quote character.
        encoding: Text encoding (stdlib fallback only).
        batch_size: Rows per emitted RecordBatch.
        columns: Optional projection (polars path only).

    Yields:
        :class:`pyarrow.RecordBatch` objects.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV file not found: {p}")

    if HAS_POLARS:
        async for batch in _scan_csv_polars_arrow(
            p,
            separator=delimiter,
            quote_char=quotechar,
            batch_size=batch_size,
            columns=columns,
        ):
            yield batch
        return

    logger.debug(
        "polars not installed; CSV batch mode falling back to stdlib csv"
    )
    async for batch in _scan_csv_stdlib_arrow(
        p,
        delimiter=delimiter,
        quotechar=quotechar,
        encoding=encoding,
        batch_size=batch_size,
    ):
        yield batch


async def _scan_csv_stdlib_arrow(
    path: Path,
    *,
    delimiter: str,
    quotechar: str,
    encoding: str,
    batch_size: int,
) -> AsyncIterator[pa.RecordBatch]:
    """stdlib-csv fallback that emits string-typed RecordBatches."""
    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
        try:
            header = next(reader)
        except StopIteration:
            return
        cols: list[list[str]] = [[] for _ in header]
        rows_in_batch = 0
        for row in reader:
            for i, name in enumerate(header):
                cols[i].append(row[i] if i < len(row) else "")
            rows_in_batch += 1
            if rows_in_batch >= batch_size:
                yield pa.RecordBatch.from_pydict(dict(zip(header, cols, strict=True)))
                cols = [[] for _ in header]
                rows_in_batch = 0
        if rows_in_batch:
            yield pa.RecordBatch.from_pydict(dict(zip(header, cols, strict=True)))


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
    batch_size: int = 10_000,
    use_polars: bool | None = None,
) -> Iterator[dict[str, Any]]:
    """Streaming sync iterator over a CSV file.

    Uses the polars path when available (Rust-side row-dict
    construction is roughly 3-5× faster than ``csv.DictReader``).
    Falls back to :class:`csv.DictReader` when polars is not installed.
    """
    del chunk_size
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV file not found: {p}")

    take_polars = use_polars if use_polars is not None else HAS_POLARS
    if take_polars and HAS_POLARS and polars is not None:
        yield from _scan_csv_polars_sync(
            p,
            separator=delimiter,
            quote_char=quotechar,
            batch_size=batch_size,
        )
        return

    with p.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter, quotechar=quotechar)
        yield from reader


def _scan_csv_polars_sync(
    path: Path,
    *,
    separator: str,
    quote_char: str,
    batch_size: int,
) -> Iterator[dict[str, Any]]:
    """Sync mirror of :func:`_scan_csv_polars`.

    Uses :func:`polars.read_csv_batched` so the file is streamed in
    chunks; see :func:`_scan_csv_polars` for the v0.2 regression
    fix this addresses.
    """
    pl = polars
    assert pl is not None  # guarded by caller
    reader = pl.read_csv_batched(
        str(path),
        separator=separator,
        quote_char=quote_char,
        batch_size=batch_size,
        infer_schema_length=0,
    )
    while True:
        chunks = reader.next_batches(1)
        if not chunks:
            return
        for chunk in chunks:
            yield from chunk.to_dicts()

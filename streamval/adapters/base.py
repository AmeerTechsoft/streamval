"""Adapter protocol and shared configuration.

Adapters expose two yield modes:

* ``"row"`` — async/sync iterator of ``dict[str, Any]`` rows (the
  original v0.1 contract; still the default behaviour for
  :func:`stream_rows` / :func:`stream_rows_sync`).
* ``"batch"`` — async iterator of :class:`pyarrow.RecordBatch` objects
  surfaced via :func:`stream_record_batches`. The batch mode bypasses
  the per-row Python dict construction entirely on the CSV and
  Parquet adapters and is the basis for the Arrow fast path used by
  :class:`streamval.StreamValidator` when ``use_arrow=True``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

AdapterMode = Literal["row", "batch"]
"""Yield-mode tag for adapters; see :class:`AdapterConfig`."""


@runtime_checkable
class FormatAdapter(Protocol):
    """Async-iterable protocol for format adapters."""

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class AdapterConfig:
    """Configuration shared by text-based adapters.

    Attributes:
        encoding: Text encoding for byte-oriented reads (default ``"utf-8"``).
        chunk_size: Read-buffer size in bytes for streaming I/O.
        skip_header: Whether to skip the first row (CSV-style adapters).
        mode: Yield mode. ``"row"`` (the default) yields per-row dicts.
            ``"batch"`` yields :class:`pyarrow.RecordBatch` objects.
    """

    encoding: str = "utf-8"
    chunk_size: int = 8192
    skip_header: bool = False
    mode: AdapterMode = "row"

"""Adapter protocol and shared configuration.

All adapters in this package expose the same shape: an async generator
function that yields ``dict[str, Any]`` rows one at a time, plus a sync
wrapper that runs the async generator via ``asyncio.run``. They all
accept a path and a small set of keyword options.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


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
    """

    encoding: str = "utf-8"
    chunk_size: int = 8192
    skip_header: bool = False

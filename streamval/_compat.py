"""Optional-dependency guards.

`orjson`, `polars`, and `httpx` are soft dependencies; the package
must remain fully functional without any of them. This module exposes
boolean flags (`HAS_ORJSON`, `HAS_POLARS`, `HAS_HTTPX`) and helper
raisers that produce informative ImportError messages when a caller
explicitly requests a fast path that isn't installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import types

    orjson: types.ModuleType | None
    polars: types.ModuleType | None
    httpx: types.ModuleType | None

try:
    import orjson as _orjson

    orjson = _orjson
    HAS_ORJSON: bool = True
except ImportError:
    orjson = None
    HAS_ORJSON = False

try:
    import polars as _polars

    polars = _polars
    HAS_POLARS: bool = True
except ImportError:
    polars = None
    HAS_POLARS = False

try:
    import httpx as _httpx

    httpx = _httpx
    HAS_HTTPX: bool = True
except ImportError:
    httpx = None
    HAS_HTTPX = False


def require_orjson() -> Any:
    """Return the imported ``orjson`` module or raise a helpful ImportError."""
    if orjson is None:
        raise ImportError(
            "orjson is not installed. Install with: pip install streamval[fast]"
        )
    return orjson


def require_polars() -> Any:
    """Return the imported ``polars`` module or raise a helpful ImportError."""
    if polars is None:
        raise ImportError(
            "polars is not installed. Install with: pip install streamval[fast]"
        )
    return polars


def require_httpx() -> Any:
    """Return the imported ``httpx`` module or raise a helpful ImportError."""
    if httpx is None:
        raise ImportError(
            "httpx is not installed. Install with: pip install streamval[http]"
        )
    return httpx


__all__ = [
    "HAS_HTTPX",
    "HAS_ORJSON",
    "HAS_POLARS",
    "httpx",
    "orjson",
    "polars",
    "require_httpx",
    "require_orjson",
    "require_polars",
]

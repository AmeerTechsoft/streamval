"""JSONL streaming adapter.

Reads a JSONL file line by line via ``aiofiles`` and yields one dict per
line. Uses ``orjson`` if available (via :mod:`streamval._compat`),
otherwise falls back to the stdlib ``json`` module. Blank lines are
silently skipped.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import aiofiles

from streamval._compat import HAS_ORJSON, orjson


async def stream_rows(
    path: str | Path,
    *,
    encoding: str = "utf-8",
) -> AsyncIterator[dict[str, Any]]:
    """Yield one dict per line from a JSONL file.

    Args:
        path: Path to the JSONL file.
        encoding: Text encoding (default ``"utf-8"``).

    Yields:
        Parsed JSON objects as dicts.

    Raises:
        FileNotFoundError: If the path does not exist.
        ValueError: If a non-blank line is not valid JSON or not an object.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"JSONL file not found: {p}")

    async with aiofiles.open(p, mode="rb") as f:
        line_no = 0
        async for raw in f:
            line_no += 1
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                if HAS_ORJSON and orjson is not None:
                    obj = orjson.loads(stripped)
                else:
                    obj = json.loads(stripped.decode(encoding))
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid JSON on line {line_no} of {p}: {exc}"
                ) from exc
            if not isinstance(obj, dict):
                raise ValueError(
                    f"Line {line_no} of {p} is not a JSON object: {type(obj).__name__}"
                )
            yield obj


def stream_rows_sync(
    path: str | Path,
    *,
    encoding: str = "utf-8",
) -> Iterator[dict[str, Any]]:
    """Streaming sync iterator over a JSONL file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"JSONL file not found: {p}")
    with p.open("rb") as f:
        for ln, raw in enumerate(f, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                if HAS_ORJSON and orjson is not None:
                    obj = orjson.loads(stripped)
                else:
                    obj = json.loads(stripped.decode(encoding))
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid JSON on line {ln} of {p}: {exc}"
                ) from exc
            if not isinstance(obj, dict):
                raise ValueError(
                    f"Line {ln} of {p} is not a JSON object: {type(obj).__name__}"
                )
            yield obj

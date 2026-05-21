"""Tests for streamval.adapters.jsonl_adapter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from streamval.adapters import jsonl_adapter


def _write_jsonl(tmp_path: Path, rows: list[dict]) -> Path:
    p = tmp_path / "data.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return p


async def test_jsonl_basic(tmp_path: Path) -> None:
    p = _write_jsonl(tmp_path, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    rows = [r async for r in jsonl_adapter.stream_rows(p)]
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


async def test_jsonl_skips_blank_lines(tmp_path: Path) -> None:
    p = tmp_path / "x.jsonl"
    p.write_text('{"a": 1}\n\n{"a": 2}\n   \n{"a": 3}\n', encoding="utf-8")
    rows = [r async for r in jsonl_adapter.stream_rows(p)]
    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]


async def test_jsonl_invalid_line_raises(tmp_path: Path) -> None:
    p = tmp_path / "x.jsonl"
    p.write_text('{"a": 1}\nnot json\n', encoding="utf-8")
    with pytest.raises(ValueError, match="line 2"):
        async for _ in jsonl_adapter.stream_rows(p):
            pass


async def test_jsonl_non_object_raises(tmp_path: Path) -> None:
    p = tmp_path / "x.jsonl"
    p.write_text("[1, 2, 3]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="not a JSON object"):
        async for _ in jsonl_adapter.stream_rows(p):
            pass


async def test_jsonl_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        async for _ in jsonl_adapter.stream_rows(tmp_path / "missing.jsonl"):
            pass


def test_jsonl_sync_wrapper(tmp_path: Path) -> None:
    p = _write_jsonl(tmp_path, [{"a": 1}, {"a": 2}])
    rows = list(jsonl_adapter.stream_rows_sync(p))
    assert rows == [{"a": 1}, {"a": 2}]

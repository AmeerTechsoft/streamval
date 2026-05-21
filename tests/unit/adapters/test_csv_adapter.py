"""Tests for streamval.adapters.csv_adapter."""

from __future__ import annotations

from pathlib import Path

import pytest

from streamval.adapters import csv_adapter


def _write_csv(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "data.csv"
    p.write_text(content, encoding="utf-8", newline="")
    return p


async def test_basic_csv_yields_rows(tmp_path: Path) -> None:
    p = _write_csv(
        tmp_path,
        "id,name,value\n1,alice,1.5\n2,bob,2.5\n3,carol,3.5\n",
    )
    rows = [r async for r in csv_adapter.stream_rows(p)]
    assert rows == [
        {"id": "1", "name": "alice", "value": "1.5"},
        {"id": "2", "name": "bob", "value": "2.5"},
        {"id": "3", "name": "carol", "value": "3.5"},
    ]


async def test_csv_handles_quoted_commas(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, 'id,name\n1,"hello, world"\n2,plain\n')
    rows = [r async for r in csv_adapter.stream_rows(p)]
    assert rows[0]["name"] == "hello, world"
    assert rows[1]["name"] == "plain"


async def test_csv_alternate_delimiter(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a;b\n1;2\n3;4\n")
    rows = [r async for r in csv_adapter.stream_rows(p, delimiter=";")]
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


async def test_csv_no_trailing_newline(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a,b\n1,2\n3,4")
    rows = [r async for r in csv_adapter.stream_rows(p)]
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


async def test_csv_small_chunk_size(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a,b\n1,2\n3,4\n5,6\n")
    rows = [r async for r in csv_adapter.stream_rows(p, chunk_size=4)]
    assert rows == [
        {"a": "1", "b": "2"},
        {"a": "3", "b": "4"},
        {"a": "5", "b": "6"},
    ]


async def test_csv_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        async for _ in csv_adapter.stream_rows(tmp_path / "missing.csv"):
            pass


def test_csv_sync_wrapper(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, "a,b\n1,2\n3,4\n")
    rows = list(csv_adapter.stream_rows_sync(p))
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


async def test_csv_polars_path_used_when_available(tmp_path: Path) -> None:
    """Force the polars path and confirm rows still come back as strings."""
    from streamval._compat import HAS_POLARS

    if not HAS_POLARS:
        pytest.skip("polars not installed")
    p = _write_csv(
        tmp_path,
        "id,name,value\n1,alice,1.5\n2,bob,2.5\n",
    )
    rows = [r async for r in csv_adapter.stream_rows(p, use_polars=True)]
    assert rows == [
        {"id": "1", "name": "alice", "value": "1.5"},
        {"id": "2", "name": "bob", "value": "2.5"},
    ]


async def test_csv_aiofiles_fallback(tmp_path: Path) -> None:
    """Force the aiofiles fallback path explicitly."""
    p = _write_csv(
        tmp_path,
        "id,name\n1,alice\n2,bob\n3,carol\n",
    )
    rows = [r async for r in csv_adapter.stream_rows(p, use_polars=False)]
    assert rows == [
        {"id": "1", "name": "alice"},
        {"id": "2", "name": "bob"},
        {"id": "3", "name": "carol"},
    ]


async def test_csv_both_paths_produce_identical_output(tmp_path: Path) -> None:
    from streamval._compat import HAS_POLARS

    if not HAS_POLARS:
        pytest.skip("polars not installed")
    p = _write_csv(
        tmp_path,
        "id,name,score\n1,alice,9.5\n2,bob,8.0\n3,carol,7.25\n",
    )
    polars_rows = [r async for r in csv_adapter.stream_rows(p, use_polars=True)]
    aiofiles_rows = [r async for r in csv_adapter.stream_rows(p, use_polars=False)]
    assert polars_rows == aiofiles_rows


async def test_csv_polars_arrow_scaffold_yields_record_batches(
    tmp_path: Path,
) -> None:
    """Smoke test for the ARROW_FAST_PATH scaffolded for PROMPT A3."""
    from streamval._compat import HAS_POLARS

    if not HAS_POLARS:
        pytest.skip("polars not installed")
    import pyarrow as pa

    p = _write_csv(
        tmp_path,
        "id,name\n1,alice\n2,bob\n3,carol\n",
    )
    batches = [
        b
        async for b in csv_adapter._scan_csv_polars_arrow(
            p, separator=",", quote_char='"', batch_size=2
        )
    ]
    assert len(batches) >= 1
    assert all(isinstance(b, pa.RecordBatch) for b in batches)
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 3

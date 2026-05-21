"""End-to-end JSONL validation."""

from __future__ import annotations

import json
import tracemalloc
from pathlib import Path

from pydantic import BaseModel

from streamval.core.validator import StreamValidator, stream_jsonl


class Row(BaseModel):
    id: int
    name: str
    score: float


def _write_mixed_jsonl(path: Path, n: int) -> tuple[int, int]:
    valid = invalid = 0
    with path.open("w", encoding="utf-8") as f:
        for i in range(n):
            if i % 5 == 0:
                f.write(json.dumps({"id": "bad", "name": f"n{i}"}) + "\n")
                invalid += 1
            else:
                f.write(json.dumps({"id": i, "name": f"n{i}", "score": 1.5}) + "\n")
                valid += 1
    return valid, invalid


def test_jsonl_collect(tmp_path: Path) -> None:
    p = tmp_path / "data.jsonl"
    valid, invalid = _write_mixed_jsonl(p, 1000)
    v = StreamValidator(Row, on_error="collect", batch_size=100)
    results = list(v.stream_jsonl(p))
    assert sum(1 for r in results if r.valid) == valid
    assert sum(1 for r in results if not r.valid) == invalid


def test_jsonl_skip(tmp_path: Path) -> None:
    p = tmp_path / "data.jsonl"
    valid, _ = _write_mixed_jsonl(p, 200)
    results = list(stream_jsonl(p, Row, on_error="skip"))
    assert len(results) == valid
    assert all(r.valid for r in results)


def test_jsonl_memory_bounded(tmp_path: Path) -> None:
    p = tmp_path / "big.jsonl"
    _write_mixed_jsonl(p, 10000)
    tracemalloc.start()
    tracemalloc.reset_peak()
    v = StreamValidator(Row, on_error="skip", batch_size=1000)
    count = sum(1 for _ in v.stream_jsonl(p))
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert count > 7000
    assert peak < 50 * 1024 * 1024

"""Cross-format integration: validate the same logical data in multiple formats."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq
from pydantic import BaseModel

from streamval.core.validator import StreamValidator


class Row(BaseModel):
    id: int
    name: str
    value: float


def _make_data(n: int) -> dict[str, list]:
    return {
        "id": list(range(n)),
        "name": [f"n{i}" for i in range(n)],
        "value": [float(i) * 1.5 for i in range(n)],
    }


def test_same_data_all_formats_match(tmp_path: Path) -> None:
    n = 250
    data = _make_data(n)

    csv_path = tmp_path / "x.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value"])
        for i in range(n):
            w.writerow([data["id"][i], data["name"][i], data["value"][i]])

    jsonl_path = tmp_path / "x.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for i in range(n):
            f.write(
                json.dumps(
                    {
                        "id": data["id"][i],
                        "name": data["name"][i],
                        "value": data["value"][i],
                    }
                )
                + "\n"
            )

    parquet_path = tmp_path / "x.parquet"
    pq.write_table(pa.table(data), parquet_path)

    feather_path = tmp_path / "x.feather"
    feather.write_feather(pa.table(data), feather_path)

    counts = []
    for streamer in (
        lambda: StreamValidator(Row, on_error="collect").stream_csv(csv_path),
        lambda: StreamValidator(Row, on_error="collect").stream_jsonl(jsonl_path),
        lambda: StreamValidator(Row, on_error="collect").stream_parquet(parquet_path),
        lambda: StreamValidator(Row, on_error="collect").stream_arrow(feather_path),
    ):
        rows = list(streamer())
        counts.append(len(rows))
        assert all(r.valid for r in rows)

    assert counts == [n, n, n, n]

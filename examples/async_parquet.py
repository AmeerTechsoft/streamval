"""Async Parquet validation with the ``skip`` strategy.

Run with:
    python examples/async_parquet.py
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from streamval import StreamValidator


class Event(BaseModel):
    id: int
    user_id: int
    value: float


async def run(path: Path) -> None:
    v = StreamValidator(Event, on_error="skip", batch_size=500)
    n_valid = 0
    async for r in v.astream_parquet(path):
        if r.valid:
            n_valid += 1
    print(f"emitted {n_valid} valid rows")
    print(v.stats)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "events.parquet"
        table = pa.table(
            {
                "id": list(range(1000)),
                "user_id": [i % 50 for i in range(1000)],
                "value": [i * 0.5 for i in range(1000)],
            }
        )
        pq.write_table(table, path)
        asyncio.run(run(path))


if __name__ == "__main__":
    main()

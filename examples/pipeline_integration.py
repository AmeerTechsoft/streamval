"""Pipeline integration: validate, transform, and re-emit.

Reads a CSV, validates rows with streamval, applies a transformation,
and writes a clean output JSONL of the valid rows.

Run with:
    python examples/pipeline_integration.py
"""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

from pydantic import BaseModel

from streamval import StreamValidator


class Order(BaseModel):
    id: int
    sku: str
    qty: int
    price: float

    @property
    def total(self) -> float:
        return self.qty * self.price


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "orders.csv"
        dst = Path(tmp) / "orders_clean.jsonl"

        with src.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "sku", "qty", "price"])
            w.writerow([1, "A1", 2, 9.99])
            w.writerow([2, "B2", "abc", 5.00])
            w.writerow([3, "C3", 1, "free"])
            w.writerow([4, "D4", 5, 1.5])

        v = StreamValidator(Order, on_error="skip")
        with dst.open("w", encoding="utf-8") as out:
            for r in v.stream_csv(src):
                assert r.data is not None
                out.write(
                    json.dumps(
                        {
                            "id": r.data.id,
                            "sku": r.data.sku,
                            "total": r.data.total,
                        }
                    )
                    + "\n"
                )

        print(dst.read_text(encoding="utf-8").strip())
        print()
        print(v.stats)


if __name__ == "__main__":
    main()

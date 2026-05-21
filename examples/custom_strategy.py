"""Writing a custom error strategy.

Demonstrates a `StrategyHandler` that drops invalid rows but **writes
them to a side file** for later analysis.

Run with:
    python examples/custom_strategy.py
"""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from streamval import StreamValidator, ValidationResult
from streamval.strategies.base import StrategyHandler


class _QuarantineHandler(StrategyHandler):
    """Drops invalid rows and dumps them to a JSONL file."""

    def __init__(self, sidecar: Path) -> None:
        self._fp = sidecar.open("w", encoding="utf-8")
        self._dropped = 0

    async def handle(
        self, result: ValidationResult
    ) -> ValidationResult | None:
        if result.valid:
            return result
        self._fp.write(
            json.dumps(
                {
                    "row_index": result.row_index,
                    "raw": result.raw,
                    "errors": [
                        {"field": e.field, "message": e.message}
                        for e in result.errors
                    ],
                }
            )
            + "\n"
        )
        self._dropped += 1
        return None

    async def finalize(self) -> None:
        self._fp.close()

    @property
    def summary(self) -> dict[str, Any]:
        return {"strategy": "quarantine", "dropped": self._dropped}


class Row(BaseModel):
    id: int
    name: str


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "data.csv"
        bad = Path(tmp) / "bad_rows.jsonl"

        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "name"])
            w.writerow([1, "alice"])
            w.writerow(["x", "bob"])
            w.writerow([3, "carol"])

        v = StreamValidator(Row, on_error="collect")
        v._handler = _QuarantineHandler(bad)  # drop in a custom handler

        good = list(v.stream_csv(path))
        print(f"kept {len(good)} valid rows")
        print(f"quarantined: {bad.read_text(encoding='utf-8').strip()}")


if __name__ == "__main__":
    main()

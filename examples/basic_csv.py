"""Basic CSV validation with the ``collect`` strategy.

Run with:
    python examples/basic_csv.py
"""

from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from pydantic import BaseModel

from streamval import StreamValidator


class User(BaseModel):
    id: int
    name: str
    score: float
    active: bool


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "users.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "name", "score", "active"])
            w.writerow([1, "alice", 9.5, "true"])
            w.writerow(["bad", "bob", 8.0, "true"])  # invalid id
            w.writerow([3, "carol", "not-a-float", "false"])  # invalid score
            w.writerow([4, "dan", 7.0, "true"])

        v = StreamValidator(User, on_error="collect", batch_size=2)
        for r in v.stream_csv(path):
            if r.valid:
                assert r.data is not None
                print(f"row {r.row_index}: OK   {r.data}")
            else:
                print(f"row {r.row_index}: BAD  {[str(e) for e in r.errors]}")

        print()
        print(v.stats)


if __name__ == "__main__":
    main()

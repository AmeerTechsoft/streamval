# streamval

**Streaming, Pydantic-backed validation for CSV, JSONL, Parquet, and Arrow.**

Existing data-validation libraries (Pydantic, Pandera, Great Expectations,
Cerberus) all assume the dataset fits in memory. `streamval` keeps the
file on disk and validates it row by row through a Pydantic schema, so
you can validate a multi-gigabyte file with a few tens of megabytes of
RAM and start consuming valid rows immediately.

## Install

```bash
pip install streamval
# faster JSON + lazy CSV via polars/orjson:
pip install "streamval[fast]"
```

## Quickstart

```python
from pydantic import BaseModel
from streamval import stream_csv

class User(BaseModel):
    id: int
    name: str
    score: float
    active: bool

for result in stream_csv("users.csv", User, on_error="collect"):
    if result.valid:
        user = result.data
        # ... do something with the parsed model ...
    else:
        for err in result.errors:
            print(f"row {result.row_index}: {err}")
```

The generator finishes when the file ends. Stats are available on the
underlying validator:

```python
from streamval import StreamValidator
v = StreamValidator(User, on_error="skip", batch_size=2000)
for r in v.stream_csv("users.csv"):
    handle(r.data)
print(v.stats)  # rows_total, rows_valid, throughput_rps, peak_memory_mb, ...
```

## Performance

`streamval` optimises for **bounded memory** rather than peak throughput.
On a developer Windows laptop, 1,000,000 rows of CSV validate at ~11k
rows/sec with **< 1 MB** of peak Python-object memory (batch_size=1000).
A naive "load everything, validate one by one" loop is roughly 10× faster
on small files, but linearly grows in RAM and OOMs on large files.
Choose `streamval` when your file does not fit in memory or when you want
to start consuming valid rows immediately.

## Why not Pydantic / Pandera / Great Expectations?

| Library | Loads whole file? | Streams? | Multi-format? | Async? |
|---|---|---|---|---|
| Pydantic v2 | yes (caller decides) | no | no | no |
| Pandera | yes (DataFrame) | no | DataFrame only | no |
| Great Expectations | yes (DataFrame) | no | DataFrame only | no |
| Cerberus | per-record only | no | no | no |
| **streamval** | **no** | **yes** | **CSV/JSONL/Parquet/Arrow** | **yes** |

## How it works

* Each format has a tiny async-generator adapter that yields one row dict
  at a time without loading the whole file.
* A `BatchBuffer` chunks the row stream into fixed-size lists so peak
  memory stays bounded by `batch_size`.
* Each batch is run through a `CompiledValidationPlan` (a per-model,
  cached wrapper around `model.model_validate`).
* A pluggable error strategy (`fail_fast`, `collect`, `skip`) decides
  whether each row is emitted, dropped, or terminates the run.
* A `StatsAccumulator` records per-field error counts, throughput, and
  peak memory via `tracemalloc`.

## Error strategies

* `fail_fast` — raise `StreamValidationError` on the first invalid row.
* `collect` — emit every row; if `max_errors` is exceeded, raise on
  finalize.
* `skip` — drop invalid rows silently (logged at WARNING level).

## Contributing

```bash
git clone https://github.com/your-org/streamval
cd streamval
pip install -e ".[dev]"
pytest
```

## License

MIT — see [LICENSE](LICENSE).

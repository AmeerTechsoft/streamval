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

`streamval` optimises for **bounded memory** with strong throughput as
a secondary goal. The v0.2 Arrow batch fast path validates an entire
`pyarrow.RecordBatch` per Python ↔ Rust boundary crossing instead of
one row dict at a time:

| Mode | Approx rps (CI target) | Peak memory |
|---|---|---|
| streamval CSV — batch (Arrow path) | 35 000+ (polars installed) | < 5 MB |
| streamval Parquet — batch (Arrow path) | 45 000+ | < 5 MB |
| streamval CSV — row mode (polars) | ~14 000 | < 5 MB |
| streamval CSV — row mode (aiofiles fallback) | ~11 000 | < 5 MB |
| Naive Pydantic loop | ~120 000 | ~1 GB (reads whole file) |

> The naive loop is faster on small files but **loads the entire dataset
> into RAM**. `streamval` is the right choice when files don't fit in
> memory or you want to start consuming valid rows immediately.

> Numbers from a developer Windows laptop with Python 3.13. Real CI
> hardware (Linux x86, faster I/O) typically shows 2-3× higher
> throughput. Run `STREAMVAL_BENCH=1 pytest tests/benchmarks/` to
> measure on your own machine.

### Performance tuning

* Install `streamval[fast]` to unlock the polars Arrow path for CSV.
  Parquet gets the Arrow fast path with no extra dependency.
* `use_arrow=True` is the default for CSV and Parquet on the
  `StreamValidator` constructor. Pass `use_arrow=False` to fall back to
  the row-mode pipeline (useful for adapters or strategies that need
  per-row Python dicts).
* `batch_size` is the main throughput / memory dial — larger batches
  mean fewer Python ↔ Rust crossings but slightly higher peak memory.
  The defaults give comfortable bounded-memory behaviour:

      batch_size=100   → ~0.05 MB peak
      batch_size=1000  → ~0.4 MB peak  (default)
      batch_size=5000  → ~1.8 MB peak
      batch_size=10000 → ~3.5 MB peak

* `workers > 1` enables a thread pool. Pydantic's Rust core is
  thread-safe; per-row ordering is preserved.

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
git clone https://github.com/AmeerTechsoft/streamval
cd streamval
pip install -e ".[dev]"
pytest
```

## License

MIT — see [LICENSE](LICENSE).

# streamval

**Streaming, Pydantic-backed validation for CSV, JSONL, Parquet, Arrow,
and HTTP NDJSON / SSE.**

Existing data-validation libraries (Pydantic, Pandera, Great Expectations,
Cerberus) all assume the dataset fits in memory. `streamval` keeps the
file (or HTTP response) on disk / on the wire and validates it row by
row through a Pydantic schema, so you can validate a multi-gigabyte
file with a few tens of megabytes of RAM and start consuming valid
rows immediately. The same streaming model handles LLM token streams,
log services, and any REST endpoint that emits NDJSON or Server-Sent
Events.

## Install

```bash
pip install streamval
# faster JSON + lazy CSV via polars/orjson:
pip install "streamval[fast]"
# HTTP NDJSON / LLM streaming via httpx:
pip install "streamval[http]"
# everything:
pip install "streamval[fast,http]"
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
a secondary goal. Throughput, 100 000 rows, 4-column schema:

| Mode | rows/sec | Peak memory, 1M rows |
|---|---|---|
| streamval Parquet — batch (Arrow path) | ~112 000 | bounded by `batch_size` |
| streamval CSV — row mode | ~100 000 | 0.5 MB @ `batch_size=1000` |
| streamval CSV — batch (Arrow path) | ~88 000 | 1.9 MB @ `batch_size=1000` |
| Naive Pydantic loop | ~160 000 | ~1 GB (reads whole file) |

> **On the naive loop:** it stays the fastest option for files that fit
> comfortably in RAM, because `streamval` does strictly more work per
> row — it allocates a `ValidationResult` carrying the row index, the
> raw row, and any field errors. That per-row object *is* the feature.
> `streamval` is the right choice when files don't fit in memory, when
> you want valid rows immediately, or when you need per-row error
> reporting rather than a single exception.

> Numbers from a developer Windows laptop with Python 3.13, pydantic
> 2.11, polars 1.40. Run `STREAMVAL_BENCH=1 pytest tests/benchmarks/`
> to measure on your own machine.

### Performance tuning

* **Leave `track_memory=False` (the default).** It gates `tracemalloc`,
  which hooks every allocation and costs roughly **4-5× throughput**.
  Turn it on only for profiling runs — it is what populates
  `stats.peak_memory_mb`, which reads `0.0` while it is off.
* Install `streamval[fast]` to unlock the polars path for CSV.
  Parquet gets the Arrow fast path with no extra dependency. On the sync
  path polars is roughly on par with the stdlib `csv` reader for simple
  schemas — measure before adding the dependency for CSV alone.
* `use_arrow=True` is the default for CSV and Parquet, and is a clear
  win for Parquet. For CSV the two modes now measure within a few
  percent of each other; row mode holds a small edge on peak memory at
  the same `batch_size`. Benchmark both on your own data.
* CSV batch mode casts whole columns in Arrow rather than converting
  each cell in Python. Columns that parse cleanly take the vectorised
  path; a column containing even one unparseable cell falls back to
  per-row coercion, so results are identical either way — only the
  speed differs. Clean, canonically-formatted CSV is therefore fastest:
  unpadded numbers, `true`/`false` rather than `yes`/`no`, ISO dates.
* `batch_size` is the main throughput / memory dial — larger batches
  mean fewer Python ↔ Rust crossings but proportionally higher peak
  memory. Measured peak on 1M rows, Arrow batch mode:

      batch_size=100   → ~1.9 MB peak
      batch_size=1000  → ~1.9 MB peak  (default)
      batch_size=5000  → ~9 MB peak
      batch_size=10000 → ~16 MB peak

  Below `batch_size≈1000` peak memory stops falling — a fixed
  polars/Arrow scan buffer dominates, so smaller batches cost
  throughput without buying memory back. `batch_size=1000` (the
  default) is the sweet spot; go smaller only if you have measured a
  reason to.

* `workers > 1` enables a thread pool. Pydantic's Rust core is
  thread-safe; per-row ordering is preserved.

## Formats

| Format  | Source        | Requires                                       |
|---------|---------------|------------------------------------------------|
| CSV     | file / path   | (none, or `streamval[fast]` for polars path)   |
| JSONL   | file / path   | (none, or `streamval[fast]` for orjson)        |
| Parquet | file / path   | `pyarrow` (always-on dependency)               |
| Arrow   | file / path   | `pyarrow` (always-on dependency)               |
| NDJSON  | HTTP URL      | `streamval[http]` (httpx)                      |
| SSE/LLM | HTTP URL      | `streamval[http]` (httpx)                      |

## Why not Pydantic / Pandera / Great Expectations?

| Library | Loads whole file? | Streams? | Multi-format? | Async? |
|---|---|---|---|---|
| Pydantic v2 | yes (caller decides) | no | no | no |
| Pandera | yes (DataFrame) | no | DataFrame only | no |
| Great Expectations | yes (DataFrame) | no | DataFrame only | no |
| Cerberus | per-record only | no | no | no |
| **streamval** | **no** | **yes** | **CSV / JSONL / Parquet / Arrow / HTTP NDJSON / SSE** | **yes** |

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

## Documentation

**Online docs:** [streamval.readthedocs.io](https://streamval.readthedocs.io/en/latest/)

Full guides (also in [`docs/`](docs/index.md)):

| Guide | Topics |
|---|---|
| [Documentation index](docs/index.md) | End-to-end workflow, install, doc map |
| [Getting started](docs/getting-started/index.md) | Quickstart and schema design |
| [User guide](docs/user-guide/index.md) | Validation, results, error strategies |
| [Reference](docs/reference/index.md) | Adapters, API, Python docstrings |
| [Development](docs/development/index.md) | Benchmarks and changelog |

## Contributing

```bash
git clone https://github.com/AmeerTechsoft/streamval
cd streamval
pip install -e ".[dev]"
pytest
```

## License

MIT — see [LICENSE](LICENSE).

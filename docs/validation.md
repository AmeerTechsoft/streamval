# Running validation

This guide covers every way to invoke streamval: the `StreamValidator`
class, one-shot convenience functions, sync vs async, and all supported
source formats.

---

## Two ways to call streamval

### 1. Convenience functions (one-liners)

Best for scripts and quick jobs. A new `StreamValidator` is created per
call.

```python
from streamval import stream_csv, stream_jsonl, stream_parquet, stream_http_ndjson

for result in stream_csv("data.csv", MyModel, on_error="collect"):
    ...
```

Keyword arguments split automatically:

- **Validator options:** `on_error`, `batch_size`, `max_errors`,
  `workers`, `use_arrow`
- **Adapter options:** everything else (`encoding`, `delimiter`,
  `auth_token`, …)

```python
stream_csv(
    "data.csv",
    MyModel,
    on_error="skip",       # → StreamValidator
    batch_size=2000,       # → StreamValidator
    encoding="latin-1",      # → csv adapter
    use_polars=False,        # → csv adapter (force stdlib path)
)
```

### 2. StreamValidator (reusable engine)

Best when you need **stats**, **multiple files**, or **custom handlers**.

```python
from streamval import StreamValidator

validator = StreamValidator(
    MyModel,
    on_error="collect",
    batch_size=1000,
    max_errors=100,
    workers=1,
    use_arrow=True,
)

for result in validator.stream_csv("file1.csv"):
    handle(result)

for result in validator.stream_jsonl("file2.jsonl"):
    handle(result)

print(validator.stats)   # cumulative stats for the last run
```

Create a **new** `StreamValidator` per independent run if you want
clean stats. Reusing the same instance overwrites stats on each stream.

---

## StreamValidator parameters

| Parameter | Default | Description |
|---|---|---|
| `schema` | (required) | Your Pydantic `BaseModel` subclass |
| `on_error` | `"collect"` | `"fail_fast"`, `"collect"`, or `"skip"` |
| `batch_size` | `1000` | Rows per validation batch; also sizes CSV polars chunks |
| `max_errors` | `None` | With `collect`: raise if invalid count exceeds this |
| `workers` | `1` | Thread-pool size for batch validation (`>1` preserves order) |
| `use_arrow` | `True` | CSV/Parquet Arrow fast path (see below) |

---

## Sync vs async

Every format has a sync and async entry point:

| Sync | Async |
|---|---|
| `stream_csv` | `astream_csv` |
| `stream_jsonl` | `astream_jsonl` |
| `stream_parquet` | `astream_parquet` |
| `stream_http_ndjson` | `astream_http_ndjson` |

**Sync** (`StreamValidator.stream_*`):

- No asyncio overhead — fastest for scripts and CLI tools.
- Returns a plain Python `Iterator[ValidationResult]`.

**Async** (`StreamValidator.astream_*`):

- Use inside `asyncio` pipelines (FastAPI, aiohttp workers, etc.).
- Returns `AsyncIterator[ValidationResult]`.

```python
import asyncio
from streamval import astream_csv

async def main():
    async for result in astream_csv("big.csv", MyModel, on_error="skip"):
        if result.valid:
            await save(result.data)

asyncio.run(main())
```

Custom error strategies with real `await` calls require the async path.

---

## File adapters

### CSV

```python
from streamval import StreamValidator

v = StreamValidator(Order, on_error="collect")

# Default: Arrow batch path when polars is installed
for r in v.stream_csv("orders.csv"):
    ...

# Row mode (dict per row, still streaming)
v_row = StreamValidator(Order, use_arrow=False)
for r in v_row.stream_csv("orders.csv"):
    ...

# Force stdlib csv.DictReader (no polars)
for r in v_row.stream_csv("orders.csv", use_polars=False):
    ...
```

**Adapter kwargs:** `delimiter`, `quotechar`, `encoding`, `use_polars`,
`batch_size` (override validator default).

### JSONL

```python
for r in v.stream_jsonl("events.jsonl", encoding="utf-8"):
    ...
```

Each non-blank line must be a JSON object (`{...}`). Blank lines are
skipped. Uses `orjson` when installed (`streamval[fast]`).

### Parquet

```python
for r in v.stream_parquet("data.parquet"):
    ...
```

Values arrive with native Arrow types — no string coercion. The Arrow
fast path (`use_arrow=True`, default) validates `RecordBatch` objects
directly.

### Arrow IPC / Feather

```python
for r in v.stream_arrow("data.feather"):
    ...
```

---

## HTTP NDJSON and SSE

Requires `pip install streamval[http]`.

```python
from streamval import stream_http_ndjson, HttpNdjsonConfig

# URL string + kwargs
for r in stream_http_ndjson(
    "https://api.example.com/events",
    EventModel,
    on_error="collect",
    auth_token="Bearer sk-...",
    timeout_seconds=30.0,
    max_retries=3,
    event_stream=False,
):
    ...

# Full config object
config = HttpNdjsonConfig.from_url(
    "https://api.example.com/stream",
    auth_token="sk-...",
    event_stream=True,
    max_lines=500,
)
for r in stream_http_ndjson(config, EventModel):
    ...
```

See [Adapters reference](adapters.md) for every `HttpNdjsonConfig`
field, retry behaviour, and SSE parsing.

---

## LLM streaming

Requires `streamval[http]`. No OpenAI/Anthropic SDK — only HTTP + JSON.

```python
from streamval import llm
from pydantic import BaseModel

class Chunk(BaseModel):
    choices: list[dict] | None = None
    delta: dict | None = None

for result in llm.validate_llm_stream(
    "https://api.openai.com/v1/chat/completions",
    Chunk,
    provider=llm.LLMProvider.OPENAI,
    auth_token="sk-...",
    on_error="skip",
):
    if result.valid:
        text = llm.extract_content(result, llm.LLMProvider.OPENAI)
        if text:
            print(text, end="", flush=True)
```

Providers: `OPENAI`, `ANTHROPIC`, `GENERIC_SSE`, `GENERIC_NDJSON`.

Async: `llm.avalidate_llm_stream(...)`.

---

## The Arrow fast path (`use_arrow`)

When `use_arrow=True` (default) for CSV and Parquet:

- The adapter yields `pyarrow.RecordBatch` objects.
- Validation runs in bulk via Pydantic's `TypeAdapter(list[Model])`.
- Higher throughput; memory stays bounded by `batch_size`.

When `use_arrow=False`:

- The adapter yields one Python dict per row.
- Same public API and `ValidationResult` output.

JSONL and HTTP always use the row-dict path.

---

## Parsing and consuming results

Every stream method returns a **generator** of `ValidationResult`.
Iterate it once; rows are validated lazily as the source is read.

```python
valid_rows = []
invalid_rows = []

for result in stream_csv("data.csv", MyModel, on_error="collect"):
    if result.valid:
        valid_rows.append(result.data)       # MyModel instance
    else:
        invalid_rows.append({
            "row": result.row_index,
            "raw": result.raw,
            "errors": [str(e) for e in result.errors],
        })

print(f"{len(valid_rows)} valid, {len(invalid_rows)} invalid")
```

With `on_error="skip"`, invalid rows are **not yielded** — only valid
rows appear in the iterator. Check `validator.stats.rows_invalid` for
the count.

With `on_error="fail_fast"`, the iterator raises `StreamValidationError`
on the first bad row.

See [Results, errors, and logging](results-and-errors.md) for full
detail on every field and exception type.

---

## Performance tuning

| Knob | Effect |
|---|---|
| `batch_size=100` | Lowest latency to first result; ~0.1 MB peak (row mode) |
| `batch_size=1000` | Default; good balance |
| `batch_size=5000+` | Higher throughput; slightly higher peak memory |
| `use_arrow=True` | Fastest for CSV/Parquet when polars installed |
| `workers=4` | Parallel batch validation; order preserved |
| `streamval[fast]` | polars CSV path + orjson JSONL |

HTTP streams: use smaller `batch_size` (100–500) for interactive
latency; larger (1000+) for bulk API dumps.

---

## Examples in the repo

| Script | Demonstrates |
|---|---|
| `examples/basic_csv.py` | CSV + `collect` strategy + stats |
| `examples/async_parquet.py` | Async Parquet streaming |
| `examples/custom_strategy.py` | Custom `StrategyHandler` (quarantine) |
| `examples/pipeline_integration.py` | Multi-stage pipeline |
| `examples/http_ndjson_basic.py` | HTTP NDJSON over local server |
| `examples/llm_streaming.py` | OpenAI/Anthropic SSE offline demo |

Run any example:

```bash
pip install -e ".[fast,http]"
python examples/basic_csv.py
```

---

## Next steps

- [Results, errors, and logging](results-and-errors.md)
- [Error strategies](error-strategies.md)

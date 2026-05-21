# streamval documentation

**streamval** validates large datasets row-by-row through a Pydantic schema
without loading the whole file into memory. It supports CSV, JSONL,
Parquet, Arrow, HTTP NDJSON, and LLM SSE streams.

This guide walks through the full workflow: define a schema → choose an
adapter → run validation → handle results, errors, and logs.

---

## Documentation map

| Guide | What you'll learn |
|---|---|
| [Schemas and models](schemas.md) | Defining Pydantic models, type coercion per format, nested fields |
| [Running validation](validation.md) | `StreamValidator`, convenience functions, sync/async, all formats |
| [Results, errors, and logging](results-and-errors.md) | `ValidationResult`, exceptions, stats, configuring loggers |
| [Error strategies](error-strategies.md) | `fail_fast`, `collect`, `skip`, custom handlers |
| [Adapters reference](adapters.md) | Per-format options, HTTP/LLM configuration |
| [API reference](api-reference.md) | Public exports and signatures |
| [Quickstart](quickstart.md) | Minimal copy-paste examples |
| [Benchmarks](benchmarks.md) | Throughput and memory measurement |

---

## End-to-end workflow

```
  ┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
  │ 1. Define schema│ ──► │ 2. Pick adapter  │ ──► │ 3. Stream rows  │
  │  (Pydantic v2)  │     │  CSV / JSONL / … │     │  (generator)    │
  └─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                            │
  ┌─────────────────┐     ┌──────────────────┐              ▼
  │ 5. Stats / logs │ ◄── │ 4. Handle results│ ◄── ValidationResult
  └─────────────────┘     │  valid / invalid │
                          └──────────────────┘
```

### Minimal example

```python
from pydantic import BaseModel, Field
from streamval import stream_csv

class Order(BaseModel):
    order_id: int
    customer: str
    amount: float = Field(ge=0)

for result in stream_csv("orders.csv", Order, on_error="collect"):
    if result.valid:
        process(result.data)          # typed Order instance
    else:
        log_bad_row(result.row_index, result.errors)
```

See [Schemas and models](schemas.md) for schema design and
[Running validation](validation.md) for every entry point and option.

---

## Installation

```bash
pip install streamval

# Recommended for production CSV/JSONL throughput:
pip install "streamval[fast]"    # polars + orjson

# HTTP NDJSON and LLM streaming:
pip install "streamval[http]"    # httpx

# Everything:
pip install "streamval[fast,http]"
```

**Requirements:** Python 3.11+, Pydantic v2. Parquet and Arrow need
`pyarrow` (included in the base install).

---

## When to use streamval

Use streamval when:

- The file is too large to fit comfortably in RAM.
- You want to start processing valid rows before the entire file is read.
- You need one schema applied consistently across CSV, JSONL, Parquet,
  or a streaming HTTP API.
- You are validating LLM token streams (OpenAI/Anthropic SSE) chunk by
  chunk.

Use plain Pydantic in a loop when the dataset is small and already in
memory — it will be faster, but memory grows with file size.

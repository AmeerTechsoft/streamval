# Quickstart

## 1. Install

```bash
pip install streamval
# faster JSON + lazy CSV via polars/orjson:
pip install "streamval[fast]"
```

## 2. Define a schema

Any Pydantic v2 ``BaseModel`` works as a row schema:

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    score: float
    active: bool
```

## 3. Validate a CSV file

```python
from streamval import stream_csv

for result in stream_csv("users.csv", User, on_error="collect"):
    if result.valid:
        user = result.data         # parsed User instance
    else:
        print(result.row_index, result.errors)
```

## 4. Validate a JSONL or Parquet file

```python
from streamval import stream_jsonl, stream_parquet

for r in stream_jsonl("events.jsonl", User):
    ...

for r in stream_parquet("events.parquet", User):
    ...
```

## 5. Pick an error strategy

* `fail_fast` — raise `StreamValidationError` on the first bad row.
* `collect` — emit every row; if `max_errors` is set and exceeded,
  raise on finalize.
* `skip` — drop invalid rows silently (logged at WARNING level).

```python
from streamval import StreamValidator

v = StreamValidator(User, on_error="skip", batch_size=2000)
for r in v.stream_csv("users.csv"):
    handle(r.data)
print(v.stats)
```

## 6. Inspect StreamStats

After the iterator is exhausted:

```python
s = v.stats
s.rows_total          # total rows seen
s.rows_valid          # rows that passed
s.rows_invalid        # rows that failed
s.error_rate          # invalid / total
s.errors_by_field     # {field_name: count}
s.throughput_rps      # rows per second
s.peak_memory_mb      # peak Python-object memory
s.duration_seconds    # wall-clock duration
```

## 7. Write a custom error strategy

Subclass `StrategyHandler` and inject your own:

```python
from streamval.strategies.base import StrategyHandler

class QuarantineHandler(StrategyHandler):
    async def handle(self, result):
        if not result.valid:
            archive(result)
            return None
        return result
    async def finalize(self):
        ...
    @property
    def summary(self):
        return {"strategy": "quarantine"}
```

See `examples/custom_strategy.py` for a runnable version.

## 8. Async usage

Every sync helper has an async counterpart:

```python
from streamval import astream_csv

async for r in astream_csv("users.csv", User):
    ...
```

The async path is required when validation is one stage of a larger
``asyncio`` pipeline; the sync path is roughly an order of magnitude
faster per row.

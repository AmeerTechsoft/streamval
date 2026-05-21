# Error strategies

When a row fails Pydantic validation, streamval does not crash by default.
An **error strategy** (also called a **strategy handler**) decides what
happens to that row: emit it, drop it, or stop the entire run.

Choose a strategy with the `on_error` parameter on `StreamValidator` or
any convenience function (`stream_csv`, `stream_http_ndjson`, etc.).

---

## Built-in strategies

| Strategy | Invalid rows | Valid rows | Raises |
|---|---|---|---|
| `"collect"` (default) | Emitted with `valid=False` | Emitted with `valid=True` | Only if `max_errors` exceeded |
| `"fail_fast"` | Never emitted | Emitted until first failure | `StreamValidationError` on first bad row |
| `"skip"` | Dropped (not yielded) | Emitted | Never |

All three strategies still update `validator.stats` for every row seen,
including skipped and collected invalid rows.

---

## collect (default)

**Best for:** development, data-quality reports, ETL jobs where you
need a full picture of what went wrong.

Every row — valid or invalid — is yielded from the iterator. You filter
or branch on `result.valid` in your loop.

```python
from streamval import stream_csv

invalid = []
for result in stream_csv("orders.csv", Order, on_error="collect"):
    if result.valid:
        load(result.data)
    else:
        invalid.append(result)

print(f"{len(invalid)} rows failed validation")
```

### max_errors cap

Pass `max_errors=N` to abort when more than `N` invalid rows accumulate.
The exception is raised in `finalize()` — after the stream is fully read
— not on the `(N+1)`th row mid-iteration.

```python
from streamval import StreamValidationError, stream_csv

try:
    list(stream_csv("orders.csv", Order, on_error="collect", max_errors=50))
except StreamValidationError as exc:
    print(exc.message)
    print(f"Collected {len(exc.results)} invalid rows")
```

Use this when a small error rate is acceptable but a catastrophically
bad file should halt downstream processing.

---

## fail_fast

**Best for:** strict pipelines, CI checks, schema contracts where any
invalid row means the whole batch is rejected.

Stops immediately on the first invalid row. Only rows validated before
the failure are yielded.

```python
from streamval import StreamValidationError, stream_csv

try:
    rows = [
        r.data
        for r in stream_csv("orders.csv", Order, on_error="fail_fast")
    ]
except StreamValidationError as exc:
    bad = exc.results[0]
    print(f"First failure at row {bad.row_index}: {bad.errors}")
    raise
```

The exception carries:

- `exc.message` — e.g. `"Row 42 failed validation"`
- `exc.results` — list containing the failing `ValidationResult`
- `exc.stats` — optional stats snapshot (may be `None`)

---

## skip

**Best for:** production ingestion where bad rows should not block good
ones — log files, third-party exports, noisy API streams.

Invalid rows are **not** yielded. Only valid rows appear in the
iterator. Each skipped row is logged at **WARNING** level under the
`streamval` logger.

```python
import logging
logging.basicConfig(level=logging.WARNING)

from streamval import StreamValidator

v = StreamValidator(Order, on_error="skip")
for result in v.stream_csv("daily_export.csv"):
    warehouse.insert(result.data)

print(f"Skipped {v.stats.rows_invalid} bad rows out of {v.stats.rows_total}")
```

Example log line:

```
WARNING streamval Row 903 skipped: 2 field errors
```

To silence skip warnings:

```python
import logging
logging.getLogger("streamval").setLevel(logging.ERROR)
```

See [Results, errors, and logging](results-and-errors.md#logging) for
full logging setup.

---

## Choosing a strategy

```
                    ┌─────────────────────────────────────┐
                    │     How bad is one invalid row?     │
                    └─────────────────┬───────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
              ▼                       ▼                       ▼
        Must abort run          Need full report          Drop and continue
              │                       │                       │
              ▼                       ▼                       ▼
         fail_fast                  collect                    skip
                              (+ max_errors cap)
```

| Scenario | Recommended strategy |
|---|---|
| CI schema check on a golden file | `fail_fast` |
| Exploratory data profiling | `collect` |
| Warehouse load with quarantine elsewhere | `skip` |
| API stream with occasional malformed events | `skip` or `collect` |
| Contract test: zero tolerance | `fail_fast` |
| Allow ≤100 bad rows per file | `collect`, `max_errors=100` |

---

## Custom strategies

Subclass `StrategyHandler` from `streamval.strategies.base` when the
built-in three are not enough — for example, writing bad rows to a
sidecar file, sending alerts, or routing rows to different sinks.

### The StrategyHandler protocol

```python
from streamval.strategies.base import StrategyHandler
from streamval.core.result import ValidationResult

class QuarantineHandler(StrategyHandler):
    async def handle(
        self, result: ValidationResult
    ) -> ValidationResult | None:
        if result.valid:
            return result          # emit valid rows
        self.quarantine(result)    # side effect
        return None                # drop invalid rows

    async def finalize(self) -> None:
        self.flush()

    @property
    def summary(self) -> dict:
        return {"strategy": "quarantine", "dropped": self._count}
```

| Method | When called | Return value |
|---|---|---|
| `handle(result)` | After each row is validated | Result to yield, or `None` to drop |
| `finalize()` | After the stream ends | — |
| `summary` | Any time (diagnostics) | Dict with strategy metadata |

### Injecting a custom handler

Create a `StreamValidator`, then replace its handler **before** calling
`stream_*`:

```python
from streamval import StreamValidator

v = StreamValidator(Order, on_error="collect")  # placeholder strategy
v._handler = QuarantineHandler(sidecar_path)

for result in v.stream_csv("orders.csv"):
    process(result.data)
```

The `on_error` argument only sets the initial handler; assigning
`_handler` overrides it. See `examples/custom_strategy.py` for a
complete runnable example.

### Sync vs async handlers

Built-in handlers never perform real `await` calls — they work in both
sync (`stream_csv`) and async (`astream_csv`) paths.

If your custom `handle` or `finalize` **does** await I/O (e.g. writing
to an async database), use the **async** stream methods
(`astream_csv`, `astream_http_ndjson`, …). The sync path drives handler
coroutines without a full event loop and will raise `RuntimeError` if
your handler actually suspends.

---

## Strategy vs transport errors

Error strategies only apply to **row-level Pydantic validation**
failures (`ValidationResult` with `valid=False`).

They do **not** catch:

| Error | Source | Handling |
|---|---|---|
| `StreamFetchError` | HTTP adapter (network, 4xx, bad JSON line) | Raised immediately; not maskable |
| File not found | OS / adapter | Standard Python exception |
| Malformed JSONL line | JSONL adapter | Raised during iteration |

Wrap HTTP calls in `try/except StreamFetchError` separately from
validation strategy logic.

---

## Inspecting handler state

After a run, read the active handler:

```python
v = StreamValidator(Order, on_error="collect")
list(v.stream_csv("data.csv"))

print(v.handler.summary)
# {'strategy': 'collect', 'max_errors': None, 'invalid_count': 12}
```

For `CollectHandler`, access accumulated invalid results:

```python
from streamval.strategies.collect import CollectHandler

v = StreamValidator(Order, on_error="collect")
list(v.stream_csv("data.csv"))

if isinstance(v.handler, CollectHandler):
    bad = v.handler.invalid_results
```

---

## Next steps

- [Results, errors, and logging](results-and-errors.md) — `ValidationResult`,
  exceptions, stats, log configuration
- [Running validation](validation.md) — all entry points and options

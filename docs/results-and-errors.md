# Results, errors, and logging

Every validated row produces a **`ValidationResult`**. Transport and
strategy failures raise separate exception types. streamval uses Python's
standard **`logging`** module under the logger name `"streamval"`.

---

## ValidationResult

The primary output of every stream. One instance per row (when the error
strategy yields it).

```python
from streamval import ValidationResult

@dataclass(frozen=True)
class ValidationResult:
    row_index: int              # 0-based position in the stream
    raw: dict[str, Any]         # original row dict (before coercion)
    valid: bool                 # True if Pydantic accepted the row
    data: BaseModel | None      # parsed model when valid; None otherwise
    errors: list[FieldError]    # empty when valid
```

### Reading a result

```python
for result in stream_csv("data.csv", Order, on_error="collect"):
    if result.valid:
        order: Order = result.data  # type: ignore[assignment]
        ship(order)
    else:
        print(f"Row {result.row_index} failed:")
        for err in result.errors:
            print(f"  {err.field}: {err.message} (got {err.value!r})")
        print(f"  raw row: {result.raw}")
```

### FieldError

One field-level failure from Pydantic:

| Attribute | Example | Description |
|---|---|---|
| `field` | `"amount"` or `"address.zip"` | Dotted field path |
| `value` | `"not-a-number"` | Raw input that failed |
| `message` | `"Input should be a valid number"` | Human-readable |
| `error_type` | `"float_parsing"` | Pydantic error type string |

```python
for err in result.errors:
    print(err)  # amount='not-a-number': Input should be... [float_parsing]
```

---

## StreamStats

After the iterator is exhausted, read run summary from the validator:

```python
v = StreamValidator(Order, on_error="skip")
for r in v.stream_csv("big.csv"):
    process(r.data)

stats = v.stats
print(stats.rows_total)        # 1_000_000
print(stats.rows_valid)        # 998_432
print(stats.rows_invalid)      # 1_568
print(stats.error_rate)        # 0.001568
print(stats.errors_by_field)   # {"amount": 890, "order_id": 678, ...}
print(stats.throughput_rps)    # 14203.5
print(stats.peak_memory_mb)    # 0.47
print(stats.duration_seconds)  # 70.4
print(stats)                   # one-line summary string
```

`errors_by_field` counts how many times each field name appeared in an
error (useful for data-quality dashboards).

---

## Exception types

streamval has **two layers** of errors:

1. **Row-level validation** — handled by error strategies; usually
   surfaced as `ValidationResult` with `valid=False`.
2. **Transport / fatal** — raised immediately; cannot be masked by a
   strategy.

### StreamValidationError

Raised when an error **strategy** decides the run cannot continue.

```python
from streamval import StreamValidationError

try:
    for r in stream_csv("data.csv", Order, on_error="fail_fast"):
        process(r.data)
except StreamValidationError as exc:
    print(exc.message)           # "Row 42 failed validation"
    print(len(exc.results))      # 1 (the failing row)
    for r in exc.results:
        print(r.errors)
    if exc.stats:
        print(exc.stats.rows_total)
```

**When it is raised:**

| Strategy | Condition |
|---|---|
| `fail_fast` | First invalid row |
| `collect` | `max_errors` exceeded at end of stream |

Attributes: `message`, `results` (list of invalid `ValidationResult`),
`stats` (optional `StreamStats` snapshot).

### StreamFetchError

Raised by the **HTTP NDJSON adapter** for network and format failures.
Never retried for JSON parse errors mid-stream.

```python
from streamval import StreamFetchError

try:
    for r in stream_http_ndjson(url, Model, max_retries=3):
        handle(r)
except StreamFetchError as exc:
    print(exc.url)            # the URL that failed
    print(exc.status_code)    # 503, 403, None for timeout, ...
    print(exc.attempt_count)  # how many tries were made
    print(exc)                # formatted summary string
```

**Immediate (no retry):** HTTP 401, 403, 404, other 4xx, malformed JSON.

**Retried then raised:** connect errors, timeouts, HTTP 429, 5xx.

Row-level Pydantic failures on HTTP streams still flow through
`ValidationResult` — they are not `StreamFetchError`.

---

## Error handling patterns

### Collect everything, report at end

```python
results = list(stream_csv("data.csv", Order, on_error="collect"))
bad = [r for r in results if not r.valid]
if bad:
    write_report(bad)
```

### Fail fast in strict pipelines

```python
try:
    rows = [r.data for r in stream_csv("data.csv", Order, on_error="fail_fast")]
except StreamValidationError as e:
    abort_pipeline(e.results[0])
```

### Skip bad rows in production ETL

```python
v = StreamValidator(Order, on_error="skip")
for r in v.stream_csv("daily_export.csv"):
    load_warehouse(r.data)
print(f"Skipped {v.stats.rows_invalid} bad rows")
```

### Cap tolerance with max_errors

```python
# Allow up to 100 bad rows; raise if more
stream_csv("data.csv", Order, on_error="collect", max_errors=100)
```

---

## Logging

streamval logs under the standard library logger **`streamval`**.

### What gets logged

| Level | Source | Message |
|---|---|---|
| `WARNING` | `skip` strategy | `"Row N skipped: M field errors"` for each dropped row |
| `DEBUG` | CSV adapter | Fallback path notices (e.g. polars unavailable) |

Validation failures in `collect` and `fail_fast` modes are **not**
logged automatically — you handle them via `ValidationResult` or
exceptions.

### Configuring logging

streamval does not configure handlers for you. Set up logging in your
application entry point:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

# Show skip warnings (default WARNING level)
logging.getLogger("streamval").setLevel(logging.WARNING)

# Show adapter debug messages
logging.getLogger("streamval").setLevel(logging.DEBUG)
```

### Example: skip strategy with visible warnings

```python
import logging
logging.basicConfig(level=logging.WARNING)

v = StreamValidator(Order, on_error="skip")
for r in v.stream_csv("noisy.csv"):
    save(r.data)

# Logs like:
# WARNING streamval Row 17 skipped: 2 field errors
# WARNING streamval Row 903 skipped: 1 field errors
```

### Example: structured logging integration

```python
import logging
log = logging.getLogger("myapp.ingest")

for result in stream_csv("data.csv", Order, on_error="collect"):
    if not result.valid:
        log.warning(
            "validation_failed",
            extra={
                "row_index": result.row_index,
                "fields": [e.field for e in result.errors],
                "error_types": [e.error_type for e in result.errors],
            },
        )
    else:
        log.info("row_ok", extra={"row_index": result.row_index})
```

### Silencing streamval logs

```python
logging.getLogger("streamval").setLevel(logging.ERROR)
```

---

## Quick reference

| I want to… | Use |
|---|---|
| Get typed model for valid row | `result.data` |
| See why a row failed | `result.errors` |
| See original input | `result.raw` |
| Stop on first bad row | `on_error="fail_fast"` → catch `StreamValidationError` |
| Get all bad rows | `on_error="collect"`, filter `not r.valid` |
| Drop bad rows silently | `on_error="skip"`, check `v.stats.rows_invalid` |
| HTTP connection failed | catch `StreamFetchError` |
| Run summary metrics | `validator.stats` after iteration |
| Log skipped rows | `on_error="skip"` + `logging.getLogger("streamval")` |

---

## Next steps

- [Error strategies](error-strategies.md) — deep dive on each strategy
  and custom handlers
- [Schemas and models](schemas.md) — design models that fail clearly

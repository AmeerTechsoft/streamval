# API reference

Complete reference for the public `streamval` package surface. Internal
modules (`streamval.adapters.*`, `streamval.core.*`, etc.) are not
stable API — import from `streamval` or documented submodules only.

**Version:** check `streamval.__version__` (currently `0.2.2`).

---

## Package exports (`import streamval`)

| Name | Kind | Description |
|---|---|---|
| `StreamValidator` | class | Main validation engine |
| `ValidationResult` | dataclass | Per-row outcome |
| `FieldError` | dataclass | Single field failure |
| `StreamStats` | dataclass | Run summary metrics |
| `StreamValidationError` | exception | Strategy abort |
| `StreamFetchError` | exception | HTTP transport failure |
| `HttpNdjsonConfig` | dataclass | HTTP adapter configuration |
| `ErrorStrategy` | type alias | `"fail_fast" \| "collect" \| "skip"` |
| `stream_csv` | function | Sync CSV convenience |
| `stream_jsonl` | function | Sync JSONL convenience |
| `stream_parquet` | function | Sync Parquet convenience |
| `stream_http_ndjson` | function | Sync HTTP NDJSON convenience |
| `astream_csv` | async function | Async CSV convenience |
| `astream_jsonl` | async function | Async JSONL convenience |
| `astream_parquet` | async function | Async Parquet convenience |
| `astream_http_ndjson` | async function | Async HTTP NDJSON convenience |
| `llm` | module | LLM streaming helpers |
| `__version__` | str | Package version |

---

## StreamValidator

```python
StreamValidator(
    schema: type[BaseModel],
    on_error: ErrorStrategy = "collect",
    batch_size: int = 1000,
    max_errors: int | None = None,
    workers: int = 1,
    use_arrow: bool = True,
)
```

### Properties

| Property | Type | Description |
|---|---|---|
| `stats` | `StreamStats` | Snapshot after stream completes |
| `handler` | `StrategyHandler` | Active error strategy instance |
| `use_arrow` | `bool` | Whether Arrow batch path is enabled |

### Stream methods (sync)

| Method | Source | Notes |
|---|---|---|
| `stream_csv(path, **adapter_kwargs)` | CSV file | Arrow path when `use_arrow=True` |
| `stream_jsonl(path, **adapter_kwargs)` | JSONL file | Row dict path |
| `stream_parquet(path, **adapter_kwargs)` | Parquet file | Arrow path when `use_arrow=True` |
| `stream_arrow(path, **adapter_kwargs)` | Arrow IPC / Feather | Row dict path |
| `stream_http_ndjson(url_or_config, **config_kwargs)` | HTTP stream | Requires `[http]` extra |

All sync methods return `Iterator[ValidationResult]`.

### Stream methods (async)

| Method | Returns |
|---|---|
| `astream_csv(path, **adapter_kwargs)` | `AsyncIterator[ValidationResult]` |
| `astream_jsonl(path, **adapter_kwargs)` | `AsyncIterator[ValidationResult]` |
| `astream_parquet(path, **adapter_kwargs)` | `AsyncIterator[ValidationResult]` |
| `astream_arrow(path, **adapter_kwargs)` | `AsyncIterator[ValidationResult]` |
| `astream_http_ndjson(url_or_config, **config_kwargs)` | `AsyncIterator[ValidationResult]` |

There are **no** module-level `stream_arrow` / `astream_arrow` helpers —
use `StreamValidator(...).stream_arrow(...)`.

---

## Convenience functions

Each function constructs a fresh `StreamValidator` and delegates to the
matching `stream_*` / `astream_*` method.

```python
def stream_csv(
    path: str | Path,
    schema: type[BaseModel],
    **kwargs,
) -> Iterator[ValidationResult]: ...

def stream_http_ndjson(
    url_or_config: str | HttpNdjsonConfig,
    schema: type[BaseModel],
    **kwargs,
) -> Iterator[ValidationResult]: ...
```

Async variants: `astream_csv`, `astream_jsonl`, `astream_parquet`,
`astream_http_ndjson`.

### Keyword splitting

`**kwargs` are split automatically:

**Validator kwargs** (passed to `StreamValidator`):

- `on_error`
- `batch_size`
- `max_errors`
- `workers`
- `use_arrow`

**Everything else** goes to the adapter (e.g. `encoding`, `delimiter`,
`auth_token`, `timeout_seconds`).

---

## ValidationResult

```python
@dataclass(frozen=True)
class ValidationResult:
    row_index: int
    raw: dict[str, Any]
    valid: bool
    data: BaseModel | None
    errors: list[FieldError]
```

| Field | When valid | When invalid |
|---|---|---|
| `data` | Parsed Pydantic model | `None` |
| `errors` | `[]` | List of `FieldError` |
| `raw` | Original row dict | Original row dict |

Class methods (internal use, but public): `success(...)`,
`from_pydantic_error(...)`.

---

## FieldError

```python
@dataclass(frozen=True)
class FieldError:
    field: str
    value: Any
    message: str
    error_type: str
```

---

## StreamStats

```python
@dataclass(frozen=True)
class StreamStats:
    rows_total: int
    rows_valid: int
    rows_invalid: int
    errors_by_field: dict[str, int]
    throughput_rps: float
    peak_memory_mb: float
    duration_seconds: float

    @property
    def error_rate(self) -> float: ...
```

Read from `validator.stats` after exhausting the iterator.

---

## StreamValidationError

```python
class StreamValidationError(Exception):
    message: str
    results: list[ValidationResult]
    stats: StreamStats | None
```

Raised by `fail_fast` (first invalid row) and `collect` when
`max_errors` is exceeded at finalize.

---

## StreamFetchError

```python
class StreamFetchError(Exception):
    message: str
    url: str
    status_code: int | None
    attempt_count: int
```

Raised by the HTTP NDJSON adapter only. Not affected by error strategies.

---

## HttpNdjsonConfig

```python
@dataclass(frozen=True)
class HttpNdjsonConfig:
    url: str
    headers: dict[str, str]
    params: dict[str, Any]
    timeout_seconds: float = 30.0
    connect_timeout_seconds: float = 10.0
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    follow_redirects: bool = True
    auth_token: str | None = None
    event_stream: bool = False
    line_filter: str | None = None
    skip_empty_lines: bool = True
    max_lines: int | None = None
```

Factory:

```python
HttpNdjsonConfig.from_url(url: str, **kwargs) -> HttpNdjsonConfig
```

See [Adapters reference](adapters.md) for behavioural details.

---

## streamval.llm

Sub-module for LLM streaming. Requires `streamval[http]`.

### LLMProvider

```python
class LLMProvider(StrEnum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GENERIC_SSE = "generic_sse"
    GENERIC_NDJSON = "generic_ndjson"
```

### Functions

```python
def validate_llm_stream(
    url: str,
    schema: type[BaseModel],
    provider: LLMProvider = LLMProvider.GENERIC_NDJSON,
    auth_token: str | None = None,
    on_error: ErrorStrategy = "collect",
    **config_kwargs,
) -> Iterator[ValidationResult]: ...

async def avalidate_llm_stream(...) -> AsyncIterator[ValidationResult]: ...

def extract_content(
    result: ValidationResult,
    provider: LLMProvider,
) -> str | None: ...
```

`extract_content` returns text for OpenAI and Anthropic chunks; returns
`None` for generic providers (read `result.data` / `result.raw`
directly).

---

## Strategy extension API

Import from `streamval.strategies` (not re-exported at package root):

```python
from streamval.strategies import (
    StrategyHandler,
    ErrorStrategy,
    FailFastHandler,
    CollectHandler,
    SkipHandler,
    build_handler,
)
```

| Symbol | Description |
|---|---|
| `StrategyHandler` | ABC for custom strategies |
| `build_handler(strategy, max_errors=None)` | Factory for built-in handlers |
| `FailFastHandler` | `fail_fast` implementation |
| `CollectHandler` | `collect` implementation; `.invalid_results` property |
| `SkipHandler` | `skip` implementation |

---

## Logging

| Logger name | Levels used |
|---|---|
| `"streamval"` | `WARNING` (skip strategy), `DEBUG` (adapter fallbacks) |

streamval does not install handlers. Configure in your application:

```python
import logging
logging.getLogger("streamval").setLevel(logging.WARNING)
```

---

## Optional dependencies

| Extra | Packages | Enables |
|---|---|---|
| `[fast]` | polars, orjson | Faster CSV + JSONL |
| `[http]` | httpx | HTTP NDJSON, LLM streaming |
| base install | pyarrow | Parquet, Arrow, CSV Arrow path |

---

## Related guides

- [Schemas and models](schemas.md)
- [Running validation](validation.md)
- [Results, errors, and logging](results-and-errors.md)
- [Error strategies](error-strategies.md)
- [Adapters reference](adapters.md)

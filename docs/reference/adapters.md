# Adapters

`streamval` ships five adapters out of the box. Each one is an async
generator that yields one row dict at a time (or one
`pyarrow.RecordBatch` on the fast path) without ever loading the full
payload into memory.

| Format  | Source        | Requires                                       |
|---------|---------------|------------------------------------------------|
| CSV     | file / path   | (none, or `streamval[fast]` for polars path)   |
| JSONL   | file / path   | (none, or `streamval[fast]` for orjson)        |
| Parquet | file / path   | `pyarrow` (always-on dependency)               |
| Arrow   | file / path   | `pyarrow` (always-on dependency)               |
| NDJSON  | HTTP URL      | `streamval[http]` (httpx)                      |
| SSE/LLM | HTTP URL      | `streamval[http]` (httpx)                      |

## CSV / JSONL / Parquet / Arrow

See [Quickstart](../getting-started/quickstart.md) for the basic file-adapter usage. The notes here
focus on the HTTP NDJSON adapter and the LLM helpers.

## HTTP NDJSON adapter

### When to use it

* REST APIs that stream large NDJSON result sets (log services,
  analytics exports, webhook replays).
* Server-Sent Events feeds (OpenAI / Anthropic / generic SSE).
* Any endpoint where the response body is "one JSON object per line"
  and may be megabytes / hours long.

Use a file adapter when the data is already on disk; HTTP adds
network latency and the small overhead of an httpx client.

### Install

```bash
pip install "streamval[http]"
```

### Quickstart

```python
from pydantic import BaseModel
from streamval import stream_http_ndjson

class Event(BaseModel):
    id: int
    name: str

for result in stream_http_ndjson(
    "https://example.com/events",
    Event,
    on_error="collect",
):
    if result.valid:
        handle(result.data)
```

### `HttpNdjsonConfig` reference

Every field is optional except `url`.

| Field                       | Default | Description                                                                                  |
|-----------------------------|---------|----------------------------------------------------------------------------------------------|
| `url`                       | —       | HTTP / HTTPS URL. Required, validated to a known scheme.                                     |
| `headers`                   | `{}`    | Extra request headers, merged into the client.                                               |
| `params`                    | `{}`    | Query-string parameters.                                                                     |
| `timeout_seconds`           | `30.0`  | Per-request total timeout (read + write + pool).                                             |
| `connect_timeout_seconds`   | `10.0`  | Connection-establishment timeout.                                                            |
| `max_retries`               | `3`     | Maximum retry attempts on transport / 5xx / 408 / 425 / 429.                                 |
| `retry_backoff_seconds`     | `1.0`   | Linear backoff factor; the Nth retry sleeps `backoff * N`.                                   |
| `follow_redirects`          | `True`  | Follow 3xx redirects.                                                                        |
| `auth_token`                | `None`  | Bearer token; sent as `Authorization: Bearer <token>`.                                       |
| `event_stream`              | `False` | Parse SSE: drop non-`data:` lines, strip `data: ` prefix, terminate on the `[DONE]` sentinel.|
| `line_filter`               | `None`  | Literal prefix; non-matching lines are skipped and the prefix is stripped.                   |
| `skip_empty_lines`          | `True`  | Drop blank lines silently.                                                                   |
| `max_lines`                 | `None`  | Stop after this many parsed lines.                                                           |

Use the convenience constructor when only the URL is mandatory:

```python
from streamval import HttpNdjsonConfig

cfg = HttpNdjsonConfig.from_url(
    "https://example.com/events",
    auth_token="sk-...",
    max_lines=500,
)
```

### SSE format parsing

Server-Sent Events look like this on the wire:

```
event: message
id: 1
data: {"chunk": 1}

data: {"chunk": 2}

data: [DONE]

```

With `event_stream=True` `streamval` drops `event:`, `id:`, and
comment lines automatically, strips the `data: ` prefix, and treats
`data: [DONE]` as a clean end-of-stream sentinel:

```python
from streamval import stream_http_ndjson

for r in stream_http_ndjson(
    "https://example.com/stream",
    Event,
    event_stream=True,
):
    ...
```

### Authentication

* Bearer token: pass `auth_token="sk-..."`. Sets
  `Authorization: Bearer sk-...`.
* Anything else: pass `headers={"X-API-Key": "..."}` or
  `headers={"Authorization": "Basic ..."}`. Token-mode wins if both
  are set.

### Retry behaviour

Retries are linear: `retry_backoff_seconds * attempt_number` seconds
between attempts.

| Outcome                                   | Retried? |
|-------------------------------------------|----------|
| `httpx.ConnectError`                      | Yes      |
| `httpx.TimeoutException`                  | Yes      |
| `httpx.RemoteProtocolError`               | Yes      |
| HTTP `408`, `425`, `429`, `5xx`           | Yes      |
| HTTP `401`, `403`, `404`                  | **No** — `StreamFetchError` immediately |
| Other `4xx`                               | **No** — `StreamFetchError` immediately |
| JSON parse error / non-object payload     | **No** — `StreamFetchError` (data error)|

Retry exhaustion raises `StreamFetchError(url=..., status_code=...,
attempt_count=...)`. The exception is separate from
`StreamValidationError` so transport failures can never be masked by
an error strategy.

### Performance notes

Network latency dominates with HTTP streams, not Pydantic. Tune
`batch_size` accordingly:

* Interactive streams (LLM token chunks, sub-millisecond rows):
  `batch_size=100` keeps latency to first valid result low.
* Bulk APIs (log dumps, analytics): `batch_size=1000` or higher
  reduces validator overhead.
* `use_arrow` does not apply to NDJSON — the adapter is JSON-typed
  from the wire onwards.

## LLM streaming helpers (`streamval.llm`)

A thin convenience layer over `stream_http_ndjson` with pre-configured
defaults for the common LLM provider shapes.

```python
from streamval import llm
from pydantic import BaseModel

class Chunk(BaseModel):
    id: str | None = None
    choices: list[dict] | None = None
    delta: dict | None = None

for result in llm.validate_llm_stream(
    "https://api.openai.com/v1/chat/completions",
    Chunk,
    provider=llm.LLMProvider.OPENAI,
    auth_token="sk-...",
):
    text = llm.extract_content(result, llm.LLMProvider.OPENAI)
    if text:
        print(text, end="", flush=True)
```

### Providers

| `LLMProvider`           | `event_stream` | Default content path           | Notes                                                |
|-------------------------|----------------|--------------------------------|------------------------------------------------------|
| `OPENAI`                | `True`         | `choices[0].delta.content`     | Handles `[DONE]` sentinel automatically.             |
| `ANTHROPIC`             | `True`         | `delta.text`                   | Skips `{"type": "ping"}` events post-parse.          |
| `GENERIC_SSE`           | `True`         | (none)                         | Generic SSE wire format, no provider-specific magic. |
| `GENERIC_NDJSON`        | `False`        | (none)                         | Plain newline-delimited JSON.                        |

`extract_content(result, provider)` walks the provider's content path
against `result.raw` and returns the text fragment, or `None` when
the path isn't present (tool-use chunks, `message_start` frames,
etc.). For generic providers `extract_content` always returns
`None` — pull what you need directly from `result.data`.

### Bring your own SDK? No.

`streamval.llm` deliberately does not import `openai`, `anthropic`,
or any other provider SDK. It only speaks HTTP, SSE, and JSON. Bring
your own auth token and you're done.

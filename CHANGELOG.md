# Changelog

All notable changes to streamval will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2026-05-21

### Added
- **User documentation:** end-to-end guides covering schema definition,
  running validation (all formats, sync/async), results and exceptions,
  error strategies, logging, and a full API reference. See
  [`docs/index.md`](https://github.com/AmeerTechsoft/streamval/blob/main/docs/index.md).

## [0.2.1] - 2026-05-21

### Fixed
- **Row-mode memory regression (v0.2.0):** `StreamValidator.batch_size`
  was not forwarded to the CSV polars adapter, so polars always chunked
  at its default of 10,000 rows. Peak memory on 1M rows jumped from
  ~0.41 MB to ~4.43 MB. Row mode at the default `batch_size=1000` is
  back to ~0.47 MB on CI.
- **CSV Arrow path type inference:** polars no longer infers column types
  during CSV scan (`infer_schema_length=0`). A single non-integer cell in
  an otherwise integer column no longer aborts the whole file with
  `polars.ComputeError`; it surfaces as a normal Pydantic validation
  error per row.
- **Arrow CSV batch_size:** validator `batch_size` is now forwarded to
  the CSV Arrow adapter the same way as row mode.

### Changed
- Migrated CSV polars paths from deprecated `read_csv_batched` to
  `scan_csv().collect_batches()`, removing deprecation warnings on
  polars 1.40+.

## [0.2.0] - 2026-05-21

### Added
- **Arrow batch fast path** for CSV (`streamval[fast]`, polars) and
  Parquet adapters: validate `pyarrow.RecordBatch` objects directly
  with no per-row Python dict construction.
- `use_arrow: bool = True` parameter on `StreamValidator` (default
  on; pass `use_arrow=False` to keep the v0.1 row-mode pipeline).
- `CompiledValidationPlan.validate_batch(rows)` and
  `validate_record_batch(batch)` for one Python ↔ Rust boundary
  crossing per batch via `TypeAdapter(list[Model])`. Per-row
  fallback on mixed-validity batches.
- **HTTP NDJSON adapter** (`streamval[http]`, httpx): stream + validate
  REST endpoints that emit one JSON object per line. Bounded memory,
  linear-backoff retries, configurable timeouts, Bearer auth.
- **Server-Sent Events parsing** via `event_stream=True`: strips
  `data: ` prefix, ignores `event:` / `id:` / comment lines, exits
  cleanly on the `[DONE]` sentinel.
- **LLM streaming helpers** in `streamval.llm`: pre-configured
  presets for `OPENAI`, `ANTHROPIC`, `GENERIC_SSE`, `GENERIC_NDJSON`
  via the `LLMProvider` enum. Anthropic preset skips `{"type":
  "ping"}` events. `extract_content(result, provider)` pulls the
  text fragment out of each chunk.
- `HttpNdjsonConfig` (frozen dataclass) with every field validated:
  url scheme, positive timeouts, non-negative retries / backoff,
  positive `max_lines`.
- `StreamFetchError` for transport-level failures (connect, timeout,
  retry exhaustion, 4xx, malformed JSON). Kept separate from
  `StreamValidationError` so transport failures cannot be masked by
  an error strategy.
- New public exports: `HttpNdjsonConfig`, `StreamFetchError`,
  `stream_http_ndjson`, `astream_http_ndjson`, and the `llm`
  namespace.
- `examples/llm_streaming.py` and `examples/http_ndjson_basic.py`:
  fully offline demos against a local `ThreadingHTTPServer`.

### Changed
- CSV adapter routes through polars `scan_csv` when `polars` is
  installed (~3× faster row-mode throughput than the aiofiles
  fallback). Fallback path preserved when polars is absent.
- Parquet adapter yields `RecordBatch` directly via
  `pyarrow.iter_batches()`; no per-row dict materialisation.
- `SourceFormat.PARQUET` / `SourceFormat.ARROW` skip coercion
  entirely; values arrive natively typed via pyarrow.
- New `SourceFormat.HTTP_NDJSON` reusing the JSONL coercion path.
- Docs: `docs/adapters.md` rewritten from the placeholder, new
  HTTP / LLM sections in `docs/quickstart.md`, README updated with
  HTTP NDJSON / SSE / LLM streaming.

### Performance
- CSV row throughput: ~11k rps → ~14k rps (polars dict path).
- CSV batch throughput: new path, ~17k rps on Linux GitHub-hosted
  runners (spec target 35k+ on dedicated hardware).
- Parquet batch throughput: new path, ~29k rps on the same runners
  (spec target 45k+ on dedicated hardware).
- Aspirational floor assertions (CSV 35k rps, Parquet 45k rps) are
  gated behind `STREAMVAL_PERF=1` so default CI runs never fail on
  slower hardware. Numeric floors are overridable via
  `STREAMVAL_MIN_CSV_BATCH_RPS` / `STREAMVAL_MIN_PARQUET_BATCH_RPS`.
- Memory contract unchanged: < 5 MB peak Python-object memory on
  1M rows in batch mode on Windows; ~25 MB on Linux due to a
  constant pyarrow allocator block (still well under the 50 MB
  budget).

### Internal
- `coerce_row` caches per-model field target types in a
  `WeakKeyDictionary` and short-circuits PARQUET / ARROW entirely.
- New `RecordBatchPipeline` in `core/buffer.py` drives the Arrow
  fast path with optional thread-pool dispatch that preserves
  input row order.

## [0.1.0] - 2026-05-21

### Added
- Initial alpha release.
- StreamValidator with pluggable error strategies (fail_fast, collect, skip).
- Async generator adapters for CSV, JSONL, Parquet, and Arrow.
- Compiled, cached Pydantic validation plans.
- StreamStats with throughput, peak-memory, and per-field error counters.

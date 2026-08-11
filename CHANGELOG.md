# Changelog

All notable changes to streamval will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Performance
- **`tracemalloc` is no longer enabled by default — ~4.7× throughput.**
  `StatsAccumulator.start()` unconditionally started `tracemalloc` on
  every run, and it hooks every allocation. Parquet batch mode went from
  ~22 000 rps to ~118 000 rps; CSV batch from ~14 000 to ~71 000 rps on
  the same machine. Memory tracking is now opt-in via the new
  `track_memory=True` parameter on `StreamValidator`; when it is off,
  `stats.peak_memory_mb` reports `0.0`. If `tracemalloc` is already
  tracing (a test or profiler started it), peak memory is still reported
  at no extra cost.
- **`ValidationResult` is now a slotted dataclass** and `success()`
  bypasses the generated `__init__`, making the per-row hot-path
  allocation ~1.6× cheaper and slightly reducing peak memory. The class
  stays `frozen`; immutability and equality are unchanged.
- **No coroutine allocation per row.** `StrategyHandler` gained a
  `handle_sync()` entry point and a `sync_safe` class flag. All three
  built-in strategies implement it synchronously, so the hot loop no
  longer allocates a coroutine and raises `StopIteration` for every row
  on either the sync or async path. Custom handlers are unaffected — the
  base-class default still drives `handle()` as before.
- `StatsAccumulator.record_many()` records a whole batch in one call,
  short-cutting the all-valid case to two integer adds.
- **Vectorised CSV coercion — ~1.3-1.4× on CSV batch mode.** CSV columns
  arrive as strings; the batch path now casts them column-wise in Arrow
  (`cast_csv_batch`) instead of converting every cell in Python.
  Each column is cast independently and safely: if any cell fails to
  parse, that column alone falls back to per-row `_coerce_str`, so a
  single bad cell degrades one column rather than failing the file.
  Arrow is strictly stricter than `_coerce_str` — it rejects padded
  values, `int`-via-`float` strings and non-canonical bool spellings —
  and never accepts a value while disagreeing about it, so results are
  unchanged. CSV batch mode went from ~67 000 to ~88 000 rps.
- `coerce_row` copies the row dict lazily, so rows needing no coercion
  (every Parquet/Arrow row, and every already-cast CSV row) no longer
  allocate a copy.

### Changed
- `StreamValidator` accepts `track_memory: bool = False`.
- Raised the `STREAMVAL_PERF=1` regression floors from 35 000 / 45 000
  to 50 000 / 60 000 rps (CSV / Parquet batch). The old values were
  calibrated against `tracemalloc`-bound measurements and were too low
  to catch anything short of a total collapse.

### Fixed
- Documentation claimed polars gives "~3× faster row-mode throughput"
  for CSV. On the sync path it measures roughly on par with the stdlib
  `csv` reader; the claim has been removed.

### Documentation
- Corrected the README performance and `batch_size` memory tables, which
  reported figures measured with `tracemalloc` active and understated
  peak memory at large batch sizes by ~6×.

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

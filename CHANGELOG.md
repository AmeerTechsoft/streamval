# Changelog

All notable changes to streamval will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-08-12

A performance release. Same API, same validation results, 4-7× the
throughput — plus the removal of a default that was quietly costing
every user most of their speed.

### Upgrading

One behaviour change to be aware of:

- **`stats.peak_memory_mb` now reads `0.0` by default.** It is populated
  by `tracemalloc`, which hooks every allocation and costs roughly 4-5×
  throughput, so it is now opt-in. If you read this field, construct the
  validator with `track_memory=True`:

      v = StreamValidator(Order, track_memory=True)   # profiling only

  Leave it off in production. Every other field on `StreamStats` is
  unaffected.

Nothing else changes: `ValidationResult` is still frozen, error
strategies behave identically, and validation results are unchanged for
every input. The batch-splitting and column-casting work below are
optimisations only — each is covered by tests asserting the fast path
produces exactly what per-row validation produces.

### Performance
- **Variant CSV formatting stays on the vectorised path — up to 1.9×.**
  A plain Arrow cast is stricter than the per-row `_coerce_str` in three
  ways that real exports hit constantly, and each one used to drop a
  whole column onto the Python path: padded cells (`" 42 "`), worded
  booleans (`yes`/`no`/`y`/`n`/`t`/`f`), and integers written as floats
  (`"42.0"`). All three are now handled column-wise, against the same
  `_TRUTHY`/`_FALSY` sets the per-row path uses.

  Throughput relative to a canonically-formatted file of the same size
  (a ratio, so it is not distorted by machine-to-machine drift):

      file formatting        before    after
      canonical               1.00x    1.00x
      padded cells            0.56x    1.01x
      worded bools            0.84x    1.06x
      int-as-float            0.68x    1.02x
      all three combined      0.51x    0.97x

  Formatting variance now costs essentially nothing, where it used to
  cost up to half of throughput.

  Which strategy a column needs is discovered on the first batch and
  cached on the plan, because neither fixed order suits both cases:
  trimming every column unconditionally costs a canonical file ~7%,
  while attempting the plain cast first wastes a pass over every batch
  of a padded one. The hint is only ever an optimisation — if it fails
  the remaining strategies are still tried, so a file that changes shape
  mid-stream stays correct.

  The safety rule is unchanged: a column is cast only when the result
  provably matches per-row coercion, otherwise it declines. `"42.7"`
  deliberately declines rather than truncating, because the per-row path
  yields `42` and a decline is always safe where a disagreement is not.
- **A failed batch no longer re-validates every row — up to 1.7× on
  files with invalid rows.** `validate_batch` tried the bulk validator
  and, on any failure, fell back to validating the whole batch one row
  at a time. Throughput therefore depended on whether a batch contained
  *any* bad row rather than on how many: at `batch_size=10000`, 20 bad
  rows in 200 000 (0.01%) was enough to put every batch on the slow path
  and roughly halve throughput. The failing rows are now identified from
  the `ValidationError` — a list adapter reports the element index in
  `loc` — so only those rows are validated individually while the rest
  go back through the bulk validator in one call.

      bad rows / 200k       before    after
      2      (0.001%)        0.73x    0.93x
      20     (0.01%)         0.51x    0.76x
      200    (0.1%)          0.40x    0.67x
      2 000  (1%)            0.41x    0.67x
      20 000 (10%)           0.44x    0.58x

  This is an optimisation only: the result is always identical to
  validating row by row, and any batch whose failing rows cannot be
  localised with confidence falls back to exactly that. A differential
  test suite (104 cases, randomised dirty data across type failures,
  constraint failures, optional fields and every density of bad rows
  from 0% to 100%) checks the two against each other.
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
  Arrow never accepts a value while disagreeing with `_coerce_str` about
  it — it either matches or declines — so results are unchanged. CSV
  batch mode went from ~67 000 to ~88 000 rps. (Which formatting the
  vectorised path accepts is covered by the entry above.)
- **The CSV cast plan is resolved once per file, not once per batch.**
  `cast_csv_batch` previously rebuilt a `pyarrow.Schema` (and a `Field`
  per column) on every batch, so its allocation cost scaled with *batch
  count*: a 1M-row file at `batch_size=100` is 10 000 batches and so
  10 000 throwaway schema objects, against 1 000 at `batch_size=1000`.
  Column indices and the output schema are now cached per (model, input
  schema) and reused whenever every column casts cleanly.
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
- **The memory benchmark reported an unstable number at small batch
  sizes.** `tracemalloc` records a high-water mark, so the figure
  includes transient garbage as well as the live working set. Once the
  live set drops below ~1 MB that garbage dominates and the result
  stops being reproducible: at `batch_size=100` the same code has
  measured 0.24, 0.25, 1.85, 1.90 and 8.23 MB across runs and machines,
  while `batch_size>=1000` reproduces to within 0.01 MB everywhere.
  `bench_memory.py` now collects garbage periodically during each
  measurement, runs each configuration `REPS` times, and reports both
  the floor and the worst case so an unstable measurement is visible
  instead of being reduced to one flattering number. Neither change
  weakens leak detection — `gc.collect()` only frees unreachable
  objects. The row count dropped to 500 000 to keep the repeated runs
  affordable; peak memory at the default batch size is flat with respect
  to file size (1.89 MB at 500k rows, 1.91 MB at 1M), which is the
  property the benchmark exists to demonstrate.
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

# Changelog

All notable changes to streamval will be documented in this file.

## [0.1.0] - 2026-05-21

### Added
- Initial alpha release.
- StreamValidator with pluggable error strategies (fail_fast, collect, skip).
- Async generator adapters for CSV, JSONL, Parquet, and Arrow.
- Compiled, cached Pydantic validation plans.
- StreamStats with throughput, peak-memory, and per-field error counters.

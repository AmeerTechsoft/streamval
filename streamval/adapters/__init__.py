"""Format adapters.

Each adapter is an async generator yielding ``dict[str, Any]`` rows from a
file without loading the whole file into memory. CSV, JSONL, Parquet, and
Arrow are supported.
"""

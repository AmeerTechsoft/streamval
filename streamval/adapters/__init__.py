"""Format adapters.

Each adapter is an async generator yielding ``dict[str, Any]`` rows from a
file (or HTTP stream) without loading the whole payload into memory. CSV,
JSONL, Parquet, Arrow, and HTTP NDJSON are supported.
"""

from streamval.adapters.http_ndjson_adapter import HttpNdjsonConfig

__all__ = ["HttpNdjsonConfig"]

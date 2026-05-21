"""streamval — streaming, Pydantic-backed validation for tabular files and HTTP streams.

Public surface (everything else is internal):

* :class:`StreamValidator` — the main engine.
* :class:`ValidationResult`, :class:`FieldError`, :class:`StreamStats`
  — result and summary containers.
* :class:`StreamValidationError` — raised by ``fail_fast`` and by
  ``collect`` when ``max_errors`` is exceeded.
* :class:`StreamFetchError` — raised by the HTTP adapter on transport
  failures and JSON-format errors.
* :class:`HttpNdjsonConfig` — configuration for the HTTP NDJSON adapter.
* :data:`ErrorStrategy` — the literal type alias for strategy names.
* Convenience functions :func:`stream_csv`, :func:`stream_jsonl`,
  :func:`stream_parquet`, :func:`stream_http_ndjson` and their async
  variants ``astream_*``.
"""

from __future__ import annotations

from streamval.adapters.http_ndjson_adapter import HttpNdjsonConfig
from streamval.core.result import (
    FieldError,
    StreamFetchError,
    StreamValidationError,
    ValidationResult,
)
from streamval.core.stats import StreamStats
from streamval.core.validator import (
    StreamValidator,
    astream_csv,
    astream_http_ndjson,
    astream_jsonl,
    astream_parquet,
    stream_csv,
    stream_http_ndjson,
    stream_jsonl,
    stream_parquet,
)
from streamval.strategies.base import ErrorStrategy

__version__ = "0.1.0"

__all__ = [
    "ErrorStrategy",
    "FieldError",
    "HttpNdjsonConfig",
    "StreamFetchError",
    "StreamStats",
    "StreamValidationError",
    "StreamValidator",
    "ValidationResult",
    "__version__",
    "astream_csv",
    "astream_http_ndjson",
    "astream_jsonl",
    "astream_parquet",
    "stream_csv",
    "stream_http_ndjson",
    "stream_jsonl",
    "stream_parquet",
]

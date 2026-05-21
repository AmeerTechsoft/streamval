"""Smoke tests for the public API surface of streamval."""

from __future__ import annotations

import streamval


def test_public_surface() -> None:
    expected = {
        "StreamValidator",
        "ValidationResult",
        "FieldError",
        "StreamStats",
        "StreamValidationError",
        "ErrorStrategy",
        "stream_csv",
        "stream_jsonl",
        "stream_parquet",
        "astream_csv",
        "astream_jsonl",
        "astream_parquet",
    }
    assert expected.issubset(set(streamval.__all__))
    for name in expected:
        assert hasattr(streamval, name), name


def test_version_set() -> None:
    assert streamval.__version__ == "0.2.1"

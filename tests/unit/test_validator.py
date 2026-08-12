"""Smoke tests for the public API surface of streamval."""

from __future__ import annotations

import tomllib
from pathlib import Path

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


def test_version_matches_pyproject() -> None:
    """``__version__`` must track pyproject.toml, the source of truth.

    Read it rather than hardcoding, so a release bump can't leave the two
    disagreeing (and so this test never needs editing again).
    """
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    with pyproject.open("rb") as f:
        declared = tomllib.load(f)["project"]["version"]
    assert streamval.__version__ == declared

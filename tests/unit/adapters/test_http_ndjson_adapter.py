"""Unit tests for :class:`HttpNdjsonConfig` (PROMPT B1).

B1 is config + scaffold only. The actual streaming is implemented in
PROMPT B2; here we cover constructor defaults, ``from_url`` sugar, and
input validation. The scaffolded ``stream_rows`` coroutine is also
asserted to raise :class:`NotImplementedError` so the contract is
explicit.
"""

from __future__ import annotations

import pytest

from streamval.adapters.http_ndjson_adapter import (
    HttpNdjsonConfig,
    stream_rows,
    stream_rows_sync,
)


class TestHttpNdjsonConfigDefaults:
    def test_minimal_construction(self) -> None:
        cfg = HttpNdjsonConfig(url="https://example.com/stream")
        assert cfg.url == "https://example.com/stream"
        assert cfg.headers == {}
        assert cfg.params == {}
        assert cfg.timeout_seconds == 30.0
        assert cfg.connect_timeout_seconds == 10.0
        assert cfg.max_retries == 3
        assert cfg.retry_backoff_seconds == 1.0
        assert cfg.follow_redirects is True
        assert cfg.auth_token is None
        assert cfg.event_stream is False
        assert cfg.line_filter is None
        assert cfg.skip_empty_lines is True
        assert cfg.max_lines is None

    def test_inherited_adapter_config_defaults(self) -> None:
        cfg = HttpNdjsonConfig(url="http://localhost:8080/stream")
        assert cfg.encoding == "utf-8"
        assert cfg.chunk_size == 8192
        assert cfg.skip_header is False
        assert cfg.mode == "row"

    def test_is_frozen(self) -> None:
        cfg = HttpNdjsonConfig(url="https://example.com/s")
        with pytest.raises((AttributeError, Exception)):
            cfg.url = "https://other.example/s"  # type: ignore[misc]


class TestFromUrl:
    def test_from_url_basic(self) -> None:
        cfg = HttpNdjsonConfig.from_url("https://example.com/stream")
        assert cfg.url == "https://example.com/stream"

    def test_from_url_with_overrides(self) -> None:
        cfg = HttpNdjsonConfig.from_url(
            "https://example.com/stream",
            auth_token="sk-abc",
            event_stream=True,
            max_lines=500,
            timeout_seconds=5.0,
        )
        assert cfg.auth_token == "sk-abc"
        assert cfg.event_stream is True
        assert cfg.max_lines == 500
        assert cfg.timeout_seconds == 5.0


class TestValidation:
    def test_empty_url_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            HttpNdjsonConfig(url="")

    def test_non_http_scheme_rejected(self) -> None:
        with pytest.raises(ValueError, match="http://"):
            HttpNdjsonConfig(url="ftp://example.com/stream")

    def test_file_scheme_rejected(self) -> None:
        with pytest.raises(ValueError, match="http://"):
            HttpNdjsonConfig(url="file:///etc/passwd")

    def test_negative_timeout_rejected(self) -> None:
        with pytest.raises(ValueError, match="timeout_seconds"):
            HttpNdjsonConfig(url="https://x/s", timeout_seconds=0)

    def test_negative_connect_timeout_rejected(self) -> None:
        with pytest.raises(ValueError, match="connect_timeout_seconds"):
            HttpNdjsonConfig(
                url="https://x/s", connect_timeout_seconds=-1.0
            )

    def test_negative_retries_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_retries"):
            HttpNdjsonConfig(url="https://x/s", max_retries=-1)

    def test_zero_retries_allowed(self) -> None:
        cfg = HttpNdjsonConfig(url="https://x/s", max_retries=0)
        assert cfg.max_retries == 0

    def test_negative_backoff_rejected(self) -> None:
        with pytest.raises(ValueError, match="retry_backoff_seconds"):
            HttpNdjsonConfig(url="https://x/s", retry_backoff_seconds=-0.1)

    def test_zero_max_lines_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_lines"):
            HttpNdjsonConfig(url="https://x/s", max_lines=0)

    def test_negative_max_lines_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_lines"):
            HttpNdjsonConfig(url="https://x/s", max_lines=-5)


class TestScaffoldedStreams:
    """B2 will implement these; B1 keeps the contract explicit."""

    async def test_stream_rows_not_implemented(self) -> None:
        cfg = HttpNdjsonConfig(url="https://example.com/s")
        with pytest.raises(NotImplementedError, match="PROMPT B2"):
            async for _ in stream_rows(cfg):
                pass

    def test_stream_rows_sync_not_implemented(self) -> None:
        cfg = HttpNdjsonConfig(url="https://example.com/s")
        with pytest.raises(NotImplementedError, match="PROMPT B2"):
            for _ in stream_rows_sync(cfg):
                pass

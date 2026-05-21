"""Unit tests for the streaming HTTP NDJSON adapter (PROMPT B2).

Uses :mod:`pytest_httpx` to mock the network without spinning up a
local server. Each test exercises one branch of the adapter's
behaviour: happy NDJSON, SSE parsing, retries, fail-fast 4xx,
``max_lines``, empty-line handling, and malformed JSON.
"""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from streamval.adapters.http_ndjson_adapter import (
    HttpNdjsonConfig,
    stream_rows,
    stream_rows_sync,
)
from streamval.core.result import StreamFetchError

URL = "https://api.example.com/stream"


def _ndjson(rows: list[dict[str, object]]) -> bytes:
    import json

    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode()


async def _collect(config: HttpNdjsonConfig) -> list[dict[str, object]]:
    return [row async for row in stream_rows(config)]


class TestHappyPath:
    async def test_basic_ndjson(self, httpx_mock: HTTPXMock) -> None:
        rows = [{"id": i, "name": f"n{i}"} for i in range(100)]
        httpx_mock.add_response(url=URL, content=_ndjson(rows))

        got = await _collect(HttpNdjsonConfig(url=URL))
        assert got == rows

    async def test_skips_empty_lines(self, httpx_mock: HTTPXMock) -> None:
        body = b'{"a": 1}\n\n\n{"a": 2}\n\n{"a": 3}\n'
        httpx_mock.add_response(url=URL, content=body)

        got = await _collect(HttpNdjsonConfig(url=URL))
        assert got == [{"a": 1}, {"a": 2}, {"a": 3}]

    async def test_max_lines_truncates(self, httpx_mock: HTTPXMock) -> None:
        rows = [{"i": i} for i in range(10_000)]
        httpx_mock.add_response(url=URL, content=_ndjson(rows))

        got = await _collect(HttpNdjsonConfig(url=URL, max_lines=500))
        assert len(got) == 500
        assert got[-1] == {"i": 499}

    async def test_custom_headers_and_auth(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(url=URL, content=b'{"ok": true}\n')

        await _collect(
            HttpNdjsonConfig(
                url=URL,
                auth_token="sk-abc",
                headers={"X-Trace": "t1"},
            )
        )
        req = httpx_mock.get_requests()[0]
        assert req.headers["authorization"] == "Bearer sk-abc"
        assert req.headers["x-trace"] == "t1"

    async def test_query_params_forwarded(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url=f"{URL}?limit=10&q=foo", content=b'{"ok": true}\n'
        )
        await _collect(
            HttpNdjsonConfig(url=URL, params={"limit": 10, "q": "foo"})
        )


class TestSSE:
    async def test_openai_style_sse(self, httpx_mock: HTTPXMock) -> None:
        body = (
            b'data: {"choices":[{"delta":{"content":"Hel"}}]}\n'
            b'\n'
            b'data: {"choices":[{"delta":{"content":"lo"}}]}\n'
            b'\n'
            b'data: [DONE]\n'
            b'\n'
        )
        httpx_mock.add_response(url=URL, content=body)

        rows = await _collect(HttpNdjsonConfig(url=URL, event_stream=True))
        assert len(rows) == 2
        assert rows[0]["choices"][0]["delta"]["content"] == "Hel"  # type: ignore[index,call-overload]

    async def test_sse_ignores_non_data_lines(
        self, httpx_mock: HTTPXMock
    ) -> None:
        body = (
            b'event: message\n'
            b'id: 1\n'
            b'data: {"chunk": 1}\n'
            b'\n'
            b': heartbeat comment\n'
            b'data: {"chunk": 2}\n'
            b'\n'
        )
        httpx_mock.add_response(url=URL, content=body)

        rows = await _collect(HttpNdjsonConfig(url=URL, event_stream=True))
        assert rows == [{"chunk": 1}, {"chunk": 2}]


class TestLineFilter:
    async def test_line_filter_strips_prefix(
        self, httpx_mock: HTTPXMock
    ) -> None:
        body = (
            b'log: {"level": "info"}\n'
            b'meta: ignore-me\n'
            b'log: {"level": "warn"}\n'
        )
        httpx_mock.add_response(url=URL, content=body)
        rows = await _collect(HttpNdjsonConfig(url=URL, line_filter="log: "))
        assert rows == [{"level": "info"}, {"level": "warn"}]


class TestRetries:
    async def test_retry_on_503_then_success(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(url=URL, status_code=503)
        httpx_mock.add_response(url=URL, status_code=503)
        httpx_mock.add_response(url=URL, content=b'{"ok": true}\n')

        rows = await _collect(
            HttpNdjsonConfig(url=URL, max_retries=3, retry_backoff_seconds=0)
        )
        assert rows == [{"ok": True}]
        assert len(httpx_mock.get_requests()) == 3

    async def test_retry_exhaustion_raises(
        self, httpx_mock: HTTPXMock
    ) -> None:
        for _ in range(4):
            httpx_mock.add_response(url=URL, status_code=503)

        with pytest.raises(StreamFetchError) as ei:
            await _collect(
                HttpNdjsonConfig(
                    url=URL, max_retries=3, retry_backoff_seconds=0
                )
            )
        assert ei.value.status_code == 503
        assert ei.value.attempt_count == 4
        assert ei.value.url == URL

    async def test_retry_on_429(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=URL, status_code=429)
        httpx_mock.add_response(url=URL, content=b'{"ok": 1}\n')

        rows = await _collect(
            HttpNdjsonConfig(url=URL, max_retries=2, retry_backoff_seconds=0)
        )
        assert rows == [{"ok": 1}]


class TestFailFast4xx:
    @pytest.mark.parametrize("status", [401, 403, 404])
    async def test_fail_fast_no_retry(
        self, httpx_mock: HTTPXMock, status: int
    ) -> None:
        httpx_mock.add_response(url=URL, status_code=status)
        with pytest.raises(StreamFetchError) as ei:
            await _collect(
                HttpNdjsonConfig(
                    url=URL, max_retries=5, retry_backoff_seconds=0
                )
            )
        assert ei.value.status_code == status
        assert ei.value.attempt_count == 1
        # Only one request was made (no retry).
        assert len(httpx_mock.get_requests()) == 1

    async def test_other_4xx_no_retry(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=URL, status_code=400)
        with pytest.raises(StreamFetchError) as ei:
            await _collect(
                HttpNdjsonConfig(
                    url=URL, max_retries=5, retry_backoff_seconds=0
                )
            )
        assert ei.value.status_code == 400


class TestMalformedPayload:
    async def test_invalid_json_raises(self, httpx_mock: HTTPXMock) -> None:
        body = b'{"ok": 1}\n{not-json}\n'
        httpx_mock.add_response(url=URL, content=body)
        with pytest.raises(StreamFetchError, match="invalid JSON"):
            await _collect(HttpNdjsonConfig(url=URL))

    async def test_non_object_raises(self, httpx_mock: HTTPXMock) -> None:
        body = b'{"ok": 1}\n[1, 2, 3]\n'
        httpx_mock.add_response(url=URL, content=body)
        with pytest.raises(StreamFetchError, match="not a JSON object"):
            await _collect(HttpNdjsonConfig(url=URL))


class TestSyncWrapper:
    def test_sync_wrapper_streams(self, httpx_mock: HTTPXMock) -> None:
        rows = [{"i": i} for i in range(50)]
        httpx_mock.add_response(url=URL, content=_ndjson(rows))

        got = list(stream_rows_sync(HttpNdjsonConfig(url=URL)))
        assert got == rows

    def test_sync_wrapper_fail_fast_4xx(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(url=URL, status_code=403)
        with pytest.raises(StreamFetchError):
            list(stream_rows_sync(HttpNdjsonConfig(url=URL)))

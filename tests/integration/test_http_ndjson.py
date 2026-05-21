"""Integration tests for the HTTP NDJSON adapter and the ``llm`` helpers.

Spins up a real local HTTP server in a background thread (stdlib only
- no respx, no httpbin, no external dependencies) and validates that
the streaming pipeline survives realistic network conditions: large
bodies, retries, hangs, mixed-validity payloads, and provider-style
SSE wire formats.
"""

from __future__ import annotations

import contextlib
import json
import threading
import time
import tracemalloc
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from pydantic import BaseModel

from streamval import HttpNdjsonConfig, StreamFetchError, StreamValidator, llm
from streamval.llm import LLMProvider

# ---------------------------------------------------------------------
# Local stub HTTP server
# ---------------------------------------------------------------------


class _Behaviour:
    """Mutable behaviour state shared between the handler and tests."""

    def __init__(self) -> None:
        self.routes: dict[str, Callable[[BaseHTTPRequestHandler], None]] = {}
        # Per-route attempt counters (for retry tests).
        self.attempts: dict[str, int] = {}


def _make_handler(behaviour: _Behaviour) -> type[BaseHTTPRequestHandler]:
    class _StubHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 -- stdlib API
            path = self.path.split("?", 1)[0]
            behaviour.attempts[path] = behaviour.attempts.get(path, 0) + 1
            route = behaviour.routes.get(path)
            if route is None:
                self.send_response(404)
                self.end_headers()
                return
            route(self)

        def log_message(
            self, format: str, *args: Any
        ) -> None:  # noqa: A002 -- stdlib name
            return

    return _StubHandler


@pytest.fixture
def stub_server() -> Iterator[tuple[str, _Behaviour]]:
    behaviour = _Behaviour()
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), _make_handler(behaviour)
    )
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    try:
        # Give the server a beat to bind on slower CI runners.
        time.sleep(0.02)
        yield base, behaviour
    finally:
        server.shutdown()
        thread.join(timeout=2.0)
        server.server_close()


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


class Row(BaseModel):
    id: int
    value: str


def _ok_ndjson(rows: list[dict[str, Any]]) -> Callable[
    [BaseHTTPRequestHandler], None
]:
    body = ("\n".join(json.dumps(r) for r in rows) + "\n").encode()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "application/x-ndjson")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    return respond


def _ok_sse(events: list[dict[str, Any]]) -> Callable[
    [BaseHTTPRequestHandler], None
]:
    parts = [f"data: {json.dumps(e)}\n\n" for e in events]
    parts.append("data: [DONE]\n\n")
    body = "".join(parts).encode()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "text/event-stream")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    return respond


def _flaky_then_ok(
    fail_status: int, fail_times: int, ok_handler: Callable[
        [BaseHTTPRequestHandler], None
    ]
) -> Callable[[BaseHTTPRequestHandler], None]:
    counter = {"n": 0}

    def respond(handler: BaseHTTPRequestHandler) -> None:
        counter["n"] += 1
        if counter["n"] <= fail_times:
            handler.send_response(fail_status)
            handler.end_headers()
            return
        ok_handler(handler)

    return respond


def _slow_then_hang(
    rows_before_hang: int, hang_seconds: float
) -> Callable[[BaseHTTPRequestHandler], None]:
    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "application/x-ndjson")
        handler.send_header("Transfer-Encoding", "chunked")
        handler.end_headers()
        for i in range(rows_before_hang):
            chunk = (json.dumps({"id": i, "value": f"v{i}"}) + "\n").encode()
            handler.wfile.write(
                f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n"
            )
            handler.wfile.flush()
        # Hold the connection open without closing the chunked stream;
        # client will time out.
        time.sleep(hang_seconds)

    return respond


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


def test_streams_10k_ndjson_bounded_memory(
    stub_server: tuple[str, _Behaviour],
) -> None:
    base, behaviour = stub_server
    rows = [{"id": i, "value": f"v{i}"} for i in range(10_000)]
    behaviour.routes["/stream"] = _ok_ndjson(rows)

    v = StreamValidator(Row, on_error="collect", batch_size=500)

    tracemalloc.start()
    tracemalloc.reset_peak()
    results = list(v.stream_http_ndjson(f"{base}/stream"))
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / (1024 * 1024)

    assert len(results) == 10_000
    assert all(r.valid for r in results)
    # 10k rows over the loopback should easily fit under 10 MB of Python
    # object memory; httpx + tracemalloc both add overhead vs the file
    # adapters.
    assert peak_mb < 10.0, f"peak {peak_mb:.2f} MB exceeds 10 MB budget"


def test_streams_openai_style_sse(
    stub_server: tuple[str, _Behaviour],
) -> None:
    base, behaviour = stub_server
    events = [
        {"choices": [{"delta": {"content": ch}}]}
        for ch in "Hello from a streaming LLM!"
    ]
    behaviour.routes["/v1/chat/completions"] = _ok_sse(events)

    text: list[str] = []
    for result in llm.validate_llm_stream(
        f"{base}/v1/chat/completions",
        _OpenAIChunk,
        provider=LLMProvider.OPENAI,
    ):
        if not result.valid:
            continue
        chunk = llm.extract_content(result, LLMProvider.OPENAI)
        if chunk:
            text.append(chunk)
    assert "".join(text) == "Hello from a streaming LLM!"


class _OpenAIChunk(BaseModel):
    choices: list[dict[str, Any]] | None = None


def test_retries_503_then_succeeds(
    stub_server: tuple[str, _Behaviour],
) -> None:
    base, behaviour = stub_server
    rows = [{"id": i, "value": f"v{i}"} for i in range(50)]
    behaviour.routes["/retry"] = _flaky_then_ok(
        503, 2, _ok_ndjson(rows)
    )

    v = StreamValidator(Row, on_error="collect")
    results = list(
        v.stream_http_ndjson(
            f"{base}/retry", max_retries=3, retry_backoff_seconds=0
        )
    )
    assert len(results) == 50
    # 2 failures + 1 success = 3 attempts.
    assert behaviour.attempts["/retry"] == 3


def test_timeout_after_partial_stream_raises(
    stub_server: tuple[str, _Behaviour],
) -> None:
    base, behaviour = stub_server
    behaviour.routes["/hang"] = _slow_then_hang(
        rows_before_hang=100, hang_seconds=5.0
    )

    v = StreamValidator(Row, on_error="collect")
    config = HttpNdjsonConfig(
        url=f"{base}/hang",
        timeout_seconds=0.5,
        connect_timeout_seconds=0.5,
        max_retries=1,
        retry_backoff_seconds=0,
    )
    with pytest.raises(StreamFetchError) as ei:
        with contextlib.closing(v.stream_http_ndjson(config)) as it:
            for _ in it:
                pass
    assert ei.value.attempt_count >= 1


def test_mixed_valid_and_invalid_rows(
    stub_server: tuple[str, _Behaviour],
) -> None:
    base, behaviour = stub_server
    rows: list[dict[str, Any]] = []
    for i in range(9_000):
        rows.append({"id": i, "value": f"v{i}"})
    for i in range(1_000):
        # id is required to be int; send a string that can't coerce.
        rows.append({"id": f"not-an-int-{i}", "value": "x"})

    behaviour.routes["/mixed"] = _ok_ndjson(rows)

    v = StreamValidator(Row, on_error="collect", batch_size=500)
    results = list(v.stream_http_ndjson(f"{base}/mixed"))
    invalid = [r for r in results if not r.valid]

    assert len(results) == 10_000
    assert v.stats.rows_invalid == 1_000
    assert len(invalid) == 1_000


def test_max_lines_truncates(
    stub_server: tuple[str, _Behaviour],
) -> None:
    base, behaviour = stub_server
    rows = [{"id": i, "value": f"v{i}"} for i in range(10_000)]
    behaviour.routes["/cap"] = _ok_ndjson(rows)

    v = StreamValidator(Row, on_error="collect")
    results = list(
        v.stream_http_ndjson(f"{base}/cap", max_lines=500)
    )
    assert len(results) == 500
    assert results[-1].data is not None
    assert results[-1].data.id == 499  # type: ignore[attr-defined]

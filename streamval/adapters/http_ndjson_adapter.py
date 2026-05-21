"""HTTP NDJSON streaming adapter.

Streams an HTTP response body line by line via
``httpx.AsyncClient.stream`` without buffering the full body. Supports
NDJSON, Server-Sent Events (``event_stream=True``), custom line
prefixes (``line_filter``), Bearer-token auth, retry-with-backoff on
transport / 5xx / 429 failures, and early termination via
``max_lines``.

Transport-level failures (connect, timeout, retry exhaustion, 4xx)
raise :class:`streamval.core.result.StreamFetchError`. Per-line JSON
parse failures also raise :class:`StreamFetchError`; this is a
*format* error, not a row-level validation error, so it cannot be
masked by an error strategy.

See :mod:`streamval.llm` for pre-configured wrappers around
OpenAI / Anthropic SSE streams.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass, field
from typing import Any

from streamval._compat import HAS_ORJSON, orjson, require_httpx
from streamval.adapters.base import AdapterConfig
from streamval.core.result import StreamFetchError

_RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}
_FAIL_FAST_STATUSES = {401, 403, 404}
_SSE_PREFIX = "data: "
_SSE_DONE = "[DONE]"


@dataclass(frozen=True)
class HttpNdjsonConfig(AdapterConfig):
    """Configuration for the HTTP NDJSON adapter.

    Extends :class:`AdapterConfig` with HTTP-specific knobs. Instances
    are frozen so they can be shared between the async and sync entry
    points without aliasing surprises.

    See module docstring for behavioural details on each field.
    """

    url: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 30.0
    connect_timeout_seconds: float = 10.0
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    follow_redirects: bool = True
    auth_token: str | None = None
    event_stream: bool = False
    line_filter: str | None = None
    skip_empty_lines: bool = True
    max_lines: int | None = None

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("HttpNdjsonConfig.url must be a non-empty string")
        if not (
            self.url.startswith("http://") or self.url.startswith("https://")
        ):
            raise ValueError(
                f"HttpNdjsonConfig.url must be http:// or https://: "
                f"got {self.url!r}"
            )
        if self.timeout_seconds <= 0:
            raise ValueError(
                f"timeout_seconds must be > 0, got {self.timeout_seconds!r}"
            )
        if self.connect_timeout_seconds <= 0:
            raise ValueError(
                "connect_timeout_seconds must be > 0, got "
                f"{self.connect_timeout_seconds!r}"
            )
        if self.max_retries < 0:
            raise ValueError(
                f"max_retries must be >= 0, got {self.max_retries!r}"
            )
        if self.retry_backoff_seconds < 0:
            raise ValueError(
                "retry_backoff_seconds must be >= 0, got "
                f"{self.retry_backoff_seconds!r}"
            )
        if self.max_lines is not None and self.max_lines <= 0:
            raise ValueError(
                f"max_lines must be > 0 or None, got {self.max_lines!r}"
            )

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> HttpNdjsonConfig:
        """Convenience constructor: ``HttpNdjsonConfig.from_url(url, ...)``."""
        return cls(url=url, **kwargs)


def _build_headers(config: HttpNdjsonConfig) -> dict[str, str]:
    headers = dict(config.headers)
    if config.auth_token is not None:
        headers["Authorization"] = f"Bearer {config.auth_token}"
    return headers


def _parse_json(payload: str | bytes) -> Any:
    if HAS_ORJSON and orjson is not None:
        return orjson.loads(payload)
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    return json.loads(payload)


def _filter_line(line: str, config: HttpNdjsonConfig) -> str | None:
    """Apply ``event_stream`` and ``line_filter`` logic.

    Returns the payload string to JSON-parse, or ``None`` if the line
    should be skipped silently. Returns the literal sentinel
    ``"\x00DONE"`` to signal the SSE ``[DONE]`` terminator.
    """
    if not line and config.skip_empty_lines:
        return None
    if config.event_stream:
        if not line.startswith(_SSE_PREFIX):
            return None
        payload = line[len(_SSE_PREFIX) :].strip()
        if payload == _SSE_DONE:
            return "\x00DONE"
        return payload
    if config.line_filter is not None:
        if not line.startswith(config.line_filter):
            return None
        return line[len(config.line_filter) :]
    return line


async def stream_rows(
    config: HttpNdjsonConfig,
) -> AsyncGenerator[dict[str, Any], None]:
    """Async iterator over NDJSON / SSE lines from an HTTP URL.

    Yields one parsed JSON object per accepted line. Honours
    ``event_stream``, ``line_filter``, ``skip_empty_lines``, and
    ``max_lines``. Retries on transport / 5xx / 429 failures with
    linear backoff; raises :class:`StreamFetchError` on retry
    exhaustion, hard 4xx, or JSON parse failure.
    """
    httpx = require_httpx()
    timeout = httpx.Timeout(
        config.timeout_seconds, connect=config.connect_timeout_seconds
    )
    headers = _build_headers(config)

    attempt = 0
    last_status: int | None = None
    while True:
        attempt += 1
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=config.follow_redirects,
                headers=headers,
            ) as client:
                async with client.stream(
                    "GET", config.url, params=config.params
                ) as response:
                    last_status = response.status_code
                    if response.status_code in _FAIL_FAST_STATUSES:
                        raise StreamFetchError(
                            f"HTTP {response.status_code} (no retry)",
                            url=config.url,
                            status_code=response.status_code,
                            attempt_count=attempt,
                        )
                    if response.status_code in _RETRYABLE_STATUSES:
                        # Drain so the connection can be reused/closed.
                        await response.aread()
                        if attempt > config.max_retries:
                            raise StreamFetchError(
                                f"HTTP {response.status_code} after "
                                f"{attempt} attempts",
                                url=config.url,
                                status_code=response.status_code,
                                attempt_count=attempt,
                            )
                        await asyncio.sleep(
                            config.retry_backoff_seconds * attempt
                        )
                        continue
                    if response.status_code >= 400:
                        await response.aread()
                        raise StreamFetchError(
                            f"HTTP {response.status_code}",
                            url=config.url,
                            status_code=response.status_code,
                            attempt_count=attempt,
                        )

                    lines_emitted = 0
                    async for raw_line in response.aiter_lines():
                        # httpx already strips the trailing newline.
                        payload = _filter_line(raw_line, config)
                        if payload is None:
                            continue
                        if payload == "\x00DONE":
                            return
                        try:
                            obj = _parse_json(payload)
                        except (json.JSONDecodeError, ValueError) as exc:
                            raise StreamFetchError(
                                f"invalid JSON on streamed line: {exc}",
                                url=config.url,
                                status_code=response.status_code,
                                attempt_count=attempt,
                            ) from exc
                        if not isinstance(obj, dict):
                            raise StreamFetchError(
                                "streamed line is not a JSON object: "
                                f"{type(obj).__name__}",
                                url=config.url,
                                status_code=response.status_code,
                                attempt_count=attempt,
                            )
                        yield obj
                        lines_emitted += 1
                        if (
                            config.max_lines is not None
                            and lines_emitted >= config.max_lines
                        ):
                            return
                    return
        except (
            httpx.ConnectError,
            httpx.RemoteProtocolError,
            httpx.TimeoutException,
        ) as exc:
            if attempt > config.max_retries:
                raise StreamFetchError(
                    f"transport failure after {attempt} attempts: {exc}",
                    url=config.url,
                    status_code=last_status,
                    attempt_count=attempt,
                ) from exc
            await asyncio.sleep(config.retry_backoff_seconds * attempt)
            continue


def stream_rows_sync(
    config: HttpNdjsonConfig,
) -> Iterator[dict[str, Any]]:
    """Sync iterator over NDJSON / SSE lines from an HTTP URL.

    Drives :func:`stream_rows` via a private event loop, one item at a
    time, so the streaming contract is preserved on the sync path too.
    """
    loop = asyncio.new_event_loop()
    agen = stream_rows(config)
    try:
        while True:
            try:
                yield loop.run_until_complete(agen.__anext__())
            except StopAsyncIteration:
                return
    finally:
        try:
            loop.run_until_complete(agen.aclose())
        except Exception:  # noqa: BLE001 -- best-effort cleanup
            pass
        loop.close()


__all__ = [
    "HttpNdjsonConfig",
    "stream_rows",
    "stream_rows_sync",
]

"""HTTP NDJSON streaming adapter — config + scaffold.

This module defines :class:`HttpNdjsonConfig` (the configuration
dataclass) and the public coroutine signatures for the HTTP NDJSON
adapter. The actual HTTP streaming logic is implemented in PROMPT B2;
calling the generator here raises :class:`NotImplementedError`.

The adapter streams an HTTP response body line by line via
``httpx.AsyncClient.stream`` without buffering the full body. It is
the only adapter that is network-bound rather than I/O-bound, so it
exposes additional knobs for timeouts, retries, auth, and SSE / NDJSON
line filtering.

See also:
    * :mod:`streamval.llm` for pre-configured wrappers around OpenAI /
      Anthropic SSE streams (added in PROMPT B3).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from typing import Any

from streamval.adapters.base import AdapterConfig


@dataclass(frozen=True)
class HttpNdjsonConfig(AdapterConfig):
    """Configuration for the HTTP NDJSON adapter.

    Extends :class:`AdapterConfig` with HTTP-specific knobs. Instances
    are frozen so they can be shared between the async and sync entry
    points without aliasing surprises.

    Attributes:
        url: HTTP/HTTPS URL to stream from. Required.
        headers: Extra request headers, merged into the client's
            defaults. ``Authorization`` set from :attr:`auth_token`
            takes precedence over any value here.
        params: Query-string parameters appended to the URL.
        timeout_seconds: Per-request total timeout in seconds. Applied
            to read, write, and pool acquisition. Default ``30.0``.
        connect_timeout_seconds: Connection-establishment timeout in
            seconds. Default ``10.0``.
        max_retries: Maximum number of streaming attempts on
            retryable transport / 5xx / 429 errors. Default ``3``.
            Set to ``0`` to disable retries.
        retry_backoff_seconds: Base backoff multiplier. The Nth retry
            waits ``retry_backoff_seconds * N`` seconds.
        follow_redirects: Whether httpx should follow 3xx redirects.
        auth_token: Optional bearer token. When set, the adapter
            sends ``Authorization: Bearer <token>``.
        event_stream: When ``True``, the adapter parses incoming lines
            as Server-Sent Events: only ``data: ...`` lines are
            forwarded, the ``data: `` prefix is stripped, and the
            ``[DONE]`` sentinel terminates the stream cleanly.
        line_filter: Optional literal prefix. Lines that don't start
            with this prefix are skipped; matching lines have the
            prefix stripped before JSON parsing. ``event_stream``
            implies ``line_filter = "data: "`` but is more
            full-featured (handles ``[DONE]`` etc.).
        skip_empty_lines: Skip blank lines silently instead of raising.
        max_lines: Stop after this many parsed lines have been yielded
            (counted *after* filtering). Useful for capping LLM
            streams or partial previews. ``None`` means unbounded.
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
        """Convenience constructor: ``HttpNdjsonConfig.from_url(url, ...)``.

        Equivalent to ``HttpNdjsonConfig(url=url, **kwargs)`` but reads
        more naturally at call sites where only the URL is mandatory.
        """
        return cls(url=url, **kwargs)


async def stream_rows(
    config: HttpNdjsonConfig,
) -> AsyncIterator[dict[str, Any]]:
    """Async iterator over NDJSON / SSE lines from an HTTP URL.

    Implemented in PROMPT B2. The B1 scaffold raises so callers fail
    loudly if they try to use the adapter before B2 lands.
    """
    raise NotImplementedError(
        "HTTP NDJSON streaming is implemented in PROMPT B2"
    )
    # Make this an async generator for type-checking purposes.
    if False:  # pragma: no cover
        yield {}


def stream_rows_sync(
    config: HttpNdjsonConfig,
) -> Iterator[dict[str, Any]]:
    """Sync iterator over NDJSON / SSE lines from an HTTP URL.

    Implemented in PROMPT B2.
    """
    raise NotImplementedError(
        "HTTP NDJSON streaming is implemented in PROMPT B2"
    )
    if False:  # pragma: no cover
        yield {}


__all__ = [
    "HttpNdjsonConfig",
    "stream_rows",
    "stream_rows_sync",
]

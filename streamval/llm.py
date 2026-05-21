"""LLM streaming validation helpers.

A thin convenience layer over :mod:`streamval.adapters.http_ndjson_adapter`
that ships pre-configured :class:`HttpNdjsonConfig` defaults for the
common LLM provider streaming formats and a small ``extract_content``
helper for pulling the human-readable text chunk out of each
:class:`ValidationResult`.

This module **never** imports an LLM SDK (``openai``, ``anthropic``,
…). It only knows about HTTP, SSE, and JSON shapes. Bring your own
auth token.

Example::

    from pydantic import BaseModel
    from streamval import llm

    class Chunk(BaseModel):
        id: str | None = None
        choices: list[dict] | None = None
        ...  # whatever fields you want to assert on

    for result in llm.validate_llm_stream(
        url="https://api.openai.com/v1/chat/completions",
        schema=Chunk,
        provider=llm.LLMProvider.OPENAI,
        auth_token="sk-...",
    ):
        if result.valid:
            text = llm.extract_content(result, llm.LLMProvider.OPENAI)
            if text:
                print(text, end="", flush=True)
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from enum import StrEnum
from typing import Any

from pydantic import BaseModel

from streamval.adapters.http_ndjson_adapter import HttpNdjsonConfig
from streamval.core.result import ValidationResult
from streamval.core.validator import (
    astream_http_ndjson,
    stream_http_ndjson,
)
from streamval.strategies.base import ErrorStrategy


class LLMProvider(StrEnum):
    """Known LLM streaming wire formats."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GENERIC_SSE = "generic_sse"
    GENERIC_NDJSON = "generic_ndjson"


_PROVIDER_HTTP_DEFAULTS: dict[LLMProvider, dict[str, Any]] = {
    LLMProvider.OPENAI: {"event_stream": True},
    LLMProvider.ANTHROPIC: {"event_stream": True},
    LLMProvider.GENERIC_SSE: {"event_stream": True},
    LLMProvider.GENERIC_NDJSON: {"event_stream": False},
}

_CONTENT_PATHS: dict[LLMProvider, tuple[str, ...]] = {
    LLMProvider.OPENAI: ("choices", "0", "delta", "content"),
    LLMProvider.ANTHROPIC: ("delta", "text"),
}

_ANTHROPIC_SKIP_TYPES = {"ping"}


def _build_config(
    url: str,
    provider: LLMProvider,
    auth_token: str | None,
    overrides: dict[str, Any],
) -> HttpNdjsonConfig:
    kwargs: dict[str, Any] = dict(_PROVIDER_HTTP_DEFAULTS[provider])
    if auth_token is not None:
        kwargs["auth_token"] = auth_token
    kwargs.update(overrides)
    return HttpNdjsonConfig.from_url(url, **kwargs)


def _should_skip(provider: LLMProvider, result: ValidationResult) -> bool:
    """Provider-specific filtering applied *after* JSON parsing.

    Anthropic's SSE stream includes ``{"type": "ping"}`` events that
    are not validation failures but also aren't model output; skip
    them silently. All other providers pass through unchanged.
    """
    if provider is not LLMProvider.ANTHROPIC:
        return False
    raw = result.raw
    return isinstance(raw, dict) and raw.get("type") in _ANTHROPIC_SKIP_TYPES


def validate_llm_stream(
    url: str,
    schema: type[BaseModel],
    provider: LLMProvider = LLMProvider.GENERIC_NDJSON,
    auth_token: str | None = None,
    on_error: ErrorStrategy = "collect",
    **config_kwargs: Any,
) -> Iterator[ValidationResult]:
    """Sync iterator of :class:`ValidationResult` over an LLM stream.

    Pre-configures :class:`HttpNdjsonConfig` for the named provider
    (SSE on for OpenAI/Anthropic/generic SSE; raw NDJSON for the
    generic NDJSON shape) and forwards every other keyword argument
    onto the config.

    Args:
        url: Provider streaming endpoint.
        schema: Pydantic model each chunk must validate against.
        provider: Wire format. Defaults to :attr:`LLMProvider.GENERIC_NDJSON`.
        auth_token: Bearer token; sent as ``Authorization: Bearer <token>``.
        on_error: ``StreamValidator`` error strategy (default ``"collect"``).
        **config_kwargs: Extra :class:`HttpNdjsonConfig` overrides
            (timeout_seconds, max_lines, headers, params, …) and
            :class:`StreamValidator` knobs (batch_size, workers, …).
            Validator-level kwargs are forwarded by ``stream_http_ndjson``.

    Yields:
        :class:`ValidationResult` per accepted chunk, with provider-
        specific noise (e.g. Anthropic ``ping`` events) filtered out.
    """
    sv_kwargs = _pop_validator_kwargs(config_kwargs)
    config = _build_config(url, provider, auth_token, config_kwargs)
    for result in stream_http_ndjson(
        config, schema, on_error=on_error, **sv_kwargs
    ):
        if _should_skip(provider, result):
            continue
        yield result


async def avalidate_llm_stream(
    url: str,
    schema: type[BaseModel],
    provider: LLMProvider = LLMProvider.GENERIC_NDJSON,
    auth_token: str | None = None,
    on_error: ErrorStrategy = "collect",
    **config_kwargs: Any,
) -> AsyncIterator[ValidationResult]:
    """Async iterator of :class:`ValidationResult` over an LLM stream.

    See :func:`validate_llm_stream` for argument semantics.
    """
    sv_kwargs = _pop_validator_kwargs(config_kwargs)
    config = _build_config(url, provider, auth_token, config_kwargs)
    async for result in astream_http_ndjson(
        config, schema, on_error=on_error, **sv_kwargs
    ):
        if _should_skip(provider, result):
            continue
        yield result


def extract_content(
    result: ValidationResult,
    provider: LLMProvider,
) -> str | None:
    """Pull the human-readable text fragment from one streamed chunk.

    Walks the provider's content path against ``result.raw`` (a dict).
    Returns ``None`` when the path is missing or the leaf is not a
    string — e.g. OpenAI tool-use chunks, Anthropic ``message_start``
    frames, or generic providers that don't have a known content path.

    For :attr:`LLMProvider.GENERIC_SSE` / :attr:`LLMProvider.GENERIC_NDJSON`
    no content path is registered; the function always returns
    ``None`` (callers should extract the field they care about
    directly from ``result.data`` / ``result.raw``).
    """
    path = _CONTENT_PATHS.get(provider)
    if path is None:
        return None
    cursor: Any = result.raw
    for step in path:
        if isinstance(cursor, list):
            try:
                idx = int(step)
            except ValueError:
                return None
            if idx < 0 or idx >= len(cursor):
                return None
            cursor = cursor[idx]
            continue
        if not isinstance(cursor, dict):
            return None
        if step not in cursor:
            return None
        cursor = cursor[step]
    return cursor if isinstance(cursor, str) else None


_VALIDATOR_KWARGS = {"batch_size", "max_errors", "workers", "use_arrow"}


def _pop_validator_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Split validator kwargs out of the config kwargs in-place.

    ``validate_llm_stream`` accepts a single ``**kwargs`` blob to keep
    the call sites short; this helper removes the validator-level
    knobs so the remaining keys can be passed to
    :class:`HttpNdjsonConfig` without colliding.
    """
    out: dict[str, Any] = {}
    for key in list(kwargs):
        if key in _VALIDATOR_KWARGS:
            out[key] = kwargs.pop(key)
    return out


__all__ = [
    "LLMProvider",
    "avalidate_llm_stream",
    "extract_content",
    "validate_llm_stream",
]

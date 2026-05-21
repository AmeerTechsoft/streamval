"""Unit tests for :mod:`streamval.llm`.

Covers all four :class:`LLMProvider` variants end-to-end via
``pytest-httpx`` mocks, plus the standalone ``extract_content``
helper. No real network, no LLM SDKs.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import BaseModel
from pytest_httpx import HTTPXMock

from streamval import llm
from streamval.llm import LLMProvider

URL = "https://api.example.com/v1/chat/completions"


class Chunk(BaseModel):
    """Permissive schema — LLM chunks vary chunk-to-chunk."""

    id: str | None = None
    type: str | None = None
    choices: list[dict[str, Any]] | None = None
    delta: dict[str, Any] | None = None


def _sse(events: list[dict[str, Any]]) -> bytes:
    parts: list[str] = []
    for ev in events:
        parts.append(f"data: {json.dumps(ev)}\n\n")
    return "".join(parts).encode()


def _ndjson(events: list[dict[str, Any]]) -> bytes:
    return ("\n".join(json.dumps(e) for e in events) + "\n").encode()


class TestOpenAIProvider:
    def test_streams_and_extracts_content(
        self, httpx_mock: HTTPXMock
    ) -> None:
        body = _sse(
            [
                {"choices": [{"delta": {"content": "Hel"}}]},
                {"choices": [{"delta": {"content": "lo "}}]},
                {"choices": [{"delta": {"content": "wor"}}]},
                {"choices": [{"delta": {"content": "ld!"}}]},
            ]
        )
        # SSE [DONE] terminator.
        body += b"data: [DONE]\n\n"
        httpx_mock.add_response(url=URL, content=body)

        results = list(
            llm.validate_llm_stream(
                URL, Chunk, provider=LLMProvider.OPENAI, auth_token="sk-x"
            )
        )
        assert len(results) == 4
        assert all(r.valid for r in results)

        joined = "".join(
            (llm.extract_content(r, LLMProvider.OPENAI) or "")
            for r in results
        )
        assert joined == "Hello world!"

        req = httpx_mock.get_requests()[0]
        assert req.headers["authorization"] == "Bearer sk-x"

    def test_extract_content_returns_none_for_tool_use(self) -> None:
        from streamval.core.result import ValidationResult

        raw = {"choices": [{"delta": {"tool_calls": [{"id": "t1"}]}}]}
        result = ValidationResult(
            row_index=0, raw=raw, valid=True, data=None, errors=[]
        )
        assert llm.extract_content(result, LLMProvider.OPENAI) is None


class TestAnthropicProvider:
    def test_skips_ping_events(self, httpx_mock: HTTPXMock) -> None:
        body = _sse(
            [
                {"type": "message_start", "message": {"id": "m1"}},
                {"type": "ping"},
                {"type": "content_block_delta", "delta": {"text": "Hi"}},
                {"type": "ping"},
                {"type": "content_block_delta", "delta": {"text": "!"}},
                {"type": "message_stop"},
            ]
        )
        httpx_mock.add_response(url=URL, content=body)

        results = list(
            llm.validate_llm_stream(
                URL, Chunk, provider=LLMProvider.ANTHROPIC
            )
        )
        # Two ping frames filtered out; 4 events remain.
        assert [r.raw["type"] for r in results] == [
            "message_start",
            "content_block_delta",
            "content_block_delta",
            "message_stop",
        ]

    def test_extract_content_pulls_delta_text(
        self, httpx_mock: HTTPXMock
    ) -> None:
        body = _sse(
            [
                {"type": "content_block_delta", "delta": {"text": "Hi"}},
                {"type": "content_block_delta", "delta": {"text": " there"}},
            ]
        )
        httpx_mock.add_response(url=URL, content=body)

        results = list(
            llm.validate_llm_stream(
                URL, Chunk, provider=LLMProvider.ANTHROPIC
            )
        )
        joined = "".join(
            (llm.extract_content(r, LLMProvider.ANTHROPIC) or "")
            for r in results
        )
        assert joined == "Hi there"

    def test_extract_content_none_for_non_text_event(self) -> None:
        from streamval.core.result import ValidationResult

        raw = {"type": "message_start", "message": {"id": "m1"}}
        result = ValidationResult(
            row_index=0, raw=raw, valid=True, data=None, errors=[]
        )
        assert llm.extract_content(result, LLMProvider.ANTHROPIC) is None


class TestGenericSSE:
    def test_generic_sse_no_provider_filtering(
        self, httpx_mock: HTTPXMock
    ) -> None:
        body = _sse(
            [
                {"event": "open"},
                {"event": "message", "payload": "hi"},
                {"event": "close"},
            ]
        )
        httpx_mock.add_response(url=URL, content=body)

        results = list(
            llm.validate_llm_stream(
                URL, Chunk, provider=LLMProvider.GENERIC_SSE
            )
        )
        assert len(results) == 3

    def test_extract_content_returns_none(self) -> None:
        from streamval.core.result import ValidationResult

        result = ValidationResult(
            row_index=0,
            raw={"anything": "here"},
            valid=True,
            data=None,
            errors=[],
        )
        assert llm.extract_content(result, LLMProvider.GENERIC_SSE) is None


class TestGenericNDJSON:
    def test_raw_ndjson_no_sse_parsing(
        self, httpx_mock: HTTPXMock
    ) -> None:
        body = _ndjson(
            [
                {"id": "a", "value": 1},
                {"id": "b", "value": 2},
                {"id": "c", "value": 3},
            ]
        )
        httpx_mock.add_response(url=URL, content=body)

        results = list(
            llm.validate_llm_stream(
                URL, Chunk, provider=LLMProvider.GENERIC_NDJSON
            )
        )
        assert [r.raw["id"] for r in results] == ["a", "b", "c"]


class TestKwargForwarding:
    def test_max_lines_forwarded(self, httpx_mock: HTTPXMock) -> None:
        body = _sse(
            [{"choices": [{"delta": {"content": str(i)}}]} for i in range(20)]
        )
        httpx_mock.add_response(url=URL, content=body)

        results = list(
            llm.validate_llm_stream(
                URL, Chunk, provider=LLMProvider.OPENAI, max_lines=5
            )
        )
        assert len(results) == 5

    def test_custom_headers_merged(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=URL, content=b'data: [DONE]\n\n')

        list(
            llm.validate_llm_stream(
                URL,
                Chunk,
                provider=LLMProvider.OPENAI,
                auth_token="t1",
                headers={"OpenAI-Beta": "assistants=v1"},
            )
        )
        req = httpx_mock.get_requests()[0]
        assert req.headers["openai-beta"] == "assistants=v1"
        assert req.headers["authorization"] == "Bearer t1"

    def test_validator_kwargs_dont_leak_to_config(
        self, httpx_mock: HTTPXMock
    ) -> None:
        """``batch_size`` belongs to StreamValidator, not HttpNdjsonConfig."""
        httpx_mock.add_response(
            url=URL, content=_ndjson([{"id": "a"}, {"id": "b"}])
        )

        # If batch_size leaks to HttpNdjsonConfig this raises TypeError.
        results = list(
            llm.validate_llm_stream(
                URL,
                Chunk,
                provider=LLMProvider.GENERIC_NDJSON,
                batch_size=2,
            )
        )
        assert len(results) == 2


class TestExtractContentEdgeCases:
    @pytest.mark.parametrize(
        "provider", [LLMProvider.GENERIC_SSE, LLMProvider.GENERIC_NDJSON]
    )
    def test_generic_providers_have_no_content_path(
        self, provider: LLMProvider
    ) -> None:
        from streamval.core.result import ValidationResult

        result = ValidationResult(
            row_index=0,
            raw={"text": "hello"},
            valid=True,
            data=None,
            errors=[],
        )
        assert llm.extract_content(result, provider) is None

    def test_openai_empty_choices_array(self) -> None:
        from streamval.core.result import ValidationResult

        result = ValidationResult(
            row_index=0,
            raw={"choices": []},
            valid=True,
            data=None,
            errors=[],
        )
        assert llm.extract_content(result, LLMProvider.OPENAI) is None

    def test_openai_non_string_content(self) -> None:
        from streamval.core.result import ValidationResult

        result = ValidationResult(
            row_index=0,
            raw={"choices": [{"delta": {"content": 42}}]},
            valid=True,
            data=None,
            errors=[],
        )
        assert llm.extract_content(result, LLMProvider.OPENAI) is None


def test_namespace_is_importable_via_package() -> None:
    """``from streamval import llm`` must work without naming the module."""
    import streamval

    assert streamval.llm is llm
    assert hasattr(streamval.llm, "validate_llm_stream")
    assert hasattr(streamval.llm, "LLMProvider")

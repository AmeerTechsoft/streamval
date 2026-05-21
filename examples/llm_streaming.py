"""Validate a streaming LLM response against a Pydantic schema.

The example spins up a small local HTTP server in a background thread
that mimics either an OpenAI- or Anthropic-style Server-Sent Events
stream, then streams + validates the response with ``streamval.llm``.
No real API keys, no paid services — runs entirely offline.

Run with:

    pip install -e ".[http]"
    python examples/llm_streaming.py
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from pydantic import BaseModel

from streamval import llm


class Chunk(BaseModel):
    """Permissive schema — LLM chunks vary chunk-to-chunk."""

    id: str | None = None
    type: str | None = None
    choices: list[dict[str, Any]] | None = None
    delta: dict[str, Any] | None = None


# OpenAI-style: chat.completion.chunk frames with choices[0].delta.content.
_OPENAI_TEXT = "Hello from a streaming LLM!"
_OPENAI_CHUNKS = [
    {"id": "x", "choices": [{"delta": {"content": ch}}]}
    for ch in _OPENAI_TEXT
]

# Anthropic-style: message_start / ping / content_block_delta / message_stop.
# The ping events are interleaved so the example demonstrates the preset
# filtering them out automatically.
_ANTHROPIC_TEXT = "Hi! This is Anthropic-style."


def _anthropic_chunks() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = [
        {"type": "message_start", "message": {"id": "m"}}
    ]
    for i, ch in enumerate(_ANTHROPIC_TEXT):
        events.append(
            {"type": "content_block_delta", "delta": {"text": ch}}
        )
        if i % 4 == 3:
            events.append({"type": "ping"})
    events.append({"type": "message_stop"})
    return events


_ANTHROPIC_CHUNKS = _anthropic_chunks()


def _sse_body(events: list[dict[str, Any]]) -> bytes:
    parts = [f"data: {json.dumps(e)}\n\n" for e in events]
    parts.append("data: [DONE]\n\n")
    return "".join(parts).encode()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 -- BaseHTTPRequestHandler API
        if self.path.endswith("/openai"):
            body = _sse_body(_OPENAI_CHUNKS)
        elif self.path.endswith("/anthropic"):
            body = _sse_body(_ANTHROPIC_CHUNKS)
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # silence stderr
        pass


def _start_server() -> tuple[ThreadingHTTPServer, threading.Thread, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{port}"


def main() -> None:
    server, thread, base = _start_server()
    try:
        # Give the server a beat to bind.
        time.sleep(0.05)

        print(f"# OpenAI-style stream from {base}/openai")
        text: list[str] = []
        for result in llm.validate_llm_stream(
            f"{base}/openai",
            Chunk,
            provider=llm.LLMProvider.OPENAI,
            on_error="skip",
        ):
            chunk = llm.extract_content(result, llm.LLMProvider.OPENAI)
            if chunk:
                text.append(chunk)
        print("  reassembled:", "".join(text))

        print(f"\n# Anthropic-style stream from {base}/anthropic")
        text = []
        for result in llm.validate_llm_stream(
            f"{base}/anthropic",
            Chunk,
            provider=llm.LLMProvider.ANTHROPIC,
            on_error="skip",
        ):
            chunk = llm.extract_content(result, llm.LLMProvider.ANTHROPIC)
            if chunk:
                text.append(chunk)
        print("  reassembled:", "".join(text))
    finally:
        server.shutdown()
        thread.join(timeout=1.0)


if __name__ == "__main__":
    main()

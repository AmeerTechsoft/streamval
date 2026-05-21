"""Stream-validate an HTTP NDJSON endpoint.

Runs entirely offline against a local stdlib HTTP server so the
example doesn't depend on any third-party endpoint or API key. Swap
the URL for your own to point it at a real service.

Run with:

    pip install -e ".[http]"
    python examples/http_ndjson_basic.py
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from pydantic import BaseModel

from streamval import stream_http_ndjson


class Event(BaseModel):
    id: int
    name: str
    score: float


_PAYLOAD = [
    {"id": i, "name": f"event-{i}", "score": round(i / 7, 3)}
    for i in range(50)
]
# Inject one invalid row to demonstrate how validation reports it.
_PAYLOAD.append({"id": "not-an-int", "name": "broken", "score": "nope"})


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 -- stdlib API
        body = (
            "\n".join(json.dumps(r) for r in _PAYLOAD) + "\n"
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # silence
        return


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.05)
    url = f"http://127.0.0.1:{port}/events"

    try:
        print(f"# Streaming from {url}")
        valid = invalid = 0
        for result in stream_http_ndjson(url, Event, on_error="collect"):
            if result.valid:
                valid += 1
            else:
                invalid += 1
                print(
                    f"  row {result.row_index}: invalid "
                    f"({len(result.errors)} field errors) - "
                    f"first: {result.errors[0].field}"
                )
        print(f"\n# Done. {valid} valid, {invalid} invalid rows.")
    finally:
        server.shutdown()
        thread.join(timeout=1.0)


if __name__ == "__main__":
    main()

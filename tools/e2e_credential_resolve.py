#!/usr/bin/env python3
"""Local-only credential.resolve stand-in for `make test-e2e`.

feat/ANSTRAT-1803 exposes the EP completion callback but not
`/api/execution_plane/v1/credentials/resolve`. The EP worker still needs a
correlated token when it talks to Kind. This process returns the dispatcher
ServiceAccount token written by `tools/deploy_kind_execution_target.py`.
Remove it once AO serves that callback on devel.
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

TOKEN_PATH = Path(os.environ.get("E2E_KIND_TOKEN_PATH", "/run/e2e-secrets/kind-dispatcher.token"))
PORT = int(os.environ.get("E2E_CREDENTIAL_RESOLVE_PORT", "8080"))


class Handler(BaseHTTPRequestHandler):
    """Serve health checks and credential.resolve responses."""

    def log_message(self, fmt: str, *args: object) -> None:
        print(fmt % args, flush=True)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") == "/healthz":
            self._write(200, b"ok", content_type="text/plain")
            return
        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") != "/credentials/resolve":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self.send_error(400, "invalid JSON")
            return
        message_id = body.get("message_id") if isinstance(body, dict) else None
        if not isinstance(message_id, str) or not message_id:
            self.send_error(400, "message_id is required")
            return
        if not TOKEN_PATH.is_file():
            self.send_error(503, "kind dispatcher token is not ready")
            return
        token = TOKEN_PATH.read_text(encoding="utf-8").strip()
        if not token:
            self.send_error(503, "kind dispatcher token is empty")
            return
        payload = json.dumps({"message_id": message_id, "credential": token}).encode("utf-8")
        self._write(200, payload, content_type="application/json")

    def _write(self, status: int, body: bytes, *, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

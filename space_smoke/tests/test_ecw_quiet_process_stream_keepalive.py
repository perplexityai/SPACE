from __future__ import annotations

import base64
import json
import threading
import time
import types
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import cast
from unittest import mock

from space_smoke.adapters import EcwConfig, _SdkSandbox
from space_smoke.capabilities import EcwSandbox
from space_smoke.fixtures import Ecw
from space_smoke.runtime import InvariantViolation, ResourceScope, ScenarioContext
from space_smoke.scenarios import ecw_quiet_process_stream_keepalive as scenario

QUIET_SECONDS = 1.5


def _envelope(message: object, flags: int = 0) -> bytes:
    payload = json.dumps(message).encode()
    return bytes([flags]) + len(payload).to_bytes(4, "big") + payload


@contextmanager
def _envd(*, keepalive_every: float | None) -> Iterator[tuple[int, list[str]]]:
    """Serve one quiet Process.Start stream, optionally with keepalives."""

    intervals: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: object) -> None:
            pass

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers["Content-Length"]))
            intervals.append(self.headers["Keepalive-Ping-Interval"])
            self.send_response(200)
            self.send_header("Content-Type", "application/connect+json")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            try:
                self._send({"event": {"start": {"pid": 7}}})
                deadline = time.monotonic() + QUIET_SECONDS
                while time.monotonic() < deadline:
                    time.sleep(keepalive_every or QUIET_SECONDS)
                    if keepalive_every is not None:
                        self._send({"event": {"keepalive": {}}})
                stdout = base64.b64encode(b"done").decode()
                self._send({"event": {"data": {"stdout": stdout}}})
                self._send({"event": {"end": {"exited": True}}})
                self._send({}, flags=0x02)
                self.wfile.write(b"0\r\n\r\n")
            except OSError:
                pass

        def _send(self, message: object, flags: int = 0) -> None:
            chunk = _envelope(message, flags)
            self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
            self.wfile.flush()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield server.server_address[1], intervals
    finally:
        server.shutdown()
        server.server_close()


def _run(port: int) -> None:
    sdk = types.SimpleNamespace(
        sandbox_id="sandbox-quiet",
        envd_api_url=f"http://127.0.0.1:{port}",
        connection_config=types.SimpleNamespace(sandbox_headers={}),
    )
    sandbox = _SdkSandbox(sdk, EcwConfig(api_key="key"))
    test = Ecw(ScenarioContext(ResourceScope(), {}), cast(EcwSandbox, sandbox))
    with mock.patch.multiple(scenario, PROXY_IDLE_TIMEOUT_SECONDS=0.5, QUIET_SECONDS=2):
        scenario.quiet_process_stream_keepalive.verify(test)


class QuietProcessStreamKeepaliveTests(unittest.TestCase):
    def test_keepalives_hold_a_quiet_stream_open_through_the_idle_proxy(self) -> None:
        with _envd(keepalive_every=0.1) as (port, intervals):
            _run(port)
        self.assertEqual(intervals, ["5"])

    def test_a_silent_stream_is_dropped_by_the_idle_proxy(self) -> None:
        with (
            _envd(keepalive_every=None) as (port, _),
            self.assertRaises(InvariantViolation),
        ):
            _run(port)

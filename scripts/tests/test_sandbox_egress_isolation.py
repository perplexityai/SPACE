"""The egress-isolation smoke test against a command stream that never ends.

Run from the repo root:

    python -m pytest scripts/tests/test_sandbox_egress_isolation.py -q

A fake ECW API and envd accept the sandbox, the probe upload and the process
Start, then keep the stream open with keepalives and never send an end event.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import struct
import sys
import threading
import time
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import e2b

SCRIPT = pathlib.Path(__file__).resolve().parent / "test-sandbox-egress-isolation.py"


def _load_script():
    # The script's Sandbox is e2b_code_interpreter's, which only adds code
    # execution to e2b's; the smoke uses none of it.
    sys.modules.setdefault("e2b_code_interpreter", types.SimpleNamespace(Sandbox=e2b.Sandbox))
    spec = importlib.util.spec_from_file_location("sandbox_egress_isolation", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _envelope(message: dict) -> bytes:
    data = json.dumps(message).encode()
    return struct.pack(">BI", 0, len(data)) + data


class _StallingEcw(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    killed = threading.Event()

    def log_message(self, *_args) -> None:
        pass

    def _json(self, status: int, body: object) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path.startswith("/sandboxes"):
            self._json(201, {"sandboxID": "stalled", "templateID": "seed", "clientID": "c", "envdVersion": "0.2.4"})
        elif self.path.startswith("/files"):
            self._json(200, [{"name": "probe", "type": "file", "path": "/tmp/probe"}])
        else:
            self.send_response(200)
            self.send_header("Content-Type", "application/connect+json")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            message = _envelope({"event": {"start": {"pid": 1}}})
            try:
                while not self.killed.is_set():
                    self.wfile.write(f"{len(message):x}\r\n".encode() + message + b"\r\n")
                    self.wfile.flush()
                    message = _envelope({"event": {"keepalive": {}}})
                    time.sleep(0.2)
            except OSError:
                pass

    def do_DELETE(self) -> None:
        self.killed.set()
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()


def test_stalled_command_stream_hard_fails_and_kills_the_sandbox(monkeypatch, capsys) -> None:
    smoke = _load_script()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StallingEcw)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    for name, value in {
        "E2B_API_URL": url,
        "E2B_SANDBOX_URL": url,
        "E2B_API_KEY": "e2b_test",
        "E2B_DOMAIN": "stall.invalid",
        "EGRESS_POD_IPS": "10.0.0.1",
    }.items():
        monkeypatch.setenv(name, value)
    deadline = smoke.wall_clock_deadline
    monkeypatch.setattr(smoke, "wall_clock_deadline", lambda _seconds: deadline(1))

    started = time.monotonic()
    try:
        exit_code = smoke.main()
    finally:
        server.shutdown()

    assert exit_code == smoke.EXIT_HARD_FAIL
    assert _StallingEcw.killed.is_set()
    assert time.monotonic() - started < 30
    assert "CommandDeadlineExceeded" in capsys.readouterr().out

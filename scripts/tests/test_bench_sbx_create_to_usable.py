"""End-to-end test of bench-sbx-create-to-usable.py against a fake SPACE API."""

from __future__ import annotations

import http.server
import json
import pathlib
import re
import subprocess
import sys
import threading
import uuid

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "benchmarks" / "bench-sbx-create-to-usable.py"


def _fake_api(keep_deleted: bool):
    """Sandboxes run commands only once they have been probed twice after create or wake."""
    states: dict[str, str] = {}
    probes: dict[str, int] = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, body, status=200):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def op(self, sid):
            return self.reply({"sandbox_id": f"sbx_{sid}", "operation_id": f"op_{uuid.uuid4()}"}, 202)

        def do_GET(self):
            if self.path.startswith("/v1/operations/"):
                return self.reply({"state": "succeeded"})
            if self.path.startswith("/v1/sessions/"):
                code = 0 if self.path.endswith("ready/events") else 1
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(b"data: " + json.dumps({"type": "process_exited", "exit_code": code}).encode() + b"\n\n")
                return None
            sid = self.path.rsplit("sbx_", 1)[1]
            if states.get(sid, "deleted") == "deleted":
                return self.reply({"error": "not found"}, 404)
            return self.reply({"state": states[sid]})

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            if self.path == "/v1/sandboxes":
                sid = str(uuid.uuid4())
                states[sid], probes[sid] = "running", 0
                return self.op(sid)
            sid, action = re.fullmatch(r"/v1/sandboxes/sbx_([0-9a-f-]+)(/commands|:suspend|:wake)", self.path).groups()
            if action == "/commands":
                probes[sid] += 1
                session = "ready" if probes[sid] > 2 else "booting"
                return self.reply({"attach": {"events_url": f"/v1/sessions/{session}/events", "token": "t"}}, 201)
            states[sid] = "suspended" if action == ":suspend" else "running"
            probes[sid] = 0
            return self.op(sid)

        def do_DELETE(self):
            sid = self.path.rsplit("sbx_", 1)[1]
            if not keep_deleted:
                states[sid] = "deleted"
            return self.op(sid)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _run(tmp_path, keep_deleted=False, timeout="30"):
    server = _fake_api(keep_deleted)
    url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "--api-url", url, "--data-plane-url", url, "--runs", "3",
             "--poll-secs", "0.01", "--timeout", timeout, "--output-dir", str(tmp_path)],
            env={"SPACE_API_TOKEN": "secret-token"}, capture_output=True, text=True, timeout=60,
        )
    finally:
        server.shutdown()
    return proc, json.loads((tmp_path / "summary.json").read_text())


def test_measures_cold_and_warm_until_usable(tmp_path):
    proc, report = _run(tmp_path)
    assert proc.returncode == 0, proc.stderr
    summary = report["summary"]
    assert (summary["succeeded"], summary["deleted"], summary["leftover_sandboxes"]) == (3, 3, [])
    for metric in ("cold_op_ms", "cold_usable_ms", "warm_op_ms", "warm_usable_ms"):
        assert summary[metric]["count"] == 3
    rows = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    assert all(row["cold_usable_ms"] >= row["cold_op_ms"] for row in rows)
    assert "secret-token" not in (tmp_path / "summary.json").read_text()


def test_fails_when_a_sandbox_is_left_behind(tmp_path):
    proc, report = _run(tmp_path, keep_deleted=True, timeout="1")
    assert proc.returncode == 1
    assert report["summary"]["deleted"] == 0
    assert len(report["summary"]["leftover_sandboxes"]) == 3

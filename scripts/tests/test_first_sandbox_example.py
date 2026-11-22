"""Regression checks for the actual dependency-free tutorial executable."""

from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[2] / "examples" / "first_sandbox.py"
spec = importlib.util.spec_from_file_location("first_sandbox_example", SOURCE)
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)

GOOD = b'data: {"type":"process_exited","exit_code":0}\n\n'


@contextmanager
def server(route):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            calls.append((self.command, self.path, dict(self.headers), body))
            status, headers, payload = route(self.command, self.path, body)
            if isinstance(payload, dict):
                payload = json.dumps(payload).encode()
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = do_DELETE = handle_request

        def log_message(self, *_):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=httpd.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", calls
    finally:
        httpd.shutdown()
        httpd.server_close()
        worker.join()


class FirstSandboxExampleTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.directory = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.stdout = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
        self.stderr = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
        self.stack.enter_context(redirect_stdout(self.stdout))
        self.stack.enter_context(redirect_stderr(self.stderr))
        self.config = {
            "api_url": "http://127.0.0.1:8080",
            "data_plane_url": "http://127.0.0.1:8090",
            "token": "management-secret",
            "template": "seed:latest",
        }
        self.state = {
            "api_url": self.config["api_url"],
            "data_plane_url": self.config["data_plane_url"],
            "requests": {}, "transcripts": [],
        }

    def command(self, url="/v1/sessions/proc_test/events"):
        return {"session_id": "proc_test", "process_id": "proc_test",
                "attach": {"token": "attachment-secret", "events_url": url}}

    def saved(self):
        return json.loads((self.directory / "state.json").read_text())

    def test_missing_token_and_invalid_origins(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "SPACE_API_TOKEN"):
                example.configuration()
        for origin in ("http://user:password@host", "file:///tmp", "http://host/path",
                       "http://host?query=1", "http://host#fragment", "http://host:bad"):
            with self.subTest(origin=origin), patch.dict(
                os.environ, {"SPACE_API_TOKEN": "token", "SPACE_API_URL": origin}, clear=True
            ):
                with self.assertRaises(ValueError):
                    example.configuration()

    def test_missing_management_token_or_external_api_path_never_opens_network(self):
        with patch.object(example.HTTP, "open") as opened:
            with self.assertRaises(ValueError):
                example.api({**self.config, "token": ""}, "GET", "/v1/sandboxes")
            with self.assertRaises(ValueError):
                example.api(self.config, "GET", "//untrusted.invalid/path")
            opened.assert_not_called()

    def test_remote_http_origins_are_rejected_before_network(self):
        for key in ("SPACE_API_URL", "SPACE_DATA_PLANE_URL"):
            for origin in ("http://remote.invalid", "http://10.0.0.1:8080", "http://127.0.0.1.attacker.invalid"):
                with self.subTest(key=key, origin=origin), patch.dict(
                    os.environ, {"SPACE_API_TOKEN": "token", key: origin}, clear=True
                ), patch.object(example.HTTP, "open") as opened:
                    with self.assertRaisesRegex(ValueError, "requires HTTPS"):
                        example.configuration()
                    opened.assert_not_called()
            for origin in ("https://remote.invalid", "http://127.0.0.1:8080", "http://localhost:8090", "http://[::1]:8080"):
                with self.subTest(key=key, origin=origin), patch.dict(
                    os.environ, {"SPACE_API_TOKEN": "token", key: origin}, clear=True
                ):
                    example.configuration()

    def test_missing_ids_do_not_construct_requests(self):
        with patch.object(example, "api") as api:
            for operation in (example.run_command, example.delete_sandbox):
                with self.assertRaises(ValueError):
                    operation(self.config, self.directory, self.state)
            api.assert_not_called()
            example.inspect_run(self.config, self.state)
            api.assert_called_once_with(self.config, "GET", "/v1/sandboxes?limit=10")

    def test_partial_create_keeps_valid_id(self):
        with patch.object(example, "api", return_value={"sandbox_id": "sbx_test"}):
            with self.assertRaisesRegex(ValueError, "operation_id"):
                example.create_sandbox(self.config, self.directory, self.state)
        self.assertEqual(self.saved()["sandbox_id"], "sbx_test")
        self.assertIn("create", self.saved()["requests"])

    def test_failed_create_clears_stale_ids_retains_request_and_never_retries(self):
        self.state.update(sandbox_id="sbx_old", operation_id="op_old")
        with patch.object(example, "api", side_effect=TimeoutError("unknown outcome")) as api:
            with self.assertRaises(TimeoutError):
                example.create_sandbox(self.config, self.directory, self.state)
            saved = self.saved()
            self.assertNotIn("sandbox_id", saved)
            self.assertNotIn("operation_id", saved)
            self.assertEqual(saved["requests"]["create"]["body"]["template"], "seed:latest")
            with self.assertRaises(ValueError):
                example.create_sandbox(self.config, self.directory, self.state)
            api.assert_called_once()

    def test_failed_command_clears_stale_process_ids_retains_request(self):
        self.state.update(sandbox_id="sbx_test", session_id="proc_old", process_id="proc_old")
        with patch.object(example, "api", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                example.run_command(self.config, self.directory, self.state)
        self.assertNotIn("session_id", self.saved())
        self.assertNotIn("process_id", self.saved())
        self.assertEqual(self.saved()["requests"]["command"]["body"], example.COMMAND)

    def test_saved_request_is_immutable_and_changed_body_cannot_reuse_key(self):
        body = {"command": ["original"]}
        with patch.object(example, "api", side_effect=TimeoutError) as api:
            with self.assertRaises(TimeoutError):
                example.mutation(self.config, self.directory, self.state, "test", "POST", "/test", body)
            key = self.saved()["requests"]["test"]["key"]
            body["command"][0] = "changed"
            with self.assertRaises(ValueError):
                example.mutation(self.config, self.directory, self.state, "test", "POST", "/test", body, retry=True)
            self.assertEqual(self.state["requests"]["test"]["body"], {"command": ["original"]})
            self.assertEqual(self.saved()["requests"]["test"]["key"], key)
            api.assert_called_once()

    def test_untrusted_attachment_destinations_never_receive_token(self):
        for url in ("http://untrusted.invalid/v1/sessions/proc_test/events",
                    "http://127.0.0.1:8080/v1/sessions/proc_test/events",
                    "//untrusted.invalid/v1/sessions/proc_test/events",
                    "/v1/sessions/proc_other/events", "/v1/sessions/proc_test/events?x=1"):
            with self.subTest(url=url):
                state = {"sandbox_id": "sbx_test", "requests": {}, "transcripts": []}
                with patch.object(example, "api", return_value=self.command(url)), patch.object(example.HTTP, "open") as opened:
                    with self.assertRaises(ValueError):
                        example.run_command(self.config, self.directory, state)
                    opened.assert_not_called()
                    self.assertEqual(state["process_id"], "proc_test")

    def test_relative_and_exact_absolute_events_urls_succeed_without_saving_tokens(self):
        for url in ("/v1/sessions/proc_test/events",
                    "http://127.0.0.1:8090/v1/sessions/proc_test/events"):
            state = {"sandbox_id": "sbx_test", "requests": {}, "transcripts": []}
            with patch.object(example, "api", return_value=self.command(url)), patch.object(
                example.HTTP, "open", return_value=io.BytesIO(GOOD)
            ) as opened:
                example.run_command(self.config, self.directory, state)
                request = opened.call_args.args[0]
                self.assertEqual(request.get_header("Authorization"), "Bearer attachment-secret")
                self.assertEqual(request.full_url, self.config["data_plane_url"] + "/v1/sessions/proc_test/events")
            self.assertEqual((self.directory / state["transcripts"][0]).read_bytes(), GOOD)
            saved = (self.directory / "state.json").read_text()
            self.assertNotIn("attachment-secret", saved)
            self.assertNotIn("management-secret", saved)

    def test_missing_attachment_token_keeps_ids_without_opening_stream(self):
        self.state["sandbox_id"] = "sbx_test"
        command = self.command()
        command["attach"]["token"] = ""
        with patch.object(example, "api", return_value=command), patch.object(example.HTTP, "open") as opened:
            with self.assertRaisesRegex(ValueError, "token is missing"):
                example.run_command(self.config, self.directory, self.state)
            opened.assert_not_called()
        self.assertEqual(self.saved()["process_id"], "proc_test")
        self.assertEqual(self.saved()["transcripts"], [])

    def test_sse_comments_heartbeats_named_events_crlf_and_multiline_data(self):
        stream = (
            b": keepalive\r\n\r\nevent: stdout\r\nid: 1\r\n"
            b'data: {"type":"stdout",\r\ndata: "data_base64":"SGVsbG8K"}\r\n\r\n'
            b'data: {"type":"stderr","data_base64":"d2Fybgo="}\n\n' + GOOD
        )
        transcript, stdout, stderr = io.BytesIO(), io.BytesIO(), io.BytesIO()
        example.consume_events(io.BytesIO(stream), transcript, stdout, stderr)
        self.assertEqual(stdout.getvalue(), b"Hello\n")
        self.assertEqual(stderr.getvalue(), b"warn\n")
        self.assertEqual(transcript.getvalue(), stream)

    def test_nonzero_error_resync_invalid_and_incomplete_streams_fail(self):
        bad = [
            b'data: {"type":"process_exited","exit_code":1}\n\n',
            b'data: {"type":"process_exited","exit_code":false}\n\n',
            b'data: {"type":"error"}\n\n',
            b'data: {"type":"resync_required"}\n\n',
            GOOD + b'data: {"type":"resync_required"}\n\n',
            b'data: {"type":"process_exited","exit_code":0,"output_truncated":true}\n\n',
            b'data: {"type":"stdout","data_base64":"bad!"}\n\n',
            b'data: {"type":"stdout","data_base64":"eA=="}\n\n',
            GOOD.rstrip(b"\n"),
            GOOD + b"data: {",
            b'data: []\n\n', b"data: not-json\n\n", b": heartbeat\n\n",
        ]
        for stream in bad:
            with self.subTest(stream=stream), self.assertRaises((ValueError, RuntimeError)):
                example.consume_events(io.BytesIO(stream), io.BytesIO(), io.BytesIO(), io.BytesIO())

    def test_failed_stream_retains_transcript_and_command_request(self):
        self.state["sandbox_id"] = "sbx_test"
        stream = b'data: {"type":"stdout","data_base64":"eA=="}\n\n'
        with patch.object(example, "api", return_value=self.command()), patch.object(
            example.HTTP, "open", return_value=io.BytesIO(stream)
        ):
            with self.assertRaises(RuntimeError):
                example.run_command(self.config, self.directory, self.state)
        saved = self.saved()
        self.assertEqual((self.directory / saved["transcripts"][0]).read_bytes(), stream)
        self.assertIn("command", saved["requests"])

    def test_terminal_and_parked_states_stop_without_sleeping(self):
        for state in ("failed", "canceled", "expired", "deleted", "deleting", "stopped", "paused", "suspended"):
            with self.subTest(state=state), patch.object(example, "api", return_value={"state": state}), patch.object(example.time, "sleep") as sleep:
                with self.assertRaises(RuntimeError):
                    example.wait_for_state(self.config, "/resource", "running")
                sleep.assert_not_called()
        with patch.object(example, "api", return_value={"state": "running"}):
            example.wait_for_state(self.config, "/resource", "running")

    def test_poll_deadline_bounds_requests_and_does_not_retry_mutations(self):
        with patch.object(example, "STATE_TIMEOUT", 0.01), patch.object(
            example.time, "monotonic", side_effect=[0, 0, 0, 1]
        ), patch.object(example.time, "sleep"), patch.object(example, "api", return_value={"state": "starting"}) as api:
            with self.assertRaises(TimeoutError):
                example.wait_for_state(self.config, "/resource", "running")
            api.assert_called_once_with(self.config, "GET", "/resource", timeout=0.01)

    def test_whole_exchange_deadline_interrupts_a_blocking_stream(self):
        class SlowStream(io.BytesIO):
            def __next__(self):
                time.sleep(2)
                return super().__next__()
        self.state["sandbox_id"] = "sbx_test"
        with patch.object(example, "STREAM_TIMEOUT", 0.02), patch.object(
            example, "api", return_value=self.command()
        ), patch.object(example.HTTP, "open", return_value=SlowStream(GOOD)):
            start = time.monotonic()
            with self.assertRaises(TimeoutError):
                example.run_command(self.config, self.directory, self.state)
            self.assertLess(time.monotonic() - start, 1)
        self.assertEqual(len(self.saved()["transcripts"]), 1)

    def test_delete_error_is_not_success_and_explicit_retry_reuses_key(self):
        self.state["sandbox_id"] = "sbx_test"
        with patch.object(example, "api", side_effect=RuntimeError("HTTP 404")) as api, patch.object(example, "wait_for_state") as wait:
            with self.assertRaisesRegex(RuntimeError, "404"):
                example.delete_sandbox(self.config, self.directory, self.state)
            key = self.saved()["requests"]["delete"]["key"]
            with self.assertRaises(ValueError):
                example.delete_sandbox(self.config, self.directory, self.state)
            with self.assertRaisesRegex(RuntimeError, "404"):
                example.delete_sandbox(self.config, self.directory, self.state, retry=True)
            self.assertEqual(api.call_count, 2)
            self.assertEqual(api.call_args.kwargs["key"], key)
            self.assertNotIn("deleted", self.saved())
            wait.assert_not_called()

    def test_delete_poll_resume_and_all_transcript_cleanup(self):
        self.state["sandbox_id"] = "sbx_test"
        for name in ("events-one.sse", "events-two.sse"):
            (self.directory / name).write_bytes(GOOD)
            self.state["transcripts"].append(name)
        with patch.object(example, "api", return_value={"operation_id": "op_delete"}) as api, patch.object(
            example, "wait_for_state", side_effect=[TimeoutError, None]
        ):
            with self.assertRaises(TimeoutError):
                example.delete_sandbox(self.config, self.directory, self.state)
            self.assertEqual(len(self.saved()["transcripts"]), 2)
            example.delete_sandbox(self.config, self.directory, self.state)
            api.assert_called_once()
        self.assertTrue(self.saved()["deleted"])
        self.assertEqual(self.saved()["transcripts"], [])
        self.assertEqual(list(self.directory.glob("events-*.sse")), [])

    def test_partial_delete_keeps_valid_create_state(self):
        self.state.update(sandbox_id="sbx_test", operation_id="op_create")
        with patch.object(example, "api", return_value={}):
            with self.assertRaisesRegex(ValueError, "delete_operation_id"):
                example.delete_sandbox(self.config, self.directory, self.state)
        self.assertEqual(self.saved()["sandbox_id"], "sbx_test")
        self.assertEqual(self.saved()["operation_id"], "op_create")
        self.assertNotIn("deleted", self.saved())

    def test_real_http_errors_preserve_request_and_do_not_imply_success(self):
        for method, code, operation in (("POST", 500, example.create_sandbox),
                                        ("DELETE", 404, example.delete_sandbox)):
            state = {**self.state, "sandbox_id": "sbx_test", "requests": {}, "transcripts": []}
            with self.subTest(method=method), server(
                lambda *_: (code, {}, {"error": {"code": "fixture_failure"}})
            ) as (origin, calls):
                with self.assertRaisesRegex(RuntimeError, f"HTTP {code}.*fixture_failure"):
                    operation({**self.config, "api_url": origin}, self.directory, state)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0], method)
            self.assertTrue(self.saved()["requests"])
            self.assertNotIn("deleted", self.saved())

    def test_cleanup_rejects_paths_outside_run_directory(self):
        self.state["transcripts"] = ["../events-secret.sse"]
        with self.assertRaises(ValueError):
            example.cleanup_transcripts(self.directory, self.state)

    def test_redirects_never_forward_management_or_attachment_tokens(self):
        with server(lambda *_: (200, {}, {})) as (sink, leaked):
            for code in (301, 302, 303, 307, 308):
                with self.subTest(code=code), server(
                    lambda *_: (code, {"Location": sink + "/capture"}, b"redirect")
                ) as (origin, requests):
                    config = {**self.config, "api_url": origin, "data_plane_url": origin}
                    with self.assertRaisesRegex(RuntimeError, f"HTTP {code}"):
                        example.api(config, "GET", "/redirect")
                    state = {"sandbox_id": "sbx_test", "requests": {}, "transcripts": []}
                    with patch.object(example, "api", return_value=self.command()):
                        with self.assertRaises(example.HTTPError):
                            example.run_command(config, self.directory, state)
                    self.assertEqual(len(requests), 2)
                    self.assertEqual(requests[0][2]["Authorization"], "Bearer management-secret")
                    self.assertEqual(requests[1][2]["Authorization"], "Bearer attachment-secret")
                    self.assertEqual(leaked, [])

    def test_environment_proxies_are_ignored(self):
        with server(lambda *_: (200, {}, {})) as (proxy, leaked), server(
            lambda *_: (200, {}, {"data": []})
        ) as (origin, direct):
            with patch.dict(os.environ, {
                "http_proxy": proxy, "https_proxy": proxy, "HTTP_PROXY": proxy,
                "HTTPS_PROXY": proxy, "ALL_PROXY": proxy, "NO_PROXY": "", "no_proxy": "",
            }):
                example.api({**self.config, "api_url": origin}, "GET", "/test")
            self.assertEqual(len(direct), 1)
            self.assertEqual(leaked, [])

    def test_interruption_never_reaches_delete_confirmation(self):
        for failure in (RuntimeError("stream failed"), KeyboardInterrupt()):
            with self.subTest(failure=failure), patch.object(
                example, "configuration", return_value=self.config
            ), patch.object(example.tempfile, "mkdtemp", return_value=str(self.directory)), patch.object(
                example, "api", side_effect=[
                    {"sandbox_id": "sbx_test", "operation_id": "op_create"},
                    {"state": "succeeded"}, {"state": "running"}, {"data": []},
                ]
            ), patch.object(example, "run_command", side_effect=failure), patch.object(
                example, "confirm_delete"
            ) as deletion:
                self.assertNotEqual(example.main([]), 0)
                deletion.assert_not_called()
                self.assertEqual(self.saved()["sandbox_id"], "sbx_test")

    def test_inspection_rejects_mismatched_origins(self):
        example.save(self.directory, self.state)
        with patch.object(example, "configuration", return_value={**self.config, "api_url": "http://other.invalid"}), patch.object(example, "api") as api:
            self.assertEqual(example.main(["inspect", str(self.directory)]), 1)
            api.assert_not_called()

    def test_end_to_end_example_against_local_fixture(self):
        def route(method, path, body):
            if method == "POST" and path == "/v1/sandboxes":
                return 202, {}, {"sandbox_id": "sbx_test", "operation_id": "op_create"}
            if method == "POST":
                self.assertEqual(json.loads(body), example.COMMAND)
                return 201, {}, self.command()
            if method == "DELETE":
                return 202, {}, {"operation_id": "op_delete"}
            if path.startswith("/v1/operations/"):
                return 200, {}, {"state": "succeeded"}
            if path == "/v1/sandboxes/sbx_test":
                return 200, {}, {"state": "running"}
            if path.startswith("/v1/sessions/"):
                return 200, {"Content-Type": "text/event-stream"}, (
                    b'data: {"type":"stdout","data_base64":"SGVsbG8gZnJvbSBTUEFDRQo="}\n\n' + GOOD
                )
            return 200, {}, {"data": [], "next_cursor": None}

        with server(route) as (origin, requests), patch.object(
            example, "configuration", return_value={**self.config, "api_url": origin, "data_plane_url": origin}
        ), patch.object(example.tempfile, "mkdtemp", return_value=str(self.directory)), patch(
            "builtins.input", return_value="y"
        ):
            self.assertEqual(example.main([]), 0)
        self.stdout.flush()
        self.assertIn(b"Hello from SPACE\n", self.stdout.buffer.getvalue())
        self.assertTrue(self.saved()["deleted"])
        self.assertEqual(self.saved()["transcripts"], [])
        for _, path, headers, _ in requests:
            expected = "attachment-secret" if path.startswith("/v1/sessions/") else "management-secret"
            self.assertEqual(headers["Authorization"], f"Bearer {expected}")

    def test_end_to_end_stream_failure_keeps_state_without_prompt_or_delete(self):
        stream = b'data: {"type":"stdout","data_base64":"eA=="}\n\n'

        def route(method, path, body):
            if method == "POST" and path == "/v1/sandboxes":
                return 202, {}, {"sandbox_id": "sbx_test", "operation_id": "op_create"}
            if method == "POST":
                return 201, {}, self.command()
            if path.startswith("/v1/operations/"):
                return 200, {}, {"state": "succeeded"}
            if path == "/v1/sandboxes/sbx_test":
                return 200, {}, {"state": "running"}
            if path.startswith("/v1/sessions/"):
                return 200, {"Content-Type": "text/event-stream"}, stream
            return 200, {}, {"data": []}

        with server(route) as (origin, requests), patch.dict(os.environ, {
            "SPACE_API_TOKEN": "management-secret", "SPACE_API_URL": origin,
            "SPACE_DATA_PLANE_URL": origin,
        }, clear=True), patch.object(
            example.tempfile, "mkdtemp", return_value=str(self.directory)
        ), patch("builtins.input") as prompt:
            self.assertEqual(example.main([]), 1)
            prompt.assert_not_called()
        saved = self.saved()
        self.assertEqual(saved["sandbox_id"], "sbx_test")
        self.assertEqual(saved["process_id"], "proc_test")
        self.assertIn("command", saved["requests"])
        self.assertNotIn("delete", saved["requests"])
        self.assertNotIn("deleted", saved)
        self.assertEqual((self.directory / saved["transcripts"][0]).read_bytes(), stream)
        self.assertFalse(any(method == "DELETE" for method, *_ in requests))


if __name__ == "__main__":
    unittest.main()

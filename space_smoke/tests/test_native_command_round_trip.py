from __future__ import annotations

import base64
import json
import threading
import unittest
from collections.abc import Mapping, Sequence
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO, StringIO
from typing import Iterator
from unittest import mock
from urllib.parse import parse_qs, urlsplit

from space_smoke import native as native_module
from space_smoke.__main__ import _describe, _rerun_command, _run_all
from space_smoke.fixtures import CommandSandboxFixture
from space_smoke.model import ScenarioResult, Verdict
from space_smoke.native import NativeConfig, NativeSandboxFactory
from space_smoke.registry import Scenario
from space_smoke.runtime import SmokeConfigurationError, execute_scenario
from space_smoke.scenarios import REGISTRY
from space_smoke.scenarios.ecw_background_process_control import (
    PROCESS_COMMAND,
    PROCESS_READY_MARKER,
    PROCESS_READY_PREFIX,
    PROCESS_SIGNAL,
    background_process_control,
)
from space_smoke.scenarios.ecw_command_round_trip import MARKER, command_round_trip
from space_smoke.scenarios.ecw_pause_connect import (
    MARKER as LIFECYCLE_MARKER,
    PATH as LIFECYCLE_PATH,
    pause_connect,
)
from space_smoke.scenarios.ecw_sandbox_delete import sandbox_delete
from space_smoke.scenarios.ecw_sandbox_info import sandbox_info
from space_smoke.scenarios.ecw_sandbox_list_pagination import (
    PAGE_SIZE,
    sandbox_list_pagination,
)
from space_smoke.scenarios.native_resident_pause import (
    MARKER as RESIDENT_MARKER,
    PATH as RESIDENT_PATH,
    resident_pause,
)
from space_smoke.services import SmokeServices

_SANDBOX_IDS = tuple(
    f"sbx_00000000-0000-0000-0000-{ordinal:012d}" for ordinal in range(1, 4)
)
_SANDBOX_ID = _SANDBOX_IDS[0]
_SESSION_ID = "proc_00000000-0000-0000-0000-000000000002"
_PROCESS_ID = "proc_00000000-0000-0000-0000-000000000099"
_LIST_CURSOR = "pc1_second-page.signature"


@dataclass(frozen=True)
class _Request:
    method: str
    path: str
    authorization: str | None
    idempotency_key: str | None
    body: object | None


class _Script:
    def __init__(
        self,
        stdout: str,
        sandbox_info_id: str,
        list_pages: tuple[Mapping[str, object], ...] | None,
        post_delete_status: int,
        operation_failure: str | None,
        marker_after_wake: str | None,
        wake_ready: bool,
        pause_state: str,
        process_failure: str | None,
    ) -> None:
        self.stdout = stdout
        self.sandbox_info_id = sandbox_info_id
        self.list_pages = list_pages
        self.post_delete_status = post_delete_status
        self.operation_failure = operation_failure
        self.marker_after_wake = marker_after_wake
        self.wake_ready = wake_ready
        self.pause_state = pause_state
        self.requests: list[_Request] = []
        self.operation_polls: dict[str, int] = {}
        self.sandbox_polls: dict[str, int] = {}
        self.sandbox_states: dict[str, str] = {}
        self.operation_targets: dict[str, tuple[str, str]] = {}
        self.command_results: dict[str, tuple[str, str, int]] = {}
        self.background_sessions: set[str] = set()
        self.created_ids: list[str] = []
        self.deleted_ids: set[str] = set()
        self.list_requests = 0
        self.command_ready = False
        self.marker = ""
        self.process_failure = process_failure
        self.process_running = False
        self.process_killed = False


def _handler(script: _Script) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            del format, args

        def _record(self) -> object | None:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length)) if length else None
            script.requests.append(
                _Request(
                    self.command,
                    self.path,
                    self.headers.get("Authorization"),
                    self.headers.get("Idempotency-Key"),
                    body,
                )
            )
            return body

        def _json(self, status: int, payload: Mapping[str, object]) -> None:
            encoded = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self) -> None:
            body = self._record()
            if self.path == "/v1/sandboxes":
                sandbox_id = _SANDBOX_IDS[len(script.created_ids)]
                script.created_ids.append(sandbox_id)
                script.sandbox_states[sandbox_id] = "starting"
                ordinal = len(script.created_ids)
                self._json(
                    202,
                    {
                        "sandbox_id": sandbox_id,
                        "operation_id": (
                            "op-create" if ordinal == 1 else f"op-create-{ordinal}"
                        ),
                    },
                )
                return
            lifecycle = {
                f"/v1/sandboxes/{_SANDBOX_ID}:pause": (
                    "op-pause",
                    script.pause_state,
                ),
                f"/v1/sandboxes/{_SANDBOX_ID}:suspend": (
                    "op-suspend",
                    "suspended",
                ),
                f"/v1/sandboxes/{_SANDBOX_ID}:wake": ("op-wake", "restoring"),
            }.get(self.path)
            if lifecycle is not None:
                operation_id, target_state = lifecycle
                script.operation_targets[operation_id] = (
                    _SANDBOX_ID,
                    target_state,
                )
                script.command_ready = False
                self._json(
                    202,
                    {
                        "sandbox_id": _SANDBOX_ID,
                        "operation_id": operation_id,
                    },
                )
                return
            if self.path == f"/v1/sandboxes/{_SANDBOX_ID}/commands":
                if not script.command_ready:
                    self.send_error(409, "sandbox is not running")
                    return
                assert isinstance(body, dict)
                command = body.get("command")
                assert isinstance(command, list) and command[:2] == ["sh", "-c"]
                shell_command = command[2]
                expected_timeout = (
                    "10s"
                    if shell_command == f"cat {PROCESS_READY_PREFIX}123"
                    else "60s"
                )
                assert body.get("timeout") == expected_timeout
                if shell_command == f"printf %s {MARKER}":
                    result = (script.stdout, "native-stderr", 0)
                elif shell_command.startswith("printf %s ") and " > " in shell_command:
                    marker_and_path = shell_command.removeprefix("printf %s ")
                    script.marker, path = marker_and_path.split(" > ", 1)
                    assert path in {LIFECYCLE_PATH, RESIDENT_PATH}
                    result = ("", "", 0)
                elif shell_command in {
                    f"cat {LIFECYCLE_PATH}",
                    f"cat {RESIDENT_PATH}",
                }:
                    stdout = (
                        script.marker
                        if script.marker_after_wake is None
                        else script.marker_after_wake
                    )
                    result = (stdout, "", 0)
                elif shell_command == PROCESS_COMMAND:
                    result = ("", "", 0)
                elif shell_command == f"cat {PROCESS_READY_PREFIX}123":
                    result = (PROCESS_READY_MARKER, "", 0)
                else:
                    raise AssertionError(f"unexpected native command: {shell_command}")
                session_ordinal = len(script.command_results) + 2
                session_id = f"proc_00000000-0000-0000-0000-{session_ordinal:012d}"
                script.command_results[session_id] = result
                if shell_command == PROCESS_COMMAND:
                    script.background_sessions.add(session_id)
                self._json(
                    201,
                    {
                        "session_id": session_id,
                        "process_id": (
                            _PROCESS_ID
                            if shell_command == PROCESS_COMMAND
                            else "guest-process"
                        ),
                        "attach": {
                            "events_url": f"/v1/sessions/{session_id}/events",
                            "token": "attach-token",
                        },
                    },
                )
                return
            if self.path == (
                f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}:signal"
            ):
                assert body == {"signal": PROCESS_SIGNAL}
                self._json(200, {"ok": script.process_failure != "signal"})
                return
            if self.path == (
                f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}:kill"
            ):
                if script.process_failure == "kill":
                    self._json(200, {"ok": False})
                    return
                if script.process_failure != "terminal_timeout":
                    script.process_running = False
                    script.process_killed = True
                self._json(200, {"ok": True})
                return
            self.send_error(404)

        def do_GET(self) -> None:
            self._record()
            parsed = urlsplit(self.path)
            if parsed.path == "/v1/sandboxes":
                if script.list_pages is None:
                    self.send_error(404)
                    return
                page_index = min(script.list_requests, len(script.list_pages) - 1)
                script.list_requests += 1
                self._json(200, script.list_pages[page_index])
                return
            sandbox_id = parsed.path.removeprefix("/v1/sandboxes/")
            if sandbox_id in script.deleted_ids and "/" not in sandbox_id:
                if script.post_delete_status == 200:
                    self._json(200, {"id": sandbox_id, "state": "deleting"})
                else:
                    self.send_error(script.post_delete_status)
                return
            if sandbox_id in script.created_ids and "/" not in sandbox_id:
                state = script.sandbox_states[sandbox_id]
                if state in {"starting", "restoring"}:
                    polls = script.sandbox_polls.get(sandbox_id, 0) + 1
                    script.sandbox_polls[sandbox_id] = polls
                    ready = state == "starting" or script.wake_ready
                    if ready and polls > 1:
                        state = "running"
                        script.sandbox_states[sandbox_id] = state
                self._json(
                    200,
                    {
                        "id": (
                            script.sandbox_info_id
                            if sandbox_id == _SANDBOX_ID
                            else sandbox_id
                        ),
                        "state": state,
                    },
                )
                return
            if (
                parsed.path.removesuffix("/processes").removeprefix("/v1/sandboxes/")
                in script.created_ids
                and parsed.path.endswith("/processes")
                and parse_qs(parsed.query) == {"limit": ["1"]}
            ):
                script.command_ready = True
                self._json(200, {"data": [], "next_cursor": None})
                return
            if parsed.path == f"/v1/sandboxes/{_SANDBOX_ID}/processes":
                process = _process_payload()
                data = [process] if script.process_running else []
                if script.process_failure == "missing_list":
                    data = []
                if script.process_failure == "duplicate_list":
                    data = [process, process]
                self._json(200, {"data": data, "next_cursor": None})
                return
            if parsed.path == (f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}"):
                if script.process_killed:
                    # The native API keeps a recently exited process's exit.
                    self._json(
                        200,
                        {
                            **_process_payload(),
                            "state": "exited",
                            "exit_code": 137,
                            "exit_status": "signaled",
                            "signal": 9,
                            "ended_at": "2026-09-28T12:00:00Z",
                        },
                    )
                    return
                if not script.process_running:
                    self.send_error(404)
                    return
                process = dict(_process_payload())
                if script.process_failure == "mismatched_get":
                    process["process_id"] = "proc_wrong"
                self._json(200, process)
                return
            if self.path.startswith("/v1/operations/"):
                operation_id = self.path.rsplit("/", 1)[-1]
                polls = script.operation_polls.get(operation_id, 0) + 1
                script.operation_polls[operation_id] = polls
                operation_name = operation_id.removeprefix("op-").split("-", 1)[0]
                state = (
                    "queued"
                    if polls == 1
                    else (
                        "failed"
                        if operation_name == script.operation_failure
                        else "succeeded"
                    )
                )
                if state == "succeeded" and operation_id in script.operation_targets:
                    target_id, target_state = script.operation_targets[operation_id]
                    script.sandbox_states[target_id] = target_state
                    script.sandbox_polls[target_id] = 0
                self._json(
                    200,
                    {
                        "id": operation_id,
                        "state": state,
                    },
                )
                return
            session_id = parsed.path.removeprefix("/v1/sessions/").removesuffix(
                "/events"
            )
            if session_id in script.command_results:
                stdout, stderr, exit_code = script.command_results[session_id]
                if session_id in script.background_sessions:
                    script.process_running = True
                    background_frames = [
                        {
                            "type": "process_started",
                            "process_id": _PROCESS_ID,
                            "pid": 123,
                        }
                    ]
                    self._write_sse(background_frames)
                    return
                frames: list[Mapping[str, object]] = [
                    {"type": "process_started", "process_id": "guest-process"}
                ]
                if stdout:
                    frames.append(
                        {
                            "type": "stdout",
                            "seq": 1,
                            "data_base64": base64.b64encode(stdout.encode()).decode(),
                        }
                    )
                if stderr:
                    frames.append(
                        {
                            "type": "stderr",
                            "seq": 2,
                            "data_base64": base64.b64encode(stderr.encode()).decode(),
                        }
                    )
                frames.append(
                    {
                        "type": "process_exited",
                        "process_id": "guest-process",
                        "exit_code": exit_code,
                        "status": "exited",
                    }
                )
                self._write_sse(frames)
                return
            self.send_error(404)

        def _write_sse(self, frames: Sequence[Mapping[str, object]]) -> None:
            encoded = b"".join(
                f"event: {frame['type']}\ndata: {json.dumps(frame)}\n\n".encode()
                for frame in frames
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_DELETE(self) -> None:
            self._record()
            sandbox_id = self.path.removeprefix("/v1/sandboxes/")
            if sandbox_id in script.deleted_ids:
                self.send_error(404)
                return
            if sandbox_id in script.created_ids:
                script.deleted_ids.add(sandbox_id)
                ordinal = _SANDBOX_IDS.index(sandbox_id) + 1
                self._json(
                    202,
                    {
                        "sandbox_id": sandbox_id,
                        "operation_id": (
                            "op-delete" if ordinal == 1 else f"op-delete-{ordinal}"
                        ),
                    },
                )
                return
            self.send_error(404)

    return Handler


@contextmanager
def _server(
    stdout: str,
    *,
    sandbox_info_id: str = _SANDBOX_ID,
    list_pages: tuple[Mapping[str, object], ...] | None = None,
    post_delete_status: int = 404,
    operation_failure: str | None = None,
    marker_after_wake: str | None = None,
    wake_ready: bool = True,
    pause_state: str = "paused",
    process_failure: str | None = None,
) -> Iterator[tuple[str, _Script]]:
    script = _Script(
        stdout,
        sandbox_info_id,
        list_pages,
        post_delete_status,
        operation_failure,
        marker_after_wake,
        wake_ready,
        pause_state,
        process_failure,
    )
    server = HTTPServer(("127.0.0.1", 0), _handler(script))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", script
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _run(
    services: SmokeServices,
    *,
    scenario: Scenario[CommandSandboxFixture] = command_round_trip,
) -> ScenarioResult:
    return execute_scenario(
        title=scenario.title,
        body=lambda context: scenario.run(context, services),
    )


def _page(*sandbox_ids: str, next_cursor: object = None) -> Mapping[str, object]:
    return {
        "data": [{"id": sandbox_id} for sandbox_id in sandbox_ids],
        "next_cursor": next_cursor,
    }


def _process_payload() -> dict[str, object]:
    return {
        "process_id": _PROCESS_ID,
        "pid": 123,
        "command": ["sh", "-c", PROCESS_COMMAND],
        "cwd": None,
        "user": "user",
        "tag": None,
    }


def _native_services(
    base_url: str,
    scenario: Scenario[CommandSandboxFixture],
    *,
    ready_timeout_seconds: str = "180",
) -> SmokeServices:
    return SmokeServices.from_environment(
        scenario,
        {
            "SPACE_SMOKE_API_SURFACE": "native",
            "SPACE_API_TOKEN": "native-token",
            "SPACE_API_URL": base_url,
            "SPACE_DATA_PLANE_URL": base_url,
            "SPACE_READY_TIMEOUT_SECS": ready_timeout_seconds,
        },
    )


class NativeCommandRoundTripTests(unittest.TestCase):
    def test_delete_retries_only_snapshot_upload_conflicts_with_one_deadline(
        self,
    ) -> None:
        upload_message = (
            "sandbox has a suspend-snapshot upload in flight; "
            "retry the DELETE after the snapshot finalizes"
        )
        cases = (
            ("conflict", upload_message, "succeeded", None, 2),
            ("conflict", "fork still in progress", "succeeded", RuntimeError, 1),
            ("internal", upload_message, "succeeded", RuntimeError, 1),
            ("conflict", upload_message, "failed", TimeoutError, 3),
            ("conflict", upload_message, "queued", TimeoutError, 2),
        )
        for code, message, later_state, expected_error, expected_deletes in cases:
            with self.subTest(code=code, message=message, later_state=later_state):
                factory = NativeSandboxFactory(
                    NativeConfig(
                        "https://api.example",
                        "https://data.example",
                        "token",
                        "seed",
                        ready_timeout_seconds=3,
                        request_timeout_seconds=3,
                    )
                )
                sandbox = native_module._NativeSandbox(factory, _SANDBOX_ID)
                requests: list[object] = []
                deletes = 0

                def respond(
                    request,
                    *,
                    timeout,
                    requests=requests,
                    later_state=later_state,
                    code=code,
                    message=message,
                ):
                    nonlocal deletes
                    requests.append(request)
                    self.assertGreater(timeout, 0)
                    self.assertLessEqual(timeout, 3)
                    if request.get_method() == "DELETE":
                        deletes += 1
                        payload = {"operation_id": f"op-delete-{deletes}"}
                        status = 202
                    else:
                        self.assertTrue(
                            request.full_url.endswith(f"/op-delete-{deletes}")
                        )
                        payload = {
                            "state": "failed" if deletes == 1 else later_state,
                            "terminal_error_code": code,
                            "terminal_error": {"message": message},
                        }
                        if payload["state"] == "queued":
                            clock.monotonic.return_value = 3.0
                        status = 200
                    response = BytesIO(json.dumps(payload).encode())
                    response.status = status
                    return response

                with (
                    mock.patch.object(native_module, "_URL_OPEN", side_effect=respond),
                    mock.patch.object(native_module, "time") as clock,
                ):
                    clock.monotonic.return_value = 0.0
                    clock.sleep.side_effect = lambda seconds: setattr(
                        clock.monotonic,
                        "return_value",
                        clock.monotonic.return_value + seconds,
                    )
                    if expected_error is None:
                        factory.delete(sandbox)
                    else:
                        with self.assertRaises(expected_error):
                            factory.delete(sandbox)
                    self.assertLessEqual(clock.monotonic.return_value, 3)
                delete_requests = [r for r in requests if r.get_method() == "DELETE"]
                self.assertEqual(len(delete_requests), expected_deletes)
                self.assertEqual(
                    len({r.get_header("Idempotency-key") for r in delete_requests}),
                    expected_deletes,
                )

    def assert_cleaned_up(self, script: _Script) -> None:
        self.assertEqual(
            [request.method for request in script.requests].count("DELETE"),
            1,
        )
        self.assertEqual(script.deleted_ids, {_SANDBOX_ID})

    def test_shared_scenario_runs_the_native_http_and_sse_sequence(self) -> None:
        with _server(MARKER) as (base_url, script):
            environment = {
                "SPACE_SMOKE_API_SURFACE": "native",
                "SPACE_API_TOKEN": "native-token",
                "SPACE_API_URL": base_url,
                "SPACE_DATA_PLANE_URL": base_url,
                "SPACE_READY_TIMEOUT_SECS": "37",
            }
            with (
                mock.patch(
                    "space_smoke.services.importlib.util.find_spec", return_value=None
                ),
                mock.patch(
                    "space_smoke.native._URL_OPEN", wraps=native_module._URL_OPEN
                ) as url_open,
                mock.patch("space_smoke.native.time.sleep"),
            ):
                native = SmokeServices.from_environment(command_round_trip, environment)
                result = _run(native)

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.evidence["command_stderr"], "native-stderr")
        self.assertEqual(url_open.call_args_list[5].kwargs["timeout"], 37)
        self.assertEqual(
            [(request.method, request.path) for request in script.requests],
            [
                ("POST", "/v1/sandboxes"),
                ("GET", "/v1/operations/op-create"),
                ("GET", "/v1/operations/op-create"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes?limit=1"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}/commands"),
                ("GET", f"/v1/sessions/{_SESSION_ID}/events"),
                ("DELETE", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", "/v1/operations/op-delete"),
                ("GET", "/v1/operations/op-delete"),
            ],
        )
        mutation_keys = [
            request.idempotency_key
            for request in script.requests
            if request.method in {"POST", "DELETE"}
        ]
        self.assertEqual(len(set(mutation_keys)), 3)
        self.assertTrue(all(mutation_keys))
        self.assertTrue(
            all(
                request.authorization
                == (
                    "Bearer attach-token"
                    if request.path.endswith("/events")
                    else "Bearer native-token"
                )
                for request in script.requests
            )
        )

    def test_shared_background_process_scenario_runs_native_control_sequence(
        self,
    ) -> None:
        self.assertIs(
            REGISTRY.get("ecw.background_process_control"),
            background_process_control,
        )
        with _server(MARKER) as (base_url, script):
            native = _native_services(base_url, background_process_control)
            with mock.patch("space_smoke.native.time.sleep"):
                result = _run(native, scenario=background_process_control)

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.cleanup_failures, ())
        self.assertEqual(
            [(request.method, request.path) for request in script.requests],
            [
                ("POST", "/v1/sandboxes"),
                ("GET", "/v1/operations/op-create"),
                ("GET", "/v1/operations/op-create"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes?limit=1"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}/commands"),
                ("GET", f"/v1/sessions/{_SESSION_ID}/events"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes"),
                (
                    "GET",
                    f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}",
                ),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}/commands"),
                ("GET", "/v1/sessions/proc_00000000-0000-0000-0000-000000000003/events"),
                (
                    "POST",
                    f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}:signal",
                ),
                (
                    "GET",
                    f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}",
                ),
                (
                    "POST",
                    f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}:kill",
                ),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes"),
                (
                    "GET",
                    f"/v1/sandboxes/{_SANDBOX_ID}/processes/{_PROCESS_ID}",
                ),
                ("DELETE", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", "/v1/operations/op-delete"),
                ("GET", "/v1/operations/op-delete"),
            ],
        )
        required_keys = [
            request.idempotency_key
            for request in script.requests
            if request.path
            in {
                "/v1/sandboxes",
                f"/v1/sandboxes/{_SANDBOX_ID}/commands",
                f"/v1/sandboxes/{_SANDBOX_ID}",
            }
            and request.method in {"POST", "DELETE"}
        ]
        self.assertEqual(len(required_keys), 4)
        self.assertEqual(len(required_keys), len(set(required_keys)))
        self.assertNotIn(None, required_keys)
        process_mutations = [
            request
            for request in script.requests
            if request.path.endswith((":signal", ":kill"))
        ]
        self.assertEqual(
            [request.idempotency_key for request in process_mutations], [None, None]
        )
        self.assertTrue(
            all(
                request.authorization
                == (
                    "Bearer attach-token"
                    if request.path.endswith("/events")
                    else "Bearer native-token"
                )
                for request in script.requests
            )
        )

    def test_shared_background_process_failures_still_clean_the_sandbox(self) -> None:
        cases = (
            ("missing_list", Verdict.FAIL, "did not contain"),
            ("duplicate_list", Verdict.FAIL, "exactly once"),
            ("mismatched_get", Verdict.FAIL, "details did not match"),
            ("signal", Verdict.FAIL, "did not signal"),
            ("kill", Verdict.FAIL, "before expected SIGKILL"),
            ("terminal_timeout", Verdict.FAIL, "remained visible"),
        )
        for failure, verdict, summary in cases:
            with self.subTest(failure=failure):
                with _server(MARKER, process_failure=failure) as (base_url, script):
                    native = _native_services(base_url, background_process_control)
                    with (
                        mock.patch.object(
                            CommandSandboxFixture, "process_transition_timeout", 0
                        ),
                        mock.patch("space_smoke.native.time.sleep"),
                    ):
                        result = _run(native, scenario=background_process_control)

                self.assertEqual(result.verdict, verdict)
                self.assertIn(summary, result.summary)
                self.assertEqual(result.cleanup_failures, ())
                self.assert_cleaned_up(script)

    def test_shared_durable_lifecycle_preserves_native_marker(self) -> None:
        marker = f"{LIFECYCLE_MARKER}-{_SANDBOX_ID}"
        with _server(MARKER) as (base_url, script):
            native = _native_services(base_url, pause_connect)
            with (
                mock.patch(
                    "space_smoke.services.importlib.util.find_spec", return_value=None
                ) as find_spec,
                mock.patch("space_smoke.native.time.sleep"),
            ):
                result = _run(native, scenario=pause_connect)

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.cleanup_failures, ())
        find_spec.assert_not_called()
        self.assertEqual(script.marker, marker)
        self.assertEqual(
            [(request.method, request.path) for request in script.requests],
            [
                ("POST", "/v1/sandboxes"),
                ("GET", "/v1/operations/op-create"),
                ("GET", "/v1/operations/op-create"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes?limit=1"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}/commands"),
                ("GET", f"/v1/sessions/{_SESSION_ID}/events"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}:suspend"),
                ("GET", "/v1/operations/op-suspend"),
                ("GET", "/v1/operations/op-suspend"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}:wake"),
                ("GET", "/v1/operations/op-wake"),
                ("GET", "/v1/operations/op-wake"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes?limit=1"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}/commands"),
                (
                    "GET",
                    "/v1/sessions/proc_00000000-0000-0000-0000-000000000003/events",
                ),
                ("DELETE", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", "/v1/operations/op-delete"),
                ("GET", "/v1/operations/op-delete"),
            ],
        )
        lifecycle_mutations = [
            request
            for request in script.requests
            if request.path
            in {
                "/v1/sandboxes",
                f"/v1/sandboxes/{_SANDBOX_ID}:suspend",
                f"/v1/sandboxes/{_SANDBOX_ID}:wake",
                f"/v1/sandboxes/{_SANDBOX_ID}",
            }
            and request.method in {"POST", "DELETE"}
        ]
        self.assertEqual(len(lifecycle_mutations), 4)
        mutation_keys = [request.idempotency_key for request in lifecycle_mutations]
        self.assertEqual(len(set(mutation_keys)), 4)
        self.assertNotIn(None, mutation_keys)
        self.assertTrue(
            all(
                key is not None and key.startswith(f"space-smoke-{action}-")
                for action, key in zip(
                    ("create", "suspend", "wake", "delete"),
                    mutation_keys,
                    strict=True,
                )
            )
        )
        self.assertTrue(
            all(
                request.authorization
                == (
                    "Bearer attach-token"
                    if request.path.endswith("/events")
                    else "Bearer native-token"
                )
                for request in script.requests
            )
        )

    def test_native_resident_pause_preserves_marker_and_exact_state(self) -> None:
        marker = f"{RESIDENT_MARKER}-{_SANDBOX_ID}"
        self.assertIs(REGISTRY.get("native.resident_pause"), resident_pause)
        with _server(MARKER) as (base_url, script):
            native = _native_services(base_url, resident_pause)
            with mock.patch("space_smoke.native.time.sleep"):
                result = _run(native, scenario=resident_pause)

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.cleanup_failures, ())
        self.assertEqual(script.marker, marker)
        self.assertEqual(
            [(request.method, request.path) for request in script.requests],
            [
                ("POST", "/v1/sandboxes"),
                ("GET", "/v1/operations/op-create"),
                ("GET", "/v1/operations/op-create"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes?limit=1"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}/commands"),
                ("GET", f"/v1/sessions/{_SESSION_ID}/events"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}:pause"),
                ("GET", "/v1/operations/op-pause"),
                ("GET", "/v1/operations/op-pause"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}:wake"),
                ("GET", "/v1/operations/op-wake"),
                ("GET", "/v1/operations/op-wake"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes?limit=1"),
                ("POST", f"/v1/sandboxes/{_SANDBOX_ID}/commands"),
                (
                    "GET",
                    "/v1/sessions/proc_00000000-0000-0000-0000-000000000003/events",
                ),
                ("DELETE", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", "/v1/operations/op-delete"),
                ("GET", "/v1/operations/op-delete"),
            ],
        )
        mutation_keys = [
            request.idempotency_key
            for request in script.requests
            if request.idempotency_key is not None
        ]
        self.assertEqual(len(mutation_keys), 6)
        self.assertEqual(len(mutation_keys), len(set(mutation_keys)))
        self.assertTrue(
            all(
                key.startswith(f"space-smoke-{action}-")
                for action, key in zip(
                    ("create", "command", "pause", "wake", "command", "delete"),
                    mutation_keys,
                    strict=True,
                )
            )
        )
        self.assertTrue(
            all(
                request.authorization
                == (
                    "Bearer attach-token"
                    if request.path.endswith("/events")
                    else "Bearer native-token"
                )
                for request in script.requests
            )
        )

    def test_native_resident_pause_failures_clean_up(self) -> None:
        cases = (
            (
                "pause",
                "paused",
                "180",
                "sandbox resident pause did not complete",
                "ended in failed",
            ),
            (
                None,
                "suspended",
                "180",
                "sandbox resident pause did not complete",
                "ended in suspended",
            ),
            (
                None,
                "pausing",
                "3",
                "sandbox resident pause did not complete",
                "did not reach paused",
            ),
            (
                "wake",
                "paused",
                "180",
                "sandbox wake from resident pause did not complete",
                "ended in failed",
            ),
        )
        for operation_failure, pause_state, ready_timeout, summary, detail in cases:
            with self.subTest(
                operation_failure=operation_failure, pause_state=pause_state
            ):
                with _server(
                    MARKER,
                    operation_failure=operation_failure,
                    pause_state=pause_state,
                ) as (base_url, script):
                    native = _native_services(
                        base_url,
                        resident_pause,
                        ready_timeout_seconds=ready_timeout,
                    )
                    # Advance the polling clock with sleeps, not HTTP/server
                    # scheduling. A tiny real deadline can fail Create or Delete
                    # before the intended resident-pause timeout is observed.
                    with mock.patch.object(native_module, "time") as clock:
                        clock.monotonic.return_value = 0.0

                        def advance(seconds: float) -> None:
                            clock.monotonic.return_value += seconds

                        clock.sleep.side_effect = advance
                        result = _run(native, scenario=resident_pause)

                self.assertEqual(result.verdict, Verdict.FAIL)
                self.assertEqual(result.summary, summary)
                error_key = (
                    "wake_error" if operation_failure == "wake" else "pause_error"
                )
                self.assertIn(detail, result.evidence[error_key])
                self.assertEqual(result.cleanup_failures, ())
                self.assert_cleaned_up(script)

    def test_native_resident_wake_command_readiness_timeout_cleans_up(self) -> None:
        with _server(MARKER) as (base_url, script):
            native = _native_services(base_url, resident_pause)
            real_url_open = native_module._URL_OPEN
            readiness_requests = 0

            def open_with_readiness_timeout(request, *args, **kwargs):
                nonlocal readiness_requests
                parsed = urlsplit(request.full_url)
                if parsed.path.endswith("/processes") and parsed.query == "limit=1":
                    readiness_requests += 1
                    if readiness_requests == 2:
                        raise TimeoutError("command readiness timed out")
                return real_url_open(request, *args, **kwargs)

            with (
                mock.patch(
                    "space_smoke.native._URL_OPEN",
                    side_effect=open_with_readiness_timeout,
                ),
                mock.patch("space_smoke.native.time.sleep"),
            ):
                result = _run(native, scenario=resident_pause)

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(
            result.summary, "sandbox wake from resident pause did not complete"
        )
        self.assertEqual(result.evidence["wake_error_type"], "TimeoutError")
        self.assertIn("command readiness timed out", result.evidence["wake_error"])
        self.assertEqual(result.cleanup_failures, ())
        self.assert_cleaned_up(script)

    def test_native_resident_pause_marker_loss_or_mismatch_cleans_up(self) -> None:
        for observed in ("", "wrong-marker"):
            with self.subTest(observed=observed):
                with _server(MARKER, marker_after_wake=observed) as (
                    base_url,
                    script,
                ):
                    native = _native_services(base_url, resident_pause)
                    with mock.patch("space_smoke.native.time.sleep"):
                        result = _run(native, scenario=resident_pause)

                self.assertEqual(result.verdict, Verdict.FAIL)
                self.assertEqual(result.summary, "sandbox command result did not match")
                self.assertEqual(result.evidence["actual"], (0, observed))
                self.assertEqual(result.cleanup_failures, ())
                self.assert_cleaned_up(script)

    def test_native_lifecycle_operation_failures_clean_up(self) -> None:
        cases = (
            ("suspend", "sandbox durable suspend did not complete"),
            ("wake", "sandbox wake did not complete"),
        )
        for operation, summary in cases:
            with self.subTest(operation=operation):
                with _server(MARKER, operation_failure=operation) as (
                    base_url,
                    script,
                ):
                    native = _native_services(base_url, pause_connect)
                    with mock.patch("space_smoke.native.time.sleep"):
                        result = _run(native, scenario=pause_connect)

                self.assertEqual(result.verdict, Verdict.FAIL)
                self.assertEqual(result.summary, summary)
                self.assertEqual(result.cleanup_failures, ())
                self.assertEqual(
                    result.evidence[f"{operation}_error_type"], "RuntimeError"
                )
                self.assert_cleaned_up(script)

    def test_native_marker_loss_or_mismatch_fails_and_cleans_up(self) -> None:
        for observed in ("", "wrong-marker"):
            with self.subTest(observed=observed):
                with _server(MARKER, marker_after_wake=observed) as (
                    base_url,
                    script,
                ):
                    native = _native_services(base_url, pause_connect)
                    with mock.patch("space_smoke.native.time.sleep"):
                        result = _run(native, scenario=pause_connect)

                self.assertEqual(result.verdict, Verdict.FAIL)
                self.assertEqual(result.summary, "sandbox command result did not match")
                self.assertEqual(result.cleanup_failures, ())
                self.assertEqual(result.evidence["actual"], (0, observed))
                self.assert_cleaned_up(script)

    def test_shared_sandbox_info_scenario_runs_one_explicit_native_read(self) -> None:
        with _server(MARKER) as (base_url, script):
            with (
                mock.patch(
                    "space_smoke.services.importlib.util.find_spec", return_value=None
                ) as find_spec,
                mock.patch("space_smoke.native.time.sleep"),
            ):
                native = _native_services(base_url, sandbox_info)
                result = _run(native, scenario=sandbox_info)

        self.assertEqual(result.verdict, Verdict.PASS)
        find_spec.assert_not_called()
        self.assertEqual(
            [(request.method, request.path) for request in script.requests],
            [
                ("POST", "/v1/sandboxes"),
                ("GET", "/v1/operations/op-create"),
                ("GET", "/v1/operations/op-create"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}/processes?limit=1"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("DELETE", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", "/v1/operations/op-delete"),
                ("GET", "/v1/operations/op-delete"),
            ],
        )
        self.assertTrue(
            all(
                request.authorization == "Bearer native-token"
                for request in script.requests
            )
        )

    def test_shared_delete_waits_for_operation_then_requires_native_404(self) -> None:
        with _server(MARKER) as (base_url, script):
            with (
                mock.patch(
                    "space_smoke.services.importlib.util.find_spec", return_value=None
                ) as find_spec,
                mock.patch("space_smoke.native.time.sleep"),
            ):
                native = _native_services(base_url, sandbox_delete)
                result = _run(native, scenario=sandbox_delete)

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.evidence["deleted_sandbox_id"], _SANDBOX_ID)
        self.assertEqual(result.evidence["observed_state"], "deleted")
        self.assertEqual(result.cleanup_failures, ())
        find_spec.assert_not_called()
        self.assertEqual(
            [(request.method, request.path) for request in script.requests[-5:]],
            [
                ("DELETE", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("GET", "/v1/operations/op-delete"),
                ("GET", "/v1/operations/op-delete"),
                ("GET", f"/v1/sandboxes/{_SANDBOX_ID}"),
                ("DELETE", f"/v1/sandboxes/{_SANDBOX_ID}"),
            ],
        )
        mutation_keys = [
            request.idempotency_key
            for request in script.requests
            if request.method in {"POST", "DELETE"}
        ]
        self.assertEqual(len(mutation_keys), len(set(mutation_keys)))
        self.assertNotIn(None, mutation_keys)
        self.assertEqual(
            {request.authorization for request in script.requests},
            {"Bearer native-token"},
        )

    def test_native_delete_rejects_non_absence_and_preserves_primary_failure(
        self,
    ) -> None:
        cases = (
            (200, Verdict.FAIL, "sandbox remained visible"),
            (500, Verdict.INFRA_ERROR, "HTTP Error 500"),
        )
        for status, verdict, summary in cases:
            with self.subTest(status=status):
                with _server(MARKER, post_delete_status=status) as (base_url, script):
                    native = _native_services(base_url, sandbox_delete)
                    with (
                        mock.patch.object(
                            CommandSandboxFixture, "lifecycle_transition_timeout", 0
                        ),
                        mock.patch("space_smoke.native.time.sleep"),
                    ):
                        result = _run(native, scenario=sandbox_delete)

                self.assertEqual(result.verdict, verdict)
                self.assertIn(summary, result.summary)
                self.assertEqual(result.cleanup_failures, ())
                self.assertEqual(
                    sum(request.method == "DELETE" for request in script.requests),
                    2,
                )

    def test_wrong_native_info_id_fails_shared_assertion_and_cleans_up(self) -> None:
        with _server(MARKER, sandbox_info_id="sbx-wrong") as (base_url, script):
            native = _native_services(base_url, sandbox_info)
            with mock.patch("space_smoke.native.time.sleep"):
                result = _run(native, scenario=sandbox_info)

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.summary, "sandbox information returned a different ID")
        self.assertEqual(result.evidence["expected_sandbox_id"], _SANDBOX_ID)
        self.assertEqual(result.evidence["observed_sandbox_id"], "sbx-wrong")
        self.assertEqual(result.cleanup_failures, ())
        self.assertEqual(
            [request.method for request in script.requests].count("DELETE"), 1
        )
        self.assertEqual(
            [
                request.path
                for request in script.requests
                if request.method == "GET"
                and request.path == f"/v1/sandboxes/{_SANDBOX_ID}"
            ],
            [f"/v1/sandboxes/{_SANDBOX_ID}"] * 3,
        )

    def test_wrong_stdout_fails_through_shared_assertion_and_cleans_up(self) -> None:
        with _server("wrong") as (base_url, script):
            native = _native_services(base_url, command_round_trip)
            with mock.patch("space_smoke.native.time.sleep"):
                native_result = _run(native)

        self.assertEqual(native_result.verdict, Verdict.FAIL)
        self.assertEqual(native_result.summary, "sandbox command result did not match")
        self.assertEqual(native_result.evidence["actual"], (0, "wrong"))
        self.assertTrue(any(request.method == "DELETE" for request in script.requests))

    def test_shared_pagination_stops_after_owned_ids_and_cleans_up(
        self,
    ) -> None:
        external_id = "sbx_00000000-0000-0000-0000-999999999999"
        pages = (
            _page(_SANDBOX_IDS[0], external_id, next_cursor=_LIST_CURSOR),
            _page(*_SANDBOX_IDS[1:], next_cursor="pc1_more.signature"),
            _page(external_id, external_id, next_cursor="pc1_more.signature"),
        )
        with _server(MARKER, list_pages=pages) as (base_url, script):
            native = _native_services(base_url, sandbox_list_pagination)
            with mock.patch("space_smoke.native.time.sleep"):
                result = _run(native, scenario=sandbox_list_pagination)

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.evidence["requested_page_size"], PAGE_SIZE)
        self.assertEqual(
            [
                request.path
                for request in script.requests
                if urlsplit(request.path).path == "/v1/sandboxes"
                and request.method == "GET"
            ],
            [
                "/v1/sandboxes?limit=2",
                f"/v1/sandboxes?limit=2&cursor={_LIST_CURSOR}",
            ],
        )
        self.assertEqual(
            {
                request.path.removeprefix("/v1/sandboxes/")
                for request in script.requests
                if request.method == "DELETE"
            },
            set(_SANDBOX_IDS),
        )
        self.assertTrue(
            all(
                request.authorization == "Bearer native-token"
                for request in script.requests
            )
        )

    def test_shared_pagination_rejects_duplicate_and_missing_owned_ids(self) -> None:
        cases = (
            (
                "duplicate run-owned sandbox",
                (
                    _page(*_SANDBOX_IDS[:2], next_cursor=_LIST_CURSOR),
                    _page(_SANDBOX_IDS[0], _SANDBOX_IDS[2]),
                ),
            ),
            (
                "did not return every run-owned sandbox",
                (
                    _page(*_SANDBOX_IDS[:2], next_cursor=_LIST_CURSOR),
                    _page("sbx_00000000-0000-0000-0000-999999999999"),
                ),
            ),
        )
        for summary, pages in cases:
            with self.subTest(summary=summary):
                with _server(MARKER, list_pages=pages) as (base_url, script):
                    native = _native_services(base_url, sandbox_list_pagination)
                    with mock.patch("space_smoke.native.time.sleep"):
                        result = _run(native, scenario=sandbox_list_pagination)

                self.assertEqual(result.verdict, Verdict.FAIL)
                self.assertIn(summary, result.summary)
                self.assertEqual(result.cleanup_failures, ())
                self.assertEqual(
                    [request.method for request in script.requests].count("DELETE"),
                    len(_SANDBOX_IDS),
                )

    def test_native_pagination_rejects_page_cap_and_malformed_pages(self) -> None:
        cases = (
            (
                ({"data": "not-an-array", "next_cursor": None},),
                Verdict.INFRA_ERROR,
                "field 'data' was not an array",
            ),
            (
                (_page(*_SANDBOX_IDS, next_cursor=""),),
                Verdict.INFRA_ERROR,
                "was not a string or null",
            ),
        )
        for pages, verdict, summary in cases:
            with self.subTest(summary=summary):
                with _server(MARKER, list_pages=pages) as (base_url, script):
                    native = _native_services(base_url, sandbox_list_pagination)
                    with mock.patch("space_smoke.native.time.sleep"):
                        result = _run(native, scenario=sandbox_list_pagination)

                self.assertEqual(result.verdict, verdict)
                self.assertIn(summary, result.summary)
                self.assertEqual(result.cleanup_failures, ())
                self.assertEqual(
                    [request.method for request in script.requests].count("DELETE"),
                    len(_SANDBOX_IDS),
                )

        endless_page = (_page(*_SANDBOX_IDS[:2], next_cursor=_LIST_CURSOR),)
        with _server(MARKER, list_pages=endless_page) as (base_url, script):
            native = _native_services(base_url, sandbox_list_pagination)
            with (
                mock.patch("space_smoke.native.time.sleep"),
                mock.patch("space_smoke.fixtures._SANDBOX_PAGE_CAP", 2),
            ):
                result = _run(native, scenario=sandbox_list_pagination)

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.summary, "sandbox pagination exceeded its page cap")
        self.assertEqual(result.cleanup_failures, ())
        self.assertEqual(script.list_requests, 2)

    def test_unsupported_native_scenarios_are_rejected_before_setup(self) -> None:
        unsupported = REGISTRY.get("ecw.binary_file_round_trip")
        with mock.patch("space_smoke.services.importlib.util.find_spec") as find_spec:
            with self.assertRaisesRegex(
                SmokeConfigurationError, "does not support the 'native' API surface"
            ):
                SmokeServices.from_environment(
                    unsupported, {"SPACE_SMOKE_API_SURFACE": "native"}
                )
        find_spec.assert_not_called()

        services = mock.Mock()
        with (
            mock.patch.dict(
                "space_smoke.__main__.os.environ",
                {"SPACE_SMOKE_API_SURFACE": "native"},
                clear=True,
            ),
            mock.patch(
                "space_smoke.__main__.SmokeServices.from_environment",
                return_value=services,
            ) as from_environment,
        ):
            self.assertEqual(_run_all(category="ecw"), 2)
        from_environment.assert_not_called()
        services.bootstrap.assert_not_called()

    def test_native_only_resident_pause_is_rejected_before_ecw_setup(self) -> None:
        self.assertEqual(resident_pause.api_surfaces, frozenset({"native"}))
        with mock.patch("space_smoke.services.importlib.util.find_spec") as find_spec:
            with self.assertRaisesRegex(
                SmokeConfigurationError, "does not support the 'ecw' API surface"
            ):
                SmokeServices.from_environment(
                    resident_pause, {"SPACE_SMOKE_API_SURFACE": "ecw"}
                )
        find_spec.assert_not_called()

    def test_description_and_rerun_retain_the_selected_surface(self) -> None:
        for scenario in (
            command_round_trip,
            pause_connect,
            background_process_control,
            sandbox_info,
            sandbox_delete,
            sandbox_list_pagination,
        ):
            with self.subTest(scenario=scenario.id):
                with (
                    mock.patch.dict(
                        "space_smoke.__main__.os.environ",
                        {"SPACE_SMOKE_API_SURFACE": "native"},
                        clear=True,
                    ),
                    redirect_stdout(StringIO()) as stdout,
                ):
                    self.assertEqual(_describe(scenario.id), 0)
                    rerun = _rerun_command(scenario)

                description = json.loads(stdout.getvalue())
                self.assertEqual(description["api_surfaces"], ["ecw", "native"])
                self.assertEqual(description["selected_api_surface"], "native")
                self.assertIn("SPACE_SMOKE_API_SURFACE=native", rerun)

    def test_native_credentials_reject_unsafe_urls_and_bypass_proxies(self) -> None:
        self.assertEqual(vars(native_module._NO_PROXY)["proxies"], {})
        with self.assertRaisesRegex(ValueError, "HTTPS.*loopback"):
            NativeConfig("http://api.example", "https://data.example", "token", "seed")
        factory = NativeSandboxFactory(
            NativeConfig("https://api.example", "https://data.example", "token", "seed")
        )
        with self.assertRaisesRegex(ValueError, "configured data-plane origin"):
            factory._collect_command("https://other.example/events", "token", timeout=1)

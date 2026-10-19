"""The M1 conformance check's parsing, error classes, cleanup and SDK scope."""

from __future__ import annotations

import io
import json
import os
import signal
import sys
import tempfile
import textwrap
import time
import types
import unittest
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib.error import HTTPError

import pydantic
import urllib3

from space_smoke import m1_conformance as m1
from space_smoke.runtime import InvariantViolation, ScenarioTimedOut

UPLOAD_CONFLICT = {
    "state": "failed",
    "terminal_error_code": "conflict",
    "terminal_error": {
        "message": "sandbox has a suspend-snapshot upload in flight; "
        "retry the DELETE after the snapshot finalizes"
    },
}


class FakeApiException(Exception):
    def __init__(self, status: int, body: object = None, headers: dict[str, str] | None = None):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body if isinstance(body, str) or body is None else json.dumps(body)
        self.headers = headers or {}


def fake_sdk() -> m1.GeneratedSdk:
    return m1.GeneratedSdk(
        ApiClient=lambda configuration: SimpleNamespace(configuration=configuration),
        Configuration=lambda host, access_token: SimpleNamespace(host=host),
        DefaultApi=lambda client: mock.Mock(),
        ApiException=FakeApiException,
        CreateCommandRequest=dict,
        CreateSandboxRequest=dict,
        ProcessSignalRequest=dict,
    )


def conformance() -> m1.M1Conformance:
    return m1.M1Conformance(
        fake_sdk(),
        api_url="http://127.0.0.1:8080",
        data_plane_url="http://127.0.0.1:8090",
        token="token",
        template="seed",
        runtime="fc",
        timeout=5,
        request_timeout=1,
    )


def envelope(code: str, retryable: bool = False, request_id: str | None = "req") -> dict[str, object]:
    return {"code": code, "retryable": retryable, "request_id": request_id}


def operation(state: str, **fields: object) -> SimpleNamespace:
    record = {"state": state, **fields}
    return SimpleNamespace(state=state, to_dict=lambda: dict(record))


def unparsable() -> pydantic.ValidationError:
    try:
        pydantic.TypeAdapter(int).validate_python("not a number")
    except pydantic.ValidationError as error:
        return error
    raise AssertionError("pydantic accepted a string as an int")


class SseParsingTest(unittest.TestCase):
    def test_events_keep_ids_names_and_multiline_data_and_skip_heartbeats(self) -> None:
        stream = [
            b": heartbeat\n",
            b"data: {\"type\": \"attached\"}\r\n",
            b"\n",
            b"event: stdout\n",
            b"id: 7\n",
            b"data: {\"type\":\n",
            b"data: \"stdout\"}\n",
            b"\n",
            b"\n",
            b"id: 8\n",
            b"data:{\"type\": \"process_exited\"}\n",
            b"\n",
        ]
        self.assertEqual(
            list(m1.iter_sse_events(stream)),
            [
                {"event": None, "id": None, "data": {"type": "attached"}},
                {"event": "stdout", "id": 7, "data": {"type": "stdout"}},
                {"event": None, "id": 8, "data": {"type": "process_exited"}},
            ],
        )


class OperationInventoryTest(unittest.TestCase):
    @staticmethod
    def document(operations: set[tuple[str, str, str]]) -> dict[str, object]:
        paths: dict[str, dict[str, object]] = {}
        for method, route, operation_id in operations:
            paths.setdefault(route, {"parameters": []})[method.lower()] = {
                "operationId": operation_id
            }
        return {"paths": paths}

    def test_exactly_the_release_operations_pass(self) -> None:
        m1.check_operations(self.document(set(m1.EXPECTED_OPERATIONS)))

    def test_a_missing_unexpected_or_malformed_operation_is_a_contract_failure(self) -> None:
        missing = set(m1.EXPECTED_OPERATIONS) - {("GET", "/sandboxes", "listSandboxes")}
        extra = set(m1.EXPECTED_OPERATIONS) | {("PATCH", "/sandboxes/{sandbox_id}", "updateSandbox")}
        cases: list[tuple[object, str]] = [
            (self.document(missing), "listSandboxes"),
            (self.document(extra), "updateSandbox"),
            ({"paths": {"/sandboxes": []}}, "not an object"),
            ({"paths": {"/sandboxes": {"get": "listSandboxes"}}}, "not an object"),
            ({"paths": {"/sandboxes": {"get": {"operationId": ["listSandboxes"]}}}}, "no string operationId"),
            ({"paths": {"/sandboxes": {"get": {}}}}, "no string operationId"),
            ([], "no paths"),
        ]
        for document, named in cases:
            with self.subTest(named=named), self.assertRaisesRegex(InvariantViolation, named):
                m1.check_operations(document)


def raised(call: Callable[[], object]) -> BaseException:
    try:
        call()
    except BaseException as error:
        return error
    raise AssertionError("the call did not raise")


class ErrorClassTest(unittest.TestCase):
    """Only transport, transient and timeout failures stay infrastructure errors."""

    def classify(self, error: BaseException) -> BaseException:
        try:
            with m1.contract_errors(fake_sdk()):
                raise error
        except BaseException as classified:
            return classified
        raise AssertionError("contract_errors swallowed the error")

    def test_contract_breaks_become_invariant_violations(self) -> None:
        cases = {
            "malformed SSE data": lambda: list(m1.iter_sse_events([b"data: {no\n", b"\n"])),
            "non-integer SSE id": lambda: list(m1.iter_sse_events([b"id: x\n", b"data: {}\n", b"\n"])),
            "missing data_base64": lambda: m1.stdout_of([{"type": "stdout"}]),
            "invalid data_base64": lambda: m1.stdout_of([{"type": "stdout", "data_base64": "!"}]),
            "process_started without process_id": lambda: [
                frame["process_id"] for frame in [{"type": "process_started"}]
            ],
            "non-object frame": lambda: [1, 2].get("type"),  # type: ignore[attr-defined]
            "model missing a field": lambda: SimpleNamespace().attach,
            "undecodable JSON response": lambda: json.loads("<html>"),
            "unparsable model": lambda: (_ for _ in ()).throw(unparsable()),
            "permanent HTTP error": lambda: (_ for _ in ()).throw(FakeApiException(500, envelope("internal"))),
            "transient status the server calls permanent": lambda: (_ for _ in ()).throw(
                FakeApiException(503, envelope("service_unavailable", retryable=False))
            ),
        }
        for label, call in cases.items():
            with self.subTest(label):
                self.assertIsInstance(self.classify(raised(call)), InvariantViolation)

    def test_infrastructure_errors_pass_through_unchanged(self) -> None:
        cases = [
            urllib3.exceptions.MaxRetryError(None, "/v1/sandboxes"),
            urllib3.exceptions.ProtocolError("connection aborted"),
            ConnectionResetError(),
            TimeoutError("operation did not complete"),
            ScenarioTimedOut("scenario execution timed out"),
            m1.GeneratorUnavailable("no image"),
            m1.GeneratedSdkLocationError("imported from elsewhere"),
            *(FakeApiException(status) for status in sorted(m1.TRANSIENT_HTTP_STATUSES)),
            FakeApiException(503, "<html>gateway</html>"),
            FakeApiException(503, envelope("service_unavailable", retryable=True)),
            FakeApiException(429, envelope("rate_limited", retryable=True)),
        ]
        for error in cases:
            with self.subTest(type(error).__name__, status=getattr(error, "status", None)):
                self.assertIs(self.classify(error), error)


class ErrorEnvelopeTest(unittest.TestCase):
    def test_negative_paths_require_the_documented_envelope(self) -> None:
        cases = (
            ("documented", FakeApiException(400, envelope("invalid_request")), None),
            ("wrong status", FakeApiException(404, envelope("not_found")), "expected HTTP 400"),
            ("wrong code", FakeApiException(400, envelope("not_found")), "expected invalid_request"),
            ("retryable", FakeApiException(400, envelope("invalid_request", True)), "retryable"),
            ("undocumented code", FakeApiException(400, envelope("teapot")), "documented values"),
            ("no request id", FakeApiException(400, envelope("invalid_request", request_id=None)), "request_id"),
            (
                "retry-after",
                FakeApiException(400, envelope("invalid_request"), {"Retry-After": "1"}),
                "Retry-After",
            ),
            ("non-JSON body", FakeApiException(400, "<html>"), "not a JSON document"),
            ("non-object body", FakeApiException(400, "[1]"), "not a JSON object"),
            ("success", None, "succeeded"),
        )
        check = conformance()
        for label, error, failure in cases:
            def call(error: Exception | None = error) -> None:
                if error is not None:
                    raise error

            with self.subTest(label):
                if failure is None:
                    check.expect_error(label, call, 400, "invalid_request", retryable=False)
                else:
                    with self.assertRaisesRegex(InvariantViolation, failure):
                        check.expect_error(label, call, 400, "invalid_request", retryable=False)

    def test_a_transient_status_is_infrastructure_only_without_a_permanent_envelope(self) -> None:
        for busy in (
            FakeApiException(503, "<html>gateway</html>"),
            FakeApiException(503, envelope("service_unavailable", True)),
            FakeApiException(0),
        ):
            def call(busy: Exception = busy) -> None:
                raise busy

            with self.subTest(body=busy.body), self.assertRaises(FakeApiException) as raised_error:
                conformance().expect_error("busy", call, 400, "invalid_request", retryable=False)
            self.assertIs(raised_error.exception, busy)

        def permanent() -> None:
            raise FakeApiException(503, envelope("service_unavailable", False))

        with self.assertRaisesRegex(InvariantViolation, "expected HTTP 400, got 503"):
            conformance().expect_error("permanent", permanent, 400, "invalid_request", retryable=False)

    def test_the_served_document_follows_the_same_rule(self) -> None:
        def answering(status: int, body: bytes) -> Callable[..., object]:
            def open_url(request: object, timeout: float) -> object:
                raise HTTPError("http://api.invalid/openapi-m1.json", status, "error", {}, io.BytesIO(body))  # type: ignore[arg-type]

            return open_url

        with self.assertRaises(HTTPError):
            m1.fetch_document("http://api.invalid", timeout=1, open_url=answering(503, b"<html>"))
        with self.assertRaisesRegex(InvariantViolation, "HTTP 503"):
            m1.fetch_document(
                "http://api.invalid",
                timeout=1,
                open_url=answering(503, json.dumps(envelope("service_unavailable")).encode()),
            )
        with self.assertRaisesRegex(InvariantViolation, "HTTP 404"):
            m1.fetch_document("http://api.invalid", timeout=1, open_url=answering(404, b""))


class DeleteTest(unittest.TestCase):
    def test_a_suspend_upload_conflict_is_retried_with_a_fresh_key(self) -> None:
        for cleanup in (False, True):
            with self.subTest(cleanup=cleanup):
                check = conformance()
                check.sandbox_id = "sbx_1"
                check.api.delete_sandbox.side_effect = [
                    SimpleNamespace(operation_id="op_1"),
                    SimpleNamespace(operation_id="op_2"),
                ]
                check.api.get_operation.side_effect = [
                    operation("failed", **{k: v for k, v in UPLOAD_CONFLICT.items() if k != "state"}),
                    operation("succeeded"),
                ]
                with mock.patch.object(m1.time, "sleep"):
                    check.delete_sandbox(cleanup=cleanup)
                keys = [call.args[0] for call in check.api.delete_sandbox.call_args_list]
                self.assertEqual(len(set(keys)), 2)
                self.assertEqual("deleteSandbox" in check.called, not cleanup)

    def test_any_other_failed_delete_is_a_contract_failure(self) -> None:
        check = conformance()
        check.sandbox_id = "sbx_1"
        check.api.delete_sandbox.return_value = SimpleNamespace(operation_id="op_1")
        check.api.get_operation.return_value = operation(
            "failed", terminal_error_code="internal", terminal_error={"message": "boom"}
        )
        with self.assertRaisesRegex(InvariantViolation, "ended in failed"):
            check.delete_sandbox(cleanup=False)
        check.api.delete_sandbox.assert_called_once()


class CleanupTest(unittest.TestCase):
    @staticmethod
    def raw(status: int, body: object) -> mock.Mock:
        return mock.Mock(status=status, data=json.dumps(body).encode())

    def test_an_unparsable_create_is_replayed_raw_and_its_sandbox_deleted(self) -> None:
        check = conformance()
        check.api.create_sandbox.side_effect = unparsable()
        check.api.create_sandbox_without_preload_content.return_value = self.raw(
            202, {"sandbox_id": "sbx_1", "operation_id": "op_0"}
        )
        check.api.delete_sandbox.side_effect = [
            FakeApiException(409, envelope("conflict")),
            SimpleNamespace(operation_id="op_1"),
        ]
        check.api.get_operation.return_value = operation("succeeded")
        with self.assertRaises(InvariantViolation):
            check.run()
        with mock.patch.object(m1.time, "sleep"):
            check.cleanup()
        replay = check.api.create_sandbox_without_preload_content.call_args
        self.assertEqual(replay.args[0], check.api.create_sandbox.call_args.args[0])
        self.assertEqual(check.api.delete_sandbox.call_args.args[1], "sbx_1")
        self.assertIsNone(check.sandbox_id)

    def test_a_rejected_create_or_an_absent_sandbox_needs_nothing_more(self) -> None:
        rejected = conformance()
        rejected.api.create_sandbox.side_effect = FakeApiException(400, envelope("invalid_request"))
        with self.assertRaises(InvariantViolation):
            rejected.run()
        rejected.cleanup()
        rejected.api.create_sandbox_without_preload_content.assert_not_called()
        rejected.api.delete_sandbox.assert_not_called()

        absent = conformance()
        absent.sandbox_id = "sbx_1"
        absent.api.delete_sandbox.side_effect = FakeApiException(404, envelope("not_found"))
        absent.cleanup()
        absent.api.get_operation.assert_not_called()

    def test_a_delete_the_api_refuses_fails_cleanup(self) -> None:
        check = conformance()
        check.sandbox_id = "sbx_1"
        check.api.delete_sandbox.side_effect = FakeApiException(403, envelope("forbidden"))
        with self.assertRaises(FakeApiException):
            check.cleanup()


class GeneratedSdkScopeTest(unittest.TestCase):
    def write_package(self, root: Path) -> Path:
        package = root / "openapi_client"
        for module in ("api", "models"):
            (package / module).mkdir(parents=True)
            (package / module / "__init__.py").write_text("")
        (package / "__init__.py").write_text("ApiClient = Configuration = object\n")
        (package / "exceptions.py").write_text("class ApiException(Exception):\n    pass\n")
        (package / "api" / "default_api.py").write_text("DefaultApi = object\n")
        for module, name in (
            ("create_command_request", "CreateCommandRequest"),
            ("create_sandbox_request", "CreateSandboxRequest"),
            ("process_signal_request", "ProcessSignalRequest"),
        ):
            (package / "models" / f"{module}.py").write_text(f"{name} = dict\n")
        return root

    def test_a_package_without_the_expected_modules_is_a_contract_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.write_package(Path(directory))
            (root / "openapi_client" / "models" / "create_sandbox_request.py").unlink()
            with self.assertRaisesRegex(InvariantViolation, "ModuleNotFoundError"):
                with m1.generated_sdk(root):
                    pass
            self.assertNotIn("openapi_client", sys.modules)

    def test_a_package_imported_from_elsewhere_stays_infrastructure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.write_package(Path(directory))
            elsewhere = SimpleNamespace(__file__="/elsewhere/openapi_client/__init__.py")
            with mock.patch.object(m1.importlib, "import_module", return_value=elsewhere):
                with self.assertRaises(m1.GeneratedSdkLocationError):
                    with m1.generated_sdk(root):
                        pass

    def test_a_client_whose_construction_changed_is_a_contract_failure(self) -> None:
        sdk = fake_sdk()
        changed = m1.GeneratedSdk(**{**sdk.__dict__, "Configuration": lambda: SimpleNamespace()})
        with self.assertRaisesRegex(InvariantViolation, "TypeError"):
            m1.M1Conformance(
                changed,
                api_url="http://127.0.0.1:8080",
                data_plane_url="http://127.0.0.1:8090",
                token="token",
                template="seed",
                runtime="fc",
                timeout=5,
                request_timeout=1,
            )

    def test_imports_are_undone_after_success_and_failure(self) -> None:
        prior = types.ModuleType("openapi_client")
        path = list(sys.path)
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            sys.modules, {"openapi_client": prior}
        ):
            root = self.write_package(Path(directory))
            with m1.generated_sdk(root) as sdk:
                self.assertTrue(issubclass(sdk.ApiException, Exception))
                self.assertIsNot(sys.modules["openapi_client"], prior)
            self.assertIs(sys.modules["openapi_client"], prior)
            self.assertNotIn("openapi_client.exceptions", sys.modules)
            self.assertEqual(sys.path, path)

            with self.assertRaises(RuntimeError), m1.generated_sdk(root):
                raise RuntimeError("the run failed")
            self.assertIs(sys.modules["openapi_client"], prior)
            self.assertEqual(
                [name for name in sys.modules if name.startswith("openapi_client.")], []
            )
            self.assertEqual(sys.path, path)


class GenerationTest(unittest.TestCase):
    def generate(
        self, script: str, *, timeout: float = 30
    ) -> tuple[m1.Generation | None, BaseException | None, list[int]]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / "generate.sh"
            fake.write_text(textwrap.dedent(script))
            pid_file = root / "children.pid"
            output = root / "generated"
            output.mkdir()
            with (
                mock.patch.object(m1, "GENERATION_SCRIPT", fake),
                mock.patch.object(m1, "_GENERATION_GRACE_SECONDS", 0.5),
                mock.patch.object(m1, "remove_generator_containers") as remove,
                mock.patch.dict(os.environ, {"CHILD_PID_FILE": str(pid_file)}),
            ):
                try:
                    result, error = m1.generate_sdk(root / "spec.json", output, timeout=timeout, label="k=v"), None
                except BaseException as raised_error:
                    result, error = None, raised_error
            remove.assert_called_once_with("k=v", strict=False)
            children = [int(pid) for pid in pid_file.read_text().split()] if pid_file.exists() else []
        return result, error, children

    def assert_ended(self, pids: list[int]) -> None:
        deadline = time.monotonic() + 5
        alive = list(pids)
        while alive and time.monotonic() < deadline:
            alive = [pid for pid in alive if _running(pid)]
            time.sleep(0.05)
        for pid in alive:
            os.kill(pid, signal.SIGKILL)
        self.assertEqual(alive, [], "a generator child outlived generate_sdk")

    def test_exit_statuses_separate_contract_breaks_from_an_unavailable_generator(self) -> None:
        for status, expected in ((0, None), (1, InvariantViolation), (m1.GENERATOR_UNAVAILABLE, m1.GeneratorUnavailable)):
            with self.subTest(status=status):
                result, error, _ = self.generate(f"echo generated\nexit {status}\n")
                assert result is not None and error is None
                self.assertEqual((result.returncode, result.output), (status, "generated\n"))
                if expected is None:
                    result.require_sdk()
                else:
                    with self.assertRaises(expected):
                        result.require_sdk()

    def test_a_timeout_ends_every_child_and_keeps_the_output(self) -> None:
        # A child ignoring SIGTERM outlives the script itself; a pull bounded
        # by `timeout --foreground`, as the script runs it, stays in the group.
        _, error, children = self.generate(
            """\
            echo started
            (trap '' TERM; exec sleep 60) &
            echo "$!" >>"$CHILD_PID_FILE"
            timeout --foreground 60 sleep 60 &
            echo "$!" >>"$CHILD_PID_FILE"
            wait
            """,
            timeout=1,
        )
        self.assertIsInstance(error, m1.GenerationTimedOut)
        assert isinstance(error, m1.GenerationTimedOut)
        self.assertEqual(error.output, "started\n")
        self.assertEqual(len(children), 2)
        self.assert_ended(children)

    def test_children_left_behind_by_a_finished_script_are_ended(self) -> None:
        result, error, children = self.generate(
            """\
            (trap '' TERM; exec sleep 60) >/dev/null 2>&1 &
            echo "$!" >>"$CHILD_PID_FILE"
            exit 0
            """,
        )
        self.assertIsNone(error)
        assert result is not None
        self.assertEqual(result.returncode, 0)
        self.assert_ended(children)


def _running(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as stat:
            return stat.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False

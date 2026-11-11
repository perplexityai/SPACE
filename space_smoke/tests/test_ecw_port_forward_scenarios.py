"""Characterization tests for reusable ECW public-port smoke helpers."""

from __future__ import annotations

import io
import subprocess
import tempfile
import unittest
from email.message import Message
from http.client import IncompleteRead
from pathlib import Path
from typing import Self, cast
from unittest import mock
from urllib.error import HTTPError

from space_smoke.capabilities import (
    EcwCommandResult,
    EcwFilesystem,
    EcwProcess,
    EcwPublicPort,
    EcwSandbox,
)
from space_smoke.fixtures import (
    ALL_INTERFACES,
    Ecw,
    EcwDelayedHttpStream,
    EcwHttpService,
    HttpResponse,
    HttpStreamResponse,
    _fixed_response_server_script,
    _streaming_server_script,
)
from space_smoke.runtime import InvariantViolation, ResourceScope, ScenarioContext
from space_smoke.scenarios.ecw_loopback_route_stays_open import (
    EXPECTED_LINES,
    MINIMUM_STREAM_SECONDS,
    loopback_route_stays_open,
)
from space_smoke.scenarios.ecw_public_port_to_all_interfaces import (
    EXPECTED_BODY as ALL_INTERFACES_BODY,
)
from space_smoke.scenarios.ecw_public_port_to_all_interfaces import (
    public_port_to_all_interfaces,
)


class _Files:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.contents: dict[str, str] = {}

    def make_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self.trace.append(f"mkdir:{path}:{user}:{request_timeout}")

    def write(
        self,
        path: str,
        data: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self.trace.append(f"write:{path}:{user}:{request_timeout}")
        self.contents[path] = data


class _Process:
    pid = 123

    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def kill(self) -> bool:
        self.trace.append("process.kill")
        return True


class _Sandbox:
    sandbox_id = "sandbox-port"

    def __init__(self) -> None:
        self.trace: list[str] = []
        self.files = cast(EcwFilesystem, _Files(self.trace))

    def run(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
    ) -> EcwCommandResult:
        self.trace.append(f"run:{command}:{timeout}:{user}")
        return EcwCommandResult(0, ALL_INTERFACES_BODY if command.startswith("cat ") else "", "")

    def start_background(self, command: str, *, timeout: int = 60) -> EcwProcess:
        self.trace.append(f"background:{command}:{timeout}")
        return cast(EcwProcess, _Process(self.trace))

    def public_port(self, port: int) -> EcwPublicPort:
        self.trace.append(f"public-port:{port}")
        return EcwPublicPort(
            f"https://{port}-{self.sandbox_id}.example.test",
            {"e2b-traffic-access-token": "token"},
        )


class _UrlResponse:
    status = 200

    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.body


class _TruncatedStreamResponse(_UrlResponse):
    def __iter__(self):  # type: ignore[no-untyped-def]
        yield b"0\n"
        yield b"1\n"
        raise IncompleteRead(b"2", 1)


def _fixture() -> tuple[Ecw, _Sandbox, ScenarioContext]:
    context = ScenarioContext(ResourceScope(), {})
    sandbox = _Sandbox()
    return Ecw(context, cast(EcwSandbox, sandbox)), sandbox, context


class EcwHttpServiceTests(unittest.TestCase):
    def test_fixed_response_script_emits_valid_http(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            body_path = Path(directory) / "response"
            body_path.write_text("hello", encoding="utf-8")

            completed = subprocess.run(
                ["sh", "-c", _fixed_response_server_script(str(body_path))],
                check=False,
                input=b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n",
                capture_output=True,
            )

            self.assertEqual(completed.returncode, 0)
            self.assertEqual(
                completed.stdout,
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n"
                b"Connection: close\r\n\r\nhello",
            )

    def test_streaming_script_emits_health_and_chunked_responses(self) -> None:
        script = _streaming_server_script(
            lines=("one", "two"),
            every_seconds=0.001,
        )
        health = subprocess.run(
            ["sh", "-c", script],
            check=False,
            input=b"GET /health?cb=1 HTTP/1.1\r\nHost: example.test\r\n\r\n",
            capture_output=True,
        )
        stream = subprocess.run(
            ["sh", "-c", script],
            check=False,
            input=b"GET /stream HTTP/1.1\r\nHost: example.test\r\n\r\n",
            capture_output=True,
        )

        self.assertEqual(health.returncode, 0)
        self.assertTrue(health.stdout.endswith(b"\r\n\r\nok\n"))
        self.assertEqual(stream.returncode, 0)
        self.assertTrue(
            stream.stdout.endswith(b"4\r\none\n\r\n4\r\ntwo\n\r\n0\r\n\r\n")
        )

    def test_public_get_retries_route_setup_and_preserves_access_headers(self) -> None:
        _test, sandbox, context = _fixture()
        first = HTTPError(
            "https://route",
            502,
            "not ready",
            Message(),
            io.BytesIO(b"not ready"),
        )
        service = EcwHttpService(context, cast(EcwSandbox, sandbox), 8123)

        with (
            mock.patch.object(context, "wait") as wait,
            mock.patch(
                "space_smoke.fixtures.urlopen",
                side_effect=[first, _UrlResponse(b"ready")],
            ) as open_url,
        ):
            response = service.request_from_public_port("/index.html")

        self.assertEqual(response, HttpResponse(200, "ready", 2))
        wait.assert_called_once_with(2.0)
        request = open_url.call_args_list[-1].args[0]
        self.assertIn("cb=", request.full_url)
        self.assertEqual(request.get_header("E2b-traffic-access-token"), "token")

    def test_truncated_chunked_response_becomes_invariant_evidence(self) -> None:
        _test, sandbox, context = _fixture()
        service = EcwHttpService(context, cast(EcwSandbox, sandbox), 8124)

        with mock.patch(
            "space_smoke.fixtures.urlopen",
            return_value=_TruncatedStreamResponse(b""),
        ):
            response = service.read_from_public_port("/stream", timeout=20)

        self.assertEqual(response.status, 200)
        self.assertEqual(response.lines, ("0", "1"))
        self.assertIn("IncompleteRead", response.error or "")

    def test_delayed_stream_establishes_its_health_route(self) -> None:
        _test, sandbox, context = _fixture()
        stream = EcwDelayedHttpStream(context, cast(EcwSandbox, sandbox), 8124)

        with mock.patch(
            "space_smoke.fixtures.urlopen",
            return_value=_UrlResponse(b"ok\n"),
        ) as open_url:
            stream.establish_public_route()

        request = open_url.call_args.args[0]
        self.assertIn("/health?", request.full_url)
        self.assertEqual(context.evidence["http_status"], 200)

    def test_service_process_cleanup_is_armed_before_listener_verification(
        self,
    ) -> None:
        test, sandbox, context = _fixture()

        with (
            mock.patch.object(
                Ecw,
                "run_command",
                return_value=EcwCommandResult(1, "", "listener missing"),
            ),
            self.assertRaisesRegex(InvariantViolation, "listener did not bind"),
        ):
            test.serve_http(
                listening_on=ALL_INTERFACES,
                port=8123,
                response="unreachable",
            )

        self.assertEqual(context.scope.close(), ())
        self.assertEqual(sandbox.trace[-1], "process.kill")


class EcwPublicPortScenarioTests(unittest.TestCase):
    def test_every_port_scenario_requires_authenticated_ingress(self) -> None:
        for scenario in (
            public_port_to_all_interfaces,
            loopback_route_stays_open,
        ):
            with self.subTest(scenario=scenario.id):
                self.assertIsNotNone(scenario.sandbox.network)
                assert scenario.sandbox.network is not None
                self.assertFalse(scenario.sandbox.network.allow_public_traffic)

    def test_scenario_names_and_proves_the_all_interface_path(self) -> None:
        test, sandbox, context = _fixture()

        with mock.patch.object(
            EcwHttpService,
            "request_from_public_port",
            side_effect=[HttpResponse(200, ALL_INTERFACES_BODY, 1)]
            + [HttpResponse(502, "Sandbox service is not reachable", 1)] * 20
            + [HttpResponse(200, ALL_INTERFACES_BODY, 1)],
        ):
            public_port_to_all_interfaces.verify(test)

        self.assertEqual(context.scope.close(), ())

        self.assertTrue(
            any("--listen=0.0.0.0:8123" in item for item in sandbox.trace),
            sandbox.trace,
        )
        self.assertTrue(
            any(
                item.startswith("background:") and item.endswith(":0")
                for item in sandbox.trace
            ),
            sandbox.trace,
        )
        self.assertEqual(sandbox.trace[-1], "process.kill")
        self.assertEqual(sandbox.trace.count("process.kill"), 2)

    def test_port_server_uses_only_seed_builtins(self) -> None:
        test, sandbox, context = _fixture()

        with mock.patch.object(
            EcwHttpService,
            "request_from_public_port",
            side_effect=[HttpResponse(200, ALL_INTERFACES_BODY, 1)]
            + [HttpResponse(502, "Sandbox service is not reachable", 1)] * 20
            + [HttpResponse(200, ALL_INTERFACES_BODY, 1)],
        ):
            public_port_to_all_interfaces.verify(test)

        self.assertEqual(context.scope.close(), ())
        guest_commands = "\n".join(sandbox.trace)
        self.assertNotIn("apt-get", guest_commands)
        self.assertNotIn("python3", guest_commands)
        self.assertIn("systemd-socket-activate", guest_commands)


    def test_long_lived_scenario_requires_every_delayed_chunk(self) -> None:
        test, sandbox, context = _fixture()
        with (
            mock.patch.object(context, "wait"),
            mock.patch.object(
                EcwDelayedHttpStream,
                "establish_public_route",
            ),
            mock.patch.object(
                EcwDelayedHttpStream,
                "read_from_public_port",
                return_value=HttpStreamResponse(
                    200,
                    EXPECTED_LINES,
                    MINIMUM_STREAM_SECONDS,
                ),
            ),
        ):
            loopback_route_stays_open.verify(test)

        self.assertEqual(context.scope.close(), ())

        stream_script = next(
            content
            for path, content in cast(_Files, sandbox.files).contents.items()
            if path.endswith(".sh")
        )
        for line in EXPECTED_LINES:
            self.assertIn(f"printf '%s\\n' {line}", stream_script)
        self.assertTrue(
            any("--listen=127.0.0.1:8124" in item for item in sandbox.trace),
            sandbox.trace,
        )
        self.assertEqual(sandbox.trace[-1], "process.kill")

    def test_long_lived_scenario_rejects_truncation_after_every_data_line(
        self,
    ) -> None:
        test, _sandbox, context = _fixture()
        with (
            mock.patch.object(context, "wait"),
            mock.patch.object(
                EcwDelayedHttpStream,
                "establish_public_route",
            ),
            mock.patch.object(
                EcwDelayedHttpStream,
                "read_from_public_port",
                return_value=HttpStreamResponse(
                    200,
                    EXPECTED_LINES,
                    MINIMUM_STREAM_SECONDS,
                    "IncompleteRead: response ended early",
                ),
            ),
            self.assertRaisesRegex(InvariantViolation, "public-port stream"),
        ):
            loopback_route_stays_open.verify(test)

        self.assertEqual(context.scope.close(), ())

    def test_complete_stream_expectation_rejects_an_immediate_fixture(self) -> None:
        test, _sandbox, _context = _fixture()
        response = HttpStreamResponse(200, EXPECTED_LINES, 0.01)

        with self.assertRaisesRegex(InvariantViolation, "required duration"):
            test.expect_complete_http_stream(
                response,
                lines=EXPECTED_LINES,
                lasting_at_least_seconds=MINIMUM_STREAM_SECONDS,
            )

    def test_all_interface_scenario_rejects_the_wrong_body(self) -> None:
        test, _sandbox, context = _fixture()

        with (
            mock.patch.object(
                EcwHttpService,
                "request_from_public_port",
                return_value=HttpResponse(200, "wrong", 1),
            ),
            self.assertRaisesRegex(InvariantViolation, "public-port response"),
        ):
            public_port_to_all_interfaces.verify(test)

        self.assertEqual(context.scope.close(), ())


if __name__ == "__main__":
    unittest.main()

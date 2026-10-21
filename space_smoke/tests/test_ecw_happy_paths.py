"""Characterization tests for the small ECW happy-path scenarios."""

from __future__ import annotations

import time
import unittest
from datetime import UTC, datetime
from typing import cast
from unittest import mock

from space_smoke.capabilities import (
    CommandSandbox,
    EcwCommandResult,
    EcwSandbox,
    EcwSandboxInfo,
    EcwSandboxState,
)
from space_smoke.fixtures import CommandSandboxFixture, Ecw
from space_smoke.model import Verdict
from space_smoke.runtime import (
    InvariantViolation,
    ResourceScope,
    ScenarioContext,
    SmokeConfigurationError,
    execute_scenario,
)
from space_smoke.scenarios.ecw_filesystem_round_trip import (
    CONTENT,
    PATH,
    filesystem_round_trip,
)
from space_smoke.scenarios.ecw_pause_connect import (
    MARKER as PAUSE_MARKER,
    PATH as PAUSE_PATH,
    pause_connect,
)
from space_smoke.scenarios.ecw_refresh_keepalive import (
    REFRESH_AFTER_SECONDS,
    TIMEOUT_SECONDS as REFRESH_TIMEOUT_SECONDS,
    VERIFY_AFTER_REFRESH_SECONDS,
    refresh_keepalive,
)
from space_smoke.scenarios.ecw_sandbox_info import sandbox_info
from space_smoke.scenarios.ecw_timeout_rearm import (
    RESET_AFTER_SECONDS,
    TIMEOUT_SECONDS as REARM_TIMEOUT_SECONDS,
    VERIFY_AFTER_RESET_SECONDS,
    timeout_rearm,
)
from space_smoke.scenarios.ecw_timeout_update import (
    DEADLINE_MARGIN_SECONDS,
    EXTENDED_TIMEOUT_SECONDS,
    INITIAL_TIMEOUT_SECONDS,
    MARKER as TIMEOUT_MARKER,
    RECONNECT_TIMEOUT_SECONDS,
    SCHEDULER_TICK_SECONDS,
    SUSPENDED_TIMEOUT_SECONDS,
    timeout_update,
)


class _Filesystem:
    def __init__(self, trace: list[str]) -> None:
        self._trace = trace
        self._files: dict[str, str] = {}

    def write(
        self,
        path: str,
        data: str | bytes,
        *,
        request_timeout: float = 60,
    ) -> None:
        content = data.decode() if isinstance(data, bytes) else data
        self._trace.append(f"files.write:{path}:{content}:{request_timeout}")
        self._files[path] = content

    def read(self, path: str, *, request_timeout: float = 60) -> str:
        self._trace.append(f"files.read:{path}:{request_timeout}")
        return self._files[path]


class _Sandbox:
    sandbox_id = "sandbox-happy-path"

    def __init__(
        self,
        *,
        state: EcwSandboxState | None = EcwSandboxState.RUNNING,
        info_sandbox_id: str | None = None,
        timeout: int = INITIAL_TIMEOUT_SECONDS,
    ) -> None:
        self.trace: list[str] = []
        self.files = _Filesystem(self.trace)
        self._command_files: dict[str, str] = {}
        self.state = state
        self.info_sandbox_id = info_sandbox_id or self.sandbox_id
        self._timeout = timeout
        self._end_at = time.time() + timeout

    def run(self, command: str, *, timeout: int = 60) -> EcwCommandResult:
        self.trace.append(f"run:{command}:{timeout}")
        if command.startswith("printf %s "):
            marker_and_path = command.removeprefix("printf %s ")
            if " > " in marker_and_path:
                marker, path = marker_and_path.split(" > ", 1)
                self._command_files[path] = marker
                return EcwCommandResult(0, "", "")
            return EcwCommandResult(0, marker_and_path, "")
        if command.startswith("cat "):
            return EcwCommandResult(0, self._command_files[command[4:]], "")
        raise AssertionError(f"unexpected command: {command}")

    def get_info(self) -> EcwSandboxInfo:
        self.trace.append("get_info")
        return EcwSandboxInfo(
            self.info_sandbox_id,
            "seed",
            self.state,
            end_at=datetime.fromtimestamp(self._end_at, UTC),
        )

    def try_get_info(self) -> EcwSandboxInfo | None:
        self.trace.append("try_get_info")
        return self.get_info()

    def set_timeout(self, timeout: int) -> None:
        self.trace.append(f"set_timeout:{timeout}")
        self._timeout = timeout
        if self.state is EcwSandboxState.RUNNING:
            self._end_at = time.time() + timeout

    def refresh(self) -> None:
        self.trace.append("refresh")

    def pause(self) -> None:
        self.trace.append("pause")
        self.state = EcwSandboxState.PAUSED

    def connect(self, *, timeout: int = 300) -> _Sandbox:
        self.trace.append(f"connect:{timeout}")
        self.state = EcwSandboxState.RUNNING
        self._end_at = time.time() + max(timeout, self._timeout)
        return self

    def kill(self) -> None:
        self.trace.append("kill")


def _fixture(sandbox: _Sandbox, *, timeout: int = 300) -> Ecw:
    return Ecw(
        ScenarioContext(ResourceScope(), {}),
        cast(EcwSandbox, sandbox),
        _sandbox_timeout=timeout,
    )


class EcwHappyPathScenarioTests(unittest.TestCase):
    def test_sandbox_info_is_one_read_and_id_check(self) -> None:
        sandbox = _Sandbox()

        sandbox_info.verify(_fixture(sandbox))

        self.assertEqual(sandbox.trace, ["get_info"])

    def test_sandbox_info_id_mismatch_keeps_the_shared_failure(self) -> None:
        sandbox = _Sandbox(info_sandbox_id="sandbox-other")

        def body(context: ScenarioContext) -> None:
            sandbox_info.verify(
                CommandSandboxFixture(context, cast(CommandSandbox, sandbox))
            )

        result = execute_scenario(title=sandbox_info.title, body=body)

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.summary, "sandbox information returned a different ID")
        self.assertEqual(result.evidence["expected_sandbox_id"], sandbox.sandbox_id)
        self.assertEqual(result.evidence["observed_sandbox_id"], "sandbox-other")
        self.assertEqual(sandbox.trace, ["get_info"])

    def test_timeout_update_extends_then_proves_the_sandbox_is_usable(self) -> None:
        sandbox = _Sandbox()

        with mock.patch("space_smoke.runtime.time.sleep") as sleep:
            timeout_update.verify(_fixture(sandbox))

        self.assertEqual(timeout_update.sandbox.timeout, INITIAL_TIMEOUT_SECONDS)
        # Each wait runs out a deadline plus one scheduler tick and the margin:
        # the original deadline, then the longest earlier timeout after restore.
        settle = SCHEDULER_TICK_SECONDS + DEADLINE_MARGIN_SECONDS
        waits = [call.args[0] for call in sleep.call_args_list]
        self.assertEqual(len(waits), 2)
        self.assertAlmostEqual(waits[0], INITIAL_TIMEOUT_SECONDS + settle, delta=1)
        self.assertAlmostEqual(waits[1], EXTENDED_TIMEOUT_SECONDS + settle, delta=1)
        self.assertEqual(
            sandbox.trace,
            [
                "get_info",
                f"set_timeout:{EXTENDED_TIMEOUT_SECONDS}",
                "get_info",
                "get_info",
                f"run:printf %s {TIMEOUT_MARKER}:60",
                "get_info",
                "pause",
                "try_get_info",
                "get_info",
                f"set_timeout:{SUSPENDED_TIMEOUT_SECONDS}",
                "get_info",
                f"connect:{RECONNECT_TIMEOUT_SECONDS}",
                "get_info",
                "try_get_info",
                "get_info",
                f"run:printf %s {TIMEOUT_MARKER}:60",
            ],
        )

    def test_same_value_timeout_rearm_crosses_the_original_deadline(self) -> None:
        sandbox = _Sandbox()

        with mock.patch("space_smoke.runtime.time.sleep") as sleep:
            timeout_rearm.verify(_fixture(sandbox, timeout=REARM_TIMEOUT_SECONDS))

        self.assertEqual(timeout_rearm.sandbox.timeout, REARM_TIMEOUT_SECONDS)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [RESET_AFTER_SECONDS, VERIFY_AFTER_RESET_SECONDS],
        )
        self.assertEqual(sandbox.trace[0], f"set_timeout:{REARM_TIMEOUT_SECONDS}")
        self.assertEqual(sandbox.trace[-2], "get_info")
        self.assertEqual(
            sandbox.trace[-1],
            "run:printf %s space-smoke-idle-deadline-reset-ok:60",
        )
        self.assertGreater(
            RESET_AFTER_SECONDS + VERIFY_AFTER_RESET_SECONDS,
            REARM_TIMEOUT_SECONDS,
        )
        self.assertLess(VERIFY_AFTER_RESET_SECONDS, REARM_TIMEOUT_SECONDS)
        self.assertGreaterEqual(
            REARM_TIMEOUT_SECONDS - VERIFY_AFTER_RESET_SECONDS,
            10,
        )

    def test_refresh_keepalive_crosses_the_original_deadline(self) -> None:
        sandbox = _Sandbox()

        with mock.patch("space_smoke.runtime.time.sleep") as sleep:
            refresh_keepalive.verify(_fixture(sandbox, timeout=REFRESH_TIMEOUT_SECONDS))

        self.assertEqual(refresh_keepalive.sandbox.timeout, REFRESH_TIMEOUT_SECONDS)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [REFRESH_AFTER_SECONDS, VERIFY_AFTER_REFRESH_SECONDS],
        )
        self.assertEqual(sandbox.trace[0], "refresh")
        self.assertEqual(sandbox.trace[-2], "get_info")
        self.assertEqual(
            sandbox.trace[-1],
            "run:printf %s space-smoke-idle-deadline-reset-ok:60",
        )
        self.assertGreater(
            REFRESH_AFTER_SECONDS + VERIFY_AFTER_REFRESH_SECONDS,
            REFRESH_TIMEOUT_SECONDS,
        )
        self.assertLess(VERIFY_AFTER_REFRESH_SECONDS, REFRESH_TIMEOUT_SECONDS)
        self.assertGreaterEqual(
            REFRESH_TIMEOUT_SECONDS - VERIFY_AFTER_REFRESH_SECONDS,
            10,
        )

    def test_idle_deadline_proof_rejects_insufficient_completion_headroom(
        self,
    ) -> None:
        sandbox = _Sandbox()

        with self.assertRaisesRegex(
            SmokeConfigurationError,
            "reserve at least 10 seconds",
        ):
            _fixture(sandbox, timeout=20).prove_idle_deadline_reset(
                reset_after=10,
                verify_after_reset=17,
                reset=sandbox.refresh,
            )

    def test_refresh_rejection_is_a_failed_invariant_with_public_state(self) -> None:
        sandbox = _Sandbox()
        sandbox.refresh = mock.Mock(  # type: ignore[method-assign]
            side_effect=RuntimeError("ECW POST returned HTTP 500: reset failed")
        )

        def body(context: ScenarioContext) -> None:
            refresh_keepalive.verify(
                Ecw(
                    context,
                    cast(EcwSandbox, sandbox),
                    _sandbox_timeout=REFRESH_TIMEOUT_SECONDS,
                )
            )

        with mock.patch("space_smoke.runtime.time.sleep"):
            result = execute_scenario(title="Refresh", body=body)

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.evidence["operation"], "reset idle deadline")
        self.assertEqual(result.evidence["operation_error_type"], "RuntimeError")
        self.assertEqual(result.evidence["observed_state"], "running")
        self.assertEqual(result.diagnostics, ())

    def test_terminal_command_rejection_is_a_failed_invariant(self) -> None:
        sandbox = _Sandbox()
        sandbox.run = mock.Mock(  # type: ignore[method-assign]
            side_effect=RuntimeError("sandbox route is unavailable")
        )

        def body(context: ScenarioContext) -> None:
            timeout_rearm.verify(
                Ecw(
                    context,
                    cast(EcwSandbox, sandbox),
                    _sandbox_timeout=REARM_TIMEOUT_SECONDS,
                )
            )

        with mock.patch("space_smoke.runtime.time.sleep"):
            result = execute_scenario(title="Timeout rearm", body=body)

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.evidence["operation"], "run marker command")
        self.assertEqual(result.evidence["operation_error_type"], "RuntimeError")
        self.assertEqual(result.evidence["observed_state"], "running")
        self.assertEqual(result.diagnostics, ())

    def test_non_running_terminal_state_fails_before_the_marker_command(self) -> None:
        sandbox = _Sandbox(state=EcwSandboxState.PAUSED)

        def body(context: ScenarioContext) -> None:
            refresh_keepalive.verify(
                Ecw(
                    context,
                    cast(EcwSandbox, sandbox),
                    _sandbox_timeout=REFRESH_TIMEOUT_SECONDS,
                )
            )

        with mock.patch("space_smoke.runtime.time.sleep"):
            result = execute_scenario(title="Refresh", body=body)

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.evidence["expected_state"], "running")
        self.assertEqual(result.evidence["observed_state"], "paused")
        self.assertFalse(any(item.startswith("run:") for item in sandbox.trace))

    def test_filesystem_round_trip_is_one_write_and_read(self) -> None:
        sandbox = _Sandbox()

        filesystem_round_trip.verify(_fixture(sandbox))

        self.assertEqual(
            sandbox.trace,
            [
                f"files.write:{PATH}:{CONTENT}:60",
                f"files.read:{PATH}:60",
            ],
        )

    def test_pause_connect_reads_the_state_written_before_pause(self) -> None:
        sandbox = _Sandbox()
        marker = f"{PAUSE_MARKER}-{sandbox.sandbox_id}"

        pause_connect.verify(_fixture(sandbox))

        self.assertEqual(pause_connect.api_surfaces, frozenset({"ecw", "native"}))
        self.assertEqual(
            sandbox.trace,
            [
                f"run:printf %s {marker} > {PAUSE_PATH}:60",
                "pause",
                "connect:300",
                f"run:cat {PAUSE_PATH}:60",
            ],
        )

    def test_filesystem_mismatch_is_an_invariant_failure(self) -> None:
        sandbox = _Sandbox()
        sandbox.files.read = lambda *_args, **_kwargs: "wrong"  # type: ignore[method-assign]

        with self.assertRaisesRegex(InvariantViolation, "did not return"):
            filesystem_round_trip.verify(_fixture(sandbox))


if __name__ == "__main__":
    unittest.main()

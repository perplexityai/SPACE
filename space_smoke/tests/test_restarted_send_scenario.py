"""Contracts behind the restarted-send scenario the bench cannot exercise.

The live run proves a restarted kernel send is refused and retried. These pin
the transfer-job read (attempts and the last error); the rejection verdict
requiring both a rise in node-manager's rejected-stream counter over the
interrupt and the sandbox's own send job completing after a retry, and
ignoring jobs of another kind or path; a bench without Prometheus being
inconclusive; the interrupter's lifecycle (always ended, a pulse required);
and the on-node scripts rendering to valid shell.
"""

from __future__ import annotations

import shutil
import subprocess
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke.adapters import (
    BTRFS_SEND_STAGING_ROOT,
    KindConfig,
    KindHarness,
    _BTRFS_SEND_INTERRUPT_END,
    _BTRFS_SEND_INTERRUPTER,
    _btrfs_send_interrupt_stop_file,
)
from space_smoke.capabilities import TransferJobView
from space_smoke.fixtures import SuspendCodecAssertions
from space_smoke.runtime import (
    CommandResult,
    InconclusiveSmoke,
    InvariantViolation,
    ResourceScope,
)

SANDBOX_ID = "01a0ac88-c99e-735a-b8fa-f13c948332fa"
JOB_ID = "01a0ac88-d000-7000-8000-000000000001"
SEND_PATH = f"/space/cache/firecracker/sandboxes/{SANDBOX_ID}/snapshot/rootfs.btrfs-send"
REJECTED = (
    "stream snapshot upload: btrfs send source failed: status: Aborted, message: "
    '"btrfs send stream rejected: send command 15 at offset 1048593 fails its CRC"'
)


def _job(
    status: str,
    attempts: int,
    *,
    kind: str = "push_btrfs_send",
    local_path: str = SEND_PATH,
    job_id: str = JOB_ID,
) -> TransferJobView:
    return TransferJobView(job_id, kind, status, local_path, "", attempts)


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SANDBOX_ID)
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, key: str, value: object) -> None:
        self.records[key] = value

    def wait(self, _seconds: float) -> None:
        pass

    def phase(self, _name: str):
        return nullcontext()


class _Control:
    """A worker whose rejected-stream counter reads `rejections`, one value per read."""

    def __init__(
        self,
        *,
        rejections: list[dict[str, float]] | None = None,
        jobs: list[TransferJobView] | None = None,
        mode: str = "otel",
    ) -> None:
        self.rejections = list(rejections or [{}])
        self.jobs = list(jobs or [])
        self.mode = mode
        self.interrupter_output = ["pulse 1", "pulses 1"]
        self.calls: list[str] = []

    def telemetry_mode(self) -> str:
        return self.mode

    def telemetry_query(self, backend: str, path: str, params) -> Any:
        counts = self.rejections.pop(0) if len(self.rejections) > 1 else self.rejections[0]
        return {
            "data": {
                "result": [
                    {"metric": {"reason": reason}, "value": [0, str(value)]}
                    for reason, value in counts.items()
                ]
            }
        }

    def transfer_jobs(self, node_name: str, sandbox_id: str) -> list[TransferJobView]:
        return list(self.jobs)

    def transfer_job_log(self, node_name: str, *, since_time: str) -> list[str]:
        return []

    def interrupt_btrfs_send(
        self, node_name: str, *, token: str, pulses: int, window_seconds: int
    ) -> list[str]:
        self.calls.append(f"interrupt {token}")
        return list(self.interrupter_output)

    def end_btrfs_send_interrupt(self, node_name: str, *, token: str) -> None:
        self.calls.append(f"end {token}")


def _assertions(control: _Control) -> tuple[_Fixture, SuspendCodecAssertions, ResourceScope]:
    fixture = _Fixture()
    scope = ResourceScope()
    return fixture, SuspendCodecAssertions(cast(Any, fixture), cast(Any, control), scope), scope


def _interrupted(control: _Control) -> tuple[_Fixture, SuspendCodecAssertions]:
    fixture, assertions, _ = _assertions(control)
    with assertions.interrupting_btrfs_send("worker-a"):
        pass
    return fixture, assertions


def setUpModule() -> None:
    # The baseline is the first read: no export lag to wait out here.
    patch = mock.patch("space_smoke.fixtures.STREAM_REJECTIONS_SETTLE_SECONDS", 0)
    patch.start()
    unittest.addModuleCleanup(patch.stop)


class _ShowJobKubectl:
    def __init__(self, shown: str) -> None:
        self.shown = shown

    def run(self, command, **_options) -> CommandResult:
        argv = tuple(command)
        if "exec" not in argv:
            # The node-shell pod lookup.
            return CommandResult(argv, 0, "space-pod-x", "")
        if argv[-1].startswith("space-volmgr jobs "):
            return CommandResult(argv, 0, f"│ {JOB_ID} ┆ pending ┆ push_btrfs_send ┆\n", "")
        return CommandResult(argv, 0, self.shown, "")


class TransferJobRead(unittest.TestCase):
    def test_reads_attempts_and_the_whole_error_line(self) -> None:
        shown = "\n".join(
            [
                f"id: {JOB_ID}",
                "status: pending",
                "kind: push_btrfs_send",
                f"local_path: {SEND_PATH}",
                "attempts: 1",
                f"error: {REJECTED}",
            ]
        )
        harness = KindHarness(_ShowJobKubectl(shown), KindConfig())
        self.assertEqual(
            harness.transfer_jobs("worker-a", SANDBOX_ID),
            [TransferJobView(JOB_ID, "push_btrfs_send", "pending", SEND_PATH, REJECTED, 1)],
        )


class RejectedSendVerdict(unittest.TestCase):
    def _verdict(self, *, rejections_after: dict[str, float], jobs: list[TransferJobView]):
        control = _Control(rejections=[{"crc": 2.0}, rejections_after], jobs=jobs)
        fixture, assertions = _interrupted(control)
        return fixture, lambda: assertions.expect_rejected_send_stream(
            "worker-a", since_time="t", timeout=0
        )

    def test_a_counted_rejection_and_a_retried_job_pass(self) -> None:
        fixture, verdict = self._verdict(
            rejections_after={"crc": 3.0}, jobs=[_job("completed", 2)]
        )
        self.assertEqual(verdict().job_id, JOB_ID)
        self.assertEqual(
            fixture.records["rejected_send"],
            {"job_id": JOB_ID, "attempts": 2, "stream_rejections": {"crc": 1.0}},
        )

    def test_no_counted_rejection_fails_closed(self) -> None:
        _, verdict = self._verdict(rejections_after={"crc": 2.0}, jobs=[_job("completed", 2)])
        with self.assertRaises(InvariantViolation):
            verdict()

    def test_a_job_that_never_retried_fails_closed(self) -> None:
        _, verdict = self._verdict(rejections_after={"crc": 3.0}, jobs=[_job("completed", 1)])
        with self.assertRaises(InvariantViolation):
            verdict()

    def test_a_permanently_failed_job_fails_closed(self) -> None:
        _, verdict = self._verdict(rejections_after={"crc": 3.0}, jobs=[_job("failed", 1)])
        with self.assertRaises(InvariantViolation):
            verdict()

    def test_a_retry_on_another_kind_or_path_is_ignored(self) -> None:
        other_path = SEND_PATH.replace("rootfs.btrfs-send", "memfile")
        _, verdict = self._verdict(
            rejections_after={"crc": 3.0},
            jobs=[
                _job("completed", 1),
                _job("completed", 3, kind="push", job_id="01a0ac88-d000-7000-8000-000000000002"),
                _job("completed", 3, local_path=other_path, job_id="01a0ac88-d000-7000-8000-000000000003"),
            ],
        )
        with self.assertRaises(InvariantViolation):
            verdict()

    def test_a_bench_without_prometheus_is_inconclusive(self) -> None:
        control = _Control(mode="datadog")
        _, assertions, _ = _assertions(control)
        with self.assertRaises(InconclusiveSmoke):
            with assertions.interrupting_btrfs_send("worker-a"):
                pass
        self.assertEqual(control.calls, [])


class Interrupter(unittest.TestCase):
    def test_a_pulse_passes_and_the_interrupter_is_ended(self) -> None:
        control = _Control()
        fixture, assertions, scope = _assertions(control)
        with assertions.interrupting_btrfs_send("worker-a"):
            pass
        self.assertEqual(fixture.records["send_interrupt"], ["pulse 1", "pulses 1"])
        self.assertEqual(sorted(call.split()[0] for call in control.calls), ["end", "interrupt"])
        # Ended eagerly: nothing left for scope close.
        self.assertEqual(scope.close(), ())
        self.assertEqual(len(control.calls), 2)

    def test_no_pulse_fails_closed(self) -> None:
        control = _Control()
        control.interrupter_output = ["pulses 0"]
        _, assertions, _ = _assertions(control)
        with self.assertRaises(InvariantViolation):
            with assertions.interrupting_btrfs_send("worker-a"):
                pass

    def test_a_failed_body_still_ends_the_interrupter(self) -> None:
        control = _Control()
        _, assertions, _ = _assertions(control)
        with self.assertRaises(RuntimeError):
            with assertions.interrupting_btrfs_send("worker-a"):
                raise RuntimeError("suspend failed")
        self.assertIn("end", [call.split()[0] for call in control.calls])


class OnNodeScripts(unittest.TestCase):
    def test_stop_file_refuses_a_token_that_is_not_hex(self) -> None:
        self.assertEqual(
            _btrfs_send_interrupt_stop_file("0123abcd"), "/tmp/space-smoke-send-interrupt-0123abcd"
        )
        for token in ("", "../x", "ABCDEF01", "0123abcd; rm"):
            with self.assertRaises(ValueError):
                _btrfs_send_interrupt_stop_file(token)

    @unittest.skipIf(shutil.which("bash") is None, "bash is not installed")
    def test_scripts_render_to_valid_shell(self) -> None:
        stop = _btrfs_send_interrupt_stop_file("0123abcd")
        for script in (
            _BTRFS_SEND_INTERRUPTER.format(stop=stop, pulses=1, window=900, staging=BTRFS_SEND_STAGING_ROOT),
            _BTRFS_SEND_INTERRUPT_END.format(stop=stop),
        ):
            checked = subprocess.run(["bash", "-n", "-c", script], capture_output=True, text=True)
            self.assertEqual(checked.returncode, 0, checked.stderr)


if __name__ == "__main__":
    unittest.main()

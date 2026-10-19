"""Contracts behind the restarted-send scenario the bench cannot exercise.

The live run proves a restarted kernel send is refused and retried. These pin
the failed-attempt reader (matched by message and sandbox path), the
rejection assertion failing closed on a missing or permanent failure, the
interrupter's lifecycle (always ended, a pulse required), and the on-node
scripts rendering to valid shell.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import (
    BTRFS_SEND_STAGING_ROOT,
    TRANSFER_JOB_FAILED_MESSAGE,
    _BTRFS_SEND_INTERRUPT_END,
    _BTRFS_SEND_INTERRUPTER,
    _btrfs_send_interrupt_stop_file,
    _failed_transfers_from_logs,
)
from space_smoke.capabilities import FailedTransfer
from space_smoke.fixtures import SuspendCodecAssertions
from space_smoke.runtime import InvariantViolation, ResourceScope

SANDBOX_ID = "01a0ac88-c99e-735a-b8fa-f13c948332fa"
SEND_PATH = f"/space/cache/firecracker/sandboxes/{SANDBOX_ID}/snapshot/rootfs.btrfs-send"
REJECTED = (
    "stream snapshot upload: btrfs send source failed: status: Aborted, message: "
    '"btrfs send stream rejected: send command 15 at offset 1048593 fails its CRC"'
)


def _failed_line(local_path: str, *, error_kind: str = "retriable", error: str = REJECTED) -> str:
    return json.dumps(
        {
            "fields": {
                "message": TRANSFER_JOB_FAILED_MESSAGE,
                "kind": "push_btrfs_send",
                "local_path": local_path,
                "error_kind": error_kind,
                "error": error,
            },
        }
    )


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SANDBOX_ID)
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, key: str, value: object) -> None:
        self.records[key] = value

    def phase(self, _name: str):
        return nullcontext()


class _Control:
    def __init__(self) -> None:
        self.failures: list[FailedTransfer] = []
        self.interrupter_output = ["pulse 1", "pulses 1"]
        self.calls: list[str] = []

    def failed_transfers(self, sandbox_id: str, node_name: str, *, since_time: str):
        return list(self.failures)

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


class FailedTransferReader(unittest.TestCase):
    def test_matches_message_and_sandbox_path(self) -> None:
        other = SEND_PATH.replace(SANDBOX_ID, "01a0ac88-0000-7000-8000-000000000000")
        logs = "\n".join(
            [
                _failed_line(SEND_PATH),
                _failed_line(other),
                json.dumps({"fields": {"message": "transfer job completed", "local_path": SEND_PATH}}),
                "not json",
            ]
        )
        prefix = f"/space/cache/firecracker/sandboxes/{SANDBOX_ID}/"
        self.assertEqual(
            _failed_transfers_from_logs(logs, prefix, "worker-a"),
            [FailedTransfer("worker-a", "push_btrfs_send", SEND_PATH, "retriable", REJECTED)],
        )


class RejectedSendAssertion(unittest.TestCase):
    def test_no_rejection_fails_closed(self) -> None:
        control = _Control()
        control.failures = [
            FailedTransfer("worker-a", "push_btrfs_send", SEND_PATH, "retriable", "credential exchange failed")
        ]
        _, assertions, _ = _assertions(control)
        with self.assertRaises(InvariantViolation):
            assertions.expect_rejected_send_stream("worker-a", since_time="t")

    def test_a_permanent_rejection_fails_closed(self) -> None:
        control = _Control()
        control.failures = [FailedTransfer("worker-a", "push_btrfs_send", SEND_PATH, "permanent", REJECTED)]
        _, assertions, _ = _assertions(control)
        with self.assertRaises(InvariantViolation):
            assertions.expect_rejected_send_stream("worker-a", since_time="t")

    def test_a_retriable_rejection_is_recorded(self) -> None:
        control = _Control()
        control.failures = [FailedTransfer("worker-a", "push_btrfs_send", SEND_PATH, "retriable", REJECTED)]
        fixture, assertions, _ = _assertions(control)
        assertions.expect_rejected_send_stream("worker-a", since_time="t")
        self.assertEqual(json.loads(json.dumps(fixture.records["rejected_send"]))["error_kind"], "retriable")


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

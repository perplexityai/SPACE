"""`DeadNodeAssertions` waits for a native operation to end before the
scenario's next action on its sandbox.

A row can read `suspended` while the suspend operation is still open, and an
attempt the operation requeues suspends the sandbox again, after a wake the
scenario asked for. A bench run shows that only when the API happens to
requeue; this drives the order through a fake control plane.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from space_smoke.dead_node import DeadNodeAssertions

SANDBOX = "sbx-once-suspended"
OPERATION = "op-suspend"


class _Verified(AssertionError):
    pass


class _ControlPlane:
    """A sandbox whose row reads `suspended` as soon as it is asked to
    suspend, and whose suspend operation answers `states` in turn, the last
    one from then on. Records the order of what the scenario asked."""

    def __init__(self, states: list[str]) -> None:
        self.states = list(states)
        self.lifecycle = "started"
        self.asked: list[str] = []

    def native_sandbox_action(self, sandbox_id: str, action: str) -> str:
        self.asked.append(f"{action} {sandbox_id}")
        self.lifecycle = "suspended"
        return OPERATION

    def sandbox_residency(self, _sandbox_id: str) -> SimpleNamespace:
        return SimpleNamespace(lifecycle=self.lifecycle)

    def operation_state(self, operation: str) -> str:
        # A wait with no bound fails here rather than hanging the suite.
        if len(self.asked) > 1000:
            raise AssertionError("the operation was polled with no bound")
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        self.asked.append(f"{operation} {state}")
        return state

    def restore_portable(self, _sandbox_id: str) -> bool:
        return True

    def connect(self, **_options: object) -> None:
        self.asked.append("wake")
        self.lifecycle = "started"


def _assertions(control: _ControlPlane) -> tuple[DeadNodeAssertions, list[float]]:
    """Assertions over `control`, whose waits advance a fake clock instead of
    sleeping; returns them and the clock."""

    clock = [0.0]

    def verify(condition: bool, message: str, **evidence: object) -> None:
        if not condition:
            raise _Verified(message, evidence)

    def wait(seconds: float, **_options: object) -> None:
        clock[0] += seconds

    fixture = mock.Mock(verify=verify, wait=wait)
    return DeadNodeAssertions(fixture, control, mock.Mock()), clock


def _suspend_and_wake(control: _ControlPlane, *, timeout: float = 300) -> None:
    assertions, clock = _assertions(control)
    sandbox = SimpleNamespace(sandbox_id=SANDBOX, connect=control.connect)
    with (
        mock.patch("space_smoke.dead_node.time.monotonic", side_effect=lambda: clock[0]),
        mock.patch.object(assertions, "_expect_placed_and_stamped"),
    ):
        assertions.suspend_and_wake(sandbox, timeout=timeout)


class SuspendAndWake(unittest.TestCase):
    def test_the_wake_waits_for_the_suspend_operation_to_succeed(self) -> None:
        control = _ControlPlane(["running", "queued", "succeeded"])

        _suspend_and_wake(control)

        self.assertEqual(
            control.asked,
            [
                f"suspend {SANDBOX}",
                f"{OPERATION} running",
                f"{OPERATION} queued",
                f"{OPERATION} succeeded",
                "wake",
            ],
        )

    def test_a_suspend_that_failed_is_reported_and_nothing_is_woken(self) -> None:
        control = _ControlPlane(["running", "failed"])

        with self.assertRaises(_Verified) as raised:
            _suspend_and_wake(control)

        self.assertEqual(raised.exception.args[0], "the suspend operation did not succeed")
        self.assertEqual(raised.exception.args[1]["operation_state"], "failed")
        self.assertNotIn("wake", control.asked)

    def test_a_suspend_that_never_ends_fails_at_the_bound(self) -> None:
        control = _ControlPlane(["running"])

        with self.assertRaises(_Verified) as raised:
            _suspend_and_wake(control, timeout=30)

        self.assertEqual(raised.exception.args[1]["operation_state"], "running")
        self.assertNotIn("wake", control.asked)
        # One poll every 2 s up to the 30 s bound, and the one that found it out.
        self.assertEqual(sum(1 for asked in control.asked if asked.startswith(OPERATION)), 16)


if __name__ == "__main__":
    unittest.main()

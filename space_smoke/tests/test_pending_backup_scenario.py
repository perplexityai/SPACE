"""Contracts of the pending-backup census scenario the bench cannot exercise.

A passing bench shows one queue row appear and disappear. It cannot show that
a snapshot reaching `ready` while uploads are supposedly blocked fails the
in-flight verdict, that a queue row without an applied non-empty census fails
the corroboration, that a drained queue with an unbacked artifact still fails,
that the object store scale-back is armed before the scale to zero, or that
a failed wait for the gateway pod's deletion stops the scenario instead of
running it without its outage.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.capabilities import CensusPublication, PendingBackupRow
from space_smoke.pending_backup import PendingBackupAssertions
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope

SID = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SID)
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        pass


class _Control:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.states = ["creating"]
        self.queue = [PendingBackupRow("art-1", "full")]
        self.publications = [CensusPublication("worker-a", "applied", 1)]
        self.backed = False
        self.op_state = "succeeded"

    def object_store_replicas(self) -> int:
        self.calls.append("read")
        return 1

    def scale_object_store(self, replicas: int) -> int:
        self.calls.append(f"scale:{replicas}")
        if replicas == 0:
            raise TimeoutError("kubectl scale timed out after the write")
        return 0

    def wait_object_store_gone(self, *, timeout: float) -> None:
        self.calls.append("gone")

    def wait_object_store_rollout(self, *, timeout: float) -> None:
        self.calls.append("rollout")

    def log_since_time(self) -> str:
        return "2026-09-23T00:00:00Z"

    def snapshot_states(self, sandbox_id: str) -> list[str]:
        return list(self.states)

    def pending_backup_queue(self, sandbox_id: str):
        return list(self.queue)

    def census_publications(self, *, since_time: str):
        return list(self.publications)

    def artifact_backed(self, sandbox_id: str, artifact_id: str) -> bool:
        return self.backed

    def suspend_without_waiting(self, sandbox_id: str) -> str:
        self.calls.append("suspend")
        return "op-1"

    def operation_state(self, operation_id: str) -> str:
        return self.op_state


def _assertions(control: _Control):
    fixture = _Fixture()
    scope = ResourceScope()
    return fixture, PendingBackupAssertions(cast(Any, fixture), cast(Any, control), scope), scope


class Verdicts(unittest.TestCase):
    def test_a_ready_snapshot_while_blocked_fails_the_in_flight_verdict(self) -> None:
        control = _Control()
        control.states = ["ready"]
        _, census, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "no in-flight snapshot"):
            census.expect_snapshot_in_flight(timeout=60)

    def test_a_queue_row_without_an_applied_census_fails_corroboration(self) -> None:
        control = _Control()
        control.publications = [CensusPublication("worker-a", "applied", 0), CensusPublication("worker-a", "skipped", 1)]
        _, census, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "no census publication was applied"):
            census.expect_queue_names_this_sandbox(timeout=60)

    def test_an_empty_queue_with_an_unbacked_artifact_still_fails(self) -> None:
        control = _Control()
        _, census, _ = _assertions(control)
        census.expect_queue_names_this_sandbox(timeout=60)
        control.queue = []
        control.backed = False
        with self.assertRaisesRegex(InvariantViolation, "never left the pending queue with a backed snapshot"):
            census.expect_backed_and_dequeued(timeout=0)

    def test_success_records_the_final_observation_not_the_last_failing_poll(self) -> None:
        control = _Control()
        control.backed = True
        control.queue = []
        control.states = ["ready"]
        fixture, census, _ = _assertions(control)
        census._artifact = PendingBackupRow("art-1", "full")
        census.expect_backed_and_dequeued(timeout=60)
        self.assertEqual(fixture.records["artifact_backed"], True)
        self.assertEqual(fixture.records["snapshot_states"], ["ready"])
        self.assertEqual(fixture.records["queue"], [])

    def test_two_queue_rows_for_one_sandbox_fail(self) -> None:
        control = _Control()
        control.queue = [PendingBackupRow("a", "full"), PendingBackupRow("b", "disk")]
        _, census, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "more than once"):
            census.expect_queue_names_this_sandbox(timeout=60)


    def test_a_failed_suspend_operation_fails_the_final_verdict(self) -> None:
        control = _Control()
        control.op_state = "failed"
        _, census, _ = _assertions(control)
        census.suspend_async()
        with self.assertRaisesRegex(InvariantViolation, "suspend operation did not succeed"):
            census.expect_suspend_succeeded(timeout=60)


class Compensations(unittest.TestCase):
    def test_scale_back_is_armed_before_the_scale_to_zero(self) -> None:
        control = _Control()
        _, census, scope = _assertions(control)
        with self.assertRaises(TimeoutError):
            census.block_uploads()
        self.assertEqual(control.calls, ["read", "scale:0"])
        scope.close()
        self.assertEqual(control.calls, ["read", "scale:0", "scale:1", "rollout"])


class _OneResultKubectl:
    def __init__(self, returncode: int, stderr: str) -> None:
        self.returncode = returncode
        self.stderr = stderr

    def run(self, command, **_options) -> CommandResult:
        return CommandResult(tuple(command), self.returncode, "", self.stderr)


class ObjectStoreOutageWait(unittest.TestCase):
    def _wait(self, returncode: int, stderr: str) -> None:
        KindHarness(_OneResultKubectl(returncode, stderr), KindConfig()).wait_object_store_gone(timeout=5)

    def test_deleted_and_never_matched_pods_are_gone(self) -> None:
        self._wait(0, "")
        self._wait(1, "error: no matching resources found")

    def test_a_timeout_or_api_error_fails(self) -> None:
        for stderr in (
            "error: timed out waiting for the condition on pods/space-object-store-abc",
            "error: the server doesn't have a resource type \"pod\"",
        ):
            with self.subTest(stderr=stderr), self.assertRaisesRegex(RuntimeError, "object-store pod deletion"):
                self._wait(1, stderr)


if __name__ == "__main__":
    unittest.main()

"""Contracts of the quarantine-recovery scenario the bench cannot exercise.

A passing bench shows two episodes paced correctly. It cannot show that the
flat once-per-tick cadence fails, that a non-contiguous or short attempt list
fails, that a `RetryScheduled` record without `attempt_count` is refused as
oracle drift rather than counted as zero, that the retirement is armed before
the plant, or that a replaced sandbox-manager pod is a refusal to judge.
"""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import KindConfig, KindHarness, _parse_recovery_attempts
from space_smoke.capabilities import RecoveryAttempt
from space_smoke.quarantine_recovery import QuarantineRecoveryAssertions, judge_cadence
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope

SID = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
ORPHAN = "6f1e2d3c-4b5a-4c6d-8e9f-0a1b2c3d4e5f"


def _attempts(*offsets: float) -> list[RecoveryAttempt]:
    return [RecoveryAttempt(ORPHAN, i + 1, 1_000.0 + t) for i, t in enumerate(offsets)]


class Cadence(unittest.TestCase):
    def test_a_growing_cadence_passes(self) -> None:
        verdict = judge_cadence(_attempts(0, 60, 180), want=3)
        self.assertIsNone(verdict.failure)
        self.assertEqual(verdict.numbers, (1, 2, 3))
        self.assertEqual(verdict.gaps_seconds, (60.0, 120.0))

    def test_two_tick_gaps_both_ways_pass(self) -> None:
        # A first pass that burns its plan-lookup timeout pushes both gaps to 120 s.
        self.assertIsNone(judge_cadence(_attempts(0, 120, 240), want=3).failure)

    def test_the_flat_once_per_tick_cadence_fails(self) -> None:
        verdict = judge_cadence(_attempts(0, 60, 120), want=3)
        self.assertIn("backoff floor", verdict.failure or "")

    def test_too_few_attempts_fail(self) -> None:
        self.assertIn("wanted 3", judge_cadence(_attempts(0, 120), want=3).failure or "")

    def test_non_contiguous_numbers_fail(self) -> None:
        attempts = [RecoveryAttempt(ORPHAN, 1, 0.0), RecoveryAttempt(ORPHAN, 3, 200.0), RecoveryAttempt(ORPHAN, 4, 500.0)]
        self.assertIn("contiguous", judge_cadence(attempts, want=3).failure or "")

    def test_attempts_are_judged_in_attempt_order_not_arrival_order(self) -> None:
        shuffled = list(reversed(_attempts(0, 60, 180)))
        self.assertIsNone(judge_cadence(shuffled, want=3).failure)


EVENT = "recorded bounded quarantine recovery no-progress result"


def _json_line(stamp: str, outcome: str, sandbox: str = ORPHAN) -> str:
    return json.dumps(
        {"timestamp": stamp, "level": "INFO", "message": EVENT, "sandbox_id": sandbox, "outcome": outcome}
    )


class LogParsing(unittest.TestCase):
    def test_json_lines_yield_attempts_in_time_order(self) -> None:
        lines = [
            _json_line("2026-09-23T14:03:00.5Z", "RetryScheduled { attempt_count: 2, next_due: 1 }"),
            _json_line("2026-09-23T14:01:00Z", "RetryScheduled { attempt_count: 1, next_due: 1 }"),
            _json_line("2026-09-23T14:02:00Z", "StaleClaim"),
        ]
        attempts = _parse_recovery_attempts(lines, ORPHAN)
        self.assertEqual([a.attempt for a in attempts], [1, 2])
        self.assertAlmostEqual(attempts[1].at - attempts[0].at, 120.5)

    def test_compact_text_lines_parse_too(self) -> None:
        line = f"2026-09-23T14:01:00.000Z  INFO sbxmgr: {EVENT} sandbox_id={ORPHAN} outcome=RetryScheduled {{ attempt_count: 1 }}"
        self.assertEqual(_parse_recovery_attempts([line], ORPHAN)[0].attempt, 1)

    def test_other_sandboxes_and_other_events_are_ignored(self) -> None:
        lines = [
            _json_line("2026-09-23T14:01:00Z", "RetryScheduled { attempt_count: 1 }", sandbox=SID),
            json.dumps({"timestamp": "2026-09-23T14:01:00Z", "message": "something else", "sandbox_id": ORPHAN}),
        ]
        self.assertEqual(_parse_recovery_attempts(lines, ORPHAN), [])

    def test_a_retry_scheduled_record_without_attempt_count_is_oracle_drift(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "attempt_count"):
            _parse_recovery_attempts([_json_line("2026-09-23T14:01:00Z", "RetryScheduled { .. }")], ORPHAN)


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
        self.uid = "uid-1"
        self.quarantined: set[str] = set()
        self.attempts: dict[str, list[RecoveryAttempt]] = {}
        self.present: set[str] = set()

    def sandbox_manager_for_sandbox(self, sandbox_id: str) -> tuple[str, str, str]:
        return "worker-a", "space-sandbox-manager-a", self.uid

    def pod_uid(self, pod_name: str) -> str | None:
        return self.uid

    def plant_orphan_staging(self, node_name: str, sandbox_id: str) -> None:
        self.calls.append(f"plant:{sandbox_id[:4]}")
        self.present.add(sandbox_id)

    def remove_orphan_sandbox(self, node_name: str, sandbox_id: str) -> None:
        self.calls.append(f"remove:{sandbox_id[:4]}")
        self.present.discard(sandbox_id)

    def orphan_absent(self, node_name: str, sandbox_id: str) -> bool:
        return sandbox_id not in self.present

    def orphan_quarantined(self, pod_name: str, sandbox_id: str) -> bool:
        return sandbox_id in self.quarantined

    def recovery_attempts(self, pod_name: str, sandbox_id: str) -> list[RecoveryAttempt]:
        return list(self.attempts.get(sandbox_id, []))


def _assertions(control: _Control):
    fixture = _Fixture()
    scope = ResourceScope()
    return fixture, QuarantineRecoveryAssertions(cast(Any, fixture), cast(Any, control), scope), scope


class Lifecycle(unittest.TestCase):
    def test_retirement_is_armed_before_each_plant_and_unwinds_on_close(self) -> None:
        control = _Control()
        _, recovery, scope = _assertions(control)
        orphans = recovery.induce(count=2)
        self.assertEqual(len(orphans), 2)
        scope.close()
        self.assertEqual(
            [c.split(":")[0] for c in control.calls], ["plant", "plant", "remove", "remove"]
        )
        self.assertFalse(control.present)

    def test_a_plant_that_raises_still_has_its_retirement_armed(self) -> None:
        control = _Control()

        def failing(node, sandbox_id):
            control.calls.append(f"plant:{sandbox_id[:4]}")
            raise RuntimeError("mkdir: read-only file system")

        control.plant_orphan_staging = failing  # type: ignore[method-assign]
        _, recovery, scope = _assertions(control)
        with self.assertRaises(RuntimeError):
            recovery.induce(count=1)
        scope.close()
        self.assertEqual([c.split(":")[0] for c in control.calls], ["plant", "remove"])

    def test_explicit_retire_releases_once_and_requires_the_directories_gone(self) -> None:
        control = _Control()
        _, recovery, scope = _assertions(control)
        recovery.induce(count=2)
        recovery.retire()
        before = list(control.calls)
        scope.close()
        self.assertEqual(control.calls, before)
        self.assertEqual(sum(c.startswith("remove") for c in control.calls), 2)

    def test_residue_after_retirement_fails(self) -> None:
        control = _Control()
        control.remove_orphan_sandbox = lambda node, sid: control.calls.append("remove")  # type: ignore[method-assign]
        _, recovery, _ = _assertions(control)
        recovery.induce(count=1)
        with self.assertRaisesRegex(InvariantViolation, "survived its retirement"):
            recovery.retire()

    def test_a_replaced_pod_is_a_refusal_to_judge(self) -> None:
        control = _Control()
        _, recovery, _ = _assertions(control)
        recovery.induce(count=1)
        control.uid = "uid-2"
        with self.assertRaisesRegex(InvariantViolation, "replaced mid-run"):
            recovery.expect_quarantined(timeout=60)

    def test_quarantine_needs_every_orphan(self) -> None:
        control = _Control()
        _, recovery, _ = _assertions(control)
        orphans = recovery.induce(count=2)
        control.quarantined.add(orphans[0])
        with self.assertRaisesRegex(InvariantViolation, "not every induced orphan was quarantined"):
            recovery.expect_quarantined(timeout=0)

    def test_attempt_collection_timeout_is_not_a_cadence_verdict(self) -> None:
        control = _Control()
        _, recovery, _ = _assertions(control)
        orphans = recovery.induce(count=2)
        control.attempts[orphans[0]] = _attempts(0, 60, 180)
        with self.assertRaisesRegex(InvariantViolation, "collection timeout"):
            recovery.expect_attempts(want=3, timeout=0)

    def test_backoff_is_judged_per_episode(self) -> None:
        control = _Control()
        fixture, recovery, _ = _assertions(control)
        orphans = recovery.induce(count=2)
        control.attempts[orphans[0]] = _attempts(0, 60, 180)
        control.attempts[orphans[1]] = _attempts(0, 60, 120)
        recovery.expect_attempts(want=3, timeout=60)
        with self.assertRaisesRegex(InvariantViolation, f"episode {orphans[1][:8]}.*backoff floor"):
            recovery.expect_backoff(want=3)
        control.attempts[orphans[1]] = _attempts(0, 120, 240)
        recovery.expect_attempts(want=3, timeout=60)
        recovery.expect_backoff(want=3)
        self.assertEqual(set(fixture.records["cadence"]), {orphans[0][:8], orphans[1][:8]})


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


ARGV = ("kubectl",)
# The operator shell lookup every on-node exec starts with.
SHELL = CommandResult(ARGV, 0, "space-node-debug-a", "")


def _harness(*results: CommandResult) -> tuple[KindHarness, _ScriptedKubectl]:
    kubectl = _ScriptedKubectl(results)
    return KindHarness(kubectl, KindConfig()), kubectl


class HarnessOps(unittest.TestCase):
    """The kind ops behind the control, driven against a scripted kubectl.

    Bench round 1 (2026-09-23) died in the pod-uid read on an attribute the
    kubectl result type does not have; every op here reads the real result type.
    """

    def test_pod_uid_reads_the_jsonpath_and_is_none_when_kubectl_fails(self) -> None:
        harness, _ = _harness(CommandResult(ARGV, 0, "uid-1\n", ""), CommandResult(ARGV, 1, "", "NotFound"))
        self.assertEqual(harness.pod_uid("space-sandbox-manager-a"), "uid-1")
        self.assertIsNone(harness.pod_uid("space-sandbox-manager-a"))

    def test_plant_execs_one_mkdir_of_a_suspend_publish_staging_under_the_orphan(self) -> None:
        harness, kubectl = _harness(SHELL, CommandResult(ARGV, 0, "/space/cache/...\n", ""))
        harness.plant_orphan_staging("worker-a", ORPHAN)
        lookup, call = kubectl.calls
        self.assertIn("app.kubernetes.io/name=space-node-debug", lookup)
        self.assertIn("spec.nodeName=worker-a", lookup)
        self.assertIn("exec", call)
        self.assertIn("space-node-debug-a", call)
        self.assertIn("shell", call)
        script = call[-1]
        self.assertIn(f"/space/cache/firecracker/sandboxes/{ORPHAN}/snapshots/.suspend-publish-", script)
        self.assertTrue(script.startswith("mkdir -p "))

    def test_plant_failure_is_an_error_not_a_silent_pass(self) -> None:
        harness, _ = _harness(SHELL, CommandResult(ARGV, 1, "", "mkdir: read-only file system"))
        with self.assertRaises(Exception):
            harness.plant_orphan_staging("worker-a", ORPHAN)

    def test_retire_deletes_the_row_then_the_directory(self) -> None:
        harness, kubectl = _harness(SHELL, CommandResult(ARGV, 0, "", ""))
        harness.remove_orphan_sandbox("worker-a", ORPHAN)
        script = kubectl.calls[1][-1]
        self.assertLess(script.index(f"space-sbxmgr delete {ORPHAN}"), script.index("rm -rf"))
        self.assertIn(f"/space/cache/firecracker/sandboxes/{ORPHAN}", script)

    def test_absence_reads_the_exec_return_code(self) -> None:
        harness, _ = _harness(
            SHELL, CommandResult(ARGV, 0, "", ""), SHELL, CommandResult(ARGV, 1, "", "")
        )
        self.assertTrue(harness.orphan_absent("worker-a", ORPHAN))
        self.assertFalse(harness.orphan_absent("worker-a", ORPHAN))

    def test_quarantine_and_attempts_read_the_sandbox_managers_log(self) -> None:
        lines = "\n".join([
            json.dumps({"timestamp": "2026-09-23T14:00:00Z", "message": "quarantined", "sandbox_id": ORPHAN, "reason_code": "orphaned_sandbox_row_missing"}),
            _json_line("2026-09-23T14:01:00Z", "RetryScheduled { attempt_count: 1, next_attempt_at: 1 }"),
            _json_line("2026-09-23T14:02:00Z", "RetryScheduled { attempt_count: 9 }", sandbox=SID),
        ])
        harness, kubectl = _harness(CommandResult(ARGV, 0, lines, ""), CommandResult(ARGV, 0, lines, ""))
        self.assertTrue(harness.orphan_quarantined("space-sandbox-manager-a", ORPHAN))
        self.assertEqual([a.attempt for a in harness.recovery_attempts("space-sandbox-manager-a", ORPHAN)], [1])
        logs = kubectl.calls[0]
        self.assertEqual(logs[logs.index("logs"):logs.index("logs") + 4],
                         ("logs", "space-sandbox-manager-a", "-c", "sandbox-manager"))


if __name__ == "__main__":
    unittest.main()

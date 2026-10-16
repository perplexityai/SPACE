"""Contracts of the restore-rollback late-retry scenario the bench cannot exercise.

A passing bench run shows the fixed build's state passes. These pin the rest
off-bench: the state a build that accepts the late retry leaves fails with the
named reason, the verdict never depends on an event being absent, the adapter
expands aggregated kube events, and the Python side names the same hook
variable and message as the Rust hook.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from space_smoke import adapters
from space_smoke.adapters import RESTORE_STORAGE_URI_ANNOTATION, KindConfig, KindHarness
from space_smoke.capabilities import ExecutionRow, RestoreActivation, SnapshotArtifactRow
from space_smoke.restore_rollback import (
    INJECTED_FAULT_MESSAGE,
    FaultedAttempt,
    RestoreRollbackAssertions,
)
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope

SANDBOX_ID = "01a14600-0000-7000-8000-000000000001"
ORIGINAL = SnapshotArtifactRow("snap-original", "full", "ready", "artifact-original")
ATTEMPT = FaultedAttempt("worker-a", 2, "space-sbx-x-g2", "SandboxException: 502")
HOOK_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "crates/libs/sbx/mgr/server/src/service/restore_prepare_fault.rs"
)


def activation(generation: int, artifact: str = "", boot: str = "snapshot") -> RestoreActivation:
    return RestoreActivation(
        SANDBOX_ID, "worker-a", boot, "remote_matches_local", True, str(generation), artifact
    )


def uri(snapshot_id: str) -> str:
    return f"s3://bucket/suspend_snapshots/aa/{SANDBOX_ID}/{snapshot_id}?region=test"


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SANDBOX_ID)
        self.evidence: dict[str, object] = {}

    def record(self, name: str, value: object) -> None:
        self.evidence[name] = value

    def verify(self, condition: bool, message: str, *, failure_evidence=None, **evidence: object) -> None:
        self.evidence.update(evidence)
        if not condition:
            if failure_evidence is not None:
                self.evidence.update(failure_evidence())
            raise InvariantViolation(message)


class _Control:
    def __init__(
        self,
        rows,
        activations,
        *,
        executions=(ExecutionRow(1, "suspended", False, "g1"), ExecutionRow(2, "suspended", False, "g2"), ExecutionRow(3, "running", True, "g3")),
        storage_uri: str | None = uri("snap-original"),
    ) -> None:
        self.rows = list(rows)
        self.activations = list(activations)
        self.executions = list(executions)
        self.storage_uri = storage_uri

    def snapshot_artifact_rows(self, _sandbox_id):
        return self.rows

    def restore_activations(self, _sandbox_id, **_kw):
        return self.activations

    def sandbox_executions(self, _sandbox_id):
        return self.executions

    def restore_pod_document(self, _pod_name):
        annotations = {} if self.storage_uri is None else {RESTORE_STORAGE_URI_ANNOTATION: self.storage_uri}
        return {"metadata": {"annotations": annotations}}

    def sandbox_manager_log(self, _sandbox_id, **_kw):
        return ["log line"]

    def sandbox_start_failures(self, _pod_name):
        return ["start failure"]


def verdict(control: _Control) -> _Fixture:
    fixture = _Fixture()
    assertions = RestoreRollbackAssertions(fixture, control, ResourceScope())  # type: ignore[arg-type]
    assertions.expect_original_snapshot_restored(ORIGINAL, ATTEMPT, activation_visibility_timeout=0)
    return fixture


class Verdict(unittest.TestCase):
    def test_the_fixed_build_state_passes(self) -> None:
        verdict(_Control([ORIGINAL], [activation(1, boot="fresh"), activation(3, "artifact-original")]))

    def test_an_activated_rolled_back_attempt_alone_is_evidence_not_the_verdict(self) -> None:
        # The verdict reads state; a gen-2 event is not judged by its presence.
        verdict(_Control([ORIGINAL], [activation(2, "artifact-original"), activation(3, "artifact-original")]))

    def test_a_build_that_accepts_the_late_retry_fails_on_the_new_snapshot(self) -> None:
        second = SnapshotArtifactRow("snap-second", "full", "ready", "artifact-second")
        with self.assertRaisesRegex(InvariantViolation, "new snapshot was published"):
            verdict(
                _Control(
                    [ORIGINAL, second],
                    [activation(2, "artifact-original"), activation(3, "artifact-second")],
                )
            )

    def test_a_pending_new_snapshot_row_also_fails(self) -> None:
        pending = SnapshotArtifactRow("snap-second", "full", "creating", "")
        with self.assertRaisesRegex(InvariantViolation, "new snapshot was published"):
            verdict(_Control([ORIGINAL, pending], [activation(3, "artifact-original")]))

    def test_a_failure_carries_the_logs_as_evidence(self) -> None:
        second = SnapshotArtifactRow("snap-second", "full", "ready", "artifact-second")
        fixture = _Fixture()
        assertions = RestoreRollbackAssertions(fixture, _Control([ORIGINAL, second], []), ResourceScope())  # type: ignore[arg-type]
        with self.assertRaises(InvariantViolation):
            assertions.expect_original_snapshot_restored(ORIGINAL, ATTEMPT)
        self.assertEqual(fixture.evidence["sandbox_manager_log"], ["log line"])
        self.assertEqual(fixture.evidence["faulted_pod_start_failures"], ["start failure"])

    def test_no_active_execution_after_the_faulted_one_fails(self) -> None:
        executions = (ExecutionRow(1, "suspended", False, "g1"), ExecutionRow(2, "running", True, "g2"))
        with self.assertRaisesRegex(InvariantViolation, "one active execution is not a restore after"):
            verdict(_Control([ORIGINAL], [activation(2, "artifact-original")], executions=executions))

    def test_a_successor_on_another_artifact_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "original snapshot's artifact"):
            verdict(_Control([ORIGINAL], [activation(3, "artifact-other")]))

    def test_no_successor_activation_event_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "no runtime_activated event for the successor"):
            verdict(_Control([ORIGINAL], [activation(1, boot="fresh")]))

    def test_a_source_pinned_successor_without_a_storage_uri_passes(self) -> None:
        verdict(_Control([ORIGINAL], [activation(3, "artifact-original")], storage_uri=None))

    def test_the_storage_uri_stays_out_of_the_evidence(self) -> None:
        fixture = verdict(_Control([ORIGINAL], [activation(3, "artifact-original")]))
        self.assertNotIn("s3://", json.dumps(fixture.evidence, default=str))


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


class Adapter(unittest.TestCase):
    def test_start_failures_expand_aggregated_events_in_time_order(self) -> None:
        events = {
            "items": [
                {"message": "rolled back", "count": 3, "firstTimestamp": "2026-01-01T00:00:20Z"},
                {"message": "injected", "count": 1, "firstTimestamp": "2026-01-01T00:00:05Z"},
            ]
        }
        kubectl = _ScriptedKubectl([CommandResult(("kubectl",), 0, json.dumps(events), "")])
        harness = KindHarness(kubectl, KindConfig())
        self.assertEqual(
            harness.sandbox_start_failures("space-sbx-x-g2"),
            ["injected", "rolled back", "rolled back", "rolled back"],
        )
        self.assertTrue(any("reason=FailedCreatePodSandBox" in arg for arg in kubectl.calls[0]))

    def test_arming_refuses_a_worker_without_the_hook(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with (
            mock.patch.object(KindHarness, "_require_isolated_kind"),
            mock.patch.object(KindHarness, "sandbox_pool_nodes", return_value=["worker-a"]),
            mock.patch.object(KindHarness, "_node_service_env", return_value={}),
        ):
            with self.assertRaisesRegex(Exception, "predates the restore-prepare fault hook"):
                harness.arm_restore_prepare_fault(SANDBOX_ID)


class HookNamesMatchTheRustSide(unittest.TestCase):
    def test_the_variable_and_message_are_the_rust_hook_constants(self) -> None:
        source = HOOK_SOURCE.read_text()
        self.assertIn(f'"{adapters.RESTORE_PREPARE_FAULT_DIR_ENV}"', source)
        self.assertIn(f'"{INJECTED_FAULT_MESSAGE}"', source)


if __name__ == "__main__":
    unittest.main()

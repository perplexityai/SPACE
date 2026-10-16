"""A rolled-back restore attempt stays rolled back when kubelet retries it late.

When a restore's prepare fails after its activation claim, sandbox-manager
rolls the attempt back: the sandbox is suspended again on its original
snapshot. Kubelet does not know that. It keeps retrying the same pod, with the
same activation attempt id, until the pod is deleted. The contract is that
those late retries are refused, so the attempt never goes live, its teardown
publishes no new snapshot, and the next connect restores the original one.

A build that accepts the late retry activates the rolled-back attempt. Deleting
its pod then suspends a sandbox the user never resumed and publishes a second
snapshot, and the next connect restores that one instead. The user's data
happens to survive in that case, so the marker file alone cannot tell the two
builds apart; the snapshot rows and the restored artifact can.

The verdict is state (`space_smoke/AGENTS.md`): the cluster's snapshot rows,
the successor's execution row, the user's file, and the
`runtime_activated` contract event for the successor generation, polled for
and judged on its fields. Whether the late retries were refused is visible only
in pod events and the sandbox-manager log, so those are evidence, not the
verdict.

The one fault is sandbox-manager's restore-prepare test hook
(`crates/libs/sbx/mgr/server/src/service/restore_prepare_fault.rs`), armed by a
marker file and consumed when it fires; the control surface is
[`RestoreRollbackControl`].
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from space_smoke.adapters import RESTORE_STORAGE_URI_ANNOTATION
from space_smoke.capabilities import RestoreRollbackControl, SnapshotArtifactRow
from space_smoke.runtime import ResourceScope, ScenarioTimedOut, SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# Must match `INJECTED_FAULT_MESSAGE` in `restore_prepare_fault.rs`.
INJECTED_FAULT_MESSAGE = "test hook: injected restore prepare failure"
# Kubelet retried a failed sandbox start about every 15 s on the bench; hold
# the faulted pod for two more retries after the faulted connect returns.
LATE_RETRY_WINDOW_SECONDS = 30.0


@dataclass(frozen=True)
class FaultedAttempt:
    """The restore attempt the fault failed, as the control plane recorded it."""

    node_name: str
    generation: int
    pod_name: str
    connect_outcome: str


def _log_since_time() -> str:
    # A minute of slack for clock skew between this host and the workers.
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 60))


class RestoreRollbackAssertions:
    """Drive and judge one rolled-back restore and its late kubelet retry."""

    def __init__(
        self,
        fixture: KindEcw,
        control: RestoreRollbackControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._since_time = _log_since_time()
        self._armed: tuple[str, ...] = ()

    @property
    def _sandbox_id(self) -> str:
        return self._fixture.sandbox.sandbox_id

    def _evidence(self, attempt: FaultedAttempt | None = None) -> dict[str, object]:
        """Logs and pod events for a failure's output; no assertion reads them."""

        evidence: dict[str, object] = {
            "sandbox_manager_log": self._control.sandbox_manager_log(
                self._sandbox_id, node_names=None, since_time=self._since_time
            ),
            "activations": [
                a.__dict__
                for a in self._control.restore_activations(
                    self._sandbox_id, since_time=self._since_time
                )
            ],
        }
        if attempt is not None:
            evidence["faulted_pod_start_failures"] = [
                m[-240:] for m in self._control.sandbox_start_failures(attempt.pod_name)
            ]
        return evidence

    def original_snapshot(self, *, timeout: float = 300) -> SnapshotArtifactRow:
        """The one ready snapshot the suspend published; all later checks compare to it.

        A pause returns before the backup publishes, so this waits until the
        snapshot is portable from object storage: its row is final before the
        fault, and any later row is the late retry's.
        """

        deadline = time.monotonic() + timeout
        while not self._control.restore_portable(self._sandbox_id):
            self._fixture.verify(
                time.monotonic() <= deadline,
                "the suspend snapshot did not become portable from object storage",
                portability_timeout_seconds=timeout,
            )
            time.sleep(2)
        rows = self._control.snapshot_artifact_rows(self._sandbox_id)
        self._fixture.verify(
            len(rows) == 1 and rows[0].state == "ready" and bool(rows[0].local_artifact_id),
            "the suspend did not leave exactly one ready snapshot with a local artifact",
            snapshot_rows=[r.__dict__ for r in rows],
        )
        self._fixture.record("original_snapshot", rows[0].__dict__)
        return rows[0]

    def arm_fault(self) -> None:
        """Arm one restore-prepare failure for this sandbox on every worker."""

        self._scope.defer(
            "disarm the restore-prepare fault",
            lambda: self._control.disarm_restore_prepare_fault(self._sandbox_id),
        )
        self._armed = self._control.arm_restore_prepare_fault(self._sandbox_id)
        present = self._control.armed_restore_prepare_faults(self._sandbox_id)
        self._fixture.verify(
            bool(self._armed) and present == self._armed,
            "the restore-prepare fault marker is not present on every worker it was armed on",
            armed=self._armed,
            present=present,
        )

    def connect_through_fault(self) -> FaultedAttempt:
        """Connect once with the fault armed, then disarm it and find the faulted attempt.

        The connect is expected to fail, but its outcome is recorded rather
        than asserted: a build that accepts a late retry can activate the
        attempt while the connect is still waiting, and the snapshot checks
        that follow are the verdict on that build. That the fault caused the
        failure is state: exactly one worker's marker file is gone.
        """

        try:
            self._fixture.sandbox.connect(timeout=self._fixture._sandbox_timeout)
            outcome = "connected"
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:  # noqa: BLE001 - the outcome is evidence, not the verdict
            outcome = f"{type(error).__name__}: {str(error)[:300]}"
        remaining = self._control.armed_restore_prepare_faults(self._sandbox_id)
        self._control.disarm_restore_prepare_fault(self._sandbox_id)
        fired = tuple(node for node in self._armed if node not in remaining)
        self._fixture.record("first_connect_outcome", outcome)
        self._fixture.verify(
            len(fired) == 1,
            "the restore-prepare fault did not fire exactly once, so no attempt was rolled back by it",
            armed=self._armed,
            still_armed=remaining,
        )
        executions = self._control.sandbox_executions(self._sandbox_id)
        latest = max(executions, key=lambda e: e.generation) if executions else None
        self._fixture.verify(
            latest is not None and latest.generation >= 2 and bool(latest.pod_name),
            "no restore execution row follows the original one",
            executions=[e.__dict__ for e in executions],
        )
        assert latest is not None
        attempt = FaultedAttempt(fired[0], latest.generation, latest.pod_name, outcome)
        self._fixture.record("faulted_attempt", attempt.__dict__)
        return attempt

    def _faulted_pod_present(self, attempt: FaultedAttempt) -> bool:
        return any(
            p.pod_name == attempt.pod_name and not p.deleting
            for p in self._control.sandbox_pods(self._sandbox_id)
        )

    def hold_for_late_retry(
        self, attempt: FaultedAttempt, *, window: float = LATE_RETRY_WINDOW_SECONDS
    ) -> None:
        """Keep the faulted pod for kubelet's late retries of the same attempt.

        The precondition is state: the pod still exists after the faulted
        connect returned, so kubelet has been retrying it for the whole
        connect and keeps doing so for `window` more seconds. What the
        retries got back is in the pod events, which are evidence only.
        """

        self._fixture.verify(
            self._faulted_pod_present(attempt),
            "the faulted pod was gone before kubelet could retry it late; nothing to judge",
            faulted_pod=attempt.pod_name,
            failure_evidence=lambda: self._evidence(attempt),
        )
        deadline = time.monotonic() + window
        while time.monotonic() < deadline and self._faulted_pod_present(attempt):
            time.sleep(3)
        self._fixture.record(
            "faulted_pod_start_failures",
            [m[-240:] for m in self._control.sandbox_start_failures(attempt.pod_name)],
        )

    def tear_down_attempt(self, attempt: FaultedAttempt, *, timeout: float = 120) -> None:
        """Delete the faulted attempt's pod, as a late teardown would, and wait it out."""

        if any(p.pod_name == attempt.pod_name for p in self._control.sandbox_pods(self._sandbox_id)):
            self._control.delete_sandbox_pod(attempt.pod_name)
        deadline = time.monotonic() + timeout
        while any(
            p.pod_name == attempt.pod_name for p in self._control.sandbox_pods(self._sandbox_id)
        ):
            self._fixture.verify(
                time.monotonic() < deadline,
                "the faulted attempt's pod was not gone within the teardown window",
                pod_name=attempt.pod_name,
            )
            time.sleep(2)

    def expect_original_snapshot_restored(
        self,
        original: SnapshotArtifactRow,
        attempt: FaultedAttempt,
        *,
        activation_visibility_timeout: float = 60,
    ) -> None:
        """The verdict: one snapshot row, and a later execution restored from it.

        All state: the snapshot rows; the successor's execution row; and the
        `runtime_activated` contract event for the successor generation,
        polled for and judged on its fields. The local artifact it reports is
        the only exposed record of which snapshot the restore used: the
        successor pod carried no storage URI annotation on the bench after
        the rolled-back placement, so the pod cannot name the snapshot.
        Nothing here depends on an event being absent.
        """

        evidence = lambda: self._evidence(attempt)  # noqa: E731 - lazy failure evidence
        rows = self._control.snapshot_artifact_rows(self._sandbox_id)
        self._fixture.verify(
            [r.snapshot_id for r in rows] == [original.snapshot_id],
            "a new snapshot was published after the rolled-back restore; its late retry "
            "went live and its teardown suspended it again",
            original_snapshot_id=original.snapshot_id,
            snapshot_rows=[r.__dict__ for r in rows],
            failure_evidence=evidence,
        )
        active = [e for e in self._control.sandbox_executions(self._sandbox_id) if e.active]
        self._fixture.verify(
            len(active) == 1 and active[0].generation > attempt.generation and bool(active[0].pod_name),
            "the sandbox's one active execution is not a restore after the rolled-back attempt",
            faulted_generation=attempt.generation,
            active_executions=[e.__dict__ for e in active],
            failure_evidence=evidence,
        )
        successor = active[0]
        # Evidence only: after the failed placement the successor pod need not
        # carry a storage URI (see the docstring).
        document = self._control.restore_pod_document(successor.pod_name) or {}
        annotations = (document.get("metadata") or {}).get("annotations") or {}
        self._fixture.record(
            "successor_restore_storage_uri_present",
            bool(annotations.get(RESTORE_STORAGE_URI_ANNOTATION)),
        )
        deadline = time.monotonic() + activation_visibility_timeout
        while True:
            activations = [
                a
                for a in self._control.restore_activations(
                    self._sandbox_id, since_time=self._since_time
                )
                if a.execution_generation == str(successor.generation)
            ]
            if activations or time.monotonic() > deadline:
                break
            time.sleep(3)
        self._fixture.verify(
            bool(activations),
            "sbx-manager reported no runtime_activated event for the successor generation",
            successor_generation=successor.generation,
            failure_evidence=evidence,
        )
        self._fixture.verify(
            all(
                a.boot_type == "snapshot" and a.local_artifact_id == original.local_artifact_id
                for a in activations
            ),
            "the successor restore did not activate the original snapshot's artifact",
            original_local_artifact_id=original.local_artifact_id,
            successor_activations=[a.__dict__ for a in activations],
            failure_evidence=evidence,
        )
        self._fixture.record("successor_generation", successor.generation)

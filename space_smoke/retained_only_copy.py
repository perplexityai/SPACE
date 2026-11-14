"""An interrupted suspend that leaves the sandbox's only copy, and its one way out.

A suspend that fails with its runtime gone and no capture of its own kept
quarantines its row with `suspend_publish_failed_no_artifact_runtime_gone`:
the retained instance filesystem may be the sandbox's only copy. The shared
retention policy then refuses every ordinary delete of it, through the public
API (its delete operation doesn't complete) and through `space-sbxmgr delete` on the node, and the operator's
discard delete refuses too while the suspend attempt's window is open. Once
the window has passed, the discard erases it.

The fault is the only direct manipulation: node-manager is held (SIGSTOP)
while the suspend sits in `suspending`, the sandbox's Firecracker is killed,
and node-manager is released, so the suspend's next node-manager call finds
the runtime gone. The sandbox is paused in memory first, so the suspend needs
no node-manager call before its row reaches `suspending`. Kind sets a short
discard window through a test hook (`SPACE_SBX_DISCARD_ATTEMPT_WINDOW_SECS`);
production waits six hours.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from space_smoke.capabilities import RetainedOnlyCopyControl
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# A public operation's settled states.
TERMINAL_OPERATION_STATES = ("succeeded", "failed", "canceled")
DISCARD_REASON = "space-smoke: the interrupted suspend's filesystem is disposable"
# How long before the window ends the in-window discard must run, so a slow
# phase can't let the window close under it and turn the check into a no-op.
WINDOW_MARGIN_SECONDS = 20
# The node's row can't be read while node-manager is held (the read waits on
# sandbox-manager, which waits on node-manager), so the fault waits for the
# control plane to report the suspend, then this long for sandbox-manager to
# admit it and reach its first node-manager call. If the kill lands earlier the
# sandbox isn't quarantined as the only copy, and the delete and quarantine checks
# fail; the scenario can't pass by accident.
SUSPEND_ADMISSION_GRACE_SECONDS = 5
# How long after the window ends to wait before the discard that must succeed:
# the attempt starts when sandbox-manager admits the suspend, a little after
# the scenario issues it.
WINDOW_SLACK_SECONDS = 15
# What the failure-path discard may spend inside the scenario's cleanup budget
# (`min(120 s, 10 %)` of its timeout): waiting out the rest of the window, then
# the discard and the public delete.
CLEANUP_WINDOW_WAIT_SECONDS = 70
CLEANUP_DELETE_SECONDS = 30


class RetainedOnlyCopyAssertions:
    """Interrupt the sandbox's suspend, then check every delete against its only copy."""

    def __init__(
        self, fixture: KindEcw, control: RetainedOnlyCopyControl, scope: ResourceScope
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._node: str | None = None
        self._suspend_issued_at: float | None = None
        self._failure_discard: Compensation | None = None
        self._node_manager: Compensation | None = None

    @property
    def _sandbox_id(self) -> str:
        return self._fixture.sandbox.sandbox_id

    @property
    def node(self) -> str:
        if self._node is None:
            raise RuntimeError("locate() must run first")
        return self._node

    def locate(self) -> None:
        """Read the node the sandbox runs on."""

        node = self._control.sandbox_node_name(self._sandbox_id)
        self._fixture.verify(node is not None, "the sandbox has no active node")
        assert node is not None
        self._node = node
        self._fixture.record("node", node)

    def _row(self) -> str | None:
        return self._control.node_sandbox_state(self.node, self._sandbox_id)

    def expect_row(self, state: str | None, *, timeout: float) -> None:
        """The node's row for the sandbox reaches `state` (None: no row) within `timeout`."""

        deadline = time.monotonic() + timeout
        while True:
            observed = self._row()
            if observed == state:
                self._fixture.record(f"row_{state or 'gone'}", observed)
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the node's sandbox row did not reach {state or 'gone'}",
                node=self.node,
                row_state=observed,
            )
            self._fixture.wait(2)

    def _expect_control_plane(self, state: str, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            observed = self._control.native_sandbox_state(self._sandbox_id)
            if observed == state:
                self._fixture.record(f"control_plane_{state}", observed)
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the control plane did not report the sandbox {state}",
                control_plane_state=observed,
            )
            self._fixture.wait(1)

    def interrupt_suspend(self) -> None:
        """Pause in memory, then fail the suspend with its runtime gone and no capture."""

        self._control.pause_in_memory(self._sandbox_id)
        self.expect_row("paused", timeout=120)
        # Armed before the hold, so a hold whose reply is lost still continues
        # node-manager when the scenario ends.
        node_manager = self._scope.defer(
            "continue node-manager",
            lambda: self._control.release_node_manager(self.node),
            critical=True,
        )
        self._node_manager = node_manager
        try:
            self._control.hold_node_manager(self.node)
            operation = self._control.suspend_without_waiting(self._sandbox_id)
            self._suspend_issued_at = time.monotonic()
            self._fixture.record("suspend_operation", operation)
            self._expect_control_plane("suspending", timeout=60)
            self._fixture.wait(
                SUSPEND_ADMISSION_GRACE_SECONDS, reason="sandbox-manager admitting the suspend"
            )
            # From the kill on, the fixture's ordinary delete can't remove the
            # sandbox: the only copy is refused. Arm its discard first.
            self._failure_discard = self._scope.defer(
                "discard the retained only copy", self._discard_after_failure, critical=True
            )
            self._fixture.record(
                "runtime_kill", self._control.kill_sandbox_runtime(self.node, self._sandbox_id)
            )
        finally:
            node_manager.release()
        self.expect_row("quarantined", timeout=180)

    def _expect_retained(self, after: str) -> None:
        self._fixture.verify(
            self._row() == "quarantined",
            f"the sandbox's row left quarantine after {after}",
            node=self.node,
        )
        self._fixture.verify(
            self._control.sandbox_cache_dir_present(self.node, self._sandbox_id),
            f"the sandbox's retained filesystem is gone after {after}",
            node=self.node,
        )

    def _settled_operation(self, operation_id: str, *, timeout: float) -> dict[str, object]:
        """The operation once it reaches a terminal state, or as it stands at `timeout`."""

        deadline = time.monotonic() + timeout
        while True:
            record = self._control.operation(operation_id)
            if record.get("state") in TERMINAL_OPERATION_STATES or time.monotonic() >= deadline:
                return record
            self._fixture.wait(2)

    def expect_ordinary_deletes_refused(self) -> None:
        """The public delete doesn't complete and the node's ordinary delete fails; both keep it."""

        operation_id = self._control.api_delete(self._sandbox_id)
        record = self._settled_operation(operation_id, timeout=60)
        self._fixture.record("public_delete_operation", record)
        self._fixture.verify(
            record.get("state") == "failed" and record.get("terminal_error_code") == "conflict",
            "the public delete didn't fail with the node's only-copy conflict",
            operation=record,
        )
        self._fixture.verify(
            self._control.native_sandbox_state(self._sandbox_id) not in ("", "deleted"),
            "the public delete removed the sandbox from the control plane",
        )
        self._expect_retained("the public delete")
        result = self._control.node_sandbox_delete(
            self.node, self._sandbox_id, discard_reason=None
        )
        self._fixture.verify(
            result.returncode != 0,
            "space-sbxmgr's ordinary delete erased the only copy",
            output=result.detail(),
        )
        self._expect_retained("the ordinary delete")

    def _window(self) -> int:
        return self._control.discard_attempt_window_seconds(self.node)

    def expect_discard_refused_inside_window(self) -> None:
        """While the suspend attempt's window is open, the discard is refused too."""

        if self._suspend_issued_at is None:
            raise RuntimeError("interrupt_suspend() must run first")
        window = self._window()
        elapsed = time.monotonic() - self._suspend_issued_at
        self._fixture.record("discard_window_seconds", window)
        self._fixture.verify(
            elapsed < window - WINDOW_MARGIN_SECONDS,
            "the discard window closed before the in-window discard could run",
            window_seconds=window,
            elapsed_seconds=round(elapsed, 1),
        )
        result = self._control.node_sandbox_delete(
            self.node, self._sandbox_id, discard_reason=DISCARD_REASON
        )
        self._fixture.verify(
            result.returncode != 0,
            "the discard erased the sandbox while its suspend attempt could still finish",
            output=result.detail(),
        )
        self._expect_retained("the in-window discard")

    def expect_discard_erases(self) -> None:
        """Once the window has passed, the discard erases the sandbox and its filesystem."""

        if self._suspend_issued_at is None:
            raise RuntimeError("interrupt_suspend() must run first")
        remaining = (
            self._suspend_issued_at
            + self._window()
            + WINDOW_SLACK_SECONDS
            - time.monotonic()
        )
        if remaining > 0:
            self._fixture.wait(remaining, reason="the suspend attempt's discard window")
        result = self._control.node_sandbox_delete(
            self.node, self._sandbox_id, discard_reason=DISCARD_REASON
        )
        self._fixture.verify(
            result.returncode == 0,
            "the discard didn't erase the only copy once its window had passed",
            output=result.detail(),
        )
        self.expect_row(None, timeout=60)
        self._fixture.verify(
            not self._control.sandbox_cache_dir_present(self.node, self._sandbox_id),
            "the discard left the sandbox's filesystem on the node",
            node=self.node,
        )

    def expect_control_plane_releases(self, *, timeout: float = 180) -> None:
        """Once discarded, the public delete succeeds and the sandbox and its pod go.

        The node no longer holds the sandbox, so its node-local delete is
        admitted even though the dead VM took the pod's own address with it,
        and the pod's teardown is answered as deleted, so the pod is released.
        """

        record = self._settled_operation(self._control.api_delete(self._sandbox_id), timeout=timeout)
        self._fixture.record("delete_after_discard_operation", record)
        self._fixture.verify(
            record.get("state") == "succeeded",
            "the public delete didn't complete after the discard",
            operation=record,
        )
        deadline = time.monotonic() + timeout
        while True:
            present = self._control.native_sandbox_present(self._sandbox_id)
            pods = [pod.pod_name for pod in self._control.sandbox_pods(self._sandbox_id)]
            if not present and not pods:
                if self._failure_discard is not None:
                    self._failure_discard.disarm()
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the sandbox or its pod outlived the delete after the discard",
                sandbox_present=present,
                pods=pods,
            )
            self._fixture.wait(3)

    def _discard_after_failure(self) -> None:
        """Discard the sandbox a failed run left quarantined, then delete it.

        The window may still be open, so this waits for what's left of it, as
        far as the cleanup budget allows. A row already gone needs only the
        public delete. The delete must succeed and the sandbox and its pods
        go, or the compensation fails.

        Cleanup runs compensations newest first, so this one runs before the
        node-manager continue a failed release left armed. It continues
        node-manager first: the node's row can't be read while it's held. If
        that fails again, the continue stays armed for cleanup to retry.
        """

        if self._node_manager is not None:
            self._node_manager.release()
        if self._node is not None and self._row() is not None:
            if self._suspend_issued_at is not None:
                remaining = (
                    self._suspend_issued_at + self._window() + WINDOW_SLACK_SECONDS
                    - time.monotonic()
                )
                if remaining > 0:
                    time.sleep(min(remaining, CLEANUP_WINDOW_WAIT_SECONDS))
            result = self._control.node_sandbox_delete(
                self.node, self._sandbox_id, discard_reason=DISCARD_REASON
            )
            if result.returncode != 0:
                raise RuntimeError(f"cleanup discard failed: {result.detail()}")
        operation_id = self._control.api_delete(self._sandbox_id)
        deadline = time.monotonic() + CLEANUP_DELETE_SECONDS
        while True:
            record = self._control.operation(operation_id)
            if record.get("state") in TERMINAL_OPERATION_STATES:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("cleanup delete did not settle")
            time.sleep(2)
        if record.get("state") != "succeeded":
            raise RuntimeError(f"cleanup delete ended {record.get('state')}: {record}")
        while True:
            present = self._control.native_sandbox_present(self._sandbox_id)
            pods = [pod.pod_name for pod in self._control.sandbox_pods(self._sandbox_id)]
            if not present and not pods:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"cleanup delete left the sandbox (present={present}) or its pods {pods}"
                )
            time.sleep(2)

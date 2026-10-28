"""sandbox-manager restarted under a teardown suspend still waiting on its backup.

A teardown suspend captures the sandbox, then holds its runtime until the
capture is backed up, and only then finalizes. Its node-local row stays
`suspending` with the capture pinned the whole time. sandbox-manager's startup
recovery used to treat such a row as finished and mark it `suspended`, which
retired the runtime behind the teardown's back. The teardown job and the pod
waiting on it then stayed running, and the sandbox couldn't be woken. A
teardown that owns the row now keeps it through a restart and finishes it
itself once the backup lands.

Uploads are held with the pending-backup scenario's object store fault (see
`space_smoke.pending_backup`), so the window is as long as the scenario needs
rather than the few seconds a natural suspend's upload takes on kind. The
observable is the worker's own sandbox row, read through the product's
`space-sbxmgr list`: `suspending` through the restart, then the suspend
operation succeeding and the sandbox waking with its state.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from space_smoke.capabilities import TeardownRestartControl

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw
    from space_smoke.pending_backup import PendingBackupAssertions


SANDBOX_MANAGER = "sandbox-manager"


class TeardownRestartAssertions:
    """Restart the suspending sandbox's sandbox-manager and read its row."""

    def __init__(
        self,
        fixture: KindEcw,
        control: TeardownRestartControl,
        uploads: PendingBackupAssertions,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._uploads = uploads

    def _row_state(self) -> str | None:
        return self._control.node_sandbox_state(
            self._uploads.node_name, self._fixture.sandbox.sandbox_id
        )

    def expect_row(self, state: str, *, timeout: float = 120) -> None:
        """The worker's row for this sandbox reaches `state` within `timeout`."""

        deadline = time.monotonic() + timeout
        while True:
            observed = self._row_state()
            if observed == state:
                self._fixture.record(f"row_state_{state}", observed)
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the worker's sandbox row did not reach {state}",
                node=self._uploads.node_name,
                row_state=observed,
            )
            self._fixture.wait(3)

    def expect_row_finished(self, *, timeout: float = 180) -> None:
        """The worker's row leaves `suspending` for `suspended` within `timeout`.

        Once the backup is ready, sandbox-manager may evict the suspended
        sandbox's local presence at any time, which removes the row, so a row
        that's gone counts as finished too; the wake that follows proves the
        state survived.
        """

        deadline = time.monotonic() + timeout
        while True:
            observed = self._row_state()
            if observed in ("suspended", None):
                self._fixture.record(
                    "row_after_suspend", "evicted" if observed is None else observed
                )
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the worker's sandbox row did not finish suspending",
                node=self._uploads.node_name,
                row_state=observed,
            )
            self._fixture.wait(3)

    def restart_sandbox_manager(self, *, timeout: float = 600) -> None:
        """Replace the worker's sandbox-manager pod; its VMs keep running."""

        started = time.monotonic()
        pod = self._control.restart_node_service(
            self._uploads.node_name, SANDBOX_MANAGER, timeout=timeout
        )
        self._fixture.record("sandbox_manager_pod_after", pod)
        self._fixture.record(
            "sandbox_manager_restart_seconds", round(time.monotonic() - started, 1)
        )

    def expect_row_holds(self, state: str, *, seconds: float = 30, interval: float = 5) -> None:
        """The row reads `state` on every poll for `seconds`.

        sandbox-manager runs startup recovery before it serves, so the first
        read after the replacement is Ready already sees what recovery left;
        the later reads cover cleanup it scheduled on the way up.
        """

        deadline = time.monotonic() + seconds
        reads: list[str | None] = []
        while True:
            observed = self._row_state()
            reads.append(observed)
            self._fixture.verify(
                observed == state,
                f"the restart moved the worker's sandbox row off {state}",
                node=self._uploads.node_name,
                row_state=observed,
                reads=reads,
            )
            if time.monotonic() >= deadline:
                self._fixture.record("row_reads_after_restart", reads)
                return
            self._fixture.wait(interval)

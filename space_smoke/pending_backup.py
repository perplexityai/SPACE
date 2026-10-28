"""The pending-backup census, proven from the control plane on a real cluster.

A node reports, on every Full sandbox-inventory census, the local artifacts
it cannot prove are backed up; space-internal-api mirrors that report into
the cluster's `node_local_backup_pending` queue.
`smoke-kind-pending-backup-census.sh` proved the producer half by copying
sandbox-manager's SQLite out of the worker pod and re-running the
`list_pending_backups` predicate over it; that white-box read is not a
surface the typed framework exposes, and it is recorded rather than ported.
What the control plane exposes is enough for the contract that matters to an
operator: with uploads blocked, the queue names exactly this sandbox's pinned
artifact; once uploads resume, the snapshot reaches `ready` with a storage
location (the artifact is provably backed) and the queue row is gone.
space-internal-api's census publication log lines are failure evidence only.

Why the upload has to be broken: a natural suspend's unbacked window is a few
seconds on kind while the census fires once per node per poll interval, so an
unmodified run sees an empty report essentially always, and an empty report
is indistinguishable from a node that enumerated nothing. The object store is scaled
to zero for the suspend with its scale-back armed first. The restore does not
finalize the upload at once: the transfer climbs the shared job worker's retry
ladder (5, 30, 120, 300 s) and can wait out a full 300 s before it uploads, so
the drain budget is 600 s and the downtime is kept short so the fifth attempt
never runs with the store still down. The census is asked for, not waited
for: once the snapshot is in flight the sandbox's node-ctl publishes its
sandbox inventory on demand (`space-nodectl facet publish`), through the same
outbound sync and Full census its 60 s poll runs, so the store is back within
the transfer's first retries rather than after a minute of them. This proves
the census and its queue mirror, not that node-ctl's timer sends it: the
node-ctl crate's
`a_timer_tick_publishes_the_periodic_outbound_facets_and_reconciles_node_objects`
proves that. A failed request is recorded and retried, and the timer still
covers it within the budget. Scaling the object store to
zero stops every suspend upload on the stack, so this must not run concurrently
with another suspend-bearing smoke.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from space_smoke.capabilities import CensusPublication, PendingBackupControl, PendingBackupRow
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw


class PendingBackupAssertions:
    """Block uploads, suspend, read the queue, then watch the backup land."""

    def __init__(self, fixture: KindEcw, control: PendingBackupControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._store_back: Compensation | None = None
        self._since: str | None = None
        self._artifact: PendingBackupRow | None = None
        self._operation_id: str | None = None
        self._node_name: str | None = None

    @property
    def node_name(self) -> str:
        """The worker the suspended sandbox ran on, read by `suspend_async()`."""

        if self._node_name is None:
            raise RuntimeError("suspend_async() must run first")
        return self._node_name

    def block_uploads(self, *, timeout: float = 120) -> None:
        """Scale the object store to zero; the scale-back is armed before the write."""

        previous = self._control.object_store_replicas()
        self._fixture.record("object_store_replicas_before", previous)

        def back() -> None:
            self._control.scale_object_store(max(previous, 1))
            self._control.wait_object_store_rollout(timeout=180)

        self._store_back = self._scope.defer("scale the object store back up", back, critical=True)
        self._since = self._control.log_since_time()
        self._control.scale_object_store(0)
        self._control.wait_object_store_gone(timeout=timeout)

    def suspend_async(self) -> str:
        """Start the suspend as an operation and return without waiting on it.

        The ECW `pause` route answers only once the suspend is durable (the API
        polls the node for up to 300 s), so with uploads blocked it can only
        fail with a 502; the native `:suspend` route enqueues an operation and
        returns, which is what lets the snapshot be observed in flight. The
        operation is checked at the end, after uploads are restored.
        """

        # The worker whose node-ctl will publish the census; read while the
        # sandbox still has its running pod.
        placement = self._control.sandbox_pod_placement(self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            placement is not None,
            "the sandbox has no bound pod to read its worker from before the suspend",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        assert placement is not None
        self._node_name = placement.node_name
        self._fixture.record("census_node", placement.node_name)
        self._operation_id = self._control.suspend_without_waiting(self._fixture.sandbox.sandbox_id)
        self._fixture.record("suspend_operation_id", self._operation_id)
        return self._operation_id

    def expect_snapshot_in_flight(self, *, timeout: float = 180) -> None:
        """The suspend started an upload it cannot finish: a snapshot row sits in `creating`."""

        sandbox_id = self._fixture.sandbox.sandbox_id
        deadline = time.monotonic() + timeout
        while True:
            states = self._control.snapshot_states(sandbox_id)
            if "creating" in states:
                self._fixture.record("snapshot_states_blocked", states)
                return
            self._fixture.verify(
                time.monotonic() < deadline and "ready" not in states,
                "no in-flight snapshot appeared while uploads were blocked",
                snapshot_states=states,
            )
            self._fixture.wait(3)

    def expect_queue_names_this_sandbox(
        self, *, timeout: float = 240, republish_seconds: float = 10
    ) -> PendingBackupRow:
        """A Full census carried the pinned artifact and internal-api mirrored it into the queue.

        The queue is the census's only writer, so its row is the outcome. The
        census is requested from the sandbox's node-ctl, again every
        `republish_seconds` while the row is missing. A request that fails is
        recorded, not fatal: node-ctl's poll still sends the census inside the
        budget.
        """

        node_name = self._node_name
        if node_name is None:
            raise RuntimeError("suspend_async() must run first")
        sandbox_id = self._fixture.sandbox.sandbox_id
        deadline = time.monotonic() + timeout
        published_at = None
        request_failures: list[str] = []
        while True:
            if published_at is None or time.monotonic() - published_at >= republish_seconds:
                failure = self._control.publish_node_ctl_facet(node_name, "sandbox-inventory")
                published_at = time.monotonic()
                if failure is not None:
                    request_failures.append(failure)
                    self._fixture.record("census_request_failures", request_failures[-6:])
            rows = self._control.pending_backup_queue(sandbox_id)
            if rows:
                self._fixture.verify(
                    len(rows) == 1 and rows[0].artifact_kind in ("full", "disk"),
                    "the queue names this sandbox more than once or with an unknown artifact kind",
                    rows=[(r.artifact_id, r.artifact_kind) for r in rows],
                )
                self._artifact = rows[0]
                self._fixture.record("pending_artifact", rows[0].artifact_id)
                self._fixture.record("pending_artifact_kind", rows[0].artifact_kind)
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "no pending-backup queue row for this sandbox appeared within the census budget",
                failure_evidence=lambda: {
                    "censuses": [
                        (p.node_id, p.outcome, p.reported) for p in self._recent_publications()
                    ][-6:]
                },
            )
            # The store comes back only once the row is seen, and each of the
            # transfer's retries (5 s, then 30 s later) uploads only if it
            # already has.
            self._fixture.wait(2)
        return self._artifact

    def restore_uploads(self) -> None:
        if self._store_back is not None:
            release = self._store_back
            self._store_back = None
            release.release()

    def expect_backed_and_dequeued(self, *, timeout: float = 600) -> None:
        """Once uploads resume the snapshot is backed and the queue row is gone."""

        artifact = self._artifact
        if artifact is None:
            raise RuntimeError("expect_queue_names_this_sandbox() must run first")
        sandbox_id = self._fixture.sandbox.sandbox_id
        deadline = time.monotonic() + timeout
        while True:
            rows = self._control.pending_backup_queue(sandbox_id)
            backed = self._control.artifact_backed(sandbox_id, artifact.artifact_id)
            if not rows and backed:
                # `verify` records its evidence before it judges, so without
                # these the RESULT would carry the last failing poll's values
                # (`artifact_backed: false`, a `creating` snapshot) on a green
                # run — bench round 2 read exactly that as a contradiction.
                self._fixture.record("drained_after_seconds", round(timeout - (deadline - time.monotonic()), 1))
                self._fixture.record("queue", [])
                self._fixture.record("artifact_backed", backed)
                self._fixture.record("snapshot_states", self._control.snapshot_states(sandbox_id))
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the artifact never left the pending queue with a backed snapshot after uploads were restored",
                queue=[(r.artifact_id, r.artifact_kind) for r in rows],
                artifact_backed=backed,
                snapshot_states=self._control.snapshot_states(sandbox_id),
                suspend_operation_state=self._operation_state(),
            )
            self._fixture.wait(2)

    def expect_suspend_succeeded(self, *, timeout: float = 120) -> None:
        """The suspend operation that ran through the outage ends `succeeded` once the upload landed."""

        deadline = time.monotonic() + timeout
        while True:
            state = self._operation_state()
            if state == "succeeded":
                self._fixture.record("suspend_operation_state", state)
                return
            self._fixture.verify(
                state not in ("failed", "canceled", "expired") and time.monotonic() < deadline,
                "the suspend operation did not succeed after uploads were restored",
                suspend_operation_state=state,
                snapshot_states=self._control.snapshot_states(self._fixture.sandbox.sandbox_id),
            )
            self._fixture.wait(5)

    def _operation_state(self) -> str:
        if self._operation_id is None:
            return ""
        return self._control.operation_state(self._operation_id)

    def _recent_publications(self) -> list[CensusPublication]:
        since = self._since or self._control.log_since_time()
        return self._control.census_publications(since_time=since)

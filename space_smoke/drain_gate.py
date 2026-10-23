"""The admin `:terminate` occupancy guard, proven over HTTP on a real cluster.

The guard used to carry a third, independent definition of the drain gate,
and it disagreed with the node-drain reconciler: it asked `lifecycle =
'suspended'` only, so a `suspending` row — or a non-evacuation `restoring`
row — pinned to the node by `target_node_id` was invisible to it. Its
`active` half could not see such a row whose `active_node_id` is NULL either,
because that half keys on `active_node_id`; an in-flight user restore is the
common case. The endpoint therefore accepted a `drained` node that had picked
up such a row since it drained, one the drain gate would hold, and
`scripts/space-rollout-mng.py` drives that endpoint unattended across an MNG
rollout.

The decision now reads `drain_gate_blockers_for_node`, the same predicate the
reconciler uses. That change is not behind a feature flag, so the only thing
standing between a regression and a stranded sandbox is a test that drives the
route: the store-level agreement is asserted in `terminate_gate_agreement_tests`,
which cannot see an HTTP response.

The control surface is [`DrainGateControl`]: seeding the blocker and moving a
node to `drained` are two real writes, so they sit behind one cluster-visible
mutation Lease, are recorded on it for orphan repair, and are undone in a
scenario phase rather than in cleanup.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    DrainGateBlocker,
    DrainGateControl,
    TerminateAttempt,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# The 409 body's keys. `space-rollout-mng.py` reads `sandboxes_active` and
# `sandboxes_drain_blockers` off it to report why a rollout wave stalled.
# `sandboxes_local_snapshots` is the earlier name of `sandboxes_drain_blockers`,
# kept as an alias for a copy of the tool from before the rename, which reads
# only that key: dropping it nulls that copy's report silently rather than
# failing it.
CONFLICT_BODY_KEYS: tuple[str, ...] = (
    "sandboxes_active",
    "sandboxes_drain_blockers",
    "sandboxes_local_snapshots",
)


class DrainGateAssertions:
    """Drive admin `:terminate` against a node the drain gate must hold."""

    def __init__(
        self,
        fixture: KindEcw,
        control: DrainGateControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._release_lease: Compensation | None = None
        self._release_blocker: Compensation | None = None
        self._blocker: DrainGateBlocker | None = None

    def seed_blocker(self) -> DrainGateBlocker:
        """Pin a `suspending` row to an idle node and drain that node.

        The compensation is armed **before** the first write, not after the
        last. Seeding commits two independent statements — the row, then the
        node's move to `drained` — so a compensation registered on the return
        value would not exist yet if the second one failed, and cleanup would
        release the Lease over a seeded row it never deleted. The next
        acquisition's repair would undo it, since repair runs on every
        acquisition of an existing Lease, but only when some later run takes
        this Lease; until then the row stays pinned to the node and holds any
        drain of it.

        Arming first is possible because the compensation takes no argument:
        the Lease itself records each write before it happens, and the undo
        reads that record. It is idempotent, so arming it before anything has
        been written costs nothing.
        """

        release = self._control.acquire_drain_gate_mutation()
        self._release_lease = self._scope.defer(
            "release the drain-gate mutation Lease", release, critical=True
        )
        self._release_blocker = self._scope.defer(
            "undrain the seeded node and delete its blocker",
            self._control.clear_drain_gate_residue,
            critical=True,
        )
        blocker = self._control.seed_drain_gate_blocker()
        self._blocker = blocker
        self._fixture.record("drain_gate_node_id", blocker.node_id)
        self._fixture.record("drain_gate_blocker_sandbox_id", blocker.sandbox_id)
        return blocker

    def expect_terminate_refused(self) -> TerminateAttempt:
        """`:terminate` must answer 409, and from the drain-gate half.

        A 409 alone proves little: the same status covers an illegal
        drain-phase transition, whose body carries `error` /
        `current_drain_phase` instead. The assertions below pin the body to
        the occupancy shape, then pin `sandboxes_active` to zero — the seeded
        row sets `target_node_id` and leaves `active_node_id` NULL precisely
        so the resident half has nothing to count, which makes the refusal
        attributable to the gate and to nothing else.
        """

        blocker = self._require_blocker()
        attempt = self._control.attempt_terminate(blocker.node_id)
        self._fixture.record("terminate_status", attempt.status_code)
        self._fixture.record("terminate_body", dict(attempt.body))
        self._fixture.verify(
            attempt.status_code == 409,
            "admin :terminate accepted a node the node-drain reconciler still "
            "holds — a suspending sandbox pinned by target_node_id no longer "
            "blocks termination, and the rollout driver would strand it",
            node_id=blocker.node_id,
            blocker_sandbox_id=blocker.sandbox_id,
            terminate_status=attempt.status_code,
        )
        missing = [key for key in CONFLICT_BODY_KEYS if key not in attempt.body]
        self._fixture.verify(
            not missing,
            "the 409 body does not carry the occupancy counts; either the "
            "refusal came from the drain-phase transition check rather than "
            "the occupancy guard, or a key space-rollout-mng.py reads was "
            "dropped",
            missing_keys=missing,
            body_keys=sorted(str(key) for key in attempt.body),
        )
        self._fixture.verify(
            attempt.body.get("sandboxes_active") == 0,
            "the refusal counted a resident sandbox, so it proves nothing "
            "about the drain gate",
            sandboxes_active=attempt.body.get("sandboxes_active"),
        )
        self._fixture.verify(
            attempt.body.get("sandboxes_drain_blockers") == 1,
            "the gate did not count exactly the one blocker this scenario "
            "seeded",
            sandboxes_drain_blockers=attempt.body.get("sandboxes_drain_blockers"),
        )
        return attempt

    def release_blocker(self) -> None:
        """Undo both writes now, inside a phase, rather than at cleanup."""

        if self._release_blocker is not None:
            self._release_blocker.release()
            self._release_blocker = None
        self._blocker = None
        if self._release_lease is not None:
            self._release_lease.release()
            self._release_lease = None

    def _require_blocker(self) -> DrainGateBlocker:
        if self._blocker is None:
            raise RuntimeError("the drain-gate blocker has not been seeded")
        return self._blocker

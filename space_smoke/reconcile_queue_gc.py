"""The reconcile-queue GC's EC2 half, proven live on kind.

The cluster-controller reconciler `reconcile_queue_gc`
(crates/libs/cluster/controller/server/src/reconcile_queue_gc/) runs on Kind
at the binary's fixed timers: a 300 s tick, and 24 h before a dead letter, or
a live ref of a non-ready `managed_by='k8s'` node, can be pruned. For each
such node its `Ec2Detector` calls `DescribeInstances` by the node's
Kubernetes name, which Kind's fake EC2 endpoint (`space-kind-fake-ec2`)
answers from the Node of that name: a Ready Node is `running`, which keeps
the refs, and no Node is no instance, which reads as gone and prunes them.

No public path makes a ref a day old, so the scenario seeds two synthetic,
non-ready node rows with 25 h-old refs. The live one is named after a Ready
Node that no SPACE node row names (the control plane on Kind), because a
second row named after a SPACE worker would make every lookup of that
worker by name ambiguous for as long as it exists. The gone one is named
after no Node at all. The live row also carries a 25 h-old dead letter and
a 1 h-old one. Their removal is armed before the insert and deletes only
rows carrying the scenario's marker.

The observables are the queue rows after one GC tick: the old dead letter
and the gone node's ref are gone; the fresh dead letter and the Ready node's
ref remain. The fake endpoint's access log is attached to a failure as
evidence and never judged.
"""

from __future__ import annotations

import re
import secrets
import time
import uuid
from typing import TYPE_CHECKING

from space_smoke.capabilities import ReconcileQueueGcControl, ReconcileQueueRef
from space_smoke.runtime import ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

GC_ENABLED_ENV = "SPACE_RECONCILE_QUEUE_GC_ENABLED"
EC2_ENDPOINT_ENV = "AWS_ENDPOINT_URL_EC2"
GC_ENV_KEYS: tuple[str, ...] = (GC_ENABLED_ENV, EC2_ENDPOINT_ENV)

# `DEFAULT_TICK` in reconcile_queue_gc/mod.rs. The loop consumes its first
# tick at start, so a controller that restarts just before the seed takes a
# full tick too.
GC_TICK_SECONDS = 300

LIVE_REF_FACET = "node-status"
OLD_DEAD_FACET = "sandbox-inventory"
FRESH_DEAD_FACET = "template-inventory"

MARKER_PREFIX = "space-smoke-reconcile-queue-gc-"
_MARKER = re.compile(re.escape(MARKER_PREFIX) + r"[a-f0-9]{8}")
_NODE_NAME = re.compile(r"[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?")


def validate_marker(marker: str) -> str:
    if _MARKER.fullmatch(marker) is None:
        raise ValueError(f"refusing reconcile-queue GC marker {marker!r}")
    return marker


def validate_node_name(name: str) -> str:
    """A Kubernetes node name (DNS-1123 subdomain); nothing else reaches SQL."""

    if _NODE_NAME.fullmatch(name) is None:
        raise ValueError(f"refusing node name {name!r}")
    return name


class ReconcileQueueGcAssertions:
    """Seed one day-old queue for a live and a gone node, then judge one GC tick."""

    def __init__(self, fixture: KindEcw, control: ReconcileQueueGcControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._token = secrets.token_hex(4)
        self.marker = validate_marker(MARKER_PREFIX + self._token)
        self.live_id = str(uuid.uuid4())
        self.gone_id = str(uuid.uuid4())
        self.live_name = ""
        self.gone_name = validate_node_name(f"space-smoke-gone-{self._token}")

    def expect_gc_on_against_the_fake(self) -> None:
        """The applied controller runs the GC live and points EC2 at an endpoint override."""

        env = self._control.reconcile_queue_gc_env()
        self._fixture.record("controller_gc_env", env)
        self._fixture.verify(
            env.get(GC_ENABLED_ENV) == "true" and bool(env.get(EC2_ENDPOINT_ENV)),
            "the cluster-controller does not run the reconcile-queue GC against the fake EC2 endpoint; "
            "bring the bench up from this checkout",
            **{key: env.get(key) for key in GC_ENV_KEYS},
        )

    def seed(self) -> None:
        """Insert the synthetic nodes and their backdated refs; their removal is armed first."""

        self.live_name = validate_node_name(self._control.unregistered_ready_node())
        self._fixture.verify(
            not self._control.node_exists(self.gone_name),
            "a Node already carries the scenario's gone-node name",
            gone_name=self.gone_name,
        )
        self._fixture.record("live_node", {"id": self.live_id, "k8s_node_name": self.live_name})
        self._fixture.record("gone_node", {"id": self.gone_id, "k8s_node_name": self.gone_name})
        self._fixture.record("marker", self.marker)

        def remove() -> None:
            remaining = self._control.delete_seeded_reconcile_queue_nodes(
                (self.live_id, self.gone_id), marker=self.marker
            )
            if remaining:
                raise RuntimeError(f"{remaining} seeded node rows remain after cleanup")

        self._scope.defer("delete the seeded reconcile-queue GC nodes", remove, critical=True)
        self._control.seed_reconcile_queue_gc_nodes(
            live=(self.live_id, self.live_name),
            gone=(self.gone_id, self.gone_name),
            marker=self.marker,
        )
        seeded = sorted(self._refs())
        self._fixture.record("seeded_refs", seeded)
        self._fixture.verify(
            len(seeded) == 4,
            "the seed did not leave the four queue rows it inserted",
            seeded_refs=seeded,
        )

    def expect_one_tick_prunes_only_the_gone(self, *, margin: float = 120) -> None:
        """Within one tick: the old dead letter and the gone node's ref go; the rest stays."""

        kept = {
            (self.live_id, LIVE_REF_FACET, False),
            (self.live_id, FRESH_DEAD_FACET, True),
        }
        started = time.monotonic()
        deadline = started + GC_TICK_SECONDS + margin
        while True:
            refs = self._refs()
            if refs == kept:
                # `verify` records its evidence before judging, so a green
                # RESULT would otherwise carry the last pre-tick poll's refs.
                self._fixture.record("pruned_after_seconds", round(time.monotonic() - started, 1))
                self._fixture.record("refs", sorted(refs))
                return
            self._fixture.verify(
                kept <= refs and time.monotonic() < deadline,
                "one reconcile-queue GC tick did not prune exactly the day-old dead letter "
                "and the gone node's ref",
                expected_refs=sorted(kept),
                refs=sorted(refs),
                failure_evidence=lambda: {"fake_ec2_log": self._control.fake_ec2_log_tail()},
            )
            self._fixture.wait(10)

    def _refs(self) -> set[tuple[str, str, bool]]:
        rows: list[ReconcileQueueRef] = self._control.reconcile_queue_refs((self.live_id, self.gone_id))
        return {(row.node_id, row.facet, row.dead) for row in rows}

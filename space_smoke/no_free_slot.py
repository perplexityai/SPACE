"""The activation limiter's slot check, proven on a bench whose slots can be filled.

A sandbox pod requests one `space.pplx.net/network-slots` from its node, and
node-ctl advertises each node's count from the node-manager's network pool. The
activation limiter refuses a cold create with a 503 when no start-eligible node
has a free slot, on its own switch
(`SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED`) rather than the
limiter's.

**Why this is its own scenario rather than a phase of the rate-limiter one.**
Filling the slots is only affordable on a bench whose nodes advertise a handful
of them, which the node-manager chart's `networkPoolMaxSlots` makes possible.
At the small count this scenario needs, the whole cluster holds about as many
sandboxes as the rate-limiter scenario's fail-open phase admits by itself, so
the two cannot share a bench: whichever ran second would fail on slots rather
than on its own subject. Sharing a scenario would also mean one `SandboxConfig`
for both, and this one deliberately wants the smallest fixture it can get.

The fill is real sandboxes, not a stand-in. The limiter's slot accounting comes
from a reflector of the API's own sandbox pods, so a bystander pod requesting
the resource would be invisible to it however much kube-scheduler respects the
request -- the bench has to actually hold the slots with the thing the check
counts.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    EcwSandbox,
    LimiterProbeOutcome,
    RateLimitProbe,
    RateLimiterControl,
)
from space_smoke.rate_limiters import (
    CAPACITY_LIMIT_MESSAGE,
    NO_FREE_SLOT_MESSAGE,
    declared_capabilities,
    has_retry_after_seconds,
    status_histogram,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SLOT_CHECK_ENV = "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED"
SLOT_CHECK_CAPABILITY = "slot_check"
# Every key this scenario may write on the public API Deployment, captured
# before anything writes them and restored at scope end. One key, but the list
# is the shape the control surface refuses anything outside of.
SLOT_ENV_KEYS: tuple[str, ...] = (SLOT_CHECK_ENV,)


class NoFreeSlotAssertions:
    """Fill every eligible node's network slots, then judge what a create is told."""

    def __init__(
        self,
        fixture: KindEcw,
        control: RateLimiterControl,
        probe: RateLimitProbe,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._probe = probe
        self._scope = scope
        self._restore_env: Compensation | None = None
        self._release_lease: Compensation | None = None
        self._ballast: list[EcwSandbox] = []
        self._admitted: list[str] = []
        self._budget = 0
        self._client_id = "space-smoke-slots"
        self._template = "seed"

    # --- capability -------------------------------------------------------

    def capability_declared(self) -> bool:
        """Whether this bench asserts its API carries the slot check.

        Declared rather than probed: the check ships no migration, no chart
        key of its own, no route and no eagerly-loaded script, so nothing the
        harness can read tells a build without the code from one whose code is
        broken. The declaration is safe because it is asymmetric -- claim it on
        a build that lacks it and the refusal never comes, so the phase fails;
        leave it unclaimed and the scenario records itself absent.
        """

        declared = SLOT_CHECK_CAPABILITY in declared_capabilities()
        self._fixture.record("capability_slot_check_declared", declared)
        return declared

    # --- the bench's slot budget -----------------------------------------

    def expect_fillable_slot_budget(self, *, most_per_node: int = 4) -> int:
        """Require a small, uniform slot budget and return its total.

        The precondition the whole scenario rests on, and a verdict rather
        than a read: at the chart's default every node advertises hundreds of
        slots, and a fill sized from that would be thousands of VM boots. A
        run against a bench that forgot `networkPoolMaxSlots` must say so
        here, not discover it by never filling up.

        Uniformity is required too. The chart applies the value to every node
        of its release, so a bench where the nodes disagree is one where
        something else wrote a capacity, and the fill arithmetic below would
        be guessing which number to trust.
        """

        slots = self._control.sandbox_node_slots()
        self._fixture.record("sandbox_node_slots", dict(slots))
        self._fixture.verify(
            bool(slots),
            "no node carries the sandbox placement-pool label, so this bench has no "
            "start-eligible node whose slots could be filled",
        )
        capacities = {name: facts["capacity"] for name, facts in slots.items()}
        self._fixture.verify(
            all(capacity > 0 for capacity in capacities.values()),
            "a sandbox-pool node advertises no network slots at all, so nothing can be "
            "placed on it and the fill below would never make the cluster full",
            capacities=capacities,
        )
        distinct = set(capacities.values())
        self._fixture.verify(
            len(distinct) == 1,
            "the sandbox-pool nodes advertise different slot counts, so the fill cannot be "
            "sized from the chart value the bench was given",
            capacities=capacities,
        )
        per_node = distinct.pop()
        self._fixture.verify(
            per_node <= most_per_node,
            f"each sandbox node advertises {per_node} network slots, more than the {most_per_node} "
            "this scenario will fill. Set the node-manager chart's `networkPoolMaxSlots` to a "
            "small count for this bench; at the release default the fill would be thousands of "
            "real VM boots",
            capacities=capacities,
        )
        self._budget = per_node * len(capacities)
        self._fixture.record(
            "slot_budget", {"per_node": per_node, "nodes": len(capacities), "total": self._budget}
        )
        return self._budget

    def _slots_in_use(self) -> int:
        return sum(facts["used"] for facts in self._control.sandbox_node_slots().values())

    # --- filling ----------------------------------------------------------

    def fill_slots(self, *, timeout: float = 600) -> None:
        """Create sandboxes until every eligible node's slots are held.

        Counts what is already there first: the fixture's own sandbox holds a
        slot, and sizing the fill as if it did not would ask for one more
        sandbox than the cluster can place, which fails as a create error
        rather than as the refusal this scenario is about.

        Each sandbox is created through the run-owned collection, so its
        cleanup is armed before the create and a failure mid-fill still tears
        the fill down.
        """

        self._fixture.verify(
            self._budget > 0,
            "the slot budget has not been established, so the fill has no target",
        )
        already = self._slots_in_use()
        self._fixture.record("slots_in_use_before_fill", already)
        self._fixture.verify(
            already <= self._budget,
            f"{already} sandbox pods already hold slots against a budget of {self._budget}, so "
            "this bench was not clean when the scenario started",
        )
        for _ in range(self._budget - already):
            self._ballast.append(self._fixture.sandboxes.create(metadata=self._fill_metadata()))
        deadline = time.monotonic() + timeout
        while True:
            in_use = self._slots_in_use()
            if in_use >= self._budget:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                f"only {in_use} of {self._budget} network slots are held after the fill, so the "
                "cluster is not full and a create would be refused for some other reason, or "
                "not at all",
                slots=self._control.sandbox_node_slots(),
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record("slots_in_use_after_fill", in_use)
        # Per node, not just in total: a cluster that is "full" because one
        # node took every slot while another sat empty would leave the check
        # with somewhere to place, and the refusal below would never come --
        # but a total-only count cannot tell the two apart.
        per_node = self._control.sandbox_node_slots()
        starved = {
            name: facts for name, facts in per_node.items() if facts["used"] < facts["capacity"]
        }
        self._fixture.verify(
            not starved,
            "a sandbox node still has a free network slot after the fill, so the cluster is full "
            "only in total and the check still has somewhere to place a create",
            free=starved,
        )

    def _fill_metadata(self) -> Mapping[str, str]:
        return {"space_smoke_role": "network-slot-fill"}

    # --- the switch -------------------------------------------------------

    def enable_slot_check(self, *, rollout_timeout: float = 240) -> None:
        """Turn the slot check on, capturing and restoring the standing value.

        Enabled *after* the fill on purpose. The check refuses a create while
        no node has a free slot, and the fill's own creates are exactly that
        shape once the last of them lands -- so enabling it first would have
        the fill racing the thing it exists to set up.
        """

        release = self._control.acquire_public_api_mutation()
        self._release_lease = self._scope.defer("release the public API mutation lease", release)
        standing = self._control.public_api_env(SLOT_ENV_KEYS)
        self._fixture.record("slot_check_env_before", dict(standing))

        def restore() -> None:
            self._control.restore_public_api_env(standing)
            self._control.wait_public_api_ready(timeout=rollout_timeout)

        self._restore_env = self._scope.defer(
            "restore the public API slot-check env", restore, critical=True
        )
        self._control.set_public_api_env({SLOT_CHECK_ENV: "true"})
        self._control.wait_public_api_ready(timeout=rollout_timeout)
        self._scope.defer(
            "kill every sandbox the slot probes admitted", self.sweep_admitted, critical=True
        )

    def restore_slot_check(self) -> None:
        for slot in ("_restore_env", "_release_lease"):
            compensation = getattr(self, slot)
            if compensation is not None:
                setattr(self, slot, None)
                compensation.release()

    # --- the probes -------------------------------------------------------

    def _create_once(self, *, timeout: float = 90) -> LimiterProbeOutcome:
        body: Mapping[str, object] = {
            "templateID": self._template,
            "timeout": 120,
            "metadata": {"space_smoke_role": "network-slot-probe"},
        }
        outcome = self._probe.race_ecw_request(
            "POST", "/sandboxes", body, client_id=self._client_id, timeout=timeout
        )
        if outcome.sandbox_id:
            self._admitted.append(outcome.sandbox_id)
        return outcome

    def expect_no_free_slot(self) -> LimiterProbeOutcome:
        """A cold create is refused with a retryable 503 naming the slot shortage.

        Verdicts, most-specific first:

        `refused at all` the create did not succeed. A create admitted here is
        the whole defect: every slot is held, so placing another sandbox is
        what the check exists to prevent.

        `refused as unavailable` the status is 503, not a 429. The two carry
        different retry semantics to a caller, and a rate-limit answer here
        would mean some other gate spoke first.

        `refused for the slot shortage` the message names a free sandbox slot.
        The same build raises a second 503 under the same `Server capacity
        limit:` prefix from its empty-cluster guard, so matching the prefix
        alone would let a bench whose nodes had gone ineligible satisfy this
        while proving nothing about slots. This is the verdict that makes the
        phase about its subject.

        `retryable` the refusal carries `Retry-After` in delta-seconds. Slots
        free as sandboxes stop, which the server cannot predict, so the hint
        is the only pacing a caller gets.
        """

        outcome = self._create_once()
        self._fixture.record("no_free_slot_status", outcome.status)
        self._fixture.record("no_free_slot_message", outcome.message[:200])
        self._fixture.record("no_free_slot_retry_after", outcome.retry_after)
        self._fixture.verify(
            not 200 <= outcome.status < 300,
            "a cold create was admitted while every eligible node's network slots were held",
            status=outcome.status,
            sandbox_id=outcome.sandbox_id,
        )
        self._fixture.verify(
            outcome.status == 503,
            f"the create was refused with {outcome.status}, not the 503 the slot check answers",
            status=outcome.status,
            response_message=outcome.message[:200],
        )
        self._fixture.verify(
            outcome.message.startswith(CAPACITY_LIMIT_MESSAGE)
            and NO_FREE_SLOT_MESSAGE in outcome.message,
            "the 503 does not name a free sandbox slot, so it is not the slot check speaking -- "
            "the same prefix covers the empty-cluster refusal, which says nothing about slots",
            response_message=outcome.message[:200],
        )
        self._fixture.verify(
            has_retry_after_seconds(outcome),
            "the slot-shortage 503 carried no Retry-After delta-seconds",
            retry_after=outcome.retry_after,
        )
        return outcome

    def expect_reconnect_admitted(self, sandbox_id: str) -> None:
        """A reconnect to an already-running sandbox is never refused.

        It adds no runtime placement, so it never consults the slot check. A
        bench that refused it would be refusing on capacity a reconnect does
        not consume, which is the failure mode an operator would feel first:
        every live sandbox unreachable because the cluster is full.
        """

        outcome = self._probe.race_ecw_request(
            "POST", f"/sandboxes/{sandbox_id}/connect", {"timeout": 30},
            client_id=self._client_id, timeout=90,
        )
        self._fixture.record("no_free_slot_reconnect_status", outcome.status)
        self._fixture.verify(
            200 <= outcome.status < 300,
            "a reconnect to an already-running sandbox was refused while the cluster had no free "
            "slots, though a reconnect adds no placement and never consults the slot check",
            status=outcome.status,
            response_message=outcome.message[:200],
        )

    def expect_recovery(self, *, timeout: float = 600) -> None:
        """Freeing a slot lets a create through again.

        The verdict that separates a working check from a cluster that is
        simply broken: without it, a build that refused every create for any
        reason would satisfy every verdict above.
        """

        self._fixture.verify(
            bool(self._ballast),
            "the fill created nothing, so there is no slot to free and recovery would be "
            "asserted against a cluster this scenario never filled",
        )
        released = self._ballast.pop()
        released.kill()
        deadline = time.monotonic() + timeout
        while True:
            # `used + terminating`, not `used`. kube-scheduler charges a
            # terminating pod's slot request until the pod is gone, so a node
            # whose pod is merely on its way out has no room to place anything
            # -- and a create sent then gets past the slot check and is
            # refused by the scheduler instead, which reads as the check
            # failing to recover when nothing is wrong with it. Found exactly
            # that way on the first bench run of this scenario.
            free = {
                name: facts
                for name, facts in self._control.sandbox_node_slots().items()
                if facts["used"] + facts["terminating"] < facts["capacity"]
            }
            if free:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "no node reported a free network slot after a filled sandbox was killed, so the "
                "recovery probe below would be asking the same question as the refusal above",
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record("slots_free_after_release", free)
        outcome = self._create_once()
        self._fixture.record("recovery_statuses", status_histogram([outcome]))
        self._fixture.verify(
            200 <= outcome.status < 300,
            "a create was still refused after a network slot came free, so the refusal above was "
            "not the slot check reacting to the slots",
            status=outcome.status,
            response_message=outcome.message[:200],
        )

    def sweep_admitted(self) -> None:
        """Kill every sandbox a raw probe admitted, by its run metadata.

        The recovery probe's create is the one that succeeds, and it is made
        through the raw ECW route rather than the run-owned collection, so
        nothing else would reap it. Guarded on something having been admitted:
        `kill_matching` treats zero matches as a create that never became
        visible and raises, and a run where every probe was refused is a
        legitimate outcome of this scenario rather than a teardown failure.
        """

        if self._admitted:
            self._probe.kill_matching({"space_smoke_role": "network-slot-probe"})

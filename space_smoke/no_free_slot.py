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

Kind CI runs it by name on a job of its own, whose bench is brought up with
`SPACE_KIND_NETWORK_POOL_MAX_SLOTS` set so each sandbox worker advertises a
couple of slots, and which runs nothing else on that bench.

The fill is real sandboxes, not a stand-in. The limiter's slot accounting comes
from a reflector of the API's own sandbox pods, so a bystander pod requesting
the resource would be invisible to it however much kube-scheduler respects the
request -- the bench has to actually hold the slots with the thing the check
counts.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    EcwSandbox,
    EcwSandboxDeleteRetryable,
    LimiterProbeOutcome,
    RateLimitProbe,
    RateLimiterControl,
)
from space_smoke.rate_limiters import (
    CAPACITY_LIMIT_MESSAGE,
    has_retry_after_seconds,
    status_histogram,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SLOT_CHECK_ENV = "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED"
# The tail of the slot check's 503 (`check_network_slot_headroom`). The
# empty-cluster guard's 503 shares the `CAPACITY_LIMIT_MESSAGE` prefix, so the
# tail is what tells the two apart.
NO_FREE_SLOT_MESSAGE = "no eligible sandbox node has a free sandbox slot"
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
        self._probed = False
        self._budget = 0
        self._client_id = "space-smoke-slots"
        # Read from the fixture at construction, never left at a guess:
        # the fill goes through the run-owned collection and so uses whatever
        # template the fixture selected, and a probe create on a different one
        # answers 404. That would red `expect_no_free_slot` on "refused with
        # 404, not the 503" and make `expect_recovery` unsatisfiable -- both
        # blaming the slot check for a template mismatch.
        self._template = fixture.sandbox_template_name()
        # Per run, like the sibling limiter class's. `kill_matching` is JSONB
        # containment over the pairs it is given, so a role alone would let
        # this teardown reach a concurrent bench's sandboxes, or a stale one
        # from an aborted earlier run of this scenario.
        self._run_token = uuid.uuid4().hex

    # --- the bench's slot budget -----------------------------------------

    def expect_fillable_slot_budget(self, *, most_per_node: int = 4) -> int:
        """Require a small, uniform slot budget and return its total.

        The precondition the whole scenario rests on, and a verdict rather
        than a read: at the chart's default every node advertises hundreds of
        slots, and a fill sized from that would be many hundreds of VM boots. A
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
            "no Ready, uncordoned, unblocked node matches the sandbox node selector, so this bench has no "
            "start-eligible node whose slots could be filled",
        )
        capacities = {name: facts["capacity"] for name, facts in slots.items()}
        self._fixture.verify(
            all(capacity > 0 for capacity in capacities.values()),
            "an eligible sandbox node advertises no network slots at all, so nothing can be "
            "placed on it and the fill below would never make the cluster full",
            capacities=capacities,
        )
        distinct = set(capacities.values())
        self._fixture.verify(
            len(distinct) == 1,
            "the eligible sandbox nodes advertise different slot counts, so the fill cannot be "
            "sized from the chart value the bench was given",
            capacities=capacities,
        )
        per_node = distinct.pop()
        self._fixture.verify(
            per_node <= most_per_node,
            f"each sandbox node advertises {per_node} network slots, more than the {most_per_node} "
            "this scenario will fill. Set the node-manager chart's `networkPoolMaxSlots` to a "
            "small count for this bench; at the release default the fill would be many hundreds "
            "of real VM boots",
            capacities=capacities,
        )
        self._budget = per_node * len(capacities)
        self._fixture.record(
            "slot_budget", {"per_node": per_node, "nodes": len(capacities), "total": self._budget}
        )
        return self._budget

    def _slots_in_use(self) -> int:
        """Slots held by pods that are staying, terminating ones excluded.

        There are two different questions to ask of these counts and they
        want different sums. *How many slots are durably held* -- which is
        what sizes the fill and what tells it when it is done -- is `used`
        alone: a terminating pod is on its way out, so counting it would
        under-size the fill by exactly that many, the fill would never reach
        the budget, and the wait below would spin to its deadline and then
        blame the bench for an arithmetic error here. *Whether there is room
        to place something* is `used + terminating`, because kube-scheduler
        charges a terminating pod's request until the pod is gone; that is
        the rule `sandbox_node_slots` documents, and it is what the starvation
        check and `expect_recovery` read.
        """

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
        # kube-scheduler still charges a deleting pod's slot -- the baseline
        # smoke's sandbox, or an aborted earlier run's -- so a fill sized
        # while one is on its way out would ask for more than fits and fail
        # as a create error. Wait for those to go first.
        drain_deadline = time.monotonic() + timeout
        while True:
            leaving = {
                name: facts["terminating"]
                for name, facts in self._control.sandbox_node_slots().items()
                if facts["terminating"]
            }
            if not leaving:
                break
            self._fixture.verify(
                time.monotonic() < drain_deadline,
                "sandbox pods were still deleting when the fill had to start, so the scheduler "
                "would charge their slots and the fill would not fit",
                terminating=leaving,
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        already = self._slots_in_use()
        self._fixture.record("slots_in_use_before_fill", already)
        self._fixture.verify(
            already < self._budget,
            f"{already} sandbox pods already hold slots against a budget of {self._budget}, so "
            "this bench was not clean when the scenario started and the fill would create "
            "nothing that recovery could later free",
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
        # A second, independent read, and what it actually guards is node-set
        # churn rather than an uneven fill: the loop above exits on
        # `sum(used) >= budget`, capacity is proven uniform, and the scheduler
        # cannot place more slot-requesting pods on a node than it advertises
        # -- so by arithmetic every node is already at capacity when the loop
        # breaks. What can still be true here is that the fleet changed under
        # the fill: a node joined, or one came back cordon-free, after the
        # totals were satisfied. That node has room, and the refusal below
        # would never come.
        per_node = self._control.sandbox_node_slots()
        # A fresh read, so it needs the same guard the budget read has: an
        # empty one means no eligible sandbox node is visible any more, and
        # `not starved` over nothing would pass having judged nothing. The
        # refusal that followed would then come from the empty-cluster guard,
        # which says nothing about slots.
        self._fixture.verify(
            bool(per_node),
            "no eligible sandbox node is visible after the fill, so there is nothing to call full "
            "and a refusal would be the empty-cluster guard rather than the slot check",
        )
        starved = {
            name: facts
            for name, facts in per_node.items()
            # `used` alone: the slot check stops counting a pod once it's
            # deleting, so a node whose fill pod is on its way out is a node
            # the API sees room on, however the scheduler charges it.
            if facts["used"] < facts["capacity"]
        }
        self._fixture.verify(
            not starved,
            "a sandbox node still has a free network slot after the fill, so the cluster is full "
            "only in total and the check still has somewhere to place a create",
            free=starved,
        )

    def _wait_no_unbound_pods(self, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while unbound := self._control.unbound_sandbox_pods():
            self._fixture.verify(
                time.monotonic() < deadline,
                "a sandbox pod is still waiting for a node after the retried probes were killed, "
                "so it would take the slot recovery frees",
                unbound=unbound,
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)

    def _fill_metadata(self) -> Mapping[str, str]:
        return {"space_smoke_role": "network-slot-fill", "space_smoke_slots_run": self._run_token}

    def _probe_metadata(self) -> dict[str, str]:
        return {"space_smoke_role": "network-slot-probe", "space_smoke_slots_run": self._run_token}

    # --- the switch -------------------------------------------------------

    def enable_slot_check(self, *, rollout_timeout: float = 240) -> None:
        """Turn the slot check on, capturing and restoring the standing value.

        Enabled after the fill, so nothing the fill does depends on the switch
        and the refusal below differs from the fill's creates in that one
        respect. A full index alone doesn't make the point: the substrate's
        own network-slot preflight also refuses then, switch or no switch,
        with a 503 of its own that `expect_no_free_slot` tells apart by its
        message.
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
            "metadata": self._probe_metadata(),
        }
        # Before the request, not after it: a create the server commits while
        # this thread is interrupted mid-read still has to reach the sweep.
        self._probed = True
        outcome = self._probe.race_ecw_request(
            "POST", "/sandboxes", body, client_id=self._client_id, timeout=timeout
        )
        return outcome

    @staticmethod
    def _is_slot_shortage(outcome: LimiterProbeOutcome) -> bool:
        return (
            outcome.status == 503
            and outcome.message.startswith(CAPACITY_LIMIT_MESSAGE)
            and NO_FREE_SLOT_MESSAGE in outcome.message
        )

    def expect_no_free_slot(self, *, warm_timeout: float = 180) -> LimiterProbeOutcome:
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

        The first answer isn't always the verdict. `/readyz` covers the API's
        sandbox-pod reflector but not its node one, so a replica the rollout
        that set the switch just started can serve before its node cache is
        warm. The slot check reads a cold cache as unknown headroom and
        admits, and with every slot held the create then fails further in --
        the substrate preflight reads the same cold cache, so the pod is made
        and never scheduled -- as some other 503 or a client timeout. So any
        refusal that isn't the slot shortage is retried until `warm_timeout`,
        and the API's own answer is the readiness signal. An admitted create
        is never retried: with every slot held, that's the defect.

        Each retry is also a live-cap admission whose pending member lingers
        for the cap's TTL, so a bench with a per-client live cap set would
        answer these retries with 429s. The Kind bench sets none.
        """

        deadline = time.monotonic() + warm_timeout
        outcomes = [self._create_once()]
        while (
            not 200 <= outcomes[-1].status < 300
            and not self._is_slot_shortage(outcomes[-1])
            and time.monotonic() < deadline
        ):
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
            outcomes.append(self._create_once())
        outcome = outcomes[-1]
        self._fixture.record("no_free_slot_statuses", status_histogram(outcomes))
        if len(outcomes) > 1:
            # The API deletes a failed create's pod and row itself, so this
            # covers the attempt that timed out client-side while the server
            # was still creating: its pod waits for a slot and would take the
            # one recovery frees. A delete that conflicts with that in-flight
            # create is left to the deferred sweep; the wait below holds until
            # the pod is gone either way.
            try:
                self._kill_probe_sandboxes()
            except EcwSandboxDeleteRetryable:
                pass
            self._wait_no_unbound_pods(timeout=warm_timeout)
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
            outcome.status != 0,
            "the create got no HTTP answer, so the API refused nothing: it was unreachable or "
            "didn't answer before the client timeout",
            response_message=outcome.message[:200],
        )
        self._fixture.verify(
            outcome.status == 503,
            f"the create was refused with {outcome.status}, not the 503 the slot check answers",
            status=outcome.status,
            response_message=outcome.message[:200],
        )
        self._fixture.verify(
            self._is_slot_shortage(outcome),
            "the 503 does not name a free sandbox slot, so it is not the slot check speaking. "
            "With the switch not in force, the substrate's network-slot preflight answers a "
            "503 of its own, and the empty-cluster guard shares the capacity-limit prefix",
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

    def expect_recovery(self, *, timeout: float = 600, retry_timeout: float = 120) -> None:
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
        free_deadline = time.monotonic() + timeout
        while True:
            # `used + terminating`, not `used`. kube-scheduler charges a
            # terminating pod's slot request until the pod is gone, so a node
            # whose pod is merely on its way out has no room to place anything
            # -- and a create sent then gets past the slot check and is
            # refused by the scheduler instead, which reads as the check
            # failing to recover when nothing is wrong with it.
            free = {
                name: facts
                for name, facts in self._control.sandbox_node_slots().items()
                if facts["used"] + facts["terminating"] < facts["capacity"]
            }
            if free:
                break
            self._fixture.verify(
                time.monotonic() < free_deadline,
                "no node reported a free network slot after a filled sandbox was killed, so the "
                "recovery probe below would be asking the same question as the refusal above",
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record("slots_free_after_release", free)
        # The wait above reads the Kubernetes API. The slot check reads the
        # public API's own reflector of sandbox pods, and drops a pod from its
        # index once it's deleting, so it normally sees the freed slot before
        # this wait ends. A watch that falls behind can still leave the index
        # stale, and a slot-shortage 503 then is the check answering from that
        # index, not failing to recover. So only that refusal is retried, and
        # only until its own deadline, so a slow slot release above can't eat
        # the retry window; every other answer is the verdict.
        retry_deadline = time.monotonic() + retry_timeout
        outcomes = [self._create_once()]
        while self._is_slot_shortage(outcomes[-1]) and time.monotonic() < retry_deadline:
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
            outcomes.append(self._create_once())
        outcome = outcomes[-1]
        self._fixture.record("recovery_statuses", status_histogram(outcomes))
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
        nothing else would reap it. Skipped when no create was attempted. After
        an attempt, zero matches is swallowed below: `kill_matching` raises on
        it as a create that never became visible, but a run where every probe
        was refused is a legitimate outcome of this scenario.
        """

        if not self._probed:
            return
        self._kill_probe_sandboxes()

    def _kill_probe_sandboxes(self) -> None:
        try:
            self._probe.kill_matching(self._probe_metadata())
        except EcwSandboxDeleteRetryable:
            # A delete that conflicted with an in-flight transition, not an
            # empty match. `EcwSandboxBusy` subclasses `RuntimeError`, so a
            # bare catch below would swallow it, report this critical
            # compensation as succeeded, and leave a live VM on a bench whose
            # whole premise is that slots are scarce -- where the next run
            # then fails its own cleanliness precondition for a reason it
            # cannot diagnose.
            raise
        except RuntimeError:
            # Zero matches. `kill_matching` treats that as a create that never
            # became visible and raises; here it is the ordinary outcome of a
            # run whose probes were all refused, which is what this scenario
            # is usually proving. The guard is "a create was attempted", not
            # "an id came back": a create the server committed but whose reply
            # the client never read answers `status=0` with no `sandbox_id`,
            # and keying off the id would strand that sandbox on the bench.
            return

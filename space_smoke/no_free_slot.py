"""The activation limiter's slot check, proven with a scenario-scoped slot pool.

A sandbox pod requests one `space.pplx.net/network-slots` from its node, and
node-ctl advertises each node's count from the node-manager's network pool. The
activation limiter refuses a cold create with a 503 when no start-eligible node
has a free slot, on its own switch
(`SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED`) rather than the
limiter's.

**Why this scopes the worker slot count to the scenario.** At the chart's
standing default each worker advertises hundreds of slots, and filling them
would take hundreds of Firecracker boots. Instead of dedicating a CI bench to
this scenario, it captures the standing `SPACE_NODEMGR_NETWORK_POOL_MAX_SLOTS`
literal and per-node capacities under the mutation Leases, checks that no
worker holds a persisted slot at or above the shrunk count (`UnsafeShrink` in
`slot_pool/db.rs`), shrinks worker `SPACE_NODEMGR_NETWORK_POOL_MAX_SLOTS` to a
small count, awaits each sandbox-pool node's `capacity` and `allocatable`, and
restores both knobs in scenario phases on exit.

**Why fullness is judged by the API's answer.** The slot check is switched on
before filling, and the scenario creates sandboxes sequentially (capped at a
handful of attempts) until the API answers its first slot-shortage 503, then
asserts that admitted sandboxes (fixture included) equal the sum of
sandbox-pool nodes' `status.allocatable` network slots. That avoids maintaining
a second copy of the API's node-eligibility rules in Python while still catching
any drift between advertised capacity and admission behavior.
"""

from __future__ import annotations

import dataclasses
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from functools import partial
from typing import TYPE_CHECKING, Any

from space_smoke.adapters import expected_worker_network_pool_capacity
from space_smoke.capabilities import (
    EcwSandboxDeleteRetryable,
    EcwSandboxReconcileUnmatched,
    LimiterProbeOutcome,
    OccupiedNetworkSlot,
    RateLimiterControl,
    RateLimitProbe,
    SandboxNodeSlots,
)
from space_smoke.rate_limiters import (
    CLEANUP_ENV_BUDGET_SECONDS,
    CLEANUP_ROLLOUT_TIMEOUT_SECONDS,
    NO_FREE_SLOT_MESSAGE,
    RESTORE_ROLLOUT_TIMEOUT_SECONDS,
    create_uncertain,
    has_retry_after_seconds,
    is_slot_shortage,
    status_histogram,
)
from space_smoke.runtime import Compensation, ResourceScope, ScenarioTimedOut

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SLOT_CHECK_ENV = "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED"
SLOT_ENV_KEYS: tuple[str, ...] = (SLOT_CHECK_ENV,)
_PROBE_CLEANUP_ATTEMPTS = 10
_PROBE_CLEANUP_RETRY_SECONDS = 1.0
_BEST_EFFORT_REAP_SECONDS = 15.0
_CLEANUP_SLOT_SETTLE_SECONDS = 15.0
_CLEANUP_WORKER_RESTORE_BUDGET_SECONDS = 40.0
_NODE_CTL_SLOT_RECONCILE_NUDGE_SECONDS = 5.0

__all__ = ["NO_FREE_SLOT_MESSAGE", "NoFreeSlotAssertions", "SLOT_CHECK_ENV"]


def _slots_evidence(nodes: Mapping[str, SandboxNodeSlots]) -> dict[str, dict[str, int]]:
    return {name: dataclasses.asdict(slots) for name, slots in nodes.items()}


def _occupied_evidence(
    occupied: Mapping[str, Sequence[OccupiedNetworkSlot]],
) -> dict[str, list[dict[str, object]]]:
    return {
        name: [dataclasses.asdict(row) for row in rows]
        for name, rows in occupied.items()
    }


class NoFreeSlotAssertions:
    """Shrink worker network slots for this scenario, fill sequentially to 503, then restore."""

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
        self._release_public_lease: Compensation | None = None
        self._release_node_manager_lease: Compensation | None = None
        self._restore_worker_slots: Compensation | None = None
        self._restore_slot_check: Compensation | None = None
        self._standing_slot_check_env: dict[str, str | None] | None = None
        self._standing_worker_slots_env: str | None = None
        self._standing_capacities: dict[str, int] = {}
        self._slot_check_phase_restore_failed = False
        self._worker_slots_phase_restore_failed = False
        self._filled_ids: list[str] = []
        self._admitted: list[str] = []
        self._uncertain_probes: list[dict[str, str]] = []
        self._client_id = "space-smoke-slots"
        # Read the full selector (`name`, `name:tag`, or `name@digest`) from the
        # fixture at construction, never stripped to the bare name: stripping a
        # `:tag` or `@digest` would probe `default` (or 404 when no `default`
        # tag exists) instead of the exact template release the bench staged.
        self._template = fixture.sandbox_template_selector()
        self._run_token = uuid.uuid4().hex
        self._scope.defer(
            "kill every sandbox the slot probes admitted", self.sweep_admitted, critical=True
        )

    def _phase(self, name: str) -> AbstractContextManager[Any]:
        phase = getattr(self._fixture, "phase", None)
        return phase(name) if phase is not None else nullcontext()

    def _redefer_above_sandboxes(self) -> None:
        """Keep env restores and Lease releases ahead of sandbox deletes in LIFO backstop cleanup."""

        ordered = [
            comp
            for comp in (
                self._release_public_lease,
                self._release_node_manager_lease,
                self._restore_worker_slots,
                self._restore_slot_check,
            )
            if comp is not None and comp.armed
        ]
        if ordered:
            self._scope.redefer(*ordered)

    @contextmanager
    def scoped_slot_pool(self) -> Iterator[None]:
        """Reap probe sandboxes and restore both cluster knobs in scenario phases on exit.

        `ResourceScope` compensations stay armed as bounded backstops when
        `ScenarioTimedOut` or a non-`Exception` interrupt skips scenario-phase
        unwinding after the execution budget is spent.
        """

        try:
            yield
        except BaseException as body_error:
            if isinstance(body_error, Exception) and not isinstance(body_error, ScenarioTimedOut):
                self._unwind_remaining(body_error)
            raise
        self._unwind_remaining(None)

    def _run_unwind_step(
        self,
        phase_name: str,
        record_key: str,
        step: Callable[[], None],
        first_error: Exception | None,
    ) -> tuple[Exception | None, bool]:
        try:
            with self._phase(phase_name):
                step()
        except Exception as step_error:  # noqa: BLE001 - attached to first_error or re-raised
            timed_out = isinstance(step_error, ScenarioTimedOut)
            if first_error is None:
                return step_error, timed_out
            self._fixture.record(record_key, repr(step_error)[:1_000])
            first_error.add_note(
                f"{phase_name} also failed while unwinding: {step_error!r}; "
                "the bounded cleanup compensation stays armed"
            )
            return first_error, timed_out
        return first_error, False

    def _unwind_remaining(self, primary_error: Exception | None) -> None:
        first_error = primary_error
        timed_out = False
        if self._admitted or self._uncertain_probes:
            first_error, timed_out = self._run_unwind_step(
                "reap sandboxes created by slot probes",
                "slot_probe_reap_failure",
                self.sweep_admitted,
                first_error,
            )
        if (
            not timed_out
            and self._restore_slot_check is not None
            and not self._slot_check_phase_restore_failed
        ):
            first_error, timed_out = self._run_unwind_step(
                "restore the public API slot-check env",
                "slot_check_restore_failure",
                lambda: self.restore_slot_check(release_leases=False),
                first_error,
            )
        if (
            not timed_out
            and self._restore_worker_slots is not None
            and not self._worker_slots_phase_restore_failed
        ):
            first_error, timed_out = self._run_unwind_step(
                "restore worker network-slot capacity",
                "worker_slots_restore_failure",
                lambda: self.restore_worker_slots(release_leases=False),
                first_error,
            )
        if (
            not timed_out
            and self._restore_slot_check is None
            and self._restore_worker_slots is None
            and (self._release_node_manager_lease is not None or self._release_public_lease is not None)
        ):
            first_error, _ = self._run_unwind_step(
                "release the slot-pool mutation leases",
                "slot_lease_release_failure",
                self.release_mutation_leases,
                first_error,
            )
        if primary_error is None and first_error is not None:
            raise first_error

    def _ensure_public_lease(self) -> None:
        if self._release_public_lease is None:
            release = self._control.acquire_public_api_mutation()
            self._release_public_lease = self._scope.defer(
                "release the public API mutation lease", release, critical=True
            )
            self._redefer_above_sandboxes()

    def _ensure_node_manager_lease(self) -> None:
        self._ensure_public_lease()
        if self._release_node_manager_lease is None:
            release = self._control.acquire_worker_network_slots_mutation()
            self._release_node_manager_lease = self._scope.defer(
                "release the node-manager mutation lease", release, critical=True
            )
            self._redefer_above_sandboxes()

    def release_mutation_leases(self) -> None:
        for attr in ("_release_node_manager_lease", "_release_public_lease"):
            compensation = getattr(self, attr)
            if compensation is not None:
                compensation.release()
                setattr(self, attr, None)

    # --- scenario-scoped worker slot pool ---------------------------------

    def _wait_no_terminating_sandbox_pods(self, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            leaving = {
                name: slots.terminating
                for name, slots in self._control.sandbox_node_slots().items()
                if slots.terminating > 0
            }
            if not leaving:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "sandbox pods were still deleting when the scenario needed stable network-slot usage",
                terminating=leaving,
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)

    def _await_worker_slots_settled(
        self, expected_by_node: Mapping[str, int], *, timeout: float
    ) -> dict[str, SandboxNodeSlots]:
        deadline = time.monotonic() + timeout
        last_nudge: float | None = None
        last: dict[str, SandboxNodeSlots] = {}
        while True:
            last = self._control.sandbox_node_slots()
            if last and all(
                node_name in last
                and last[node_name].capacity == expected
                and last[node_name].allocatable == expected
                for node_name, expected in expected_by_node.items()
            ):
                return last
            now = time.monotonic()
            if now >= deadline:
                raise RuntimeError(
                    f"sandbox nodes did not report network-slot capacity and allocatable "
                    f"{dict(expected_by_node)} within {timeout:g}s (last read: {last})"
                )
            if last_nudge is None or now - last_nudge >= _NODE_CTL_SLOT_RECONCILE_NUDGE_SECONDS:
                for node_name, expected in expected_by_node.items():
                    if node_name in last and (
                        last[node_name].capacity != expected or last[node_name].allocatable != expected
                    ):
                        self._control.publish_node_ctl_facet(node_name, "template-inventory")
                last_nudge = time.monotonic()
            self._fixture.wait(self._fixture.lifecycle_poll_interval)

    def _restore_standing_worker_slots(
        self,
        standing_env: str | None,
        standing_capacities: Mapping[str, int],
        *,
        rollout_timeout: float,
        settle_timeout: float,
        shared_deadline: bool = False,
    ) -> None:
        self._control.restore_worker_network_pool_max_slots_env(
            standing_env,
            standing_capacities,
            rollout_timeout=rollout_timeout,
            settle_timeout=settle_timeout,
            shared_deadline=shared_deadline,
        )

    def shrink_worker_slots(
        self,
        *,
        slots_per_node: int = 2,
        rollout_timeout: float = 300,
        settle_timeout: float = 120,
        teardown_grace: float = 30,
    ) -> dict[str, SandboxNodeSlots]:
        """Shrink worker `SPACE_NODEMGR_NETWORK_POOL_MAX_SLOTS` after proving `UnsafeShrink` safe."""

        if slots_per_node < 1:
            raise ValueError(f"slots_per_node must be >= 1, got {slots_per_node}")
        self._wait_no_terminating_sandbox_pods(timeout=settle_timeout)
        self._ensure_node_manager_lease()
        standing_env = self._control.worker_network_pool_max_slots_env()
        self._fixture.record("worker_network_pool_max_slots_before", standing_env)
        expected_standing = expected_worker_network_pool_capacity(standing_env)
        standing_slots = self._control.sandbox_node_slots()
        self._fixture.record("sandbox_node_slots_before", _slots_evidence(standing_slots))
        self._fixture.verify(
            bool(standing_slots),
            "no node matches `space.pplx.net/node-pool=sandbox`, so this bench has no sandbox worker "
            "whose network slots can be scoped",
        )
        self._fixture.verify(
            all(
                node.capacity == expected_standing and node.allocatable == expected_standing
                for node in standing_slots.values()
            ),
            f"sandbox-pool nodes do not match the standing worker network-slot setting {standing_env!r} "
            f"(expected capacity and allocatable {expected_standing}) before shrink",
            standing_env=standing_env,
            expected_capacity=expected_standing,
            nodes=_slots_evidence(standing_slots),
        )
        standing_capacities = {name: node.capacity for name, node in standing_slots.items()}

        # Refuse to shrink if any worker holds a slot `>= slots_per_node` that
        # `validate_startup_ownership` (`slot_pool/db.rs`) would reject with
        # `UnsafeShrink`. Active ownership fails immediately; unowned interface
        # teardown from an earlier scenario gets a short settle window first.
        grace_deadline = time.monotonic() + teardown_grace
        while True:
            occupied = self._control.occupied_worker_network_slots_at_or_above(slots_per_node)
            if not occupied:
                break
            owned = {
                node: tuple(row for row in rows if row.owned)
                for node, rows in occupied.items()
                if any(row.owned for row in rows)
            }
            occupied_json = _occupied_evidence(occupied)
            self._fixture.verify(
                not owned,
                f"refusing to shrink worker network slots to {slots_per_node} (`UnsafeShrink`): "
                f"a worker holds an active network slot at or above slot {slots_per_node}",
                occupied=occupied_json,
            )
            self._fixture.verify(
                time.monotonic() < grace_deadline,
                f"refusing to shrink worker network slots to {slots_per_node} (`UnsafeShrink`): "
                f"a worker still has un-reset network-slot state at or above slot {slots_per_node}",
                occupied=occupied_json,
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)

        self._standing_worker_slots_env = standing_env
        self._standing_capacities = standing_capacities
        self._worker_slots_phase_restore_failed = False
        fallback_budget = min(
            rollout_timeout + _CLEANUP_SLOT_SETTLE_SECONDS,
            _CLEANUP_WORKER_RESTORE_BUDGET_SECONDS,
        )

        def backstop_restore() -> None:
            self._restore_standing_worker_slots(
                standing_env,
                standing_capacities,
                rollout_timeout=fallback_budget,
                settle_timeout=_CLEANUP_SLOT_SETTLE_SECONDS,
                shared_deadline=True,
            )

        self._restore_worker_slots = self._scope.defer(
            "restore worker network-slot capacity",
            backstop_restore,
            critical=True,
            timeout_seconds=CLEANUP_ENV_BUDGET_SECONDS,
        )
        self._redefer_above_sandboxes()

        self._control.set_worker_network_pool_max_slots_env(
            str(slots_per_node),
            record=(standing_env, standing_capacities),
            rollout_timeout=rollout_timeout,
        )
        target_by_node = dict.fromkeys(standing_capacities, slots_per_node)
        settled = self._await_worker_slots_settled(target_by_node, timeout=settle_timeout)
        settled_json = _slots_evidence(settled)
        self._fixture.record("sandbox_node_slots_shrunk", settled_json)
        total_allocatable = sum(node.allocatable for node in settled.values())
        self._fixture.verify(
            total_allocatable >= 2,
            f"shrunk sandbox nodes advertise only {total_allocatable} allocatable network slot in total, "
            "and this scenario keeps its fixture sandbox running while filling and freeing at least one more",
            nodes=settled_json,
        )
        return settled

    def restore_worker_slots(self, *, release_leases: bool = True) -> None:
        if self._restore_worker_slots is not None:
            try:
                self._restore_standing_worker_slots(
                    self._standing_worker_slots_env,
                    self._standing_capacities,
                    rollout_timeout=RESTORE_ROLLOUT_TIMEOUT_SECONDS,
                    settle_timeout=120.0,
                )
            except Exception:
                self._worker_slots_phase_restore_failed = True
                raise
            self._restore_worker_slots.disarm()
            self._restore_worker_slots = None
        if release_leases and self._restore_slot_check is None:
            self.release_mutation_leases()

    # --- the public-API switch --------------------------------------------

    def _restore_standing_slot_check(
        self,
        standing: Mapping[str, str | None],
        *,
        rollout_timeout: float,
        ready_timeout: float,
        shared_deadline: bool = False,
    ) -> None:
        self._control.restore_public_api_env(
            standing,
            rollout_timeout=rollout_timeout,
            shared_deadline=shared_deadline,
        )
        self._control.wait_public_api_ready(timeout=ready_timeout)

    def enable_slot_check(
        self,
        *,
        ready_timeout: float = 60,
    ) -> None:
        """Switch `SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED` on before filling."""

        self._ensure_public_lease()
        standing = self._control.public_api_env(SLOT_ENV_KEYS)
        self._fixture.record("slot_check_env_before", dict(standing))
        self._standing_slot_check_env = dict(standing)
        self._slot_check_phase_restore_failed = False

        def backstop_restore() -> None:
            self._restore_standing_slot_check(
                standing,
                rollout_timeout=CLEANUP_ROLLOUT_TIMEOUT_SECONDS,
                ready_timeout=min(ready_timeout, 10.0),
                shared_deadline=True,
            )

        self._restore_slot_check = self._scope.defer(
            "restore the public API slot-check env",
            backstop_restore,
            critical=True,
            timeout_seconds=CLEANUP_ENV_BUDGET_SECONDS,
        )
        self._redefer_above_sandboxes()
        self._control.set_public_api_env({SLOT_CHECK_ENV: "true"})
        self._control.wait_public_api_ready(timeout=ready_timeout)

    def restore_slot_check(self, *, release_leases: bool = True) -> None:
        if self._restore_slot_check is not None and self._standing_slot_check_env is not None:
            try:
                self._restore_standing_slot_check(
                    self._standing_slot_check_env,
                    rollout_timeout=RESTORE_ROLLOUT_TIMEOUT_SECONDS,
                    ready_timeout=60.0,
                )
            except Exception:
                self._slot_check_phase_restore_failed = True
                raise
            self._restore_slot_check.disarm()
            self._restore_slot_check = None
        if release_leases and self._restore_worker_slots is None:
            self.release_mutation_leases()

    # --- sequential fill & refusal verdicts -------------------------------

    def _probe_metadata(self) -> dict[str, str]:
        return {"space_smoke_role": "network-slot-probe", "space_smoke_slots_run": self._run_token}

    @staticmethod
    def _remaining_probe_timeout(deadline: float, *, limit: float = 90.0) -> float | None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        return min(limit, remaining)

    def _create_once(self, *, timeout: float = 90) -> LimiterProbeOutcome:
        probe_metadata = {
            **self._probe_metadata(),
            "space_smoke_slots_probe": uuid.uuid4().hex,
        }
        body: Mapping[str, object] = {
            "templateID": self._template,
            "timeout": 600,
            "metadata": probe_metadata,
        }
        try:
            outcome = self._probe.race_ecw_request(
                "POST", "/sandboxes", body, client_id=self._client_id, timeout=timeout
            )
        except BaseException:
            self._uncertain_probes.append(probe_metadata)
            raise
        if outcome.sandbox_id:
            self._admitted.append(outcome.sandbox_id)
        if create_uncertain(outcome):
            self._uncertain_probes.append(probe_metadata)
        return outcome

    def _wait_no_unbound_pods(self, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while unbound := self._control.unbound_sandbox_pods():
            if self._uncertain_probes and time.monotonic() < deadline:
                self._reap_uncertain_probes_best_effort(deadline=deadline)
                if not (unbound := self._control.unbound_sandbox_pods()):
                    return
            self._fixture.verify(
                time.monotonic() < deadline,
                "a sandbox pod is still waiting for a node after retried probes were reaped, "
                "so it would consume the slot recovery frees",
                unbound=unbound,
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)

    def fill_until_no_free_slot(
        self,
        *,
        max_creates: int = 8,
        warm_timeout: float = 180,
    ) -> LimiterProbeOutcome:
        """Create sequentially with the slot check on until the first slot-shortage 503.

        `/readyz` covers the API's sandbox-pod reflector but not its node
        reflector, so right after `enable_slot_check` rolls `space-api` a cold
        node cache can let one full-cluster create past pre-admission into a
        downstream scheduler timeout or non-slot-shortage 5xx. Non-2xx answers
        that aren't the slot-shortage 503 are reaped and retried within
        `warm_timeout` until the API answers from a warm cache.

        Once the slot-shortage 503 arrives, admitted sandboxes (including the
        fixture's own running sandbox) must equal the sum of `status.allocatable`
        network slots across `space.pplx.net/node-pool=sandbox` nodes.
        """

        if max_creates < 1:
            raise ValueError(f"max_creates must be >= 1, got {max_creates}")
        self._wait_no_terminating_sandbox_pods(timeout=warm_timeout)
        self._filled_ids = []
        warm_deadline = time.monotonic() + warm_timeout
        outcomes: list[LimiterProbeOutcome] = []
        refusal: LimiterProbeOutcome | None = None

        for _ in range(max_creates):
            outcome = self._create_once(timeout=min(90.0, max(warm_timeout, 1.0)))
            outcomes.append(outcome)
            if 200 <= outcome.status < 300 and outcome.sandbox_id:
                self._filled_ids.append(outcome.sandbox_id)
                continue
            while (
                not 200 <= outcome.status < 300
                and not is_slot_shortage(outcome)
                and time.monotonic() < warm_deadline
            ):
                if create_uncertain(outcome):
                    self._reap_uncertain_probes_best_effort(deadline=warm_deadline)
                self._fixture.wait(self._fixture.lifecycle_poll_interval)
                probe_timeout = self._remaining_probe_timeout(warm_deadline)
                if probe_timeout is None:
                    break
                outcome = self._create_once(timeout=probe_timeout)
                outcomes.append(outcome)
            if 200 <= outcome.status < 300 and outcome.sandbox_id:
                self._filled_ids.append(outcome.sandbox_id)
                continue
            refusal = outcome
            break

        had_uncertain = any(create_uncertain(item) for item in outcomes)
        if had_uncertain:
            self._reap_uncertain_probes_best_effort(
                deadline=time.monotonic() + _BEST_EFFORT_REAP_SECONDS
            )

        self._fixture.record("fill_admitted_count", len(self._filled_ids))
        self._fixture.record("no_free_slot_statuses", status_histogram(outcomes))
        if refusal is None:
            self._fixture.verify(
                False,
                f"{max_creates} sequential cold creates were all admitted without drawing the slot check's 503",
                admitted_creates=len(self._filled_ids),
                nodes=_slots_evidence(self._control.sandbox_node_slots()),
            )
            raise AssertionError("unreachable")
        self._fixture.record("no_free_slot_status", refusal.status)
        self._fixture.record("no_free_slot_message", refusal.message[:200])
        self._fixture.record("no_free_slot_retry_after", refusal.retry_after)
        self._fixture.verify(
            not 200 <= refusal.status < 300,
            "a cold create succeeded without returning a sandboxID instead of being refused for slot shortage",
            status=refusal.status,
            sandbox_id=refusal.sandbox_id,
        )
        self._fixture.verify(
            refusal.status != 0,
            "the create got no HTTP answer, so the API refused nothing: it was unreachable or "
            "didn't answer before the client timeout",
            response_message=refusal.message[:200],
        )
        self._fixture.verify(
            refusal.status == 503,
            f"the create was refused with {refusal.status}, not the 503 the slot check answers",
            status=refusal.status,
            response_message=refusal.message[:200],
        )
        self._fixture.verify(
            is_slot_shortage(refusal),
            "the 503 doesn't name a free sandbox slot, so it isn't the slot check speaking. "
            "With the switch off, the substrate's network-slot preflight answers a 503 of its own, "
            "and an unschedulable pod or proxy answers 503 without that message",
            response_message=refusal.message[:200],
        )
        self._fixture.verify(
            has_retry_after_seconds(refusal),
            "the slot-shortage 503 carried no Retry-After delta-seconds",
            retry_after=refusal.retry_after,
        )
        if had_uncertain:
            self._wait_no_unbound_pods(timeout=warm_timeout)

        nodes = self._control.sandbox_node_slots()
        allocatable_sum = sum(node.allocatable for node in nodes.values())
        admitted_with_fixture = 1 + len(self._filled_ids)
        self._fixture.record("admitted_with_fixture", admitted_with_fixture)
        self._fixture.record("sandbox_pool_allocatable_slots", allocatable_sum)
        self._fixture.verify(
            admitted_with_fixture == allocatable_sum,
            f"the API answered slot shortage after {admitted_with_fixture} admitted sandboxes "
            f"(fixture included), which doesn't equal the {allocatable_sum} status.allocatable "
            "network slots across sandbox-pool nodes",
            admitted_with_fixture=admitted_with_fixture,
            filled=len(self._filled_ids),
            allocatable_sum=allocatable_sum,
            nodes=_slots_evidence(nodes),
        )
        return refusal

    def expect_reconnect_admitted(self, sandbox_id: str) -> None:
        """A reconnect to an already-running sandbox is never refused when slots are full."""

        outcome = self._probe.race_ecw_request(
            "POST",
            f"/sandboxes/{sandbox_id}/connect",
            {"timeout": 600},
            client_id=self._client_id,
            timeout=90,
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
        """Freeing one filled sandbox lets a cold create succeed again."""

        self._fixture.verify(
            bool(self._filled_ids),
            "sequential fill admitted no extra sandboxes, so there's no filled slot to free for recovery",
        )
        released_id = self._filled_ids.pop()
        self._retry_delete(partial(self._probe.kill_sandbox, released_id))
        self._admitted = [sandbox_id for sandbox_id in self._admitted if sandbox_id != released_id]

        free_deadline = time.monotonic() + timeout
        while True:
            observed_slots = self._control.sandbox_node_slots()
            free = {
                name: slots
                for name, slots in observed_slots.items()
                if slots.used + slots.terminating < slots.allocatable
            }
            if free:
                break
            self._fixture.verify(
                time.monotonic() < free_deadline,
                "no sandbox node reported `used + terminating < allocatable` after killing a filled sandbox",
                nodes=_slots_evidence(observed_slots),
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record("slots_free_after_release", _slots_evidence(free))

        retry_deadline = time.monotonic() + retry_timeout
        outcomes = [self._create_once(timeout=min(90.0, max(retry_timeout, 1.0)))]
        while is_slot_shortage(outcomes[-1]) and time.monotonic() < retry_deadline:
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
            probe_timeout = self._remaining_probe_timeout(retry_deadline)
            if probe_timeout is None:
                break
            outcomes.append(self._create_once(timeout=probe_timeout))
        outcome = outcomes[-1]
        if any(create_uncertain(item) for item in outcomes):
            self._reap_uncertain_probes_best_effort(
                deadline=time.monotonic() + _BEST_EFFORT_REAP_SECONDS
            )
        self._fixture.record("recovery_statuses", status_histogram(outcomes))
        self._fixture.verify(
            200 <= outcome.status < 300,
            "a cold create was still refused after a network slot came free, so the refusal above "
            "wasn't the slot check reacting to slot occupancy",
            status=outcome.status,
            response_message=outcome.message[:200],
        )
        self._fixture.verify(
            outcome.status == 201 and bool(outcome.sandbox_id),
            "the recovery create succeeded without returning a 201 and a sandboxID",
            status=outcome.status,
            sandbox_id=outcome.sandbox_id,
            response_message=outcome.message[:200],
        )

    # --- probe cleanup ----------------------------------------------------

    def _retry_delete(
        self,
        delete: Callable[[], None],
        *,
        deadline: float | None = None,
    ) -> None:
        for attempt in range(1, _PROBE_CLEANUP_ATTEMPTS + 1):
            if deadline is not None and time.monotonic() >= deadline:
                raise EcwSandboxDeleteRetryable("probe delete deadline expired before attempt")
            try:
                delete()
                return
            except EcwSandboxDeleteRetryable:
                if attempt == _PROBE_CLEANUP_ATTEMPTS:
                    raise
                wait_seconds = _PROBE_CLEANUP_RETRY_SECONDS
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise
                    wait_seconds = min(wait_seconds, remaining)
                self._fixture.wait(wait_seconds)

    def _reap_uncertain_probes_best_effort(self, *, deadline: float) -> None:
        remaining: list[dict[str, str]] = []
        for idx, probe_metadata in enumerate(self._uncertain_probes):
            if time.monotonic() >= deadline:
                remaining.extend(self._uncertain_probes[idx:])
                break
            try:
                self._retry_delete(
                    partial(self._probe.kill_matching, probe_metadata, deadline=deadline),
                    deadline=deadline,
                )
            except (EcwSandboxReconcileUnmatched, EcwSandboxDeleteRetryable):
                remaining.append(probe_metadata)
        self._uncertain_probes = remaining

    def sweep_admitted(self) -> None:
        """Kill every sandbox a probe admitted, and reconcile commit-ambiguous creates."""

        first_error: Exception | None = None
        still_admitted: list[str] = []
        for sandbox_id in dict.fromkeys(self._admitted):
            try:
                self._retry_delete(partial(self._probe.kill_sandbox, sandbox_id))
            except Exception as error:  # noqa: BLE001 - finish remaining deletes first
                still_admitted.append(sandbox_id)
                first_error = first_error or error
        self._admitted = still_admitted
        self._filled_ids = [sid for sid in self._filled_ids if sid in set(still_admitted)]

        unmatched_errors: list[str] = []
        still_pending: list[dict[str, str]] = []
        for probe_metadata in self._uncertain_probes:
            try:
                self._retry_delete(partial(self._probe.kill_matching, probe_metadata))
            except EcwSandboxReconcileUnmatched as error:
                unmatched_errors.append(str(error))
            except Exception as error:  # noqa: BLE001 - preserve earlier delete failure if any
                still_pending.append(probe_metadata)
                first_error = first_error or error
        self._uncertain_probes = still_pending
        if unmatched_errors:
            self._fixture.record(
                "unanswered_creates_unmatched",
                {"count": len(unmatched_errors), "error": unmatched_errors[0]},
            )
        if first_error is not None:
            raise first_error

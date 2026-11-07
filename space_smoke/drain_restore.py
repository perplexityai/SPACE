"""The drain restore operation lands a suspended sandbox in its target lifecycle.

A forced node drain's v2 brings each sandbox it suspended back through a
durable `drain_restore_sandbox` operation: the cluster-controller's
public-operation dispatcher claims the `suspended` row (`restoring`, with
`restore_episode_target` set to the target), restores it from the suspend
snapshot the payload names, and lands it `started` or `paused`. A user's
lifecycle call supersedes the operation while it waits (`canceled`); one that
arrives while the claim holds the row meets that restore instead.

Nothing in production enqueues the kind yet, so the scenario enqueues it with
a named insert that mirrors the API's own (`enqueue_drain_restore` in
`crates/l40_api/api/src/drain_restore_operations.rs`). Every verdict is read
from state: the operation row, the sandbox row's lifecycle and episode target,
its active `sandbox_k8s_executions`, and the ECW responses. On a build that
does not know the kind, the operation stays `queued` and the sandbox
`suspended`, and every wait timeout carries both, which is the control
evidence.

The claimed window is opened with the `SPACE_DRAIN_RESTORE_TEST_HOLD_SECS`
test hook, which holds each claimed drain restore that long before its node
work. The controller reads it per claim, but from its process environment,
so the scenario overrides the controller Deployment's env for that key alone
(closed key list, bounded integer seconds) under the controller-mutation
Lease, awaits the rollout, checks the literal on every running controller
pod, and puts the captured standing literal back in the scenario body.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, cast

from space_smoke.capabilities import (
    DrainRestoreControl,
    DrainRestoreObservation,
    EcwLifecycleProbe,
    RestoreCallerOutcome,
    RestoreRacer,
)
from space_smoke.runtime import Compensation, ResourceScope, background

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

DRAIN_RESTORE_HOLD_ENV = "SPACE_DRAIN_RESTORE_TEST_HOLD_SECS"

# The only controller env this scenario may touch. The hook parses a u64 of
# seconds; the bound keeps an override from wedging restores for longer than
# a scenario phase.
DRAIN_RESTORE_HOLD_ENV_KEYS: tuple[str, ...] = (DRAIN_RESTORE_HOLD_ENV,)
MAX_HOLD_SECONDS = 120

DRAIN_RESTORE_TARGETS = frozenset({"started", "paused"})
TERMINAL_OPERATION_STATES = frozenset({"succeeded", "failed", "canceled", "expired"})


def validate_drain_restore_hold_keys(keys: Iterable[str]) -> None:
    unknown = sorted(set(keys) - set(DRAIN_RESTORE_HOLD_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing non-drain-restore-hold controller env keys: {unknown}")


def validate_drain_restore_hold_overrides(overrides: Mapping[str, str]) -> None:
    """Refuse any key but the hold, and any value but whole seconds in `1..MAX_HOLD_SECONDS`."""

    validate_drain_restore_hold_keys(overrides)
    for key, value in overrides.items():
        if not value.isascii() or not value.isdigit() or not 1 <= int(value) <= MAX_HOLD_SECONDS:
            raise ValueError(f"refusing drain-restore hold value {value!r} for {key}")


class DrainRestoreAssertions:
    """Enqueue drain restores of the fixture sandbox and judge where they land."""

    def __init__(self, fixture: KindEcw, control: DrainRestoreControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._release_lease: Compensation | None = None
        self._restore_env: Compensation | None = None
        self._standing: dict[str, str | None] | None = None
        self._hold_seconds = 0
        # Every enqueued operation, in order, with the outcome it was judged to have.
        self._operations: list[dict[str, str]] = []
        # Every raw ECW answer, by action, in order.
        self._ecw_outcomes: dict[str, list[dict[str, object]]] = {}

    @property
    def _sandbox_id(self) -> str:
        return self._fixture.sandbox.sandbox_id

    def _verify(
        self, condition: bool, message: str, *, observed: DrainRestoreObservation, **evidence: object
    ) -> None:
        """`verify` with the operation and sandbox read attached, which every verdict here carries."""

        self._fixture.verify(condition, message, failure_evidence=None, **observed.evidence(), **evidence)

    # --- the scoped hold -------------------------------------------------------

    @contextmanager
    def scoped_hold(self) -> Iterator[None]:
        """Put the standing hold env back as the body unwinds, success or failure alike.

        `hold_claimed_restores` runs inside the body, in its own phase. The
        scope compensation stays armed as the failure backstop, but the
        cleanup window is too short to own a controller rollout, so the
        restore runs here. When the body failed, its error propagates: a
        restore failure is attached to it as a note and recorded, and the
        backstop stays armed.
        """

        try:
            yield
        except BaseException as body_error:
            try:
                self.restore_standing()
            except Exception as restore_error:  # noqa: BLE001 - attached to the body's error below
                self._fixture.record("drain_restore_hold_env_restore_failure", repr(restore_error)[:1_000])
                body_error.add_note(
                    "restoring the standing drain-restore hold env also failed: "
                    f"{restore_error!r}; the scope's restore compensation stays armed"
                )
            raise
        with self._fixture.phase("put the standing drain-restore hold back"):
            self.restore_standing()

    def hold_claimed_restores(self, seconds: int) -> None:
        """Set the hold, and see it on the Deployment and every running controller pod."""

        overrides = {DRAIN_RESTORE_HOLD_ENV: str(seconds)}
        validate_drain_restore_hold_overrides(overrides)
        release = self._control.acquire_controller_mutation()
        self._release_lease = self._scope.defer(
            "release the controller mutation lease", release, critical=True
        )
        standing = self._control.drain_restore_hold_env()
        self._standing = standing
        self._fixture.record("drain_restore_hold_env_before", dict(standing))
        # Armed before the override: a set that half-applies is still put back.
        self._restore_env = self._scope.defer(
            "restore the standing drain-restore hold env",
            lambda: self._control.restore_drain_restore_hold_env(standing),
            critical=True,
        )
        self._control.set_drain_restore_hold_env(overrides)
        live = self._control.drain_restore_hold_env()
        pods = self._control.running_controller_drain_restore_hold_envs()
        self._fixture.verify(
            live == overrides and bool(pods) and all(pod == overrides for pod in pods),
            "the rolled cluster-controller does not carry the drain-restore hold",
            expected=overrides,
            deployment=live,
            running_pods=pods,
        )
        self._hold_seconds = seconds
        self._fixture.record("drain_restore_hold_env_live", dict(live))

    def restore_standing(self) -> None:
        """Put the captured env back now and check it took; then release the Lease.

        The restore compensation is disarmed only once the restore is
        verified, and the Lease stays held while it is armed (its release was
        registered earlier, so it runs after the retry).
        """

        if self._restore_env is not None:
            standing = self._standing
            if standing is None:
                raise RuntimeError("a restore compensation without a captured standing env")
            self._control.restore_drain_restore_hold_env(standing)
            after = self._control.drain_restore_hold_env()
            self._fixture.verify(
                after == standing,
                "the controller's drain-restore hold env did not come back to its standing value",
                standing=standing,
                after=after,
            )
            self._restore_env.disarm()
            self._restore_env = None
            self._hold_seconds = 0
        if self._release_lease is not None:
            release, self._release_lease = self._release_lease, None
            release.release()

    # --- enqueue and observe ---------------------------------------------------

    def source_snapshot(self) -> str:
        """S: the suspend snapshot a drain would name, once it is durable.

        Call after `cross_node_restore.wait_until_restore_portable()`, which
        waits for the same row to have a storage location.
        """

        snapshot_id = self._control.drain_restore_source_snapshot(self._sandbox_id)
        self._fixture.verify(
            snapshot_id is not None,
            "the suspended sandbox has no ready full snapshot with a live storage location",
            sandbox_id=self._sandbox_id,
        )
        assert snapshot_id is not None
        return snapshot_id

    def enqueue(self, label: str, *, target: str, snapshot_id: str, due_in_seconds: int = 0) -> str:
        """Queue a drain restore toward `target`; `due_in_seconds` > 0 keeps it unclaimable that long."""

        if target not in DRAIN_RESTORE_TARGETS:
            raise ValueError(f"not a drain restore target: {target!r}")
        operation_id = self._control.enqueue_drain_restore(
            self._sandbox_id, target=target, snapshot_id=snapshot_id, due_in_seconds=due_in_seconds
        )
        self._operations.append(
            {
                "label": label,
                "operation_id": operation_id,
                "target": target,
                "snapshot_id": snapshot_id,
                "due_in_seconds": str(due_in_seconds),
                "outcome": "queued",
            }
        )
        self._record_operations()
        return operation_id

    def observe(self, operation_id: str) -> DrainRestoreObservation:
        return self._control.drain_restore_observation(operation_id, self._sandbox_id)

    def _record_outcome(self, operation_id: str, observed: DrainRestoreObservation) -> None:
        for operation in self._operations:
            if operation["operation_id"] == operation_id:
                operation["outcome"] = observed.operation_state or "missing"
        self._record_operations()

    def _record_operations(self) -> None:
        self._fixture.record("drain_restore_operations", [dict(op) for op in self._operations])

    def _wait_for(
        self,
        operation_id: str,
        done: Callable[[DrainRestoreObservation], bool],
        *,
        timeout: float,
        waiting_for: str,
    ) -> DrainRestoreObservation:
        """Poll the operation and sandbox together until `done`; fail with both on the deadline."""

        deadline = time.monotonic() + timeout
        while True:
            observed = self.observe(operation_id)
            if done(observed):
                return observed
            self._verify(
                time.monotonic() <= deadline,
                f"the drain restore did not reach {waiting_for} within {timeout:g} s",
                operation_id=operation_id,
                observed=observed,
            )
            self._fixture.wait(1)

    def wait_until_terminal(self, operation_id: str, *, timeout: float = 240) -> DrainRestoreObservation:
        """Block until the operation is terminal; the budget covers a pickup and a kind restore."""

        observed = self._wait_for(
            operation_id,
            lambda o: o.operation_state in TERMINAL_OPERATION_STATES,
            timeout=timeout,
            waiting_for="a terminal state",
        )
        self._record_outcome(operation_id, observed)
        return observed

    def wait_until_claimed(
        self, operation_id: str, *, target: str, timeout: float = 60
    ) -> DrainRestoreObservation:
        """Block until the claim holds the row: `restoring`, episode target set, the operation `running`."""

        return self._wait_for(
            operation_id,
            lambda o: o.operation_state == "running"
            and o.lifecycle == "restoring"
            and o.restore_episode_target == target,
            timeout=timeout,
            waiting_for=f"a claimed `restoring` row with episode target {target}",
        )

    # --- verdicts ------------------------------------------------------------

    def expect_landed(self, operation_id: str, *, target: str) -> DrainRestoreObservation:
        """`succeeded`, the row in `target` with its episode ended, and one active execution."""

        observed = self.wait_until_terminal(operation_id)
        self._verify(
            observed.operation_state == "succeeded"
            and observed.lifecycle == target
            and observed.restore_episode_target is None
            and observed.active_executions == 1,
            f"the drain restore did not land the sandbox {target} on one execution",
            operation_id=operation_id,
            observed=observed,
        )
        return observed

    def expect_canceled(
        self, operation_id: str, *, lifecycle: str, active_executions: int | None = None
    ) -> DrainRestoreObservation:
        """`canceled`, with the row in `lifecycle`, no episode left open and, if given, that many executions."""

        observed = self.wait_until_terminal(operation_id, timeout=60)
        self._verify(
            observed.operation_state == "canceled"
            and observed.lifecycle == lifecycle
            and observed.restore_episode_target is None
            and active_executions in (None, observed.active_executions),
            f"the superseded drain restore was not canceled with the sandbox left {lifecycle}",
            expected_active_executions=active_executions,
            operation_id=operation_id,
            observed=observed,
        )
        return observed

    def expect_unchanged(self, operation_id: str, *, state: str, lifecycle: str) -> DrainRestoreObservation:
        """A finished operation keeps its outcome after a later user call on the sandbox."""

        observed = self.observe(operation_id)
        self._verify(
            observed.operation_state == state
            and observed.lifecycle == lifecycle
            and observed.restore_episode_target is None,
            f"after a later user call the drain restore is not still {state} with the sandbox {lifecycle}",
            operation_id=operation_id,
            observed=observed,
        )
        return observed

    def ecw_request(self, action: str, *, timeout: float = 60) -> RestoreCallerOutcome:
        """One raw ECW `pause` or `resume`; the answer is recorded and returned, never raised."""

        outcome = cast(EcwLifecycleProbe, self._fixture.sandbox).lifecycle_request(action, timeout=timeout)
        answers = self._ecw_outcomes.setdefault(action, [])
        answers.append(
            {
                "status": outcome.status,
                "elapsed_seconds": round(outcome.elapsed_seconds, 2),
                "detail": outcome.detail,
            }
        )
        self._fixture.record(f"ecw_{action}_outcomes", list(answers))
        return outcome

    def expect_refused_during_claim(
        self, operation_id: str, action: str, *, status: int
    ) -> None:
        """An ECW `action` while the claim holds the row answers `status`, and the claim keeps the row."""

        outcome = self.ecw_request(action)
        observed = self.observe(operation_id)
        self._verify(
            outcome.status == status
            and observed.operation_state == "running"
            and observed.lifecycle == "restoring",
            f"an ECW {action} during the claimed drain restore did not answer {status} "
            "with the restore still holding the row",
            operation_id=operation_id,
            ecw_status=outcome.status,
            ecw_detail=outcome.detail,
            observed=observed,
        )

    def expect_connect_joins_claim(self, operation_id: str, *, timeout: float = 180) -> None:
        """A connect issued while the claim holds the row returns 200 once the restore lands.

        The connect runs on its own thread; the row is read right after it
        leaves, so "issued during the hold" is a sampled observation. Once it
        returns, the row must have left `restoring`.
        """

        outcomes: list[RestoreCallerOutcome] = []
        racer = cast(RestoreRacer, self._fixture.sandbox)
        connect = background(
            "the connect during the claim",
            lambda: outcomes.append(racer.race_connect(timeout=timeout)),
        )
        during = self.observe(operation_id)
        self._verify(
            during.operation_state == "running" and during.lifecycle == "restoring",
            "the claim no longer held the row when the connect was issued; the hold is too short",
            operation_id=operation_id,
            hold_seconds=self._hold_seconds,
            observed=during,
        )
        connect.join(timeout=timeout + 30)
        (outcome,) = outcomes
        after = self.observe(operation_id)
        self._fixture.record("connect_during_claim_elapsed_seconds", round(outcome.elapsed_seconds, 2))
        self._verify(
            outcome.status == 200 and after.lifecycle == "started",
            "a connect issued during the claimed drain restore did not return once it landed",
            operation_id=operation_id,
            connect_status=outcome.status,
            connect_detail=outcome.detail,
            observed=after,
        )

    def expect_deleted_behind_claim(self, operation_id: str, *, timeout: float = 300) -> DrainRestoreObservation:
        """The operation ends `succeeded` or `canceled`, the row is gone, and no execution is active.

        The delete queues behind the running restore, so the budget covers
        the rest of the hold, the restore, and the teardown.
        """

        observed = self._wait_for(
            operation_id,
            lambda o: o.operation_state in TERMINAL_OPERATION_STATES
            and o.lifecycle is None
            and o.active_executions == 0,
            timeout=timeout,
            waiting_for="a terminal state with its sandbox deleted",
        )
        self._record_outcome(operation_id, observed)
        self._verify(
            observed.operation_state in {"succeeded", "canceled"},
            "the drain restore a delete queued behind did not end succeeded or canceled",
            operation_id=operation_id,
            observed=observed,
        )
        return observed

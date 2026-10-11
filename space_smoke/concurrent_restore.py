"""Restore convergence under concurrent callers, proven on a real cluster.

The compose hammer fired gated `connect` callers at three API processes and
read a stub apiserver's counters. This module is its kind form: the callers
are threads over the public route, the API is scaled to three replicas for
the scenario, the counters are the API's own `sandbox_k8s_executions` rows and
the sandbox's pods by label, and the windows are a namespace pod-count quota.
Every fault is a named kind-only operation with its compensation armed before
dispatch; the control surface is [`ConcurrentRestoreControl`].
"""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from typing import TYPE_CHECKING, cast

from space_smoke.capabilities import (
    ConcurrentRestoreControl,
    ExecutionRow,
    RestoreCallerOutcome,
    RestoreRacer,
    SandboxPodView,
)
from space_smoke.runtime import (
    BackgroundWork,
    Compensation,
    ResourceScope,
    ScenarioTimedOut,
    SmokeConfigurationError,
)

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw


class RunningPodWatch:
    """Poll a pod listing from a background thread; keep every pod seen Running.

    Owned by [`ConcurrentRestoreAssertions.watch_for_running_pods`]. The
    polling thread uses plain sleeps rather than the fixture's `wait`, which
    is not built for use off the scenario thread. The thread is a
    [`BackgroundWork`], so a listing failure is re-raised by `stop_after` and
    a broken poll cannot read as "no pod ran".
    """

    def __init__(self, pods: Callable[[], list[SandboxPodView]], *, interval: float = 2.0) -> None:
        self._pods = pods
        self._interval = interval
        self._stop = Event()
        self._seen: dict[str, SandboxPodView] = {}
        self._work = BackgroundWork("running-pod watch", self._run)

    def start(self) -> None:
        self._work.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            for pod in self._pods():
                if pod.phase == "Running":
                    self._seen[pod.pod_name] = pod
            self._stop.wait(self._interval)

    def stop_after(self, seconds: float) -> list[SandboxPodView]:
        """Keep watching `seconds` longer, then stop and return the Running pods seen."""

        self._stop.wait(seconds)
        self._stop.set()
        # One in-flight listing at most stands between the stop and the exit.
        self._work.join(timeout=60)
        return list(self._seen.values())


class ConcurrentRestoreAssertions:
    """Prove restore convergence under concurrent callers on a real cluster.

    The compose hammer fired gated `connect` callers at three API processes
    and read a stub apiserver's counters. Here the callers are threads over
    the public route, the API is scaled to three replicas for the scenario,
    the counters are the API's own `sandbox_k8s_executions` rows and the pods
    by label, the forced first-POST failure is a namespace pod-count quota
    (the POST is refused at admission), and the mid-restore window is the
    real restore's own: from the `creating` claim row until the pod runs.
    Every fault is a named kind-only operation with its compensation armed
    before dispatch.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: ConcurrentRestoreControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._scale_back: Compensation | None = None
        self._lift_cap: Compensation | None = None
        self._callers = 0

    # --- environment shape ------------------------------------------------

    def scale_api_for_contention(self, replicas: int = 3, *, rollout_timeout: float = 300) -> None:
        """Run the public API at `replicas` for the scenario; scaled back at scope end."""

        previous_holder: list[int] = []

        def scale_back() -> None:
            # Scale back and wait until the route is usable again: compensations
            # registered earlier (the sandbox kill, the API key delete) run
            # after this one and must not hit a replica that is being
            # terminated. `rollout status` returns on a scale-down while the
            # surplus pods are still Terminating, and the local port-forward
            # may be pinned to one of them; wait for the drain, then for
            # `/readyz` through the same URL the compensations use.
            self._control.scale_public_api(previous_holder[0] if previous_holder else 1)
            self._control.wait_public_api_rollout(timeout=rollout_timeout)
            self._control.wait_public_api_pods_drained(timeout=rollout_timeout)
            self._control.wait_public_api_ready(timeout=rollout_timeout)

        self._scale_back = self._scope.defer("scale the public API back", scale_back, critical=True)
        previous_holder.append(self._control.scale_public_api(replicas))
        self._control.wait_public_api_rollout(timeout=rollout_timeout)
        self._fixture.record("api_replicas_before", previous_holder[0])
        self._fixture.record("api_replicas_during", replicas)

    def restore_api_scale(self) -> None:
        if self._scale_back is not None:
            release = self._scale_back
            self._scale_back = None
            release.release()

    # --- windows ------------------------------------------------------------

    def cap_namespace_pods(self) -> int:
        """Refuse the next pod POST at admission; lifted eagerly or at scope end."""

        # A Terminating pod is still charged by the quota controller; a cap
        # pinned while one is leaving is off by one once it has gone and a
        # single POST slips through. The drain is a precondition, not a
        # best-effort wait: a pod still present at its deadline would leave
        # the create path open and the phase proving nothing, so fail here.
        remaining = self.wait_for_pods_gone()
        self._fixture.verify(
            not remaining,
            "the sandbox's pods did not drain before the pod-count cap was pinned",
            pods=[(p.pod_name, p.phase, p.deleting) for p in remaining],
        )
        self._lift_cap = self._scope.defer(
            "lift the namespace pod-count quota",
            self._control.lift_namespace_pod_cap,
            critical=True,
        )
        cap = self._control.pin_namespace_pod_count()
        self._fixture.record("pod_cap", cap)
        return cap

    def lift_pod_cap(self) -> None:
        if self._lift_cap is not None:
            release = self._lift_cap
            self._lift_cap = None
            release.release()

    # --- the race ------------------------------------------------------------

    def race_connects(self, count: int, *, timeout: float = 240) -> list[RestoreCallerOutcome]:
        """Fire `count` concurrent `connect` calls on one gate; return every outcome."""

        self._callers = count
        racer = cast(RestoreRacer, self._fixture.sandbox)
        barrier = Barrier(count)
        with ThreadPoolExecutor(max_workers=count) as pool:
            futures = [
                pool.submit(racer.race_connect, timeout=timeout, gate=barrier.wait)
                for _ in range(count)
            ]
            outcomes = [future.result() for future in futures]
        histogram: dict[str, int] = {}
        for outcome in outcomes:
            histogram[str(outcome.status)] = histogram.get(str(outcome.status), 0) + 1
        self._fixture.record("connect_status_histogram", histogram)
        return outcomes

    def wait_for_executions(
        self, *, at_least: int, timeout: float = 120
    ) -> list[ExecutionRow]:
        deadline = time.monotonic() + timeout
        while True:
            rows = self._control.sandbox_executions(self._fixture.sandbox.sandbox_id)
            if len(rows) >= at_least or time.monotonic() > deadline:
                return rows
            self._fixture.wait(1)

    def wait_for_abandoned_execution(self, *, after: int, timeout: float = 120) -> list[ExecutionRow]:
        """Block until a row created after the first `after` rows has been retired.

        A pod POST the apiserver refuses makes the API retire its `creating`
        claim (`abandon_created_unmaterialized_create_claim`): the row stays
        but goes inactive. That retired row is the observable that the
        refusal happened, and the moment the cap may be lifted.
        """

        deadline = time.monotonic() + timeout
        while True:
            rows = self._control.sandbox_executions(self._fixture.sandbox.sandbox_id)
            if any(not r.active for r in rows[after:]) or time.monotonic() > deadline:
                return rows
            self._fixture.wait(1)

    def executions(self) -> list[ExecutionRow]:
        return self._control.sandbox_executions(self._fixture.sandbox.sandbox_id)

    def pods(self) -> list[SandboxPodView]:
        return self._control.sandbox_pods(self._fixture.sandbox.sandbox_id)

    def wait_for_pods_gone(self, *, timeout: float = 180) -> list[SandboxPodView]:
        deadline = time.monotonic() + timeout
        while True:
            pods = self.pods()
            if not pods or time.monotonic() > deadline:
                return pods
            self._fixture.wait(2)

    def watch_for_running_pods(self) -> RunningPodWatch:
        """Start collecting, in the background, every sandbox pod seen Running.

        The teardown verdict's window opens when the cap is lifted, not when
        the callers' join returns: a pod admitted right after the lift could
        reach Running and be reaped before the join completes, so a poll that
        starts after the join has nothing to observe. Start this before the
        lift and close it with `stop_after` once the join is back.
        """

        watch = RunningPodWatch(self.pods)
        watch.start()
        return watch

    def delete_current_pod(self) -> SandboxPodView:
        """Delete the sandbox's running execution pod out from under the API."""

        pods = [p for p in self.pods() if not p.deleting]
        self._fixture.verify(
            len(pods) == 1,
            "expected exactly one live execution pod to delete",
            pods=[(p.pod_name, p.phase) for p in pods],
        )
        self._control.delete_sandbox_pod(pods[0].pod_name)
        self._fixture.record("deleted_pod", pods[0].pod_name)
        return pods[0]

    def wait_until_runtime_gone(self, *, timeout: float = 120) -> str:
        """Block until the deleted pod's VM is really unreachable.

        The API observes a lost pod lazily: a `connect` on a Running sandbox
        first delivers a keepalive touch to the node, and only when the node
        reports the runtime unreachable or the local sandbox no longer
        started does the handler drive a wake (`connect_touch_recovery`).
        Straight after `kubectl delete pod` the VM is still alive inside its
        grace period, so twenty connects would all be answered "already
        running" and no handoff would run — rev5 on the bench did exactly
        that. Wait for the pod to leave the apiserver and a data-plane probe
        to fail before releasing the callers. Returns which signal fired.
        """

        deadline = time.monotonic() + timeout
        pods_gone = False
        while True:
            if not pods_gone:
                pods_gone = not self.pods()
            probe_failed = False
            if pods_gone:
                try:
                    result = self._fixture.sandbox.run("true", timeout=5)
                    probe_failed = result.exit_code != 0
                except (ScenarioTimedOut, SmokeConfigurationError):
                    raise
                except Exception:  # noqa: BLE001 - unreachability is the signal
                    probe_failed = True
            if pods_gone and probe_failed:
                self._fixture.record("runtime_gone_signal", "pods_gone+probe_failed")
                return "pods_gone+probe_failed"
            self._fixture.verify(
                time.monotonic() <= deadline,
                "the deleted execution pod's runtime stayed reachable",
                pods=[(p.pod_name, p.phase, p.deleting) for p in self.pods()],
                executions=[(r.generation, r.state, r.active) for r in self.executions()],
                pods_gone=pods_gone,
            )
            self._fixture.wait(2)

    # --- verdicts ------------------------------------------------------------

    def expect_converged(
        self,
        outcomes: list[RestoreCallerOutcome],
        *,
        executions_total: int,
        allowed_failures: dict[int, int] | None = None,
        label: str,
    ) -> ExecutionRow:
        """Exactly one active execution and one pod; callers converged as allowed.

        `allowed_failures` maps a status to the exact number of callers that
        may have ended with it (the hammer's `failed_post` allows exactly one
        5xx); every other caller must have ended 200.
        """

        self._require_all_callers(outcomes, label)
        allowed = allowed_failures or {}
        rows = self.executions()
        active = [r for r in rows if r.active]
        pods = [p for p in self.pods() if not p.deleting]
        statuses: dict[int, int] = {}
        for outcome in outcomes:
            statuses[outcome.status] = statuses.get(outcome.status, 0) + 1
        expected_ok = len(outcomes) - sum(allowed.values())
        ok = (
            len(active) == 1
            and len(rows) == executions_total
            and len(pods) == 1
            and statuses.get(200, 0) == expected_ok
            and all(statuses.get(code, 0) == n for code, n in allowed.items())
            and set(statuses) <= {200, *allowed}
        )
        self._fixture.verify(
            ok,
            f"{label}: concurrent connects did not converge to one execution and one pod",
            executions=[(r.generation, r.state, r.active) for r in rows],
            pods=[(p.pod_name, p.phase, p.execution_generation) for p in pods],
            statuses={str(k): v for k, v in statuses.items()},
            expected_executions=executions_total,
            allowed_failures={str(k): v for k, v in allowed.items()},
            slowest_caller_seconds=round(max(o.elapsed_seconds for o in outcomes), 1),
            sample_errors=sorted({o.detail for o in outcomes if o.status != 200})[:3],
        )
        self._fixture.record(f"{label}_active_generation", active[0].generation)
        return active[0]

    def expect_converged_through_refusals(
        self, outcomes: list[RestoreCallerOutcome], *, before: int, label: str
    ) -> ExecutionRow:
        """Refused pod POSTs retire their generations; the survivors converge on one.

        The stub failed exactly the first POST, so the hammer required two
        rows and one 500. A real admission refusal rejects every POST until
        the cap is lifted, so the invariant is the dodge contract itself
        (`transient_create_dodge_handoff_yields_fresh_claim`): every refused
        generation is finished as `replaced`, exactly one generation is left
        active with one pod, every caller whose generation was refused saw
        the failure and every other caller landed — the refused count and the
        500 count agree, and at least one refusal happened.
        """

        self._require_all_callers(outcomes, label)
        rows = self.executions()[before:]
        active = [r for r in rows if r.active]
        replaced = [r for r in rows if r.state == "replaced" and not r.active]
        pods = [p for p in self.pods() if not p.deleting]
        statuses: dict[int, int] = {}
        for outcome in outcomes:
            statuses[outcome.status] = statuses.get(outcome.status, 0) + 1
        failures = statuses.get(500, 0)
        ok = (
            len(active) == 1
            and len(rows) == len(replaced) + 1
            and len(replaced) >= 1
            and len(replaced) == failures
            and len(pods) == 1
            and pods[0].execution_generation == str(active[0].generation)
            and statuses.get(200, 0) == len(outcomes) - failures
            and set(statuses) <= {200, 500}
        )
        self._fixture.verify(
            ok,
            f"{label}: refused pod POSTs did not retire their generations and converge on one",
            executions=[(r.generation, r.state, r.active) for r in rows],
            pods=[(p.pod_name, p.phase, p.execution_generation) for p in pods],
            statuses={str(k): v for k, v in statuses.items()},
            replaced_generations=len(replaced),
            sample_errors=sorted({o.detail for o in outcomes if o.status != 200})[:3],
        )
        self._fixture.record(f"{label}_refused_generations", len(replaced))
        self._fixture.record(f"{label}_active_generation", active[0].generation)
        return active[0]

    def wait_for_restore_in_flight(self, *, before: int, timeout: float = 60) -> ExecutionRow:
        """Block until the callers have started claiming — the restore is in flight.

        With the pod-count cap standing every claim's POST is refused and the
        claim is retired as `replaced` within milliseconds, then the caller
        re-claims; a poll for an *active* `creating` row never catches one
        (rev7 on the bench saw dozens of `replaced` rows and no active one).
        Any new row after `before` is the signal: the callers are inside the
        restore path. It doesn't mean they'll still be claiming when the cap
        lifts: each caller owns at most one refused claim before it returns
        500, so the re-claiming stops once every caller has had one.
        """

        deadline = time.monotonic() + timeout
        while True:
            rows = self._control.sandbox_executions(self._fixture.sandbox.sandbox_id)[before:]
            if rows:
                self._fixture.record("teardown_window_rows", len(rows))
                self._fixture.record("teardown_window_state", rows[-1].state)
                return rows[-1]
            self._fixture.verify(
                time.monotonic() <= deadline,
                "no restore claim appeared while the callers were firing",
                executions=[(r.generation, r.state, r.active) for r in rows],
            )
            self._fixture.wait(0.2)

    def _require_all_callers(self, outcomes: list[RestoreCallerOutcome], label: str) -> None:
        """Every verdict first requires every caller to have reported — a firing thread
        that died would otherwise leave an empty histogram that satisfies any subset check."""

        self._fixture.verify(
            len(outcomes) == self._callers,
            f"{label}: not every concurrent caller reported an outcome",
            outcomes_reported=len(outcomes),
            callers=self._callers,
        )

    def expect_torn_down(
        self,
        outcomes: list[RestoreCallerOutcome],
        *,
        label: str,
        running_after_lift: list[SandboxPodView],
        refused_before_delete: int,
        delete_returned_at: float,
    ) -> None:
        """A delete while POSTs are refused leaves nothing: no pod ever reached
        Running after the window closed, the sandbox is gone from the public API,
        no execution row is active (the cluster rows cascade on delete, so an
        empty list is the expected terminal state), and every caller reported,
        none hung or lost its connection (0), none saw a 200 (with the cap
        standing a 200 would mean a POST was admitted) or a 429 (placement
        admission refusing is a real failure). Allowed: {404, 409, 503}, and the
        refused-claim 500 — every one of which must have finished BEFORE the
        delete returned (after the fence a connect can only 404/409), and there
        must be at least as many as the generations the cap had retired when
        the snapshot before the delete was taken (more can land between that
        snapshot and the fence, and a refused POST on a re-acquired `creating`
        claim yields a 500 with no `replaced` row)."""

        self._require_all_callers(outcomes, label)
        pods = self.wait_for_pods_gone()
        info = self._fixture.sandbox.try_get_info()
        rows = self.executions()
        statuses: dict[int, int] = {}
        for outcome in outcomes:
            statuses[outcome.status] = statuses.get(outcome.status, 0) + 1
        late_500s = [o for o in outcomes if o.status == 500 and o.finished_at > delete_returned_at]
        self._fixture.verify(
            not running_after_lift
            and not pods
            and info is None
            and not [r for r in rows if r.active]
            and set(statuses) <= {404, 409, 500, 503}
            and not late_500s
            and statuses.get(500, 0) >= refused_before_delete,
            f"{label}: teardown while pod POSTs were refused did not fence cleanly",
            pods_reached_running_after_lift=[(p.pod_name, p.execution_generation) for p in running_after_lift],
            executions=[(r.generation, r.state, r.active) for r in rows],
            pods=[(p.pod_name, p.phase) for p in pods],
            sandbox_visible=info is not None,
            statuses={str(k): v for k, v in statuses.items()},
            refused_before_delete=refused_before_delete,
            late_500s=len(late_500s),
            sample_errors=sorted({o.detail for o in outcomes if o.status not in (404, 409, 500, 503)})[:3],
        )
        self._fixture.record(f"{label}_caller_statuses", {str(k): v for k, v in statuses.items()})
        self._fixture.record(f"{label}_refused_before_delete", refused_before_delete)

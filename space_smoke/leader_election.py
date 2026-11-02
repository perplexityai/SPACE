"""Cluster-controller leader election against a live control plane.

The crate's leader-election tests run the state machine against a fake lease
store. What they cannot show needs a real apiserver: that a 403 arrives as the
category the policy spares, that a holder cut off from the apiserver stops
before its Lease can be taken over, and that a replica which spends its
liveness budget is restarted by kubelet rather than returning a value nobody
acts on. This module is the typed form of the opt-in shell suite that proved
them by reading cluster-controller log lines off the nodes; every verdict here
comes from state instead.

- **A budget sized for one run.** The liveness budget ships at 600 s and the
  lease TTL at 60 s. This run asks for the TTL's floor and the budget's floor
  at that TTL through the controller's test hooks, which every replica and the
  contender read at startup, records the Deployment's standing budget first
  and puts both back in its last phase. The outage runs first: its
  budget exit has to land within the requested budget's ceiling, so a binary
  that ignored the hook fails there, before the refusal is judged against it.
- **One leader, one runner.** Two healthy replicas, the Lease naming one of
  them with a fresh `renewTime`, and exactly one pod whose
  `space.cluster_reconciler.loop_latency_ms` tick count moves. Every
  reconciler tick records that histogram and only the holder runs reconcilers,
  so it is a per-pod signal no standby emits. Kind exports the Rust metrics
  cumulatively, so a fresh sample proves nothing; a moved count does. The
  count is compared per series against a baseline taken when the window opens,
  which is no earlier than the phase, the holder's current container or its
  acquisition: a holder that restarts keeps its name and can keep its
  `acquireTime`, so an older anchor would credit it with its predecessor's
  ticks. The window stays open for two of the slowest loop's 60 s ticks, so a
  second runner that starts late is still seen. That is judged once the
  apiserver is back, with the refused contender already running, and opens at
  the restore, so the outage's record checks run inside it. Before the outage
  the window lasts one tick of every loop plus two exports, enough to show the
  holder ticking and no other process ticking throughout, which the outage's
  check that the holder's ticks stop relies on.
- **A refused lease call is not a restart.** A third contender runs the
  controller's own pod template under a ServiceAccount with no `leases` grant.
  It is placed before the outage, held by a gate container, and released once
  the apiserver is back, so its controller's first lease call follows the
  restore. The watch ends a budget ceiling, a retry interval and the time a
  restart takes to show after its first refused call reaches Prometheus.
  Prometheus shows its `acquire_failed{reason="refused"}`; it stays NotReady
  with `restartCount` 0 for the whole liveness-budget ceiling counted from its
  container's start, and the healthy pair keeps its holder throughout.
- **An apiserver outage.** The control-plane apiserver's static-pod manifest is
  moved aside. A container that held the readiness marker before the outage
  loses it while still running. The holder's `lease_lost` reaches Prometheus
  stamped before the Lease's last `renewTime` plus its duration, the earliest
  instant a standby could take over; its tick count does not grow between two
  samples stamped after that instant and before the apiserver is restored
  (nothing can acquire the Lease in that window, so every tick in it is the
  expired holder's; a restarted container that keeps the pod name may win a
  fresh Lease afterwards); and its container ends within the bound
  the controller's own deadlines give its exit
  (`HOLDER_EXIT_AFTER_EXPIRY_SECONDS`).
  The last `renewTime` is the newest one seen by a read just before the park,
  a Lease watch, and back-to-back reads until the apiserver stopped
  answering; Kubernetes rounds it down to a whole second, so every bound from
  it allows one more. A
  controller container then exits after that instant, which no resignation can
  explain; once the apiserver is back its pod reports the restart with a
  non-zero exit, and Prometheus holds that replica's
  `lease_unreachable{reason="unreachable"}`. Finally the cluster converges to
  one fresh holder, one runner and two Ready replicas.

The readiness marker is the file the controller's readiness probe tests, read
through the node's CRI; container exits are read there too during the outage,
because nothing else answers then, and from the node's containerd events,
because kubelet can remove an exited container before any read lists it. Logs are never read for a verdict: the
controller's container logs are attached to a failure as evidence.

Not covered, as in the shell suite: the `overloaded` category (nothing in Kind
sheds load on demand) and a wedge that spares the substrate but not the lease
path (they share one client). The refused contender's NotReady is an outcome
only: it never wrote a marker, so it cannot show the marker being cleared;
the outage's marker check is what shows that.

Ctrl-C and SIGTERM still run the restore. If a run is killed outright with
the apiserver parked, put it back with
`docker exec space-local-control-plane mv
/etc/kubernetes/kube-apiserver.yaml.space-smoke-parked
/etc/kubernetes/manifests/kube-apiserver.yaml`.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from space_smoke.capabilities import (
    ControllerLease,
    ControllerPod,
    LeaderElectionControl,
    NodeContainer,
    NodeExitWatch,
)
from space_smoke.runtime import Compensation, ResourceScope, SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# The timings `LeaderElectionConfig::from_environment` resolves
# (crates/libs/cluster/controller/server/src/leader_election/mod.rs), which
# nothing but the Lease's `leaseDurationSeconds` exposes: the lease TTL, which
# this run requests at MIN_LEASE_TTL through LEASE_TTL_TEST_ENV (checked
# against the Lease), and the standby retry interval, DEFAULT_RETRY_INTERVAL
# clamped to the 2 s that TTL's renew deadline leaves room for. The liveness
# budget is the requested one raised to at least MIN_GRACE_LEASE_TTL_MULTIPLE
# TTLs and MIN_GRACE_RETRY_INTERVALS retries, plus a per-pod spread of up to
# 1/GRACE_SPREAD_DIVISOR of it. The crate pins each of these with a
# compile-time assert, and `the_lease_ttl_override_is_bounded_like_the_default`
# pins the values at the TTL floor; change them here with it. This run
# requests the floor, so every wait for a budget is bounded by the floor's
# ceiling: 72 s and 90 s, where the default TTL's are 240 s and 300 s.
LEASE_TTL_SECONDS = 18
RETRY_INTERVAL_SECONDS = 2.0
LIVENESS_BUDGET_SECONDS = max(4 * LEASE_TTL_SECONDS, 30 * int(RETRY_INTERVAL_SECONDS))
LIVENESS_BUDGET_SPREAD = 1 + 1 / 4
# Past the first refused call plus the budget's ceiling and one retry interval
# (the attempt that would cross it), how long until a contender whose
# refusals spent its budget shows the restart: its exit runs only the metric
# and trace providers' shutdowns (5 s each, crates/libs/observability); kubelet
# notices within a PLEG relist (1 s), restarts a first exit at once, and
# reports the restart count within its status batch (10 s); the watch reads
# every 5 s. That is 26 s; 30 s leaves room for a loaded node.
REFUSAL_RESTART_SEEN_SECONDS = 30.0
# The OTLP metric export interval (crates/libs/observability/src/sink/otlp.rs).
EXPORT_INTERVAL_SECONDS = 10.0
# The interval of the loops that record the reconciler tick histogram in Kind:
# 60 s for started_sandbox_repair, failed_restorable_repair,
# transient_lifecycle_sweeper and the two pod GCs. The warm pool's 5 s loop
# records it only with its orphan GC on, which Kind leaves off.
SLOWEST_TICK_SECONDS = 60.0
# A process that runs the reconcilers throughout this window records a tick of
# every loop above in its first 60 s, and exports it with an export interval
# to spare, even when it started in the same second as the holder and its
# first tick fell just before the window opened. Before the outage this
# shows the holder running and no other process running throughout.
ONE_TICK_WINDOW_SECONDS = SLOWEST_TICK_SECONDS + 2 * EXPORT_INTERVAL_SECONDS
# How far past the Lease's expiry the holder's process may still exit. It stops
# acting at its renew deadline, the duration less RENEW_DEADLINE_MARGIN (12 s),
# after its last confirmed renewal, whose call was issued before the
# `renewTime` it wrote; its reconcilers then drain within
# SHUTDOWN_DRAIN_TIMEOUT (10 s), so they stop 2 s before expiry. Only bounded
# steps follow: the release attempt (RELEASE_TIMEOUT, 5 s) and the metric and
# trace providers' shutdowns (5 s each, crates/libs/observability). So the exit
# is due by expiry + 13 s; a later exit is a holder that kept running.
HOLDER_EXIT_AFTER_EXPIRY_SECONDS = -12.0 + 10.0 + 5.0 + 5.0 + 5.0
# The apiserver stores the Lease's `renewTime` and `acquireTime`, and the pod
# status its container times, in whole seconds, rounded down: the instant
# each records can be up to this much later than it reads.
KUBE_TIME_RESOLUTION_SECONDS = 1.0

# Prometheus names of the OTLP metrics (`translation_strategy:
# UnderscoreEscapingWithSuffixes` in infra/space/observability/charts/metrics):
# `cluster_reconciler.loop_latency_ms` is the native histogram
# `space.cluster_reconciler.loop_latency` in milliseconds, and the
# leader-election counters gain `_total`.
RECONCILER_TICKS = "space_cluster_reconciler_loop_latency_milliseconds"
ACQUIRE_FAILED = "space_cluster_controller_leader_election_acquire_failed_total"
LEASE_LOST = "space_cluster_controller_leader_election_lease_lost_total"
LEASE_UNREACHABLE = "space_cluster_controller_leader_election_lease_unreachable_total"
# Evidence only: at the 18 s TTL a renewal stalled past about 4 s resigns the
# holder, and a resignation inside a judged window fails the run. The counter
# names that cause beside the window's own verdict.
RENEW_FAILED = "space_cluster_controller_leader_election_renew_failed_total"

POLL_SECONDS = 2.0
# Cleanup's time plan after a failure with the apiserver down. Its budget is
# shared, min(120 s, 10 % of the scenario timeout), so 120 s at the 30-minute
# default, and its compensations run in this order. The apiserver's restore:
# the node exec within APISERVER_RESTORE_SECONDS, then /readyz probes until
# APISERVER_ANSWER_SECONDS, the last bounded at 10 s, so 40 s at most. The
# budget's restore: two tries of an ownership read and the env write, 32 s.
# The contender's deletion, 8 s, and the mutation Lease's release, a read and
# a patch, 16 s (8 s per request, CLEANUP_REQUEST_SECONDS in adapters.py).
# That is 96 s, which leaves the fixture's own compensations 24 s.
APISERVER_RESTORE_SECONDS = 10.0
APISERVER_ANSWER_SECONDS = 20.0
LOG_TAIL_LINES = 40

T = TypeVar("T")


class LeaderElectionAssertions:
    """Drive the two faults and judge the controllers from state."""

    def __init__(
        self, fixture: KindEcw, control: LeaderElectionControl, scope: ResourceScope
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._control_plane = ""
        self._healthy: dict[str, ControllerPod] = {}
        self._contender: ControllerPod | None = None
        self._remove_contender: Compensation | None = None
        self._restore_apiserver: Compensation | None = None
        self._restore_budget: Compensation | None = None
        self._standing_budget: str | None = None
        # The ceiling of the budget this run requested, once it is live.
        self._budget_ceiling: float | None = None
        self._outage_started = 0.0
        # When the restore was requested: no Lease can be acquired before it.
        self._restore_requested = 0.0
        self._lease_before_outage: ControllerLease | None = None
        self._budget_exit: tuple[str, NodeContainer] | None = None
        # The initial check's Lease and the instant its runner window may open.
        self._initial: tuple[ControllerLease, float] | None = None
        # Pod name -> node, for reading container state and logs while the
        # apiserver is down.
        self._nodes: dict[str, str] = {}
        # (pod, container id) that held the readiness marker before the outage.
        self._markers: list[tuple[str, str]] = []
        # Pod name -> the container ids running when the outage began.
        self._running_before: dict[str, set[str]] = {}
        # Pod name -> every container id the node's runtime has listed for it.
        self._listed: dict[str, set[str]] = {}
        # Node -> its containerd's exit events, recorded through the outage.
        self._exit_watches: dict[str, NodeExitWatch] = {}

    # --- setup ----------------------------------------------------------------

    def take_ownership(self) -> None:
        """Refuse a bench this cannot judge, then hold the Kind mutation Lease."""

        mode = self._control.telemetry_mode()
        if mode != "otel":
            raise SmokeConfigurationError(
                f"reconciler uniqueness is read from Prometheus, which only otel mode installs; "
                f"the bench runs {mode}"
            )
        replicas = self._control.controller_replicas()
        if replicas != 2 or not self._control.controller_leader_election_enabled():
            raise SmokeConfigurationError(
                f"requires two cluster-controller replicas with leader election on; found {replicas}"
            )
        release = self._control.acquire_retention_sweeper_mutation()
        self._scope.defer("release the Kind mutation Lease", release, critical=True)
        # A contender a killed run left behind, in this shape or an earlier one.
        self._control.remove_refused_contender()
        self._control_plane = self._control.control_plane_node()
        self._fixture.record("control_plane_node", self._control_plane)

    def shrink_liveness_budget(self) -> None:
        """Record the standing budget hook, request the floor and await the rollout."""

        standing = self._control.controller_liveness_budget_env()
        self._standing_budget = standing
        self._fixture.record("liveness_budget_env_before", standing)
        # Cleanup runs these after the apiserver's restore and before the
        # mutation Lease's release, in reverse: the budget's write first, then
        # the contender's deletion. Neither waits, so after a failed outage
        # both fit the shared cleanup budget and the Lease is still released;
        # the phases at the end wait for the rollout and the deletion instead.
        # The contender's is armed here, before its pod exists, because its
        # deletion is idempotent; the budget's before the write, so a set that
        # half-applies is still put back.
        self._remove_contender = self._scope.defer(
            "remove the refused leader-election contender",
            lambda: self._control.remove_refused_contender(wait=False),
            critical=True,
        )
        self._restore_budget = self._scope.defer(
            "restore the standing leader-election liveness budget",
            lambda: self._control.restore_controller_liveness_budget_env(standing, wait=False),
            critical=True,
        )
        requested = str(LIVENESS_BUDGET_SECONDS)
        self._control.set_controller_liveness_budget_env(
            requested, lease_ttl=str(LEASE_TTL_SECONDS)
        )
        live = self._control.controller_liveness_budget_env()
        self._fixture.verify(
            live == requested,
            "the rolled cluster-controller does not carry this run's liveness budget",
            requested=requested,
            live=live,
        )
        self._budget_ceiling = LIVENESS_BUDGET_SECONDS * LIVENESS_BUDGET_SPREAD
        self._fixture.record(
            "liveness_budget", {"requested": requested, "ceiling_seconds": self._budget_ceiling}
        )

    def restore_liveness_budget(self) -> None:
        """Put the standing budget hook back and check it took."""

        if self._restore_budget is None:
            return
        standing = self._standing_budget
        self._control.restore_controller_liveness_budget_env(standing)
        after = self._control.controller_liveness_budget_env()
        self._fixture.verify(
            after == standing,
            "the controller's liveness-budget env did not come back to its standing value",
            standing=standing,
            after=after,
        )
        self._fixture.record("liveness_budget_env_after", after)
        self._fixture.record("renewal_failures", self._renewal_failures())
        self._restore_budget.disarm()
        self._restore_budget = None

    def _ceiling(self) -> float:
        if self._budget_ceiling is None:
            raise RuntimeError("the liveness budget was not shrunk before a wait was bounded by it")
        return self._budget_ceiling

    def place_refused_contender(self) -> None:
        """Place the contender with no `leases` grant, held before its first lease call.

        Its removal is armed by `shrink_liveness_budget`, before this dispatch.
        """

        self._control.place_refused_contender()
        # Cloned from the live Deployment, so it carries the same budget the
        # outage is about to show that image honours.
        inherited = self._control.controller_liveness_budget_env(refused_contender=True)
        self._fixture.verify(
            inherited == str(LIVENESS_BUDGET_SECONDS),
            "the refused contender does not carry this run's liveness budget, so its watch "
            "would end before its own budget could",
            inherited=inherited,
        )

    def start_refused_contender(self) -> None:
        """Release the placed contender and wait for its controller container.

        Released only once a healthy replica has renewed the Lease since the
        restore: the apiserver can answer on its host port a minute or more
        before pods reach it through the cluster's Service, and the
        contender's calls in that gap would be unreachable, which charges its
        budget, rather than refused. A container that started before the
        restore fails here.
        """

        before = self._lease_before_outage
        assert before is not None
        # Nothing renews the Lease while the apiserver is down, so a renewal
        # newer than the outage's last one was written after the restore.
        self._poll(
            "a healthy replica to renew the Lease through the cluster after the restore",
            self._control.controller_lease,
            lambda lease: lease is not None and lease.renew_time > before.renew_time,
            timeout=300,
            tolerate_errors=True,
        )
        self._control.release_refused_contender()
        pods = self._poll(
            "the refused contender's container to start",
            lambda: self._control.controller_pods(refused_contender=True),
            lambda pods: len(pods) == 1 and pods[0].started_at is not None,
            timeout=180,
            tolerate_errors=True,
        )
        contender = pods[0]
        assert contender.started_at is not None
        self._fixture.verify(
            contender.restart_count == 0
            and contender.started_at + KUBE_TIME_RESOLUTION_SECONDS >= self._restore_requested,
            "the refused contender's controller started before the apiserver was restored",
            contender_started_at=contender.started_at,
            restore_requested_at=self._restore_requested,
            contender_restarts=contender.restart_count,
        )
        self._contender = contender
        self._nodes[contender.name] = contender.node
        self._fixture.record(
            "refused_contender", {"pod": contender.name, "started_at": contender.started_at}
        )

    # --- one leader, one runner -------------------------------------------------

    def expect_one_leader(self, *, label: str, since: float | None = None, timeout: float = 180) -> str:
        """Two Ready replicas, a holder renewing among them, and only it reconciling.

        The window opens no earlier than `since`, the phase start by default.
        """

        since = time.time() if since is None else since
        lease = self.expect_a_holder(label=label, since=since, timeout=timeout)
        self._expect_only_runner(lease, label=label, since=since)
        return lease.holder

    def expect_initial_holder(self) -> None:
        """Two Ready replicas and a renewing holder, on the TTL the budget's floor assumes.

        The runner window that `expect_initial_runner` judges may open as
        early as this check's start.
        """

        since = time.time()
        lease = self.expect_a_holder(label="initial", since=since)
        self._fixture.verify(
            lease.duration_seconds == LEASE_TTL_SECONDS,
            f"the Lease lasts {lease.duration_seconds}s, but the liveness budget this run requests "
            f"is the floor of a {LEASE_TTL_SECONDS}s TTL",
        )
        self._initial = (lease, since)

    def expect_initial_runner(self) -> None:
        """Before the outage, only the holder reconciles, for one tick of every loop.

        The outage's later checks that the holder's ticks stop are only as
        good as this proof that it was ticking.
        """

        assert self._initial is not None
        lease, since = self._initial
        self._expect_only_runner(lease, label="initial", since=since, window=ONE_TICK_WINDOW_SECONDS)

    def expect_recovered_leader(self) -> str:
        """One fresh holder and one runner, judged from the restore on.

        Nothing could hold the Lease before the restore, so the window may
        open there, and the outage's record checks that ran since count
        toward it.
        """

        return self.expect_one_leader(label="recovered", since=self._restore_requested, timeout=300)

    def expect_a_holder(
        self, *, label: str, since: float | None = None, timeout: float = 180
    ) -> ControllerLease:
        """Two Ready replicas and a holder among them that renewed since `since`."""

        since = time.time() if since is None else since

        def settled() -> tuple[list[ControllerPod], ControllerLease | None]:
            return self._live_healthy_pods(), self._control.controller_lease()

        pods, lease = self._poll(
            f"{label}: a Lease renewed by one of two Ready replicas",
            settled,
            lambda state: len(state[0]) == 2
            and all(pod.ready for pod in state[0])
            and state[1] is not None
            and state[1].holder in {pod.name for pod in state[0]}
            # A departed holder keeps its name on the Lease until it lapses; a
            # renewal after the wait began proves this holder is live.
            and state[1].renew_time >= since,
            timeout=timeout,
            tolerate_errors=True,
        )
        assert lease is not None
        for pod in pods:
            self._nodes[pod.name] = pod.node
        self._healthy = {pod.name: pod for pod in pods}
        self._fixture.record(
            f"{label}_lease",
            {"holder": lease.holder, "renew_time": lease.renew_time,
             "acquire_time": lease.acquire_time, "transitions": lease.transitions},
        )
        return lease

    def _expect_only_runner(
        self,
        lease: ControllerLease,
        *,
        label: str,
        since: float,
        window: float = 2 * SLOWEST_TICK_SECONDS,
    ) -> None:
        """Only the holder's reconciler tick count moves in a window opened now.

        The window opens at the latest of `since`, the holder's current
        container start and its acquisition. Each series' count at that
        instant is its baseline, so a series that first appears inside the
        window counts from zero rather than from its first sample. It stays
        open for `window`, by default two of the slowest loop's ticks so a
        second runner that starts late is still seen, and until the holder's
        count has moved; any other pod's count moving fails at once.
        """

        holder_started = self._healthy[lease.holder].started_at or 0.0
        # The two Kubernetes times are rounded down, so each floors the window
        # one resolution later; the phase start is the runner's own clock.
        opened = max(
            since,
            holder_started + KUBE_TIME_RESOLUTION_SECONDS,
            (lease.acquire_time or 0.0) + KUBE_TIME_RESOLUTION_SECONDS,
        )
        closes = opened + window
        # The candidates: both replicas, and the refused contender if it runs.
        pods = sorted({*self._healthy, *([self._contender.name] if self._contender else [])})
        selector = f'{{k8s_pod_name=~"{"|".join(pods)}"}}'
        # A sample stamped before `opened` can arrive up to an export later.
        settled = opened + EXPORT_INTERVAL_SECONDS + POLL_SECONDS
        baseline = self._poll(
            f"{label}: Prometheus to answer for the window's baseline",
            lambda: self._tick_counts(selector, at=opened) if time.time() >= settled else None,
            lambda counts: counts is not None,
            timeout=max(settled - time.time(), 0) + 120,
            tolerate_errors=True,
        )
        assert baseline is not None
        deadline = max(time.time(), closes) + 180
        runners: list[str] = []
        while True:
            now = time.time()
            try:
                counts = self._tick_counts(selector, at=now)
            except Exception as error:  # noqa: BLE001 - the forward may be catching up
                counts, failure = None, f"{type(error).__name__}: {error}"
            if counts is not None:
                runners = sorted(
                    {key[0] for key, count in counts.items() if _moved(count, baseline.get(key))}
                )
                self._fixture.verify(
                    runners in ([], [lease.holder]),
                    f"{label}: expected only the Lease holder {lease.holder} to run reconcilers; "
                    f"ticks moved on {runners}",
                    failure_evidence=self._log_evidence,
                    **{f"{label}_reconciler_window": {"opened": opened, "seconds": round(now - opened)}},
                )
                if runners and now >= closes:
                    break
            if now >= deadline:
                self._fixture.verify(
                    False,
                    f"{label}: the holder {lease.holder}'s reconciler ticks never moved in "
                    f"{round(now - opened)}s from {opened:.0f}",
                    failure_evidence=self._log_evidence,
                    last_observed=repr(counts if counts is not None else failure)[:2000],
                )
            time.sleep(POLL_SECONDS)
        self._fixture.record(
            f"{label}_reconciler_window", {"opened": opened, "seconds": round(time.time() - opened)}
        )

    # --- a refused lease call is not a restart ---------------------------------

    def expect_refusal_is_not_a_restart(self, leader: str) -> None:
        """The contender is refused, stays NotReady and unrestarted for a full budget."""

        watch_started = time.time()
        contender = self._contender
        assert contender is not None and contender.started_at is not None

        # The contender's name is fixed, so a series an earlier run left can
        # still answer an instant query; only an increment since this
        # container started is this contender's.
        first_refused = self._poll(
            "the contender's first refused lease call to reach Prometheus",
            lambda: self._first_increment(
                f'{ACQUIRE_FAILED}{{reason="refused", k8s_pod_name="{contender.name}"}}',
                since=contender.started_at - KUBE_TIME_RESOLUTION_SECONDS,
            ),
            lambda at: at is not None,
            timeout=120,
            tolerate_errors=True,
        )
        assert first_refused is not None

        # A budget that charged refusals would start at the first refused
        # call, stamped no earlier than the sample that shows it, and end the
        # process on the attempt that crosses its ceiling.
        until = first_refused + self._ceiling() + RETRY_INTERVAL_SECONDS + REFUSAL_RESTART_SEEN_SECONDS
        self._fixture.record(
            "contender_refusals",
            {"first_after_start_seconds": round(first_refused - contender.started_at, 1),
             "watched_until": until},
        )
        lease = self._control.controller_lease()
        assert lease is not None
        while True:
            now = time.time()
            pods = self._control.controller_pods(refused_contender=True)
            current = pods[0] if len(pods) == 1 else None
            self._fixture.verify(
                current is not None and current.uid == contender.uid,
                "the refused contender's pod was replaced",
                failure_evidence=self._log_evidence,
                contender_pods=[pod.name for pod in pods],
            )
            assert current is not None
            self._fixture.verify(
                current.restart_count == 0,
                f"the refused contender restarted ({current.restart_count}) on a refusal "
                "a restart cannot fix",
                failure_evidence=self._log_evidence,
                contender_last_exit_code=current.last_exit_code,
            )
            self._fixture.verify(
                not current.ready,
                "the refused contender is Ready although it cannot reach the Lease",
                failure_evidence=self._log_evidence,
            )
            healthy = {pod.name: pod for pod in self._live_healthy_pods()}
            self._fixture.verify(
                set(healthy) == set(self._healthy)
                and all(
                    healthy[name].restart_count == pod.restart_count
                    for name, pod in self._healthy.items()
                ),
                "a healthy replica restarted or was replaced while a third contender was refused",
                failure_evidence=self._log_evidence,
                healthy_restarts={name: pod.restart_count for name, pod in healthy.items()},
            )
            lease = self._control.controller_lease()
            self._fixture.verify(
                lease is not None and lease.holder == leader,
                f"the leader changed from {leader} while a third contender was refused",
                failure_evidence=self._log_evidence,
                lease_holder=lease.holder if lease else None,
            )
            if now >= until:
                break
            time.sleep(min(5.0, max(until - now, 0.1)))
        assert lease is not None
        self._fixture.verify(
            all(pod.ready for pod in healthy.values()),
            "a healthy replica is NotReady after a third contender was refused",
            failure_evidence=self._log_evidence,
        )
        self._fixture.record("contender_watched_seconds", round(time.time() - contender.started_at))
        # The holder kept renewing, and still alone ran the reconcilers.
        self._expect_only_runner(lease, label="refused", since=watch_started)

    def remove_refused_contender(self) -> None:
        """Delete the contender and wait for its pod to be gone."""

        self._control.remove_refused_contender()
        if self._remove_contender is not None:
            self._remove_contender.disarm()
        self._contender = None

    # --- the apiserver outage ------------------------------------------------------

    def take_the_apiserver_away(self) -> None:
        """Record the marker holders, park the apiserver and wait for it to stop."""

        markers = []
        for pod in self._healthy.values():
            for container in self._control.node_controller_containers(pod.node, pod.name):
                if container.running and self._control.readiness_marker_present(
                    pod.node, container.container_id
                ):
                    markers.append((pod.name, container.container_id))
        self._fixture.verify(
            bool(markers),
            "no controller container held a readiness marker before the outage, so its "
            "removal cannot be observed",
        )
        self._markers = markers
        self._running_before = {
            name: {container.container_id for container in self._containers(name) if container.running}
            for name in self._healthy
        }
        self._fixture.record("readiness_markers_before_outage", [pod for pod, _ in markers])

        # Started before the fault, so every controller container's exit
        # during it is recorded as containerd reports it.
        for controller_node in sorted({pod.node for pod in self._healthy.values()}):
            exits = self._control.watch_node_container_exits(controller_node)
            self._scope.defer(f"stop the container exit watch on {controller_node}", exits.stop)
            self._exit_watches[controller_node] = exits

        node = self._control_plane
        # Started before the fault, so it has delivered every renewal the
        # apiserver committed by the time it stops answering.
        watch = self._control.watch_controller_lease()
        self._scope.defer("stop the Lease watch", watch.stop)
        # kubelet stops the apiserver within seconds of the park, possibly
        # before the watch's first document arrives, so the anchor is read
        # here; later reads may only replace it with a newer renewal.
        before = self._control.controller_lease()
        self._fixture.verify(before is not None, "the cluster-controller has no Lease to anchor the outage on")

        def restore() -> None:
            # It answers within seconds of the restore; bounded by cleanup's
            # time plan (by APISERVER_RESTORE_SECONDS), so what follows still runs.
            self._control.restore_apiserver(node, timeout=APISERVER_RESTORE_SECONDS)
            deadline = time.monotonic() + APISERVER_ANSWER_SECONDS
            while not self._control.apiserver_answers() and time.monotonic() < deadline:
                time.sleep(POLL_SECONDS)

        # Armed before dispatch: a move whose reply is lost may still have run.
        self._restore_apiserver = self._scope.defer("restore the Kind apiserver", restore, critical=True)
        self._control.park_apiserver(node)
        self._outage_started = time.time()
        self._fixture.record("outage_started_at", self._outage_started)

        # The newest renewal the apiserver served: the watch's, and reads back
        # to back until the first that goes unanswered.
        polled: ControllerLease | None = before
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                polled = _newest(polled, self._control.controller_lease())
            except Exception:  # noqa: BLE001 - an unanswered read is the outage arriving
                if not self._control.apiserver_answers():
                    break
            time.sleep(0.1)
        self._fixture.verify(
            not self._control.apiserver_answers(),
            "the apiserver still answers 90 s after its manifest was parked",
        )
        watched = watch.latest()
        watch.stop()
        lease = _newest(polled, watched)
        assert lease is not None
        self._fixture.record(
            "lease_renewals_seen",
            {"polled": polled.renew_time if polled else None,
             "watched": watched.renew_time if watched else None},
        )
        self._lease_before_outage = lease
        self._fixture.record(
            "lease_before_outage",
            {"holder": lease.holder, "renew_time": lease.renew_time,
             "duration_seconds": lease.duration_seconds},
        )
        self._fixture.verify(
            lease.holder in self._healthy,
            f"the Lease holder {lease.holder} is not a healthy replica",
        )

    def expect_marker_cleared(self) -> None:
        """A container that had the readiness marker loses it while still running."""

        def cleared() -> list[str]:
            pods = []
            for pod, container_id in self._markers:
                running = {c.container_id for c in self._containers(pod) if c.running}
                # Only an explicit answer from a running container counts; an
                # exec that fails, whatever the reason, is read again.
                if container_id in running and self._control.readiness_marker_present(
                    self._nodes[pod], container_id
                ) is False:
                    pods.append(pod)
            return pods

        pods = self._poll(
            "a running container to clear its readiness marker",
            cleared,
            bool,
            timeout=90,
        )
        self._fixture.record("readiness_marker_cleared", pods)

    def expect_holder_stops(self) -> None:
        """The holder's container ends before its Lease could be taken over."""

        lease = self._lease_before_outage
        assert lease is not None
        holder = lease.holder
        expiry = _latest_expiry(lease)
        running = self._running_before[holder]

        def ended() -> list[NodeContainer]:
            return [container for container in self._exits(holder) if container.container_id in running]

        due = expiry + HOLDER_EXIT_AFTER_EXPIRY_SECONDS
        # Read past `due`, so a late exit is measured rather than timed out on.
        [container] = self._poll(
            f"the holder {holder}'s container to end",
            ended,
            bool,
            timeout=max(due + 60 - time.time(), 30),
        )[:1]
        finished = container.finished_at or 0.0
        self._fixture.record(
            "holder_exit",
            {"exit_code": container.exit_code, "finished_at": finished,
             "after_renewal_seconds": round(finished - lease.renew_time, 1)},
        )
        self._fixture.verify(
            finished <= due,
            f"the holder's container ended {finished - expiry:.1f}s after its Lease expired; "
            f"its deadlines end it within {HOLDER_EXIT_AFTER_EXPIRY_SECONDS:.0f}s",
            failure_evidence=self._log_evidence,
        )
        self._fixture.verify(
            container.exit_code not in (None, 0),
            f"the holder's container exited {container.exit_code}; losing the Lease must exit non-zero",
            failure_evidence=self._log_evidence,
        )

    def expect_budget_exit(self) -> None:
        """A controller container exits after the Lease lapsed: a spent budget."""

        lease = self._lease_before_outage
        assert lease is not None
        # Nothing holds the Lease after it lapses with the apiserver down, so no
        # later exit is a resignation; the holder's own container is left out
        # in case it ended late, which `expect_outage_recorded` reports. A
        # budget the requested one did not bound, such as the shipped 600 s,
        # exits after this deadline.
        lapsed = _latest_expiry(lease)
        resigned = self._running_before[lease.holder]
        until = self._outage_started + self._ceiling() + 120

        def exited() -> list[tuple[str, NodeContainer]]:
            return [
                (pod, container)
                for pod in self._healthy
                for container in self._exits(pod)
                if (container.finished_at or 0) > lapsed and container.container_id not in resigned
            ]

        found = self._poll(
            "a replica to spend its liveness budget and exit",
            exited,
            bool,
            timeout=max(until - time.time(), 60),
        )
        pod, container = found[0]
        self._budget_exit = (pod, container)
        self._fixture.record(
            "budget_exit",
            {"pod": pod, "exit_code": container.exit_code,
             "into_outage_seconds": round((container.finished_at or 0) - self._outage_started)},
        )
        self._fixture.verify(
            container.exit_code not in (None, 0),
            f"{pod} exited {container.exit_code} after the Lease lapsed; a spent budget exits non-zero",
            failure_evidence=self._log_evidence,
        )
        for exits in self._exit_watches.values():
            exits.stop()

    def bring_the_apiserver_back(self) -> None:
        self._restore_requested = time.time()
        self._fixture.record("restore_requested_at", self._restore_requested)
        self._control.restore_apiserver(self._control_plane)
        self._poll("the apiserver to answer again", self._control.apiserver_answers, bool, timeout=300)
        # The localhost forwards to Prometheus restart once kubectl can reach
        # the apiserver again; reads below tolerate the gap.
        if self._restore_apiserver is not None:
            self._restore_apiserver.disarm()
        self._fixture.record("outage_seconds", round(time.time() - self._outage_started))

    def expect_outage_recorded(self) -> None:
        """The pods report the restarts, and Prometheus holds why each happened."""

        lease = self._lease_before_outage
        assert lease is not None and self._budget_exit is not None
        holder, (spender, _) = lease.holder, self._budget_exit
        before = {name: pod.restart_count for name, pod in self._healthy.items()}

        # kubelet reports the restarts once it can reach the apiserver again.
        pods = self._poll(
            "kubelet to report the outage's restarts",
            lambda: {pod.name: pod for pod in self._control.controller_pods()},
            lambda pods: all(
                name in pods and pods[name].restart_count > before[name] for name in {holder, spender}
            ),
            timeout=180,
            tolerate_errors=True,
        )
        self._fixture.record(
            "restarts_after_outage", {name: pods[name].restart_count for name in before if name in pods}
        )
        # The spender's non-zero exit is judged from containerd's record in
        # `expect_budget_exit`. kubelet's last termination is evidence only:
        # it reports none for a container it removed as it restarted it.
        self._fixture.record(
            "spender_last_termination",
            {"exit_code": pods[spender].last_exit_code, "finished_at": pods[spender].last_finished_at},
        )

        lost = self._poll(
            "the holder's lease_lost to reach Prometheus",
            lambda: self._first_increment(f'{LEASE_LOST}{{identity="{holder}"}}', since=self._outage_started),
            lambda at: at is not None,
            timeout=120,
            tolerate_errors=True,
        )
        assert lost is not None
        expiry = _latest_expiry(lease)
        self._fixture.record(
            "holder_lease_lost", {"after_renewal_seconds": round(lost - lease.renew_time, 1),
                                  "lease_duration_seconds": lease.duration_seconds},
        )
        self._fixture.verify(
            lost < expiry,
            f"the holder reported losing the Lease {lost - lease.renew_time:.0f}s after its last "
            f"renewal, not before the Lease's {lease.duration_seconds}s duration ran out",
            failure_evidence=self._log_evidence,
        )

        # Two samples stamped after expiry with a higher count between them
        # are a tick that finished after a standby could have taken over. Only
        # samples before the restore are judged: until then nothing can hold
        # the Lease, so a tick is the expired holder's, and after it a
        # restarted container with the holder's pod name may lead again.
        cutoff = self._restore_requested
        self._fixture.record("holder_ticks_judged", {"from": expiry, "until": cutoff})
        late = self._poll(
            "Prometheus to answer for the holder's ticks after expiry",
            lambda: self._ticks_between(holder, after=expiry, until=cutoff),
            lambda _: True,
            timeout=120,
            tolerate_errors=True,
        )
        self._fixture.verify(
            late is None,
            f"the holder's reconciler ticks grew after its Lease expired and before the "
            f"apiserver was restored: {late}",
            failure_evidence=self._log_evidence,
        )

        unreachable = self._poll(
            f"{spender}'s lease_unreachable to reach Prometheus",
            lambda: self._first_increment(
                f'{LEASE_UNREACHABLE}{{identity="{spender}", reason="unreachable"}}',
                since=self._outage_started,
            ),
            lambda at: at is not None,
            timeout=120,
            tolerate_errors=True,
        )
        assert unreachable is not None
        self._fixture.record(
            "budget_spent", {"pod": spender, "into_outage_seconds": round(unreachable - self._outage_started)}
        )

    # --- reads -------------------------------------------------------------------

    def _live_healthy_pods(self) -> list[ControllerPod]:
        return [pod for pod in self._control.controller_pods() if not pod.deleting]

    def _containers(self, pod: str) -> list[NodeContainer]:
        containers = self._control.node_controller_containers(self._nodes[pod], pod)
        self._listed.setdefault(pod, set()).update(container.container_id for container in containers)
        return containers

    def _exits(self, pod: str) -> list[NodeContainer]:
        """The pod's ended controller containers.

        Those the node's runtime lists as exited, and those it no longer lists
        whose exit its containerd reported: kubelet can remove an exited
        container as soon as it starts the next one, before any listing shows
        it exited.
        """

        containers = self._containers(pod)
        ended = {container.container_id: container for container in containers if not container.running}
        listed = {container.container_id for container in containers}
        watch = self._exit_watches.get(self._nodes[pod])
        for container_id, (exit_code, exited_at) in (watch.exits() if watch else {}).items():
            if container_id in self._listed.get(pod, set()) and container_id not in listed:
                ended[container_id] = NodeContainer(
                    container_id=container_id,
                    # Gone with the container; only log evidence names attempts.
                    attempt=-1,
                    running=False,
                    started_at=None,
                    finished_at=exited_at,
                    exit_code=exit_code,
                )
        return list(ended.values())

    def _prometheus(self, query: str, at: float | None = None) -> list[dict[str, Any]]:
        body = self._control.telemetry_query(
            "prometheus", "/api/v1/query", {"query": query, "time": str(at or time.time())}
        )
        return list(body["data"]["result"])

    def _tick_counts(self, selector: str, *, at: float) -> dict[tuple[str, str, str], float]:
        """Each tick series' count at `at`, keyed by pod, process and loop."""

        return {
            (
                str(sample["metric"].get("k8s_pod_name", "")),
                str(sample["metric"].get("service_instance_id", "")),
                str(sample["metric"].get("loop", "")),
            ): float(sample["value"][1])
            for sample in self._prometheus(f"histogram_count({RECONCILER_TICKS}{selector})", at=at)
        }

    def _ticks_between(self, pod: str, *, after: float, until: float) -> tuple[float, float] | None:
        """Two samples stamped in `[after, until)` with the count grown between them."""

        now = time.time()
        window = int(now - after) + 60
        for series in self._prometheus(f'{RECONCILER_TICKS}{{k8s_pod_name="{pod}"}}[{window}s]', at=now):
            samples = sorted(
                (float(stamp), float(histogram["count"]))
                for stamp, histogram in series.get("histograms") or []
                if after <= float(stamp) < until
            )
            for (earlier, before), (later, count) in zip(samples, samples[1:]):
                if count > before:
                    return earlier, later
        return None

    def _first_increment(self, selector: str, *, since: float) -> float | None:
        """When a counter first moved at or after `since`, from its raw samples.

        The window reaches back before `since`, so a series that already
        existed is compared with its own earlier value. A count below the one
        before it is a process that started over, and moved if it is not zero.
        """

        now = time.time()
        window = int(now - since) + 60
        first = None
        for series in self._prometheus(f"{selector}[{window}s]", at=now):
            previous = None
            for stamp, raw in series.get("values") or []:
                at, value = float(stamp), float(raw)
                increased = value > 0 if previous is None else value > previous or 0 < value < previous
                if at >= since and increased:
                    first = at if first is None else min(first, at)
                    break
                previous = value
        return first

    def _log_evidence(self) -> Mapping[str, object]:
        """Each controller container's last log lines, read on its node, and
        each node's exit watch's state and stderr: a watch that died leaves
        exits to the runtime's listings alone."""

        logs: dict[str, str] = {}
        for pod, node in sorted(self._nodes.items()):
            try:
                for container in self._control.node_controller_containers(node, pod):
                    logs[f"{pod}#{container.attempt}"] = self._control.node_container_log_tail(
                        node, container.container_id, lines=LOG_TAIL_LINES
                    )
            except Exception as error:  # noqa: BLE001 - evidence must not mask the failure
                logs[pod] = f"could not read logs: {type(error).__name__}: {error}"
        watches = {node: watch.diagnostics() for node, watch in sorted(self._exit_watches.items())}
        return {
            "controller_logs": logs,
            "renewal_failures": self._renewal_failures(),
            **({"exit_watches": watches} if watches else {}),
        }

    def _renewal_failures(self) -> dict[str, float] | str:
        """Each pod's failed Lease renewals by reason, as Prometheus last saw them."""

        try:
            return {
                f'{sample["metric"].get("k8s_pod_name", "")}/{sample["metric"].get("reason", "")}': float(
                    sample["value"][1]
                )
                for sample in self._prometheus(f"sum by (k8s_pod_name, reason) ({RENEW_FAILED})")
            }
        except Exception as error:  # noqa: BLE001 - evidence must not mask the failure
            return f"could not read {RENEW_FAILED}: {type(error).__name__}: {error}"

    def _poll(
        self,
        what: str,
        read: Callable[[], T],
        done: Callable[[T], bool],
        *,
        timeout: float,
        tolerate_errors: bool = False,
    ) -> T:
        """Read until `done`, failing at the deadline with the last reading.

        `tolerate_errors` retries a read that raised, for reads that follow
        the outage: kubectl and the localhost forwards come back a little
        after the apiserver does.
        """

        deadline = time.monotonic() + timeout
        while True:
            try:
                observed: object = read()
            except Exception as error:  # noqa: BLE001 - retried until the deadline
                if not tolerate_errors:
                    raise
                observed = f"{type(error).__name__}: {error}"
            else:
                if done(observed):  # type: ignore[arg-type]
                    return observed  # type: ignore[return-value]
            if time.monotonic() >= deadline:
                self._fixture.verify(
                    False,
                    f"timed out after {timeout:.0f}s waiting for {what}",
                    failure_evidence=self._log_evidence,
                    last_observed=repr(observed)[:2000],
                )
            time.sleep(POLL_SECONDS)


def _moved(count: float, baseline: float | None) -> bool:
    """A series counted a tick since its baseline.

    No baseline is a series that began inside the window, counting from zero.
    A count below its baseline is a process that started over in the same
    series, and moved if it is not zero.
    """

    if baseline is None:
        return count > 0
    return count > baseline or 0 < count < baseline


def _latest_expiry(lease: ControllerLease) -> float:
    """The latest instant the Lease can expire, given `renewTime`'s whole seconds.

    Every bound measured from it errs toward the holder: a correct holder is
    never failed for the rounding.
    """

    return lease.renew_time + lease.duration_seconds + KUBE_TIME_RESOLUTION_SECONDS


def _newest(*leases: ControllerLease | None) -> ControllerLease | None:
    return max((lease for lease in leases if lease is not None), key=lambda lease: lease.renew_time, default=None)

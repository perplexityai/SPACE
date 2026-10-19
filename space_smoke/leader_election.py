"""Cluster-controller leader election against a live control plane.

The crate's leader-election tests run the state machine against a fake lease
store. What they cannot show needs a real apiserver: that a 403 arrives as the
category the policy spares, that a holder cut off from the apiserver stops
before its Lease can be taken over, and that a replica which spends its
liveness budget is restarted by kubelet rather than returning a value nobody
acts on. This module is the typed form of the opt-in shell suite that proved
them by reading cluster-controller log lines off the nodes; every verdict here
comes from state instead.

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
  second runner that starts late is still seen.
- **A refused lease call is not a restart.** A third contender runs the
  controller's own pod template under a ServiceAccount with no `leases` grant.
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
because nothing else answers then. Logs are never read for a verdict: the
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

from space_smoke.capabilities import ControllerLease, ControllerPod, LeaderElectionControl, NodeContainer
from space_smoke.runtime import Compensation, ResourceScope, SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# The timings `LeaderElectionConfig::from_env` resolves, which no Kubernetes
# object or metric exposes (crates/libs/cluster/controller/server/src/
# leader_election/mod.rs): a standby retries every 8 s, and the liveness budget
# is 600 s plus a per-pod spread of up to a quarter of it. The Lease's own
# `leaseDurationSeconds` is read from the Lease.
RETRY_INTERVAL_SECONDS = 8.0
LIVENESS_BUDGET_CEILING_SECONDS = 600.0 * 1.25
# The OTLP metric export interval (crates/libs/observability/src/sink/otlp.rs).
EXPORT_INTERVAL_SECONDS = 10.0
# The slowest interval among the loops that record the reconciler tick
# histogram: 60 s for started_sandbox_repair, failed_restorable_repair,
# suspended_execution_reaper, transient_lifecycle_sweeper and the two pod GCs;
# the warm pool's is 5 s.
SLOWEST_TICK_SECONDS = 60.0
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

POLL_SECONDS = 2.0
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
        self._outage_started = 0.0
        # When the restore was requested: no Lease can be acquired before it.
        self._restore_requested = 0.0
        self._lease_before_outage: ControllerLease | None = None
        self._budget_exit: tuple[str, NodeContainer] | None = None
        # Pod name -> node, for reading container state and logs while the
        # apiserver is down.
        self._nodes: dict[str, str] = {}
        # (pod, container id) that held the readiness marker before the outage.
        self._markers: list[tuple[str, str]] = []
        # Pod name -> the container ids running when the outage began.
        self._running_before: dict[str, set[str]] = {}

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
        self._control_plane = self._control.control_plane_node()
        self._fixture.record("control_plane_node", self._control_plane)

    def start_refused_contender(self) -> None:
        """Start the contender with no `leases` grant and wait for its container."""

        # Armed before dispatch: an apply whose reply is lost may still commit.
        self._remove_contender = self._scope.defer(
            "remove the refused leader-election contender",
            self._control.remove_refused_contender,
            critical=True,
        )
        self._control.start_refused_contender()
        pods = self._poll(
            "the refused contender's container to start",
            lambda: self._control.controller_pods(refused_contender=True),
            lambda pods: len(pods) == 1 and pods[0].started_at is not None,
            timeout=180,
        )
        self._contender = pods[0]
        self._nodes[pods[0].name] = pods[0].node
        self._fixture.record(
            "refused_contender", {"pod": pods[0].name, "started_at": pods[0].started_at}
        )

    # --- one leader, one runner -------------------------------------------------

    def expect_one_leader(self, *, label: str, timeout: float = 180) -> str:
        """Two Ready replicas, a holder renewing among them, and only it reconciling."""

        since = time.time()

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
        self._expect_only_runner(lease, label=label, since=since)
        return lease.holder

    def _expect_only_runner(self, lease: ControllerLease, *, label: str, since: float) -> None:
        """Only the holder's reconciler tick count moves in a window opened now.

        The window opens at the latest of `since`, the holder's current
        container start and its acquisition. Each series' count at that
        instant is its baseline, so a series that first appears inside the
        window counts from zero rather than from its first sample. It stays
        open for two of the slowest loop's ticks and until the holder's count
        has moved; any other pod's count moving fails at once.
        """

        holder_started = self._healthy[lease.holder].started_at or 0.0
        # The two Kubernetes times are rounded down, so each floors the window
        # one resolution later; the phase start is the runner's own clock.
        opened = max(
            since,
            holder_started + KUBE_TIME_RESOLUTION_SECONDS,
            (lease.acquire_time or 0.0) + KUBE_TIME_RESOLUTION_SECONDS,
        )
        closes = opened + 2 * SLOWEST_TICK_SECONDS
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

        def refused() -> float:
            return sum(
                float(sample["value"][1])
                for sample in self._prometheus(
                    f'{ACQUIRE_FAILED}{{reason="refused", k8s_pod_name="{contender.name}"}}'
                )
            )

        refusals = self._poll(
            "the contender's refused lease calls to reach Prometheus",
            refused,
            lambda count: count > 0,
            timeout=120,
        )
        self._fixture.record("contender_refused_calls", refusals)

        # A budget that charged refusals would end the process one budget after
        # its first lease call, which follows the container's start.
        until = contender.started_at + LIVENESS_BUDGET_CEILING_SECONDS + 2 * RETRY_INTERVAL_SECONDS
        self._fixture.record("contender_watched_until", until)
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
        if self._remove_contender is not None:
            self._remove_contender.release()
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
            self._control.restore_apiserver(node)
            deadline = time.monotonic() + 90
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
            return [c for c in self._containers(holder) if c.container_id in running and not c.running]

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
        # in case it ended late, which `expect_outage_recorded` reports.
        lapsed = _latest_expiry(lease)
        resigned = self._running_before[lease.holder]
        until = self._outage_started + LIVENESS_BUDGET_CEILING_SECONDS + 120

        def exited() -> list[tuple[str, NodeContainer]]:
            return [
                (pod, container)
                for pod in self._healthy
                for container in self._containers(pod)
                if not container.running
                and (container.finished_at or 0) > lapsed
                and container.container_id not in resigned
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
        self._fixture.verify(
            pods[spender].last_exit_code not in (None, 0),
            f"{spender}'s last container exited {pods[spender].last_exit_code}",
            failure_evidence=self._log_evidence,
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
        return self._control.node_controller_containers(self._nodes[pod], pod)

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
        """Each controller container's last log lines, read on its node."""

        logs: dict[str, str] = {}
        for pod, node in sorted(self._nodes.items()):
            try:
                for container in self._control.node_controller_containers(node, pod):
                    logs[f"{pod}#{container.attempt}"] = self._control.node_container_log_tail(
                        node, container.container_id, lines=LOG_TAIL_LINES
                    )
            except Exception as error:  # noqa: BLE001 - evidence must not mask the failure
                logs[pod] = f"could not read logs: {type(error).__name__}: {error}"
        return {"controller_logs": logs}

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

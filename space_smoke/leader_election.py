"""Cluster-controller leader election against a live control plane.

The crate's leader-election tests run the state machine against a fake lease
store. What they cannot show needs a real apiserver: that a 403 arrives as the
category the policy spares, that a holder cut off from the apiserver stops
before its Lease can be taken over, that a replica which spends its liveness
budget exits non-zero rather than returning a value nobody acts on, and that a
holder which loses its Lease is restarted by kubelet. This module is the
typed form of the opt-in shell suite that proved them by reading
cluster-controller log lines off the nodes; every verdict here comes from state
instead. One scenario runs every fault at once, so the budget waits overlap
instead of running in series.

- **A budget sized for one run.** The liveness budget ships at 600 s and the
  lease TTL at 60 s. This run asks for the TTL's floor and the budget's floor
  at that TTL through the controller's test hooks, which every replica and
  contender reads at startup, records the Deployment's standing budget first
  and puts both back at the end. Each contender reports the budget it
  resolved, spread included, through a gauge, and its waits are sized from
  that report rather than from a copy of the spread's hash, which a change in
  Rust would leave behind. An image that does not report it stops the run
  inconclusive before the floors roll out.
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
  ticks. Both contenders are in the window's selector. Every window is sized
  from the slowest loop's tick and the controller's live export interval
  (`detection_window_seconds`), which the preflight reads with the ticks
  once it holds the Kind mutation Lease, so no other run can change them.
  The recovered window opens at the reconnection and the outage's record
  checks run inside it.
- **A budget exit.** A contender runs the same image and settings against an
  unreachable pod-local API endpoint from the start of the run. It must
  report an unreachable Lease call, spend the budget it reports, and exit
  non-zero within it. A startup failure, or a binary that ignores the hook,
  cannot satisfy that.
- **A refused lease call is not a restart.** A third contender runs the
  controller's own pod template under a ServiceAccount with no `leases` grant.
  It is held by a gate container and released after a healthy holder renews
  the Lease, so cluster connectivity is established before its first call. It
  runs through the outage, which does not touch it. Prometheus shows its
  `acquire_failed{reason="refused"}`; from its first refused call until its
  reported budget, a retry and its shutdown have passed, it stays the same
  pod, never Ready, with `restartCount` 0 and no container exit on its node,
  and it never records a reconciler tick.
- **An outage.** Both healthy replicas are cut off from the apiserver by a
  REJECT rule in their own network namespaces, so every other client,
  kubelet's status reports included, keeps it. Each must still be the pod,
  unrestarted, that it was when the initial holder was seen, since the
  outage's restart checks are measured from the cut. A container that held the
  readiness marker before the outage loses it while still running. Nothing
  can write the Lease meanwhile, so once the holder's container has ended, a
  read of the Lease gives its last renewal exactly. That renewal must be
  stamped by the end of the holder's cut, within the stamp's whole second,
  or the holder renewed once its cut was in place. The holder's container
  ends non-zero within the bound the controller's own deadlines give its exit
  (`HOLDER_EXIT_AFTER_EXPIRY_SECONDS`), kubelet reports its restart, its
  `lease_lost` reaches Prometheus stamped before the Lease's last `renewTime`
  plus its duration, the earliest instant a standby could take over, and its
  tick count does not grow after that instant and before the reconnection:
  not from its process's last sample before that instant to its first after
  it, nor between two samples after it. The standby, which keeps running and
  exporting, counts its Lease calls through the cut Service as unreachable,
  the category the budget charges, and its container runs through the
  outage. The outage ends once that last window can show a running holder's
  tick, and just before it ends the Lease must still hold the holder's last
  renewal, or a cut let a replica through. Every step from the standby's cut
  to its reconnection is bounded from that cut, and the preflight refuses,
  as inconclusive, tick and export settings under which the outage could
  last as long as the standby's minimum budget (`outage_schedule_seconds`),
  or an export too slow to tell a late tick from a late export
  (`DRAIN_MARGIN_SECONDS`); the budget exit is the control's. Finally the
  cluster converges to one fresh holder, one runner and two Ready replicas.

The readiness marker is the file the controller's readiness probe tests, read
through the node's CRI; container exits are read there too, and from the
node's containerd events, because kubelet can remove an exited container
before any read lists it. Logs are never read for a verdict: the controller's
container logs are attached to a failure as evidence.

Not covered, as in the shell suite: the `overloaded` category (nothing in Kind
sheds load on demand), a wedge that spares the substrate but not the lease
path (they share one client), and a control plane that is down for every
client rather than for the controllers. The refused contender's NotReady is
an outcome only: it never wrote a marker, so it cannot show the marker being
cleared; the outage's marker check is what shows that.

Ctrl-C and SIGTERM still run the reconnection, restore the standing budget
and release the Kind mutation Lease. A run killed outright can leave all
three behind. The commands below name the default Kind cluster's context,
`kind-space-local`; use the context of the cluster the run targeted if it was
another one:

- The mutation Lease, from the preflight on. It is named
  `space-smoke-retention-<16 hex digits>` after the kube context and lasts an
  hour from the preflight; until then every run on the cluster that needs
  it, this one included, stops inconclusive with "another smoke holds". Read its
  holder and expiry with `kubectl --context kind-space-local -n space get
  leases -o custom-columns=NAME:.metadata.name,HOLDER:.spec.holderIdentity,RENEWED:.spec.renewTime,SECONDS:.spec.leaseDurationSeconds`:
  the holder names the runner's uid, pid and host. Confirm that runner is
  gone, then wait until `RENEWED` plus `SECONDS` has passed; the next run
  takes the expired Lease over. Never delete the Lease or clear its holder:
  a live runner may still hold it, and it fences that runner's writes.
- Its cuts, from the outage on: delete the two cluster-controller pods; their
  replacements get new network namespaces. The rollout below replaces them
  too.
- Its timing override, from the budget's request on: the controller
  Deployment keeps this run's budget and TTL hooks, which replacement pods
  inherit, and the next run refuses a Deployment that already sets the TTL
  hook. Put the standing budget back and remove the TTL hook:
  `kubectl --context kind-space-local -n space set env deploy/<controller>
  -c cluster-controller SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_FAILURE_GRACE_SECS=<standing>
  SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_TTL_SECS-`, where
  `<standing>` is the run's recorded `liveness_budget_env_before`, or write
  `..._LEASE_FAILURE_GRACE_SECS-` when that was unset, as on Kind.
  Reapplying the standing configuration does the same only if it replaces
  the container's env, which a client-side `kubectl apply` does not do for
  keys `kubectl set env` added. Then wait for the rollout
  (`kubectl --context kind-space-local -n space rollout status
  deploy/<controller>`) before the next run.

The next run's preflight removes the contenders and any cut left on a
running pod.
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
    from space_smoke.fixtures import KindAdmin

# The timings `LeaderElectionConfig::from_environment` resolves
# (crates/l40_api/cluster/controller/server/src/leader_election/mod.rs): the
# lease TTL, which this run requests at MIN_LEASE_TTL through
# LEASE_TTL_TEST_ENV (checked against the Lease), and the standby retry
# interval, DEFAULT_RETRY_INTERVAL clamped to the 2 s that TTL's renew deadline
# leaves room for. The liveness budget is the requested one raised to at least
# MIN_GRACE_LEASE_TTL_MULTIPLE TTLs and MIN_GRACE_RETRY_INTERVALS retries,
# plus a per-pod spread of up to 1/GRACE_SPREAD_DIVISOR of it: 72 s to 90 s at
# the floor this run requests, where the default TTL's are 240 s and 300 s.
# Each contender's own budget is read from its gauge; these bounds only check
# that the report is one this request can produce. The crate pins each of
# these with a compile-time assert, and
# `the_lease_ttl_override_is_bounded_like_the_default` pins the values at the
# TTL floor; change them here with it.
LEASE_TTL_SECONDS = 18
RETRY_INTERVAL_SECONDS = 2.0
LIVENESS_BUDGET_SECONDS = max(4 * LEASE_TTL_SECONDS, 30 * int(RETRY_INTERVAL_SECONDS))
LIVENESS_BUDGET_SPREAD = 1 + 1 / 4
# A process that ends runs only the metric and trace providers' shutdowns
# (5 s each, crates/l10_foundation/observability) after it decides to.
SHUTDOWN_SECONDS = 5.0 + 5.0
# The Collector batches what it receives for up to 1 s before Prometheus has
# it (infra/space/observability/charts/metrics/config/collector.yaml).
COLLECTOR_BATCH_SECONDS = 1.0
# How often every wait below reads again.
POLL_SECONDS = 2.0
# The loops that record the reconciler tick histogram in Kind, by the
# controller setting that ticks each: failed_restorable_repair,
# transient_lifecycle_sweeper and the orphaned-restore-pod GC ship at 60 s,
# and the Kind bring-up ticks them every 10 s
# (infra/space/kind/charts/space-cluster-controller-local/values-kind.yaml).
# The stuck-ContainerCreating GC, also at 60 s, is off on Kind. The warm
# pool's 5 s loop records it only with its orphan GC on, which Kind leaves off.
# The preflight reads these from the controller Deployment, so a bench that
# still ticks at 60 s gets the windows a minute's ticks need.
RECONCILER_TICK_ENV: dict[str, float] = {
    "SPACE_FAILED_RESTORABLE_REPAIR_TICK_SECS": 60.0,
    "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_TICK_SECS": 60.0,
    "SPACE_ORPHANED_RESTORE_POD_GC_INTERVAL_SECS": 60.0,
}


def slowest_tick_seconds(env: Mapping[str, str | None]) -> float:
    """The slowest of those loops' ticks, as the controller reads its env.

    A setting that is unset, or not a positive whole number of seconds, leaves
    that loop at its default.
    """

    ticks = []
    for key, default in RECONCILER_TICK_ENV.items():
        raw = env.get(key) or ""
        ticks.append(float(raw) if raw.isdigit() and int(raw) > 0 else default)
    return max(ticks)


# Every window that must see a tick keeps this much past the instant the tick
# could first be seen: the Collector's batch before Prometheus has the sample,
# a poll before this run reads it, and one more export interval for a loaded
# node whose export runs late. Kind's 2 s export makes it 5 s; the SDKs' 10 s
# default makes it 13 s.
def window_spare_seconds(export: float) -> float:
    return COLLECTOR_BATCH_SECONDS + POLL_SECONDS + export


# A process that runs the reconcilers throughout a window finishes a tick of
# every loop above within the slowest tick of the window opening, and the next
# export, at most one interval later, carries it; the spare follows. The count
# is compared with its value when the window opened, so a tick that finished
# just before then and was exported just after still counts as a move. On
# Kind: 10 s tick + 2 s export + 5 s spare = 17 s. Before the outage this
# shows the holder running and no other process running throughout.
def detection_window_seconds(slowest_tick: float, export: float) -> float:
    return slowest_tick + export + window_spare_seconds(export)


# The after-expiry check reads raw samples, so a tick is seen only between two
# samples stamped inside the window: the first export after it opens (one
# interval), the slowest tick after that sample, the export that carries it
# (one more), then the spare. On Kind 10 + 2 + 2 + 5 = 19 s past expiry, which
# also passes the holder's exit bound, expiry + 13 s: the outage ends about
# 38 s after the last renewal rather than after a standby's 72-90 s budget.
def after_expiry_window_seconds(slowest_tick: float, export: float) -> float:
    return slowest_tick + 2 * export + window_spare_seconds(export)


# After the reconnection, a second runner may start late, so the window spans
# two of the slowest ticks, and never less than one detection window: 20 s on
# Kind, 8 s past a tick and its export.
def recovered_window_seconds(slowest_tick: float, export: float) -> float:
    return max(2 * slowest_tick, detection_window_seconds(slowest_tick, export))


# How long before the Lease's expiry the holder's reconcilers have stopped. It
# stops acting at its renew deadline, the duration less RENEW_DEADLINE_MARGIN
# (12 s), after its last confirmed renewal, whose call was issued before the
# `renewTime` it wrote; its reconcilers then drain within
# SHUTDOWN_DRAIN_TIMEOUT (10 s), so its last tick completes 2 s before expiry.
# An export interval no longer than this carries that tick in a sample stamped
# before expiry, so a count that grows from the last such sample to the first
# after it is a tick that completed after expiry; the preflight refuses a
# longer interval, under which it could be a late export instead.
DRAIN_MARGIN_SECONDS = 12.0 - 10.0
# How far past the Lease's expiry the holder's process may still exit. Only
# bounded steps follow the drain: the release attempt (RELEASE_TIMEOUT, 5 s)
# and the shutdowns. So the exit is due by expiry + 13 s; a later exit is a
# holder that kept running.
HOLDER_EXIT_AFTER_EXPIRY_SECONDS = -DRAIN_MARGIN_SECONDS + 5.0 + SHUTDOWN_SECONDS
# The apiserver stores the Lease's `renewTime` and `acquireTime`, and the pod
# status its times, in whole seconds, rounded down: the instant each records
# can be up to this much later than it reads.
KUBE_TIME_RESOLUTION_SECONDS = 1.0

# Each apiserver cut, and each reconnection that ends the outage, is one node
# exec bounded by this, which applies its rules in one iptables transaction.
CUT_EXEC_SECONDS = 8.0
# The bound on each Lease read during the outage, its retries included: once
# the holder's container has ended, and just before the reconnection.
LEASE_READ_SECONDS = 5.0


# From the standby's cut, the latest instant the reconnection's Lease read may
# start. The holder's cut ends within the two cuts' execs, and the holder's
# last renewal precedes that. The read starts once the after-expiry window
# past that renewal's latest expiry has passed and once the holder's exit,
# due HOLDER_EXIT_AFTER_EXPIRY_SECONDS past it, has been read with the Lease
# after it; the holder's exit and that read are bounded by this too. On Kind
# 16 + 19 + max(19, 18) = 54 s.
def reconnection_read_seconds(slowest_tick: float, export: float) -> float:
    return (
        2 * CUT_EXEC_SECONDS + LEASE_TTL_SECONDS + KUBE_TIME_RESOLUTION_SECONDS
        + max(after_expiry_window_seconds(slowest_tick, export),
              HOLDER_EXIT_AFTER_EXPIRY_SECONDS + LEASE_READ_SECONDS)
    )


# The longest the standby can be cut off, from its cut to its reconnection:
# the reconnection's Lease read then follows, and the standby is reconnected
# first. A standby spends its budget from its first failed call, so this must
# stay below the budget's floor, or the standby could exit before the
# reconnection. On Kind 54 + 5 + 8 = 67 s, under 72 s; at a 60 s tick and a
# 2 s export, 117 s.
def outage_schedule_seconds(slowest_tick: float, export: float) -> float:
    return reconnection_read_seconds(slowest_tick, export) + LEASE_READ_SECONDS + CUT_EXEC_SECONDS


# Prometheus names of the OTLP metrics (`translation_strategy:
# UnderscoreEscapingWithSuffixes` in infra/space/observability/charts/metrics):
# `cluster_reconciler.loop_latency_ms` is the native histogram
# `space.cluster_reconciler.loop_latency` in milliseconds, and the
# leader-election counters gain `_total`.
RECONCILER_TICKS = "space_cluster_reconciler_loop_latency_milliseconds"
ACQUIRE_FAILED = "space_cluster_controller_leader_election_acquire_failed_total"
LEASE_LOST = "space_cluster_controller_leader_election_lease_lost_total"
LEASE_UNREACHABLE = "space_cluster_controller_leader_election_lease_unreachable_total"
# The budget a contender resolved, reported once at its startup.
LIVENESS_BUDGET = "space_cluster_controller_leader_election_liveness_budget_seconds"
# Evidence only: at the 18 s TTL a renewal stalled past about 4 s resigns the
# holder, and a resignation inside a judged window fails the run. The counter
# names that cause beside the window's own verdict.
RENEW_FAILED = "space_cluster_controller_leader_election_renew_failed_total"

# Cleanup's time plan after a failure. Its budget is shared, min(120 s, 10 %
# of the scenario timeout), so 120 s at the 30-minute default, and its
# compensations run in this order. The reconnection: one node exec per healthy
# replica, each within CLEANUP_REQUEST_SECONDS (8 s in adapters.py), 16 s.
# Each node's container exit watch stops next, normally at once; only a watch
# that ignores its closed stdin and its signals runs to the waits
# `CtrExitWatch.stop` bounds itself by. The budget's restore: two tries of an
# ownership read and the env write, 32 s. The two contenders' deletions, 8 s
# each, and the mutation Lease's release, a read and a patch, 16 s. That is
# 80 s without the watches' stops, which leaves the fixture's own
# compensations up to 40 s.
LOG_TAIL_LINES = 40

T = TypeVar("T")


class LeaderElectionAssertions:
    """Drive the faults and judge the controllers from state."""

    def __init__(
        self, fixture: KindAdmin, control: LeaderElectionControl, scope: ResourceScope
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._healthy: dict[str, ControllerPod] = {}
        self._contender: ControllerPod | None = None
        self._budget_contender: ControllerPod | None = None
        self._remove_budget_contender: Compensation | None = None
        self._remove_contender: Compensation | None = None
        self._reconnect: Compensation | None = None
        self._restore_budget: Compensation | None = None
        # The slowest histogram-recording loop's tick and the controller's
        # metric export interval, read in the preflight before any use.
        self._slowest_tick: float
        self._export: float
        self._standing_budget: str | None = None
        # The `kubernetes` Service's targets, read and checked in the preflight.
        self._targets: list[str] = []
        # Each healthy pod's cut, as the exec that made it started and ended.
        self._cuts: dict[str, tuple[float, float]] = {}
        # When the standby's cut began: every outage bound runs from here.
        self._outage_started = 0.0
        # When the reconnection was requested: no Lease can be acquired before it.
        self._restore_requested = 0.0
        # The Lease as the cut began, and as the holder's last renewal left it.
        self._lease_before_outage: ControllerLease | None = None
        self._last_renewal: ControllerLease | None = None
        # The initial check's Lease and the instant its runner window may open.
        self._initial: tuple[ControllerLease, float] | None = None
        # Pod name -> node, for reading container state and logs on the node.
        self._nodes: dict[str, str] = {}
        # (pod, container id) that held the readiness marker before the outage.
        self._markers: list[tuple[str, str]] = []
        # Pod name -> the container ids running when the outage began.
        self._running_before: dict[str, set[str]] = {}
        # The healthy pods as the outage began, and their (name, node) in the
        # order they are cut and reconnected: the standby first.
        self._pods_at_cut: dict[str, ControllerPod] = {}
        self._cut_order: list[tuple[str, str]] = []
        # When the holder's container that ran into the outage ended.
        self._holder_ended_at: float | None = None
        # Pod name -> every container id the node's runtime has listed for it.
        self._listed: dict[str, set[str]] = {}
        # Node -> its containerd's exit events, recorded from the contender's
        # release to the end of the run.
        self._exit_watches: dict[str, NodeExitWatch] = {}

    # --- setup ----------------------------------------------------------------

    def take_ownership(self) -> None:
        """Refuse a bench this cannot judge, hold the Kind mutation Lease, then read its timings.

        The tick and export settings, and the apiserver targets, are read
        under the Lease, which every run that writes the controller's
        Deployment holds, so none can change them afterwards.
        """

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
        self._slowest_tick = slowest_tick_seconds(self._control.controller_reconciler_tick_env())
        self._export = self._control.controller_metric_export_interval_seconds()
        tick, export = self._slowest_tick, self._export
        self._fixture.record("slowest_reconciler_tick_seconds", tick)
        self._fixture.record("metric_export_interval_seconds", export)
        self._fixture.record(
            "window_seconds",
            {"spare": window_spare_seconds(export),
             "detection": detection_window_seconds(tick, export),
             "after_expiry": after_expiry_window_seconds(tick, export),
             "recovered": recovered_window_seconds(tick, export)},
        )
        schedule = outage_schedule_seconds(tick, export)
        self._fixture.record("outage_schedule_seconds", schedule)
        if export > DRAIN_MARGIN_SECONDS:
            raise SmokeConfigurationError(
                f"a {export:g}s metric export can stamp a tick the holder completed before its "
                f"Lease expired after the expiry, so the after-expiry check could not tell it "
                f"from a late tick; it needs an export of at most {DRAIN_MARGIN_SECONDS:g}s"
            )
        if schedule >= LIVENESS_BUDGET_SECONDS:
            raise SmokeConfigurationError(
                f"at a {tick:g}s reconciler tick and a {export:g}s metric export, the outage can "
                f"cut the standby off for {schedule:g}s, which does not fit below its "
                f"{LIVENESS_BUDGET_SECONDS}s minimum liveness budget"
            )
        # A target the cut cannot cover is refused here, before any change.
        self._targets = self._control.apiserver_cut_targets()
        self._fixture.record("apiserver_cut_targets", self._targets)
        # Contenders and cuts a killed run left behind.
        self._control.remove_refused_contender()
        self._control.remove_budget_contender(wait=True)
        # A pod not yet on a node has no sandbox to hold a cut.
        for pod in self._control.controller_pods():
            if pod.node:
                self._control.reconnect_controller_to_apiserver(pod.node, pod.name)

    def start_contenders(self) -> None:
        """Start the budget control and read its start, then place the refused contender at its gate.

        Both get this run's budget and TTL hooks directly, so the control's
        budget runs while the healthy replicas roll. Their removals are armed
        before their pods exist, since deletion is idempotent. First, an
        image that does not report its resolved budget, which every wait on a
        contender is sized by, stops the run inconclusive.
        """

        self._expect_budget_reported()
        self._remove_contender = self._scope.defer(
            "remove the refused leader-election contender",
            lambda: self._control.remove_refused_contender(wait=False),
            critical=True,
        )
        self._remove_budget_contender = self._scope.defer(
            "remove the unreachable leader-election budget control",
            self._control.remove_budget_contender,
            critical=True,
        )
        timings = {"liveness_budget": str(LIVENESS_BUDGET_SECONDS), "lease_ttl": str(LEASE_TTL_SECONDS)}
        self._control.place_budget_contender(**timings)
        # Its start is read now, before the gate and rollout waits: it comes
        # from the container's running state, which its budget exit ends.
        control = self._poll(
            "the unreachable budget control to start",
            lambda: self._control.controller_pods(budget_contender=True),
            lambda pods: len(pods) == 1 and pods[0].started_at is not None,
            timeout=120,
            tolerate_errors=True,
        )[0]
        self._budget_contender = control
        self._nodes[control.name] = control.node
        self._control.place_refused_contender(**timings)
        inherited = self._control.controller_liveness_budget_env(refused_contender=True)
        self._fixture.verify(
            inherited == str(LIVENESS_BUDGET_SECONDS),
            "the refused contender does not carry this run's liveness budget",
            inherited=inherited,
        )

    def shrink_liveness_budget(self) -> None:
        """Request the floor and capture the standing budget for restoration."""

        standing = self._control.controller_liveness_budget_env()
        self._standing_budget = standing
        self._fixture.record("liveness_budget_env_before", standing)
        # Cleanup runs this after the reconnection and before the contenders'
        # deletions. It does not wait, so after a failure it fits the shared
        # cleanup budget; it is armed before the write, so a set that
        # half-applies is still put back.
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

    def restore_liveness_budget(self) -> None:
        """Put the standing budget hook back and check it took; the rollout is awaited later."""

        if self._restore_budget is None:
            return
        standing = self._standing_budget
        self._control.restore_controller_liveness_budget_env(standing, wait=False)
        after = self._control.controller_liveness_budget_env()
        self._fixture.verify(
            after == standing,
            "the controller's liveness-budget env did not come back to its standing value",
            standing=standing,
            after=after,
        )
        self._fixture.record("liveness_budget_env_after", after)
        self._fixture.record("renewal_failures", self._renewal_failures())

    def await_restored_budget(self) -> None:
        """Wait for the restore's rollout, then let cleanup forget it."""

        if self._restore_budget is None:
            return
        self._control.await_controller_rollout()
        self._restore_budget.disarm()
        self._restore_budget = None

    def start_refused_contender(self) -> None:
        """Release the refused contender after a healthy replica renews through the cluster.

        A renewal newer than the initial check's proves the Service path is
        up before refusals are observed, so connection failures cannot
        consume the contender's liveness budget first. Every controller node's
        containerd exit events are recorded from here on.
        """

        assert self._initial is not None
        before = self._initial[0]
        self._poll(
            "a healthy replica to renew the Lease through the cluster",
            self._control.controller_lease,
            lambda lease: lease is not None and lease.renew_time > before.renew_time,
            timeout=60,
            tolerate_errors=True,
        )
        nodes = {pod.node for pod in self._healthy.values()}
        nodes.update(pod.node for pod in self._control.controller_pods(refused_contender=True))
        for node in sorted(nodes):
            exits = self._control.watch_node_container_exits(node)
            self._scope.defer(f"stop the container exit watch on {node}", exits.stop)
            self._exit_watches[node] = exits
        released_at = time.time()
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
            and contender.started_at + KUBE_TIME_RESOLUTION_SECONDS >= released_at,
            "the refused contender's controller started before its release",
            contender_started_at=contender.started_at,
            released_at=released_at,
            contender_restarts=contender.restart_count,
        )
        self._contender = contender
        self._nodes[contender.name] = contender.node
        # Its first controller container, so an exit the node reports for it
        # is read even once kubelet has removed it.
        self._containers(contender.name)
        self._fixture.record(
            "refused_contender", {"pod": contender.name, "started_at": contender.started_at}
        )

    # --- one leader, one runner -------------------------------------------------

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

    def expect_initial_runner(self) -> str:
        """Before the outage, only the holder reconciles, for one tick of every loop.

        The outage's later check that the holder's ticks stop is only as good
        as this proof that it was ticking.
        """

        assert self._initial is not None
        lease, since = self._initial
        self._expect_only_runner(
            lease, label="initial", since=since,
            window=detection_window_seconds(self._slowest_tick, self._export),
        )
        return lease.holder

    def expect_recovered_leader(self) -> str:
        """One fresh holder and one runner, judged from the reconnection on.

        Nothing could hold the Lease before the reconnection, so the window
        may open there, and the outage's record checks that ran since count
        toward it.
        """

        since = self._restore_requested
        lease = self.expect_a_holder(label="recovered", since=since, timeout=300)
        # A replica starts its reconcilers once it holds the Lease again,
        # which the first renewal seen since the reconnection bounds, and a
        # holder that kept its name can keep its old `acquireTime`. So the
        # window also lasts a detection window past that renewal: a second
        # runner that started with the holder is still seen.
        elected = lease.renew_time + KUBE_TIME_RESOLUTION_SECONDS
        window = max(
            recovered_window_seconds(self._slowest_tick, self._export),
            elected + detection_window_seconds(self._slowest_tick, self._export) - since,
        )
        self._expect_only_runner(lease, label="recovered", since=since, window=window)
        return lease.holder

    def expect_a_holder(self, *, label: str, since: float, timeout: float = 180) -> ControllerLease:
        """Two Ready replicas and a holder among them that renewed since `since`."""

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
        window: float,
    ) -> None:
        """Only the holder's reconciler tick count moves in a window opened now.

        The window opens at the latest of `since`, the holder's current
        container start and its acquisition. Each series' count at that
        instant is its baseline, so a series that first appears inside the
        window counts from zero rather than from its first sample. It stays
        open for `window`, and until the holder's count has moved; any other
        pod's count moving fails at once.
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
        # Neither fault control may become a second reconciler runner.
        contenders = (self._contender, self._budget_contender)
        pods = sorted({*self._healthy, *(pod.name for pod in contenders if pod is not None)})
        selector = f'{{k8s_pod_name=~"{"|".join(pods)}"}}'
        # The sample stamped last before `opened` reaches Prometheus within the
        # Collector's batch; an export interval and a poll cover it.
        settled = opened + self._export + POLL_SECONDS
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
        holder_moved_at: float | None = None
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
                if runners and holder_moved_at is None:
                    holder_moved_at = now
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
        assert holder_moved_at is not None
        self._fixture.record(
            f"{label}_reconciler_window",
            {"opened": opened, "window_seconds": window, "seconds": round(time.time() - opened, 1),
             # How long after the window opened the holder's move was first
             # read, and how much of the window remained: a second runner
             # ticking as late as the holder did would still have been seen.
             "holder_seen_after_seconds": round(holder_moved_at - opened, 1),
             "spare_seconds": round(closes - holder_moved_at, 1)},
        )

    # --- a refused lease call is not a restart ---------------------------------

    def expect_refusal_is_not_a_restart(self) -> None:
        """The contender is refused, and stays NotReady and unrestarted past its budget."""

        contender = self._contender
        assert contender is not None and contender.started_at is not None
        started = contender.started_at - KUBE_TIME_RESOLUTION_SECONDS

        # The contender's name is fixed, so a series an earlier run left can
        # still answer an instant query; only a count sampled since this
        # container started is this contender's.
        first_refused = self._poll(
            "the contender's first refused lease call to reach Prometheus",
            lambda: self._first_counted(
                f'{ACQUIRE_FAILED}{{reason="refused", k8s_pod_name="{contender.name}"}}',
                since=started,
            ),
            lambda at: at is not None,
            timeout=120,
            tolerate_errors=True,
        )
        assert first_refused is not None
        budget = self._reported_budget(contender.name, since=started)

        # Its name is fixed, so so is its spread: the watch is anchored on its
        # own first refusal and sized by the budget it reports. A budget that
        # charged refusals would start no later than the sample showing the
        # first one, end the process on the attempt that crosses it, and the
        # exit would follow its shutdowns; containerd reports it at once, and
        # the watch reads every poll.
        until = (
            first_refused + budget + RETRY_INTERVAL_SECONDS + SHUTDOWN_SECONDS
            + window_spare_seconds(self._export)
        )
        self._fixture.record(
            "contender_refusals",
            {"first_after_start_seconds": round(first_refused - contender.started_at, 1),
             "reported_budget_seconds": budget, "watched_until": until,
             "watch_seconds_from_first_refusal": round(until - first_refused, 1)},
        )
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
            # Two detectors of a restart: containerd's record of the exit,
            # and kubelet's count of the restart that follows it.
            exits = self._exits(contender.name)
            self._fixture.verify(
                not exits,
                "the refused contender's controller container exited on a refusal a restart "
                "cannot fix",
                failure_evidence=self._log_evidence,
                exits=[(c.container_id[:12], c.exit_code, c.finished_at) for c in exits],
            )
            self._fixture.verify(
                current.restart_count == 0,
                f"the refused contender restarted ({current.restart_count}) on a refusal "
                "a restart cannot fix",
                failure_evidence=self._log_evidence,
                contender_last_exit_code=current.last_exit_code,
            )
            # Ready now, or Ready at any time since its controller started:
            # the condition's last change would then postdate the start.
            self._fixture.verify(
                not current.ready
                and current.ready_changed_at is not None
                and current.ready_changed_at <= contender.started_at,
                "the refused contender was Ready although it cannot reach the Lease",
                failure_evidence=self._log_evidence,
                ready=current.ready,
                ready_changed_at=current.ready_changed_at,
                started_at=contender.started_at,
            )
            if now >= until:
                break
            time.sleep(min(POLL_SECONDS, max(until - now, 0.1)))
        ticked = self._poll(
            "Prometheus to answer for the refused contender's ticks",
            lambda: self._tick_samples(contender.name, after=started),
            lambda _: True,
            timeout=60,
            tolerate_errors=True,
        )
        self._fixture.verify(
            not ticked,
            f"the refused contender ran reconcilers: {ticked}",
            failure_evidence=self._log_evidence,
        )
        self._fixture.record("contender_watched_seconds", round(time.time() - contender.started_at))

    def expect_budget_control_exited(self) -> None:
        """An actual unreachable Lease call spends the budget the control reports."""

        contender = self._budget_contender
        assert contender is not None and contender.started_at is not None
        started = contender.started_at - KUBE_TIME_RESOLUTION_SECONDS
        first_failed = self._poll(
            "the budget control's first unreachable Lease call to reach Prometheus",
            lambda: self._first_counted(
                f'{ACQUIRE_FAILED}{{reason="unreachable", k8s_pod_name="{contender.name}"}}',
                since=started,
            ),
            lambda at: at is not None,
            timeout=120,
            tolerate_errors=True,
        )
        assert first_failed is not None
        budget = self._reported_budget(contender.name, since=started)
        # The budget starts at the first charged call, which its sample
        # trails by an export interval, or by the window spare on a loaded
        # node, and ends on the attempt that crosses it, a retry later at
        # most, then the shutdowns. Both timestamps are the process's own; a
        # startup failure cannot stand in for the election's
        # lease_unreachable counter and budget exit.
        earliest = (
            first_failed - self._export - window_spare_seconds(self._export)
            - KUBE_TIME_RESOLUTION_SECONDS + budget
        )
        latest = first_failed + budget + RETRY_INTERVAL_SECONDS + SHUTDOWN_SECONDS
        pods = self._poll(
            "the unreachable budget control to exit non-zero",
            lambda: self._control.controller_pods(budget_contender=True),
            lambda pods: len(pods) == 1 and pods[0].last_finished_at is not None,
            timeout=max(1, latest + 15 - time.time()),
            tolerate_errors=True,
        )
        ended = pods[0]
        self._fixture.verify(
            ended.uid == contender.uid
            and ended.restart_count == 0
            and ended.last_exit_code not in (None, 0)
            and ended.last_finished_at is not None
            and earliest <= ended.last_finished_at <= latest + KUBE_TIME_RESOLUTION_SECONDS,
            "the budget control did not exit non-zero within its liveness budget",
            first_unreachable_at=first_failed,
            reported_budget_seconds=budget,
            earliest_exit=earliest,
            latest_exit=latest,
            exit_code=ended.last_exit_code,
            exited_at=ended.last_finished_at,
            failure_evidence=self._log_evidence,
        )
        spent = self._poll(
            "the budget control's lease_unreachable counter to reach Prometheus",
            lambda: self._first_counted(
                f'{LEASE_UNREACHABLE}{{identity="{contender.name}", reason="unreachable"}}',
                since=started,
            ),
            lambda at: at is not None,
            timeout=2 * self._export + 30,
            tolerate_errors=True,
        )
        self._fixture.record("budget_control", {
            "pod": contender.name, "first_unreachable_at": first_failed,
            "reported_budget_seconds": budget, "budget_spent_at": spent,
            "exited_at": ended.last_finished_at, "exit_code": ended.last_exit_code,
            "exit_after_first_unreachable_seconds": round((ended.last_finished_at or 0) - first_failed, 1),
        })

    def remove_contenders(self) -> None:
        """Delete both contenders and wait for the refused one to be gone."""

        self._control.remove_refused_contender()
        if self._remove_contender is not None:
            self._remove_contender.disarm()
            self._remove_contender = None
        if self._remove_budget_contender is not None:
            self._remove_budget_contender.release()
            self._remove_budget_contender = None
        self._contender = None

    def _expect_budget_reported(self) -> None:
        """Stop inconclusive unless the controller image reports the budget each replica resolved.

        Read from the healthy replicas, which run the image the contenders
        are placed from and report it as their campaigns start; the budget
        control reports its own only once its substrate's startup wait has
        ended, about half a minute after it starts.
        """

        pods = [pod for pod in self._control.controller_pods() if pod.started_at is not None]

        def reporting() -> list[str]:
            return [
                pod.name for pod in pods
                if self._latest_since(
                    f'{LIVENESS_BUDGET}{{identity="{pod.name}"}}',
                    since=(pod.started_at or 0.0) - KUBE_TIME_RESOLUTION_SECONDS,
                ) is not None
            ]

        self._poll(
            "a healthy replica's resolved liveness budget to reach Prometheus",
            reporting,
            bool,
            timeout=2 * self._export + 60,
            tolerate_errors=True,
            inconclusive=f"the controller image reports no {LIVENESS_BUDGET}",
        )

    def _reported_budget(self, pod: str, *, since: float) -> float:
        """The liveness budget the pod's own process resolved, from its gauge.

        Refused unless the requested floor can produce it, so a binary that
        ignored the hook fails here rather than stretching a watch to its
        shipped budget.
        """

        budget = self._poll(
            f"{pod}'s resolved liveness budget to reach Prometheus",
            lambda: self._latest_since(f'{LIVENESS_BUDGET}{{identity="{pod}"}}', since=since),
            lambda value: value is not None,
            timeout=2 * self._export + 60,
            tolerate_errors=True,
        )
        assert budget is not None
        self._fixture.verify(
            LIVENESS_BUDGET_SECONDS <= budget <= LIVENESS_BUDGET_SECONDS * LIVENESS_BUDGET_SPREAD,
            f"{pod} resolved a {budget}s liveness budget, which the requested "
            f"{LIVENESS_BUDGET_SECONDS}s floor cannot produce",
            failure_evidence=self._log_evidence,
        )
        return budget

    # --- the outage ------------------------------------------------------------------

    def cut_off_the_healthy_pair(self) -> None:
        """Record the marker holders, then cut both healthy replicas off from the apiserver."""

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
        before = self._control.controller_lease()
        self._fixture.verify(
            before is not None and before.holder in self._healthy,
            "the Lease holder is not a healthy replica as the outage begins",
            lease_holder=before.holder if before else None,
        )
        assert before is not None
        self._lease_before_outage = before
        # Each one's current container, whose samples alone the outage's
        # counts are read from.
        current = {pod.name: pod for pod in self._control.controller_pods()}
        # The outage's restart checks are measured from these pods, so one
        # that restarted since the initial holder was seen would pass them.
        self._fixture.verify(
            all(
                name in current
                and current[name].uid == pod.uid
                and current[name].restart_count == pod.restart_count
                for name, pod in self._healthy.items()
            ),
            "a healthy replica restarted before the outage",
            failure_evidence=self._log_evidence,
            healthy_at_initial_holder={name: {"uid": pod.uid, "restart_count": pod.restart_count}
                                       for name, pod in self._healthy.items()},
            healthy_before_cut={name: {"uid": current[name].uid, "restart_count": current[name].restart_count}
                                for name in self._healthy if name in current},
        )
        self._fixture.verify(
            all(name in current and current[name].started_at is not None for name in self._healthy),
            "a healthy replica has no running controller container as the outage begins",
            pods=sorted(current),
        )
        self._pods_at_cut = {name: current[name] for name in self._healthy}

        # The standby is cut first: a cut that stalls then only lets the holder
        # keep renewing, where the other order would let the standby take the
        # Lease once it expired. It is reconnected first too, so its outage
        # stays within `outage_schedule_seconds`.
        self._cut_order = sorted(
            ((name, pod.node) for name, pod in self._healthy.items()),
            key=lambda item: (item[0] == before.holder, item[0]),
        )
        pods = dict(self._cut_order)

        def reconnect() -> None:
            # Each bounded by the cleanup's request bound; every pod is tried.
            failures = []
            for name, node in pods.items():
                try:
                    self._control.reconnect_controller_to_apiserver(node, name)
                except Exception as error:  # noqa: BLE001 - the next pod still gets its turn
                    failures.append(f"{name}: {type(error).__name__}: {error}")
            if failures:
                raise RuntimeError("; ".join(failures))

        # Armed before the first rule: a cut whose reply is lost may still
        # have been made. Deferred after every other compensation, so cleanup
        # runs it first.
        self._reconnect = self._scope.defer(
            "reconnect the cluster-controllers to the apiserver", reconnect, critical=True
        )
        self._cuts = self._control.cut_controllers_off_from_apiserver(
            list(pods), self._targets, timeout=CUT_EXEC_SECONDS
        )
        standby = self._cut_order[0][0]
        self._outage_started = self._cuts[standby][0]
        self._fixture.record(
            "outage", {"started_at": self._outage_started,
                       "cut_seconds": {name: round(ended - started, 1)
                                       for name, (started, ended) in self._cuts.items()}},
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
            timeout=60,
        )
        self._fixture.record("readiness_marker_cleared", pods)

    def expect_holder_stops(self) -> None:
        """The holder's container ends before its Lease could be taken over."""

        before = self._lease_before_outage
        assert before is not None
        holder = before.holder
        running = self._running_before[holder]

        def ended() -> list[NodeContainer]:
            return [container for container in self._exits(holder) if container.container_id in running]

        # Its last renewal was issued before its cut ended, so its exit is due
        # by then plus the Lease's duration and HOLDER_EXIT_AFTER_EXPIRY_SECONDS;
        # read a little past that, so a late exit is measured rather than
        # timed out on, but no later than leaves time for the Lease read
        # before the reconnection's.
        latest_due = (
            self._cuts[holder][1] + before.duration_seconds + KUBE_TIME_RESOLUTION_SECONDS
            + HOLDER_EXIT_AFTER_EXPIRY_SECONDS
        )
        read_by = self._reconnection_read_due()
        [container] = self._poll(
            f"the holder {holder}'s container to end",
            ended,
            bool,
            timeout=min(latest_due + 15, read_by - LEASE_READ_SECONDS) - time.time(),
        )[:1]
        # Nothing could write the Lease since the cut: its holder's process
        # has ended, its standby is cut off, the contender is refused and the
        # control cannot reach the apiserver. So this read is its last renewal.
        # It ends by the instant the reconnection's read must start, so the
        # standby's outage keeps to its schedule; a read that cannot end the
        # outage in time judges nothing.
        try:
            lease = self._lease_within(min(LEASE_READ_SECONDS, read_by - time.time()))
        except _LeaseUnread as error:
            self._reconnect_pair()
            raise SmokeConfigurationError(
                f"could not read the Lease after the holder's exit in the time the standby's "
                f"outage leaves ({error}); reconnected both replicas"
            ) from None
        self._fixture.verify(
            lease is not None and lease.holder == holder and lease.renew_time >= before.renew_time,
            f"the Lease changed hands from {holder} while no replica could reach it",
            lease_holder=lease.holder if lease else None,
        )
        assert lease is not None
        self._last_renewal = lease
        expiry = _latest_expiry(lease)
        due = expiry + HOLDER_EXIT_AFTER_EXPIRY_SECONDS
        finished = container.finished_at or 0.0
        self._holder_ended_at = container.finished_at
        cut_ended = self._cuts[holder][1]
        self._fixture.record(
            "holder_exit",
            {"exit_code": container.exit_code, "finished_at": finished,
             "last_renewal": lease.renew_time,
             "after_renewal_seconds": round(finished - lease.renew_time, 1),
             "renewal_after_cut_seconds": round(lease.renew_time - self._cuts[holder][0], 1),
             "renewal_before_cut_end_seconds": round(cut_ended - lease.renew_time, 1)},
        )
        # The holder stamps `renewTime` from its own clock before it sends the
        # write (`renewed_spec`, then `replace`, in the controller's
        # leader_election/store.rs), and the cut rejects every packet sent
        # after its transaction commits, on open connections too. So a renewal
        # the cut let through, one already in flight when it landed included,
        # is stamped before that commit, which precedes the end of the cut's
        # exec; on Kind the holder and this run read the same clock. The
        # stamp's whole second is all that is allowed past that end: a later
        # one is a renewal the holder made once its cut was in place, and the
        # exit and tick checks below would be measured from it.
        self._fixture.verify(
            lease.renew_time <= cut_ended + KUBE_TIME_RESOLUTION_SECONDS,
            f"the holder renewed its Lease after its cut was in place: its last renewal is "
            f"stamped {lease.renew_time - cut_ended:.1f}s after its cut's exec ended, past "
            f"the {KUBE_TIME_RESOLUTION_SECONDS:.0f}s the stamp's resolution allows",
            failure_evidence=self._log_evidence,
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

    def end_the_outage(self) -> None:
        """Reconnect the healthy pair once the after-expiry window could show a tick.

        The Lease must still be the holder's last renewal, and the standby's
        container must have run throughout.
        """

        lease = self._last_renewal
        assert lease is not None and self._reconnect is not None
        # The window ends within the schedule, since the renewal preceded the
        # holder's cut; the bound is kept even if it did not.
        ends = _latest_expiry(lease) + after_expiry_window_seconds(self._slowest_tick, self._export)
        time.sleep(max(min(ends, self._reconnection_read_due()) - time.time(), 0))
        # Nothing could write the Lease since the holder's exit unless a cut
        # let a replica through, and a standby that got through takes the
        # Lease once it has expired, before this.
        try:
            current = self._lease_within(LEASE_READ_SECONDS)
        except _LeaseUnread as error:
            self._fixture.verify(
                False,
                f"could not read the Lease before the reconnection: {error}",
                failure_evidence=self._log_evidence,
            )
            raise
        self._fixture.verify(
            current is not None and current.holder == lease.holder
            and current.renew_time == lease.renew_time,
            "the Lease changed between the holder's exit and the reconnection, so a cut did not hold",
            failure_evidence=self._log_evidence,
            last_renewal={"holder": lease.holder, "renew_time": lease.renew_time},
            before_reconnection={"holder": current.holder, "renew_time": current.renew_time}
            if current else None,
        )
        self._restore_requested = time.time()
        standby_reconnected = self._reconnect_pair()
        self._fixture.record(
            "outage_ended", {"at": self._restore_requested,
                             "after_last_renewal_seconds": round(self._restore_requested - lease.renew_time, 1),
                             # From the standby's cut to its reconnection.
                             "outage_seconds": round(standby_reconnected - self._outage_started, 1),
                             "schedule_seconds": outage_schedule_seconds(self._slowest_tick, self._export)},
        )
        # A standby that spent its budget during the cut has exited: the
        # container that ran into the outage must still be running.
        [standby] = [name for name in self._healthy if name != lease.holder]
        running = self._poll(
            f"the standby {standby}'s containers after the reconnection",
            lambda: {container.container_id for container in self._containers(standby) if container.running},
            lambda _: True,
            timeout=30,
            tolerate_errors=True,
        )
        self._fixture.verify(
            self._running_before[standby] <= running,
            f"the standby {standby}'s container did not run through the outage",
            failure_evidence=self._log_evidence,
            running_before=sorted(c[:12] for c in self._running_before[standby]),
            running_after=sorted(c[:12] for c in running),
        )

    def expect_outage_recorded(self) -> None:
        """kubelet reports the holder's restart, and Prometheus holds why it stopped and that it did.

        Prometheus also holds the cut-off standby's unreachable Lease calls.
        """

        lease = self._last_renewal
        assert lease is not None
        holder = lease.holder
        [standby] = [name for name in self._healthy if name != holder]
        before = self._pods_at_cut[holder].restart_count
        pods = self._poll(
            "kubelet to report the holder's restart",
            lambda: {pod.name: pod for pod in self._control.controller_pods()},
            lambda pods: holder in pods and pods[holder].restart_count > before,
            timeout=60,
            tolerate_errors=True,
        )
        self._fixture.record(
            "holder_restarts", {"before": before, "after": pods[holder].restart_count,
                                "last_exit_code": pods[holder].last_exit_code},
        )
        standby_at_cut = self._pods_at_cut[standby]
        self._fixture.verify(
            standby in pods
            and pods[standby].uid == standby_at_cut.uid
            and pods[standby].restart_count == standby_at_cut.restart_count,
            f"kubelet restarted the standby {standby} across the outage",
            failure_evidence=self._log_evidence,
            restarts_at_cut=standby_at_cut.restart_count,
            restarts_now=pods[standby].restart_count if standby in pods else None,
        )

        lost = self._poll(
            "the holder's lease_lost to reach Prometheus",
            lambda: self._first_counted(f'{LEASE_LOST}{{identity="{holder}"}}', since=self._outage_started),
            lambda at: at is not None,
            timeout=60,
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

        # The standby stays up and exporting throughout, so its Lease calls
        # through the cut `kubernetes` Service must be counted as unreachable,
        # the category the budget charges (the control shows the budget's
        # exit). Its count at the cut and at the reconnection, from its
        # current container's samples only: a restarted pod reuses its series,
        # and an earlier process's last count would stand in for one this
        # process never made.
        assert standby_at_cut.started_at is not None
        started = standby_at_cut.started_at - KUBE_TIME_RESOLUTION_SECONDS
        selector = f'{ACQUIRE_FAILED}{{reason="unreachable", k8s_pod_name="{standby}"}}'
        # Counted once its cut had certainly taken hold, as its exec ended.
        cut = self._cuts[standby][1]
        at_cut = self._poll(
            f"Prometheus to answer for the standby {standby}'s unreachable Lease calls at the cut",
            lambda: self._latest_at(selector, since=started, at=cut),
            lambda _: True,
            timeout=60,
            tolerate_errors=True,
        )
        at_reconnection = self._poll(
            f"the standby {standby}'s unreachable Lease calls during the cut to reach Prometheus",
            lambda: self._latest_at(selector, since=started, at=self._restore_requested),
            lambda count: count > at_cut,
            timeout=2 * self._export + 30,
            tolerate_errors=True,
        )
        self._fixture.record(
            "standby_unreachable_calls",
            {"pod": standby, "at_cut": at_cut, "at_reconnection": at_reconnection},
        )

        # A count that grew from the holder process's last sample stamped
        # before expiry to its first after, or between two samples stamped
        # after it, is a tick that finished after a standby could have taken
        # over: the first only because the preflight held the export interval
        # to DRAIN_MARGIN_SECONDS, so every tick a correct holder completed
        # was exported before expiry. Only samples before the reconnection are judged: until then
        # nothing can hold the Lease, so a tick is the expired holder's, and
        # after it a restarted container with the holder's pod name may lead
        # again.
        cutoff = self._restore_requested
        holder_at_cut = self._pods_at_cut[holder]
        assert holder_at_cut.started_at is not None
        process = (
            holder_at_cut.started_at - KUBE_TIME_RESOLUTION_SECONDS,
            self._holder_ended_at if self._holder_ended_at is not None else cutoff,
        )
        late = self._poll(
            "Prometheus to answer for the holder's ticks after expiry",
            lambda: self._ticks_between(holder, after=expiry, until=cutoff, process=process),
            lambda _: True,
            timeout=60,
            tolerate_errors=True,
        )
        self._fixture.record(
            "holder_ticks_judged", {"from": expiry, "until": cutoff, "seconds": round(cutoff - expiry, 1)},
        )
        self._fixture.verify(
            late is None,
            f"the holder's reconciler ticks grew after its Lease expired and before the "
            f"controllers were reconnected: {late}",
            failure_evidence=self._log_evidence,
        )

    def _reconnection_read_due(self) -> float:
        """The latest instant the reconnection's Lease read may start, from the standby's cut."""

        return self._outage_started + reconnection_read_seconds(self._slowest_tick, self._export)

    def _reconnect_pair(self) -> float:
        """Reconnect the healthy pair, the standby first; when the standby's reconnection ended."""

        assert self._reconnect is not None
        standby_reconnected = 0.0
        for name, node in self._cut_order:
            self._control.reconnect_controller_to_apiserver(node, name, timeout=CUT_EXEC_SECONDS)
            standby_reconnected = standby_reconnected or time.time()
        self._reconnect.disarm()
        self._reconnect = None
        return standby_reconnected

    # --- reads -------------------------------------------------------------------

    def _lease_within(self, seconds: float) -> ControllerLease | None:
        """One read of the Lease, retried on a failed read, all within `seconds`.

        Raises `_LeaseUnread` when no read succeeds in time, or when less than
        a second is left for one.
        """

        if seconds < 1:
            raise _LeaseUnread(f"{max(seconds, 0):.1f}s left for the read")
        deadline = time.monotonic() + seconds
        while True:
            try:
                return self._control.controller_lease(timeout=max(deadline - time.monotonic(), 0.5))
            except Exception as error:  # noqa: BLE001 - kubectl can briefly fail
                failure = f"{type(error).__name__}: {error}"
            if time.monotonic() >= deadline - 0.5:
                raise _LeaseUnread(f"no read within {seconds:.1f}s: {failure[:2000]}")
            time.sleep(0.5)

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

    def _latest_at(self, selector: str, *, since: float, at: float) -> float:
        """A counter's sum over its series of each one's latest sample stamped in `[since, at]`.

        A series with no sample there counts 0, so a reused series' value
        from a process that started before `since` is never read.
        """

        now = time.time()
        window = int(now - since) + 60
        total = 0.0
        for series in self._prometheus(f"{selector}[{window}s]", at=now):
            stamped = [
                (float(stamp), float(raw))
                for stamp, raw in series.get("values") or []
                if since <= float(stamp) <= at
            ]
            if stamped:
                total += max(stamped)[1]
        return total

    def _ticks_between(
        self, pod: str, *, after: float, until: float, process: tuple[float, float]
    ) -> tuple[float, float] | None:
        """Two samples whose count grew past `after`, before `until`.

        Either two samples stamped in `[after, until)`, or the last sample
        stamped before `after` and the first stamped from it, both inside
        `process`, the span one container's process can have stamped: a
        process that ticks once past `after` and exits leaves only one sample
        past it. That crossing is a late tick, not a late export, only while
        each sample follows the last by at most the margin between `after`
        and the holder's last allowed tick (`DRAIN_MARGIN_SECONDS`). Its pod's
        next process counts from zero in the same series, so a sample outside
        the span is never compared with one inside it.
        """

        started, ended = process
        now = time.time()
        window = int(now - after) + 60
        for series in self._prometheus(f'{RECONCILER_TICKS}{{k8s_pod_name="{pod}"}}[{window}s]', at=now):
            stamped = sorted(
                (float(stamp), float(histogram["count"]))
                for stamp, histogram in series.get("histograms") or []
            )
            samples = [(stamp, count) for stamp, count in stamped if after <= stamp < until]
            own = [(stamp, count) for stamp, count in stamped if started <= stamp <= ended]
            baseline = [(stamp, count) for stamp, count in own if stamp < after]
            first = next(((stamp, count) for stamp, count in own if after <= stamp < until), None)
            if baseline and first is not None and first[1] > baseline[-1][1]:
                return baseline[-1][0], first[0]
            for (earlier, before), (later, count) in zip(samples, samples[1:]):
                if count > before:
                    return earlier, later
        return None

    def _tick_samples(self, pod: str, *, after: float) -> list[tuple[str, float, float]]:
        """Every tick sample stamped at or after `after` with a count: (loop, stamp, count)."""

        now = time.time()
        window = int(now - after) + 60
        return [
            (str(series["metric"].get("loop", "")), float(stamp), float(histogram["count"]))
            for series in self._prometheus(f'{RECONCILER_TICKS}{{k8s_pod_name="{pod}"}}[{window}s]', at=now)
            for stamp, histogram in series.get("histograms") or []
            if float(stamp) >= after and float(histogram["count"]) > 0
        ]

    def _first_counted(self, selector: str, *, since: float) -> float | None:
        """When a counter was first counted at or after `since`: its first non-zero sample there.

        A pod's process exports as `service_instance_id` its pod name and
        pid, and every controller runs as pid 1, so a pod name a run reuses
        (each contender's, a restarted holder's) reuses the series, and a
        counter that a process counts once, such as `lease_unreachable` at
        its exit, carries the same value its predecessor left. So no earlier
        sample is compared: every caller's `since` is no earlier than the
        start of the process it judges, or, for a holder's `lease_lost`, a
        point before which that process cannot have counted it, since losing
        the Lease ends the process. A counter shows no series until counted.
        """

        now = time.time()
        window = int(now - since) + 60
        first = None
        for series in self._prometheus(f"{selector}[{window}s]", at=now):
            for stamp, raw in series.get("values") or []:
                at = float(stamp)
                if at >= since and float(raw) > 0:
                    first = at if first is None else min(first, at)
                    break
        return first

    def _latest_since(self, selector: str, *, since: float) -> float | None:
        """A gauge's newest value among samples stamped at or after `since`."""

        now = time.time()
        window = int(now - since) + 60
        newest: tuple[float, float] | None = None
        for series in self._prometheus(f"{selector}[{window}s]", at=now):
            for stamp, raw in series.get("values") or []:
                if float(stamp) >= since and (newest is None or float(stamp) > newest[0]):
                    newest = (float(stamp), float(raw))
        return None if newest is None else newest[1]

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
        inconclusive: str | None = None,
    ) -> T:
        """Read until `done`, failing at the deadline with the last reading.

        `tolerate_errors` retries a read that raised, for reads through
        kubectl and the localhost forwards, which can briefly fail. With
        `inconclusive`, the deadline stops the run inconclusive instead, for
        that reason.
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
                if inconclusive is not None:
                    raise SmokeConfigurationError(
                        f"timed out after {timeout:.0f}s waiting for {what}: {inconclusive}; "
                        f"last observed {repr(observed)[:500]}"
                    )
                self._fixture.verify(
                    False,
                    f"timed out after {timeout:.0f}s waiting for {what}",
                    failure_evidence=self._log_evidence,
                    last_observed=repr(observed)[:2000],
                )
            time.sleep(POLL_SECONDS)


class _LeaseUnread(Exception):
    """No read of the Lease succeeded in the time given."""


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

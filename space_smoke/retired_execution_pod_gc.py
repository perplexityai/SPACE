"""The retired-execution-pod GC, proven against real retired pods on kind.

The cluster-controller reconciler `retired_execution_pod_gc`
(crates/l40_api/cluster/controller/server/src/retired_execution_pod_gc/)
deletes sandbox pods whose execution row is retired: a Succeeded or Failed pod
with every container terminated, and a Pending fresh-create pod whose execution
was retired as `replaced` before it ever had a pod IP. It ships default-off and
dry-run and reads its config once at startup, so the scenario scopes its
config to itself: the controller Deployment's `SPACE_RETIRED_EXECUTION_POD_GC_*`
env is captured, overridden, and put back, under the controller-mutation Lease
the other controller scenarios share.

The pods are real. A node-side access-idle suspend leaves its Succeeded pod
behind. A cold create whose sandbox-manager is killed mid-activation is retired
as `replaced` once node-ctl publishes the rolled-back row, and the kubelet's
retry of the activation is then refused, so the pod stays Pending with no IP.
When that create ends, the API's create cleanup deletes the pod; force-killing
the API first leaves it for the GC. The Pending-with-IP control is planted:
an unschedulable clone at a deleted pod's name, with a status pod IP.

The observables are state: the controller Deployment's env once rolled, each
pod seen present or gone, its uid, the execution rows, and the GC's
`cluster_reconciler.outcome` counter in the stack's Datadog intake (the only
place dry-run's would-reap verdict is visible). Log lines are never judged.
"""

from __future__ import annotations

import copy
import re
import secrets
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from space_smoke.capabilities import RetiredExecutionPodGcControl, SandboxPodExecution
from space_smoke.runtime import BackgroundWork, Compensation, ResourceScope, background

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

GC_ENABLED_ENV = "SPACE_RETIRED_EXECUTION_POD_GC_ENABLED"
GC_DRY_RUN_ENV = "SPACE_RETIRED_EXECUTION_POD_GC_DRY_RUN"
GC_INTERVAL_ENV = "SPACE_RETIRED_EXECUTION_POD_GC_INTERVAL_SECS"
GC_THRESHOLD_ENV = "SPACE_RETIRED_EXECUTION_POD_GC_THRESHOLD_SECS"
GC_TICK_BUDGET_ENV = "SPACE_RETIRED_EXECUTION_POD_GC_TICK_BUDGET_SECS"

# The only controller env this scenario may touch (`RetiredExecutionPodGcConfig::from_env`
# reads exactly these), and the closed vocabulary its overrides come from.
GC_ENV_KEYS: tuple[str, ...] = (
    GC_ENABLED_ENV,
    GC_DRY_RUN_ENV,
    GC_INTERVAL_ENV,
    GC_TICK_BUDGET_ENV,
    GC_THRESHOLD_ENV,
    "SPACE_RETIRED_EXECUTION_POD_GC_MAX_DELETES_PER_MINUTE",
    "SPACE_RETIRED_EXECUTION_POD_GC_BATCH_SIZE",
)
_BOOLEAN_KEYS = frozenset({GC_ENABLED_ENV, GC_DRY_RUN_ENV})
_COUNT_VALUE = re.compile(r"[1-9][0-9]{0,5}")

TERMINAL_RETIRED = "terminal_retired"
REPLACED_NEVER_STARTED = "replaced_never_started"
VERDICTS = (TERMINAL_RETIRED, REPLACED_NEVER_STARTED)
DRY_RUN = "dry_run"
LIVE = "live"

SANDBOX_ID_KEY = "space.pplx.net/sandbox-id"
EXECUTION_GENERATION_KEY = "space.pplx.net/execution-generation"
PLANTED_POD_LABEL = "space.pplx.net/smoke-planted-orphan"
UNSCHEDULABLE_SELECTOR_KEY = "space.pplx.net/smoke-no-such-node"
PLANTED_POD_IP = "192.0.2.10"
_PLANT_TOKEN = re.compile(r"[a-f0-9]{8}")


def validate_gc_keys(keys: Iterable[str]) -> None:
    unknown = sorted(set(keys) - set(GC_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing non-GC controller env keys: {unknown}")


def validate_gc_overrides(overrides: Mapping[str, str]) -> None:
    """Refuse any key off the GC list and any value off the smoke vocabulary."""

    validate_gc_keys(overrides)
    for key, value in overrides.items():
        valid = (
            value in {"true", "false"}
            if key in _BOOLEAN_KEYS
            else _COUNT_VALUE.fullmatch(value) is not None
        )
        if not valid:
            raise ValueError(f"refusing GC env value {value!r} for {key}")


def smoke_gc_env(*, dry_run: bool, threshold_seconds: int, interval_seconds: int) -> dict[str, str]:
    return {
        GC_ENABLED_ENV: "true",
        GC_DRY_RUN_ENV: "true" if dry_run else "false",
        GC_INTERVAL_ENV: str(interval_seconds),
        GC_THRESHOLD_ENV: str(threshold_seconds),
        GC_TICK_BUDGET_ENV: str(max(2, interval_seconds - 1)),
    }


def would_reap(pod: SandboxPodExecution) -> str | None:
    """The reaper's verdict on one pod and its execution row (`classify.rs`), or None to skip."""

    if pod.deleting or pod.row_state is None or pod.row_active:
        return None
    if pod.row_sandbox_id != pod.sandbox_id or pod.row_generation != pod.generation:
        return None
    if pod.phase in {"Succeeded", "Failed"} and pod.all_containers_terminated:
        return TERMINAL_RETIRED
    if (
        pod.phase == "Pending"
        and pod.row_state == "replaced"
        and not pod.is_restore
        and not pod.pod_ip
        and pod.sandbox_not_ready
    ):
        return REPLACED_NEVER_STARTED
    return None


def build_planted_pod(
    template: Mapping[str, Any], *, name: str, sandbox_id: str, generation: int, token: str
) -> dict[str, Any]:
    """An unschedulable clone of a real fresh-create pod at a chosen name and identity."""

    if generation <= 0 or _PLANT_TOKEN.fullmatch(token) is None:
        raise ValueError(f"refusing plant generation {generation} or token {token!r}")
    metadata = template.get("metadata") or {}
    annotations = dict(metadata.get("annotations") or {})
    labels = dict(metadata.get("labels") or {})
    for keys in (annotations, labels):
        keys[SANDBOX_ID_KEY] = sandbox_id
        keys[EXECUTION_GENERATION_KEY] = str(generation)
    labels[PLANTED_POD_LABEL] = token
    spec = copy.deepcopy(dict(template.get("spec") or {}))
    spec.pop("nodeName", None)
    spec["nodeSelector"] = {**(spec.get("nodeSelector") or {}), UNSCHEDULABLE_SELECTOR_KEY: token}
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": labels, "annotations": annotations},
        "spec": spec,
    }


@dataclass(frozen=True)
class GcSettings:
    dry_run: bool
    threshold_seconds: int
    interval_seconds: int
    started_at: float
    """`time.monotonic()` before the env write: no controller ran these knobs earlier."""
    started_epoch: int

    @property
    def reap_budget_seconds(self) -> float:
        return self.threshold_seconds + 6 * self.interval_seconds + 60

    @property
    def survival_window_seconds(self) -> float:
        """Past the threshold plus several ticks: a pod the loop would delete is gone by then."""

        return self.threshold_seconds + 4 * self.interval_seconds + 10


@dataclass(frozen=True)
class RetiredPod:
    name: str
    uid: str
    sandbox_id: str
    generation: int
    verdict: str
    """The verdict the reaper must reach, or "" for a control it must leave alone."""


class RetiredExecutionPodGcAssertions:
    """Produce retired pods, scope the GC's config, and judge what it deletes."""

    # Generous: a cold create, a sandbox-manager restart and node-ctl publishing.
    STRAND_TIMEOUT_SECONDS = 120
    STRAND_ATTEMPTS = 3

    def __init__(
        self,
        fixture: KindEcw,
        control: RetiredExecutionPodGcControl,
        scope: ResourceScope,
        attempt_create: Callable[[], object],
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._attempt_create = attempt_create
        self._release_lease: Compensation | None = None
        self._restore_env: Compensation | None = None
        self._standing: dict[str, str | None] | None = None
        self._settings: GcSettings | None = None
        self._removals: dict[str, Compensation] = {}

    # --- the scoped config ---------------------------------------------------

    @contextmanager
    def scoped_gc(self) -> Iterator[None]:
        """Put the standing env back as the body unwinds; a body error wins over a restore error."""

        try:
            yield
        except BaseException as body_error:
            try:
                self.restore_standing()
            except Exception as restore_error:  # noqa: BLE001 - attached to the body's error
                body_error.add_note(f"restoring the standing retired-pod GC env also failed: {restore_error!r}")
            raise
        self.restore_standing()

    def configure(self, *, dry_run: bool, threshold_seconds: int, interval_seconds: int) -> GcSettings:
        if not (5 <= threshold_seconds <= 120 and 1 <= interval_seconds <= threshold_seconds):
            raise ValueError("refusing a GC threshold outside 5..120 s or an interval above it")
        if self._release_lease is None:
            release = self._control.acquire_controller_mutation()
            self._release_lease = self._scope.defer(
                "release the controller mutation lease", release, critical=True
            )
        if self._standing is None:
            standing = self._control.retired_execution_pod_gc_env()
            self._standing = standing
            self._fixture.record("gc_env_before", dict(standing))
            self._restore_env = self._scope.defer(
                "restore the standing retired-execution-pod GC env",
                lambda: self._control.restore_retired_execution_pod_gc_env(standing),
                critical=True,
            )
        self._control.reset_retired_execution_pod_gc_counts()
        started_at, started_epoch = time.monotonic(), int(time.time())
        overrides = smoke_gc_env(
            dry_run=dry_run, threshold_seconds=threshold_seconds, interval_seconds=interval_seconds
        )
        self._control.set_retired_execution_pod_gc_env(overrides)
        live = self._control.retired_execution_pod_gc_env()
        expected = {**self._standing, **overrides}
        self._fixture.verify(
            live == expected,
            "the rolled cluster-controller does not carry the scenario's GC knobs",
            expected=expected,
            live=live,
        )
        self._fixture.record(f"gc_env_{DRY_RUN if dry_run else LIVE}", dict(live))
        self._settings = GcSettings(dry_run, threshold_seconds, interval_seconds, started_at, started_epoch)
        return self._settings

    def restore_standing(self) -> None:
        if self._restore_env is not None and self._standing is not None:
            self._control.restore_retired_execution_pod_gc_env(self._standing)
            after = self._control.retired_execution_pod_gc_env()
            self._fixture.verify(
                after == self._standing,
                "the controller's GC env did not come back to its standing values",
                standing=self._standing,
                after=after,
            )
            self._restore_env.disarm()
            self._restore_env = None
        if self._release_lease is not None:
            release, self._release_lease = self._release_lease, None
            release.release()

    # --- producing retired pods ------------------------------------------------

    def idle_suspended_pod(self, *, idle_timeout_seconds: int = 3) -> RetiredPod:
        """A second sandbox left access-idle until the node suspends it; its Succeeded pod remains."""

        sandbox = self._fixture.create()
        sandbox_id = _bare(sandbox.sandbox_id)
        pods = self._wait_bound_pods(sandbox_id)
        sandbox.set_timeout(idle_timeout_seconds)
        deadline = time.monotonic() + 180
        while True:
            rows = self._control.sandbox_executions(sandbox_id)
            lifecycle = self._control.sandbox_residency(sandbox_id).lifecycle
            document = self._control.restore_pod_document(pods[0])
            phase = ((document or {}).get("status") or {}).get("phase")
            if lifecycle == "suspended" and rows and not any(r.active for r in rows) and phase == "Succeeded":
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the access-idle sandbox did not settle suspended with its Succeeded pod left behind",
                lifecycle=lifecycle,
                executions=[(r.generation, r.state, r.active) for r in rows],
                pod=pods[0],
                pod_phase=phase,
            )
            self._fixture.wait(2)
        pod = self._pod(pods[0], TERMINAL_RETIRED)
        self._fixture.record("idle_suspended_pod", {"pod": pod.name, "executions": [(r.generation, r.state) for r in rows]})
        return pod

    def strand_cold_create(self) -> tuple[RetiredPod, BackgroundWork]:
        """A cold create retired as `replaced` while its pod is Pending; the create still in flight.

        Kills the sandbox-manager mid-activation, then has node-ctl publish
        until the API applies the rolled-back row and retires the execution as
        `replaced`; the kubelet's retry is refused from then on. If the retry
        wins the race the sandbox is simply live; the next attempt tries again.
        """

        for attempt in range(1, self.STRAND_ATTEMPTS + 1):
            known = set(self._control.run_sandbox_ids(self._fixture.sandbox.sandbox_id))
            work = background("in-flight create", self._create_ignoring_outcome)
            sandbox_id = self._wait_new_run_sandbox(known)
            name = self._wait_bound_pods(sandbox_id, require_node=True)[0]
            node = self._control.sandbox_pods(sandbox_id)[0].node_name
            crashed_at = self._control.crash_sandbox_manager_at(node, sandbox_id, "starting", timeout=60)
            outcome = self._publish_until_replaced(node, sandbox_id, name)
            self._fixture.record(
                f"strand_attempt_{attempt}",
                {"sandbox_id": sandbox_id, "pod": name, "node": node, "crashed": crashed_at, "outcome": outcome},
            )
            if outcome == "replaced":
                return self._pod(name, REPLACED_NEVER_STARTED), work
            work.join(timeout=self.STRAND_TIMEOUT_SECONDS)
        self._fixture.verify(False, "no cold create was stranded `replaced` in its Pending pod", attempts=self.STRAND_ATTEMPTS)
        raise AssertionError("unreachable")

    def _create_ignoring_outcome(self) -> None:
        # The create's response is not the subject: a stranded create fails or
        # times out, and its run-marker cleanup stays armed either way.
        try:
            self._attempt_create()
        except Exception:  # noqa: BLE001 - the outcome is judged from state
            pass

    def _wait_new_run_sandbox(self, known: set[str]) -> str:
        deadline = time.monotonic() + 60
        while True:
            new = sorted(set(self._control.run_sandbox_ids(self._fixture.sandbox.sandbox_id)) - known)
            if new:
                return new[0]
            self._fixture.verify(time.monotonic() < deadline, "the in-flight create never wrote its sandbox row")
            self._fixture.wait(0.2)

    def _wait_bound_pods(self, sandbox_id: str, *, require_node: bool = True) -> list[str]:
        deadline = time.monotonic() + 60
        while True:
            pods = [p for p in self._control.sandbox_pods(sandbox_id) if not p.deleting]
            if len(pods) == 1 and (pods[0].node_name or not require_node):
                return [pods[0].pod_name]
            self._fixture.verify(
                time.monotonic() < deadline,
                "the sandbox did not get exactly one bound pod",
                sandbox_id=sandbox_id,
                pods=[(p.pod_name, p.phase, p.node_name) for p in pods],
            )
            self._fixture.wait(0.2)

    def _publish_until_replaced(self, node: str, sandbox_id: str, pod_name: str) -> str:
        deadline = time.monotonic() + self.STRAND_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            rows = {r.pod_name: r for r in self._control.sandbox_executions(sandbox_id)}
            row = rows.get(pod_name)
            if row is not None and row.state == "replaced" and not row.active:
                return "replaced"
            document = self._control.restore_pod_document(pod_name) or {}
            if (document.get("status") or {}).get("phase") == "Running":
                return "activation retry won"
            self._control.publish_node_ctl_facet(node, "sandbox-inventory")
        return "timed out"

    def kill_public_api(self) -> None:
        """Force-kill every API replica so no create cleanup runs; returns once a fresh API serves."""

        release = self._control.acquire_public_api_mutation()
        try:
            self._control.force_restart_public_api(timeout=180)
        finally:
            release()

    def plant_pending_with_ip(self, at: RetiredPod, *, template_pod: str) -> RetiredPod:
        """A Pending clone at `at`'s name and identity with a pod IP: the reaper must leave it.

        `at`'s own pod must be gone, so the clone is the only pod its retired
        `replaced` row names.
        """

        template = self._control.restore_pod_document(template_pod)
        self._fixture.verify(template is not None, "the template pod vanished", pod=template_pod)
        assert template is not None
        token = secrets.token_hex(4)
        self._removals[token] = self._scope.defer(
            f"remove the planted pod {at.name}",
            lambda: self._control.remove_planted_pods(token),
            critical=True,
        )
        uid = self._control.plant_pending_pod_with_ip(
            build_planted_pod(template, name=at.name, sandbox_id=at.sandbox_id, generation=at.generation, token=token),
            pod_ip=PLANTED_POD_IP,
        )
        view = self._census().get(at.name)
        self._fixture.verify(
            view is not None and view.uid == uid and view.phase == "Pending" and view.pod_ip == PLANTED_POD_IP
            and view.sandbox_not_ready and view.row_state == "replaced" and not view.row_active,
            "the planted pod is not a Pending pod with an IP on a retired `replaced` row",
            view=repr(view),
        )
        return RetiredPod(at.name, uid, at.sandbox_id, at.generation, "")

    def _pod(self, name: str, verdict: str) -> RetiredPod:
        view = self._census().get(name)
        self._fixture.verify(
            view is not None and would_reap(view) == verdict,
            f"the produced pod does not classify as {verdict}",
            pod=name,
            view=repr(view),
        )
        assert view is not None
        return RetiredPod(name, view.uid, view.sandbox_id, view.generation, verdict)

    def _census(self) -> dict[str, SandboxPodExecution]:
        return {p.pod_name: p for p in self._control.sandbox_pod_executions()}

    # --- verdicts -----------------------------------------------------------------

    def expect_gone(self, pod: RetiredPod, *, timeout: float, message: str) -> float:
        """The pod is gone within `timeout`, whoever deleted it; returns how long that took."""

        started = time.monotonic()
        while self._present(pod):
            self._fixture.verify(time.monotonic() - started < timeout, message, pod=pod.name, timeout_seconds=timeout)
            self._fixture.wait(1)
        return round(time.monotonic() - started, 1)

    def expect_persist(self, *pods: RetiredPod, seconds: float, record_key: str) -> None:
        """Every pod keeps its uid, undeleted, for `seconds`."""

        deadline = time.monotonic() + seconds
        while True:
            missing = [p.name for p in pods if not self._present(p)]
            self._fixture.verify(not missing, "a pod the GC must keep was deleted", pods=missing, record=record_key)
            if time.monotonic() >= deadline:
                break
            self._fixture.wait(2)
        self._fixture.record(record_key, {"pods": [p.name for p in pods], "seconds": seconds})

    def expect_dry_run_verdicts(self, *targets: RetiredPod) -> dict[str, int]:
        """Dry-run reports would-reap for exactly the pods that classify reapable, and deletes none.

        The expected count per verdict is the targets plus any other pod on
        the bench the reaper's rule takes (recorded); the metric is summed
        from the intake since this config rolled.
        """

        settings = self._require_settings(dry_run=True)
        census = self._census()
        mine = {t.name for t in targets}
        foreign = {name: v for name, p in census.items() if name not in mine and (v := would_reap(p))}
        expected = {verdict: 0 for verdict in VERDICTS}
        for verdict in [t.verdict for t in targets] + list(foreign.values()):
            expected[verdict] += 1
        self._fixture.record("dry_run_foreign_candidates", foreign)
        self.expect_persist(*targets, seconds=settings.survival_window_seconds, record_key="dry_run_kept")
        # The intake receives the agent's flush some seconds after the tick.
        deadline = time.monotonic() + 120
        while True:
            counts = self._control.retired_execution_pod_gc_counts(since_epoch=settings.started_epoch)
            observed = {verdict: counts.get(f"{DRY_RUN}/{verdict}", 0) for verdict in VERDICTS}
            if all(observed[v] >= expected[v] for v in VERDICTS) or time.monotonic() > deadline:
                break
            self._fixture.wait(15)
        self._fixture.record("dry_run_would_reap", observed)
        self._fixture.verify(
            observed == expected and counts.get(f"{LIVE}/{TERMINAL_RETIRED}", 0) == 0
            and counts.get(f"{LIVE}/{REPLACED_NEVER_STARTED}", 0) == 0,
            "dry-run did not report would-reap for exactly the retired pods",
            expected=expected,
            observed=counts,
        )
        return observed

    def expect_reaped(self, *pods: RetiredPod) -> dict[str, float]:
        """The live loop deletes every pod, none before the threshold could have passed."""

        settings = self._require_settings(dry_run=False)
        earliest = settings.started_at + settings.threshold_seconds - settings.interval_seconds
        deadline = time.monotonic() + settings.reap_budget_seconds
        last_present = {p.name: settings.started_at for p in pods}
        gone_after: dict[str, float] = {}
        while len(gone_after) < len(pods):
            for pod in pods:
                if pod.name in gone_after:
                    continue
                polled = time.monotonic()
                if self._present(pod):
                    last_present[pod.name] = polled
                    continue
                gone_after[pod.name] = round(polled - settings.started_at, 1)
                self._fixture.verify(
                    last_present[pod.name] >= earliest,
                    "the GC deleted a retired pod before the threshold",
                    pod=pod.name,
                    last_seen_present_after_seconds=round(last_present[pod.name] - settings.started_at, 1),
                    threshold_seconds=settings.threshold_seconds,
                )
            if len(gone_after) == len(pods):
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "a retired pod was not reaped within the budget",
                pods=[p.name for p in pods if p.name not in gone_after],
                reap_budget_seconds=settings.reap_budget_seconds,
            )
            self._fixture.wait(1)
        self._fixture.record("reaped_after_config_seconds", gone_after)
        return gone_after

    def expect_live_reap_counts(self, *pods: RetiredPod) -> None:
        settings = self._require_settings(dry_run=False)
        expected = {verdict: sum(1 for p in pods if p.verdict == verdict) for verdict in VERDICTS}
        deadline = time.monotonic() + 120
        while True:
            counts = self._control.retired_execution_pod_gc_counts(since_epoch=settings.started_epoch)
            observed = {verdict: counts.get(f"{LIVE}/{verdict}", 0) for verdict in VERDICTS}
            if all(observed[v] >= expected[v] for v in VERDICTS) or time.monotonic() > deadline:
                break
            self._fixture.wait(15)
        self._fixture.record("live_reaped", observed)
        self._fixture.verify(
            all(observed[v] >= expected[v] for v in VERDICTS),
            "the live GC's reap counter does not cover the deleted pods",
            expected=expected,
            observed=counts,
        )

    def expect_live_sandbox_untouched(self, *, seconds: float) -> None:
        """The fixture sandbox's pod keeps its uid and its one active execution."""

        sandbox_id = _bare(self._fixture.sandbox.sandbox_id)
        pods = [p for p in self._control.sandbox_pods(sandbox_id) if not p.deleting]
        self._fixture.verify(len(pods) == 1, "the live sandbox does not have one pod", pods=[p.pod_name for p in pods])
        view = self._census()[pods[0].pod_name]
        live = RetiredPod(view.pod_name, view.uid, view.sandbox_id, view.generation, "")
        self.expect_persist(live, seconds=seconds, record_key="live_sandbox_kept")
        active = [r for r in self._control.sandbox_executions(sandbox_id) if r.active]
        self._fixture.verify(
            len(active) == 1 and active[0].generation == live.generation,
            "the live sandbox's execution was disturbed while the GC ran",
            active=[(r.generation, r.state) for r in active],
        )

    def _present(self, pod: RetiredPod) -> bool:
        document = self._control.restore_pod_document(pod.name)
        metadata = (document or {}).get("metadata") or {}
        return metadata.get("uid") == pod.uid and not metadata.get("deletionTimestamp")

    def release_plants(self) -> None:
        for token in list(self._removals):
            self._removals.pop(token).release()

    def _require_settings(self, *, dry_run: bool) -> GcSettings:
        if self._settings is None or self._settings.dry_run != dry_run:
            raise RuntimeError(f"configure(dry_run={dry_run}) must run first")
        return self._settings


def _bare(sandbox_id: str) -> str:
    return sandbox_id.removeprefix("sbx_")

"""A forced drain re-places a resident `stopped` sandbox, still stopped, on another worker.

A forced `:drain` of a worker that holds a `stopped` sandbox publishes the
sandbox's disk artifact to object storage, waits until it is durable, and
releases the node: the row's `active_node_id` goes NULL, its placement goes
`unplaced`, and `evacuation_restore_target` is `'stopped'`. The restore sweep
then claims the row (`stopped -> restoring`) and creates an execution pod on
another worker whose `space.pplx.net/checkpoint` annotation is
`stopped-materialization`. That pod's shim asks sandbox-manager to stage the
disk and leave the sandbox stopped, with no microVM, and the control plane
finalizes the row back to `stopped`, `on_node`, marker cleared, pinned to the
new worker. A later user `start` cold-starts there from the staged rootfs.

The evacuation ships default-off behind three cluster-controller flags, and
it is on only when all three are (`ReconcilerConfig::evacuates_stopped_residents`
in `crates/libs/cluster/controller/server/src/node_drain/mod.rs`). The
controller reads them once at startup, so the scenario overrides the
controller Deployment's env for these three keys alone (closed key list,
`true`/`false` only) under the controller-mutation Lease the retention and
orphaned-restore-pod GC scenarios share, awaits the rollout, checks the
literals on every running controller pod, and puts the captured standing
literals back.

The observables are state: the `sandboxes` row's lifecycle, node pin,
placement and marker; the execution pod objects and their annotations; and
sandbox-manager's own lifecycle for the sandbox on the destination worker,
read through `space-sbxmgr doctor`. The control-plane lifecycle is sampled
on every poll between the stop and the user start, so "never `started`" is a
sampled observation, not a proof that no transition happened between two
reads. sandbox-manager log lines are attached to a failure as evidence and
never judged.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from space_smoke.capabilities import (
    AdminNodeView,
    SandboxPodView,
    SandboxPodWatch,
    StoppedReplacementControl,
    StoppedResidency,
)
from space_smoke.node_drain import NodeDrainAssertions
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import CommandSandboxFixture

EVACUATE_STOPPED_RESIDENTS_ENV = "SPACE_NODE_DRAIN_EVACUATE_STOPPED_RESIDENTS"
TERMINATION_BLOCKING_GATE_ENV = "SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE"
REPLACE_STOPPED_EVACUATIONS_ENV = "SPACE_NODE_DRAIN_REPLACE_STOPPED_EVACUATIONS"

# The only controller env this scenario may touch, and the closed vocabulary
# its overrides come from. `truthy_default` reads each one.
STOPPED_REPLACEMENT_ENV_KEYS: tuple[str, ...] = (
    EVACUATE_STOPPED_RESIDENTS_ENV,
    TERMINATION_BLOCKING_GATE_ENV,
    REPLACE_STOPPED_EVACUATIONS_ENV,
)
_FLAG_VALUES = frozenset({"true", "false"})

# `crates/libs/common/core/src/k8s.rs`.
CHECKPOINT_ANNOTATION = "space.pplx.net/checkpoint"
STOPPED_MATERIALIZATION_CHECKPOINT = "stopped-materialization"
STOPPED_MATERIALIZATION_ARTIFACT_ID_ANNOTATION = (
    "space.pplx.net/stopped-materialization-artifact-id"
)
RESTORE_STORAGE_URI_ANNOTATION = "space.pplx.net/suspend-restore-storage-uri"
RESTORE_SOURCE_NODE_ANNOTATION = "space.pplx.net/suspend-restore-source-node"

# `space-sbxmgr doctor` prints `Doctor: <short id>  (<lifecycle label>)` first
# (crates/apps/sbx/mgr/cli/src/dispatch/diagnostics.rs).
_DOCTOR_LINE = re.compile(r"^Doctor: \S+\s+\(([a-z_]+)\)$", re.MULTILINE)
_TERMINAL_POD_PHASES = frozenset({"Succeeded", "Failed"})


def validate_stopped_replacement_keys(keys: Iterable[str]) -> None:
    unknown = sorted(set(keys) - set(STOPPED_REPLACEMENT_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing non-stopped-replacement controller env keys: {unknown}")


def validate_stopped_replacement_overrides(overrides: Mapping[str, str]) -> None:
    """Refuse any key off the three flags and any value other than `true`/`false`."""

    validate_stopped_replacement_keys(overrides)
    for key, value in overrides.items():
        if value not in _FLAG_VALUES:
            raise ValueError(f"refusing stopped-replacement env value {value!r} for {key}")


def smoke_stopped_replacement_env() -> dict[str, str]:
    """All three on: any one off leaves the evacuation off."""

    return {key: "true" for key in STOPPED_REPLACEMENT_ENV_KEYS}


def parse_doctor_lifecycle(output: str) -> str:
    """The lifecycle label from `space-sbxmgr doctor` output; refuses anything else."""

    labels = _DOCTOR_LINE.findall(output)
    if len(labels) != 1:
        raise ValueError(f"unrecognized space-sbxmgr doctor output: {output[:200]!r}")
    return str(labels[0])


def replacement_landed(residency: StoppedResidency, drained_node_id: str) -> bool:
    """The re-placement finalized: `stopped`, `on_node`, marker cleared, pinned off the drained node."""

    return (
        residency.lifecycle == "stopped"
        and residency.active_node_id not in (None, drained_node_id)
        and residency.placement == "on_node"
        and residency.evacuation_restore_target is None
    )


def materialization_pod_problems(
    document: Mapping[str, Any] | None, node_name: str, prior_host: str
) -> list[str]:
    """What keeps `document` from being a stopped-materialization pod bound to `node_name`.

    It must also be kept off `prior_host`, a node the sandbox ran on before:
    every required node-affinity term excludes it by name, since terms OR and
    one that didn't would let the scheduler place the pod there.
    """

    if document is None:
        return ["the pod does not exist"]
    metadata = document.get("metadata") or {}
    annotations = metadata.get("annotations") or {}
    spec = document.get("spec") or {}
    problems = []
    checkpoint = annotations.get(CHECKPOINT_ANNOTATION)
    if checkpoint != STOPPED_MATERIALIZATION_CHECKPOINT:
        problems.append(f"{CHECKPOINT_ANNOTATION} is {checkpoint!r}")
    for key in (STOPPED_MATERIALIZATION_ARTIFACT_ID_ANNOTATION, RESTORE_STORAGE_URI_ANNOTATION):
        value = annotations.get(key)
        if not isinstance(value, str) or not value.strip():
            problems.append(f"{key} is missing or empty")
    if spec.get("nodeName") != node_name:
        problems.append(f"the pod is bound to {spec.get('nodeName')!r}, not {node_name!r}")
    if metadata.get("deletionTimestamp"):
        problems.append("the pod is being deleted")
    if not excludes_node_in_every_term(document, prior_host):
        problems.append(f"the pod's required node affinity does not exclude the prior host {prior_host!r}")
    return problems


def excludes_node_in_every_term(document: Mapping[str, Any], node_name: str) -> bool:
    """Every required node-affinity term of the pod excludes `node_name` by a `metadata.name NotIn`.

    Terms OR, so one term without the exclusion would let the scheduler place
    the pod on that node.
    """

    spec = document.get("spec") or {}
    terms = (
        (((spec.get("affinity") or {}).get("nodeAffinity") or {}).get(
            "requiredDuringSchedulingIgnoredDuringExecution"
        )
        or {}).get("nodeSelectorTerms")
        or []
    )
    return bool(terms) and all(
        any(
            field.get("key") == "metadata.name"
            and field.get("operator") == "NotIn"
            and node_name in (field.get("values") or [])
            for field in (term.get("matchFields") or [])
        )
        for term in terms
    )


def stopped_start_pod_problems(document: Mapping[str, Any] | None, node_name: str) -> list[str]:
    """What keeps `document` from being a stopped start's pod pre-bound to `node_name`.

    Pre-bound means the control plane planned the node alone: the source-node
    annotation names it, no storage-URI fallback is carried (with one, the
    node would be only a scheduling preference), and the pod is bound there.
    """

    if document is None:
        return ["the pod does not exist"]
    annotations = (document.get("metadata") or {}).get("annotations") or {}
    problems = []
    if annotations.get(RESTORE_SOURCE_NODE_ANNOTATION) != node_name:
        problems.append(
            f"{RESTORE_SOURCE_NODE_ANNOTATION} is {annotations.get(RESTORE_SOURCE_NODE_ANNOTATION)!r}"
        )
    if annotations.get(RESTORE_STORAGE_URI_ANNOTATION):
        problems.append(f"{RESTORE_STORAGE_URI_ANNOTATION} is set, so the node is only a preference")
    bound = (document.get("spec") or {}).get("nodeName")
    if bound != node_name:
        problems.append(f"the pod is bound to {bound!r}, not {node_name!r}")
    return problems


def pod_is_unschedulable(document: Mapping[str, Any]) -> bool:
    """The scheduler marked the pod `PodScheduled=False, reason=Unschedulable` and bound it nowhere."""

    if (document.get("spec") or {}).get("nodeName"):
        return False
    conditions = (document.get("status") or {}).get("conditions") or []
    return any(
        condition.get("type") == "PodScheduled"
        and condition.get("status") == "False"
        and condition.get("reason") == "Unschedulable"
        for condition in conditions
    )


def _since_now() -> str:
    # A minute of slack for the host/node clock and log flush.
    return (datetime.now(UTC) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


class StoppedReplacementAssertions:
    """Scope the three flags to the scenario, drain a stopped resident's worker, judge the re-placement."""

    def __init__(
        self,
        fixture: CommandSandboxFixture,
        control: StoppedReplacementControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self.drain = NodeDrainAssertions(fixture, control, scope)
        self.re_drain: NodeDrainAssertions | None = None
        self._disk_snapshots_before: list[str] = []
        self._release_lease: Compensation | None = None
        self._restore_env: Compensation | None = None
        self._standing: dict[str, str | None] | None = None
        self._sampling = False
        self._observed: list[str] = []
        self._since: str | None = None
        self._destination: AdminNodeView | None = None
        self._materialization_pod_uid: str | None = None
        # The sandbox's pods, watched from just before the drain. The
        # materialization pod lives only from the claim until the drain settles
        # the row and deletes it, so it is recorded as the apiserver delivers
        # it rather than looked up afterwards.
        self._pod_watch: SandboxPodWatch | None = None

    @property
    def _sandbox_id(self) -> str:
        return self.drain.sandbox_id

    # --- preconditions and cleanup ---------------------------------------------

    def require_preconditions(self) -> None:
        """Arm the local-row reclaim, then require a clean fleet of at least two workers.

        The evacuation leaves the sandbox's node-local copy on the drained
        worker until the node terminates (`docs/configuration.md`), and the
        drain-keep override keeps it from terminating. The reclaim runs after
        every other compensation, deletes
        the sandbox (a 404 is success, so the fixture's own delete after it is
        a no-op) and then removes the sandbox's local rows on every worker so
        none outlives the scenario into another scenario's view of a worker.
        """

        sandbox_id = self._sandbox_id

        def delete_then_reclaim() -> None:
            self._fixture.delete_sandbox()
            self._control.reclaim_local_sandbox_everywhere(sandbox_id)

        self._scope.defer(
            "delete the sandbox, then reclaim its local rows on every worker",
            delete_then_reclaim,
            critical=True,
        )
        self.drain.require_clean_fleet()

    # --- the scoped flags --------------------------------------------------------

    @contextmanager
    def scoped_flags(self) -> Iterator[None]:
        """Put the standing env back as the body unwinds, success or failure alike.

        `enable_flags` runs inside the body, in its own phase. The scope
        compensation stays armed as the failure backstop, but the cleanup
        window is too short to own a controller rollout, so the restore runs
        here. When the body failed, its error propagates: a restore failure is
        attached to it as a note and recorded, and the backstop stays armed.
        """

        try:
            yield
        except BaseException as body_error:
            try:
                self.restore_standing()
            except Exception as restore_error:  # noqa: BLE001 - attached to the body's error below
                self._fixture.record(
                    "stopped_replacement_env_restore_failure", repr(restore_error)[:1_000]
                )
                body_error.add_note(
                    "restoring the standing stopped-replacement controller env also failed: "
                    f"{restore_error!r}; the scope's restore compensation stays armed"
                )
            raise
        self.restore_standing()

    def enable_flags(self) -> None:
        """Turn the three flags on, and see them on the Deployment and every running controller pod."""

        release = self._control.acquire_controller_mutation()
        self._release_lease = self._scope.defer(
            "release the controller mutation lease", release, critical=True
        )
        standing = self._control.stopped_replacement_env()
        self._standing = standing
        self._fixture.record("stopped_replacement_env_before", dict(standing))
        # Armed before the override: a set that half-applies is still put back.
        self._restore_env = self._scope.defer(
            "restore the standing stopped-replacement controller env",
            lambda: self._control.restore_stopped_replacement_env(standing),
            critical=True,
        )
        overrides = smoke_stopped_replacement_env()
        self._control.set_stopped_replacement_env(overrides)
        live = self._control.stopped_replacement_env()
        pods = self._control.running_controller_stopped_replacement_envs()
        self._fixture.verify(
            live == overrides and bool(pods) and all(pod == overrides for pod in pods),
            "the rolled cluster-controller does not carry the three stopped-replacement flags",
            expected=overrides,
            deployment=live,
            running_pods=pods,
        )
        self._fixture.record("stopped_replacement_env_live", dict(live))

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
            self._control.restore_stopped_replacement_env(standing)
            after = self._control.stopped_replacement_env()
            self._fixture.verify(
                after == standing,
                "the controller's stopped-replacement env did not come back to its standing values",
                standing=standing,
                after=after,
            )
            self._restore_env.disarm()
            self._restore_env = None
        if self._release_lease is not None:
            release, self._release_lease = self._release_lease, None
            release.release()

    # --- the stopped resident ----------------------------------------------------

    def record_resident(self) -> AdminNodeView:
        """The worker the started sandbox runs on: the node the drain will target."""

        return self.drain.resident_node()

    def stop_on_resident(self) -> None:
        """Stop the sandbox; the row is `stopped`, still pinned to the recorded node, with no claim."""

        node = self.drain.drained_node
        self._fixture.stop_on_node()
        self._sampling = True
        residency = self._read()
        self._fixture.verify(
            residency.lifecycle == "stopped"
            and residency.active_node_id == node.id
            and residency.evacuation_restore_target is None,
            "the stopped sandbox is not an unclaimed stopped resident of the node it ran on",
            lifecycle=residency.lifecycle,
            active_node_id=residency.active_node_id,
            evacuation_restore_target=residency.evacuation_restore_target,
            node_id=node.id,
        )
        self._fixture.record("stopped_resident_placement", residency.placement)

    def drain_forced(self) -> None:
        watch = self._control.watch_sandbox_pods(self._sandbox_id)
        self._pod_watch = watch
        self._scope.defer("stop the sandbox pod watch", watch.stop)
        self._since = _since_now()
        self.drain.drain_forced()

    # --- the re-placement ---------------------------------------------------------

    def expect_replaced_stopped(self, *, timeout: float = 900) -> AdminNodeView:
        """The row comes back `stopped`, `on_node`, marker cleared, pinned to another worker.

        The budget covers a disk-artifact upload and its durability wait on a
        slow bench, the release, the claim, and the materialization.
        """

        drained = self.drain.drained_node
        deadline = time.monotonic() + timeout
        released_seen = False
        while True:
            residency = self._read()
            if residency.active_node_id is None:
                released_seen = True
            if replacement_landed(residency, drained.id):
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the forced drain did not re-place its stopped resident, stopped, on another worker",
                lifecycle=residency.lifecycle,
                active_node_id=residency.active_node_id,
                placement=residency.placement,
                evacuation_restore_target=residency.evacuation_restore_target,
                drained_node_id=drained.id,
                observed_lifecycles=self._transitions(),
                failure_evidence=self._manager_log_evidence,
            )
            self._fixture.wait(1)
        # Evidence only: a release that lands between two polls is not seen.
        self._fixture.record("release_observed", released_seen)
        destination = next(
            (n for n in self._control.admin_nodes() if n.id == residency.active_node_id), None
        )
        self._fixture.verify(
            destination is not None
            and bool(destination.k8s_node_name)
            and destination.k8s_node_name != drained.k8s_node_name,
            "the re-placed sandbox names a node with no other kube worker behind it",
            active_node_id=residency.active_node_id,
            drained_node=drained.k8s_node_name,
        )
        assert destination is not None
        self._destination = destination
        self._fixture.record("replaced_to", destination.k8s_node_name)
        return destination

    def expect_materialized_without_boot(self, *, release_timeout: float = 60) -> None:
        """Every pod the re-placement created was a stopped materialization, one on the
        destination, none was left once the row settled, and sandbox-manager there reports `stopped`.

        The pods show the control plane asked the destination for a stopped
        materialization rather than a boot. They come from a watch started
        before the drain, because the pod lives only from the claim until the
        drain settles the row and deletes it; a pod with any other checkpoint is
        a boot request. Every pod created after the drain began is judged,
        wherever it bound; the stopped resident's own pod, which existed before,
        isn't. The destination's pod must also exclude the drained node, a prior
        host of the sandbox, in every required node-affinity term. Each pod is
        judged by its first version bound to a node and not yet being deleted,
        or by its last version if it never bound; the watch is read again once
        the row has settled. Once the row
        settles no live pod is left, the shape an
        ordinarily stopped sandbox has. sandbox-manager's own lifecycle for the
        sandbox must be `stopped`, not `started` or `paused`, the two
        lifecycles `space-sbxmgr doctor` treats as running
        (crates/apps/sbx/mgr/cli/src/dispatch/diagnostics.rs).
        """

        destination = self._require_destination()
        watch_failure = self._pod_watch.failure() if self._pod_watch is not None else None
        self._fixture.verify(
            watch_failure is None,
            "the sandbox pod watch ended before the re-placement could be judged",
            watch_failure=watch_failure,
        )
        seen = self._watched_pods()
        summary = [(p.pod_name, p.phase, p.node_name, p.checkpoint) for p, _ in seen]
        self._fixture.verify(
            bool(seen) and all(p.checkpoint == STOPPED_MATERIALIZATION_CHECKPOINT for p, _ in seen),
            "the re-placement created a pod that is not a stopped materialization, or none was seen",
            pods_seen=summary,
        )
        on_destination = [(p, d) for p, d in seen if p.node_name == destination.k8s_node_name]
        self._fixture.verify(
            bool(on_destination),
            "no stopped-materialization pod was seen on the destination worker",
            destination=destination.k8s_node_name,
            pods_seen=summary,
        )
        pod, document = on_destination[-1]
        problems = materialization_pod_problems(
            document, destination.k8s_node_name, self.drain.drained_node.k8s_node_name
        )
        self._fixture.verify(
            not problems,
            "the destination pod is not a stopped materialization",
            pod=pod.pod_name,
            problems=problems,
        )
        assert document is not None
        self._materialization_pod_uid = str((document.get("metadata") or {}).get("uid", ""))
        self._fixture.record("materialization_pod", pod.pod_name)
        self._fixture.record("pods_seen_during_replacement", summary)

        deadline = time.monotonic() + release_timeout
        while True:
            pods = self._control.sandbox_pods(self._sandbox_id)
            live = [p for p in pods if not p.deleting and p.phase not in _TERMINAL_POD_PHASES]
            if not live:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the settled re-placement still holds a live pod",
                pods=[(p.pod_name, p.phase, p.node_name, p.checkpoint, p.deleting) for p in pods],
            )
            self._fixture.wait(1)
        # Read the watch again now the row has settled and its pod is gone, so
        # a pod created after the first read is judged too.
        watch_failure = self._pod_watch.failure() if self._pod_watch is not None else None
        settled = self._watched_pods()
        self._fixture.verify(
            watch_failure is None
            and all(p.checkpoint == STOPPED_MATERIALIZATION_CHECKPOINT for p, _ in settled),
            "a pod that is not a stopped materialization was created as the re-placement settled",
            watch_failure=watch_failure,
            pods_seen=[(p.pod_name, p.phase, p.node_name, p.checkpoint) for p, _ in settled],
        )

        lifecycle = self._control.sandbox_manager_lifecycle(
            destination.k8s_node_name, self._sandbox_id
        )
        self._fixture.verify(
            lifecycle == "stopped",
            "sandbox-manager on the destination does not report the re-placed sandbox stopped",
            destination=destination.k8s_node_name,
            sandbox_manager_lifecycle=lifecycle,
            failure_evidence=self._manager_log_evidence,
        )
        self._fixture.record("destination_sandbox_manager_lifecycle", lifecycle)
        self._read()

    # --- the emptied node ---------------------------------------------------------

    def expect_drained_and_held(self) -> None:
        self._read()
        self.drain.expect_drained_and_held()
        self._read()

    def undrain(self) -> None:
        self.drain.undrain()
        self._read()

    # --- the user start -----------------------------------------------------------

    def expect_never_started(self) -> None:
        """No lifecycle read between the stop and now saw `started` (a sampled observation)."""

        self._read()
        self._sampling = False
        transitions = self._transitions()
        self._fixture.record("lifecycles_between_stop_and_start", transitions)
        self._fixture.verify(
            "started" not in self._observed,
            "the sandbox was observed started between its stop and the user start",
            observed_lifecycles=transitions,
        )

    def start_on_destination(self, *, timeout: float = 180) -> None:
        """A user start succeeds on the destination, in a pod that replaced the materialization.

        The start's pod must be pre-bound to the destination, the node holding
        the sandbox's copy: the control plane plans that node alone, with no
        storage-URI fallback that would make it only a preference. On a node
        without a local copy the start can't complete (sandbox-manager's
        cross-node path doesn't boot a disk artifact; see
        `stopped_restore_plan` in the Kubernetes substrate), so the only other
        node one could land on and succeed is one still holding an older copy,
        such as the undrained source here, which would silently boot stale
        bytes.
        """

        destination = self._require_destination()
        failure = self._fixture.start_from_node()
        self._fixture.verify(
            failure is None,
            "the re-placed stopped sandbox did not start",
            restart_error=failure,
        )
        deadline = time.monotonic() + timeout
        while True:
            residency = self._control.stopped_residency(self._sandbox_id)
            placement = self._control.sandbox_pod_placement(self._sandbox_id)
            if (
                residency.lifecycle == "started"
                and residency.active_node_id == destination.id
                and placement is not None
                and placement.node_name == destination.k8s_node_name
                and placement.pod_uid != self._materialization_pod_uid
            ):
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the started sandbox is not running on the destination in a fresh pod",
                lifecycle=residency.lifecycle,
                active_node_id=residency.active_node_id,
                destination=(destination.id, destination.k8s_node_name),
                materialization_pod_uid=self._materialization_pod_uid,
                pod_now=None
                if placement is None
                else (placement.pod_name, placement.pod_uid, placement.node_name),
            )
            self._fixture.wait(2)
        self._fixture.record("started_pod", placement.pod_name)
        problems = stopped_start_pod_problems(
            self._control.restore_pod_document(placement.pod_name), destination.k8s_node_name
        )
        self._fixture.verify(
            not problems,
            "the start pod is not pre-bound to the destination",
            pod=placement.pod_name,
            problems=problems,
        )

    def stop_on_destination(self) -> None:
        """Stop the sandbox again; the row is `stopped` on the destination, with no claim."""

        destination = self._require_destination()
        self._fixture.stop_on_node()
        residency = self._read()
        self._fixture.verify(
            residency.lifecycle == "stopped"
            and residency.active_node_id == destination.id
            and residency.evacuation_restore_target is None,
            "the restarted sandbox is not an unclaimed stopped resident of the destination",
            lifecycle=residency.lifecycle,
            active_node_id=residency.active_node_id,
            evacuation_restore_target=residency.evacuation_restore_target,
        )

    # --- draining the destination again --------------------------------------------

    def re_drain_destination(self) -> None:
        """Force-drain the destination before anyone has started the re-placed sandbox.

        Records the sandbox's disk snapshot rows first, so the release can be
        shown to publish nothing. Run after `undrain()`, which gives the worker
        cordon lease back for this drain to take.
        """

        destination = self._require_destination()
        self._disk_snapshots_before = self._control.disk_snapshot_ids(self._sandbox_id)
        self._fixture.record("disk_snapshots_before_re_drain", self._disk_snapshots_before)
        self.re_drain = NodeDrainAssertions(self._fixture, self._control, self._scope)
        self.re_drain.target_node(destination)
        self.re_drain.drain_forced()

    def expect_released_from_destination(self, *, new_backup: bool, timeout: float = 600) -> None:
        """The destination gives up its copy, on a backup published for it or not.

        The row leaves the destination with its evacuation marker set: released
        and unplaced, or already claimed for the next re-placement. With
        `new_backup` false, no new `disk` snapshot row appears: the cluster row
        and the node both pin the artifact the sandbox was re-placed from, whose
        backup is already durable, so the evacuation skips the publish. With it
        true, one does: a start since the re-placement dropped that pin and the
        stop after it minted a new one, so the evacuation took the ordinary path
        and published it. Either way the drain didn't park the node.
        """

        destination = self._require_destination()
        deadline = time.monotonic() + timeout
        while True:
            residency = self._read()
            if residency.active_node_id is None and residency.evacuation_restore_target == "stopped":
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the re-drained destination did not release the re-placed stopped sandbox",
                lifecycle=residency.lifecycle,
                active_node_id=residency.active_node_id,
                evacuation_restore_target=residency.evacuation_restore_target,
                destination=destination.id,
                failure_evidence=self._manager_log_evidence,
            )
            self._fixture.wait(2)
        after = self._control.disk_snapshot_ids(self._sandbox_id)
        self._fixture.record("disk_snapshots_after_re_drain", after)
        if new_backup:
            self._fixture.verify(
                len(after) > len(self._disk_snapshots_before)
                and set(self._disk_snapshots_before) <= set(after),
                "releasing the restarted sandbox published no new disk backup",
                before=self._disk_snapshots_before,
                after=after,
            )
        else:
            self._fixture.verify(
                after == self._disk_snapshots_before,
                "releasing the re-placed sandbox published a new disk backup",
                before=self._disk_snapshots_before,
                after=after,
            )

    def expect_re_placement_waits_unschedulable(self, *, timeout: float = 300) -> None:
        """With both workers prior hosts, the next re-placement waits `Unschedulable`, excluding both.

        On a two-worker bench the sandbox has now run on each, so its next
        re-placement pod must be kept off both: it waits unbound, and the row
        stays a claimed re-placement, still marked.
        """

        destination = self._require_destination()
        source = self.drain.drained_node
        deadline = time.monotonic() + timeout
        while True:
            residency = self._read()
            waiting = [
                (pod, document)
                for pod in self._control.sandbox_pods(self._sandbox_id)
                if pod.checkpoint == STOPPED_MATERIALIZATION_CHECKPOINT and not pod.deleting
                for document in [self._control.restore_pod_document(pod.pod_name)]
                if document is not None and pod_is_unschedulable(document)
            ]
            if waiting and residency.lifecycle == "restoring":
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the next re-placement did not wait unschedulable",
                lifecycle=residency.lifecycle,
                evacuation_restore_target=residency.evacuation_restore_target,
                pods=[(p.pod_name, p.phase, p.node_name, p.checkpoint) for p in self._control.sandbox_pods(self._sandbox_id)],
            )
            self._fixture.wait(2)
        pod, document = waiting[0]
        excluded = {
            node: excludes_node_in_every_term(document, node)
            for node in (source.k8s_node_name, destination.k8s_node_name)
        }
        self._fixture.record("unschedulable_re_placement_pod", pod.pod_name)
        self._fixture.verify(
            all(excluded.values()),
            "the waiting re-placement pod must exclude both prior hosts in every required term",
            excluded=excluded,
        )
        self._fixture.verify(
            residency.evacuation_restore_target == "stopped" and residency.active_node_id is None,
            "the waiting re-placement must stay a claimed, unplaced re-placement",
            evacuation_restore_target=residency.evacuation_restore_target,
            active_node_id=residency.active_node_id,
        )

    # --- helpers ------------------------------------------------------------------

    def _watched_pods(self) -> list[tuple[SandboxPodView, Mapping[str, Any] | None]]:
        """Each pod the watch delivered that didn't exist before the drain, by UID.

        The pods in the listing the watch started from (the stopped resident's
        own) are left out; every pod created after it is judged, wherever it
        bound. A pod is represented by its first version bound to a node and
        not yet being deleted, the shape the destination ran; one never bound,
        by its last version and no document. Keyed by UID because a pod name
        can come back as a new pod.
        """

        watch = self._pod_watch
        if watch is None:
            raise RuntimeError("the sandbox pod watch was not started before the drain")
        listed = watch.listed_uids()
        by_uid: dict[str, list[tuple[SandboxPodView, Mapping[str, Any]]]] = {}
        for pod, document in watch.versions():
            uid = str((document.get("metadata") or {}).get("uid") or "")
            if uid and uid not in listed:
                by_uid.setdefault(uid, []).append((pod, document))
        seen: list[tuple[SandboxPodView, Mapping[str, Any] | None]] = []
        for versions in by_uid.values():
            bound = next(((p, d) for p, d in versions if p.node_name and not p.deleting), None)
            seen.append(bound if bound is not None else (versions[-1][0], None))
        return seen

    def _read(self) -> StoppedResidency:
        residency = self._control.stopped_residency(self._sandbox_id)
        if self._sampling:
            self._observed.append(residency.lifecycle)
        return residency

    def _transitions(self) -> list[str]:
        """The sampled lifecycles with consecutive repeats collapsed."""

        collapsed: list[str] = []
        for lifecycle in self._observed:
            if not collapsed or collapsed[-1] != lifecycle:
                collapsed.append(lifecycle)
        return collapsed

    def _require_destination(self) -> AdminNodeView:
        if self._destination is None:
            raise RuntimeError("expect_replaced_stopped() must run first")
        return self._destination

    def _manager_log_evidence(self) -> Mapping[str, object]:
        return {
            "sandbox_manager_log": self._control.sandbox_manager_log(
                self._sandbox_id, since_time=self._since or _since_now()
            )
        }

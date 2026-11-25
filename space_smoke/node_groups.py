"""Org-scoped node-group placement, proven against a real scheduler.

A rendered `nodeAffinity` term whose values don't match what the labeler wrote,
or a missing `space.pplx.net/dedicated-org` toleration against a tainted
dedicated worker, isn't a failing unit assertion — it's a sandbox that sits
`Pending` forever while unit gates stay green. Proving the full rollout contract
needs real Kubernetes Node labels, real `NoSchedule` taints, and the real
`kube-scheduler`:

1. Gate off by default: a created pod carries zero node-group `nodeAffinity`
   terms and zero dedicated-org tolerations.
2. Provisioning order: `POST /admin/api/v1/node-groups` refuses a dedicated
   group with HTTP 409 naming the missing generic group when none exists.
3. Stage 0–2 non-regression (`SPACE_SANDBOX_NODE_GROUP_PLACEMENT=false` while
   the labeler and membership mirror are active and workers carry the default
   group's boundary label): both a `generic` create and an `org_specific`
   create, plus a pause/restore of that sandbox, reach `Running` with zero
   node-group `nodeAffinity` terms and zero dedicated-org tolerations.
4. Proof 4 (`SPACE_SANDBOX_NODE_GROUP_PLACEMENT=true` with no dedicated group
   for the template's owning org): creating from the owner's `org_specific`
   template fails closed with HTTP 409 without dispatching a pod, while a
   `generic` create for an org without a dedicated group renders
   `node-restriction.kubernetes.io/space-node-group In (<default>)` and reaches
   `Running`.
5. Proof 5 (dedicated group + boundary label + `space.pplx.net/dedicated-org`
   `NoSchedule` taint on one sandbox worker, leaving shared capacity untainted
   in `<default>`): the owner's `org_specific` create and its subsequent
   pause/restore render required `In (<dedicated>)` plus the matching `Equal`
   `NoSchedule` toleration and schedule onto the dedicated worker; a foreign org
   without a dedicated group schedules its `generic` create onto the untainted
   `<default>` worker and never lands on the dedicated worker, and its attempt
   to boot the owner's `org_specific` template is rejected with HTTP 404.

Three things this scenario leaves on a fresh stack, and says so: enabling the
labeler seeds the `default` generic group, which the API refuses to delete by
design; the membership mirror populates `nodes.node_group_id` with that default
group's id, which stays populated once the mirror is rolled back off before the
pre-run worker labels are restored; and the labeler writes every sandbox worker,
so pre-run boundary labels and dedicated taints are snapshotted and restored
after the dedicated group is deleted and the Deployment environment is rolled
back.
"""

from __future__ import annotations

import json
import secrets
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    AdminNodeGroupView,
    DedicatedOrgToleration,
    EcwSandboxDeleteRetryable,
    LimiterProbeOutcome,
    NodeGroupControl,
    RateLimitProbe,
    SandboxPodBoundaryRender,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

DEDICATED_TAINT_KEY = "space.pplx.net/dedicated-org"


@dataclass
class _ProbeTracker:
    probe: RateLimitProbe
    admitted: list[str] = field(default_factory=list)
    ambiguous_dispatches: int = 0


class NodeGroupAssertions:
    """Drive the node-group rollout stages through the public and admin routes and judge live pods."""

    def __init__(
        self,
        fixture: KindEcw,
        control: NodeGroupControl,
        probe: RateLimitProbe,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._probe = probe
        self._scope = scope
        self._sweep_guard: Compensation | None = None
        self._restore_taints: Compensation | None = None
        self._delete_dedicated: Compensation | None = None
        self._restore_env: Compensation | None = None
        self._restore_labels: Compensation | None = None
        self._release_lease: Compensation | None = None
        self._probes: dict[int, _ProbeTracker] = {}
        self._default_group_name: str | None = None
        self._dedicated_group_name: str | None = None
        self._dedicated_group_id: str | None = None
        self._dedicated_worker: str | None = None
        self._dedicated_delete_timed_out: bool = False
        # One dict for every create and sweep, unique per run: with only the
        # constant scenario and role tags a deferred `kill_matching` could match
        # a probe a later run created after this run's lease was released.
        self._metadata = {
            "space_smoke_scenario": "local.node_group_boundary_schedules",
            "space_smoke_role": "node-group-probe",
            "space_smoke_run": secrets.token_hex(8),
        }

    def _tracker(self, probe: RateLimitProbe) -> _ProbeTracker:
        key = id(probe)
        tracker = self._probes.get(key)
        if tracker is None:
            tracker = _ProbeTracker(probe=probe)
            self._probes[key] = tracker
        return tracker

    # --- phase 1 --------------------------------------------------------------

    def expect_inert_by_default(self) -> None:
        """With the gate off (the shipped default) the fixture pod carries no boundary term or taint toleration."""

        standing = self._control.node_group_env()
        self._fixture.record("node_group_env_before", dict(standing))
        gate_off = all(not _truthy(v) for k, v in standing.items() if k.endswith("PLACEMENT"))
        self._fixture.verify(
            gate_off,
            "the placement gate is already on; the inert-by-default claim can't be judged",
            env=dict(standing),
        )
        taints = self._control.worker_dedicated_taints()
        self._fixture.verify(
            all(value is None for value in taints.values()),
            "a worker already carries a dedicated-org taint before the run starts",
            worker_taints=dict(taints),
        )
        sandbox_id = self._fixture.sandbox.sandbox_id
        terms = self._control.sandbox_pod_node_group_terms(sandbox_id)
        self._fixture.verify(
            terms == [],
            "the gate is off but the pod carries a node-group affinity term",
            terms=terms,
        )
        boundary = self._control.sandbox_pod_boundary(sandbox_id)
        self._fixture.verify(
            boundary is not None,
            "the standing fixture sandbox has no live pod to inspect for default inertness",
            sandbox_id=sandbox_id,
        )
        assert boundary is not None
        self._expect_pod_inert(boundary, context="standing fixture pod with gate off")

    # --- phase 2 --------------------------------------------------------------

    def expect_dedicated_refused_without_generic(self, *, org_id: str, name: str) -> None:
        """A dedicated group is a 409 naming the missing generic group; skipped when one exists."""

        generic = [g for g in self._control.node_groups() if g.org_id is None]
        if generic:
            self._fixture.record(
                "provisioning_order_check",
                f"skipped: {len(generic)} generic group(s) already exist",
            )
            return
        status, body = self._control.try_create_node_group(name=name, org_id=org_id)
        self._fixture.verify(
            status == 409 and "generic" in body.lower(),
            "a dedicated group with no generic group must be refused with a 409 that names the fix",
            status=status,
            body=body[:300],
        )
        self._fixture.record("provisioning_order_check", "refused with 409")

    # --- cluster mutation arming & env transitions ----------------------------

    def _arm_cluster_compensations(self, *, rollout_timeout: float) -> None:
        if self._release_lease is not None:
            return
        release = self._control.acquire_public_api_mutation()
        self._release_lease = self._scope.defer("release the public API mutation lease", release)
        labels = self._control.worker_boundary_labels()
        self._fixture.record("worker_boundary_labels_before", dict(labels))
        self._restore_labels = self._scope.defer(
            "restore the workers' boundary labels",
            lambda: self._control.restore_worker_boundary_labels(labels),
            critical=True,
        )
        standing = self._control.node_group_env()

        def restore_env() -> None:
            # Gate off on the API first: with it off a stray group can't affect
            # a create while the controller is still rolling back.
            self._control.restore_node_group_env(standing)
            self._control.wait_public_api_ready(timeout=rollout_timeout)

        self._restore_env = self._scope.defer(
            "restore the node-group env", restore_env, critical=True
        )
        self._delete_dedicated = self._scope.defer(
            "delete the run-owned dedicated node group",
            self._cleanup_dedicated_group,
            critical=True,
        )
        taints = self._control.worker_dedicated_taints()
        self._fixture.record("worker_dedicated_taints_before", dict(taints))
        self._restore_taints = self._scope.defer(
            "restore the workers' dedicated-org taints",
            lambda: self._control.restore_worker_dedicated_taints(taints),
            critical=True,
        )
        self._sweep_guard = self._scope.defer(
            "kill every sandbox the probes admitted", self.sweep_admitted, critical=True
        )

    def enable_labeler_with_gate_off(self, *, rollout_timeout: float = 300) -> None:
        """Stage 0–2 shape: enable labeler and mirror while keeping the placement gate false."""

        self._arm_cluster_compensations(rollout_timeout=rollout_timeout)
        self._control.set_node_group_env(gate=False, labeler=True, mirror=True)
        self._control.wait_public_api_ready(timeout=rollout_timeout)

    def enable_placement_gate(self, *, rollout_timeout: float = 300) -> None:
        """Stage 3+ shape: flip `SPACE_SANDBOX_NODE_GROUP_PLACEMENT=true` alongside labeler and mirror."""

        self._arm_cluster_compensations(rollout_timeout=rollout_timeout)
        self._control.set_node_group_env(gate=True, labeler=True, mirror=True)
        self._control.wait_public_api_ready(timeout=rollout_timeout)

    def wait_for_default_group_and_labels(
        self, *, min_workers: int = 1, timeout: float = 120
    ) -> AdminNodeGroupView:
        """The labeler seeds the default generic group and labels workers, and the mirror stamps `nodes.node_group_id`."""

        deadline = time.monotonic() + timeout
        while True:
            default = next((g for g in self._control.node_groups() if g.is_default), None)
            labelled = (
                sorted(
                    n for n, v in self._control.worker_boundary_labels().items() if v == default.name
                )
                if default
                else []
            )
            labelled_set = set(labelled)
            # `default.nodes_count` counts NULL-membership workers too (`groups.rs`),
            # so check `/admin/api/v1/nodes` `node_group_id` directly to prove the
            # membership mirror actually published `default.id` onto the workers.
            mirrored = (
                sorted(
                    n.k8s_node_name
                    for n in self._control.admin_nodes()
                    if n.k8s_node_name in labelled_set and n.node_group_id == default.id
                )
                if default
                else []
            )
            if (
                default is not None
                and len(labelled) >= min_workers
                and len(mirrored) >= min_workers
            ):
                self._default_group_name = default.name
                self._fixture.record("default_group", default.name)
                self._fixture.record("default_group_nodes_count", default.nodes_count)
                self._fixture.record("labelled_workers", labelled)
                self._fixture.record("mirrored_workers", mirrored)
                return default
            self._fixture.verify(
                time.monotonic() < deadline,
                "the labeler seeded no default generic group, labelled too few workers, or the membership mirror hasn't stamped nodes.node_group_id yet",
                default_group=None if default is None else default.name,
                nodes_count=None if default is None else default.nodes_count,
                mirrored_workers=mirrored,
                min_workers=min_workers,
                labels=self._control.worker_boundary_labels(),
            )
            self._fixture.wait(3)

    # --- Stage 0–2 non-regression (gate=false, labeler+mirror=true) -----------

    def expect_stage_zero_to_two_inert(
        self,
        *,
        owner_probe: RateLimitProbe,
        generic_template: str,
        org_specific_template: str,
        schedule_timeout: float = 180,
    ) -> None:
        """With labels present and gate=false, generic create, org_specific create, and pause/restore render zero boundary terms."""

        generic_sbx, generic_pod = self._create_and_wait_running(
            owner_probe,
            template=generic_template,
            context="Stage 0–2 generic create with gate off",
            schedule_timeout=schedule_timeout,
        )
        self._expect_pod_inert(generic_pod, context="Stage 0–2 generic create with gate off")
        self._fixture.record("stage0_2_generic_node", generic_pod.node_name)
        self._kill_admitted(owner_probe, generic_sbx)

        private_sbx, private_pod = self._create_and_wait_running(
            owner_probe,
            template=org_specific_template,
            context="Stage 0–2 org_specific create with gate off",
            schedule_timeout=schedule_timeout,
        )
        self._expect_pod_inert(private_pod, context="Stage 0–2 org_specific create with gate off")
        restored_pod = self._pause_and_restore_running(
            owner_probe,
            private_sbx,
            previous_pod_name=private_pod.pod_name,
            context="Stage 0–2 org_specific pause/restore with gate off",
            schedule_timeout=schedule_timeout,
        )
        self._expect_pod_inert(
            restored_pod, context="Stage 0–2 org_specific restored pod with gate off"
        )
        self._fixture.record(
            "stage0_2_private_pods",
            {
                "initial_pod": private_pod.pod_name,
                "restored_pod": restored_pod.pod_name,
                "restored_node": restored_pod.node_name,
            },
        )
        self._kill_admitted(owner_probe, private_sbx)

    # --- Proof 4 (gate=true, no dedicated group for the owner org) ------------

    def expect_org_specific_fails_closed_without_dedicated_group(
        self,
        *,
        owner_probe: RateLimitProbe,
        owner_org_id: str,
        org_specific_template: str,
    ) -> None:
        """With gate=true and no dedicated group for `owner_org_id`, `org_specific` create fails closed with 409 and dispatches no pod."""

        dedicated_for_owner = [
            g.name for g in self._control.node_groups() if g.org_id == owner_org_id
        ]
        self._fixture.verify(
            not dedicated_for_owner,
            "owner org already has a dedicated node group before the fail-closed check",
            owner_org_id=owner_org_id,
            dedicated_groups=dedicated_for_owner,
        )
        pods_before = set(self._control.live_sandbox_pods())
        outcome = self._dispatch_create(owner_probe, template=org_specific_template)
        pods_after = set(self._control.live_sandbox_pods())
        dispatched_pods = sorted(pods_after - pods_before)
        lowered = outcome.message.lower()
        self._fixture.verify(
            outcome.status == 409
            and outcome.sandbox_id is None
            and "no node group" in lowered
            and org_specific_template.lower() in lowered,
            "an org_specific create with no compatible dedicated group must fail closed with HTTP 409 naming the template",
            status=outcome.status,
            sandbox_id=outcome.sandbox_id,
            response_message=outcome.message,
            template=org_specific_template,
        )
        self._fixture.verify(
            not dispatched_pods,
            "a 409 node-group admission refusal must not dispatch a sandbox pod",
            dispatched_pods=dispatched_pods,
        )
        self._fixture.record(
            "proof4_org_specific_refusal",
            {"status": outcome.status, "message": outcome.message},
        )

    def expect_generic_create_renders_and_schedules(
        self,
        default: AdminNodeGroupView,
        *,
        template: str,
        probe: RateLimitProbe | None = None,
        kill_after: bool = False,
        schedule_timeout: float = 180,
    ) -> str:
        """A generic create renders the boundary term naming the default group AND its pod runs."""

        active_probe = self._probe if probe is None else probe
        outcome = self._dispatch_create(active_probe, template=template)
        self._fixture.verify(
            outcome.status == 201 and outcome.sandbox_id is not None,
            "a generic create failed with the gate on; the boundary refused capacity it should admit",
            status=outcome.status,
            response_message=outcome.message,
        )
        assert outcome.sandbox_id is not None
        terms = self._control.sandbox_pod_node_group_terms(outcome.sandbox_id)
        self._fixture.verify(
            default.name in terms,
            "the gate is on but the pod's node-group term does not name the default group",
            terms=terms,
            default_group=default.name,
        )
        self._fixture.record("rendered_terms", terms)
        # The half no unit test can assert: the scheduler accepts the term. A
        # values/label mismatch shows up here and nowhere else — as a pod that
        # never runs.
        deadline = time.monotonic() + schedule_timeout
        while True:
            phase, node = self._control.sandbox_pod_phase_and_node(outcome.sandbox_id)
            if phase == "Running":
                boundary = self._control.sandbox_pod_boundary(outcome.sandbox_id)
                self._fixture.verify(
                    boundary is not None,
                    "generic create reached Running but has no live sandbox pod boundary to inspect",
                    sandbox_id=outcome.sandbox_id,
                )
                assert boundary is not None
                self._expect_pod_shared_boundary(
                    boundary,
                    expected_group=default.name,
                    context="generic create with gate on and no dedicated group",
                )
                self._fixture.record("scheduled_node", node)
                if kill_after:
                    self._kill_admitted(active_probe, outcome.sandbox_id)
                return outcome.sandbox_id
            self._fixture.verify(
                phase != "Failed" and time.monotonic() < deadline,
                "the pod carries the boundary term but never scheduled; the term and the labeler disagree",
                phase=phase,
                node=node,
                terms=terms,
            )
            self._fixture.wait(3)

    # --- Proof 5 (dedicated group + label + taint + pause/restore + foreign) --

    def expect_dedicated_isolation_and_restore(
        self,
        default: AdminNodeGroupView,
        *,
        dedicated_group_name: str,
        owner_org_id: str,
        owner_probe: RateLimitProbe,
        foreign_probe: RateLimitProbe,
        generic_template: str,
        org_specific_template: str,
        schedule_timeout: float = 180,
    ) -> None:
        """Provision a dedicated tainted worker and prove owner create+restore placement and foreign-org isolation."""

        self._default_group_name = default.name
        labels_before = self._control.worker_boundary_labels()
        sandbox_workers = sorted(
            node for node, value in labels_before.items() if value == default.name
        )
        self._fixture.verify(
            len(sandbox_workers) >= 2,
            "dedicated taint isolation proof needs at least two sandbox workers in the default group",
            default_group=default.name,
            worker_labels=labels_before,
        )
        _, fixture_node = self._control.sandbox_pod_phase_and_node(
            self._fixture.sandbox.sandbox_id
        )
        # Prefer a worker that isn't already hosting the standing fixture pod so
        # the dedicated worker has full Firecracker capacity for create+restore.
        unoccupied = [node for node in sandbox_workers if node != fixture_node]
        dedicated_worker = unoccupied[0] if unoccupied else sandbox_workers[0]
        shared_workers = [node for node in sandbox_workers if node != dedicated_worker]

        self._dedicated_group_id = None
        self._dedicated_group_name = None
        self._dedicated_delete_timed_out = False
        try:
            status, body = self._control.try_create_node_group(
                name=dedicated_group_name, org_id=owner_org_id
            )
        except BaseException:
            # A transport timeout or connection drop leaves commit uncertain;
            # reconcile by name during teardown.
            self._dedicated_group_name = dedicated_group_name
            raise
        if status in (200, 201):
            created_id: str | None = None
            try:
                parsed = json.loads(body)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                raw_id = parsed.get("id")
                if isinstance(raw_id, str) and raw_id:
                    created_id = raw_id
            if created_id is not None:
                self._dedicated_group_id = created_id
            else:
                self._dedicated_group_name = dedicated_group_name
        elif status == 0 or status >= 500:
            # Ambiguous gateway/server outcome: reconcile by name during teardown.
            self._dedicated_group_name = dedicated_group_name
        self._fixture.verify(
            status in (200, 201),
            "admin dedicated node-group creation failed after the default generic group existed",
            status=status,
            body=body[:300],
            dedicated_group=dedicated_group_name,
            owner_org_id=owner_org_id,
        )
        dedicated = next(
            (g for g in self._control.node_groups() if g.name == dedicated_group_name),
            None,
        )
        self._fixture.verify(
            dedicated is not None and dedicated.org_id == owner_org_id and not dedicated.is_default,
            "created dedicated node group is missing from the admin listing or has the wrong owner",
            dedicated=None if dedicated is None else asdict(dedicated),
            owner_org_id=owner_org_id,
        )
        assert dedicated is not None
        self._dedicated_group_id = dedicated.id
        self._dedicated_group_name = None
        self._dedicated_worker = dedicated_worker

        self._control.set_worker_boundary_label(dedicated_worker, dedicated.name)
        self._control.set_worker_dedicated_taint(dedicated_worker, owner_org_id)

        live_labels = self._control.worker_boundary_labels()
        live_taints = self._control.worker_dedicated_taints()
        self._fixture.verify(
            live_labels.get(dedicated_worker) == dedicated.name
            and all(live_labels.get(w) == default.name for w in shared_workers)
            and live_taints.get(dedicated_worker) == owner_org_id
            and all(live_taints.get(w) is None for w in shared_workers),
            "dedicated worker label/taint split didn't converge before placement assertions",
            dedicated_worker=dedicated_worker,
            shared_workers=shared_workers,
            labels=live_labels,
            taints=live_taints,
        )
        self._fixture.record(
            "proof5_topology",
            {
                "dedicated_group": dedicated.name,
                "dedicated_group_id": dedicated.id,
                "dedicated_worker": dedicated_worker,
                "shared_workers": shared_workers,
            },
        )

        # 1. Owner `org_specific` create lands on the tainted dedicated worker
        #    with required `In (<dedicated>)` and matching Equal/NoSchedule toleration.
        owner_sbx, owner_pod = self._create_and_wait_running(
            owner_probe,
            template=org_specific_template,
            context="Proof 5 owner org_specific create",
            schedule_timeout=schedule_timeout,
        )
        self._expect_pod_dedicated_boundary(
            owner_pod,
            expected_group=dedicated.name,
            owner_org_id=owner_org_id,
            expected_worker=dedicated_worker,
            expect_preferred=True,
            context="Proof 5 owner org_specific create",
        )

        # 2. Pause + restore of that sandbox waits for its portable full snapshot
        #    so `parse_pod_create_context` leaves `direct_node_name` unset and
        #    `kube-scheduler` evaluates the dedicated boundary affinity + taint
        #    toleration rather than a pre-bound `spec.nodeName`.
        restored_pod = self._pause_and_restore_running(
            owner_probe,
            owner_sbx,
            previous_pod_name=owner_pod.pod_name,
            context="Proof 5 owner org_specific pause/restore",
            schedule_timeout=schedule_timeout,
            require_portable_snapshot=True,
        )
        self._fixture.verify(
            restored_pod.has_restore_storage_uri,
            "Proof 5 owner org_specific restored pod: missing portable restore storage URI annotation; restore may have bypassed kube-scheduler",
            pod=restored_pod.pod_name,
            node=restored_pod.node_name,
        )
        self._expect_pod_dedicated_boundary(
            restored_pod,
            expected_group=dedicated.name,
            owner_org_id=owner_org_id,
            expected_worker=dedicated_worker,
            expect_preferred=False,
            context="Proof 5 owner org_specific restored pod",
        )
        self._fixture.record(
            "proof5_owner_pods",
            {
                "initial_pod": owner_pod.pod_name,
                "initial_node": owner_pod.node_name,
                "restored_pod": restored_pod.pod_name,
                "restored_node": restored_pod.node_name,
            },
        )
        self._kill_admitted(owner_probe, owner_sbx)

        # 3. Foreign org (no dedicated group) booting a `generic` template
        #    carries `In (<default>)`, no dedicated toleration, and schedules
        #    onto an untainted shared worker — never onto the dedicated worker.
        foreign_sbx, foreign_pod = self._create_and_wait_running(
            foreign_probe,
            template=generic_template,
            context="Proof 5 foreign org generic create",
            schedule_timeout=schedule_timeout,
        )
        self._expect_pod_shared_boundary(
            foreign_pod,
            expected_group=default.name,
            context="Proof 5 foreign org generic create",
        )
        self._fixture.verify(
            foreign_pod.node_name in shared_workers and foreign_pod.node_name != dedicated_worker,
            "foreign org generic pod scheduled outside untainted shared workers or onto the dedicated worker",
            scheduled_node=foreign_pod.node_name,
            dedicated_worker=dedicated_worker,
            shared_workers=shared_workers,
        )
        self._fixture.record("proof5_foreign_generic_node", foreign_pod.node_name)
        self._kill_admitted(foreign_probe, foreign_sbx)

        # 4. Foreign org attempting the owner's `org_specific` template is rejected (404).
        pods_before_foreign = set(self._control.live_sandbox_pods())
        foreign_refusal = self._dispatch_create(foreign_probe, template=org_specific_template)
        pods_after_foreign = set(self._control.live_sandbox_pods())
        dispatched_foreign_pods = sorted(pods_after_foreign - pods_before_foreign)
        self._fixture.verify(
            foreign_refusal.status == 404 and foreign_refusal.sandbox_id is None,
            "foreign org attempt on owner's org_specific template must be rejected with HTTP 404",
            status=foreign_refusal.status,
            sandbox_id=foreign_refusal.sandbox_id,
            response_message=foreign_refusal.message,
        )
        self._fixture.verify(
            not dispatched_foreign_pods,
            "rejected foreign org org_specific create must not dispatch a sandbox pod",
            dispatched_pods=dispatched_foreign_pods,
        )
        self._fixture.record("proof5_foreign_private_refusal_status", foreign_refusal.status)

    # --- shared pod & lifecycle helpers ---------------------------------------

    def _dispatch_create(self, probe: RateLimitProbe, *, template: str) -> LimiterProbeOutcome:
        tracker = self._tracker(probe)
        tracker.ambiguous_dispatches += 1
        outcome = probe.race_ecw_request(
            "POST",
            "/sandboxes",
            {"templateID": template, "timeout": 180, "metadata": dict(self._metadata)},
            client_id="space-smoke",
            timeout=180,
        )
        if outcome.sandbox_id:
            tracker.ambiguous_dispatches -= 1
            tracker.admitted.append(outcome.sandbox_id)
        elif 400 <= outcome.status < 500:
            # Definite 4xx refusals created nothing; transport failures (status 0),
            # 5xx errors, 2xx replies missing a sandboxID, and interrupted reads
            # that raised out of `race_ecw_request` stay eligible for metadata sweep.
            tracker.ambiguous_dispatches -= 1
        return outcome

    def _kill_admitted(self, probe: RateLimitProbe, sandbox_id: str) -> None:
        self._retry_delete(lambda: probe.kill_sandbox(sandbox_id))
        tracker = self._tracker(probe)
        if sandbox_id in tracker.admitted:
            tracker.admitted.remove(sandbox_id)

    def _retry_delete(self, delete: Callable[[], None], *, attempts: int = 5) -> None:
        for attempt in range(1, attempts + 1):
            try:
                delete()
                return
            except EcwSandboxDeleteRetryable:
                if attempt >= attempts:
                    raise
                self._fixture.wait(float(attempt), reason="retrying transient sandbox delete")

    def _create_and_wait_running(
        self,
        probe: RateLimitProbe,
        *,
        template: str,
        context: str,
        schedule_timeout: float,
    ) -> tuple[str, SandboxPodBoundaryRender]:
        outcome = self._dispatch_create(probe, template=template)
        self._fixture.verify(
            outcome.status == 201 and outcome.sandbox_id is not None,
            f"{context}: ECW sandbox create didn't succeed with 201",
            status=outcome.status,
            response_message=outcome.message,
            template=template,
        )
        assert outcome.sandbox_id is not None
        pod = self._wait_for_running_pod(
            outcome.sandbox_id,
            previous_pod_name=None,
            context=context,
            schedule_timeout=schedule_timeout,
        )
        return outcome.sandbox_id, pod

    def _pause_and_restore_running(
        self,
        probe: RateLimitProbe,
        sandbox_id: str,
        *,
        previous_pod_name: str,
        context: str,
        schedule_timeout: float,
        require_portable_snapshot: bool = False,
    ) -> SandboxPodBoundaryRender:
        paused = probe.race_ecw_request(
            "POST",
            f"/sandboxes/{sandbox_id}/pause",
            None,
            client_id="space-smoke",
            timeout=240,
        )
        self._fixture.verify(
            paused.status == 200,
            f"{context}: ECW pause (suspend) didn't return HTTP 200",
            sandbox_id=sandbox_id,
            status=paused.status,
            response_message=paused.message,
        )
        if require_portable_snapshot:
            self._wait_for_ready_full_snapshot(
                sandbox_id,
                context=context,
                timeout=schedule_timeout,
            )
        resumed = probe.race_ecw_request(
            "POST",
            f"/sandboxes/{sandbox_id}/resume",
            {"timeout": 180},
            client_id="space-smoke",
            timeout=240,
        )
        self._fixture.verify(
            resumed.status in (200, 201),
            f"{context}: ECW resume (restore) didn't return HTTP 200",
            sandbox_id=sandbox_id,
            status=resumed.status,
            response_message=resumed.message,
        )
        return self._wait_for_running_pod(
            sandbox_id,
            previous_pod_name=previous_pod_name,
            context=context,
            schedule_timeout=schedule_timeout,
        )

    def _wait_for_ready_full_snapshot(
        self,
        sandbox_id: str,
        *,
        context: str,
        timeout: float,
    ) -> None:
        deadline = time.monotonic() + timeout
        while True:
            snapshots = self._control.ready_full_snapshots(sandbox_id)
            if any(s.storage_location_count > 0 and bool(s.storage_uri) for s in snapshots):
                return
            if time.monotonic() >= deadline:
                self._fixture.verify(
                    False,
                    f"{context}: suspended sandbox never published a ready portable full snapshot",
                    sandbox_id=sandbox_id,
                    ready_snapshots=[
                        {
                            "snapshot_id": s.snapshot_id,
                            "storage_location_count": s.storage_location_count,
                        }
                        for s in snapshots
                    ],
                )
            self._fixture.wait(1, reason="waiting for suspended sandbox portable full snapshot")

    def _wait_for_running_pod(
        self,
        sandbox_id: str,
        *,
        previous_pod_name: str | None,
        context: str,
        schedule_timeout: float,
    ) -> SandboxPodBoundaryRender:
        deadline = time.monotonic() + schedule_timeout
        last: SandboxPodBoundaryRender | None = None
        while True:
            last = self._control.sandbox_pod_boundary(sandbox_id)
            if (
                last is not None
                and last.pod_name != (previous_pod_name or "")
                and last.phase == "Running"
            ):
                return last
            failed = (
                last is not None
                and last.pod_name != (previous_pod_name or "")
                and last.phase == "Failed"
            )
            self._fixture.verify(
                not failed and time.monotonic() < deadline,
                f"{context}: sandbox pod didn't reach Running within {schedule_timeout}s",
                sandbox_id=sandbox_id,
                previous_pod_name=previous_pod_name,
                pod=None if last is None else asdict(last),
            )
            self._fixture.wait(2)

    def _expect_pod_inert(self, pod: SandboxPodBoundaryRender, *, context: str) -> None:
        self._fixture.verify(
            pod.any_node_group_terms == () and pod.dedicated_org_tolerations == (),
            f"{context}: pod rendered node-group affinity or dedicated-org toleration while gate was off",
            pod_name=pod.pod_name,
            any_node_group_terms=list(pod.any_node_group_terms),
            dedicated_org_tolerations=[asdict(t) for t in pod.dedicated_org_tolerations],
        )

    def _expect_pod_shared_boundary(
        self,
        pod: SandboxPodBoundaryRender,
        *,
        expected_group: str,
        context: str,
    ) -> None:
        required_ok = bool(pod.required_node_group_sets) and all(
            term_groups == (expected_group,) for term_groups in pod.required_node_group_sets
        )
        only_required_in = bool(pod.any_node_group_terms) and all(
            term == f"required:In:{expected_group}" for term in pod.any_node_group_terms
        )
        self._fixture.verify(
            required_ok and only_required_in and pod.dedicated_org_tolerations == (),
            f"{context}: shared-capacity pod didn't render exclusively required In ({expected_group}) with zero dedicated tolerations",
            pod_name=pod.pod_name,
            required_node_group_sets=[list(groups) for groups in pod.required_node_group_sets],
            any_node_group_terms=list(pod.any_node_group_terms),
            dedicated_org_tolerations=[asdict(t) for t in pod.dedicated_org_tolerations],
        )

    def _expect_pod_dedicated_boundary(
        self,
        pod: SandboxPodBoundaryRender,
        *,
        expected_group: str,
        owner_org_id: str,
        expected_worker: str,
        expect_preferred: bool,
        context: str,
    ) -> None:
        expected_toleration = DedicatedOrgToleration(
            operator="Equal", value=owner_org_id, effect="NoSchedule"
        )
        required_ok = bool(pod.required_node_group_sets) and all(
            term_groups == (expected_group,) for term_groups in pod.required_node_group_sets
        )
        required_terms = [t for t in pod.any_node_group_terms if t.startswith("required:")]
        preferred_terms = [t for t in pod.any_node_group_terms if t.startswith("preferred:")]
        required_terms_ok = bool(required_terms) and all(
            t == f"required:In:{expected_group}" for t in required_terms
        )
        preferred_terms_ok = (
            preferred_terms == [f"preferred:In:{expected_group}"]
            if expect_preferred
            else preferred_terms == []
        )
        self._fixture.verify(
            required_ok
            and required_terms_ok
            and preferred_terms_ok
            and pod.dedicated_org_tolerations == (expected_toleration,)
            and pod.node_name == expected_worker,
            f"{context}: dedicated pod didn't carry expected node-group terms + {DEDICATED_TAINT_KEY}={owner_org_id}:NoSchedule on {expected_worker}",
            pod_name=pod.pod_name,
            node_name=pod.node_name,
            expected_worker=expected_worker,
            expect_preferred=expect_preferred,
            required_node_group_sets=[list(groups) for groups in pod.required_node_group_sets],
            any_node_group_terms=list(pod.any_node_group_terms),
            dedicated_org_tolerations=[asdict(t) for t in pod.dedicated_org_tolerations],
        )

    # --- teardown -------------------------------------------------------------

    def _cleanup_dedicated_group(self, *, delete_timeout: float = 90) -> None:
        if self._dedicated_worker is not None:
            self._control.set_worker_dedicated_taint(self._dedicated_worker, None)
            self._control.set_worker_boundary_label(
                self._dedicated_worker, self._default_group_name
            )
        group_id = self._dedicated_group_id
        if group_id is None and self._dedicated_group_name is not None:
            group_id = next(
                (
                    g.id
                    for g in self._control.node_groups()
                    if g.name == self._dedicated_group_name
                ),
                None,
            )
        if group_id is None:
            self._dedicated_group_name = None
            self._dedicated_worker = None
            return
        # When the node-group mirror stamped `nodes.node_group_id` on its 60 s
        # tick while the worker carried the dedicated label, `DELETE` answers
        # 409 until the next mirror tick observes the restored default label and
        # drops `nodes_count` to zero. If an earlier explicit `restore()` already
        # waited out the full budget, retry once immediately on `scope.close()`
        # so the failure is still recorded without burning a second 90 s wait.
        budget = 0.0 if self._dedicated_delete_timed_out else delete_timeout
        deadline = time.monotonic() + budget
        while True:
            status, body = self._control.try_delete_node_group(group_id)
            if status in (200, 204, 404):
                self._dedicated_group_id = None
                self._dedicated_group_name = None
                self._dedicated_worker = None
                self._dedicated_delete_timed_out = False
                return
            if status == 409 and time.monotonic() < deadline:
                self._fixture.wait(
                    3, reason="waiting for node-group mirror to clear dedicated membership"
                )
                continue
            if status == 409:
                self._dedicated_delete_timed_out = True
            raise RuntimeError(
                f"DELETE /admin/api/v1/node-groups/{group_id} returned HTTP {status}: {body[:300]}"
            )

    def sweep_admitted(self) -> None:
        """Kill every sandbox a probe admitted, per credential, by run metadata.

        Runs when a create was admitted and not yet deleted, and also when a
        dispatch ended in a transport failure or timeout: the server may have
        committed that create, and `kill_matching` reconciles by metadata. A
        clean refusal dispatched nothing, so it doesn't sweep.
        """

        for tracker in list(self._probes.values()):
            if tracker.admitted or tracker.ambiguous_dispatches:
                self._retry_delete(partial(tracker.probe.kill_matching, self._metadata))
                tracker.admitted.clear()
                tracker.ambiguous_dispatches = 0

    def restore(self) -> None:
        for slot in (
            "_sweep_guard",
            "_restore_taints",
            "_delete_dedicated",
            "_restore_env",
            "_restore_labels",
            "_release_lease",
        ):
            compensation = getattr(self, slot)
            if compensation is not None:
                setattr(self, slot, None)
                compensation.release()


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}

"""The admission fence over node-ctl's worker-resident `patch nodes` grant, proved live.

`patch nodes` cannot be scoped by RBAC to one Node or one field, and node-ctl
holds it from a DaemonSet resident on every sandbox worker. `node-fence.yaml`
supplies the granularity at admission as a `ValidatingAdmissionPolicy`, and
`kubectl auth can-i patch nodes` still answers `yes` for that ServiceAccount —
so only a real projected token in front of a real apiserver shows the writes
are refused. `smoke-kind-node-ctl-fence.sh` was that proof; this module is its
typed form.

Two halves, and the second is the one that would fail silently:

1. **Refusals, by rule.** With the token kubelet projects into the node-ctl
   pod on the fixture sandbox's worker (pod-bound, so the apiserver surfaces
   `authentication.kubernetes.io/node-name` for it, which the own-node rule
   keys off), each forbidden write is refused *by the named policy and by the
   expected rule's message*: a malformed patch, a missing verb and a fence
   pinned to the wrong identity all fail too, and only the rule distinguishes
   them. Then the refused writes must have left nothing behind, and a control
   write from the unfenced admin identity shows the refused write was
   otherwise possible.
2. **Permissive half.** The allowlists are the complete set of Node metadata
   keys node-ctl's reconcilers touch; getting them wrong breaks node-ctl on
   every cluster at once and the refusals cannot see it. So every write shape
   node-ctl has is replayed with the same credential — the SSD placement
   apply, the template-presence apply and its same-manager prune, the merge
   patch clearing a `template-write` annotation, the `nodes/status` capacity
   apply — and then a label and the slot capacity node-ctl owns are stripped
   and the *running binary* has to write both back through its own credential.
   The two canaries run concurrently: node-ctl rides a 60 s poll, so each is
   up to a poll plus slack, and serially they were the bulk of the shell run.

Residue: every field a probe can touch is snapshotted before the probes and
reverted on every exit path, keyed on markers the run plants (a forged
condition's `reason`, a probe key marker, a resource nothing in the system
writes) rather than on live-vs-snapshot drift, because node-ctl and the kubelet
rewrite Node status continuously. The one exception is the slot capacity the
run strips itself: only the fence *holding* puts it back, so its repair is
keyed on the strip and on the value still being absent — every sandbox pod
requests one unit, so a node left without it is every later scenario
unschedulable.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    FencePolicy,
    NodeFenceControl,
    NodeCtlIdentity,
    SaWriteOutcome,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SCENARIO_ID = "local.node_ctl_fence_refuses_and_reconciles"
TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
NODE_CLAIM_EXTRA = "authentication.kubernetes.io/node-name"

# Keys the fence must refuse. Literals, not values-file reads: the boundary is
# asserted from the outside, and building the attack out of the allowlist would
# make a widened allowlist pass.
BOUNDARY_LABEL = "node-restriction.kubernetes.io/space-node-group"
DEDICATED_TAINT_KEY = "space.pplx.net/dedicated-org"
CRED_AUTHORITY_POOL_LABEL = "space.pplx.net/node-pool"
PROBE_FINALIZER = "space.pplx.net/smoke-fence-probe"
PROBE_CONDITION_REASON = "SmokeFenceProbeForged"
OWNED_LABEL = "space.pplx.net/ssd-headroom-tier"
BLOCKED_LABEL = "space.pplx.net/ssd-placement-blocked"
SLOTS_RESOURCE = "space.pplx.net/network-slots"
FOREIGN_CAPACITY_RESOURCE = "nvidia.com/gpu"
PROBE_KEY_MARKER = "smoke-fence-probe"
PROBE_PRESENCE_KEY = f"space.pplx.net/template-build-{PROBE_KEY_MARKER}"
PROBE_WRITE_KEY = f"space.pplx.net/template-write-{PROBE_KEY_MARKER}"

# Rule-message fragments of `node-fence.yaml`; a refusal must carry the policy
# name and the fragment of the rule the probe is aimed at.
RULE_OWN_NODE = "may only write the Node it runs on"
RULE_SPEC = "may not write a Node's .spec"
RULE_LABELS = "may only write the Node labels it owns"
RULE_FINALIZERS = "may not write a Node's finalizers"
RULE_CAPACITY = "may only advertise its own extended resource"
RULE_STATUS_FROZEN = "may not write a Node's status outside its own extended resource"
RULE_TEMPLATE_WRITE = "may only clear a template-write annotation"

# node-ctl's facet poll is 60 s (`DEFAULT_POLL_INTERVAL_SECS`); the kind chart
# sets none. Three periods of slack is a ceiling for a stalled run, not an
# expected duration: observed convergence is 10–60 s.
RECONCILE_BUDGET_SECONDS = 180.0
RECONCILE_POLL_SECONDS = 5.0


@dataclass(frozen=True)
class Probe:
    label: str
    rule: str
    node: str  # "own" | "other"
    patch: Mapping[str, object]
    subresource: str | None = None


def forbidden_probes() -> tuple[Probe, ...]:
    """The shell suite's refusal list, unchanged in substance; the last four give the residue checks a possible presence on the other node."""

    taint = {"spec": {"taints": [{"key": DEDICATED_TAINT_KEY, "value": "forged", "effect": "NoSchedule"}]}}
    boundary = {"metadata": {"labels": {BOUNDARY_LABEL: "victim-org"}}}
    forged_condition = {
        "status": {
            "conditions": [
                {
                    "type": "Ready",
                    "status": "False",
                    "reason": PROBE_CONDITION_REASON,
                    "lastHeartbeatTime": "2026-01-01T00:00:00Z",
                    "lastTransitionTime": "2026-01-01T00:00:00Z",
                }
            ]
        }
    }
    return (
        Probe("dedicated-org taint on its own node", RULE_SPEC, "own", taint),
        Probe("dedicated-org taint on another node", RULE_OWN_NODE, "other", taint),
        Probe("node-group boundary label on its own node", RULE_LABELS, "own", boundary),
        Probe("node-group boundary label through nodes/status", RULE_LABELS, "own", boundary, "status"),
        Probe(
            "cred-authority pool label on its own node",
            RULE_LABELS,
            "own",
            {"metadata": {"labels": {CRED_AUTHORITY_POOL_LABEL: "cred-authority"}}},
        ),
        Probe("an owned label on another node", RULE_OWN_NODE, "other", {"metadata": {"labels": {OWNED_LABEL: "0"}}}),
        Probe("cordoning its own node", RULE_SPEC, "own", {"spec": {"unschedulable": True}}),
        Probe("a finalizer on its own node", RULE_FINALIZERS, "own", {"metadata": {"finalizers": [PROBE_FINALIZER]}}),
        Probe(
            "capacity on another node's status",
            RULE_OWN_NODE,
            "other",
            {"status": {"capacity": {SLOTS_RESOURCE: "1"}}},
            "status",
        ),
        Probe(
            "a foreign extended resource on its own status.capacity",
            RULE_CAPACITY,
            "own",
            {"status": {"capacity": {FOREIGN_CAPACITY_RESOURCE: "8"}}},
            "status",
        ),
        Probe(
            "a foreign extended resource on its own status.allocatable",
            RULE_STATUS_FROZEN,
            "own",
            {"status": {"allocatable": {FOREIGN_CAPACITY_RESOURCE: "8"}}},
            "status",
        ),
        Probe(
            "inflating its own resource in status.allocatable",
            RULE_STATUS_FROZEN,
            "own",
            {"status": {"allocatable": {SLOTS_RESOURCE: "100000"}}},
            "status",
        ),
        Probe("forging its own Ready condition", RULE_STATUS_FROZEN, "own", forged_condition, "status"),
        Probe(
            "adding a template-write pull request on its own node",
            RULE_TEMPLATE_WRITE,
            "own",
            {"metadata": {"annotations": {PROBE_WRITE_KEY: "test-template-restricted:latest"}}},
        ),
        Probe("cordoning another node", RULE_OWN_NODE, "other", {"spec": {"unschedulable": True}}),
        Probe("a finalizer on another node", RULE_OWN_NODE, "other", {"metadata": {"finalizers": [PROBE_FINALIZER]}}),
        Probe("the node-group boundary label on another node", RULE_OWN_NODE, "other", boundary),
        Probe("forging a condition on another node", RULE_OWN_NODE, "other", forged_condition, "status"),
    )


def judge_refusal(outcome: SaWriteOutcome, *, policy: str, rule: str) -> str | None:
    """None when the write was refused by `policy` with `rule`'s message; else why not."""

    if outcome.returncode == 0:
        return "the write SUCCEEDED"
    if policy not in outcome.output:
        return f"refused, but not by {policy}"
    if rule not in outcome.output:
        return f"refused by the wrong rule (expected {rule!r})"
    return None


def residue(node: Mapping[str, object]) -> list[str]:
    """What a probe that got through would have left on a Node, by marker."""

    spec = node.get("spec") or {}
    meta = node.get("metadata") or {}
    status = node.get("status") or {}
    found: list[str] = []
    if any(t.get("key") == DEDICATED_TAINT_KEY for t in (spec.get("taints") or [])):  # type: ignore[union-attr]
        found.append("dedicated-org taint")
    if BOUNDARY_LABEL in (meta.get("labels") or {}):  # type: ignore[operator]
        found.append("forged boundary label")
    if spec.get("unschedulable"):  # type: ignore[union-attr]
        found.append("cordoned")
    if PROBE_FINALIZER in (meta.get("finalizers") or []):  # type: ignore[operator]
        found.append("probe finalizer")
    if any(c.get("reason") == PROBE_CONDITION_REASON for c in (status.get("conditions") or [])):  # type: ignore[union-attr]
        found.append("forged condition")
    for map_name in ("capacity", "allocatable"):
        if FOREIGN_CAPACITY_RESOURCE in (status.get(map_name) or {}):  # type: ignore[operator]
            found.append(f"forged {FOREIGN_CAPACITY_RESOURCE} in status.{map_name}")
    return found


def slots_repair_action(*, armed: bool, live: str | None) -> str:
    """The repair for the capacity the run strips itself: `restore` | `leave` | `nothing`.

    Keyed on the run's own strip (`armed`) and on the value still being absent,
    never on a live-vs-snapshot comparison: node-ctl re-advertising a different
    number is it doing its job, an unarmed run has no strip to undo, and an
    unreadable node (`live is None`) is repaired rather than skipped — one no-op
    write against a node left unschedulable.
    """

    if not armed:
        return "nothing"
    if live:
        return "leave"
    return "restore"


def owned_labels_manifest(node: str) -> str:
    return (
        f"apiVersion: v1\nkind: Node\nmetadata:\n  name: {node}\n  labels:\n"
        f'    {OWNED_LABEL}: "3"\n    {BLOCKED_LABEL}: "false"\n'
    )


def presence_manifest(node: str, *, present: bool) -> str:
    head = f"apiVersion: v1\nkind: Node\nmetadata:\n  name: {node}\n"
    if not present:
        return head
    return head + f'  labels:\n    {PROBE_PRESENCE_KEY}: "true"\n  annotations:\n    {PROBE_PRESENCE_KEY}: "sha256:smoke"\n'


def slots_manifest(node: str, slots: str) -> str:
    return f"apiVersion: v1\nkind: Node\nmetadata:\n  name: {node}\nstatus:\n  capacity:\n    {SLOTS_RESOURCE}: \"{slots}\"\n"


class NodeFenceAssertions:
    """Read the worker's own credential, probe the fence by rule, replay node-ctl's writes, watch it reconcile."""

    def __init__(self, fixture: KindEcw, control: NodeFenceControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._identity: NodeCtlIdentity | None = None
        self._other_node: str | None = None
        self._policy: FencePolicy | None = None
        self._kubeconfig: str | None = None
        self._snapshots: dict[str, Mapping[str, object]] = {}
        self._release_lease: Compensation | None = None
        self._reverts: list[Compensation] = []
        self._slots: str | None = None
        self._slots_stripped = False
        self._slots_repair: Compensation | None = None

    # --- locate + fence -------------------------------------------------------

    def locate(self) -> NodeCtlIdentity:
        identity = self._control.node_ctl_identity_for_sandbox(self._fixture.sandbox.sandbox_id)
        others = [n for n in self._control.kube_node_names() if n != identity.node_name]
        self._fixture.verify(
            bool(others),
            "the own-node rule needs a second node; this cluster has one",
            nodes=self._control.kube_node_names(),
        )
        self._identity, self._other_node = identity, others[0]
        self._fixture.record("own_node", identity.node_name)
        self._fixture.record("other_node", self._other_node)
        self._fixture.record("node_ctl_pod", identity.pod_name)
        self._fixture.record("service_account", identity.service_account)
        return identity

    def expect_fence_enforcing(self) -> FencePolicy:
        identity = self._require_identity()
        policy = self._control.fence_policy_for(identity.service_account)
        self._fixture.verify(
            policy is not None,
            "no ValidatingAdmissionPolicy matches the node-ctl ServiceAccount",
            service_account=identity.service_account,
        )
        assert policy is not None
        self._fixture.verify(
            policy.validation_actions == ["Deny"],
            "the fence is not bound with validationActions=[Deny]; this scenario asserts refusals, which only Deny produces",
            policy=policy.name,
            actions=policy.validation_actions,
        )
        self._fixture.verify(
            policy.type_check_warnings == 0,
            "the fence has CEL type-checking warnings; on this policy every warning has meant an expression that cannot evaluate",
            policy=policy.name,
            warnings=policy.type_check_warnings,
        )
        self._policy = policy
        self._fixture.record("policy", policy.name)
        return policy

    # --- credential -----------------------------------------------------------

    def read_credential(self) -> str:
        """The projected token from the running container; a pod-bound TokenRequest only when the mount is absent."""

        identity = self._require_identity()
        if self._control.pod_file_readable(identity.pod_name, TOKEN_PATH):
            token = self._control.pod_file(identity.pod_name, TOKEN_PATH)
            source = "projected-mount"
        else:
            token = self._control.bound_token(identity)
            source = "tokenrequest-bound-to-pod"
        self._fixture.verify(bool(token.strip()), "obtained an empty token", source=source)
        self._fixture.record("token_source", source)
        self._kubeconfig = self._control.kubeconfig_with_token(token.strip())
        whoami = self._control.whoami(self._kubeconfig)
        expected = f"system:serviceaccount:{self._control.namespace}:{identity.service_account}"
        self._fixture.verify(
            whoami.username == expected,
            "the SA kubeconfig does not authenticate as the node-ctl ServiceAccount; every refusal would be meaningless",
            username=whoami.username,
            expected=expected,
        )
        self._fixture.verify(
            self._control.can_i_patch_nodes(self._kubeconfig),
            "RBAC no longer grants this ServiceAccount patch on nodes; the fence is asserted on top of that grant",
        )
        self._fixture.verify(
            whoami.node_claim == identity.node_name,
            "the apiserver does not surface this pod's node name for its token; the own-node rule would refuse every node-ctl write",
            node_claim=whoami.node_claim,
            own_node=identity.node_name,
        )
        self._fixture.record("node_claim", whoami.node_claim)
        return source

    # --- snapshots + reverts ----------------------------------------------------

    def snapshot_and_arm(self) -> None:
        """Snapshot every field a probe can touch on both nodes and arm the reverts (and the slots repair) first."""

        identity = self._require_identity()
        release = self._control.acquire_worker_cordon_mutation()
        self._release_lease = self._scope.defer("release the worker cordon lease", release)
        for node in (identity.node_name, self._require_other()):
            self._snapshots[node] = self._control.node_json(node)
            self._reverts.append(
                self._scope.defer(f"revert probe residue on {node}", lambda node=node: self._revert(node), critical=True)
            )
        self._slots_repair = self._scope.defer("restore the stripped slot capacity", self._repair_slots, critical=True)

    def _revert(self, node: str) -> None:
        snapshot = self._snapshots.get(node)
        if snapshot is None:
            return
        repairs = self._control.revert_node_from_snapshot(node, snapshot)
        if repairs:
            self._fixture.record(f"REPAIR:{node}", repairs)

    def _repair_slots(self) -> None:
        identity = self._require_identity()
        live = self._control.node_capacity(identity.node_name, SLOTS_RESOURCE)
        action = slots_repair_action(armed=self._slots_stripped, live=live)
        if action == "restore" and self._slots:
            self._control.admin_patch_node(
                identity.node_name, {"status": {"capacity": {SLOTS_RESOURCE: self._slots}}}, subresource="status"
            )
            self._fixture.record(f"REPAIR:{identity.node_name}:slots", self._slots)

    # --- refusals -----------------------------------------------------------------

    def expect_refusals(self) -> list[str]:
        identity, other, policy = self._require_identity(), self._require_other(), self._require_policy()
        passed: list[str] = []
        for probe in forbidden_probes():
            node = identity.node_name if probe.node == "own" else other
            outcome = self._control.patch_node_as(
                self._require_kubeconfig(), node, probe.patch, subresource=probe.subresource
            )
            why = judge_refusal(outcome, policy=policy.name, rule=probe.rule)
            self._fixture.verify(
                why is None,
                f"refusal probe failed — {probe.label}: {why or ''}",
                probe=probe.label,
                node=node,
                returncode=outcome.returncode,
                output=outcome.output[:600],
            )
            passed.append(probe.label)
        self._fixture.record("refusals", passed)
        return passed

    def expect_no_residue(self) -> None:
        for node in (self._require_identity().node_name, self._require_other()):
            found = residue(self._control.node_json(node))
            self._fixture.verify(found == [], "a refused write left residue on a node", node=node, residue=found)
        self._fixture.record("residue_after_refusals", "none")

    def expect_control_write_then_refused_strip(self) -> None:
        """The boundary label is writable by the unfenced admin, and the SA may not strip it; admin removes it."""

        own, policy = self._require_identity().node_name, self._require_policy()
        self._control.admin_label_node(own, BOUNDARY_LABEL, "smoke-control")
        try:
            outcome = self._control.patch_node_as(
                self._require_kubeconfig(), own, {"metadata": {"labels": {BOUNDARY_LABEL: None}}}
            )
            why = judge_refusal(outcome, policy=policy.name, rule=RULE_LABELS)
            self._fixture.verify(
                why is None,
                f"stripping an existing boundary label from its own node: {why or ''}",
                output=outcome.output[:600],
            )
        finally:
            self._control.admin_label_node(own, BOUNDARY_LABEL, None)
        self._fixture.record("control_write", "admin wrote and removed the boundary label; the SA strip was refused by the label rule")

    # --- permissive half ------------------------------------------------------------

    def expect_owned_writes_succeed(self) -> None:
        own, kubeconfig = self._require_identity().node_name, self._require_kubeconfig()
        steps: list[str] = []

        def ok(label: str, outcome: SaWriteOutcome) -> None:
            self._fixture.verify(outcome.returncode == 0, f"node-ctl's own write was refused — {label}", output=outcome.output[:600])
            steps.append(label)

        ok(
            "the ssd placement labels (server-side apply)",
            self._control.apply_as(kubeconfig, owned_labels_manifest(own), field_manager="space-node-ctl-ssd-headroom-tier"),
        )
        ok(
            "a template-presence label and annotation (server-side apply)",
            self._control.apply_as(kubeconfig, presence_manifest(own, present=True), field_manager="space-node-ctl-fence-smoke"),
        )
        # Re-applying with the same manager and none of the previous keys is how
        # template presence drops a build no longer local: a key removal has to
        # pass the same allowlist as a key write.
        ok(
            "pruning that presence key back off (server-side apply)",
            self._control.apply_as(kubeconfig, presence_manifest(own, present=False), field_manager="space-node-ctl-fence-smoke"),
        )
        node = self._control.node_json(own)
        left = [
            k
            for k in list((node.get("metadata") or {}).get("labels") or {}) + list((node.get("metadata") or {}).get("annotations") or {})  # type: ignore[union-attr]
            if k == PROBE_PRESENCE_KEY
        ]
        self._fixture.verify(left == [], "a probe presence key survived the pruning apply", left=left)
        # A reference node-ctl can never satisfy, so its own reconciler leaves it alone.
        self._control.admin_annotate_node(own, PROBE_WRITE_KEY, "smoke-fence-probe-nonexistent:latest")
        planted = ((self._control.node_json(own).get("metadata") or {}).get("annotations") or {}).get(PROBE_WRITE_KEY)  # type: ignore[union-attr]
        self._fixture.verify(bool(planted), "the planted template-write annotation is already gone; clearing it would pass vacuously")
        ok(
            "clearing a template-write annotation (merge)",
            self._control.patch_node_as(kubeconfig, own, {"metadata": {"annotations": {PROBE_WRITE_KEY: None}}}),
        )
        slots = self._control.node_capacity(own, SLOTS_RESOURCE)
        self._fixture.verify(bool(slots), "the node advertises no network-slots capacity; nothing for the re-advertisement case to restore")
        assert slots
        self._slots = slots
        self._fixture.record("slots_capacity", slots)
        # Strip as admin first so the SA's apply is a real key addition; armed before the write.
        self._slots_stripped = True
        self._control.admin_remove_capacity(own, SLOTS_RESOURCE)
        if self._control.node_capacity(own, SLOTS_RESOURCE):
            self._fixture.verify(
                False, "node-ctl re-advertised network-slots before the apply could; that case would have passed vacuously"
            )
        ok(
            "re-advertising network-slots capacity (status apply)",
            self._control.apply_as(kubeconfig, slots_manifest(own, slots), field_manager="space-node-ctl-network-slots", subresource="status"),
        )
        self._fixture.record("owned_writes", steps)

    # --- convergence ------------------------------------------------------------------

    def expect_node_ctl_reconciles(self, *, budget: float = RECONCILE_BUDGET_SECONDS) -> dict[str, float]:
        """Strip an owned label and the slot capacity; the running node-ctl writes both back through its own credential.

        Concurrent, not serial: both ride node-ctl's 60 s poll, and waiting them
        out one after the other was the bulk of the shell run.
        """

        own = self._require_identity().node_name
        before = self._control.node_label(own, OWNED_LABEL)
        self._fixture.verify(bool(before), "the node has no ssd-headroom-tier label to begin with; node-ctl's ssd reconcile is not running")
        self._control.admin_label_node(own, OWNED_LABEL, None)
        self._slots_stripped = True
        self._control.admin_remove_capacity(own, SLOTS_RESOURCE)
        started = time.monotonic()
        deadline = started + budget
        seen: dict[str, float] = {}
        while len(seen) < 2:
            self._fixture.wait(RECONCILE_POLL_SECONDS)
            if "label" not in seen and self._control.node_label(own, OWNED_LABEL):
                seen["label"] = round(time.monotonic() - started, 1)
            if "slots" not in seen and self._control.node_capacity(own, SLOTS_RESOURCE):
                seen["slots"] = round(time.monotonic() - started, 1)
            self._fixture.verify(
                len(seen) == 2 or time.monotonic() < deadline,
                "node-ctl did not write back an owned label and its slot capacity within the budget; check its logs for an admission denial",
                reconciled=dict(seen),
                budget_seconds=budget,
                node_ctl_log_tail=self._control.node_ctl_log_tail(self._require_identity().pod_name, 40),
            )
        self._fixture.record("reconciled_after_seconds", seen)
        return seen

    # --- teardown ---------------------------------------------------------------------

    def retire(self) -> None:
        """Run the reverts and the slots repair now, then verify both nodes are clean."""

        for compensation in [*self._reverts, self._slots_repair, self._release_lease]:
            if compensation is not None:
                compensation.release()
        self._reverts = []
        self._slots_repair = None
        self._release_lease = None
        for node in (self._require_identity().node_name, self._require_other()):
            found = residue(self._control.node_json(node))
            self._fixture.verify(found == [], "residue remains after the reverts", node=node, residue=found)
        self._fixture.record("final_residue", "none")

    def _require_identity(self) -> NodeCtlIdentity:
        if self._identity is None:
            raise RuntimeError("locate() must run first")
        return self._identity

    def _require_other(self) -> str:
        if self._other_node is None:
            raise RuntimeError("locate() must run first")
        return self._other_node

    def _require_policy(self) -> FencePolicy:
        if self._policy is None:
            raise RuntimeError("expect_fence_enforcing() must run first")
        return self._policy

    def _require_kubeconfig(self) -> str:
        if self._kubeconfig is None:
            raise RuntimeError("read_credential() must run first")
        return self._kubeconfig


def probe_labels(probes: Sequence[Probe]) -> list[str]:
    return [p.label for p in probes]


__all__ = [
    "BOUNDARY_LABEL",
    "NodeFenceAssertions",
    "Probe",
    "forbidden_probes",
    "judge_refusal",
    "residue",
    "slots_repair_action",
    "json",
]

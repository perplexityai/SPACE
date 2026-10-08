"""Contracts of the node-ctl fence scenario the bench cannot exercise.

A passing bench shows every probe refused by the right rule and node-ctl
converging. It cannot show that a probe which *succeeds* fails the run, that a
refusal by the wrong policy or the wrong rule fails, that residue on either
node fails, that the reverts and the slots repair are armed before any probe
and run on scope close, that the slots repair is keyed on the run's own strip
and on the value being absent (never on drift), that a fence bound Audit or
carrying type-check warnings is refused before any probe, that a token whose
node claim is not this node is refused, or that the kubectl ops build the
ServiceAccount calls with a token-only kubeconfig.
"""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.capabilities import FencePolicy, NodeCtlIdentity, SaWriteOutcome, WhoAmI
from space_smoke.node_fence import (
    BOUNDARY_LABEL,
    FOREIGN_CAPACITY_RESOURCE,
    OWNED_LABEL,
    PROBE_CONDITION_REASON,
    PROBE_FINALIZER,
    RULE_LABELS,
    RULE_OWN_NODE,
    SLOTS_RESOURCE,
    NodeFenceAssertions,
    forbidden_probes,
    judge_refusal,
    residue,
    slots_repair_action,
)
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope

SID = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
POLICY = "space-node-ctl-node-fence"
OWN, OTHER = "space-local-worker", "space-local-worker2"


class Pure(unittest.TestCase):
    def test_refusal_verdicts(self) -> None:
        refused = SaWriteOutcome(1, f'error: admission webhook denied: ValidatingAdmissionPolicy "{POLICY}" ... {RULE_LABELS} ...')
        self.assertIsNone(judge_refusal(refused, policy=POLICY, rule=RULE_LABELS))
        self.assertIn("SUCCEEDED", judge_refusal(SaWriteOutcome(0, "node/x patched"), policy=POLICY, rule=RULE_LABELS) or "")
        self.assertIn("not by", judge_refusal(SaWriteOutcome(1, "error: forbidden by RBAC"), policy=POLICY, rule=RULE_LABELS) or "")
        self.assertIn("wrong rule", judge_refusal(refused, policy=POLICY, rule=RULE_OWN_NODE) or "")

    def test_probe_list_covers_both_nodes_and_both_paths(self) -> None:
        probes = forbidden_probes()
        self.assertGreaterEqual(len(probes), 18)
        self.assertTrue(any(p.node == "other" for p in probes))
        self.assertTrue(any(p.subresource == "status" for p in probes))
        self.assertEqual(len({p.label for p in probes}), len(probes))

    def test_residue_by_marker(self) -> None:
        clean = {"spec": {}, "metadata": {"labels": {"kubernetes.io/os": "linux"}}, "status": {"capacity": {SLOTS_RESOURCE: "7"}}}
        self.assertEqual(residue(clean), [])
        dirty = {
            "spec": {"unschedulable": True, "taints": [{"key": "space.pplx.net/dedicated-org"}]},
            "metadata": {"labels": {BOUNDARY_LABEL: "x"}, "finalizers": [PROBE_FINALIZER]},
            "status": {"conditions": [{"reason": PROBE_CONDITION_REASON}], "allocatable": {FOREIGN_CAPACITY_RESOURCE: "8"}},
        }
        self.assertEqual(len(residue(dirty)), 6)

    def test_slots_repair_is_keyed_on_the_strip_and_on_absence(self) -> None:
        self.assertEqual(slots_repair_action(armed=True, live=None), "restore")
        self.assertEqual(slots_repair_action(armed=True, live=""), "restore")
        self.assertEqual(slots_repair_action(armed=True, live="9"), "leave")
        self.assertEqual(slots_repair_action(armed=False, live=None), "nothing")


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SID)
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        pass


def _node(name: str, *, labels: dict[str, str] | None = None, capacity: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "metadata": {"name": name, "labels": labels if labels is not None else {OWNED_LABEL: "2", "kubernetes.io/os": "linux"}},
        "spec": {},
        "status": {"capacity": capacity if capacity is not None else {SLOTS_RESOURCE: "7"}, "conditions": [{"type": "Ready", "status": "True"}]},
    }


class _Control:
    namespace = "space"

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.identity = NodeCtlIdentity("space-node-ctl-a", "uid-1", OWN, "space-node-ctl")
        self.nodes = {OWN: _node(OWN), OTHER: _node(OTHER)}
        self.policy: FencePolicy | None = FencePolicy(POLICY, ["Deny"], 0)
        self.token_readable = True
        self.who = WhoAmI("system:serviceaccount:space:space-node-ctl", OWN)
        self.can_patch = True
        self.refuse_all = True
        self.reconcile_after_polls = 1
        self._polls = 0

    def acquire_worker_cordon_mutation(self):
        self.calls.append("lease")
        return lambda: self.calls.append("lease:release")

    def node_ctl_identity_for_sandbox(self, sandbox_id):
        return self.identity

    def kube_node_names(self):
        return sorted(self.nodes)

    def fence_policy_for(self, service_account):
        return self.policy

    def pod_file_readable(self, pod, path):
        return self.token_readable

    def pod_file(self, pod, path):
        self.calls.append("read-token")
        return "tok\n"

    def bound_token(self, identity):
        self.calls.append("token-request")
        return "bound-tok"

    def kubeconfig_with_token(self, token):
        return "/tmp/sa.kubeconfig"

    def whoami(self, kubeconfig):
        return self.who

    def can_i_patch_nodes(self, kubeconfig):
        return self.can_patch

    def node_json(self, node):
        return json.loads(json.dumps(self.nodes[node]))

    def node_label(self, node, key):
        return self.nodes[node]["metadata"]["labels"].get(key)

    def node_capacity(self, node, resource):
        return self.nodes[node]["status"]["capacity"].get(resource)

    def patch_node_as(self, kubeconfig, node, patch, *, subresource=None):
        self.calls.append(f"sa-patch:{node}:{subresource or 'node'}")
        clears_template_write = node == OWN and patch == {
            "metadata": {"annotations": {"space.pplx.net/template-write-smoke-fence-probe": None}}
        }
        if self.refuse_all and not clears_template_write:
            rule = "may only write the Node it runs on" if node != OWN else _rule_for(patch, subresource)
            return SaWriteOutcome(1, f'ValidatingAdmissionPolicy "{POLICY}" denied: space-node-ctl {rule}')
        # A fence that admits: apply the patch to the fake node.
        _apply_patch(self.nodes[node], patch)
        return SaWriteOutcome(0, "node patched")

    def apply_as(self, kubeconfig, manifest, *, field_manager, subresource=None):
        self.calls.append(f"sa-apply:{field_manager}:{subresource or 'node'}")
        if "template-build-smoke-fence-probe" in manifest and "labels" in manifest:
            self.nodes[OWN]["metadata"]["labels"]["space.pplx.net/template-build-smoke-fence-probe"] = "true"
        elif field_manager == "space-node-ctl-fence-smoke":
            self.nodes[OWN]["metadata"]["labels"].pop("space.pplx.net/template-build-smoke-fence-probe", None)
        if subresource == "status":
            self.nodes[OWN]["status"]["capacity"][SLOTS_RESOURCE] = "7"
        return SaWriteOutcome(0, "applied")

    def admin_label_node(self, node, key, value):
        self.calls.append(f"admin-label:{node}:{key}={value}")
        labels = self.nodes[node]["metadata"]["labels"]
        if value is None:
            labels.pop(key, None)
        else:
            labels[key] = value

    def admin_annotate_node(self, node, key, value):
        ann = self.nodes[node]["metadata"].setdefault("annotations", {})
        if value is None:
            ann.pop(key, None)
        else:
            ann[key] = value

    def admin_patch_node(self, node, patch, *, subresource=None):
        self.calls.append(f"admin-patch:{node}:{subresource or 'node'}")
        _apply_patch(self.nodes[node], patch)

    def admin_remove_capacity(self, node, resource):
        self.calls.append(f"strip:{node}:{resource}")
        self.nodes[node]["status"]["capacity"].pop(resource, None)
        self._polls = 0

    def revert_node_from_snapshot(self, node, snapshot):
        # Like the real one: everything a probe can touch goes back, but the
        # slot capacity is the slots repair's, keyed on the strip, not this.
        self.calls.append(f"revert:{node}")
        live_slots = self.nodes[node]["status"]["capacity"].get(SLOTS_RESOURCE)
        self.nodes[node] = json.loads(json.dumps(snapshot))
        capacity = self.nodes[node]["status"]["capacity"]
        capacity.pop(SLOTS_RESOURCE, None)
        if live_slots is not None:
            capacity[SLOTS_RESOURCE] = live_slots
        return ["restoring .spec taints/unschedulable"] if node == OWN and self.refuse_all is False else []

    def node_ctl_log_tail(self, pod, lines):
        return ["denied"]

    # the fake node-ctl: writes back after N polls of node_label/node_capacity
    def tick(self) -> None:
        self._polls += 1
        if self._polls >= self.reconcile_after_polls:
            self.nodes[OWN]["metadata"]["labels"].setdefault(OWNED_LABEL, "2")
            self.nodes[OWN]["status"]["capacity"].setdefault(SLOTS_RESOURCE, "7")


def _rule_for(patch, subresource):
    if "spec" in patch:
        return "may not write a Node's .spec"
    meta = patch.get("metadata", {})
    if "finalizers" in meta:
        return "may not write a Node's finalizers"
    if "annotations" in meta:
        return "may only clear a template-write annotation"
    if "labels" in meta:
        return "may only write the Node labels it owns"
    status = patch.get("status", {})
    if "capacity" in status:
        return "may only advertise its own extended resource"
    return "may not write a Node's status outside its own extended resource"


def _apply_patch(node, patch):
    for section, body in patch.items():
        target = node.setdefault(section, {})
        for k, v in body.items():
            if isinstance(v, dict):
                sub = target.setdefault(k, {})
                for kk, vv in v.items():
                    if vv is None:
                        sub.pop(kk, None)
                    else:
                        sub[kk] = vv
            else:
                target[k] = v


class _TickingFixture(_Fixture):
    def __init__(self, control: _Control) -> None:
        super().__init__()
        self._control = control

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        self._control.tick()


def _assertions(control: _Control | None = None):
    control = control or _Control()
    fixture = _TickingFixture(control)
    scope = ResourceScope()
    return fixture, NodeFenceAssertions(cast(Any, fixture), cast(Any, control), scope), scope, control


def _through_credential(control=None):
    fixture, fence, scope, control = _assertions(control)
    fence.locate()
    fence.expect_fence_enforcing()
    fence.read_credential()
    fence.snapshot_and_arm()
    return fixture, fence, scope, control


class Preconditions(unittest.TestCase):
    def test_one_node_is_refused(self) -> None:
        control = _Control()
        del control.nodes[OTHER]
        _, fence, _, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "second node"):
            fence.locate()

    def test_an_audit_binding_or_type_warnings_are_refused(self) -> None:
        control = _Control()
        control.policy = FencePolicy(POLICY, ["Audit"], 0)
        _, fence, _, _ = _assertions(control)
        fence.locate()
        with self.assertRaisesRegex(InvariantViolation, "Deny"):
            fence.expect_fence_enforcing()
        control.policy = FencePolicy(POLICY, ["Deny"], 2)
        with self.assertRaisesRegex(InvariantViolation, "type-checking warnings"):
            fence.expect_fence_enforcing()

    def test_the_token_request_fallback_is_used_and_recorded_only_when_the_mount_is_absent(self) -> None:
        control = _Control()
        control.token_readable = False
        fixture, fence, _, _ = _assertions(control)
        fence.locate()
        fence.expect_fence_enforcing()
        fence.read_credential()
        self.assertEqual(fixture.records["token_source"], "tokenrequest-bound-to-pod")
        self.assertIn("token-request", control.calls)

    def test_a_token_claiming_another_node_is_refused(self) -> None:
        control = _Control()
        control.who = WhoAmI("system:serviceaccount:space:space-node-ctl", OTHER)
        _, fence, _, _ = _assertions(control)
        fence.locate()
        fence.expect_fence_enforcing()
        with self.assertRaisesRegex(InvariantViolation, "node name for its token"):
            fence.read_credential()

    def test_a_wrong_identity_or_no_grant_is_refused(self) -> None:
        control = _Control()
        control.who = WhoAmI("kubernetes-admin", OWN)
        _, fence, _, _ = _assertions(control)
        fence.locate()
        fence.expect_fence_enforcing()
        with self.assertRaisesRegex(InvariantViolation, "does not authenticate as"):
            fence.read_credential()
        control.who = WhoAmI("system:serviceaccount:space:space-node-ctl", OWN)
        control.can_patch = False
        with self.assertRaisesRegex(InvariantViolation, "RBAC no longer grants"):
            fence.read_credential()


class Refusals(unittest.TestCase):
    def test_every_probe_refused_by_its_rule_passes_and_leaves_no_residue(self) -> None:
        fixture, fence, _, control = _through_credential()
        labels = fence.expect_refusals()
        self.assertEqual(len(labels), len(forbidden_probes()))
        fence.expect_no_residue()
        self.assertTrue(any(c.startswith(f"sa-patch:{OTHER}") for c in control.calls))

    def test_a_probe_that_succeeds_fails_the_run_and_the_reverts_repair_on_close(self) -> None:
        control = _Control()
        control.refuse_all = False
        _, fence, scope, control = _through_credential(control)
        with self.assertRaisesRegex(InvariantViolation, "SUCCEEDED"):
            fence.expect_refusals()
        # The taint landed on the fake node; the armed revert puts the snapshot back.
        self.assertTrue(control.nodes[OWN]["spec"].get("taints"))
        scope.close()
        self.assertIn(f"revert:{OWN}", control.calls)
        self.assertIn(f"revert:{OTHER}", control.calls)
        self.assertFalse(control.nodes[OWN]["spec"].get("taints"))

    def test_reverts_and_slots_repair_are_armed_before_any_probe(self) -> None:
        _, fence, scope, control = _through_credential()
        first_probe = next(i for i, c in enumerate(control.calls) if c.startswith("sa-patch"))if any(c.startswith("sa-patch") for c in control.calls) else len(control.calls)
        self.assertLess(control.calls.index("lease"), first_probe)
        names = [c.name for c in scope._compensations]
        self.assertIn(f"revert probe residue on {OWN}", names)
        self.assertIn(f"revert probe residue on {OTHER}", names)
        self.assertIn("restore the stripped slot capacity", names)

    def test_the_control_write_is_undone_even_when_the_strip_is_not_refused(self) -> None:
        control = _Control()
        control.refuse_all = False
        _, fence, _, control = _through_credential(control)
        with self.assertRaises(InvariantViolation):
            fence.expect_control_write_then_refused_strip()
        self.assertNotIn(BOUNDARY_LABEL, control.nodes[OWN]["metadata"]["labels"])


class Permissive(unittest.TestCase):
    def test_owned_writes_pass_and_the_slots_strip_is_armed_before_the_apply(self) -> None:
        fixture, fence, _, control = _through_credential()
        fence.expect_owned_writes_succeed()
        self.assertEqual(len(fixture.records["owned_writes"]), 5)
        strip = control.calls.index(f"strip:{OWN}:{SLOTS_RESOURCE}")
        apply_status = control.calls.index("sa-apply:space-node-ctl-network-slots:status")
        self.assertLess(strip, apply_status)
        self.assertEqual(control.nodes[OWN]["status"]["capacity"][SLOTS_RESOURCE], "7")

    def test_a_refused_owned_write_fails_by_name(self) -> None:
        control = _Control()
        original = control.apply_as

        def refuse(kubeconfig, manifest, *, field_manager, subresource=None):
            if field_manager == "space-node-ctl-ssd-headroom-tier":
                return SaWriteOutcome(1, f'"{POLICY}" denied')
            return original(kubeconfig, manifest, field_manager=field_manager, subresource=subresource)

        control.apply_as = refuse  # type: ignore[method-assign]
        _, fence, _, _ = _through_credential(control)
        with self.assertRaisesRegex(InvariantViolation, "own write was refused — the ssd placement labels"):
            fence.expect_owned_writes_succeed()


class Convergence(unittest.TestCase):
    def test_both_canaries_are_stripped_first_and_both_waited_for_together(self) -> None:
        control = _Control()
        control.reconcile_after_polls = 3
        fixture, fence, _, control = _through_credential(control)
        seen = fence.expect_node_ctl_reconciles(budget=60)
        self.assertEqual(set(seen), {"label", "slots"})
        self.assertIn(f"admin-label:{OWN}:{OWNED_LABEL}=None", control.calls)
        self.assertIn(f"strip:{OWN}:{SLOTS_RESOURCE}", control.calls)

    def test_a_stalled_node_ctl_fails_with_its_log_tail_and_the_slots_repair_restores(self) -> None:
        control = _Control()
        control.reconcile_after_polls = 10_000
        fixture, fence, scope, control = _through_credential(control)
        fence.expect_owned_writes_succeed()  # records the slots value the repair uses
        with self.assertRaisesRegex(InvariantViolation, "did not write back"):
            fence.expect_node_ctl_reconciles(budget=0)
        self.assertIsNone(control.nodes[OWN]["status"]["capacity"].get(SLOTS_RESOURCE))
        scope.close()
        self.assertEqual(control.nodes[OWN]["status"]["capacity"].get(SLOTS_RESOURCE), "7")
        self.assertIn(f"REPAIR:{OWN}:slots", fixture.records)

    def test_the_slots_repair_leaves_a_readvertised_different_value_alone(self) -> None:
        control = _Control()
        fixture, fence, scope, control = _through_credential(control)
        fence.expect_owned_writes_succeed()
        control.nodes[OWN]["status"]["capacity"][SLOTS_RESOURCE] = "9"
        scope.close()
        self.assertEqual(control.nodes[OWN]["status"]["capacity"][SLOTS_RESOURCE], "9")
        self.assertNotIn(f"REPAIR:{OWN}:slots", fixture.records)

    def test_an_unarmed_run_repairs_nothing(self) -> None:
        control = _Control()
        fixture, fence, scope, control = _through_credential(control)
        control.nodes[OWN]["status"]["capacity"].pop(SLOTS_RESOURCE)
        scope.close()
        self.assertNotIn(SLOTS_RESOURCE, control.nodes[OWN]["status"]["capacity"])
        self.assertNotIn(f"REPAIR:{OWN}:slots", fixture.records)

    def test_explicit_retire_releases_once_and_verifies_clean(self) -> None:
        fixture, fence, scope, control = _through_credential()
        fence.expect_refusals()
        fence.retire()
        self.assertEqual(fixture.records["final_residue"], "none")
        before = list(control.calls)
        scope.close()
        self.assertEqual(control.calls, before)


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


ARGV = ("kubectl",)


class HarnessOps(unittest.TestCase):
    def test_service_account_calls_use_the_token_kubeconfig_and_the_context(self) -> None:
        kubectl = _ScriptedKubectl([CommandResult(ARGV, 1, "", f'denied by "{POLICY}"')])
        harness = KindHarness(kubectl, KindConfig())
        outcome = harness.patch_node_as("/tmp/sa.kubeconfig", OWN, {"spec": {"unschedulable": True}}, subresource="status")
        self.assertEqual(outcome.returncode, 1)
        call = kubectl.calls[0]
        self.assertEqual(call[:3], ("kubectl", "--kubeconfig", "/tmp/sa.kubeconfig"))
        self.assertIn("--context", call)
        self.assertIn("--subresource=status", call)
        self.assertNotIn("-n", call)

    def test_unsafe_inputs_are_refused_before_kubectl(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with self.assertRaises(ValueError):
            harness.patch_node_as("/tmp/k", OWN, {}, subresource="spec")
        with self.assertRaises(ValueError):
            harness.apply_as("/tmp/k", "apiVersion: v1", field_manager="bad manager")
        with self.assertRaises(ValueError):
            harness.pod_file("space-node-ctl-a", "../etc/passwd")

    def test_fence_policy_is_matched_by_the_service_account_and_read_with_its_binding(self) -> None:
        policies = json.dumps({"items": [
            {"metadata": {"name": "other"}, "spec": {"matchConditions": [{"expression": 'request.userInfo.username == "x"'}]}},
            {"metadata": {"name": POLICY}, "spec": {"matchConditions": [{"expression": 'request.userInfo.username == "system:serviceaccount:space:space-node-ctl"'}]},
             "status": {"typeChecking": {"expressionWarnings": []}}},
        ]})
        bindings = json.dumps({"items": [{"spec": {"policyName": POLICY, "validationActions": ["Deny"]}}]})
        harness = KindHarness(_ScriptedKubectl([CommandResult(ARGV, 0, policies, ""), CommandResult(ARGV, 0, bindings, "")]), KindConfig())
        policy = harness.fence_policy_for("space-node-ctl")
        self.assertEqual(policy, FencePolicy(POLICY, ["Deny"], 0))

    def test_revert_from_snapshot_repairs_only_by_marker(self) -> None:
        snapshot = _node(OWN)
        live = json.loads(json.dumps(snapshot))
        live["spec"]["unschedulable"] = True
        live["metadata"]["finalizers"] = [PROBE_FINALIZER]
        live["metadata"]["labels"]["space.pplx.net/template-write-smoke-fence-probe"] = "x"
        live["status"]["allocatable"] = {FOREIGN_CAPACITY_RESOURCE: "8"}
        results = [CommandResult(ARGV, 0, json.dumps(live), "")] + [CommandResult(ARGV, 0, "", "")] * 20
        kubectl = _ScriptedKubectl(results)
        harness = KindHarness(kubectl, KindConfig())
        repairs = harness.revert_node_from_snapshot(OWN, snapshot)
        self.assertEqual(
            sorted(repairs),
            sorted([
                "restoring .spec taints/unschedulable",
                "removing probe key space.pplx.net/template-write-smoke-fence-probe",
                "removing probe finalizer",
                f"removing forged status.allocatable[{FOREIGN_CAPACITY_RESOURCE}]",
            ]),
        )


if __name__ == "__main__":
    unittest.main()

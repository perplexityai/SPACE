"""Framework contracts behind the cross-node durable-restore scenario.

These test the assertion logic and the safety choreography — properties the
kind bench cannot exercise (a lost apiserver reply, a killed process, the
negative provenance classes) — not the scenario's happy path, which the bench
proves directly.
"""

from __future__ import annotations

import json
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from space_smoke import adapters
from space_smoke.adapters import (
    CORDONED_NODE_ANNOTATION,
    NODE_SANDBOX_MANAGER,
    KindConfig,
    KindHarness,
)
from space_smoke.capabilities import RestoreActivation, SandboxPodPlacement
from space_smoke.fixtures import CrossNodeRestoreAssertions
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope
from space_smoke.scenarios.local_suspend_restore_cross_node_from_object_storage import (
    suspend_restore_cross_node_from_object_storage as scenario,
)

SANDBOX_ID = "01a0ac88-c99e-735a-b8fa-f13c948332fa"


def _activation(node: str, provenance: str, generation: str, *, uri: bool = True):
    return RestoreActivation(
        sandbox_id=SANDBOX_ID,
        node_name=node,
        boot_type="snapshot",
        restore_provenance=provenance,
        restore_storage_uri_present=uri,
        execution_generation=generation,
    )


def _placement(node: str, generation: str) -> SandboxPodPlacement:
    return SandboxPodPlacement(SANDBOX_ID, f"pod-{node}", f"uid-{node}", node, generation)


class _Fixture:
    """Just enough of KindEcw for the assertions class and the scenario body."""

    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SANDBOX_ID)
        self.calls: list[str] = []

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def phase(self, _name: str):
        return nullcontext()

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        pass

    def run_command_when_ready(self, command: str):
        self.calls.append(f"cmd:{command}")
        return SimpleNamespace(exit_code=0, stdout="", stderr="")

    def expect_command(self, result, *, exit_code: int = 0, stdout: str) -> None:
        pass

    def pause(self) -> None:
        self.calls.append("pause")

    def reconnect_by_id(self) -> None:
        self.calls.append("reconnect")


class _Control:
    """Records the order the fixture drives the privileged surface in."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.activations: list[RestoreActivation] = [
            RestoreActivation(SANDBOX_ID, "worker-a", "fresh", "fresh", False, "1")
        ]
        self.placement = _placement("worker-a", "1")
        self.fail_cordon = False
        self.portable_after_polls = 0

    def acquire_worker_cordon_mutation(self):
        self.calls.append("lease:acquire")
        return lambda: self.calls.append("lease:release")

    def sandbox_pod_placement(self, sandbox_id: str):
        self.calls.append("placement")
        return self.placement

    def restore_portable(self, sandbox_id: str) -> bool:
        self.calls.append("portable?")
        if self.portable_after_polls > 0:
            self.portable_after_polls -= 1
            return False
        return True

    def cordon_kube_node(self, node_name: str) -> None:
        self.calls.append(f"cordon:{node_name}")
        if self.fail_cordon:
            raise RuntimeError("apiserver reply lost")
        # The restore lands elsewhere and pulls from S3.
        self.placement = _placement("worker-b", "2")
        self.activations.append(_activation("worker-b", "remote_matches_local", "2"))

    def uncordon_kube_node(self, node_name: str) -> None:
        self.calls.append(f"uncordon:{node_name}")

    def restore_activations(self, sandbox_id: str, *, node_names=None, since_time=None):
        self.calls.append("activations")
        return list(self.activations)


def _assertions(control: _Control):
    fixture = _Fixture()
    scope = ResourceScope()
    return fixture, CrossNodeRestoreAssertions(cast(Any, fixture), control, scope), scope


class CordonChoreography(unittest.TestCase):
    def test_compensation_is_armed_before_the_cordon_is_dispatched(self) -> None:
        control = _Control()
        control.fail_cordon = True
        _, assertions, scope = _assertions(control)

        with self.assertRaises(RuntimeError):
            with assertions.cordoned("worker-a"):
                pass

        # The lost-reply cordon is still undone — eagerly on unwind — and the
        # ledger compensation is released rather than left dangling.
        self.assertEqual(
            control.calls,
            ["lease:acquire", "cordon:worker-a", "uncordon:worker-a", "lease:release"],
        )
        self.assertTrue(all(not c.armed for c in scope._compensations))

    def test_uncordon_and_release_happen_inside_the_body_not_cleanup(self) -> None:
        control = _Control()
        _, assertions, scope = _assertions(control)

        with assertions.cordoned("worker-a"):
            self.assertIn("cordon:worker-a", control.calls)
            self.assertNotIn("uncordon:worker-a", control.calls)

        self.assertEqual(control.calls[-2:], ["uncordon:worker-a", "lease:release"])
        self.assertTrue(all(not c.armed for c in scope._compensations))

    def test_scenario_waits_for_portability_before_cordoning(self) -> None:
        # The cordon only works if the planner has a remote snapshot to select;
        # a restore planned on the source node alone pre-binds spec.nodeName and
        # ignores the cordon. The wait must therefore precede the cordon.
        control = _Control()
        control.portable_after_polls = 2
        fixture, assertions, _ = _assertions(control)
        test_double = cast(Any, fixture)
        test_double.cross_node_restore = assertions

        scenario.verify(test_double)

        polls = [i for i, c in enumerate(control.calls) if c == "portable?"]
        cordon = control.calls.index("cordon:worker-a")
        # Two "not yet" answers plus the final "yes" — the loop actually polled.
        self.assertEqual(len(polls), 3)
        # Every portability poll completed before the cordon was dispatched.
        self.assertLess(max(polls), cordon)


class ProvenanceAssertion(unittest.TestCase):
    def _after_cross_node_restore(self, control: _Control):
        _, assertions, _ = _assertions(control)
        control.cordon_kube_node("worker-a")
        return assertions, _placement("worker-a", "1"), control.placement

    def test_healthy_remote_pull_on_another_node_passes(self) -> None:
        control = _Control()
        assertions, source, landed = self._after_cross_node_restore(control)

        activation = assertions.expect_restored_from_object_storage(
            source=source, landed=landed
        )
        self.assertEqual(activation.node_name, "worker-b")
        self.assertEqual(activation.restore_provenance, "remote_matches_local")

    def test_same_node_local_reuse_fails(self) -> None:
        control = _Control()
        _, assertions, _ = _assertions(control)
        control.activations.append(_activation("worker-a", "local_only", "2", uri=False))

        with self.assertRaises(InvariantViolation) as caught:
            assertions.expect_restored_from_object_storage(
                source=_placement("worker-a", "1"), landed=_placement("worker-a", "2")
            )
        self.assertIn("cordoned source node", str(caught.exception))

    def test_moved_node_served_locally_fails(self) -> None:
        control = _Control()
        _, assertions, _ = _assertions(control)
        control.activations.append(_activation("worker-b", "local_only", "2", uri=False))

        with self.assertRaises(InvariantViolation) as caught:
            assertions.expect_restored_from_object_storage(
                source=_placement("worker-a", "1"), landed=_placement("worker-b", "2")
            )
        self.assertIn("object storage", str(caught.exception))

    def test_anomalous_remote_classes_fail(self) -> None:
        # `remote_local_mismatch` and `remote_metadata_unknown` mean the pull
        # happened but adoption disagreed with the sidecar; that is a bug signal,
        # not a pass.
        for provenance in ("remote_local_mismatch", "remote_metadata_unknown"):
            with self.subTest(provenance=provenance):
                control = _Control()
                _, assertions, _ = _assertions(control)
                control.activations.append(_activation("worker-b", provenance, "2"))

                with self.assertRaises(InvariantViolation) as caught:
                    assertions.expect_restored_from_object_storage(
                        source=_placement("worker-a", "1"),
                        landed=_placement("worker-b", "2"),
                    )
                self.assertIn("object storage", str(caught.exception))

    def test_cold_boot_generation_cannot_satisfy_the_check(self) -> None:
        # Only the cold boot (generation 1) exists; asking about generation 2
        # must not find it, regardless of log history.
        control = _Control()
        _, assertions, _ = _assertions(control)

        with self.assertRaises(InvariantViolation) as caught:
            # The live helper polls a minute for log visibility; a fake that
            # will never produce the event should not make the test wait it out.
            assertions.expect_restored_from_object_storage(
                source=_placement("worker-a", "1"),
                landed=_placement("worker-b", "2"),
                visibility_timeout=0,
            )
        self.assertIn("exactly one runtime_activated event", str(caught.exception))


class _ScriptedKubectl:
    """Answer kubectl invocations by subcommand; record everything."""

    def __init__(self, lease: dict | None, *, fail_patch: bool = False) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.lease = lease
        self.fail_patch = fail_patch

    def run(self, command, **_options) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        verb = argv[5]  # kubectl --context X -n space <verb> ...
        if verb == "get" and argv[6] == "lease":
            if self.lease is None:
                return CommandResult(argv, 1, "", 'leases "x" not found')
            return CommandResult(argv, 0, json.dumps(self.lease), "")
        if verb == "patch":
            if self.fail_patch:
                # Another contender won the resourceVersion race.
                return CommandResult(argv, 1, "", "the object has been modified")
            body = dict(self.lease or {})
            body["spec"] = json.loads(argv[10])[2]["value"]
            self.lease = body  # the fence re-reads the Lease after the patch
            return CommandResult(argv, 0, json.dumps(body), "")
        if verb == "create":
            return CommandResult(argv, 0, json.dumps(self.lease), "")
        return CommandResult(argv, 0, "", "")


def _expired_lease(*, cordoned_node: str | None) -> dict:
    metadata: dict[str, Any] = {"uid": "u1", "resourceVersion": "7", "name": "x"}
    if cordoned_node:
        metadata["annotations"] = {CORDONED_NODE_ANNOTATION: cordoned_node}
    return {
        "metadata": metadata,
        "spec": {
            "holderIdentity": "uid=0;pid=1;token=dead",
            "leaseDurationSeconds": 1,
            "acquireTime": "2000-01-01T00:00:00.000000Z",
            "renewTime": "2000-01-01T00:00:00.000000Z",
        },
    }


class OrphanedCordonRecovery(unittest.TestCase):
    def test_taking_over_an_expired_lease_uncordons_the_recorded_node(self) -> None:
        kubectl = _ScriptedKubectl(_expired_lease(cordoned_node="space-local-worker"))
        harness = KindHarness(kubectl, KindConfig())

        harness.acquire_worker_cordon_mutation()

        verbs = [c[5:8] for c in kubectl.calls]
        self.assertIn(("uncordon", "space-local-worker"), [v[:2] for v in verbs])
        # Ownership is claimed FIRST; the repair runs only once the guarded
        # patch has succeeded, then re-checks ownership right before writing.
        names = [v[0] for v in verbs]
        self.assertLess(names.index("patch"), names.index("uncordon"))
        self.assertIn("get", names[names.index("patch") : names.index("uncordon")])
        # The stale record is cleared so a later takeover cannot replay it.
        self.assertTrue(
            any(
                c[5] == "annotate" and c[8] == f"{CORDONED_NODE_ANNOTATION}-"
                for c in kubectl.calls
            )
        )

    def test_losing_the_takeover_race_repairs_nothing(self) -> None:
        kubectl = _ScriptedKubectl(
            _expired_lease(cordoned_node="space-local-worker"), fail_patch=True
        )
        harness = KindHarness(kubectl, KindConfig())

        with self.assertRaises(adapters.SmokeConfigurationError):
            harness.acquire_worker_cordon_mutation()

        # The winner may already have cordoned this node for its own fault.
        self.assertNotIn("uncordon", [c[5] for c in kubectl.calls])

    def test_expired_lease_without_a_record_repairs_nothing(self) -> None:
        kubectl = _ScriptedKubectl(_expired_lease(cordoned_node=None))
        KindHarness(kubectl, KindConfig()).acquire_worker_cordon_mutation()
        self.assertNotIn("uncordon", [c[5] for c in kubectl.calls])

    def test_cordon_records_the_node_before_patching_it(self) -> None:
        kubectl = _ScriptedKubectl(None)
        harness = KindHarness(kubectl, KindConfig())
        # Creation path: give the scripted create a document to hand back.
        kubectl.lease = _expired_lease(cordoned_node=None)
        kubectl.lease["spec"]["holderIdentity"] = ""
        harness.acquire_worker_cordon_mutation()
        # Make the ownership fence pass on the re-check.
        owned = harness._cordon_lease_ownership
        assert owned is not None
        kubectl.lease["spec"] = {
            "holderIdentity": owned.holder,
            "leaseDurationSeconds": 3600,
            "acquireTime": adapters._kube_timestamp(adapters.datetime.now(adapters.UTC)),
            "renewTime": adapters._kube_timestamp(adapters.datetime.now(adapters.UTC)),
        }

        harness.cordon_kube_node("space-local-worker")

        verbs = [c[5] for c in kubectl.calls]
        self.assertLess(verbs.index("annotate"), verbs.index("cordon"))
        annotate = next(c for c in kubectl.calls if c[5] == "annotate")
        self.assertIn(f"{CORDONED_NODE_ANNOTATION}=space-local-worker", annotate)


class SandboxManagerSelectorMatchesTheKindCharts(unittest.TestCase):
    """Sandbox events are read from the worker sandbox-manager pods.

    A selector the charts do not render matches nothing, and the scenario then
    fails with "no runtime_activated event", which reads like a product bug.
    Pin the selector and container to what the node-service library renders.
    """

    def test_selector_is_the_worker_role_and_component(self) -> None:
        charts = Path(__file__).resolve().parents[2] / "infra/space/kind/charts"
        helpers = (charts / "space-node-service/templates/_helpers.tpl").read_text()
        self.assertIn(
            'app.kubernetes.io/name: space-{{ include "space-node-service.role" .ctx }}', helpers
        )
        self.assertIn("app.kubernetes.io/component: {{ .component }}", helpers)
        daemonset = (charts / "space-sandbox-manager-local/templates/daemonset.yaml").read_text()
        self.assertIn('"component" "sandbox-manager"', daemonset)
        self.assertIn("- name: sandbox-manager", daemonset)
        self.assertEqual(
            adapters._node_service_selector(NODE_SANDBOX_MANAGER),
            "app.kubernetes.io/name=space-worker,app.kubernetes.io/component=sandbox-manager",
        )


if __name__ == "__main__":
    unittest.main()

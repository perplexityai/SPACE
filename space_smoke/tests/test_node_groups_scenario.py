"""Contracts of the node-group scenario the bench cannot exercise.

A passing bench shows the inert default, the 409 (on a fresh stack) and a
scheduled pod. It cannot show that a gate already on is refused rather than
read as inert, that a 409 whose body does not name the generic group fails,
that a `Failed` pod fails the schedule verdict before the timeout, or that the
label snapshot and env restore are armed before the first write and unwind
gate-first.
"""

from __future__ import annotations

import json
import threading
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import KindConfig, KindHarness, _LeaseOwnership, _parse_pod_boundary_render
from space_smoke.capabilities import (
    AdminNodeGroupView,
    AdminNodeView,
    LimiterProbeOutcome,
    SandboxPodBoundaryRender,
)
from space_smoke.node_groups import NodeGroupAssertions
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope

SID = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"


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


class _Control:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.env: dict[str, str | None] = {
            "api:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": None,
            "controller:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": None,
            "controller:SPACE_NODE_GROUP_LABELER_ENABLED": None,
            "controller:SPACE_NODE_GROUP_MIRROR_ENABLED": None,
        }
        self.groups: list[AdminNodeGroupView] = []
        self.nodes: list[AdminNodeView] = []
        self.labels: dict[str, str | None] = {"worker-a": None, "worker-b": None}
        self.taints: dict[str, str | None] = {"worker-a": None, "worker-b": None}
        self.terms: list[str] = []
        self.phase: tuple[str | None, str | None] = ("Running", "worker-a")
        self.create_status = (
            409,
            "this cluster has no generic node group; create the default first",
        )
        self.fail_on_write = False
        self.pod_tolerations: list[dict[str, str]] = []

    def node_group_env(self):
        return dict(self.env)

    def set_node_group_env(self, *, gate, labeler, mirror):
        self.calls.append(f"set:gate={gate},labeler={labeler},mirror={mirror}")
        self.env = {
            "api:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": "true" if gate else "false",
            "controller:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": "true" if gate else "false",
            "controller:SPACE_NODE_GROUP_LABELER_ENABLED": "true" if labeler else "false",
            "controller:SPACE_NODE_GROUP_MIRROR_ENABLED": "true" if mirror else "false",
        }
        self.groups = [AdminNodeGroupView("g1", "workers", None, True, 2)]
        self.labels = {"worker-a": "workers", "worker-b": "workers"}
        self.nodes = [
            AdminNodeView("n1", "worker-a", None, None, 0, node_group_id="g1"),
            AdminNodeView("n2", "worker-b", None, None, 0, node_group_id="g1"),
        ]
        if self.fail_on_write:
            self.taints = {"worker-a": "019d0000-0000-7000-8000-000000000001", "worker-b": None}
            raise RuntimeError("write failed mid-rollout")

    def restore_node_group_env(self, standing):
        self.env = dict(standing)
        self.calls.append("restore-env")

    def wait_public_api_ready(self, *, timeout):
        self.calls.append("ready")

    def acquire_public_api_mutation(self):
        self.calls.append("lease")
        return lambda: self.calls.append("lease:release")

    def node_groups(self):
        return list(self.groups)

    def admin_nodes(self):
        return list(self.nodes)

    def try_create_node_group(self, *, name, org_id):
        return self.create_status

    def try_delete_node_group(self, node_group_id):
        return 200, ""

    def worker_boundary_labels(self):
        return dict(self.labels)

    def restore_worker_boundary_labels(self, snapshot):
        self.labels = dict(snapshot)
        self.calls.append(f"restore-labels:{sorted(snapshot.items())}")

    def worker_dedicated_taints(self):
        return dict(self.taints)

    def restore_worker_dedicated_taints(self, snapshot):
        self.taints = dict(snapshot)
        self.calls.append(f"restore-taints:{sorted(snapshot.items())}")

    def sandbox_pod_boundary(self, sandbox_id):
        phase, node = self.phase
        parsed = _parse_pod_boundary_render(
            {
                "metadata": {"name": f"pod-{sandbox_id}"},
                "spec": {"nodeName": node, "tolerations": self.pod_tolerations},
                "status": {"phase": phase},
            }
        )
        req_sets = (tuple(sorted(self.terms)),) if self.terms else ()
        any_terms = (
            (f"required:In:{','.join(sorted(self.terms))}",) if self.terms else ()
        )
        return SandboxPodBoundaryRender(
            pod_name=parsed.pod_name,
            phase=parsed.phase,
            node_name=parsed.node_name,
            required_node_group_sets=req_sets,
            any_node_group_terms=any_terms,
            dedicated_org_tolerations=parsed.dedicated_org_tolerations,
        )

    def sandbox_pod_node_group_terms(self, sandbox_id):
        return list(self.terms)

    def sandbox_pod_phase_and_node(self, sandbox_id):
        return self.phase

    def ready_full_snapshots(self, sandbox_id):
        return []


class _Probe:
    def __init__(self, outcome: LimiterProbeOutcome | None = None) -> None:
        self.outcome = outcome or LimiterProbeOutcome(201, None, "", "sbx_new", 0.0)
        self.kills = 0
        self.created_with: dict[str, str] = {}
        self.killed_with: dict[str, str] = {}

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        self.created_with = dict(body.get("metadata", {})) if body else {}
        return self.outcome

    def kill_matching(self, metadata):
        self.kills += 1
        self.killed_with = dict(metadata)

    def kill_sandbox(self, sandbox_id):
        pass


def _assertions(control: _Control, probe: _Probe | None = None):
    fixture = _Fixture()
    scope = ResourceScope()
    probe = probe or _Probe()
    return (
        fixture,
        NodeGroupAssertions(cast(Any, fixture), cast(Any, control), cast(Any, probe), scope),
        scope,
    )


class Phase1(unittest.TestCase):
    def test_a_gate_already_on_is_refused_not_read_as_inert(self) -> None:
        control = _Control()
        control.env["api:SPACE_SANDBOX_NODE_GROUP_PLACEMENT"] = "true"
        _, groups, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "already on"):
            groups.expect_inert_by_default()

    def test_a_term_with_the_gate_off_fails(self) -> None:
        control = _Control()
        control.terms = ["workers"]
        _, groups, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "carries a node-group affinity term"):
            groups.expect_inert_by_default()
        control.terms = []
        control.pod_tolerations = [{"operator": "Exists", "effect": "NoSchedule"}]
        with self.assertRaisesRegex(InvariantViolation, "dedicated-org toleration"):
            groups.expect_inert_by_default()


class Phase2(unittest.TestCase):
    def test_a_409_that_does_not_name_the_generic_group_fails(self) -> None:
        control = _Control()
        control.create_status = (409, "conflict")
        _, groups, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "names the fix"):
            groups.expect_dedicated_refused_without_generic(org_id="o", name="n")

    def test_an_existing_generic_group_skips_with_a_recorded_reason(self) -> None:
        control = _Control()
        control.groups = [AdminNodeGroupView("g0", "pre", None, False, 0)]
        fixture, groups, _ = _assertions(control)
        groups.expect_dedicated_refused_without_generic(org_id="o", name="n")
        self.assertIn("skipped", str(fixture.records["provisioning_order_check"]))


class Phase3(unittest.TestCase):
    def _enabled(self):
        control = _Control()
        _, groups, scope = _assertions(control)
        groups.enable_placement_gate()
        default = groups.wait_for_default_group_and_labels(timeout=1)
        return control, groups, scope, default

    def test_a_failed_pod_fails_before_the_schedule_timeout(self) -> None:
        control, groups, _, default = self._enabled()
        control.terms = ["workers"]
        control.phase = ("Failed", None)
        with self.assertRaisesRegex(InvariantViolation, "never scheduled"):
            groups.expect_generic_create_renders_and_schedules(
                default, template="seed", schedule_timeout=60
            )

    def test_a_term_missing_the_default_group_fails(self) -> None:
        control, groups, _, default = self._enabled()
        control.terms = ["somewhere-else"]
        with self.assertRaisesRegex(InvariantViolation, "does not name the default group"):
            groups.expect_generic_create_renders_and_schedules(default, template="seed")


class Sweep(unittest.TestCase):
    def test_an_ambiguous_dispatch_still_sweeps_by_metadata(self) -> None:
        for outcome in (
            LimiterProbeOutcome(0, None, "timed out", None, 0.0),
            LimiterProbeOutcome(502, None, "bad gateway", None, 0.0),
            LimiterProbeOutcome(201, None, "{}", None, 0.0),
        ):
            with self.subTest(status=outcome.status):
                control = _Control()
                probe = _Probe(outcome)
                _, groups, _ = _assertions(control, probe)
                groups.enable_placement_gate()
                default = groups.wait_for_default_group_and_labels(timeout=1)
                with self.assertRaisesRegex(InvariantViolation, "generic create failed"):
                    groups.expect_generic_create_renders_and_schedules(default, template="seed")
                groups.sweep_admitted()
                self.assertEqual(probe.kills, 1)

    def test_the_sweep_uses_the_create_metadata_and_it_is_unique_per_run(self) -> None:
        runs = []
        for _ in range(2):
            control = _Control()
            control.terms = ["workers"]
            probe = _Probe()
            _, groups, _ = _assertions(control, probe)
            groups.enable_placement_gate()
            default = groups.wait_for_default_group_and_labels(timeout=1)
            groups.expect_generic_create_renders_and_schedules(default, template="seed")
            groups.sweep_admitted()
            self.assertEqual(probe.killed_with, probe.created_with)
            runs.append(probe.created_with["space_smoke_run"])
        self.assertTrue(all(runs))
        self.assertNotEqual(runs[0], runs[1])

    def test_a_clean_refusal_does_not_sweep(self) -> None:
        control = _Control()
        probe = _Probe(LimiterProbeOutcome(409, None, "refused", None, 0.0))
        _, groups, _ = _assertions(control, probe)
        groups.enable_placement_gate()
        default = groups.wait_for_default_group_and_labels(timeout=1)
        with self.assertRaisesRegex(InvariantViolation, "generic create failed"):
            groups.expect_generic_create_renders_and_schedules(default, template="seed")
        groups.sweep_admitted()
        self.assertEqual(probe.kills, 0)


class Compensations(unittest.TestCase):
    def test_snapshot_and_restore_are_armed_before_the_write_and_unwind_env_first(self) -> None:
        control = _Control()
        control.fail_on_write = True
        _, groups, scope = _assertions(control)
        with self.assertRaisesRegex(RuntimeError, "write failed mid-rollout"):
            groups.enable_placement_gate()
        self.assertEqual(
            control.calls[:2],
            ["lease", "set:gate=True,labeler=True,mirror=True"],
        )
        scope.close()
        unwind = [
            c
            for c in control.calls
            if c.startswith(("restore-taints", "restore-env", "restore-labels", "lease:release"))
        ]
        self.assertTrue(
            unwind[0].startswith("restore-taints:[('worker-a', None), ('worker-b', None)]")
        )
        self.assertEqual(unwind[1], "restore-env")
        self.assertTrue(
            unwind[2].startswith("restore-labels:[('worker-a', None), ('worker-b', None)]")
        )
        self.assertEqual(unwind[3], "lease:release")
        self.assertEqual(control.labels, {"worker-a": None, "worker-b": None})
        self.assertEqual(control.taints, {"worker-a": None, "worker-b": None})
        self.assertTrue(all(value is None for value in control.env.values()))


class _ScriptedKubectl:
    """Answers by command shape; records call order across threads."""

    def __init__(
        self,
        *,
        rollout_barrier: threading.Barrier | None = None,
        release_rollouts: threading.Event | None = None,
    ) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.completed_rollouts: list[str] = []
        self._lock = threading.Lock()
        self._rollout_barrier = rollout_barrier
        self._release_rollouts = release_rollouts

    def run(self, command, **_options) -> CommandResult:
        with self._lock:
            self.calls.append(tuple(command))
        argv = tuple(command)
        if "get" in argv and "lease" in argv:
            # The caller's live public-API mutation Lease, which a fast rollout requires.
            renewed = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
            lease = {
                "metadata": {"uid": "lease-uid", "resourceVersion": "7"},
                "spec": {
                    "holderIdentity": "me",
                    "leaseDurationSeconds": 3600,
                    "renewTime": renewed,
                },
            }
            return CommandResult(argv, 0, json.dumps(lease), "")
        if "get" in argv and "deploy" in argv and "-l" in argv:
            selector = argv[argv.index("-l") + 1]
            return CommandResult(
                argv, 0, "space-cluster-controller" if "controller" in selector else "space-api", ""
            )
        if "get" in argv and "deploy" in argv and argv[-1] == "json":
            # The post-rollout drain: one desired pod, one present.
            deployment = {
                "metadata": {"uid": "u", "resourceVersion": "1"},
                "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "x"}}},
            }
            return CommandResult(argv, 0, json.dumps(deployment), "")
        if "get" in argv and "pods" in argv:
            return CommandResult(argv, 0, json.dumps({"items": [{"metadata": {"name": "p"}}]}), "")
        if "get" in argv and "deploy" in argv:
            return CommandResult(argv, 0, "main", "")
        if "rollout" in argv:
            if self._rollout_barrier is not None:
                self._rollout_barrier.wait(timeout=2.0)
            if self._release_rollouts is not None and not self._release_rollouts.wait(timeout=2.0):
                raise TimeoutError("rollout release gate timed out")
            with self._lock:
                self.completed_rollouts.append(argv[argv.index("status") + 1])
        return CommandResult(argv, 0, "", "")


def _leased(harness: KindHarness) -> KindHarness:
    harness._public_api_lease_ownership = _LeaseOwnership(
        name="space-smoke-public-api-x",
        uid="lease-uid",
        holder="me",
        subject="public API deployment mutation",
    )
    return harness


class ConcurrentRollouts(unittest.TestCase):
    def test_both_env_writes_land_in_order_before_either_rollout_and_both_are_awaited(
        self,
    ) -> None:
        rollout_barrier = threading.Barrier(3, timeout=2.0)
        release_rollouts = threading.Event()
        kubectl = _ScriptedKubectl(
            rollout_barrier=rollout_barrier, release_rollouts=release_rollouts
        )
        harness = _leased(KindHarness(kubectl, KindConfig()))
        errors: list[BaseException] = []

        def _flip() -> None:
            try:
                harness.set_node_group_env(gate=True, labeler=True, mirror=True)
            except BaseException as exc:  # pragma: no cover - re-raised in test thread
                errors.append(exc)

        worker = threading.Thread(target=_flip)
        worker.start()
        try:
            # Both rollout callbacks must enter concurrently for the 3-party barrier to pass.
            rollout_barrier.wait(timeout=2.0)
            self.assertTrue(worker.is_alive())
            self.assertEqual(kubectl.completed_rollouts, [])
        finally:
            release_rollouts.set()
            worker.join(timeout=2.0)
        if errors:
            raise errors[0]
        self.assertFalse(worker.is_alive())
        self.assertEqual(
            sorted(kubectl.completed_rollouts),
            ["deploy/space-api", "deploy/space-cluster-controller"],
        )
        sets = [c for c in kubectl.calls if "set" in c and "env" in c]
        rollouts = [c for c in kubectl.calls if "rollout" in c]
        self.assertEqual(
            [c[c.index("env") + 1] for c in sets],
            ["deploy/space-cluster-controller", "deploy/space-api"],
        )
        self.assertEqual(
            sorted(c[c.index("status") + 1] for c in rollouts),
            ["deploy/space-api", "deploy/space-cluster-controller"],
        )
        self.assertLess(
            max(kubectl.calls.index(c) for c in sets),
            min(kubectl.calls.index(c) for c in rollouts),
        )

    def test_restore_writes_the_api_first(self) -> None:
        kubectl = _ScriptedKubectl()
        harness = _leased(KindHarness(kubectl, KindConfig()))
        harness.restore_node_group_env(
            {
                "api:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": None,
                "controller:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": None,
                "controller:SPACE_NODE_GROUP_LABELER_ENABLED": None,
                "controller:SPACE_NODE_GROUP_MIRROR_ENABLED": "TRUE",
            }
        )
        sets = [c for c in kubectl.calls if "set" in c and "env" in c]
        self.assertEqual(
            [c[c.index("env") + 1] for c in sets],
            ["deploy/space-api", "deploy/space-cluster-controller"],
        )


if __name__ == "__main__":
    unittest.main()

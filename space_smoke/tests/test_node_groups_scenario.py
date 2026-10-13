"""Contracts of the node-group scenario the bench cannot exercise.

A passing bench shows the inert default, the 409 (on a fresh stack) and a
scheduled pod. It cannot show that a gate already on is refused rather than
read as inert, that a 409 whose body does not name the generic group fails,
that a `Failed` pod fails the schedule verdict before the timeout, or that the
label snapshot and env restore are armed before the first write and unwind
gate-first.
"""

from __future__ import annotations

import unittest
import time
import json
import threading
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.capabilities import AdminNodeGroupView, LimiterProbeOutcome
from space_smoke.node_groups import NodeGroupAssertions
from space_smoke.adapters import KindConfig, KindHarness
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
        self.env = {"SPACE_SANDBOX_NODE_GROUP_PLACEMENT": None, "SPACE_NODE_GROUP_LABELER_ENABLED": None}
        self.groups: list[AdminNodeGroupView] = []
        self.labels = {"worker-a": None, "worker-b": None}
        self.terms: list[str] = []
        self.phase = ("Running", "worker-a")
        self.create_status = (409, "this cluster has no generic node group; create the default first")

    def node_group_env(self):
        return dict(self.env)

    def set_node_group_env(self, *, gate, labeler):
        self.calls.append(f"set:gate={gate},labeler={labeler}")
        self.groups = [AdminNodeGroupView("g1", "workers", None, True, 2)]
        self.labels = {"worker-a": "workers", "worker-b": "workers"}

    def restore_node_group_env(self, standing):
        self.calls.append("restore-env")

    def wait_public_api_ready(self, *, timeout):
        self.calls.append("ready")

    def acquire_public_api_mutation(self):
        self.calls.append("lease")
        return lambda: self.calls.append("lease:release")

    def node_groups(self):
        return list(self.groups)

    def try_create_node_group(self, *, name, org_id):
        return self.create_status

    def worker_boundary_labels(self):
        return dict(self.labels)

    def restore_worker_boundary_labels(self, snapshot):
        self.calls.append(f"restore-labels:{sorted(snapshot.items())}")

    def sandbox_pod_node_group_terms(self, sandbox_id):
        return list(self.terms)

    def sandbox_pod_phase_and_node(self, sandbox_id):
        return self.phase


class _Probe:
    def __init__(self, outcome=None) -> None:
        self.outcome = outcome or LimiterProbeOutcome(201, None, "", "sbx_new", 0.0)
        self.kills = 0

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        self.created_with = dict(body.get("metadata", {}))
        return self.outcome

    def kill_matching(self, metadata):
        self.kills += 1
        self.killed_with = dict(metadata)


def _assertions(control: _Control, probe: _Probe | None = None):
    fixture = _Fixture()
    scope = ResourceScope()
    probe = probe or _Probe()
    return fixture, NodeGroupAssertions(cast(Any, fixture), cast(Any, control), cast(Any, probe), scope), scope


class Phase1(unittest.TestCase):
    def test_a_gate_already_on_is_refused_not_read_as_inert(self) -> None:
        control = _Control()
        control.env["SPACE_SANDBOX_NODE_GROUP_PLACEMENT"] = "true"
        _, groups, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "already on"):
            groups.expect_inert_by_default()

    def test_a_term_with_the_gate_off_fails(self) -> None:
        control = _Control()
        control.terms = ["workers"]
        _, groups, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "carries a node-group affinity term"):
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
        groups.enable_labeler_and_gate()
        default = groups.wait_for_default_group_and_labels(timeout=1)
        return control, groups, scope, default

    def test_a_failed_pod_fails_before_the_schedule_timeout(self) -> None:
        control, groups, _, default = self._enabled()
        control.terms = ["workers"]
        control.phase = ("Failed", None)
        with self.assertRaisesRegex(InvariantViolation, "never scheduled"):
            groups.expect_generic_create_renders_and_schedules(default, template="seed", schedule_timeout=60)

    def test_a_term_missing_the_default_group_fails(self) -> None:
        control, groups, _, default = self._enabled()
        control.terms = ["somewhere-else"]
        with self.assertRaisesRegex(InvariantViolation, "does not name the default group"):
            groups.expect_generic_create_renders_and_schedules(default, template="seed")


class Sweep(unittest.TestCase):
    def _default(self, control):
        _, groups, _ = _assertions(control)
        groups.enable_labeler_and_gate()
        return groups.wait_for_default_group_and_labels(timeout=1)

    def test_an_ambiguous_dispatch_still_sweeps_by_metadata(self) -> None:
        control = _Control()
        probe = _Probe(LimiterProbeOutcome(0, None, "timed out", None, 0.0))
        _, groups, _ = _assertions(control, probe)
        groups.enable_labeler_and_gate()
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
            groups.enable_labeler_and_gate()
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
        groups.enable_labeler_and_gate()
        default = groups.wait_for_default_group_and_labels(timeout=1)
        with self.assertRaisesRegex(InvariantViolation, "generic create failed"):
            groups.expect_generic_create_renders_and_schedules(default, template="seed")
        groups.sweep_admitted()
        self.assertEqual(probe.kills, 0)


class Compensations(unittest.TestCase):
    def test_snapshot_and_restore_are_armed_before_the_write_and_unwind_env_first(self) -> None:
        control = _Control()
        _, groups, scope = _assertions(control)
        groups.enable_labeler_and_gate()
        self.assertEqual(control.calls[:2], ["lease", "set:gate=True,labeler=True"][:1] + ["set:gate=True,labeler=True"])
        scope.close()
        unwind = [c for c in control.calls if c.startswith(("restore-env", "restore-labels", "lease:release"))]
        self.assertEqual(unwind[0], "restore-env")
        self.assertTrue(unwind[1].startswith("restore-labels:[('worker-a', None), ('worker-b', None)]"))
        self.assertEqual(unwind[2], "lease:release")


class _ScriptedKubectl:
    """Answers by command shape; records call order across threads."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._lock = threading.Lock()

    def run(self, command, **_options) -> CommandResult:
        with self._lock:
            self.calls.append(tuple(command))
        argv = tuple(command)
        if "get" in argv and "deploy" in argv and "-l" in argv:
            selector = argv[argv.index("-l") + 1]
            return CommandResult(argv, 0, "space-cluster-controller" if "controller" in selector else "space-api", "")
        if "get" in argv and "deploy" in argv and argv[-1] == "json":
            # The post-rollout drain: one desired pod, one present.
            deployment = {"spec": {"replicas": 1, "selector": {"matchLabels": {"app": "x"}}}}
            return CommandResult(argv, 0, json.dumps(deployment), "")
        if "get" in argv and "pods" in argv:
            return CommandResult(argv, 0, json.dumps({"items": [{"metadata": {"name": "p"}}]}), "")
        if "get" in argv and "deploy" in argv:
            return CommandResult(argv, 0, "main", "")
        if "rollout" in argv:
            time.sleep(0.2)
        return CommandResult(argv, 0, "", "")


class ConcurrentRollouts(unittest.TestCase):
    def test_both_env_writes_land_in_order_before_either_rollout_and_both_are_awaited(self) -> None:
        kubectl = _ScriptedKubectl()
        harness = KindHarness(kubectl, KindConfig())
        started = time.monotonic()
        harness.set_node_group_env(gate=True, labeler=True)
        elapsed = time.monotonic() - started
        sets = [c for c in kubectl.calls if "set" in c and "env" in c]
        rollouts = [c for c in kubectl.calls if "rollout" in c]
        self.assertEqual([c[c.index("env") + 1] for c in sets], ["deploy/space-cluster-controller", "deploy/space-api"])
        self.assertEqual(sorted(c[c.index("status") + 1] for c in rollouts), ["deploy/space-api", "deploy/space-cluster-controller"])
        self.assertLess(max(kubectl.calls.index(c) for c in sets), min(kubectl.calls.index(c) for c in rollouts))
        # Two 0.2 s rollouts awaited together, not one after the other.
        self.assertLess(elapsed, 0.35)

    def test_restore_writes_the_api_first(self) -> None:
        kubectl = _ScriptedKubectl()
        harness = KindHarness(kubectl, KindConfig())
        harness.restore_node_group_env({
            "api:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": None,
            "controller:SPACE_SANDBOX_NODE_GROUP_PLACEMENT": None,
            "controller:SPACE_NODE_GROUP_LABELER_ENABLED": None,
        })
        sets = [c for c in kubectl.calls if "set" in c and "env" in c]
        self.assertEqual([c[c.index("env") + 1] for c in sets], ["deploy/space-api", "deploy/space-cluster-controller"])


if __name__ == "__main__":
    unittest.main()

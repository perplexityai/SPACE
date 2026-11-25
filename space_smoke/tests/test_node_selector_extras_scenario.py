"""Contracts of the nodeSelector-extras scenario the bench cannot exercise.

A passing bench shows two pods carry the merged selector and one create is
refused. It cannot show that a dropped or spurious selector entry fails, that a
pod from the warm pool is refused as evidence, that a node lacking an entry of
its own selector fails, that a fleet still matching the full set fails the
negative phase before the request, that an admitted create (a rendered pod) is
named as fail-open before the status is judged, that a 429 with the wrong
message or without Retry-After fails, that the restore is armed before the
first write and the standing env is verified after it, that a killed sandbox's
pod counts as gone only once it is absent (a pod stuck Terminating fails the
kill phase, by name), or that the kubectl parsing of nodes and pods matches the
filter's predicate.
"""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke.adapters import (
    KindConfig,
    KindHarness,
    _newest_pod_render,
    _parse_nodes,
)
from space_smoke.capabilities import LimiterProbeOutcome, NodeSnapshot, SandboxPodRender, SandboxPodView
from space_smoke.node_selector_extras import (
    ABSENT_EXTRA,
    SATISFIED_EXTRAS,
    SERVER_CAPACITY_MESSAGE,
    NodeSelectorExtrasAssertions,
    expected_selector,
    extras_env,
    render_mismatch,
    schedulable_nodes_matching,
)
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope

SID = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
NATIVE = "6f1e2d3c-4b5a-4c6d-8e9f-0a1b2c3d4e5f"
DISC = ("space.pplx.net/worker", "true")
FULL = expected_selector(DISC, SATISFIED_EXTRAS)


def _node(name: str, labels: dict[str, str], *, ready: bool = True, cordoned: bool = False) -> NodeSnapshot:
    return NodeSnapshot(name, labels, ready, cordoned)


WORKER_LABELS = {DISC[0]: DISC[1], "space.pplx.net/node-pool": "sandbox", "kubernetes.io/os": "linux"}


class Pure(unittest.TestCase):
    def test_extras_env_and_expected_selector(self) -> None:
        self.assertEqual(extras_env(SATISFIED_EXTRAS), "space.pplx.net/node-pool=sandbox,kubernetes.io/os=linux")
        self.assertEqual(FULL, {DISC[0]: "true", "space.pplx.net/node-pool": "sandbox", "kubernetes.io/os": "linux"})
        # An extra on the discriminator's key wins — the merge rule, reimplemented.
        self.assertEqual(expected_selector(DISC, ((DISC[0], "other"),)), {DISC[0]: "other"})
        with self.assertRaises(ValueError):
            extras_env((("a=b", "c"),))
        self.assertGreaterEqual(len(SERVER_CAPACITY_MESSAGE), 24)

    def test_schedulable_nodes_matching_mirrors_the_filter(self) -> None:
        nodes = [
            _node("w1", WORKER_LABELS),
            _node("w2", WORKER_LABELS, cordoned=True),
            _node("w3", WORKER_LABELS, ready=False),
            _node("w4", {**WORKER_LABELS, "space.pplx.net/ssd-placement-blocked": "true"}),
            _node("w5", {**WORKER_LABELS, "kubernetes.io/os": "windows"}),
            _node("cp", {"node-role.kubernetes.io/control-plane": ""}),
        ]
        self.assertEqual(schedulable_nodes_matching(nodes, FULL), ["w1"])
        self.assertEqual(schedulable_nodes_matching(nodes, expected_selector(DISC, (*SATISFIED_EXTRAS, ABSENT_EXTRA))), [])

    def test_render_mismatch_names_dropped_and_spurious_entries(self) -> None:
        ok = SandboxPodRender("p", dict(FULL), "Running", "w1", None)
        self.assertIsNone(render_mismatch(ok, FULL))
        dropped = SandboxPodRender("p", {DISC[0]: "true"}, "Running", "w1", None)
        self.assertIn("kubernetes.io/os=linux", render_mismatch(dropped, FULL) or "")
        spurious = SandboxPodRender("p", {**FULL, "x": "y"}, "Running", "w1", None)
        self.assertIn("x=y", render_mismatch(spurious, FULL) or "")


class Parsing(unittest.TestCase):
    def test_nodes_and_newest_pod(self) -> None:
        nodes = _parse_nodes(json.dumps({"items": [
            {"metadata": {"name": "w1", "labels": WORKER_LABELS}, "spec": {}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}},
            {"metadata": {"name": "w2", "labels": {}}, "spec": {"unschedulable": True}, "status": {"conditions": []}},
        ]}))
        self.assertEqual([(n.name, n.ready, n.unschedulable) for n in nodes], [("w1", True, False), ("w2", False, True)])
        pods = json.dumps({"items": [
            {"metadata": {"name": "old", "creationTimestamp": "2026-09-23T10:00:00Z", "labels": {}}, "spec": {"nodeSelector": {"a": "1"}, "nodeName": "w1"}, "status": {"phase": "Running"}},
            {"metadata": {"name": "gone", "creationTimestamp": "2026-09-23T12:00:00Z", "deletionTimestamp": "x", "labels": {}}, "spec": {}, "status": {}},
            {"metadata": {"name": "new", "creationTimestamp": "2026-09-23T11:00:00Z", "labels": {"space.pplx.net/pool-role": "warm"}}, "spec": {"nodeSelector": {"a": "2"}}, "status": {"phase": "Pending"}},
        ]})
        render = _newest_pod_render(pods)
        assert render is not None
        self.assertEqual((render.pod_name, render.node_selector, render.phase, render.node_name, render.pool_role), ("new", {"a": "2"}, "Pending", None, "warm"))
        self.assertIsNone(_newest_pod_render(json.dumps({"items": []})))


class _Fixture:
    def __init__(self, control: _Control) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SID)
        self.records: dict[str, object] = {}
        self.sandboxes = self
        self._control = control
        self.created: list[_Sandbox] = []

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        pass

    def sandbox_template_name(self) -> str:
        return "seed"

    def create(self, *, metadata=None):
        # The real helper's guard (services.py `_create_metadata`): the scenario
        # and run identity are the framework's; a caller supplying them is a
        # configuration error. Bench round 1 died on exactly this.
        reserved = set(metadata or {}) & {"space_smoke_scenario", "space_smoke_run", "space_smoke_instance"}
        if reserved:
            from space_smoke.runtime import SmokeConfigurationError

            raise SmokeConfigurationError(
                "scenario metadata cannot override run identity: " + ", ".join(sorted(reserved))
            )
        self.created_with = dict(metadata or {})
        sandbox = _Sandbox(SID, self._control)
        self.created.append(sandbox)
        return sandbox


class _Sandbox:
    def __init__(self, sandbox_id: str, control: _Control) -> None:
        self.sandbox_id = sandbox_id
        self._control = control

    def kill(self) -> None:
        self._control.calls.append(f"kill:{self.sandbox_id[:4]}")
        self._control.renders.pop(self.sandbox_id, None)


class _Control:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.env: dict[str, str | None] = {
            "api:SPACE_SANDBOX_NODE_SELECTOR_LABELS": None,
            "api:SPACE_SANDBOX_NODE_SELECTOR_KEY": DISC[0],
            "api:SPACE_SANDBOX_NODE_SELECTOR_VALUE": DISC[1],
            "api:SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED": "",
            "controller:SPACE_SANDBOX_NODE_SELECTOR_LABELS": None,
            "internal-api:SPACE_SANDBOX_NODE_SELECTOR_LABELS": "example.com/standing=true",
        }
        self.holders: list[str] = []
        self.nodes = [_node("w1", WORKER_LABELS), _node("w2", WORKER_LABELS)]
        self.renders: dict[str, SandboxPodRender] = {
            SID: SandboxPodRender("pod-ecw", dict(FULL), "Running", "w1", None),
            NATIVE: SandboxPodRender("pod-native", dict(FULL), "Running", "w2", None),
        }
        self.pod_count = 0
        # What `sandbox_pods` shows poll by poll after a kill; the last entry
        # repeats. An unscripted sandbox's pod is absent at once.
        self.pods_after_kill: dict[str, list[list[SandboxPodView]]] = {}

    def acquire_public_api_mutation(self):
        self.calls.append("lease")
        return lambda: self.calls.append("lease:release")

    def renderer_env(self):
        return dict(self.env)

    def set_renderer_env(self, values):
        self.calls.append("set:" + ",".join(f"{k}={v}" for k, v in sorted(values.items())))
        self.env.update(values)

    def restore_renderer_env(self, standing):
        self.calls.append("restore")
        self.env.update(standing)

    def wait_public_api_ready(self, *, timeout):
        self.calls.append("ready")

    def nodes_snapshot(self):
        return list(self.nodes)

    def nodes_carrying_label(self, key):
        return list(self.holders)

    def node_labels(self, node_name):
        return dict(next(n.labels for n in self.nodes if n.name == node_name))

    def sandbox_pod_render(self, sandbox_id):
        return self.renders.get(sandbox_id)

    def sandbox_pods(self, sandbox_id):
        script = self.pods_after_kill.get(sandbox_id)
        if not script:
            return []
        return list(script.pop(0) if len(script) > 1 else script[0])

    def sandbox_pod_count(self):
        return self.pod_count

    def native_create_sandbox(self, *, template, metadata):
        self.calls.append(f"native-create:{template}")
        self.native_metadata = dict(metadata)
        return NATIVE



class _Probe:
    def __init__(self, outcome: LimiterProbeOutcome | None = None, control: _Control | None = None) -> None:
        self.outcome = outcome or LimiterProbeOutcome(429, "1", SERVER_CAPACITY_MESSAGE + " are available. Not specific to your client — retry with backoff.", None, 0.0)
        self.kills = 0
        self.control = control

    def kill_sandbox(self, sandbox_id):
        assert self.control is not None
        self.control.calls.append(f"kill:{sandbox_id[:4]}")
        self.control.renders.pop(sandbox_id, None)

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        self.body = dict(body)
        return self.outcome

    def kill_matching(self, metadata):
        self.kills += 1
        self.killed_with = dict(metadata)


def _assertions(control: _Control | None = None, probe: _Probe | None = None):
    control = control or _Control()
    probe = probe or _Probe()
    probe.control = control
    fixture = _Fixture(control)
    scope = ResourceScope()
    return fixture, NodeSelectorExtrasAssertions(cast(Any, fixture), cast(Any, control), cast(Any, probe), scope), scope, control, probe


def _through_positive(control=None, probe=None):
    fixture, extras, scope, control, probe = _assertions(control, probe)
    extras.preconditions()
    extras.configure(SATISFIED_EXTRAS)
    return fixture, extras, scope, control, probe


class Preconditions(unittest.TestCase):
    def test_a_held_absent_key_is_refused(self) -> None:
        control = _Control()
        control.holders = ["w1"]
        _, extras, _, _, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "already carries the absent extra"):
            extras.preconditions()

    def test_a_bench_already_composing_from_our_extras_is_refused(self) -> None:
        control = _Control()
        control.env["api:SPACE_SANDBOX_NODE_SELECTOR_LABELS"] = extras_env(SATISFIED_EXTRAS)
        _, extras, _, _, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "already composes"):
            extras.preconditions()

    def test_a_missing_discriminator_is_refused(self) -> None:
        control = _Control()
        control.env["api:SPACE_SANDBOX_NODE_SELECTOR_KEY"] = None
        _, extras, _, _, _ = _assertions(control)
        with self.assertRaisesRegex(InvariantViolation, "SPACE_SANDBOX_NODE_SELECTOR_KEY"):
            extras.preconditions()


class Configure(unittest.TestCase):
    def test_restore_is_armed_before_the_first_write_and_all_renderers_get_the_value(self) -> None:
        fixture, extras, scope, control, _ = _through_positive()
        first_set = next(i for i, c in enumerate(control.calls) if c.startswith("set:"))
        self.assertLess(control.calls.index("lease"), first_set)
        self.assertIn("api:SPACE_SANDBOX_NODE_SELECTOR_LABELS=" + extras_env(SATISFIED_EXTRAS), control.calls[first_set])
        self.assertIn("controller:SPACE_SANDBOX_NODE_SELECTOR_LABELS=" + extras_env(SATISFIED_EXTRAS), control.calls[first_set])
        self.assertIn("internal-api:SPACE_SANDBOX_NODE_SELECTOR_LABELS=" + extras_env(SATISFIED_EXTRAS), control.calls[first_set])
        self.assertIn("api:SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED=true", control.calls[first_set])
        scope.close()
        self.assertLess(control.calls.index("restore"), control.calls.index("lease:release"))
        self.assertEqual(control.env["api:SPACE_SANDBOX_NODE_SELECTOR_LABELS"], None)
        self.assertEqual(control.env["internal-api:SPACE_SANDBOX_NODE_SELECTOR_LABELS"], "example.com/standing=true")
        self.assertEqual(control.env["api:SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED"], "")

    def test_a_renderer_not_carrying_the_value_fails(self) -> None:
        control = _Control()
        original = control.set_renderer_env

        def half(values):
            original({k: v for k, v in values.items() if not k.startswith("internal-api:")})

        control.set_renderer_env = half  # type: ignore[method-assign]
        _, extras, _, _, _ = _assertions(control)
        extras.preconditions()
        with self.assertRaisesRegex(InvariantViolation, "does not carry the extras"):
            extras.configure(SATISFIED_EXTRAS)


class Positive(unittest.TestCase):
    def test_both_renders_pass_and_the_sandboxes_are_killed(self) -> None:
        fixture, extras, _, control, _ = _through_positive()
        extras.expect_ecw_create_renders_and_places()
        extras.expect_native_create_renders_and_places(template="seed")
        self.assertIn("kill:01a0", control.calls)
        self.assertIn("kill:6f1e", control.calls)
        self.assertEqual(control.native_metadata["space_smoke_scenario"], "local.node_selector_extras_render_and_enforce")
        self.assertIn("render:space-api (ECW route)", fixture.records)
        self.assertEqual(fixture.created_with, {"space_smoke_role": "selector-extras-probe"})
        self.assertIn("render:space-cluster-controller (native route)", fixture.records)

    def test_a_dropped_extra_fails_by_renderer(self) -> None:
        control = _Control()
        control.renders[NATIVE] = SandboxPodRender("pod-native", {DISC[0]: "true"}, "Running", "w2", None)
        _, extras, _, _, _ = _through_positive(control)
        extras.expect_ecw_create_renders_and_places()
        with self.assertRaisesRegex(InvariantViolation, r"not the discriminator plus every extra \[space-cluster-controller"):
            extras.expect_native_create_renders_and_places(template="seed")

    def test_a_warm_pool_pod_is_refused_as_evidence(self) -> None:
        control = _Control()
        control.renders[SID] = SandboxPodRender("pod-ecw", dict(FULL), "Running", "w1", "warm")
        _, extras, _, _, _ = _through_positive(control)
        with self.assertRaisesRegex(InvariantViolation, "warm pool"):
            extras.expect_ecw_create_renders_and_places()

    def test_a_node_lacking_an_entry_fails(self) -> None:
        control = _Control()
        control.nodes = [_node("w1", {**WORKER_LABELS, "kubernetes.io/os": "windows"}), _node("w2", WORKER_LABELS)]
        _, extras, _, _, _ = _through_positive(control)
        with self.assertRaisesRegex(InvariantViolation, "node lacks an entry"):
            extras.expect_ecw_create_renders_and_places()

    def test_a_failed_pod_fails_before_the_budget(self) -> None:
        control = _Control()
        control.renders[SID] = SandboxPodRender("pod-ecw", dict(FULL), "Failed", None, None)
        _, extras, _, _, _ = _through_positive(control)
        with self.assertRaisesRegex(InvariantViolation, "did not reach Running"):
            extras.expect_ecw_create_renders_and_places(pod_timeout=60)


def _terminating(pod_name: str = "pod-ecw", node_name: str = "w1") -> SandboxPodView:
    return SandboxPodView(pod_name, "Running", node_name, "1", "", "", "", deleting=True)


class PodGone(unittest.TestCase):
    """The kill phases wait for the pod object to be absent, not only non-terminating."""

    def _run_ecw_phase(self, script: list[list[SandboxPodView]], clock: list[float]) -> None:
        fixture, extras, _, control, _ = _through_positive()
        control.pods_after_kill[SID] = script

        def advance(seconds: float, *, reason: str | None = None) -> None:
            clock[0] += seconds

        fixture.wait = advance  # type: ignore[method-assign]
        with mock.patch("space_smoke.node_selector_extras.time.monotonic", lambda: clock[0]):
            extras.expect_ecw_create_renders_and_places()

    def test_a_pod_that_finishes_terminating_is_waited_out(self) -> None:
        clock = [0.0]
        self._run_ecw_phase([[_terminating()]] * 3 + [[]], clock)
        self.assertEqual(clock[0], 3)

    def test_a_pod_stuck_terminating_fails_the_kill_phase_long_before_the_budget(self) -> None:
        clock = [0.0]
        with self.assertRaisesRegex(InvariantViolation, "stuck Terminating"):
            self._run_ecw_phase([[_terminating()]], clock)
        self.assertEqual(clock[0], 30)


class Negative(unittest.TestCase):
    def _negative(self, control=None, probe=None):
        fixture, extras, scope, control, probe = _through_positive(control, probe)
        extras.configure((*SATISFIED_EXTRAS, ABSENT_EXTRA))
        return fixture, extras, scope, control, probe

    def test_the_capacity_429_with_retry_after_passes_and_nothing_is_swept(self) -> None:
        fixture, extras, scope, control, probe = self._negative()
        outcome = extras.expect_absent_extra_refuses()
        self.assertEqual(outcome.status, 429)
        self.assertEqual(probe.body["templateID"], "seed")
        scope.close()
        self.assertEqual(probe.kills, 0)

    def test_a_fully_passing_run_owes_the_sweep_nothing(self) -> None:
        # Bench round 3: every verify passed and teardown then failed because
        # the natively created sandbox, already killed by its own phase, was
        # still owed to the sweep, whose empty match is an error.
        fixture, extras, scope, control, probe = _through_positive()
        extras.expect_ecw_create_renders_and_places()
        extras.expect_native_create_renders_and_places(template="seed")
        extras.configure((*SATISFIED_EXTRAS, ABSENT_EXTRA))
        extras.expect_absent_extra_refuses()
        extras.restore()
        failures = scope.close()
        self.assertEqual(failures, ())
        self.assertEqual(probe.kills, 0)

    def test_a_native_create_that_raises_after_dispatch_is_still_swept(self) -> None:
        # The POST may commit and the operation poll then
        # fail; the sweep must reconcile by metadata regardless.
        fixture, extras, scope, control, probe = _through_positive()

        def failing_create(*, template, metadata):
            raise RuntimeError("native create operation op-1 ended failed")

        control.native_create_sandbox = failing_create  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "ended failed"):
            extras.expect_native_create_renders_and_places(template="seed")
        scope.close()
        self.assertEqual(probe.kills, 1)

    def test_a_native_kill_that_raises_leaves_the_sandbox_owed_to_the_sweep(self) -> None:
        fixture, extras, scope, control, probe = _through_positive()

        def failing_kill(sandbox_id):
            raise RuntimeError("HTTP 502 from kill")

        probe.kill_sandbox = failing_kill  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "502"):
            extras.expect_native_create_renders_and_places(template="seed")
        scope.close()
        self.assertEqual(probe.kills, 1)

    def test_a_fleet_still_matching_the_full_set_fails_before_the_request(self) -> None:
        control = _Control()
        control.nodes = [_node("w1", {**WORKER_LABELS, ABSENT_EXTRA[0]: ABSENT_EXTRA[1]})]
        probe = _Probe()
        _, extras, _, _, _ = self._negative(control, probe)
        with self.assertRaisesRegex(InvariantViolation, "added extra is not absent"):
            extras.expect_absent_extra_refuses()
        self.assertFalse(hasattr(probe, "body"))

    def test_an_admitted_create_is_named_fail_open_before_the_status(self) -> None:
        control = _Control()
        probe = _Probe(LimiterProbeOutcome(201, None, "", "sbx_new", 0.0))
        _, extras, scope, control, probe = self._negative(control, probe)
        control.pod_count = 0
        original = control.sandbox_pod_count
        counts = iter([0, 1])
        control.sandbox_pod_count = lambda: next(counts)  # type: ignore[method-assign]
        with self.assertRaisesRegex(InvariantViolation, "rendered a sandbox pod"):
            extras.expect_absent_extra_refuses()
        del original
        scope.close()
        self.assertEqual(probe.kills, 1)
        self.assertEqual(probe.killed_with["space_smoke_role"], "selector-extras-probe")

    def test_a_429_with_another_limiter_message_fails(self) -> None:
        probe = _Probe(LimiterProbeOutcome(429, "1", "Server rate limit: slow down", None, 0.0))
        _, extras, _, _, _ = self._negative(None, probe)
        with self.assertRaisesRegex(InvariantViolation, "not refused with the empty-cluster capacity 429"):
            extras.expect_absent_extra_refuses()

    def test_a_capacity_429_without_retry_after_fails(self) -> None:
        probe = _Probe(LimiterProbeOutcome(429, None, SERVER_CAPACITY_MESSAGE + " are available.", None, 0.0))
        _, extras, _, _, _ = self._negative(None, probe)
        with self.assertRaisesRegex(InvariantViolation, "no Retry-After"):
            extras.expect_absent_extra_refuses()

    def test_an_ambiguous_dispatch_sweeps_by_metadata(self) -> None:
        probe = _Probe(LimiterProbeOutcome(0, None, "timed out", None, 0.0))
        _, extras, scope, _, probe = self._negative(None, probe)
        with self.assertRaises(InvariantViolation):
            extras.expect_absent_extra_refuses()
        scope.close()
        self.assertEqual(probe.kills, 1)


class Restore(unittest.TestCase):
    def test_explicit_restore_verifies_the_standing_env_and_releases_once(self) -> None:
        fixture, extras, scope, control, _ = self_negative = _through_positive()
        extras.restore()
        self.assertEqual(control.env["api:SPACE_SANDBOX_NODE_SELECTOR_LABELS"], None)
        self.assertEqual(control.env["internal-api:SPACE_SANDBOX_NODE_SELECTOR_LABELS"], "example.com/standing=true")
        self.assertIn("restored_env", fixture.records)
        before = list(control.calls)
        scope.close()
        self.assertEqual(control.calls, before)
        del self_negative

    def test_a_restore_that_did_not_land_fails(self) -> None:
        control = _Control()
        control.restore_renderer_env = lambda standing: control.calls.append("restore-noop")  # type: ignore[method-assign]
        _, extras, _, _, _ = _through_positive(control)
        with self.assertRaisesRegex(InvariantViolation, "does not carry its standing env"):
            extras.restore()


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


ARGV = ("kubectl",)


class HarnessOps(unittest.TestCase):
    def test_renderer_env_reads_all_deployments(self) -> None:
        api_env = json.dumps([{"name": "SPACE_SANDBOX_NODE_SELECTOR_KEY", "value": DISC[0]}, {"name": "SPACE_SANDBOX_NODE_SELECTOR_VALUE", "value": DISC[1]}])
        harness = KindHarness(_ScriptedKubectl([
            CommandResult(ARGV, 0, "space-api", ""), CommandResult(ARGV, 0, api_env, ""),
            CommandResult(ARGV, 0, "space-cluster-controller", ""), CommandResult(ARGV, 0, "[]", ""),
            CommandResult(ARGV, 0, "space-internal-api", ""), CommandResult(ARGV, 0, "[]", ""),
        ]), KindConfig())
        env = harness.renderer_env()
        self.assertEqual(env["api:SPACE_SANDBOX_NODE_SELECTOR_KEY"], DISC[0])
        self.assertIsNone(env["api:SPACE_SANDBOX_NODE_SELECTOR_LABELS"])
        self.assertIsNone(env["controller:SPACE_SANDBOX_NODE_SELECTOR_LABELS"])
        self.assertIsNone(env["internal-api:SPACE_SANDBOX_NODE_SELECTOR_LABELS"])

    def test_set_renderer_env_refuses_foreign_keys_and_unsafe_values(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with self.assertRaises(ValueError):
            harness.set_renderer_env({"api:SPACE_INTERNAL_TOKEN": "x"})
        with self.assertRaises(ValueError):
            harness.set_renderer_env({"api:SPACE_SANDBOX_NODE_SELECTOR_LABELS": "a=b;rm -rf /"})
        with self.assertRaises(ValueError):
            harness.set_renderer_env({"api:SPACE_SANDBOX_NODE_SELECTOR_KEY": "x"})

    def test_nodes_carrying_label_refuses_an_unsafe_key(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with self.assertRaises(ValueError):
            harness.nodes_carrying_label("a b")

    def test_native_create_strips_the_prefix_and_waits_for_the_operation(self) -> None:
        import sys
        import types
        from unittest import mock

        class _Resp:
            def __init__(self, payload):
                self._payload = payload
                self.status = 202

            def read(self):
                return json.dumps(self._payload).encode()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        payloads = iter([
            {"operation_id": "op-1", "sandbox_id": f"sbx_{NATIVE}"},
            {"state": "running"},
            {"state": "succeeded"},
        ])
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        seen: list[str] = []

        def fake_urlopen(request, timeout=None):
            seen.append(f"{request.get_method()} {request.full_url}")
            # Every admin call logs in first; the login answers a token, the rest the script.
            if request.full_url.endswith("/auth/v1/sessions"):
                return _Resp({"token": "tok"})
            return _Resp(next(payloads))

        with (
            mock.patch("space_smoke.adapters.urlopen", fake_urlopen),
            mock.patch("space_smoke.adapters._URL_OPEN", fake_urlopen),
            mock.patch("space_smoke.adapters.time.sleep", lambda s: None),
        ):
            sandbox_id = harness.native_create_sandbox(template="seed", metadata={"space_smoke_run": "r"})
        self.assertEqual(sandbox_id, NATIVE)
        self.assertTrue(any(s.startswith("POST") and s.endswith("/v1/sandboxes") for s in seen))
        self.assertEqual(sum("operations/op-1" in s for s in seen), 2)
        del sys, types


if __name__ == "__main__":
    unittest.main()

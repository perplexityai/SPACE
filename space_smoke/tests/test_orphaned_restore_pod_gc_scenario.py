"""Contracts of the orphaned-restore-pod GC scenario the bench cannot show.

A passing bench run shows planted orphans reaped and a pod restored under the
live GC left alone. It cannot show that the controller env override refuses
keys and values off its closed lists or runs without the Lease, that the
standing env is put back after a failed override, that a plant's removal is
armed before the create and selects on the plant's own label, that the clone
drops the source pod's identity and binding, or that the verdicts fail on a
reap with the wrong reason or a GC record naming the live pod. Those are
pinned here.
"""

from __future__ import annotations

import json
import unittest
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke import adapters
from space_smoke import orphaned_restore_pod_gc as gc_module
from space_smoke.adapters import KindConfig, KindHarness, _LeaseOwnership
from space_smoke.capabilities import ExecutionRow, SandboxPodView
from space_smoke.fixtures import KindEcw
from space_smoke.orphaned_restore_pod_gc import (
    GC_ENABLED_ENV,
    GC_THRESHOLD_ENV,
    PLANTED_ORPHAN_LABEL,
    UNSCHEDULABLE_SELECTOR_KEY,
    GcSettings,
    LivePod,
    OrphanedRestorePodGcAssertions,
    PlantedOrphan,
    build_planted_orphan,
    planted_pod_name,
)
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    SmokeConfigurationError,
)
from space_smoke.scenarios.local_orphaned_restore_pod_gc_reaps_only_superseded import (
    orphaned_restore_pod_gc_reaps_only_superseded as scenario,
)

SANDBOX = "3f0b6c1e-8f5d-4d7e-9a61-0c2b8e4f7a10"
ARGV = ("kubectl",)


def _ok(stdout: str = "") -> CommandResult:
    return CommandResult(ARGV, 0, stdout, "")


def _template() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"space-sbx-{SANDBOX.replace('-', '')}-g2",
            "namespace": "space",
            "uid": "source-uid",
            "resourceVersion": "42",
            "creationTimestamp": "2026-09-29T00:00:00Z",
            "ownerReferences": [{"kind": "Node", "name": "worker"}],
            "finalizers": ["example.test/keep"],
            "managedFields": [{"manager": "space-api"}],
            "labels": {
                "space.pplx.net/managed-by": "space-api",
                "space.pplx.net/sandbox-id": SANDBOX,
                "space.pplx.net/execution-generation": "2",
            },
            "annotations": {
                "space.pplx.net/sandbox-id": SANDBOX,
                "space.pplx.net/execution-generation": "2",
                "space.pplx.net/checkpoint": "suspend",
            },
        },
        "spec": {
            "nodeName": "space-local-worker",
            "nodeSelector": {"space.pplx.net/node-pool": "sandbox"},
            "containers": [{"name": "sandbox", "image": "shim"}],
        },
        "status": {"phase": "Running", "podIP": "10.0.0.2"},
    }


class _ScriptedKubectl:
    def __init__(self, results: list[CommandResult]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []
        self.inputs: list[str | None] = []

    def run(self, command: Any, *, input_text: str | None = None, **_options: Any) -> CommandResult:
        self.calls.append(tuple(command))
        self.inputs.append(input_text)
        return self.results.pop(0)


def _live_lease(harness: KindHarness) -> _ScriptedKubectl:
    harness._retention_lease_ownership = _LeaseOwnership(
        name="space-smoke-retention-x", uid="lease-uid", holder="me", subject="retention controller mutation"
    )
    return cast(_ScriptedKubectl, harness.executor)


def _lease_json() -> str:
    now = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    return json.dumps(
        {
            "metadata": {"uid": "lease-uid", "resourceVersion": "7"},
            "spec": {"holderIdentity": "me", "leaseDurationSeconds": 3600, "renewTime": now},
        }
    )


class Registration(unittest.TestCase):
    def test_registered_with_kind_only_fixture(self) -> None:
        self.assertEqual(scenario.id, "local.orphaned_restore_pod_gc_reaps_only_superseded")
        self.assertEqual(scenario.category, "local")
        self.assertIs(scenario.fixture_type, KindEcw)
        self.assertEqual(scenario.backends, frozenset({"kind"}))


class Choreography(unittest.TestCase):
    def test_goes_live_before_planting_and_reaps_the_own_row_orphan_before_restoring(self) -> None:
        fixture = mock.Mock()
        fixture.sandbox.sandbox_id = SANDBOX
        fixture.phase.side_effect = lambda _name: nullcontext()
        fixture.run_command_when_ready.return_value = SimpleNamespace(exit_code=0, stdout="", stderr="")

        def verify(condition: bool, message: str, **_evidence: object) -> None:
            if not condition:
                raise InvariantViolation(message)

        fixture.verify.side_effect = verify
        gc = fixture.orphaned_restore_pod_gc
        gc.scoped_live_gc.return_value = nullcontext()
        gc.capture_restore_pod.return_value = LivePod("space-sbx-a-g2", "uid-2", 2)
        suspended_rows = [
            [ExecutionRow(1, "replaced", False, "g1"), ExecutionRow(2, "replaced", False, "g2")],
            [ExecutionRow(g, "replaced", False, f"g{g}") for g in (1, 2, 3)],
            [ExecutionRow(g, "replaced", False, f"g{g}") for g in (1, 2, 3, 4)],
        ]
        gc.wait_until_suspended.side_effect = suspended_rows
        gc.plant.side_effect = lambda **kw: SimpleNamespace(**kw)
        gc.expect_one_live_execution.side_effect = [
            LivePod("space-sbx-a-g2", "uid-2", 2),
            LivePod("space-sbx-a-g4", "uid-4", 4),
            LivePod("space-sbx-a-g5", "uid-5", 5),
        ]

        scenario.verify(cast(Any, fixture))

        names = [c[0] for c in fixture.mock_calls]
        calls = fixture.mock_calls
        live = names.index("orphaned_restore_pod_gc.go_live")
        self.assertEqual(
            calls[live],
            mock.call.orphaned_restore_pod_gc.go_live(threshold_seconds=20, interval_seconds=2),
        )
        self.assertLess(names.index("orphaned_restore_pod_gc.capture_restore_pod"), live)
        plants = [i for i, n in enumerate(names) if n == "orphaned_restore_pod_gc.plant"]
        self.assertTrue(all(live < i for i in plants))
        self.assertEqual(
            [calls[i] for i in plants],
            [
                mock.call.orphaned_restore_pod_gc.plant(sandbox_id=SANDBOX, generation=1),
                mock.call.orphaned_restore_pod_gc.plant(generation=1),
                mock.call.orphaned_restore_pod_gc.plant(sandbox_id=SANDBOX, generation=4),
            ],
        )
        # The restored phase restores with the GC live, then watches the one
        # live execution's pod above the suspended row's generations.
        suspends = [i for i, n in enumerate(names) if n == "orphaned_restore_pod_gc.wait_until_suspended"]
        reconnects = [i for i, n in enumerate(names) if n == "reconnect_by_id"]
        lives = [i for i, n in enumerate(names) if n == "orphaned_restore_pod_gc.expect_one_live_execution"]
        undisturbed = names.index("orphaned_restore_pod_gc.expect_undisturbed")
        self.assertLess(live, reconnects[1])
        self.assertLess(suspends[1], reconnects[1])
        self.assertLess(reconnects[1], lives[1])
        self.assertLess(lives[1], undisturbed)
        self.assertEqual(calls[lives[1]].kwargs["above"], 3)
        self.assertEqual(calls[undisturbed].args[0], LivePod("space-sbx-a-g4", "uid-4", 4))
        self.assertNotIn("concurrent_restore", {n.split(".")[0] for n in names})
        # The own-row orphan is reaped before the final restore is attempted.
        reaps = [i for i, n in enumerate(names) if n == "orphaned_restore_pod_gc.expect_reaped"]
        self.assertEqual(len(reaps), 3)
        self.assertLess(plants[2], reaps[2])
        self.assertLess(reaps[2], reconnects[-1])
        gc.scoped_live_gc.assert_called_once_with()


class EnvOverride(unittest.TestCase):
    def test_refuses_keys_and_values_off_the_lists_before_kubectl(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        _live_lease(harness)
        with self.assertRaisesRegex(ValueError, "non-GC"):
            harness.set_orphaned_restore_pod_gc_env({"SPACE_INTERNAL_TOKEN": "x"})
        with self.assertRaisesRegex(ValueError, "refusing GC env value"):
            harness.set_orphaned_restore_pod_gc_env({GC_ENABLED_ENV: "yes"})
        with self.assertRaisesRegex(ValueError, "refusing GC env value"):
            harness.set_orphaned_restore_pod_gc_env({GC_THRESHOLD_ENV: "0"})
        with self.assertRaisesRegex(ValueError, "non-GC"):
            harness.restore_orphaned_restore_pod_gc_env({"SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED": None})
        self.assertEqual(cast(_ScriptedKubectl, harness.executor).calls, [])

    def test_refuses_to_write_without_the_lease(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with self.assertRaisesRegex(SmokeConfigurationError, "live mutation Lease"):
            harness.set_orphaned_restore_pod_gc_env({GC_ENABLED_ENV: "true"})
        self.assertEqual(cast(_ScriptedKubectl, harness.executor).calls, [])

    def test_sets_env_on_the_controller_container_then_awaits_the_rollout(self) -> None:
        kubectl = _ScriptedKubectl(
            [
                _ok(_lease_json()),
                _ok("space-cluster-controller"),
                _ok("cluster-controller"),
                _ok(),
                _ok(),
                # The post-rollout drain: the Deployment, then its one pod.
                _ok(json.dumps({"spec": {"replicas": 1, "selector": {"matchLabels": {"app": "x"}}}})),
                _ok(json.dumps({"items": [{"metadata": {"name": "p"}}]})),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        _live_lease(harness)
        harness.restore_orphaned_restore_pod_gc_env({GC_ENABLED_ENV: "false", GC_THRESHOLD_ENV: None})
        set_env = kubectl.calls[3]
        self.assertIn("set", set_env)
        self.assertEqual(
            set_env[set_env.index("env"):],
            (
                "env",
                "deploy/space-cluster-controller",
                "-c",
                "cluster-controller",
                f"{GC_ENABLED_ENV}=false",
                f"{GC_THRESHOLD_ENV}-",
            ),
        )
        self.assertIn("rollout", kubectl.calls[4])
        self.assertIn("deploy/space-cluster-controller", kubectl.calls[4])


class PlantedOrphanShape(unittest.TestCase):
    def test_clone_drops_identity_and_binding_and_restamps_the_generation(self) -> None:
        name = planted_pod_name(SANDBOX, 1, "0a1b2c3d")
        self.assertEqual(name, f"space-sbx-{SANDBOX.replace('-', '')}-g1-0a1b2c3d")
        template = _template()
        manifest = build_planted_orphan(
            template, name=name, sandbox_id=SANDBOX, generation=1, token="0a1b2c3d"
        )
        self.assertEqual(set(manifest), {"apiVersion", "kind", "metadata", "spec"})
        self.assertEqual(set(manifest["metadata"]), {"name", "labels", "annotations"})
        self.assertNotIn("nodeName", manifest["spec"])
        self.assertEqual(
            manifest["spec"]["nodeSelector"],
            {"space.pplx.net/node-pool": "sandbox", UNSCHEDULABLE_SELECTOR_KEY: "0a1b2c3d"},
        )
        for stamps in (manifest["metadata"]["labels"], manifest["metadata"]["annotations"]):
            self.assertEqual(stamps["space.pplx.net/execution-generation"], "1")
            self.assertEqual(stamps["space.pplx.net/sandbox-id"], SANDBOX)
        self.assertEqual(manifest["metadata"]["labels"][PLANTED_ORPHAN_LABEL], "0a1b2c3d")
        self.assertEqual(manifest["metadata"]["labels"]["space.pplx.net/managed-by"], "space-api")
        self.assertEqual(manifest["metadata"]["annotations"]["space.pplx.net/checkpoint"], "suspend")
        # The source document is not mutated.
        self.assertEqual(template, _template())

    def test_refuses_a_template_the_reaper_would_never_see(self) -> None:
        fresh = _template()
        del fresh["metadata"]["annotations"]["space.pplx.net/checkpoint"]
        with self.assertRaisesRegex(ValueError, "restore annotation"):
            build_planted_orphan(fresh, name="n", sandbox_id=SANDBOX, generation=1, token="0a1b2c3d")
        unmanaged = _template()
        del unmanaged["metadata"]["labels"]["space.pplx.net/managed-by"]
        with self.assertRaisesRegex(ValueError, "reflector"):
            build_planted_orphan(unmanaged, name="n", sandbox_id=SANDBOX, generation=1, token="0a1b2c3d")
        with self.assertRaisesRegex(ValueError, "non-positive"):
            build_planted_orphan(_template(), name="n", sandbox_id=SANDBOX, generation=0, token="0a1b2c3d")

    def test_harness_creates_the_clone_and_removes_by_plant_label_only(self) -> None:
        kubectl = _ScriptedKubectl([_ok(json.dumps({"metadata": {"uid": "planted-uid"}})), _ok()])
        harness = KindHarness(kubectl, KindConfig())
        name = planted_pod_name(SANDBOX, 1, "0a1b2c3d")
        uid = harness.plant_orphaned_restore_pod(
            _template(), name=name, sandbox_id=SANDBOX, generation=1, token="0a1b2c3d"
        )
        self.assertEqual(uid, "planted-uid")
        self.assertEqual(kubectl.calls[0][-5:], ("create", "-f", "-", "-o", "json"))
        created = json.loads(kubectl.inputs[0] or "{}")
        self.assertEqual(created["metadata"]["name"], name)
        self.assertNotIn("nodeName", created["spec"])
        harness.remove_planted_restore_pods("0a1b2c3d")
        self.assertEqual(
            kubectl.calls[1][-6:],
            (
                "delete",
                "pods",
                "-l",
                f"{PLANTED_ORPHAN_LABEL}=0a1b2c3d",
                "--ignore-not-found",
                "--wait=false",
            ),
        )
        with self.assertRaisesRegex(ValueError, "plant token"):
            harness.remove_planted_restore_pods("a,b")

    def test_planting_refuses_a_non_local_cluster(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig(cluster="prod"))
        with self.assertRaises(SmokeConfigurationError):
            harness.plant_orphaned_restore_pod(
                _template(), name="space-sbx-x-g1", sandbox_id=SANDBOX, generation=1, token="0a1b2c3d"
            )


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


class _Fixture:
    def __init__(self, clock: _Clock) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SANDBOX)
        self.records: dict[str, object] = {}
        self._clock = clock

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def wait(self, seconds: float) -> None:
        self._clock.now += seconds


class _Control:
    """A fake control plane: the plant/env writes and what the controller logged."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.present: set[str] = set()
        self.records: list[dict[str, Any]] = []
        self.fail_set = False
        self.fail_plant = False
        self.fail_restore = False
        self.env_after_restore: dict[str, str | None] | None = None
        self.restored = False
        self.executions = [ExecutionRow(4, "running", True, "space-sbx-a-g4")]

    def acquire_controller_mutation(self):
        self.calls.append("acquire")
        return lambda: self.calls.append("release")

    def orphaned_restore_pod_gc_env(self) -> dict[str, str | None]:
        self.calls.append("read_env")
        if self.restored and self.env_after_restore is not None:
            return dict(self.env_after_restore)
        return {GC_ENABLED_ENV: "false", GC_THRESHOLD_ENV: None}

    def set_orphaned_restore_pod_gc_env(self, overrides) -> None:
        self.calls.append("set_env")
        if self.fail_set:
            raise RuntimeError("rollout timed out after the write")

    def restore_orphaned_restore_pod_gc_env(self, standing) -> None:
        self.calls.append(f"restore_env:{sorted(standing.items(), key=lambda kv: kv[0])}")
        if self.fail_restore:
            raise RuntimeError("controller rollout did not complete")
        self.restored = True

    def controller_log_records(self, *, since_time: str) -> list[dict[str, Any]]:
        return list(self.records)

    def restore_pod_document(self, pod_name: str) -> dict[str, Any] | None:
        if pod_name not in self.present:
            return None
        return {"metadata": {"name": pod_name, "uid": f"uid-{pod_name}"}, "spec": {}}

    def plant_orphaned_restore_pod(self, template, *, name, sandbox_id, generation, token) -> str:
        self.calls.append(f"plant:{token}")
        if self.fail_plant:
            raise RuntimeError("create timed out")
        self.present.add(name)
        return f"uid-{name}"

    def remove_planted_restore_pods(self, token: str) -> None:
        self.calls.append(f"remove:{token}")

    def sandbox_executions(self, sandbox_id: str) -> list[ExecutionRow]:
        return list(self.executions)

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]:
        return [
            SandboxPodView("space-sbx-a-g4", "Running", "w", "4", "suspend", "w", "", False)
        ]

    def sandbox_residency(self, sandbox_id: str):
        return SimpleNamespace(lifecycle="started", active_node_id=None)


def _reap_records(orphan: PlantedOrphan, *, detail: str | None = None, age: int = 21) -> list[dict[str, Any]]:
    common = {"pod_name": orphan.name, "sandbox_id": orphan.sandbox_id}
    return [
        {
            **common,
            "loop": "orphaned_restore_pod_gc",
            "outcome": "requested_orphaned_restore_pod_delete",
            "mode": "live",
            "detail": detail or orphan.reason,
            "generation": orphan.generation,
            "age_secs": age,
        },
        {**common, "event_type": "cleanup_requested", "operation": "orphaned_restore_pod_gc"},
        {
            **common,
            "loop": "orphaned_restore_pod_gc",
            "outcome": "reaped_orphaned_restore_pod",
            "mode": "live",
            "generation": orphan.generation,
        },
        {
            **common,
            "event_type": "cleanup_completed",
            "operation": "orphaned_restore_pod_gc",
            "reason": "orphaned_restore_pod",
            "result": "deleted",
            "pod_uid": orphan.uid,
            "execution_generation": str(orphan.generation),
        },
    ]


class AssertionsContract(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = _Clock()
        patcher = mock.patch.object(gc_module, "time", self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.control = _Control()
        self.scope = ResourceScope()
        self.assertions = OrphanedRestorePodGcAssertions(
            cast(Any, _Fixture(self.clock)), cast(Any, self.control), self.scope
        )

    def _live(self) -> None:
        self.assertions._settings = GcSettings(threshold_seconds=20, interval_seconds=5)
        self.assertions._since = "2026-09-29T00:00:00Z"

    def _orphan(self, reason: str = "row_reverted") -> PlantedOrphan:
        return PlantedOrphan(
            name=planted_pod_name(SANDBOX, 3, "0a1b2c3d"),
            uid="uid-x",
            sandbox_id=SANDBOX,
            generation=3,
            token="0a1b2c3d",
            reason=reason,
        )

    def test_a_failed_override_still_puts_the_standing_env_back_then_releases(self) -> None:
        self.control.fail_set = True
        with self.assertRaises(RuntimeError):
            self.assertions.go_live(threshold_seconds=20, interval_seconds=5)
        self.scope.close()
        self.assertEqual(
            self.control.calls,
            [
                "acquire",
                "read_env",
                "set_env",
                f"restore_env:{[(GC_ENABLED_ENV, 'false'), (GC_THRESHOLD_ENV, None)]}",
                "release",
            ],
        )

    def test_go_live_requires_a_live_start_with_the_knobs(self) -> None:
        self.control.records = [
            {"message": "starting orphaned_restore_pod_gc", "dry_run": True, "threshold_secs": 20, "interval_secs": 5},
            {"message": "orphaned_restore_pod_gc tick complete", "dry_run": True},
        ]
        with self.assertRaisesRegex(InvariantViolation, "did not start"):
            self.assertions.go_live(threshold_seconds=20, interval_seconds=5)
        self.control.records = [
            {"message": "starting orphaned_restore_pod_gc", "dry_run": False, "threshold_secs": 20, "interval_secs": 5},
            {"message": "orphaned_restore_pod_gc tick complete", "dry_run": False, "scanned": 1},
        ]
        self.assertions.go_live(threshold_seconds=20, interval_seconds=5)

    def test_restore_standing_verifies_the_env_came_back(self) -> None:
        self.control.records = [
            {"message": "starting orphaned_restore_pod_gc", "dry_run": False, "threshold_secs": 20, "interval_secs": 5},
            {"message": "orphaned_restore_pod_gc tick complete", "dry_run": False},
        ]
        with self.assertions.scoped_live_gc():
            self.assertions.go_live(threshold_seconds=20, interval_seconds=5)
        self.assertEqual(self.control.calls[-2:], ["read_env", "release"])
        self.assertTrue(self.control.calls[-3].startswith("restore_env:"))

    def _go_live(self) -> None:
        self.control.records = [
            {"message": "starting orphaned_restore_pod_gc", "dry_run": False, "threshold_secs": 20, "interval_secs": 5},
            {"message": "orphaned_restore_pod_gc tick complete", "dry_run": False},
        ]
        self.assertions.go_live(threshold_seconds=20, interval_seconds=5)

    def _restores(self) -> int:
        return sum(1 for c in self.control.calls if c.startswith("restore_env:"))

    def test_a_failing_restore_keeps_the_body_error_and_the_backstop(self) -> None:
        self._go_live()
        self.control.fail_restore = True
        with self.assertRaises(ValueError) as raised:
            with self.assertions.scoped_live_gc():
                raise ValueError("the body's own failure")
        self.assertEqual(str(raised.exception), "the body's own failure")
        self.assertTrue(any("also failed" in note for note in raised.exception.__notes__))
        # Lease still held: its release waits for the backstop's retry.
        self.assertNotIn("release", self.control.calls)
        self.assertEqual(self._restores(), 1)
        self.control.fail_restore = False
        self.assertEqual(self.scope.close(), ())
        self.assertEqual(self._restores(), 2)
        self.assertEqual(self.control.calls[-1], "release")

    def test_an_env_that_did_not_come_back_keeps_the_backstop_armed(self) -> None:
        self._go_live()
        self.control.env_after_restore = {GC_ENABLED_ENV: "true", GC_THRESHOLD_ENV: "20"}
        with self.assertRaisesRegex(InvariantViolation, "did not come back"):
            with self.assertions.scoped_live_gc():
                pass
        self.assertNotIn("release", self.control.calls)
        self.scope.close()
        self.assertEqual(self._restores(), 2)
        self.assertEqual(self.control.calls[-1], "release")

    def test_a_verified_restore_disarms_the_backstop(self) -> None:
        self._go_live()
        with self.assertions.scoped_live_gc():
            pass
        self.assertEqual(self.control.calls[-1], "release")
        self.scope.close()
        self.assertEqual(self._restores(), 1)

    def test_a_plant_whose_create_fails_is_still_removed(self) -> None:
        self.assertions._template = {"metadata": {}, "spec": {}}
        self.control.fail_plant = True
        with self.assertRaises(RuntimeError):
            self.assertions.plant(generation=1)
        token = self.control.calls[0].split(":", 1)[1]
        self.scope.close()
        self.assertEqual(self.control.calls, [f"plant:{token}", f"remove:{token}"])

    def test_plant_without_a_row_expects_sandbox_missing(self) -> None:
        self.assertions._template = {"metadata": {}, "spec": {}}
        orphan = self.assertions.plant(generation=1)
        self.assertEqual(orphan.reason, "sandbox_missing")
        self.assertNotEqual(orphan.sandbox_id, SANDBOX)
        self.assertEqual(self.assertions.plant(sandbox_id=SANDBOX, generation=2).reason, "row_reverted")

    def test_a_confirmed_reap_passes_and_removes_nothing_twice(self) -> None:
        self._live()
        orphan = self._orphan()
        self.assertions._removals[orphan.token] = self.scope.defer(
            "remove", lambda: self.control.remove_planted_restore_pods(orphan.token)
        )
        self.control.records = _reap_records(orphan)
        self.assertions.expect_reaped(orphan)
        self.assertEqual(self.control.calls, [f"remove:{orphan.token}"])
        self.scope.close()
        self.assertEqual(self.control.calls, [f"remove:{orphan.token}"])

    def test_a_reap_for_the_wrong_reason_or_before_the_threshold_fails(self) -> None:
        self._live()
        orphan = self._orphan()
        self.control.records = _reap_records(orphan, detail="sandbox_missing")
        with self.assertRaisesRegex(InvariantViolation, "expected identity"):
            self.assertions.expect_reaped(orphan)
        self.control.records = _reap_records(orphan, age=5)
        with self.assertRaisesRegex(InvariantViolation, "expected identity"):
            self.assertions.expect_reaped(orphan)

    def test_a_double_confirmation_or_completion_fails(self) -> None:
        self._live()
        orphan = self._orphan()
        records = _reap_records(orphan)
        self.control.records = records + [records[2]]
        with self.assertRaisesRegex(InvariantViolation, "exactly once"):
            self.assertions.expect_reaped(orphan)
        self.control.records = records + [records[3]]
        with self.assertRaisesRegex(InvariantViolation, "exactly once"):
            self.assertions.expect_reaped(orphan)
        self.control.records = records + [records[0]]
        with self.assertRaisesRegex(InvariantViolation, "exactly once"):
            self.assertions.expect_reaped(orphan)

    def test_an_unconfirmed_reap_times_out(self) -> None:
        self._live()
        orphan = self._orphan()
        # The DELETE was accepted but the pod never left the cache.
        self.control.records = _reap_records(orphan)[:2]
        with self.assertRaisesRegex(InvariantViolation, "not reaped and confirmed"):
            self.assertions.expect_reaped(orphan)

    def test_any_gc_record_naming_the_live_pod_fails_the_survival_verdict(self) -> None:
        self._live()
        live = LivePod("space-sbx-a-g4", "uid-space-sbx-a-g4", 4)
        self.control.present.add(live.name)
        ticks = [{"message": "orphaned_restore_pod_gc tick complete", "scanned": 1}] * 10
        self.control.records = ticks
        self.assertions.expect_undisturbed(live)
        self.control.records = ticks + [
            {"loop": "orphaned_restore_pod_gc", "outcome": "skipped", "pod_name": live.name}
        ]
        with self.assertRaisesRegex(InvariantViolation, "acted on the live restore pod"):
            self.assertions.expect_undisturbed(live)

    def test_a_replaced_live_pod_fails_the_survival_verdict(self) -> None:
        self._live()
        live = LivePod("space-sbx-a-g4", "uid-before", 4)
        self.control.present.add(live.name)
        with self.assertRaisesRegex(InvariantViolation, "disturbed"):
            self.assertions.expect_undisturbed(live)


class PrivilegedNames(unittest.TestCase):
    def test_pod_reads_refuse_non_dns_names(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with self.assertRaises(ValueError):
            harness.restore_pod_document("x; rm -rf /")
        self.assertTrue(hasattr(adapters, "GC_ENV_KEYS"))


if __name__ == "__main__":
    unittest.main()

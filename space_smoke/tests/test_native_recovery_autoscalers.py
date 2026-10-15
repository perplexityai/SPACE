"""Native recovery against autoscaled and leader-elected Deployments, off-bench.

The faults wait for exact replica counts, which an autoscaler could move under
them, so each target's autoscaler is pinned at its Deployment's current count
for the faults' duration and its captured bounds put back after the
Deployments are restored, refusing an autoscaler replaced or re-bounded in
between. The autoscaler controller's own status writes between the harness's
read and its patch are not a conflict. The
dispatcher-fairness precondition counts dispatchers, not controller replicas:
any count has one while leader election is on.
"""

from __future__ import annotations

import json
import unittest
from typing import Any

from space_smoke.native_recovery import (
    DeploymentCapture,
    NativeRecoveryControl,
)
from space_smoke.runtime import CommandResult, SmokeConfigurationError


class _Kubectl:
    def __init__(self) -> None:
        self.hpas: dict[str, dict[str, Any]] = {
            "space-api": _hpa("space-api", "uid-api", 2, 3),
            "space-data-plane": _hpa("space-data-plane", "uid-dp", 2, 3),
            "unrelated": _hpa("space-ui", "uid-ui", 1, 1),
        }
        self.deployments = {
            name: {"metadata": {"uid": f"deploy-{name}", "resourceVersion": "1"}, "spec": {"replicas": replicas}}
            for name, replicas in (("space-api", 2), ("space-data-plane", 3), ("space-cluster-controller", 2))
        }
        self.patches: list[tuple[str, int, int]] = []
        self.scales: list[tuple[str, int]] = []

    def kubectl(self, *args: str, timeout: float | None = None, **_options: Any) -> CommandResult:
        if args[:2] == ("get", "hpa") and args[2] == "-o":
            return self._ok({"items": list(self.hpas.values())})
        if args[:2] == ("get", "hpa"):
            return self._read_then_write_status(self.hpas[args[2]])
        if args[:2] == ("get", "deployment"):
            return self._read_then_write_status(self.deployments[args[2]])
        if args[:2] == ("patch", "deployment"):
            deployment = self.deployments[args[2]]
            ops = json.loads(args[-1])
            if not self._preconditions_hold(deployment, ops):
                return CommandResult(args, 1, "", "test operation failed")
            (replicas,) = (op["value"] for op in ops if op["op"] == "replace")
            deployment["spec"]["replicas"] = replicas
            self.scales.append((args[2], replicas))
            return CommandResult(args, 0, "", "")
        if args[:2] == ("patch", "hpa"):
            hpa = self.hpas[args[2]]
            spec = hpa["spec"]
            ops = json.loads(args[-1])
            if not self._preconditions_hold(hpa, ops):
                return CommandResult(args, 1, "", "test operation failed")
            values = {op["path"].rsplit("/", 1)[-1]: op["value"] for op in ops if op["op"] == "replace"}
            spec.update(values)
            self.patches.append((args[2], values["minReplicas"], values["maxReplicas"]))
            return CommandResult(args, 0, "", "")
        raise AssertionError(f"unexpected kubectl call {args}")

    def _read_then_write_status(self, item: dict[str, Any]) -> CommandResult:
        reply = self._ok(item)
        # The owning controller writes status after this read and before the
        # harness's patch, which bumps the resourceVersion but not the spec.
        metadata = item["metadata"]
        metadata["resourceVersion"] = str(int(metadata["resourceVersion"]) + 1)
        return reply

    @staticmethod
    def _preconditions_hold(item: dict[str, Any], ops: list[dict[str, Any]]) -> bool:
        for op in ops:
            section, field = op["path"].strip("/").split("/")
            if op["op"] == "test" and item[section].get(field) != op["value"]:
                return False
        return True

    @staticmethod
    def _ok(value: object) -> CommandResult:
        return CommandResult(("kubectl",), 0, json.dumps(value), "")


def _hpa(target: str, uid: str, minimum: int, maximum: int) -> dict[str, Any]:
    return {
        "metadata": {"name": target, "uid": uid, "resourceVersion": "1"},
        "spec": {
            "scaleTargetRef": {"kind": "Deployment", "name": target},
            "minReplicas": minimum,
            "maxReplicas": maximum,
        },
    }


class _Context:
    def __init__(self) -> None:
        self.records: dict[str, object] = {}

    def record(self, name: str, value: object) -> None:
        self.records[name] = value


def _control(kubectl: _Kubectl) -> NativeRecoveryControl:
    control = NativeRecoveryControl.__new__(NativeRecoveryControl)
    control.context = _Context()  # type: ignore[assignment]
    control.harness = kubectl  # type: ignore[assignment]
    control._captures = {}
    control._autoscalers = {}
    control._release = lambda: None
    control._fence = lambda: None  # type: ignore[method-assign]
    control.sleeps = []  # type: ignore[attr-defined]
    control._sleep = control.sleeps.append  # type: ignore[attr-defined, method-assign]
    return control


class AutoscalerPinning(unittest.TestCase):
    def test_target_autoscalers_are_pinned_at_the_current_count_and_restored(self) -> None:
        kubectl = _Kubectl()
        control = _control(kubectl)
        control._pin_autoscalers(("space-api", "space-cluster-controller", "space-data-plane"))
        self.assertEqual(kubectl.patches, [("space-api", 2, 2), ("space-data-plane", 3, 3)])
        self.assertEqual(kubectl.hpas["unrelated"]["spec"]["maxReplicas"], 1)
        control.restore()
        self.assertEqual(kubectl.patches[2:], [("space-api", 2, 3), ("space-data-plane", 2, 3)])
        self.assertEqual(control._autoscalers, {})

    def test_a_foreign_change_fails_the_restore_instead_of_being_overwritten(self) -> None:
        cases = (
            ("uid", "metadata", "someone-else", "replaced autoscaler"),
            ("bounds", "spec", 4, "bounds changed concurrently"),
        )
        for case, section, value, message in cases:
            with self.subTest(case):
                kubectl = _Kubectl()
                control = _control(kubectl)
                control._pin_autoscalers(("space-api",))
                field = "uid" if section == "metadata" else "maxReplicas"
                kubectl.hpas["space-api"][section][field] = value
                with self.assertRaisesRegex(RuntimeError, message):
                    control.restore()
                self.assertEqual(kubectl.patches, [("space-api", 2, 2)])
                self.assertIn("space-api", control._autoscalers)

    def test_a_pin_that_never_landed_restores_as_a_no_op(self) -> None:
        kubectl = _Kubectl()
        control = _control(kubectl)
        real = kubectl.kubectl

        def refuse_patches(*args: str, **options: Any) -> CommandResult:
            if args[:2] == ("patch", "hpa"):
                return CommandResult(args, 1, "", "transient")
            return real(*args, **options)

        kubectl.kubectl = refuse_patches  # type: ignore[method-assign]
        with self.assertRaisesRegex(SmokeConfigurationError, "bounds update failed"):
            control._pin_autoscalers(("space-api",))
        kubectl.kubectl = real  # type: ignore[method-assign]
        self.assertEqual(control.sleeps, [0.5, 1.0])  # type: ignore[attr-defined]
        control.restore()
        self.assertEqual(kubectl.patches, [])
        self.assertEqual(kubectl.hpas["space-api"]["spec"]["maxReplicas"], 3)
        self.assertEqual(control._autoscalers, {})


class ControllerFault(unittest.TestCase):
    def test_the_controller_is_stopped_and_restored_across_status_writes(self) -> None:
        kubectl = _Kubectl()
        control = _control(kubectl)
        name = "space-cluster-controller"
        control._captures[name] = DeploymentCapture(f"deploy-{name}", 2, "x=y", "d")
        waits: list[tuple[str, int]] = []
        control._wait = lambda target, count, **_: waits.append((target, count))  # type: ignore[method-assign]
        control.stop_controller()
        control.restore_controller()
        self.assertEqual(kubectl.scales, [(name, 0), (name, 2)])
        self.assertEqual(waits, [(name, 0), (name, 2)])


class DispatcherCapacity(unittest.TestCase):
    def _check(self, replicas: int, election: str | None) -> None:
        control = _control(_Kubectl())
        control._captures["space-cluster-controller"] = DeploymentCapture("uid", replicas, "x=y", "d")

        class Harness:
            @staticmethod
            def _deployment_env_literals(name: str, keys: Any) -> dict[str, str | None]:
                return {
                    "SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY": None,
                    "SPACE_PUBLIC_OPERATION_DISPATCHER_TICK_SECS": None,
                    "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_ENABLED": election,
                }

        control.harness = Harness()  # type: ignore[assignment]
        control.require_dispatcher_capacity()

    def test_two_leader_elected_controllers_have_one_dispatcher(self) -> None:
        self._check(2, "true")
        self._check(2, None)

    def test_two_controllers_without_election_are_refused(self) -> None:
        with self.assertRaisesRegex(SmokeConfigurationError, "leader election"):
            self._check(2, "false")
        self._check(1, "false")


if __name__ == "__main__":
    unittest.main()

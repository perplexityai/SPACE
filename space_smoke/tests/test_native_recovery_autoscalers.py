"""Native recovery against autoscaled and leader-elected Deployments, off-bench.

The faults wait for exact replica counts, which an autoscaler could move under
them, so each target's autoscaler is pinned at its Deployment's current count
for the faults' duration and its captured bounds put back after the
Deployments are restored, refusing an autoscaler replaced in between. The
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
        self.replicas = {"space-api": 2, "space-data-plane": 3}
        self.patches: list[tuple[str, int, int]] = []

    def kubectl(self, *args: str, timeout: float | None = None, **_options: Any) -> CommandResult:
        if args[:2] == ("get", "hpa") and args[2] == "-o":
            return self._ok({"items": list(self.hpas.values())})
        if args[:2] == ("get", "hpa"):
            return self._ok(self.hpas[args[2]])
        if args[:2] == ("get", "deployment"):
            name = args[2]
            return self._ok({"metadata": {"uid": f"deploy-{name}"}, "spec": {"replicas": self.replicas[name]}})
        if args[:2] == ("patch", "hpa"):
            spec = self.hpas[args[2]]["spec"]
            ops = json.loads(args[-1])
            values = {op["path"].rsplit("/", 1)[-1]: op["value"] for op in ops if op["op"] == "replace"}
            spec.update(values)
            self.patches.append((args[2], values["minReplicas"], values["maxReplicas"]))
            return CommandResult(args, 0, "", "")
        raise AssertionError(f"unexpected kubectl call {args}")

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

    def test_a_replaced_autoscaler_fails_the_restore_instead_of_being_overwritten(self) -> None:
        kubectl = _Kubectl()
        control = _control(kubectl)
        control._pin_autoscalers(("space-api",))
        kubectl.hpas["space-api"]["metadata"]["uid"] = "someone-else"
        with self.assertRaisesRegex(RuntimeError, "replaced autoscaler"):
            control.restore()
        self.assertEqual(kubectl.patches, [("space-api", 2, 2)])


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

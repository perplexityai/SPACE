"""Contracts of the data-plane deletion-cost scenario the bench cannot exercise.

A passing bench shows the annotation go 0 → 10 → 0. It cannot show that a pod
reading the wrong bucket, a second busy pod, or no Ready pod at all fails the
scenario, that an absent annotation counts as idle, or that the harness read
is a read-only, context-scoped `kubectl get` that skips unready and
terminating pods.
"""

from __future__ import annotations

import json
import unittest
from unittest import mock

from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.data_plane_deletion_cost import DataPlaneDeletionCostAssertions, is_idle
from space_smoke.runtime import CommandResult, InvariantViolation

ANNOTATION = "controller.kubernetes.io/pod-deletion-cost"


class _Fixture:
    def __init__(self) -> None:
        self.records: dict[str, object] = {}
        self.waits = 0
        self.evidence: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **evidence: object) -> None:
        if not condition:
            self.evidence = evidence
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        self.waits += 1


class _Observer:
    def __init__(self, *observations: dict[str, str | None]) -> None:
        self.observations = list(observations)

    def data_plane_deletion_costs(self) -> dict[str, str | None]:
        if len(self.observations) > 1:
            return self.observations.pop(0)
        return self.observations[0]

    def data_plane_stream_loads(self) -> dict[str, int | None]:
        return {"dp-a": 3}


def _assertions(*observations: dict[str, str | None]) -> tuple[DataPlaneDeletionCostAssertions, _Fixture]:
    fixture = _Fixture()
    return DataPlaneDeletionCostAssertions(fixture, _Observer(*observations)), fixture  # type: ignore[arg-type]


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        self.now += 5.0
        return self.now


class Assertions(unittest.TestCase):
    def test_absent_and_zero_are_both_idle(self) -> None:
        self.assertTrue(is_idle(None))
        self.assertTrue(is_idle("0"))
        self.assertFalse(is_idle("10"))

    def test_idle_waits_for_a_busy_pod_to_settle(self) -> None:
        cost, fixture = _assertions({"dp-a": "100"}, {"dp-a": "10"}, {"dp-a": "0", "dp-b": None})
        cost.expect_all_idle(label="before_stream")
        self.assertEqual(fixture.waits, 2)
        self.assertEqual(fixture.records["before_stream_costs"], {"dp-a": "0", "dp-b": None})

    def test_idle_fails_when_a_pod_never_settles(self) -> None:
        cost, fixture = _assertions({"dp-a": "10"})
        with mock.patch("space_smoke.data_plane_deletion_cost.time.monotonic", _Clock()):
            with self.assertRaises(InvariantViolation):
                cost.expect_all_idle(label="after_stream", timeout=30)
        self.assertEqual(
            fixture.evidence.get("data_plane_active_streams"),
            {"dp-a": 3},
            "a failed settle reports what each pod really holds",
        )

    def test_no_ready_pod_fails_instead_of_passing_vacuously(self) -> None:
        cost, _ = _assertions({})
        with self.assertRaises(InvariantViolation):
            cost.expect_all_idle(label="before_stream")

    def test_one_stream_records_the_busy_pod(self) -> None:
        cost, fixture = _assertions({"dp-a": None, "dp-b": "0"}, {"dp-a": "10", "dp-b": "0"})
        cost.expect_one_stream()
        self.assertEqual(fixture.records["open_stream_pod"], "dp-a")

    def test_one_stream_rejects_a_wrong_bucket_or_a_second_busy_pod(self) -> None:
        for observed in ({"dp-a": "100"}, {"dp-a": "10", "dp-b": "10"}, {"dp-a": "10", "dp-b": "1000"}):
            cost, _ = _assertions(observed)
            with mock.patch("space_smoke.data_plane_deletion_cost.time.monotonic", _Clock()):
                with self.assertRaises(InvariantViolation, msg=str(observed)):
                    cost.expect_one_stream(timeout=30)


def _pod(
    name: str,
    *,
    ready: bool = True,
    terminating: bool = False,
    cost: str | None = None,
    internal_port: int | None = None,
) -> dict:
    metadata: dict[str, object] = {"name": name}
    if terminating:
        metadata["deletionTimestamp"] = "2026-10-04T00:00:00Z"
    if cost is not None:
        metadata["annotations"] = {ANNOTATION: cost}
    ports = [{"name": "http", "containerPort": 8090}]
    if internal_port is not None:
        ports.append({"name": "internal", "containerPort": internal_port})
    return {
        "metadata": metadata,
        "spec": {"containers": [{"name": "data-plane", "ports": ports}]},
        "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}]},
    }


class _ScriptedKubectl:
    def __init__(self, results: list[CommandResult]) -> None:
        self.results = results
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


class HarnessRead(unittest.TestCase):
    def test_reads_ready_live_data_plane_pods_with_a_scoped_get(self) -> None:
        pods = json.dumps({"items": [
            _pod("dp-busy", cost="10"),
            _pod("dp-unannotated"),
            _pod("dp-unready", ready=False, cost="1000"),
            _pod("dp-terminating", terminating=True, cost="1000"),
        ]})
        kubectl = _ScriptedKubectl([CommandResult(("kubectl",), 0, pods, "")])
        harness = KindHarness(kubectl, KindConfig())

        self.assertEqual(harness.data_plane_deletion_costs(), {"dp-busy": "10", "dp-unannotated": None})
        call = kubectl.calls[0]
        self.assertEqual(call[0], "kubectl")
        self.assertIn("--context", call)
        self.assertEqual(call[call.index("--context") + 1], KindConfig().context)
        self.assertIn("get", call)
        self.assertIn("app.kubernetes.io/name=space-data-plane", call)
        self.assertFalse({"patch", "annotate", "apply", "delete"} & set(call))

    def test_reads_stream_loads_through_the_pod_proxy_read_only(self) -> None:
        pods = json.dumps({"items": [
            _pod("dp-a", internal_port=8091),
            _pod("dp-unreachable", internal_port=8091),
            _pod("dp-no-listener"),
            _pod("dp-unready", ready=False, internal_port=8091),
        ]})
        kubectl = _ScriptedKubectl([
            CommandResult(("kubectl",), 0, pods, ""),
            CommandResult(("kubectl",), 0, json.dumps({"active_streams": 4}), ""),
            CommandResult(("kubectl",), 1, "", "error trying to reach service"),
        ])
        harness = KindHarness(kubectl, KindConfig())

        self.assertEqual(
            harness.data_plane_stream_loads(),
            {"dp-a": 4, "dp-unreachable": None, "dp-no-listener": None},
        )
        proxy_call = kubectl.calls[1]
        self.assertIn("--raw", proxy_call)
        self.assertEqual(
            proxy_call[proxy_call.index("--raw") + 1],
            "/api/v1/namespaces/space/pods/dp-a:8091/proxy/internal/v1/stream-load",
        )
        for call in kubectl.calls:
            self.assertEqual(call[call.index("--context") + 1], KindConfig().context)
            self.assertIn("get", call)
            self.assertFalse({"patch", "annotate", "apply", "delete", "exec"} & set(call))

    def test_a_failed_read_raises(self) -> None:
        harness = KindHarness(_ScriptedKubectl([CommandResult(("kubectl",), 1, "", "forbidden")]), KindConfig())
        with self.assertRaises(RuntimeError):
            harness.data_plane_deletion_costs()


if __name__ == "__main__":
    unittest.main()

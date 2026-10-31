"""What the leader-election scenario's bench runs only sometimes show.

kubelet can remove an exited container as soon as it starts the next one, so a
bench run only sometimes shows an exit through a container the runtime no
longer lists, and then kubelet reports no last termination for it either.
These pin that such an exit is still read, exactly, from the node's containerd
events, and that those events are parsed as containerd writes them. A request
can also stall right after an outage; these pin that cleanup's restore of the
budget is bounded, survives one stalled read, and looks nothing up.
"""

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import (
    CLEANUP_REQUEST_SECONDS,
    LEASE_TTL_ENV,
    LIVENESS_BUDGET_ENV,
    KindConfig,
    KindHarness,
    _LeaseOwnership,
    task_exit,
)
from space_smoke.capabilities import NodeContainer
from space_smoke.leader_election import LeaderElectionAssertions
from space_smoke.runtime import CommandResult, ResourceScope, SmokeConfigurationError

POD = "space-cluster-controller-abc12-xyz34"
NODE = "space-local-worker"
OWN = "a" * 64
EXEC = "e" * 64


def _container(container_id: str, *, running: bool, finished_at: float | None = None,
               exit_code: int | None = None) -> NodeContainer:
    return NodeContainer(
        container_id=container_id,
        attempt=0,
        running=running,
        started_at=10.0,
        finished_at=finished_at,
        exit_code=exit_code,
    )


class _Node:
    """Answers each read with the next listing."""

    def __init__(self, *listings: list[NodeContainer]) -> None:
        self._listings = list(listings)

    def node_controller_containers(self, node_name: str, pod_name: str) -> list[NodeContainer]:
        return self._listings.pop(0)


class _Exits:
    def __init__(self, exits: dict[str, tuple[int, float]]) -> None:
        self._exits = exits

    def exits(self) -> dict[str, tuple[int, float]]:
        return dict(self._exits)

    def diagnostics(self) -> str:
        return "running; stderr: <none>"

    def stop(self) -> None:
        pass


def _assertions(node: _Node, exits: dict[str, tuple[int, float]]) -> LeaderElectionAssertions:
    election = LeaderElectionAssertions(cast(Any, SimpleNamespace()), cast(Any, node), ResourceScope())
    election._nodes[POD] = NODE
    election._exit_watches[NODE] = _Exits(exits)
    return election


class ExitsTests(unittest.TestCase):
    def test_an_exit_the_runtime_lists_is_read_from_the_listing(self) -> None:
        exited = _container(OWN, running=False, finished_at=60.0, exit_code=1)
        election = _assertions(_Node([_container(OWN, running=True)], [exited]), {})
        election._containers(POD)

        self.assertEqual(election._exits(POD), [exited])

    def test_a_removed_container_is_read_from_its_exit_event(self) -> None:
        replacement = _container("b" * 64, running=True)
        election = _assertions(
            _Node([_container(OWN, running=True)], [replacement]), {OWN: (1, 60.5)}
        )
        election._containers(POD)

        [ended] = election._exits(POD)
        self.assertEqual(
            (ended.container_id, ended.running, ended.exit_code, ended.finished_at),
            (OWN, False, 1, 60.5),
        )

    def test_a_container_gone_without_an_exit_event_is_not_an_exit(self) -> None:
        election = _assertions(_Node([_container(OWN, running=True)], []), {})
        election._containers(POD)

        self.assertEqual(election._exits(POD), [])

    def test_an_exit_event_of_another_pods_container_is_not_this_pods(self) -> None:
        election = _assertions(
            _Node([_container(OWN, running=True)], [_container(OWN, running=True)]),
            {"c" * 64: (1, 60.5)},
        )
        election._containers(POD)

        self.assertEqual(election._exits(POD), [])


class TaskExitTests(unittest.TestCase):
    PREFIX = "2026-01-01 00:00:00.000000000 +0000 UTC k8s.io /tasks/exit "

    def test_a_containers_own_exit_carries_its_status_and_time(self) -> None:
        line = self.PREFIX + (
            f'{{"container_id":"{OWN}","id":"{OWN}","pid":7,"exit_status":1,'
            '"exited_at":{"seconds":1700000000,"nanos":500000000}}'
        )
        self.assertEqual(task_exit(line), (OWN, 1, 1700000000.5))

    def test_a_zero_status_is_left_out_of_the_event(self) -> None:
        line = self.PREFIX + (
            f'{{"container_id":"{OWN}","id":"{OWN}","pid":7,"exited_at":{{"seconds":1700000000}}}}'
        )
        self.assertEqual(task_exit(line), (OWN, 0, 1700000000.0))

    def test_an_exec_into_the_container_is_not_its_exit(self) -> None:
        line = self.PREFIX + (
            f'{{"container_id":"{OWN}","id":"{EXEC}","pid":8,"exit_status":1,'
            '"exited_at":{"seconds":1700000000}}'
        )
        self.assertIsNone(task_exit(line))

    def test_a_line_without_an_event_is_ignored(self) -> None:
        self.assertIsNone(task_exit("ctr: failed to dial\n"))


class _Executor:
    """Answers each request with the next result, recording its command and bound."""

    def __init__(self, *results: CommandResult) -> None:
        self._results = list(results)
        self.calls: list[tuple[tuple[str, ...], float | None]] = []

    def run(self, command: Any, *, timeout: float | None = None, **_options: Any) -> CommandResult:
        self.calls.append((tuple(command), timeout))
        return self._results.pop(0)


def _lease() -> str:
    now = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    return json.dumps(
        {
            "metadata": {"uid": "lease-uid", "resourceVersion": "7"},
            "spec": {"holderIdentity": "me", "leaseDurationSeconds": 3600, "renewTime": now},
        }
    )


class CleanupRestoreTests(unittest.TestCase):
    def test_the_budget_restore_survives_one_stalled_read_within_its_bounds(self) -> None:
        stalled = CommandResult(("kubectl",), 124, "", "command timed out")
        executor = _Executor(
            stalled,
            CommandResult(("kubectl",), 0, _lease(), ""),
            CommandResult(("kubectl",), 0, "deployment.apps/controller env updated", ""),
        )
        harness = KindHarness(cast(Any, executor), KindConfig())
        harness._retention_lease_ownership = _LeaseOwnership(
            name="space-smoke-retention-x", uid="lease-uid", holder="me",
            subject="retention controller mutation",
        )
        harness._liveness_budget_target = ("controller", "cluster-controller")

        harness.restore_controller_liveness_budget_env(None, wait=False)

        commands = [command for command, _ in executor.calls]
        # The stalled ownership read, its retry, and the write: no lookups.
        self.assertEqual(
            [command[5:7] for command in commands],
            [("get", "lease"), ("get", "lease"), ("set", "env")],
        )
        # One write puts the budget back and removes the TTL hook with it.
        self.assertIn(f"{LIVENESS_BUDGET_ENV}-", commands[-1])
        self.assertIn(f"{LEASE_TTL_ENV}-", commands[-1])
        self.assertTrue(
            all(bound is not None and bound <= CLEANUP_REQUEST_SECONDS for _, bound in executor.calls)
        )

    def test_the_budget_restore_writes_nothing_when_setup_never_wrote(self) -> None:
        # A preset TTL key refuses setup before its first write; the restore
        # cleanup armed before that call must then leave the Deployment alone.
        executor = _Executor()
        harness = KindHarness(cast(Any, executor), KindConfig())
        harness._retention_lease_ownership = _LeaseOwnership(
            name="space-smoke-retention-x", uid="lease-uid", holder="me",
            subject="retention controller mutation",
        )

        harness.restore_controller_liveness_budget_env(None, wait=False)

        self.assertEqual(executor.calls, [])

    def test_the_budget_restore_gives_up_after_its_second_try(self) -> None:
        stalled = CommandResult(("kubectl",), 124, "", "command timed out")
        executor = _Executor(stalled, stalled)
        harness = KindHarness(cast(Any, executor), KindConfig())
        harness._retention_lease_ownership = _LeaseOwnership(
            name="space-smoke-retention-x", uid="lease-uid", holder="me",
            subject="retention controller mutation",
        )
        harness._liveness_budget_target = ("controller", "cluster-controller")

        with self.assertRaises(SmokeConfigurationError):
            harness.restore_controller_liveness_budget_env(None, wait=False)
        self.assertEqual(len(executor.calls), 2)


if __name__ == "__main__":
    unittest.main()

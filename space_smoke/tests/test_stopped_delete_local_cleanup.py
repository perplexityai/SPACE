"""The stopped-delete cleanup check asks node-ctl for the evicting run, and still fails without it.

`delete_stopped_and_wait_for_local_cleanup` runs the worker's sandbox
desired-state facet on demand after the delete instead of waiting for
node-ctl's poll. These pin, off-bench, that the request is only an
accelerator: a request or run that fails falls back to the poll within the
same deadline, and a rootfs nothing removes still fails the check, naming the
directory.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import cast
from unittest import mock

from space_smoke.capabilities import (
    ColdEvictionControl,
    CommandSandbox,
    NodeCtlFacetControl,
    NodeCtlFacetRun,
)
from space_smoke.fixtures import (
    LOCAL_CLEANUP_DEADLINE_SECONDS,
    LOCAL_CLEANUP_FACET,
    CommandSandboxFixture,
)
from space_smoke.node_ctl_facets import FacetSync, sync_node_ctl_facet
from space_smoke.runtime import InvariantViolation, ResourceScope, ScenarioContext

SID = "00000000-0000-4000-8000-000000000001"
WORKER = "space-local-worker"
QUIESCENCE = 10


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def monotonic(self) -> float:
        return self.now


class _Context(ScenarioContext):
    def __init__(self, clock: _Clock) -> None:
        super().__init__(ResourceScope(), {})
        self._clock = clock

    def wait(self, seconds: float) -> None:
        if seconds <= 0:
            raise ValueError("wait duration must be positive")
        self._clock.now += seconds


@dataclass
class _Node:
    """One worker: its node-ctl facet status and the retained rootfs."""

    clock: _Clock
    # Unix second the poll timer next runs the facet; None never.
    poll_at: float | None = None
    # What a requested run does: evict the rootfs (True), leave it (False),
    # or fail; and how long it takes.
    run_evicts: list[bool] = field(default_factory=lambda: [True])
    run_fails: bool = False
    run_seconds: float = 3
    reconcile_error: Exception | None = None
    facet: str = LOCAL_CLEANUP_FACET
    present: bool = True
    reconciles: list[str] = field(default_factory=list)
    _started: int = 0
    _completed: int = 0
    _success: int = 0
    _ends_at: float | None = None
    _evicts: bool = False
    _fails: bool = False

    def _unix(self) -> int:
        return int(self.clock.now)

    def _advance(self) -> None:
        if self._ends_at is None and self.poll_at is not None and self.clock.now >= self.poll_at:
            self.poll_at = None
            self._begin(evicts=True, fails=False)
        if self._ends_at is not None and self.clock.now >= self._ends_at:
            self._ends_at = None
            self._completed = self._unix()
            if not self._fails:
                self._success = self._completed
                if self._evicts:
                    self.present = False

    def _begin(self, *, evicts: bool, fails: bool) -> None:
        self._started = self._unix()
        self._ends_at = self.clock.now + self.run_seconds
        self._evicts = evicts
        self._fails = fails

    # NodeCtlFacetControl
    def reconcile_node_ctl_facet(self, node_name: str, facet: str) -> None:
        self.reconciles.append(facet)
        if self.reconcile_error is not None:
            raise self.reconcile_error
        self._advance()
        evicts = facet == self.facet and (self.run_evicts.pop(0) if self.run_evicts else True)
        self._begin(evicts=evicts, fails=self.run_fails)

    def node_ctl_facet_run(self, node_name: str, facet: str) -> NodeCtlFacetRun:
        self._advance()
        return NodeCtlFacetRun(
            last_started_at=self._started,
            last_completed_at=self._completed,
            last_success_at=self._success,
            running=self._ends_at is not None,
            last_error="boom" if self._fails else "",
        )

    # ColdEvictionControl
    def cluster_deleted_quiescence_seconds(self, node_name: str) -> int:
        return QUIESCENCE

    def sandbox_pods(self, sandbox_id: str) -> list[object]:
        return []

    def substrate(self) -> str:
        return "kubernetes"

    def sandbox_cache_dir_present(self, node_name: str, sandbox_id: str) -> bool:
        self._advance()
        return self.present

    def sandbox_manager_log(self, sandbox_id: str, **_: object) -> str:
        return ""


@dataclass
class _Sandbox:
    sandbox_id: str = f"sbx_{SID}"


def _run(node: _Node, *, facets: bool = True) -> tuple[_Context, float]:
    """Run the check; return its context and the seconds it spent after the delete."""

    clock = node.clock
    context = _Context(clock)
    fixture = CommandSandboxFixture(
        context,
        cast(CommandSandbox, _Sandbox()),
        _local_cleanup_control=cast(ColdEvictionControl, node),
        _node_ctl_facet_control=cast(NodeCtlFacetControl, node) if facets else None,
    )
    deleted: list[float] = []
    with (
        mock.patch("space_smoke.fixtures.time.monotonic", clock.monotonic),
        mock.patch("space_smoke.node_ctl_facets.time.monotonic", clock.monotonic),
        mock.patch.object(CommandSandboxFixture, "delete_sandbox", lambda self: deleted.append(clock.now)),
        mock.patch.object(CommandSandboxFixture, "wait_until_deleted", lambda self: None),
    ):
        fixture.delete_stopped_and_wait_for_local_cleanup(WORKER)
    return context, clock.now - deleted[0]


def _poll_in(clock: _Clock, seconds: float) -> float:
    # The poll timer's next run, `seconds` after the delete: the delete comes
    # after the quiescence wait and its margin.
    return clock.now + QUIESCENCE + 2 + seconds


class StoppedDeleteLocalCleanup(unittest.TestCase):
    def test_the_requested_run_evicts_without_waiting_for_the_poll(self) -> None:
        clock = _Clock()
        node = _Node(clock, poll_at=_poll_in(clock, 50))

        context, after_delete = _run(node)

        self.assertEqual(node.reconciles, [LOCAL_CLEANUP_FACET])
        self.assertEqual(context.evidence["stopped_delete_eviction_runs"], 1)
        self.assertNotIn("stopped_delete_eviction_run_failure", context.evidence)
        self.assertLess(after_delete, 10)

    def test_a_run_that_left_the_rootfs_is_asked_for_once_more(self) -> None:
        clock = _Clock()
        node = _Node(clock, poll_at=_poll_in(clock, 50), run_evicts=[False, True])

        context, after_delete = _run(node)

        self.assertEqual(node.reconciles, [LOCAL_CLEANUP_FACET] * 2)
        self.assertEqual(context.evidence["stopped_delete_eviction_runs"], 2)
        self.assertLess(after_delete, 30)

    def test_a_failed_request_falls_back_to_the_poll(self) -> None:
        clock = _Clock()
        node = _Node(
            clock,
            poll_at=_poll_in(clock, 50),
            reconcile_error=RuntimeError("unknown facet"),
        )

        context, after_delete = _run(node)

        failure = context.evidence["stopped_delete_eviction_run_failure"]
        assert isinstance(failure, dict)
        self.assertIn("unknown facet", failure["error"])
        self.assertGreaterEqual(after_delete, 50)
        self.assertLess(after_delete, LOCAL_CLEANUP_DEADLINE_SECONDS)

    def test_a_failed_run_falls_back_to_the_poll(self) -> None:
        clock = _Clock()
        node = _Node(clock, poll_at=_poll_in(clock, 50), run_fails=True)

        context, after_delete = _run(node)

        self.assertIn("stopped_delete_eviction_run_failure", context.evidence)
        self.assertGreaterEqual(after_delete, 50)

    def test_without_node_commands_the_check_waits_for_the_poll(self) -> None:
        clock = _Clock()
        node = _Node(clock, poll_at=_poll_in(clock, 50))

        context, after_delete = _run(node, facets=False)

        self.assertEqual(node.reconciles, [])
        self.assertGreaterEqual(after_delete, 50)
        self.assertEqual(context.evidence["stopped_delete_local_cleanup"], "retained rootfs removed")

    def test_a_rootfs_already_gone_asks_for_no_run(self) -> None:
        clock = _Clock()
        # node-ctl's own poll evicts it the moment the delete lands.
        node = _Node(clock, run_seconds=0)
        node.poll_at = _poll_in(clock, 0)

        context, seconds = _run(node)

        self.assertEqual(node.reconciles, [])
        self.assertEqual(seconds, 0)
        self.assertNotIn("stopped_delete_eviction_runs", context.evidence)

    def test_a_rootfs_nothing_removes_fails_naming_the_directory(self) -> None:
        clock = _Clock()
        # Runs of the wrong facet, and no poll before the deadline.
        node = _Node(clock, facet="network-policy")

        with self.assertRaises(InvariantViolation) as raised:
            _run(node)

        self.assertIn(f"/space/cache/firecracker/sandboxes/{SID} is still on {WORKER}", str(raised.exception))
        self.assertEqual(node.reconciles, [LOCAL_CLEANUP_FACET] * 2)


class SyncNodeCtlFacet(unittest.TestCase):
    def _sync(self, node: _Node, *, timeout: float = 60) -> FacetSync:
        clock = node.clock

        def wait(seconds: float) -> None:
            clock.now += seconds

        with mock.patch("space_smoke.node_ctl_facets.time.monotonic", clock.monotonic):
            return sync_node_ctl_facet(node, WORKER, LOCAL_CLEANUP_FACET, timeout=timeout, wait=wait)

    def test_a_run_finished_before_the_read_does_not_count(self) -> None:
        clock = _Clock()
        node = _Node(clock)
        node.reconcile_node_ctl_facet(WORKER, LOCAL_CLEANUP_FACET)
        clock.now += 5
        node.reconciles.clear()

        sync = self._sync(node)

        self.assertEqual(node.reconciles, [LOCAL_CLEANUP_FACET])
        self.assertGreater(sync.latest.last_started_at, sync.before.last_started_at)
        self.assertTrue(sync.succeeded)

    def test_no_run_in_time_reports_none(self) -> None:
        clock = _Clock()
        node = _Node(clock, run_seconds=120)

        sync = self._sync(node, timeout=20)

        self.assertIsNone(sync.completed_at)
        self.assertFalse(sync.succeeded)

    def test_a_run_that_failed_in_the_success_second_is_not_succeeded(self) -> None:
        failed = NodeCtlFacetRun(
            last_started_at=10, last_completed_at=12, last_success_at=12, running=False, last_error="boom"
        )

        self.assertFalse(FacetSync(failed, failed, 1.0).succeeded)

    def test_the_timeout_counts_from_the_call(self) -> None:
        clock = _Clock()
        node = _Node(clock, run_seconds=120)
        reads: list[float] = []

        class _Slow:
            # The first status read and the request each take 30 s, as a
            # slow exec can on Kind; later reads are quick.
            def node_ctl_facet_run(self, node_name: str, facet: str) -> NodeCtlFacetRun:
                if not reads:
                    clock.now += 30
                reads.append(clock.now)
                return node.node_ctl_facet_run(node_name, facet)

            def reconcile_node_ctl_facet(self, node_name: str, facet: str) -> None:
                clock.now += 30
                node.reconcile_node_ctl_facet(node_name, facet)

        start = clock.now

        def wait(seconds: float) -> None:
            clock.now += seconds

        with mock.patch("space_smoke.node_ctl_facets.time.monotonic", clock.monotonic):
            sync = sync_node_ctl_facet(
                cast(NodeCtlFacetControl, _Slow()), WORKER, LOCAL_CLEANUP_FACET, timeout=20, wait=wait
            )

        self.assertIsNone(sync.completed_at)
        # The read before the request and one read after it: the read and
        # the request alone outlasted the timeout, so nothing polls past it.
        self.assertEqual(len(reads), 2)
        self.assertLess(clock.now - start, 2 * 30 + 2)

    def test_a_failed_run_is_finished_but_not_succeeded(self) -> None:
        clock = _Clock()
        node = _Node(clock, run_fails=True)

        sync = self._sync(node)

        self.assertIsNotNone(sync.completed_at)
        self.assertFalse(sync.succeeded)


if __name__ == "__main__":
    unittest.main()

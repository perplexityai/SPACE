"""Contracts of the dead-node dry-run scenario the bench cannot exercise.

A passing bench run stops a worker, judges it and starts it again. It cannot
show that a stop or a wait that fails part-way still leaves the worker with
every label it had, or that an entry written before the latest config is not
taken as that config's verdict.
"""

from __future__ import annotations

import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.capabilities import AdminNodeView, DeadNodeDryRun
from space_smoke.dead_node import DeadNodeAssertions
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope

WORKER = "space-local-worker2"
LABELS = {"kubernetes.io/hostname": WORKER, "space.synthetic/pool": "sandbox"}
CONFIGURED_AT = datetime(2026, 1, 1, tzinfo=UTC)


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id="01a0b0c9-b8a3-7473-9c12-3821a701bc28")
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        pass

    @contextmanager
    def phase(self, _title: str) -> Iterator[None]:
        yield


class _Control:
    def __init__(self) -> None:
        self.started_with: list[dict[str, str]] = []
        self.entries: list[DeadNodeDryRun] = []

    def other_live_rows_on_node(self, node_id, sandbox_ids):
        return []

    def kind_node_labels(self, node_name):
        return dict(LABELS)

    def stop_kind_worker(self, node_name):
        # The container stopped and the Node is deleted, then the call fails.
        raise RuntimeError("synthetic failure after the Node delete")

    def start_kind_worker(self, node_name, labels, *, timeout):
        self.started_with.append(dict(labels))

    def sandbox_row_states(self, sandbox_ids):
        return []

    def acquire_controller_mutation(self):
        return lambda: None

    def dead_node_env(self):
        return {}

    def restore_dead_node_env(self, standing):
        pass

    def set_dead_node_env(self, overrides):
        pass

    def database_now(self):
        return CONFIGURED_AT

    def dead_node_dry_runs(self):
        return list(self.entries)


def _assertions(control: _Control) -> tuple[DeadNodeAssertions, ResourceScope]:
    scope = ResourceScope()
    dead = DeadNodeAssertions(cast(Any, _Fixture()), cast(Any, control), scope)
    dead._node = AdminNodeView("n1", WORKER, None, None, 1)
    return dead, scope


class KillWorkerKeepsTheLabels(unittest.TestCase):
    def test_a_stop_that_fails_after_the_delete_restarts_with_every_label(self) -> None:
        control = _Control()
        dead, scope = _assertions(control)
        with self.assertRaisesRegex(RuntimeError, "after the Node delete"):
            dead.kill_worker()
        self.assertEqual(scope.close(), ())
        self.assertEqual(control.started_with, [LABELS])


class _Executor:
    """Kind's commands, with the re-registered Node missing the bring-up's
    label and its Ready wait failing."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        if argv[:2] == ("docker", "start"):
            return CommandResult(argv, 0, WORKER, "")
        verb = argv[5:]
        if verb[:2] == ("get", "node") and "name" in verb:
            return CommandResult(argv, 0, f"node/{WORKER}", "")
        if verb[:2] == ("get", "node"):
            return CommandResult(argv, 0, '{"kubernetes.io/hostname": "space-local-worker2"}', "")
        if verb[:1] == ("wait",):
            return CommandResult(argv, 1, "", "timed out waiting for the condition")
        return CommandResult(argv, 0, "", "")


class StartWorkerRestoresTheLabelsFirst(unittest.TestCase):
    def test_a_ready_wait_that_fails_still_leaves_the_labels_restored(self) -> None:
        executor = _Executor()
        harness = KindHarness(cast(Any, executor), KindConfig())
        with self.assertRaisesRegex(RuntimeError, "Ready"):
            harness.start_kind_worker(WORKER, LABELS, timeout=30)
        labelled = [argv[5:] for argv in executor.calls if argv[5:6] == ("label",)]
        self.assertEqual(labelled, [("label", "node", WORKER, "space.synthetic/pool=sandbox")])


def _entry(observed_at: datetime) -> DeadNodeDryRun:
    return DeadNodeDryRun("n1", observed_at, "lost", "node_deleted", False, {}, {})


class OnlyAnEntryWrittenUnderTheConfigCounts(unittest.TestCase):
    def _expect(self, observed_at: datetime) -> None:
        control = _Control()
        control.entries = [_entry(observed_at)]
        dead, _ = _assertions(control)
        with dead.scoped_dry_run() as configure:
            configure({"SPACE_DEAD_NODE_MODE": "dry-run"})
            dead.expect_dry_run(reason="node_deleted", timeout=0)

    def test_an_entry_from_before_the_config_is_not_its_verdict(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "under the latest config"):
            self._expect(CONFIGURED_AT - timedelta(seconds=1))
        with self.assertRaisesRegex(InvariantViolation, "under the latest config"):
            self._expect(CONFIGURED_AT)

    def test_an_entry_written_after_the_config_is(self) -> None:
        self._expect(CONFIGURED_AT + timedelta(seconds=1))


if __name__ == "__main__":
    unittest.main()

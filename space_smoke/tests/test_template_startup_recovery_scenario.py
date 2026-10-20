"""Contracts of the startup-recovery scenario the bench cannot exercise.

The live run proves the decision table. These pin what a passing run would not
catch: each branch, and every state no branch leaves, mapping to its own
outcome from what a plant held at restart; outcomes read only once the stale
row is pruned, and never when that row was gone before the restart; the
shell-word guards on the names and digests that reach an on-node `bash -c`;
and a bench with a single sandbox worker being a configuration error rather
than a restart of the worker running the fixture sandbox.
"""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import (
    KindConfig,
    KindHarness,
    _ready_pod_names,
    _smoke_template_name,
    _template_digest,
)
from space_smoke.fixtures import (
    RecoveredBuild,
    StartupRecoveryAssertions,
    recovery_outcome,
)
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    SmokeConfigurationError,
)

H1 = "sha256:" + "1" * 64
H2 = "sha256:" + "2" * 64

ROW_AND_DIR = (True, True)
DIR_ONLY = (False, True)
ROW_ONLY = (True, False)


class RecoveryOutcomeFromState(unittest.TestCase):
    def test_each_branch_and_each_impossible_state_has_its_own_outcome(self) -> None:
        cases = [
            (ROW_AND_DIR, (H1,), "ready", "confirmed"),
            (ROW_AND_DIR, (), None, "rejected"),
            (ROW_AND_DIR, (H1,), "deleting", "inconsistent"),
            (ROW_AND_DIR, (), "ready", "inconsistent"),
            (ROW_AND_DIR, (H1,), None, "inconsistent"),
            (DIR_ONLY, (H1,), "ready", "registered"),
            (DIR_ONLY, (), None, "rejected"),
            (DIR_ONLY, (H1,), None, "untouched"),
            (ROW_ONLY, (), None, "pruned"),
            (ROW_ONLY, (), "ready", "untouched"),
            (ROW_ONLY, (H1,), "ready", "inconsistent"),
            # A directory under any other digest is never this plant's outcome.
            (DIR_ONLY, (H2,), "ready", "inconsistent"),
            (ROW_AND_DIR, (H1, H2), "ready", "inconsistent"),
        ]
        for held, dirs, state, expected in cases:
            with self.subTest(held=held, dirs=dirs, state=state):
                self.assertEqual(recovery_outcome(held, H1, RecoveredBuild(dirs, state)), expected)


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id="01a0ac88-c99e-735a-b8fa-f13c948332fa")
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, key: str, value: object) -> None:
        self.records[key] = value

    def wait(self, _seconds: float) -> None:
        pass


class _Worker:
    """One worker's plants on disk and in the catalog, and a recovery sweep.

    The restarted daemon's sweep lands its whole verdict at once, after
    `sweep_after_reads` reads following the restart: before that every plant
    still holds what it held at the restart.
    """

    def __init__(self, *, sweep_after_reads: int) -> None:
        self.dirs: dict[str, tuple[str, ...]] = {}
        self.rows: dict[str, str] = {}
        self.stale: set[str] = set()
        self.tampered: set[str] = set()
        self.restarted = False
        self.swept = False
        self.sweep_after_reads = sweep_after_reads
        self.reads_after_restart = 0

    def sandbox_pod_placement(self, _sandbox_id: str):
        return SimpleNamespace(node_name="worker-a")

    def other_sandbox_worker_node(self, _excluding: str) -> str:
        return "worker-b"

    def register_unpinned_template(self, _node: str, name: str, _size: int) -> None:
        self.dirs[name] = (H1,)
        self.rows[name] = "ready"

    def template_digest_dirs(self, _node: str, name: str) -> tuple[str, ...]:
        return self.dirs.get(name, ())

    def template_build_state(self, _node: str, name: str, _digest: str) -> str | None:
        if self.restarted and not self.swept:
            self.reads_after_restart += 1
            if self.reads_after_restart > self.sweep_after_reads:
                self._sweep()
        return self.rows.get(name)

    def _sweep(self) -> None:
        self.swept = True
        for name in self.tampered:
            self.dirs[name] = ()
            self.rows.pop(name, None)
        for name in self.stale:
            self.rows.pop(name, None)

    def remove_template_digest_dir(self, _node: str, name: str, _digest: str) -> None:
        self.dirs[name] = ()
        self.stale.add(name)

    def orphan_template_digest_dir(self, _node: str, name: str, _digest: str) -> None:
        self.rows.pop(name, None)

    def corrupt_template_rootfs(self, _node: str, name: str, _digest: str) -> None:
        self.tampered.add(name)

    def restart_node_service(self, _node: str, _component: str, *, timeout: float) -> str:
        self.restarted = True
        return "template-manager-new"

    def startup_recovery_log(self, _node: str) -> list[str]:
        return []


def _recovery(worker: _Worker) -> StartupRecoveryAssertions:
    assertions = StartupRecoveryAssertions(cast(Any, _Fixture()), cast(Any, worker), ResourceScope())
    assertions.spare_worker()
    return assertions


class RecoveryWait(unittest.TestCase):
    def test_outcomes_reflect_the_finished_sweep(self) -> None:
        # The sweep lands only after a few polls; a plant read before it would
        # still look untouched, so the tamper plant would not read as rejected.
        worker = _Worker(sweep_after_reads=3)
        recovery = _recovery(worker)
        happy = recovery.plant("happy")
        stale = recovery.plant("stale")
        recovery.make_row_stale(stale)
        tamper = recovery.plant("tamper")
        recovery.tamper(tamper)
        recovery.restart_and_wait_for_recovery()
        recovery.expect_confirmed(happy)
        recovery.expect_pruned(stale)
        recovery.expect_rejected(tamper)
        with self.assertRaises(InvariantViolation):
            recovery.expect_rejected(happy)

    def test_a_row_never_pruned_fails_closed(self) -> None:
        worker = _Worker(sweep_after_reads=10**6)
        recovery = _recovery(worker)
        recovery.make_row_stale(recovery.plant("stale"))
        with self.assertRaises(InvariantViolation):
            recovery.restart_and_wait_for_recovery(recovery_timeout=0)

    def test_a_stale_row_gone_before_the_restart_fails_closed(self) -> None:
        # An eviction that dropped the row first would otherwise read as the prune.
        worker = _Worker(sweep_after_reads=0)
        recovery = _recovery(worker)
        stale = recovery.plant("stale")
        recovery.make_row_stale(stale)
        worker.rows.pop(stale.name)
        with self.assertRaises(InvariantViolation):
            recovery.restart_and_wait_for_recovery()
        self.assertFalse(worker.restarted)

    def test_restarting_without_a_stale_plant_is_a_configuration_error(self) -> None:
        worker = _Worker(sweep_after_reads=0)
        recovery = _recovery(worker)
        recovery.plant("happy")
        with self.assertRaises(SmokeConfigurationError):
            recovery.restart_and_wait_for_recovery()
        self.assertFalse(worker.restarted)


class ShellWordGuards(unittest.TestCase):
    def test_only_the_scenarios_own_names_and_real_digests_pass(self) -> None:
        self.assertEqual(_smoke_template_name("smoke-rec-0123abcd-tamper"), "smoke-rec-0123abcd-tamper")
        self.assertEqual(_smoke_template_name("smoke-lru-0123abcd"), "smoke-lru-0123abcd")
        for bad in ("seed", "smoke-rec-0123abcd-evil", "smoke-rec-0123abcd-happy; rm -rf /", "smoke-rec-XYZ-happy"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                _smoke_template_name(bad)
        self.assertEqual(_template_digest(H1), H1)
        for bad in ("sha256:abc", H1 + "/..", "sha1:" + "1" * 40, "$(id)"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                _template_digest(bad)


class ReplacementPodSelection(unittest.TestCase):
    def test_only_a_ready_pod_other_than_the_deleted_one_counts(self) -> None:
        def pod(name: str, ready: str | None) -> dict:
            conditions = [] if ready is None else [{"type": "Ready", "status": ready}]
            return {"metadata": {"name": name}, "status": {"conditions": conditions}}

        listing = json.dumps(
            {"items": [pod("old", "True"), pod("new", "False"), pod("newer", "True"), pod("bare", None)]}
        )
        self.assertEqual(_ready_pod_names(listing, excluding="old"), ["newer"])
        # Garbage from kubectl is "nothing ready", never a crash or a match.
        self.assertEqual(_ready_pod_names("not json", excluding="old"), [])


class _NodesKubectl:
    def __init__(self, nodes: str) -> None:
        self.nodes = nodes

    def run(self, command, **_options) -> CommandResult:
        return CommandResult(tuple(command), 0, self.nodes, "")


class SpareWorkerSelection(unittest.TestCase):
    def test_single_sandbox_worker_is_a_configuration_error(self) -> None:
        harness = KindHarness(_NodesKubectl("space-local-worker"), KindConfig())
        with self.assertRaises(SmokeConfigurationError):
            harness.other_sandbox_worker_node("space-local-worker")

    def test_picks_a_different_sandbox_worker(self) -> None:
        harness = KindHarness(_NodesKubectl("space-local-worker2 space-local-worker"), KindConfig())
        self.assertEqual(harness.other_sandbox_worker_node("space-local-worker"), "space-local-worker2")


if __name__ == "__main__":
    unittest.main()

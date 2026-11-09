"""Contracts of the retired-execution-pod GC scenario the bench cannot show.

A passing bench run shows retired pods kept in dry-run and reaped live, and a
pod with an IP and a live sandbox left alone. It cannot show that the env
override refuses keys and values off its closed lists, that the classifier
mirror the dry-run count rests on agrees with the reaper's rule on the cases
the bench doesn't produce, that a plant can't be scheduled, or that the
verdicts fail on an early reap or a dry-run count off by one. Those are
pinned here.
"""

from __future__ import annotations

import dataclasses
import itertools
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke import retired_execution_pod_gc as gc
from space_smoke.adapters import _sandbox_pod_execution
from space_smoke.capabilities import SandboxPodExecution
from space_smoke.runtime import InvariantViolation

SANDBOX = "3f0b6c1e-8f5d-4d7e-9a61-0c2b8e4f7a10"
NAME = f"space-sbx-{SANDBOX.replace('-', '')}-g1"


def _pod(**overrides: Any) -> SandboxPodExecution:
    base = SandboxPodExecution(
        pod_name=NAME,
        uid="uid-1",
        sandbox_id=SANDBOX,
        generation=1,
        phase="Pending",
        all_containers_terminated=False,
        pod_ip="",
        sandbox_not_ready=True,
        is_restore=False,
        deleting=False,
        row_sandbox_id=SANDBOX,
        row_generation=1,
        row_state="replaced",
        row_active=False,
    )
    return dataclasses.replace(base, **overrides)


class ClassifierMirrorTest(unittest.TestCase):
    def test_reapable_shapes(self) -> None:
        self.assertEqual(gc.would_reap(_pod()), gc.REPLACED_NEVER_STARTED)
        terminal = _pod(phase="Succeeded", all_containers_terminated=True, row_state="suspended")
        self.assertEqual(gc.would_reap(terminal), gc.TERMINAL_RETIRED)

    def test_kept_shapes(self) -> None:
        kept = {
            "pod ip": _pod(pod_ip="192.0.2.10"),
            "no not-ready condition": _pod(sandbox_not_ready=False),
            "restore pod": _pod(is_restore=True),
            "retired otherwise": _pod(row_state="suspended"),
            "active row": _pod(row_active=True),
            "no row": _pod(row_state=None, row_active=None, row_sandbox_id=None, row_generation=None),
            "other generation": _pod(row_generation=2),
            "deleting": _pod(deleting=True),
            "terminal with a running container": _pod(phase="Failed", row_state="failed"),
        }
        for case, pod in kept.items():
            with self.subTest(case):
                self.assertIsNone(gc.would_reap(pod))

    def test_adapter_reads_the_reaper_fields(self) -> None:
        pod = {
            "metadata": {
                "name": NAME,
                "uid": "uid-1",
                "annotations": {gc.SANDBOX_ID_KEY: SANDBOX, gc.EXECUTION_GENERATION_KEY: "1"},
            },
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{"state": {"terminated": {"exitCode": 0}}}],
                "conditions": [{"type": "PodReadyToStartContainers", "status": "False"}],
            },
        }
        view = _sandbox_pod_execution(pod, {NAME: (SANDBOX, 1, "suspended", False)})
        self.assertEqual(gc.would_reap(view), gc.TERMINAL_RETIRED)
        self.assertIsNone(gc.would_reap(_sandbox_pod_execution(pod, {})))


class EnvAndPlantTest(unittest.TestCase):
    def test_overrides_are_closed(self) -> None:
        gc.validate_gc_overrides(gc.smoke_gc_env(dry_run=True, threshold_seconds=20, interval_seconds=5))
        with self.assertRaises(ValueError):
            gc.validate_gc_overrides({"SPACE_ORPHANED_RESTORE_POD_GC_ENABLED": "true"})
        with self.assertRaises(ValueError):
            gc.validate_gc_overrides({gc.GC_THRESHOLD_ENV: "0"})
        with self.assertRaises(ValueError):
            gc.validate_gc_overrides({gc.GC_DRY_RUN_ENV: "yes"})

    def test_plant_cannot_schedule_and_drops_the_source_identity(self) -> None:
        template = {
            "metadata": {"name": "src", "uid": "src-uid", "labels": {gc.SANDBOX_ID_KEY: "other"}},
            "spec": {"nodeName": "worker", "nodeSelector": {"pool": "sandbox"}},
            "status": {"podIP": "10.0.0.1"},
        }
        plant = gc.build_planted_pod(template, name=NAME, sandbox_id=SANDBOX, generation=1, token="abcd1234")
        self.assertNotIn("nodeName", plant["spec"])
        self.assertNotIn("status", plant)
        self.assertNotIn("uid", plant["metadata"])
        self.assertEqual(plant["spec"]["nodeSelector"][gc.UNSCHEDULABLE_SELECTOR_KEY], "abcd1234")
        self.assertEqual(plant["metadata"]["labels"][gc.SANDBOX_ID_KEY], SANDBOX)
        self.assertEqual(plant["metadata"]["annotations"][gc.EXECUTION_GENERATION_KEY], "1")
        with self.assertRaises(ValueError):
            gc.build_planted_pod(template, name=NAME, sandbox_id=SANDBOX, generation=1, token="nope")


class _Fixture:
    def __init__(self) -> None:
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, seconds: float, **_: object) -> None:
        pass


def _assertions(control: Any, settings: gc.GcSettings) -> gc.RetiredExecutionPodGcAssertions:
    assertions = gc.RetiredExecutionPodGcAssertions(
        cast(Any, _Fixture()), control, cast(Any, SimpleNamespace()), lambda: None
    )
    assertions._settings = settings
    return assertions


class VerdictTest(unittest.TestCase):
    TARGET = gc.RetiredPod(NAME, "uid-1", SANDBOX, 1, gc.REPLACED_NEVER_STARTED)

    def _dry_run(self, counts: dict[str, int]) -> None:
        control = mock.Mock()
        control.sandbox_pod_executions.return_value = [_pod()]
        control.restore_pod_document.return_value = {"metadata": {"uid": "uid-1"}}
        control.retired_execution_pod_gc_counts.return_value = counts
        settings = gc.GcSettings(True, 20, 5, started_at=0.0, started_epoch=0)
        with mock.patch.object(gc.GcSettings, "survival_window_seconds", 0):
            _assertions(control, settings).expect_dry_run_verdicts(self.TARGET)

    def test_dry_run_count_must_match_exactly(self) -> None:
        self._dry_run({"dry_run/replaced_never_started": 1})
        wrong = (
            {},
            {"dry_run/replaced_never_started": 2},
            {"dry_run/replaced_never_started": 1, "live/replaced_never_started": 1},
        )
        for counts in wrong:
            with self.subTest(counts=counts), mock.patch.object(gc.time, "monotonic", side_effect=itertools.count(0, 100)):
                with self.assertRaises(InvariantViolation):
                    self._dry_run(counts)

    def test_reap_before_the_threshold_fails(self) -> None:
        control = mock.Mock()
        control.restore_pod_document.return_value = None
        settings = gc.GcSettings(False, 20, 5, started_at=100.0, started_epoch=0)
        with mock.patch.object(gc.time, "monotonic", return_value=101.0):
            with self.assertRaises(InvariantViolation):
                _assertions(control, settings).expect_reaped(self.TARGET)
        presence = iter([{"metadata": {"uid": "uid-1"}}, None])
        control.restore_pod_document.side_effect = lambda _name: next(presence)
        with mock.patch.object(gc.time, "monotonic", side_effect=itertools.count(10, 10)):
            settings = gc.GcSettings(False, 20, 5, started_at=0.0, started_epoch=0)
            self.assertEqual(_assertions(control, settings).expect_reaped(self.TARGET), {NAME: 40.0})


if __name__ == "__main__":
    unittest.main()

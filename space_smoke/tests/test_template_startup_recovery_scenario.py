"""Contracts of the startup-recovery scenario the bench cannot exercise.

The live run proves the decision table. These pin what a passing run would not
catch: the verdict scan joining each line to exactly one `(template, digest)`
across the two tracing layouts; the shell-word guards on the names and digests
that reach an on-node `bash -c`; and a bench with a single sandbox worker being
a configuration error rather than a restart of the worker running the fixture
sandbox.
"""

from __future__ import annotations

import json
import unittest

from space_smoke.adapters import (
    TEMPLATE_RECOVERY_CONFIRMED_MESSAGE,
    TEMPLATE_RECOVERY_FINISHED_MESSAGE,
    TEMPLATE_RECOVERY_PRUNED_MESSAGE,
    TEMPLATE_RECOVERY_REGISTERED_MESSAGE,
    TEMPLATE_RECOVERY_REJECTED_MESSAGE,
    KindConfig,
    KindHarness,
    _parse_startup_recovery,
    _ready_pod_names,
    _smoke_template_name,
    _template_digest,
)
from space_smoke.runtime import CommandResult, SmokeConfigurationError

H1 = "sha256:" + "1" * 64
H2 = "sha256:" + "2" * 64


class RecoveryVerdictScan(unittest.TestCase):
    def test_joins_each_line_to_one_build_across_layouts(self) -> None:
        log = "\n".join(
            [
                json.dumps({"message": "template-manager startup recovery started"}),
                json.dumps({"message": TEMPLATE_RECOVERY_CONFIRMED_MESSAGE, "template": "smoke-rec-abcdef01-happy", "content_hash": H1}),
                # Nested tracing layout.
                json.dumps({"fields": {"message": TEMPLATE_RECOVERY_REGISTERED_MESSAGE, "template": "smoke-rec-abcdef01-orphan", "content_hash": H2}}),
                json.dumps({"message": TEMPLATE_RECOVERY_REJECTED_MESSAGE, "template": "smoke-rec-abcdef01-tamper", "content_hash": H1, "reason": 'bootable artifact sha256 "sha256:ff" does not match digest dir name "' + H1 + '"'}),
                json.dumps({"level": "WARN", "fields": {"message": TEMPLATE_RECOVERY_PRUNED_MESSAGE, "template": "smoke-rec-abcdef01-stale", "content_hash": H2}}),
                # Same message for another template must not attach to ours.
                json.dumps({"message": TEMPLATE_RECOVERY_CONFIRMED_MESSAGE, "template": "seed", "content_hash": H1}),
                "not json startup recovery noise",
                json.dumps({"message": TEMPLATE_RECOVERY_FINISHED_MESSAGE}),
            ]
        )
        report = _parse_startup_recovery("worker-2", "pod-x", log)
        self.assertTrue(report.finished)
        self.assertEqual(
            [(v.template.rsplit("-", 1)[-1], v.outcome) for v in report.verdicts if v.template.startswith("smoke-")],
            [("happy", "confirmed"), ("orphan", "registered"), ("tamper", "rejected"), ("stale", "pruned")],
        )
        (tamper,) = report.for_build("smoke-rec-abcdef01-tamper", H1)
        self.assertIn("does not match digest dir name", tamper.reason)
        self.assertEqual(report.for_build("smoke-rec-abcdef01-happy", H2), ())

    def test_unfinished_log_reports_not_finished(self) -> None:
        report = _parse_startup_recovery("n", "p", json.dumps({"message": "template-manager startup recovery started"}))
        self.assertFalse(report.finished)
        self.assertEqual(report.verdicts, ())


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

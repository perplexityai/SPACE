"""Tests for versioned suite manifests and repeated suite runs."""

from __future__ import annotations

import dataclasses
import hashlib
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from unittest import mock

from space_smoke.__main__ import _run_all
from space_smoke.registry import ScenarioRegistry
from space_smoke.scenarios import REGISTRY
from space_smoke.suite import ScenarioProcessResult
from space_smoke.suites import SUITES, Suite, resolve_suite

# The digest of each released version's scenario list. A released version
# never changes: a different list is a new version with its own entry here.
RELEASED = {
    "lifecycle-faults@1": "10f0f0b67328f0a2ec3860aeddb09badc24635fed8c7265cba5e2b1eb0407bb4",
}

ENVIRONMENT = {"SPACE_SMOKE_BACKEND": "kind", "SPACE_SMOKE_API_SURFACE": "native"}


def _digest(suite: Suite) -> str:
    return hashlib.sha256("\n".join(suite.scenarios).encode()).hexdigest()


def _services() -> mock.Mock:
    services = mock.Mock()
    services.telemetry_mode.return_value = "otel"
    services.storage_credentials.return_value = "exchange"
    services.substrate.return_value = "kubernetes"
    services.browser_unavailable.return_value = None
    return services


class SuiteManifestTests(unittest.TestCase):
    def test_released_versions_are_unchanged_registered_and_unique(self) -> None:
        registered = {scenario.id for scenario in REGISTRY}
        self.assertEqual({suite.label for suite in SUITES}, set(RELEASED))
        for suite in SUITES:
            with self.subTest(suite.label):
                self.assertEqual(_digest(suite), RELEASED[suite.label])
                self.assertEqual(len(set(suite.scenarios)), len(suite.scenarios))
                self.assertLessEqual(set(suite.scenarios), registered)

    def test_resolves_the_latest_or_a_named_version(self) -> None:
        self.assertEqual(resolve_suite("lifecycle-faults"), SUITES[0])
        self.assertEqual(resolve_suite("lifecycle-faults@1"), SUITES[0])
        with self.assertRaisesRegex(ValueError, "no version"):
            resolve_suite("lifecycle-faults@99")

    def test_repeated_suite_runs_every_pass_and_reports_failures_per_thousand(self) -> None:
        suite = resolve_suite("lifecycle-faults")
        selected = tuple(scenario for scenario in REGISTRY if scenario.id in suite.scenarios)
        flaky = selected[0].id
        passes = (
            tuple(ScenarioProcessResult(scenario.id, 0, "", "") for scenario in selected),
            tuple(
                ScenarioProcessResult(scenario.id, 1 if scenario.id == flaky else 0, "", "")
                for scenario in selected
            ),
        )
        stdout = StringIO()
        with (
            mock.patch.dict("space_smoke.__main__.os.environ", ENVIRONMENT, clear=True),
            mock.patch(
                "space_smoke.__main__.SmokeServices.from_environment",
                return_value=_services(),
            ),
            mock.patch("space_smoke.__main__.run_scenarios", side_effect=passes) as run,
            redirect_stdout(stdout),
        ):
            exit_code = _run_all(suite=suite, repeat=2)

        self.assertEqual(exit_code, 1)
        self.assertEqual(run.call_count, 2)
        self.assertEqual({call.args[0] for call in run.call_args_list}, {selected})
        runs = 2 * len(selected)
        self.assertIn(
            f"FAILURE RATE lifecycle-faults@1: 1 of {runs} scenario runs failed "
            f"or left cleanup failures ({1000 / runs:.1f} per 1,000)",
            stdout.getvalue(),
        )
        self.assertIn(f"  {flaky}: 1/2", stdout.getvalue())

    def test_a_passing_run_that_left_cleanup_failures_counts_against_the_rate(self) -> None:
        suite = resolve_suite("lifecycle-faults")
        selected = tuple(scenario for scenario in REGISTRY if scenario.id in suite.scenarios)
        leaked = {"verdict": "pass", "summary": "ok", "cleanup_failures": ["sandbox: still present"]}
        results = (
            ScenarioProcessResult(selected[0].id, 0, "", "", leaked),
            *(ScenarioProcessResult(scenario.id, 0, "", "") for scenario in selected[1:]),
        )
        stdout = StringIO()
        with (
            mock.patch.dict("space_smoke.__main__.os.environ", ENVIRONMENT, clear=True),
            mock.patch(
                "space_smoke.__main__.SmokeServices.from_environment",
                return_value=_services(),
            ),
            mock.patch("space_smoke.__main__.run_scenarios", return_value=results),
            redirect_stdout(stdout),
        ):
            exit_code = _run_all(suite=suite)

        self.assertEqual(exit_code, 0)
        self.assertIn(f"1 of {len(selected)} scenario runs failed", stdout.getvalue())

    def test_a_suite_with_a_withheld_scenario_refuses_to_start(self) -> None:
        suite = resolve_suite("lifecycle-faults")
        held = dataclasses.replace(REGISTRY.get(suite.scenarios[0]), quarantined="tracked in #0000")
        registry = ScenarioRegistry(
            held if scenario.id == held.id else scenario for scenario in REGISTRY
        )
        stderr = StringIO()
        with (
            mock.patch.dict("space_smoke.__main__.os.environ", ENVIRONMENT, clear=True),
            mock.patch("space_smoke.__main__.REGISTRY", registry),
            mock.patch("space_smoke.__main__.SmokeServices.from_environment") as from_environment,
            redirect_stdout(StringIO()),
            redirect_stderr(stderr),
        ):
            exit_code = _run_all(suite=suite)

        self.assertEqual(exit_code, 2)
        self.assertIn(f"suite lifecycle-faults@1 runs only whole; this run withholds {held.id}", stderr.getvalue())
        from_environment.assert_not_called()

    def test_a_suite_on_another_api_surface_refuses_to_start(self) -> None:
        stderr = StringIO()
        with (
            mock.patch.dict(
                "space_smoke.__main__.os.environ",
                {**ENVIRONMENT, "SPACE_SMOKE_API_SURFACE": "ecw"},
                clear=True,
            ),
            mock.patch("space_smoke.__main__.SmokeServices.from_environment") as from_environment,
            redirect_stdout(StringIO()),
            redirect_stderr(stderr),
        ):
            exit_code = _run_all(suite=resolve_suite("lifecycle-faults"))

        self.assertEqual(exit_code, 2)
        self.assertIn("API surface does not support", stderr.getvalue())
        from_environment.assert_not_called()


if __name__ == "__main__":
    unittest.main()

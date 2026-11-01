"""Tests for sequential smoke-suite execution."""

from __future__ import annotations

import dataclasses
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from space_smoke.__main__ import _apply_substrate, _failure_detail, _run_all
from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.registry import ScenarioRegistry
from space_smoke.runtime import CommandResult
from space_smoke.scenarios import REGISTRY
from space_smoke.suite import (
    RESULT_PREFIX,
    ScenarioProcessResult,
    _run_one_streamed,
    load_known_failures,
    run_scenarios,
    suite_exit_code,
)


class SmokeSuiteTests(unittest.TestCase):
    def test_category_with_no_scenarios_for_the_template_fails_before_bootstrap(
        self,
    ) -> None:
        stderr = StringIO()
        with (
            mock.patch.dict(
                "space_smoke.__main__.os.environ",
                {"E2B_TEMPLATE": "seed"},
                clear=True,
            ),
            mock.patch(
                "space_smoke.__main__.SmokeServices.from_environment"
            ) as from_environment,
            redirect_stderr(stderr),
        ):
            exit_code = _run_all(category="runtime")

        self.assertEqual(exit_code, 2)
        self.assertIn("no smoke scenarios apply", stderr.getvalue())
        from_environment.assert_not_called()

    def test_all_command_bootstraps_once_before_starting_scenario_processes(
        self,
    ) -> None:
        services = mock.Mock()
        services.telemetry_mode.return_value = "otel"
        services.storage_credentials.return_value = "exchange"
        services.substrate.return_value = "kubernetes"
        selected = tuple(
            scenario
            for scenario in REGISTRY
            if "ecw" in scenario.api_surfaces
            and "kind" in scenario.backends
            and scenario.quarantined is None
            and scenario.ignored is None
        )
        results = tuple(
            ScenarioProcessResult(scenario.id, 0, "", "") for scenario in selected
        )

        with (
            mock.patch.dict(
                "space_smoke.__main__.os.environ",
                {"SPACE_SMOKE_BACKEND": "kind", "SPACE_SMOKE_API_SURFACE": "ecw"},
                clear=True,
            ),
            mock.patch(
                "space_smoke.__main__.SmokeServices.from_environment",
                return_value=services,
            ) as from_environment,
            mock.patch(
                "space_smoke.__main__.run_scenarios",
                return_value=results,
            ) as run,
            redirect_stdout(StringIO()),
        ):
            exit_code = _run_all()

        self.assertEqual(exit_code, 0)
        required_tools = tuple(
            dict.fromkeys(
                tool for scenario in selected for tool in scenario.required_tools
            )
        )
        self.assertIn("aws", required_tools)
        from_environment.assert_called_once_with(
            next(iter(REGISTRY.select())),
            {"SPACE_SMOKE_BACKEND": "kind", "SPACE_SMOKE_API_SURFACE": "ecw"},
            required_tools=required_tools,
        )
        services.bootstrap.assert_called_once_with()
        self.assertEqual(run.call_args.args[0], selected)
        self.assertEqual(run.call_args.kwargs["environment"]["SKIP_BOOTSTRAP"], "1")
        self.assertEqual(
            run.call_args.kwargs["environment"]["SPACE_SMOKE_SUITE_BOOTSTRAPPED"], "1"
        )

    def test_all_command_names_each_quarantined_scenario_and_skips_it(self) -> None:
        # Quarantine a copy of a real scenario rather than reading the live
        # quarantine list, which is usually empty and would make this vacuous.
        services = mock.Mock()
        services.telemetry_mode.return_value = "otel"
        services.storage_credentials.return_value = "exchange"
        services.substrate.return_value = "kubernetes"
        live = tuple(
            scenario
            for scenario in REGISTRY
            if "ecw" in scenario.api_surfaces
            and "kind" in scenario.backends
            and scenario.quarantined is None
            and scenario.ignored is None
        )
        held = dataclasses.replace(live[0], quarantined="tracked in #0000")
        registry = ScenarioRegistry((held, *live[1:]))
        selected = live[1:]
        results = tuple(
            ScenarioProcessResult(scenario.id, 0, "", "") for scenario in selected
        )
        stdout = StringIO()
        with (
            mock.patch.dict(
                "space_smoke.__main__.os.environ",
                {"SPACE_SMOKE_BACKEND": "kind", "SPACE_SMOKE_API_SURFACE": "ecw"},
                clear=True,
            ),
            mock.patch("space_smoke.__main__.REGISTRY", registry),
            mock.patch(
                "space_smoke.__main__.SmokeServices.from_environment",
                return_value=services,
            ),
            mock.patch(
                "space_smoke.__main__.run_scenarios",
                return_value=results,
            ) as run,
            redirect_stdout(stdout),
        ):
            exit_code = _run_all()

        self.assertEqual(exit_code, 0)
        self.assertEqual(run.call_args.args[0], selected)
        self.assertNotIn(held, run.call_args.args[0])
        self.assertIn(f"QUARANTINED {held.id}: tracked in #0000", stdout.getvalue())

    def test_deployed_suite_does_not_inject_kind_bootstrap_state(self) -> None:
        services = mock.Mock()
        selected = tuple(
            scenario
            for scenario in REGISTRY
            if "ecw" in scenario.api_surfaces and "deployed" in scenario.backends
        )
        results = tuple(
            ScenarioProcessResult(scenario.id, 0, "", "") for scenario in selected
        )

        with (
            mock.patch.dict(
                "space_smoke.__main__.os.environ",
                {"SPACE_SMOKE_BACKEND": "deployed"},
                clear=True,
            ),
            mock.patch(
                "space_smoke.__main__.SmokeServices.from_environment",
                return_value=services,
            ),
            mock.patch(
                "space_smoke.__main__.run_scenarios",
                return_value=results,
            ) as run,
            redirect_stdout(StringIO()),
        ):
            exit_code = _run_all()

        self.assertEqual(exit_code, 0)
        services.bootstrap.assert_called_once_with()
        self.assertEqual(run.call_args.args[0], selected)
        self.assertEqual(
            run.call_args.kwargs["environment"],
            {"SPACE_SMOKE_BACKEND": "deployed"},
        )

    def test_scenarios_run_sequentially_in_separate_processes(self) -> None:
        scenarios = tuple(REGISTRY)[:3]
        commands: list[tuple[str, ...]] = []
        events: list[str] = []

        def run(command, **options):
            commands.append(tuple(command))
            self.assertEqual(options["env"]["SKIP_BOOTSTRAP"], "1")
            scenario_id = command[-1]
            report = json.dumps(
                {
                    "scenario_id": scenario_id,
                    "verdict": "pass",
                    "summary": "passed",
                    "evidence": {},
                    "cleanup_failures": [],
                }
            )
            return subprocess.CompletedProcess(
                command,
                0,
                f"passed\n{RESULT_PREFIX}{report}\n",
                "",
            )

        results = run_scenarios(
            scenarios,
            environment={"SKIP_BOOTSTRAP": "1"},
            executable="python-smoke",
            process_runner=run,
            on_start=lambda scenario_id: events.append(f"start:{scenario_id}"),
            line_sink=lambda line: events.append(f"output:{line.splitlines()[0]}"),
            on_result=lambda result: events.append(f"end:{result.scenario_id}"),
        )

        self.assertEqual(
            [result.scenario_id for result in results], [s.id for s in scenarios]
        )
        self.assertEqual(
            commands,
            [
                ("python-smoke", "-m", "space_smoke", "run", scenario.id)
                for scenario in scenarios
            ],
        )
        self.assertTrue(all(result.exit_code == 0 for result in results))
        self.assertTrue(all(result.report is not None for result in results))
        self.assertEqual(
            events,
            [
                event
                for scenario in scenarios
                for event in (
                    f"start:{scenario.id}",
                    "output:passed",
                    f"end:{scenario.id}",
                )
            ],
        )

    def test_failed_suite_writes_actionable_alert_detail(self) -> None:
        services = mock.Mock()
        report = {
            "scenario_id": "ecw.command_round_trip",
            "verdict": "fail",
            "summary": "sandbox command did not return the expected marker",
            "evidence": {"sandbox_id": "sandbox-123"},
            "cleanup_failures": [],
        }
        results = (
            ScenarioProcessResult(
                "ecw.command_round_trip",
                1,
                "",
                "",
                report,
            ),
        )

        with tempfile.TemporaryDirectory() as directory:
            detail_path = Path(directory) / "failure-details"
            with (
                mock.patch.dict(
                    "space_smoke.__main__.os.environ",
                    {
                        "SPACE_SMOKE_BACKEND": "deployed",
                        "SPACE_SMOKE_FAILURE_DETAILS_PATH": str(detail_path),
                    },
                    clear=True,
                ),
                mock.patch(
                    "space_smoke.__main__.SmokeServices.from_environment",
                    return_value=services,
                ),
                mock.patch(
                    "space_smoke.__main__.run_scenarios",
                    return_value=results,
                ),
                redirect_stdout(StringIO()),
            ):
                exit_code = _run_all()

            self.assertEqual(exit_code, 1)
            self.assertEqual(
                detail_path.read_text(encoding="utf-8"),
                "ecw.command_round_trip [fail]: sandbox command did not return "
                "the expected marker (sandbox_id=sandbox-123)\n",
            )

    @staticmethod
    def _run_all_with(
        results: tuple[ScenarioProcessResult, ...], environment: dict[str, str]
    ) -> tuple[int, str]:
        stdout = StringIO()
        with (
            mock.patch.dict("space_smoke.__main__.os.environ", environment, clear=True),
            mock.patch("space_smoke.__main__.SmokeServices.from_environment") as from_environment,
            mock.patch("space_smoke.__main__.run_scenarios", return_value=results),
            redirect_stdout(stdout),
        ):
            from_environment.return_value.substrate.return_value = "kubernetes"
            exit_code = _run_all()
        return exit_code, stdout.getvalue()

    # Shapes seen in main CI: a body that passed but whose cleanup failed with a
    # command's multi-line stderr, and a scenario process that died without a
    # result line.
    CLEANUP_FAILED = ScenarioProcessResult(
        "ecw.command_round_trip",
        2,
        "",
        "",
        {
            "verdict": "infra_error",
            "summary": "ECW command round trip passed; critical cleanup failed",
            "evidence": {
                "sandbox_id": "sandbox-123",
                "phases": [{"name": "create sandbox"}, {"name": "run marker command"}],
            },
            "cleanup_failures": [
                "kill sandbox: the delete failed: grpc_code=14\n"
                "ERROR: sandbox host mutation or reset is in progress; retry"
            ],
        },
    )
    CRASHED = ScenarioProcessResult(
        "ecw.sandbox_info", 2, "--- create sandbox\nSegmentation fault\n", ""
    )
    PASSED = ScenarioProcessResult("ecw.sandbox_delete", 0, "", "", {"verdict": "pass"})

    def test_a_failed_run_ends_on_every_scenario_that_did_not_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            detail_path = Path(directory) / "failure-details"
            exit_code, stdout = self._run_all_with(
                (self.CLEANUP_FAILED, self.PASSED, self.CRASHED),
                {
                    "SPACE_SMOKE_BACKEND": "kind",
                    "SPACE_SMOKE_FAILURE_DETAILS_PATH": str(detail_path),
                },
            )
            details = detail_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 2)
        tail = stdout[stdout.index("SUITE: ") :].splitlines()
        self.assertEqual(
            tail,
            [
                "SUITE: 1 passed, 0 warned, 0 failed, "
                "2 inconclusive or infrastructure errors",
                "",
                "NOT PASSED on the ecw API surface (2):",
                "  INFRA_ERROR ecw.command_round_trip: "
                "ECW command round trip passed; critical cleanup failed",
                "    last phase: run marker command",
                "    sandbox_id: sandbox-123",
                "    cleanup failure: kill sandbox: the delete failed: grpc_code=14",
                "      ERROR: sandbox host mutation or reset is in progress; retry",
                "    rerun: SPACE_SMOKE_BACKEND=kind uv run --frozen --python 3.11 "
                "-m space_smoke run ecw.command_round_trip",
                "  INFRA_ERROR ecw.sandbox_info: "
                "scenario process exited 2 without a result",
                "    | --- create sandbox",
                "    | Segmentation fault",
                "    rerun: SPACE_SMOKE_BACKEND=kind uv run --frozen --python 3.11 "
                "-m space_smoke run ecw.sandbox_info",
            ],
        )
        # The details file stays one line per scenario, with the multi-line
        # cleanup failure folded onto its line.
        self.assertEqual(
            details.splitlines(),
            [
                "ecw.command_round_trip [infra_error]: ECW command round trip "
                "passed; critical cleanup failed (sandbox_id=sandbox-123); cleanup "
                "failures: kill sandbox: the delete failed: grpc_code=14 | ERROR: "
                "sandbox host mutation or reset is in progress; retry",
                "ecw.sandbox_info [infra_error]: scenario process exited 2 without "
                "a result",
            ],
        )

    def test_github_actions_annotates_every_scenario_that_did_not_pass(self) -> None:
        warned = ScenarioProcessResult(
            "ecw.sandbox_delete", 42, "", "", {"verdict": "warn", "summary": "slow"}
        )
        _, stdout = self._run_all_with(
            (self.CLEANUP_FAILED, warned),
            {"SPACE_SMOKE_BACKEND": "kind", "GITHUB_ACTIONS": "true"},
        )

        annotations = [line for line in stdout.splitlines() if line.startswith("::")]
        self.assertEqual(
            annotations,
            [
                "::error title=typed smoke (ecw)%3A ecw.command_round_trip::"
                "ecw.command_round_trip [infra_error]: ECW command round trip passed; "
                "critical cleanup failed (sandbox_id=sandbox-123); cleanup failures: "
                "kill sandbox: the delete failed: grpc_code=14 | ERROR: sandbox host "
                "mutation or reset is in progress; retry%0Arerun: SPACE_SMOKE_BACKEND=kind "
                "uv run --frozen --python 3.11 -m space_smoke run ecw.command_round_trip",
                "::warning title=typed smoke (ecw)%3A ecw.sandbox_delete::"
                "ecw.sandbox_delete [warn]: slow%0Arerun: SPACE_SMOKE_BACKEND=kind "
                "uv run --frozen --python 3.11 -m space_smoke run ecw.sandbox_delete",
            ],
        )

    def test_streamed_runner_emits_lines_and_retains_the_result(self) -> None:
        payload = json.dumps(
            {
                "scenario_id": "ecw.example",
                "verdict": "fail",
                "summary": "marker mismatch",
                "evidence": {},
                "cleanup_failures": [],
            }
        )
        lines: list[str] = []

        result = _run_one_streamed(
            "ecw.example",
            command=[
                sys.executable,
                "-c",
                f"print('--- phase'); print({f'{RESULT_PREFIX}{payload}'!r})",
            ],
            environment=os.environ,
            line_sink=lines.append,
        )

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(lines, ["--- phase\n", f"{RESULT_PREFIX}{payload}\n"])
        self.assertIsNotNone(result.report)
        assert result.report is not None
        self.assertEqual(result.report["summary"], "marker mismatch")

    def test_failure_detail_survives_a_process_without_structured_output(self) -> None:
        result = ScenarioProcessResult("ecw.crashed", 2, "", "process vanished")

        self.assertEqual(
            _failure_detail(result),
            "ecw.crashed [infra_error]: scenario process exited 2 without a result",
        )

    def test_suite_exit_code_preserves_failures_and_infrastructure_errors(self) -> None:
        passed = ScenarioProcessResult("pass", 0, "", "")
        warning = ScenarioProcessResult("warn", 42, "", "")
        failed = ScenarioProcessResult("fail", 1, "", "")
        infrastructure = ScenarioProcessResult("infra", 2, "", "")

        self.assertEqual(suite_exit_code((passed,)), 0)
        self.assertEqual(suite_exit_code((passed, warning)), 42)
        self.assertEqual(suite_exit_code((warning, infrastructure)), 2)
        self.assertEqual(suite_exit_code((passed, infrastructure)), 2)
        self.assertEqual(suite_exit_code((infrastructure, failed)), 1)

    def _run_with_known_failures(
        self, results: tuple[ScenarioProcessResult, ...], listing: str | None
    ) -> tuple[int, str]:
        environment = {"SPACE_SMOKE_BACKEND": "kind", "GITHUB_ACTIONS": "true"}
        with tempfile.TemporaryDirectory() as directory:
            if listing is not None:
                path = Path(directory) / "known-failures.txt"
                path.write_text(listing, encoding="utf-8")
                environment["SPACE_SMOKE_KNOWN_FAILURES"] = str(path)
            return self._run_all_with(results, environment)

    @staticmethod
    def _failed_in(*phases: str) -> ScenarioProcessResult:
        evidence = {"phases": [{"name": phase} for phase in phases]}
        return ScenarioProcessResult(
            "ecw.sandbox_info",
            1,
            "",
            "",
            {"verdict": "fail", "summary": "no info", "evidence": evidence},
        )

    def test_a_known_failures_list_excuses_only_a_failure_in_the_listed_phase(
        self,
    ) -> None:
        listing = (
            "# Direct lane\n"
            "\n"
            "ecw.sandbox_info | verify node cleanup | cleanup needs the sandbox pod\n"
            "ecw.command_round_trip | run marker command | volume keys need a node\n"
        )
        warned = dataclasses.replace(
            self.CLEANUP_FAILED,
            returncode=42,
            report={**self.CLEANUP_FAILED.report, "verdict": "warn"},
        )
        in_listed_phase = self._failed_in("create sandbox", "verify node cleanup")
        # (results, listing, exit status, annotation levels, "not excused"
        # lines): only a fail verdict in the listed phase is excused. Every
        # other result of a listed scenario counts as an unlisted one would,
        # and without the variable nothing is excused.
        cases = {
            "listed failure in the listed phase": (
                (in_listed_phase, self.PASSED), listing, 0, ["::notice"], 0
            ),
            "listed failure in an earlier phase": (
                (self._failed_in("create sandbox"),), listing, 1, ["::error"], 1
            ),
            "listed failure after the listed phase": (
                (self._failed_in("verify node cleanup", "delete sandbox"),), listing, 1, ["::error"], 1
            ),
            "listed failure with no phase recorded": (
                (self._failed_in(),), listing, 1, ["::error"], 1
            ),
            "listed infrastructure error in the listed phase": (
                (self.CLEANUP_FAILED,), listing, 2, ["::error"], 1
            ),
            "listed warning in the listed phase": ((warned,), listing, 42, ["::warning"], 1),
            "listed scenario passed": (
                (dataclasses.replace(self.PASSED, scenario_id="ecw.sandbox_info"),), listing, 0, ["::warning"], 0
            ),
            "unlisted failure": (
                (in_listed_phase,),
                "ecw.command_round_trip | run marker command | x\n",
                1,
                ["::error"],
                0,
            ),
            "unlisted infrastructure error": (
                (in_listed_phase, self.CLEANUP_FAILED),
                "ecw.sandbox_info | verify node cleanup | x\n",
                2,
                ["::notice", "::error"],
                0,
            ),
            "no list": ((in_listed_phase,), None, 1, ["::error"], 0),
        }
        for name, (results, text, expected, levels, not_excused) in cases.items():
            with self.subTest(name):
                exit_code, stdout = self._run_with_known_failures(results, text)
                self.assertEqual(exit_code, expected)
                lines = stdout.splitlines()
                self.assertEqual(
                    [line.split(" ", 1)[0] for line in lines if line.startswith("::")],
                    levels,
                )
                self.assertEqual(
                    sum(line.endswith("; not excused") for line in lines), not_excused
                )

    def test_a_known_failures_list_reports_known_failures_and_unexpected_passes(
        self,
    ) -> None:
        listing = (
            "ecw.sandbox_info | verify node cleanup | cleanup needs the sandbox pod\n"
            "ecw.command_round_trip | verify marker | volume keys need a node registration\n"
            "ecw.sandbox_delete | delete sandbox | the delete never settles\n"
            "ecw.filesystem_round_trip | write file | listed but not run in this pass\n"
        )
        exit_code, stdout = self._run_with_known_failures(
            (
                self._failed_in("create sandbox", "verify node cleanup"),
                self.CLEANUP_FAILED,
                self.PASSED,
            ),
            listing,
        )

        self.assertEqual(exit_code, 2)
        lines = stdout.splitlines()
        self.assertIn("    known failure: cleanup needs the sandbox pod", lines)
        not_excused = lines.index("  INFRA_ERROR ecw.command_round_trip: " + (
            "ECW command round trip passed; critical cleanup failed"
        ))
        self.assertTrue(
            next(
                line for line in lines[not_excused + 1 :] if line.startswith("    listed in ")
            ).endswith('known-failures.txt for phase "verify marker"; not excused'),
        )
        header = next(index for index, line in enumerate(lines) if line.startswith("KNOWN FAILURES "))
        self.assertTrue(
            lines[header].endswith(
                "known-failures.txt): 1 failed as listed, 1 failed otherwise, "
                "1 passed unexpectedly"
            ),
            lines[header],
        )
        self.assertEqual(
            [line for line in lines[header + 1 :] if not line.startswith("::")],
            [
                "  KNOWN FAILURE ecw.sandbox_info: cleanup needs the sandbox pod",
                "  UNEXPECTED PASS ecw.sandbox_delete: listed as \"the delete never "
                "settles\"; remove it from the list if it keeps passing",
            ],
        )
        annotations = [line for line in lines if line.startswith("::")]
        self.assertEqual(
            [line.split(" ", 1)[0] for line in annotations],
            ["::notice", "::error", "::warning"],
        )
        self.assertIn("typed smoke (ecw)%3A ecw.sandbox_delete", annotations[2])
        self.assertIn("unexpected pass", annotations[2])

    def test_a_known_failures_list_that_cannot_be_trusted_fails_before_bootstrap(
        self,
    ) -> None:
        # (listing, the line the error names)
        cases = {
            "unknown scenario": ("ecw.no_such_scenario | create sandbox | renamed\n", 1),
            "no phase": ("# comment\necw.sandbox_info |  | no phase\n", 2),
            "no phase field": ("ecw.sandbox_info  cleanup needs the sandbox pod\n", 1),
            "no reason": ("\necw.sandbox_info | create sandbox |  \n", 2),
            "no reason field": ("ecw.sandbox_info | create sandbox\n", 1),
            "no scenario id": (" | create sandbox | reason\n", 1),
            "listed twice": (
                "ecw.sandbox_info | create sandbox | one\n"
                "ecw.sandbox_info | verify node cleanup | two\n",
                2,
            ),
        }
        for name, (listing, line) in cases.items():
            with (
                self.subTest(name),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "known-failures.txt"
                path.write_text(listing, encoding="utf-8")
                environment = {
                    "SPACE_SMOKE_BACKEND": "kind",
                    "SPACE_SMOKE_KNOWN_FAILURES": str(path),
                }
                with (
                    mock.patch.dict("space_smoke.__main__.os.environ", environment, clear=True),
                    mock.patch(
                        "space_smoke.__main__.SmokeServices.from_environment"
                    ) as from_environment,
                    redirect_stdout(StringIO()),
                    redirect_stderr(StringIO()) as stderr,
                ):
                    self.assertEqual(_run_all(), 2)
                self.assertIn(f"ERROR: {path}:{line}:", stderr.getvalue())
                from_environment.assert_not_called()
        with (
            mock.patch.dict(
                "space_smoke.__main__.os.environ",
                {"SPACE_SMOKE_KNOWN_FAILURES": "/nonexistent/known-failures.txt"},
                clear=True,
            ),
            redirect_stderr(StringIO()) as stderr,
        ):
            self.assertEqual(_run_all(), 2)
        self.assertIn("known-failures.txt", stderr.getvalue())

    def test_the_direct_lane_lists_only_scenarios_the_direct_lane_runs(self) -> None:
        # The lane reads this list only after a full bring-up, and a listed
        # scenario it never runs is silently dropped, so a rename, a narrowing
        # or a quarantine must update the list in the same change.
        path = Path(__file__).resolve().parents[2] / "ci" / "kind-direct-known-failures.txt"
        listed = load_known_failures(path, {scenario.id for scenario in REGISTRY})

        for scenario_id in listed:
            with self.subTest(scenario_id):
                registered = REGISTRY.get(scenario_id)
                self.assertIn("direct", registered.substrates)
                self.assertIsNone(registered.quarantined)
                self.assertIsNone(registered.ignored)


class BenchSubstrateTests(unittest.TestCase):
    """The suite runs only the scenarios its bench's substrate serves."""

    KIND = tuple(scenario for scenario in REGISTRY if "kind" in scenario.backends)

    def _apply(self, environment, *, backend="kind", bench="kubernetes"):
        services = mock.Mock()
        if isinstance(bench, Exception):
            services.substrate.side_effect = bench
        else:
            services.substrate.return_value = bench
        stdout, stderr = StringIO(), StringIO()
        with (
            mock.patch.dict("space_smoke.__main__.os.environ", environment, clear=True),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            runnable, error = _apply_substrate(self.KIND, services, backend)
        return runnable, error, stdout.getvalue(), stderr.getvalue(), services

    def test_kind_filters_by_the_substrate_read_from_the_bench(self) -> None:
        for bench in ("kubernetes", "direct"):
            for requested in ({}, {"SPACE_SMOKE_SUBSTRATE": bench}, {"SPACE_KIND_SUBSTRATE": bench}):
                with self.subTest(bench=bench, requested=requested):
                    runnable, error, stdout, _, _ = self._apply(requested, bench=bench)
                    self.assertIsNone(error)
                    self.assertEqual(
                        runnable, tuple(s for s in self.KIND if bench in s.substrates)
                    )
                    for skipped in (s for s in self.KIND if s not in runnable):
                        self.assertIn(f"SKIPPED {skipped.id}: runs only on substrate(s)", stdout)
        # Every Kind scenario serves the Kubernetes substrate, so the default
        # lane keeps its whole inventory; the Direct lane does not.
        self.assertEqual(self._apply({})[0], self.KIND)
        self.assertLess(len(self._apply({}, bench="direct")[0]), len(self.KIND))

    def test_kind_fails_a_request_for_the_substrate_the_bench_does_not_run(self) -> None:
        for variable in ("SPACE_SMOKE_SUBSTRATE", "SPACE_KIND_SUBSTRATE"):
            with self.subTest(variable=variable):
                _, error, _, _, _ = self._apply({variable: "direct"}, bench="kubernetes")
                self.assertEqual(
                    error, f"{variable}=direct, but the bench runs the kubernetes substrate"
                )

    def test_kind_bench_that_cannot_be_read_fails_only_a_request(self) -> None:
        unreadable = RuntimeError("no API Deployment")
        runnable, error, _, stderr, _ = self._apply({}, bench=unreadable)
        self.assertIsNone(error)
        self.assertEqual(runnable, self.KIND)
        self.assertIn("could not read the bench's substrate", stderr)
        _, error, _, _, _ = self._apply({"SPACE_SMOKE_SUBSTRATE": "direct"}, bench=unreadable)
        self.assertIn("SPACE_SMOKE_SUBSTRATE=direct, but the bench's substrate could not be read", error)

    def test_deployed_backend_filters_only_by_an_explicit_selector(self) -> None:
        runnable, error, _, _, services = self._apply({}, backend="deployed")
        self.assertEqual((runnable, error), (self.KIND, None))
        runnable, error, _, _, _ = self._apply(
            {"SPACE_SMOKE_SUBSTRATE": "direct", "SPACE_KIND_SUBSTRATE": "kubernetes"},
            backend="deployed",
        )
        self.assertIsNone(error)
        self.assertEqual(runnable, tuple(s for s in self.KIND if "direct" in s.substrates))
        services.substrate.assert_not_called()

    def test_suite_refuses_an_unknown_substrate_before_bootstrap(self) -> None:
        for variable in ("SPACE_SMOKE_SUBSTRATE", "SPACE_KIND_SUBSTRATE"):
            with (
                self.subTest(variable=variable),
                mock.patch.dict("space_smoke.__main__.os.environ", {variable: "Direct"}, clear=True),
                mock.patch("space_smoke.__main__.SmokeServices.from_environment") as from_environment,
                redirect_stderr(StringIO()) as stderr,
            ):
                self.assertEqual(_run_all(), 2)
                self.assertIn(f"{variable} must be one of direct, kubernetes", stderr.getvalue())
                from_environment.assert_not_called()

    def test_native_suite_reads_the_bench_whatever_its_first_fixture(self) -> None:
        # The native contract category starts with a NativeHardeningFixture
        # scenario, whose services carry no Kind fixture controls.
        category = REGISTRY.select(category="native_contract")
        self.assertNotEqual(category[0].fixture_type.__name__, "CommandSandboxFixture")
        environment = {
            "SPACE_SMOKE_BACKEND": "kind",
            "SPACE_SMOKE_API_SURFACE": "native",
            "SPACE_API_TOKEN": "token",
            "SPACE_KIND_SUBSTRATE": "direct",
        }
        with (
            mock.patch.dict("space_smoke.__main__.os.environ", environment, clear=True),
            mock.patch("space_smoke.services.shutil.which", return_value="/usr/bin/true"),
            mock.patch.object(KindHarness, "substrate", return_value="direct") as substrate,
            mock.patch("space_smoke.__main__.run_scenarios", return_value=()) as run,
            redirect_stdout(StringIO()) as stdout,
            redirect_stderr(StringIO()) as stderr,
        ):
            exit_code = _run_all(category="native_contract")
        self.assertEqual(exit_code, 0, stderr.getvalue())
        substrate.assert_called_once_with()
        self.assertEqual(
            run.call_args.args[0], tuple(s for s in category if "direct" in s.substrates)
        )
        for skipped in (s for s in category if "direct" not in s.substrates):
            self.assertIn(f"SKIPPED {skipped.id}: runs only on substrate(s)", stdout.getvalue())

    def test_a_shard_the_other_modes_empty_still_fails_a_substrate_mismatch(self) -> None:
        environment = {"SPACE_SMOKE_BACKEND": "kind", "SPACE_SMOKE_SUBSTRATE": "direct"}
        with (
            mock.patch.dict("space_smoke.__main__.os.environ", environment, clear=True),
            mock.patch("space_smoke.__main__.SmokeServices.from_environment") as from_environment,
            mock.patch("space_smoke.__main__._apply_telemetry_mode", return_value=((), None)),
            mock.patch("space_smoke.__main__.run_scenarios") as run,
            redirect_stdout(StringIO()),
            redirect_stderr(StringIO()) as stderr,
        ):
            from_environment.return_value.substrate.return_value = "kubernetes"
            exit_code = _run_all(category="local", shard=(1, 4))
        self.assertEqual(exit_code, 2)
        self.assertIn(
            "SPACE_SMOKE_SUBSTRATE=direct, but the bench runs the kubernetes substrate",
            stderr.getvalue(),
        )
        run.assert_not_called()

    def test_kind_harness_reads_the_api_deployments_control_plane_flag(self) -> None:
        argv = ("kubectl",)
        cases = {"true": "kubernetes", "false": "direct", "yes": None, None: None}
        for value, expected in cases.items():
            env = [] if value is None else [{"name": "SPACE_K8S_CONTROL_PLANE", "value": value}]
            harness = KindHarness(
                _ScriptedKubectl(
                    [
                        CommandResult(argv, 0, "space-api\n", ""),
                        CommandResult(argv, 0, json.dumps({"name": "api", "env": env}), ""),
                    ]
                ),
                KindConfig(),
            )
            with self.subTest(value=value):
                if expected is None:
                    with self.assertRaisesRegex(Exception, "SPACE_K8S_CONTROL_PLANE"):
                        harness.substrate()
                else:
                    self.assertEqual(harness.substrate(), expected)


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)

    def run(self, command, **_options) -> CommandResult:
        return self.results.pop(0)


if __name__ == "__main__":
    unittest.main()

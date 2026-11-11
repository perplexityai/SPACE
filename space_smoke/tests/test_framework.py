"""Characterization tests for the minimal smoke framework."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import space_smoke as smoke
from space_smoke.__main__ import _rerun_command
from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.fixtures import CommandSandboxFixture, Ecw
from space_smoke.model import Verdict
from space_smoke.registry import ScenarioRegistry
from space_smoke.runtime import (
    InvariantViolation,
    background,
    CommandResult,
    ResourceScope,
    SmokeConfigurationError,
    SubprocessExecutor,
    execute_scenario,
)
from space_smoke.scenarios import REGISTRY


class FrameworkTraceTests(unittest.TestCase):
    def test_scenario_inventory_does_not_require_the_optional_ecw_sdk(self) -> None:
        script = """
import builtins
import runpy
import sys

original_import = builtins.__import__

def without_e2b(name, *args, **kwargs):
    if name in {"e2b", "httpx"} or name.startswith(("e2b.", "httpx.")):
        raise ModuleNotFoundError(f"blocked optional dependency: {name}")
    return original_import(name, *args, **kwargs)

builtins.__import__ = without_e2b
sys.argv = ["space_smoke", "list"]
runpy.run_module("space_smoke", run_name="__main__")
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("ecw.command_round_trip", completed.stdout)

    def test_run_reports_missing_smoke_client_dependencies(self) -> None:
        script = """
import builtins
import runpy
import sys

original_import = builtins.__import__

def without_smoke_dependencies(name, *args, **kwargs):
    if name in {"e2b", "httpx"} or name.startswith(("e2b.", "httpx.")):
        raise ModuleNotFoundError(f"blocked optional dependency: {name}")
    return original_import(name, *args, **kwargs)

builtins.__import__ = without_smoke_dependencies
import importlib.util
original_find_spec = importlib.util.find_spec
importlib.util.find_spec = lambda name: None if name == "e2b" else original_find_spec(name)
sys.argv = ["space_smoke", "run", "ecw.command_round_trip"]
runpy.run_module("space_smoke", run_name="__main__")
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 2, completed.stderr)
        self.assertIn("smoke client dependencies are unavailable", completed.stderr)
        self.assertIn("uv run -m space_smoke", completed.stderr)
        self.assertNotIn("ModuleNotFoundError", completed.stderr)

    def test_decorated_scenario_is_discovered_without_a_manual_import(self) -> None:
        registered = REGISTRY.get("ecw.command_round_trip")

        self.assertIs(registered.fixture_type, CommandSandboxFixture)
        self.assertEqual(registered.fixture_name, "command_sandbox")
        self.assertTrue(callable(registered.verify))
        self.assertEqual(
            REGISTRY.categories,
            ("ecw", "local", "native", "native_concurrency", "native_contract",
             "native_recovery", "network_policy", "node")
        )

    def test_external_scenario_package_extends_the_public_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "partner_smokes"
            package.mkdir()
            (package / "__init__.py").write_text("", encoding="utf-8")
            (package / "partner_round_trip.py").write_text(
                """import space_smoke as smoke

@smoke.scenario("partner.round_trip")
def partner_round_trip(_test: smoke.Ecw) -> None:
    \"\"\"A caller-owned scenario is discovered without modifying core.\"\"\"
""",
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment["SPACE_SMOKE_SCENARIO_PACKAGES"] = "partner_smokes"
            environment["PYTHONPATH"] = os.pathsep.join(
                (directory, str(Path(__file__).resolve().parents[2]))
            )

            completed = subprocess.run(
                [sys.executable, "-m", "space_smoke", "list"],
                cwd=Path(__file__).resolve().parents[2],
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("ecw.command_round_trip", completed.stdout)
        self.assertIn("partner.round_trip", completed.stdout)

    def test_decorator_derives_metadata_from_id_docstring_and_fixture(self) -> None:
        @smoke.scenario("ecw.example")
        def example(_test: smoke.Ecw) -> None:
            """A wrapped invariant remains one line.
            Even when its docstring is formatted across lines.
            """

        self.assertEqual(example.title, "ECW example")
        self.assertEqual(example.category, "ecw")
        self.assertEqual(
            example.invariant,
            "A wrapped invariant remains one line. Even when its docstring is "
            "formatted across lines.",
        )
        self.assertIs(example.fixture_type, Ecw)
        self.assertEqual(example.sandbox, smoke.SandboxConfig())
        self.assertEqual(example.sandbox_count, 1)
        self.assertIsNone(example.templates)
        self.assertEqual(example.rerun_environment, ())
        self.assertEqual(example.required_tools, ())
        self.assertEqual(
            example.sandbox.lifecycle,
            smoke.pause_on_timeout(),
        )
        self.assertEqual(example.timeout_minutes, Ecw.timeout_minutes)

    def test_sandbox_config_rejects_non_positive_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "sandbox timeout must be positive"):
            smoke.SandboxConfig(timeout=0)

    def test_network_policy_sandbox_preserves_the_legacy_create_shape(self) -> None:
        configured = smoke.network_policy_sandbox(
            internet=False,
            allow=("example.com",),
            deny=("1.0.0.1/32",),
        )

        self.assertEqual(configured.timeout, 600)
        self.assertIsNone(configured.lifecycle)
        self.assertIs(configured.allow_internet_access, False)
        self.assertEqual(
            configured.network,
            smoke.NetworkConfig(
                allow_public_traffic=False,
                allow_out=("example.com",),
                deny_out=("1.0.0.1/32",),
            ),
        )

    def test_decorator_carries_typed_sandbox_and_execution_configuration(
        self,
    ) -> None:
        sandbox = smoke.SandboxConfig(
            timeout=3,
            template="configured-seed",
            lifecycle=smoke.pause_on_timeout(auto_resume=True),
        )

        @smoke.scenario("ecw.configured", sandbox=sandbox, sandboxes=3)
        def configured(_test: smoke.Ecw) -> None:
            """Typed configuration reaches fixture construction."""

        self.assertEqual(configured.sandbox, sandbox)
        self.assertEqual(configured.sandbox_count, 3)

    def test_decorator_carries_template_applicability(self) -> None:
        @smoke.scenario("runtime.example", templates=("custom-runtime",))
        def example(_test: smoke.Ecw) -> None:
            """One runtime certification applies only to custom-runtime."""

        self.assertEqual(example.title, "Runtime example")
        self.assertEqual(example.category, "runtime")
        self.assertEqual(example.templates, frozenset({"custom-runtime"}))
        self.assertTrue(example.applies_to("custom-runtime:release"))
        self.assertFalse(example.applies_to("seed"))

    def test_decorator_carries_scenario_specific_rerun_environment(self) -> None:
        @smoke.scenario(
            "ecw.rerun_context",
            rerun_environment=("EXPERIMENT_FLAG",),
        )
        def rerun_context(_test: smoke.Ecw) -> None:
            """A failed scenario retains the environment that selected its path."""

        with mock.patch.dict(
            os.environ,
            {
                "SPACE_SMOKE_BACKEND": "deployed",
                "SPACE_SMOKE_SCENARIO_PACKAGES": "private_smokes.scenarios",
                "EXPERIMENT_FLAG": "enabled value",
            },
            clear=True,
        ):
            command = _rerun_command(rerun_context)

        self.assertEqual(
            command,
            "SPACE_SMOKE_BACKEND=deployed "
            "SPACE_SMOKE_SCENARIO_PACKAGES=private_smokes.scenarios "
            "EXPERIMENT_FLAG='enabled value' "
            "uv run --frozen --python 3.11 -m space_smoke run ecw.rerun_context",
        )

    def test_decorator_carries_required_host_tools(self) -> None:
        @smoke.scenario("ecw.tooling", required_tools=("aws", "jq"))
        def tooling(_test: smoke.Ecw) -> None:
            """Tool prerequisites remain attached to the scenario declaration."""

        self.assertEqual(tooling.required_tools, ("aws", "jq"))

    def test_quarantine_is_declared_with_a_reason_and_withheld_from_selection(
        self,
    ) -> None:
        @smoke.scenario("ecw.held", quarantined="tracked in #0000")
        def held(_test: smoke.Ecw) -> None:
            """A quarantined scenario keeps its declaration and its reason."""

        @smoke.scenario("ecw.live")
        def live(_test: smoke.Ecw) -> None:
            """An unquarantined scenario is selected as before."""

        self.assertEqual(held.quarantined, "tracked in #0000")
        self.assertIsNone(live.quarantined)
        registry = smoke.registry.ScenarioRegistry([held, live])
        self.assertEqual(registry.select(), (live,))
        self.assertEqual(registry.select(include_quarantined=True), (held, live))
        self.assertIs(registry.get("ecw.held"), held)
        with self.assertRaisesRegex(ValueError, "quarantine must state its reason"):

            @smoke.scenario("ecw.blank", quarantined="  ")
            def blank(_test: smoke.Ecw) -> None:
                """A blank reason is not a reason."""

    def test_decorator_rejects_invalid_required_host_tools(self) -> None:
        with self.assertRaisesRegex(ValueError, "required tools"):

            @smoke.scenario("ecw.bad_tooling", required_tools=("aws", "aws"))
            def bad_tooling(_test: smoke.Ecw) -> None:
                """Tool prerequisite declarations cannot be ambiguous."""

    def test_manual_tier_requires_a_reason_and_is_explicitly_selectable(self) -> None:
        @smoke.scenario("runtime.manual", tier="manual", manual_reason="Requires a dedicated bench")
        def manual(_test: smoke.Ecw) -> None:
            """An operator can select a dedicated-bench scenario by name."""

        @smoke.scenario("runtime.main", tier="main")
        def main(_test: smoke.Ecw) -> None:
            """Main coverage remains outside the routine selection."""

        registry = smoke.registry.ScenarioRegistry([manual, main])
        self.assertEqual(registry.select(), ())
        self.assertEqual(registry.select(tiers=("routine", "main")), (main,))
        self.assertEqual(registry.select(include_scenarios=(main.id,)), (main,))
        self.assertIs(registry.get(manual.id), manual)
        self.assertTrue(manual.applies_to("seed"))
        for options, message in (
            ({"tier": "manual"}, "manual smoke must state its reason"),
            ({"tier": "manual", "manual_reason": " "}, "manual smoke must state its reason"),
            ({"tier": "unknown"}, "smoke tier"),
            ({"manual_reason": "unused"}, "manual_reason requires"),
            ({"templates": ()}, "non-empty template ids"),
        ):
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, message):
                smoke.scenario("runtime.invalid", **options)
        with self.assertRaisesRegex(ValueError, "must have tier='main'"):
            registry.select(include_scenarios=(manual.id,))

    def test_decorator_rejects_unsafe_rerun_environment_names(self) -> None:
        with self.assertRaisesRegex(ValueError, "shell variable names"):

            @smoke.scenario(
                "ecw.unsafe_rerun_context",
                rerun_environment=("FEATURE-FLAG",),
            )
            def unsafe_rerun_context(_test: smoke.Ecw) -> None:
                """Rerun context cannot inject shell syntax."""

    def test_decorator_rejects_invalid_sandbox_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "sandbox count must be positive"):

            @smoke.scenario("ecw.no_sandboxes", sandboxes=0)
            def no_sandboxes(_test: smoke.Ecw) -> None:
                """A scenario must own at least one sandbox."""

        with self.assertRaisesRegex(ValueError, "KindAdmin does not allocate sandboxes"):

            @smoke.scenario("local.admin_sandboxes", sandboxes=2)
            def admin_sandboxes(_test: smoke.KindAdmin) -> None:
                """A control-plane fixture cannot silently ignore requested sandboxes."""

    def test_decorator_requires_an_invariant_docstring(self) -> None:
        with self.assertRaisesRegex(ValueError, "must describe its invariant"):

            @smoke.scenario("ecw.missing_invariant")
            def missing_invariant(_test: smoke.Ecw) -> None:
                pass

    def test_decorator_requires_one_supported_typed_fixture(self) -> None:
        with self.assertRaisesRegex(TypeError, "fixture must be one of"):

            @smoke.scenario("ecw.untyped")
            def untyped(_test) -> None:
                """An untyped scenario is rejected."""

        with self.assertRaisesRegex(TypeError, "Native.*CommandSandboxFixture"):

            @smoke.scenario("ecw.native_fixture", api_surfaces=("native",))
            def native_fixture(_test: smoke.Ecw) -> None:
                """Native scenarios require the shared command fixture."""

    def test_registry_rejects_duplicate_ids(self) -> None:
        registered = REGISTRY.get("ecw.command_round_trip")
        with self.assertRaisesRegex(ValueError, "duplicate smoke id"):
            ScenarioRegistry([registered, registered])

    def test_kind_transport_always_names_context_and_fixed_namespace(self) -> None:
        seen: list[tuple[str, ...]] = []

        class Executor:
            def run(self, command, **_options):
                seen.append(tuple(command))
                return CommandResult(tuple(command), 0, "", "")

        KindHarness(Executor(), KindConfig(cluster="local-test")).kubectl("get", "pods")
        self.assertEqual(
            seen,
            [
                (
                    "kubectl",
                    "--context",
                    "kind-local-test",
                    "-n",
                    "space",
                    "get",
                    "pods",
                )
            ],
        )
        with self.assertRaises(TypeError):
            KindConfig(namespace="space-test")  # type: ignore[call-arg]

    def test_kind_bootstrap_command_has_an_outer_deadline(self) -> None:
        seen: dict[str, object] = {}

        class Executor:
            def run(self, command, **options):
                seen["command"] = tuple(command)
                seen["timeout"] = options.get("timeout")
                seen["environment"] = options.get("env")
                return CommandResult(tuple(command), 0, "", "")

        KindHarness(Executor(), KindConfig()).bootstrap()

        timeout = seen["timeout"]
        self.assertIsInstance(timeout, (int, float))
        assert isinstance(timeout, (int, float))
        self.assertGreater(timeout, 0)
        environment = seen["environment"]
        self.assertIsInstance(environment, dict)
        assert isinstance(environment, dict)
        self.assertEqual(environment["DATA_PLANE_URL"], "http://127.0.0.1:8090")

    def test_subprocess_executor_can_remove_an_inherited_environment_value(
        self,
    ) -> None:
        executor = SubprocessExecutor(
            base_env={"PATH": os.environ.get("PATH", ""), "AWS_PROFILE": "host"}
        )
        completed = subprocess.CompletedProcess(
            args=("aws",), returncode=0, stdout="", stderr=""
        )
        with mock.patch(
            "space_smoke.runtime.subprocess.run", return_value=completed
        ) as run:
            executor.run(["aws"], env={"AWS_PROFILE": None})

        self.assertNotIn("AWS_PROFILE", run.call_args.kwargs["env"])

    def test_subprocess_executor_empty_base_environment_inherits(self) -> None:
        executor = SubprocessExecutor(base_env={})
        completed = subprocess.CompletedProcess(
            args=("env",), returncode=0, stdout="", stderr=""
        )
        with mock.patch(
            "space_smoke.runtime.subprocess.run", return_value=completed
        ) as run:
            executor.run(["env"])

        self.assertIsNone(run.call_args.kwargs["env"])

    def test_subprocess_executor_can_explicitly_empty_the_environment(self) -> None:
        executor = SubprocessExecutor(base_env={"AWS_PROFILE": "host"})
        completed = subprocess.CompletedProcess(
            args=("aws",), returncode=0, stdout="", stderr=""
        )
        with mock.patch(
            "space_smoke.runtime.subprocess.run", return_value=completed
        ) as run:
            executor.run(["aws"], env={"AWS_PROFILE": None})

        self.assertEqual(run.call_args.kwargs["env"], {})

    def test_cleanup_is_lifo_and_critical_failure_changes_success(self) -> None:
        trace: list[str] = []
        scope = ResourceScope()
        scope.defer("first", lambda: trace.append("first"))
        scope.defer("second", lambda: trace.append("second"))
        self.assertEqual(scope.close(), ())
        self.assertEqual(trace, ["second", "first"])

        def body(context) -> None:
            context.scope.defer(
                "uncordon",
                lambda: (_ for _ in ()).throw(RuntimeError("still cordoned")),
                critical=True,
            )

        result = execute_scenario(title="Trace", body=body)
        self.assertEqual(result.verdict, Verdict.INFRA_ERROR)
        self.assertIn("critical cleanup failed", result.summary)

    def test_configuration_and_unexpected_errors_have_distinct_verdicts(self) -> None:
        def misconfigured(_context) -> None:
            raise SmokeConfigurationError("unsafe target")

        def broken(_context) -> None:
            raise RuntimeError("transport failed")

        configuration = execute_scenario(title="Trace", body=misconfigured)
        infrastructure = execute_scenario(title="Trace", body=broken)

        self.assertEqual(configuration.verdict, Verdict.INCONCLUSIVE)
        self.assertEqual(configuration.exit_code, 2)
        self.assertEqual(infrastructure.verdict, Verdict.INFRA_ERROR)
        self.assertEqual(infrastructure.exit_code, 2)
        self.assertEqual(configuration.diagnostics, ())
        self.assertEqual(len(infrastructure.diagnostics), 1)
        self.assertIn("RuntimeError: transport failed", infrastructure.diagnostics[0])

    def test_quarantined_failure_is_visible_without_becoming_a_pass(self) -> None:
        def body(context) -> None:
            context.warn("known gateway timeout", category="gateway_timeout")

        result = execute_scenario(title="Runtime", body=body)

        self.assertEqual(result.verdict, Verdict.WARN)
        self.assertEqual(result.exit_code, 42)
        self.assertEqual(result.evidence["category"], "gateway_timeout")

    def test_scenario_timeout_still_runs_cleanup(self) -> None:
        trace: list[str] = []

        def body(context) -> None:
            context.scope.defer("cleanup", lambda: trace.append("cleanup"))
            until = time.monotonic() + 1
            while time.monotonic() < until:
                pass

        result = execute_scenario(
            title="Trace",
            body=body,
            timeout_seconds=0.01,
        )

        self.assertEqual(result.verdict, Verdict.INFRA_ERROR)
        self.assertIn("timed out after", result.summary)
        self.assertEqual(trace, ["cleanup"])

    def test_cleanup_ledger_stops_at_its_deadline(self) -> None:
        scope = ResourceScope()

        def hangs() -> None:
            until = time.monotonic() + 1
            while time.monotonic() < until:
                pass

        scope.defer("never returns", hangs, critical=True)
        started = time.monotonic()
        failures = scope.close(timeout_seconds=0.01)

        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].name, "never returns")
        self.assertIn("cleanup timed out", failures[0].detail)


class BackgroundWorkJoinIsAVerdict(unittest.TestCase):
    """The bench never hangs or kills a helper thread on a passing run, so
    the two properties a scenario relies on are pinned here."""

    def test_work_still_running_at_the_deadline_fails_by_name(self) -> None:
        import threading

        release = threading.Event()
        work = background("the stuck callers", release.wait)
        try:
            with self.assertRaisesRegex(InvariantViolation, "the stuck callers was still running"):
                work.join(timeout=0.05)
        finally:
            release.set()
            work.join(timeout=5)

    def test_a_dead_worker_re_raises_on_the_joining_thread(self) -> None:
        def die() -> None:
            raise RuntimeError("kubectl exploded")

        with self.assertRaisesRegex(RuntimeError, "kubectl exploded"):
            background("a lifter", die).join(timeout=5)



if __name__ == "__main__":
    unittest.main()

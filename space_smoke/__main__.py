"""Discover and run SPACE smoke scenarios."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any

from space_smoke.registry import Scenario
from space_smoke.runtime import SmokeConfigurationError, execute_scenario
from space_smoke.scenarios import REGISTRY
from space_smoke.services import MissingToolError, SmokeServices
from space_smoke.sharding import select_shard
from space_smoke.suite import (
    RESULT_PREFIX,
    ScenarioProcessResult,
    run_scenarios,
    suite_exit_code,
)


def _describe(scenario_id: str) -> int:
    scenario = REGISTRY.get(scenario_id)
    payload = {
        "id": scenario.id,
        "category": scenario.category,
        "title": scenario.title,
        "invariant": scenario.invariant,
        "fixture": scenario.fixture_name,
        "api_surfaces": sorted(scenario.api_surfaces),
        "quarantined": scenario.quarantined,
        "ignored": scenario.ignored,
        "selected_api_surface": os.environ.get("SPACE_SMOKE_API_SURFACE", "ecw"),
        "sandbox": {
            "count": scenario.sandbox_count,
            "template": scenario.sandbox.template,
            "timeout_seconds": scenario.sandbox.timeout,
            "lifecycle": (
                {
                    "on_timeout": scenario.sandbox.lifecycle.on_timeout,
                    "auto_resume": scenario.sandbox.lifecycle.auto_resume,
                }
                if scenario.sandbox.lifecycle is not None
                else None
            ),
            "network": (
                {
                    "allow_public_traffic": (
                        scenario.sandbox.network.allow_public_traffic
                    ),
                    "allow_out": list(scenario.sandbox.network.allow_out),
                    "deny_out": list(scenario.sandbox.network.deny_out),
                }
                if scenario.sandbox.network is not None
                else None
            ),
        },
        "timeout_minutes": scenario.timeout_minutes,
        "rerun_environment": list(scenario.rerun_environment),
        "templates": (
            sorted(scenario.templates) if scenario.templates is not None else None
        ),
    }
    print(json.dumps(payload, indent=2))
    return 0


def _run(scenario_id: str) -> int:
    scenario = REGISTRY.get(scenario_id)
    template = os.environ.get("E2B_TEMPLATE") or None
    # An empty template set opts a scenario out of implicit selection; naming
    # the scenario explicitly still runs it against the requested template.
    if scenario.templates != frozenset() and not scenario.applies_to(template):
        supported = ", ".join(sorted(scenario.templates or ()))
        print(
            f"ERROR: {scenario_id} applies to template(s) {supported}; "
            f"the selected template is {template}",
            file=sys.stderr,
        )
        return 2
    try:
        services = SmokeServices.from_environment(scenario)
    except MissingToolError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except SmokeConfigurationError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    result = execute_scenario(
        title=scenario.title,
        body=lambda context: scenario.run(context, services),
        timeout_seconds=scenario.timeout_minutes * 60,
    )
    print(f"{result.verdict.value.upper()}: {result.summary}", flush=True)
    for diagnostic in result.diagnostics:
        print("TRACEBACK:", file=sys.stderr, flush=True)
        print(diagnostic, file=sys.stderr, end="", flush=True)
    if result.exit_code != 0:
        print(f"RERUN: {_rerun_command(scenario)}", flush=True)
    print(
        RESULT_PREFIX
        + json.dumps(
            {
                "scenario_id": scenario_id,
                "verdict": result.verdict.value,
                "summary": result.summary,
                "evidence": result.evidence,
                "cleanup_failures": result.cleanup_failures,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return result.exit_code


def _rerun_command(scenario: Scenario[Any]) -> str:
    assignments = []
    names = dict.fromkeys(
        (
            "SPACE_SMOKE_BACKEND",
            "SPACE_SMOKE_API_SURFACE",
            "SPACE_SMOKE_SCENARIO_PACKAGES",
            "E2B_TEMPLATE",
            *scenario.rerun_environment,
        )
    )
    for name in names:
        value = os.environ.get(name)
        if value:
            assignments.append(f"{name}={shlex.quote(value)}")
    command = [
        "uv",
        "run",
        "--frozen",
        "--python",
        "3.11",
        "-m",
        "space_smoke",
        "run",
        scenario.id,
    ]
    return " ".join((*assignments, *(shlex.quote(part) for part in command)))


def _print_started(scenario_id: str) -> None:
    print(f"\n=== RUN {scenario_id} ===", flush=True)


def _print_line(line: str) -> None:
    print(line, end="", flush=True)


def _print_finished(result: ScenarioProcessResult) -> None:
    print(
        f"=== END {result.scenario_id} (exit {result.exit_code}) ===",
        flush=True,
    )


def _verdict_and_summary(result: ScenarioProcessResult) -> tuple[str, str]:
    report = result.report or {}
    verdict = report.get("verdict")
    if not isinstance(verdict, str):
        verdict = "fail" if result.exit_code == 1 else "infra_error"
    summary = report.get("summary")
    if not isinstance(summary, str):
        summary = f"scenario process exited {result.exit_code} without a result"
    return verdict, summary


def _cleanup_failures(result: ScenarioProcessResult) -> list[str]:
    failures = (result.report or {}).get("cleanup_failures")
    return [str(failure) for failure in failures] if isinstance(failures, list) else []


def _one_line(text: str) -> str:
    """A multi-line message (a failed command's captured stderr, say) on one line."""
    return " | ".join(line.strip() for line in text.splitlines() if line.strip())


def _failure_detail(result: ScenarioProcessResult) -> str:
    verdict, summary = _verdict_and_summary(result)
    detail = f"{result.scenario_id} [{verdict}]: {_one_line(summary)}"
    evidence = (result.report or {}).get("evidence")
    if isinstance(evidence, dict) and evidence.get("sandbox_id"):
        detail += f" (sandbox_id={evidence['sandbox_id']})"
    cleanup_failures = _cleanup_failures(result)
    if cleanup_failures:
        detail += f"; cleanup failures: {'; '.join(map(_one_line, cleanup_failures))}"
    return detail


# How much of a scenario's own output to repeat when it exited without a result.
OUTPUT_TAIL_LINES = 5


def _print_not_passed(
    results: tuple[ScenarioProcessResult, ...],
    scenarios: tuple[Scenario[Any], ...],
    *,
    api_surface: str,
) -> None:
    """Repeat every scenario that did not pass where the log ends.

    Each verdict is printed inside its scenario's own block, and a full
    inventory run is thousands of lines long, so otherwise the log ends on a
    count and the reader searches upward for which scenario failed and why.
    Under GitHub Actions each one is also an annotation, which the run page
    shows without opening the log.
    """
    not_passed = tuple(result for result in results if result.exit_code != 0)
    if not not_passed:
        return
    reruns = {scenario.id: _rerun_command(scenario) for scenario in scenarios}
    print(f"\nNOT PASSED on the {api_surface} API surface ({len(not_passed)}):")
    for result in not_passed:
        verdict, summary = _verdict_and_summary(result)
        print(f"  {verdict.upper()} {result.scenario_id}: {summary}")
        evidence = (result.report or {}).get("evidence")
        if isinstance(evidence, dict):
            phases = evidence.get("phases")
            if isinstance(phases, list) and phases and isinstance(phases[-1], dict):
                print(f"    last phase: {phases[-1].get('name')}")
            if evidence.get("sandbox_id"):
                print(f"    sandbox_id: {evidence['sandbox_id']}")
        for failure in _cleanup_failures(result):
            first, *rest = failure.splitlines() or [""]
            print(f"    cleanup failure: {first}")
            for line in rest:
                print(f"      {line}")
        if result.report is None:
            for line in result.stdout.splitlines()[-OUTPUT_TAIL_LINES:]:
                print(f"    | {line}")
        rerun = reruns.get(result.scenario_id)
        if rerun is not None:
            print(f"    rerun: {rerun}")
        if os.environ.get("GITHUB_ACTIONS") == "true":
            level = "warning" if result.exit_code == 42 else "error"
            title = f"typed smoke ({api_surface}): {result.scenario_id}"
            message = _failure_detail(result)
            if rerun is not None:
                message += f"\nrerun: {rerun}"
            print(
                f"::{level} title={_workflow_command_value(title, for_property=True)}"
                f"::{_workflow_command_value(message)}"
            )
    sys.stdout.flush()


def _workflow_command_value(text: str, *, for_property: bool = False) -> str:
    """Escape text for a GitHub Actions workflow command (`::error title=…::…`)."""
    escaped = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    if for_property:
        escaped = escaped.replace(":", "%3A").replace(",", "%2C")
    return escaped


def _write_failure_details(results: tuple[ScenarioProcessResult, ...]) -> None:
    path = os.environ.get("SPACE_SMOKE_FAILURE_DETAILS_PATH")
    failures = tuple(result for result in results if result.exit_code != 0)
    if not path or not failures:
        return
    try:
        Path(path).write_text(
            "\n".join(_failure_detail(result) for result in failures) + "\n",
            encoding="utf-8",
        )
    except OSError as error:
        print(f"WARN: could not write smoke failure details: {error}", file=sys.stderr)


def _run_all(
    *, category: str | None = None, shard: tuple[int, int] | None = None
) -> int:
    template = os.environ.get("E2B_TEMPLATE") or None
    backend = os.environ.get("SPACE_SMOKE_BACKEND", "kind")
    eligible = REGISTRY.select(category=category, template=template)
    # A quarantine is a visible hole in coverage, never a pass: name each one
    # and its reason so the run log shows what the suite did not prove.
    for held in REGISTRY.select(
        category=category, template=template, include_quarantined=True, include_ignored=True
    ):
        if held.quarantined is not None:
            print(f"QUARANTINED {held.id}: {held.quarantined}")
        if held.ignored is not None:
            print(f"IGNORED {held.id}: {held.ignored}")
    api_surface = os.environ.get("SPACE_SMOKE_API_SURFACE", "ecw")
    if category is None:
        eligible = tuple(
            scenario for scenario in eligible if api_surface in scenario.api_surfaces
        )
    unsupported = tuple(
        scenario for scenario in eligible if api_surface not in scenario.api_surfaces
    )
    if unsupported:
        print(
            f"ERROR: the {api_surface!r} API surface does not support: "
            + ", ".join(scenario.id for scenario in unsupported),
            file=sys.stderr,
        )
        return 2
    scenarios = tuple(scenario for scenario in eligible if backend in scenario.backends)
    # Backend-inapplicable scenarios are a visible skip on an implicit
    # selection — a skip is not a pass, and hiding it would read as coverage.
    for skipped in eligible:
        if skipped not in scenarios:
            backends = ", ".join(sorted(skipped.backends))
            print(f"SKIPPED {skipped.id}: runs only on backend(s) {backends}")
    if shard is not None:
        index, count = shard
        # Partition the eligible inventory, not execution: scenarios mutate
        # cluster-wide state and must remain serial within an isolated shard.
        scenarios = select_shard(
            scenarios, index=index, count=count, api_surface=api_surface
        )
        print(f"SHARD {index}/{count}: {len(scenarios)} scenarios ({api_surface})")
    if not scenarios:
        selection = f"category {category!r}" if category is not None else "this run"
        print(
            f"ERROR: no smoke scenarios apply to {selection} on the "
            f"{backend!r} backend.",
            file=sys.stderr,
        )
        return 2

    try:
        required_tools = tuple(
            dict.fromkeys(
                tool for scenario in scenarios for tool in scenario.required_tools
            )
        )
        services = SmokeServices.from_environment(
            scenarios[0], required_tools=required_tools
        )
        services.bootstrap()
    except MissingToolError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except SmokeConfigurationError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    except Exception as error:  # noqa: BLE001 - suite bootstrap boundary
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        return 2

    environment = dict(os.environ)
    if environment.get("SPACE_SMOKE_BACKEND", "kind") == "kind":
        environment["SKIP_BOOTSTRAP"] = "1"
    label = f" {category}" if category is not None else ""
    print(f"Running {len(scenarios)}{label} smoke scenarios sequentially.")
    results = run_scenarios(
        scenarios,
        environment=environment,
        on_start=_print_started,
        line_sink=_print_line,
        on_result=_print_finished,
    )
    _write_failure_details(results)

    passed = sum(result.exit_code == 0 for result in results)
    warned = sum(result.exit_code == 42 for result in results)
    failed = sum(result.exit_code == 1 for result in results)
    incomplete = len(results) - passed - warned - failed
    print(
        f"SUITE: {passed} passed, {warned} warned, {failed} failed, "
        f"{incomplete} inconclusive or infrastructure errors"
    )
    _print_not_passed(results, scenarios, api_surface=api_surface)
    return suite_exit_code(results)


def _parse_shard(value: str) -> tuple[int, int]:
    try:
        index, count = (int(part) for part in value.split("/"))
        if 1 <= index <= count:
            return index, count
    except ValueError:
        pass
    raise argparse.ArgumentTypeError(
        "shard must be INDEX/COUNT with 1 <= INDEX <= COUNT"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list")
    describe = subparsers.add_parser("describe")
    describe.add_argument("scenario")
    run = subparsers.add_parser("run")
    run.add_argument("scenario", nargs="?")
    run.add_argument("--all", action="store_true", dest="run_all")
    run.add_argument("--category", choices=REGISTRY.categories)
    run.add_argument(
        "--shard",
        type=_parse_shard,
        metavar="INDEX/COUNT",
        help="run one duration-balanced partition on an isolated backend (1-based)",
    )
    args = parser.parse_args()

    if args.command == "list":
        for scenario in REGISTRY:
            print(f"{scenario.id}\t{scenario.invariant}")
        return 0
    if args.command == "describe":
        return _describe(args.scenario)
    selectors = sum(
        (
            args.scenario is not None,
            args.run_all,
            args.category is not None,
        )
    )
    if selectors != 1:
        parser.error("run requires exactly one scenario, --all, or --category")
    if args.shard is not None and args.scenario is not None:
        parser.error("--shard requires --all or --category")
    if args.run_all:
        return _run_all(shard=args.shard)
    if args.category is not None:
        return _run_all(category=args.category, shard=args.shard)
    assert args.scenario is not None
    return _run(args.scenario)


if __name__ == "__main__":
    raise SystemExit(main())

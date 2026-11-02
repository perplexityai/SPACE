"""Sequential process runner for discovered smoke scenarios."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple

from space_smoke.registry import Scenario

ProcessRunner = Callable[..., subprocess.CompletedProcess[str]]
LineSink = Callable[[str], None]
ScenarioStarted = Callable[[str], None]
RESULT_PREFIX = "RESULT: "
# Names a lane's known-failures list (load_known_failures). Only a runner
# that sets it, as the Kind Direct lane does, excuses the scenarios listed.
KNOWN_FAILURES_ENV = "SPACE_SMOKE_KNOWN_FAILURES"


@dataclass(frozen=True)
class ScenarioProcessResult:
    scenario_id: str
    returncode: int
    stdout: str
    stderr: str
    report: Mapping[str, Any] | None = None

    @property
    def exit_code(self) -> int:
        return self.returncode if self.returncode in {0, 1, 2, 42} else 2

    @property
    def last_phase(self) -> str | None:
        """The name of the last phase the scenario recorded, or None.

        Phases are recorded as they end. For a failure inside a phase this is
        that phase (the outermost one when phases nest); for a failure outside
        any phase it is the last phase that ended before it.
        """

        evidence = (self.report or {}).get("evidence")
        phases = evidence.get("phases") if isinstance(evidence, Mapping) else None
        if isinstance(phases, list) and phases and isinstance(phases[-1], Mapping):
            name = phases[-1].get("name")
            return name if isinstance(name, str) else None
        return None


class KnownFailure(NamedTuple):
    """A known-failures entry: the phase a scenario fails in, and why."""

    phase: str
    reason: str


def run_scenarios(
    scenarios: Sequence[Scenario[Any]],
    *,
    environment: Mapping[str, str] | None = None,
    executable: str = sys.executable,
    process_runner: ProcessRunner | None = None,
    on_start: ScenarioStarted | None = None,
    line_sink: LineSink | None = None,
    on_result: Callable[[ScenarioProcessResult], None] | None = None,
) -> tuple[ScenarioProcessResult, ...]:
    """Run each scenario in its own process, one scenario at a time."""

    values = dict(os.environ if environment is None else environment)
    results: list[ScenarioProcessResult] = []
    for registered in scenarios:
        if on_start is not None:
            on_start(registered.id)
        result = _run_one(
            registered.id,
            environment=values,
            executable=executable,
            process_runner=process_runner,
            line_sink=line_sink,
        )
        results.append(result)
        if on_result is not None:
            on_result(result)
    return tuple(results)


def suite_exit_code(results: Sequence[ScenarioProcessResult]) -> int:
    """Return one stable exit status for a complete suite."""

    if any(result.exit_code == 1 for result in results):
        return 1
    if any(result.exit_code not in {0, 42} for result in results):
        return 2
    if any(result.exit_code == 42 for result in results):
        return 42
    return 0


def load_known_failures(
    path: Path, scenario_ids: Collection[str]
) -> dict[str, KnownFailure]:
    """Read a lane's known-failures list: scenario id to its listed phase and reason.

    One `<scenario id> | <phase> | <reason>` per line; blank lines and `#`
    comments are ignored. A list the runner cannot trust fails it rather than
    excusing the wrong scenarios: an id no scenario has (a rename leaves it
    behind), an entry without a phase or a reason, or an id listed twice
    raises ValueError naming the line.
    """

    listed: dict[str, KnownFailure] = {}
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        fields = [field.strip() for field in entry.split("|", 2)]
        scenario_id, phase, reason = (*fields, "", "")[:3]
        where = f"{path}:{number}"
        if not scenario_id or not phase:
            raise ValueError(
                f"{where}: an entry needs a scenario id and the phase it fails in: "
                "`<scenario id> | <phase> | <reason>`"
            )
        if not reason:
            raise ValueError(f"{where}: {scenario_id} needs a reason")
        if scenario_id not in scenario_ids:
            raise ValueError(f"{where}: no registered scenario is named {scenario_id}")
        if scenario_id in listed:
            raise ValueError(f"{where}: {scenario_id} is listed twice")
        listed[scenario_id] = KnownFailure(phase, reason)
    return listed


def is_known_failure(
    result: ScenarioProcessResult, known_failures: Mapping[str, KnownFailure]
) -> bool:
    """Whether a known-failures list excuses this result.

    Only a fail verdict (exit code 1) whose last recorded phase is the listed
    phase is excused, together with any cleanup failures it reports: a fail
    verdict keeps its verdict when its cleanup fails. Any other result of a
    listed scenario (another last phase, an infrastructure error, a timeout, a
    crash, a warning) counts as it would unlisted.
    """

    listed = known_failures.get(result.scenario_id)
    return (
        listed is not None
        and result.exit_code == 1
        and result.last_phase == listed.phase
    )


def _run_one(
    scenario_id: str,
    *,
    environment: Mapping[str, str],
    executable: str,
    process_runner: ProcessRunner | None,
    line_sink: LineSink | None,
) -> ScenarioProcessResult:
    command = [executable, "-m", "space_smoke", "run", scenario_id]
    if process_runner is None and line_sink is not None:
        return _run_one_streamed(
            scenario_id,
            command=command,
            environment=environment,
            line_sink=line_sink,
        )

    runner = process_runner or subprocess.run
    try:
        completed = runner(
            command,
            env=dict(environment),
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as error:
        return ScenarioProcessResult(
            scenario_id, 2, "", f"{type(error).__name__}: {error}"
        )
    if line_sink is not None:
        if completed.stdout:
            line_sink(completed.stdout)
        if completed.stderr:
            line_sink(completed.stderr)
    return ScenarioProcessResult(
        scenario_id,
        completed.returncode,
        completed.stdout,
        completed.stderr,
        _parse_report(completed.stdout),
    )


def _run_one_streamed(
    scenario_id: str,
    *,
    command: list[str],
    environment: Mapping[str, str],
    line_sink: LineSink,
) -> ScenarioProcessResult:
    """Tee one scenario's merged output while retaining its structured result."""

    try:
        process = subprocess.Popen(
            command,
            env=dict(environment),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
    except OSError as error:
        detail = f"{type(error).__name__}: {error}"
        line_sink(f"{detail}\n")
        return ScenarioProcessResult(scenario_id, 2, "", detail)

    assert process.stdout is not None
    lines: list[str] = []
    with process.stdout:
        for line in process.stdout:
            lines.append(line)
            line_sink(line)
    returncode = process.wait()
    stdout = "".join(lines)
    return ScenarioProcessResult(
        scenario_id,
        returncode,
        stdout,
        "",
        _parse_report(stdout),
    )


def _parse_report(stdout: str) -> Mapping[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        if not line.startswith(RESULT_PREFIX):
            continue
        try:
            payload = json.loads(line.removeprefix(RESULT_PREFIX))
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None
    return None

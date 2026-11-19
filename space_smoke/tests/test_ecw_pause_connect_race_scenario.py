"""Characterization tests for repeated ECW pause/connect coverage."""

from __future__ import annotations

import unittest
from typing import cast

from space_smoke.capabilities import (
    EcwCommandResult,
    EcwSandbox,
    EcwSandboxInfo,
    EcwSandboxState,
)
from space_smoke.fixtures import Ecw
from space_smoke.model import Verdict
from space_smoke.runtime import ResourceScope, ScenarioContext, execute_scenario
from space_smoke.scenarios.ecw_pause_connect_race import (
    ITERATIONS,
    MARKER_PATH,
    SANDBOX_TIMEOUT_SECONDS,
    pause_connect_race,
)


class _RaceSandbox:
    sandbox_id = "sandbox-pause-race"

    def __init__(self, *, corrupt_iteration: int | None = None) -> None:
        self.trace: list[str] = []
        self.state = EcwSandboxState.RUNNING
        self.marker = ""
        self.iteration = 0
        self.corrupt_iteration = corrupt_iteration

    def run(self, command: str, *, timeout: int = 60) -> EcwCommandResult:
        self.trace.append(f"run:{command}:{timeout}")
        if command.startswith("printf %s "):
            marker, path = command.removeprefix("printf %s ").split(" > ", 1)
            if path != MARKER_PATH:
                raise AssertionError(f"unexpected marker path: {path}")
            self.marker = marker
            self.iteration += 1
            return EcwCommandResult(0, "", "")
        if command == f"cat {MARKER_PATH}":
            marker = self.marker
            if self.iteration == self.corrupt_iteration:
                marker = "corrupt"
            return EcwCommandResult(0, marker, "")
        raise AssertionError(f"unexpected command: {command}")

    def pause(self) -> None:
        self.trace.append("pause")
        self.state = EcwSandboxState.PAUSED

    def connect(self, *, timeout: int = 300) -> _RaceSandbox:
        self.trace.append(f"connect:{timeout}")
        self.state = EcwSandboxState.RUNNING
        return self

    def try_get_info(self) -> EcwSandboxInfo | None:
        self.trace.append("try_get_info")
        return EcwSandboxInfo(self.sandbox_id, "seed", self.state)


def _fixture(sandbox: _RaceSandbox) -> Ecw:
    return Ecw(
        ScenarioContext(ResourceScope(), {}),
        cast(EcwSandbox, sandbox),
        _sandbox_timeout=SANDBOX_TIMEOUT_SECONDS,
    )


class EcwPauseConnectRaceScenarioTests(unittest.TestCase):
    def test_ten_cycles_reconnect_immediately_and_preserve_each_marker(self) -> None:
        sandbox = _RaceSandbox()

        pause_connect_race.verify(_fixture(sandbox))

        self.assertEqual(pause_connect_race.sandbox.timeout, SANDBOX_TIMEOUT_SECONDS)
        self.assertEqual(sandbox.trace.count("pause"), ITERATIONS)
        self.assertEqual(
            sandbox.trace.count(f"connect:{SANDBOX_TIMEOUT_SECONDS}"),
            ITERATIONS,
        )
        self.assertEqual(len(sandbox.trace), ITERATIONS * 4)
        for iteration in range(1, ITERATIONS + 1):
            marker = f"space-smoke-pause-connect:{sandbox.sandbox_id}:{iteration}"
            offset = (iteration - 1) * 4
            self.assertEqual(
                sandbox.trace[offset : offset + 4],
                [
                    f"run:printf %s {marker} > {MARKER_PATH}:10",
                    "pause",
                    f"connect:{SANDBOX_TIMEOUT_SECONDS}",
                    f"run:cat {MARKER_PATH}:10",
                ],
            )

    def test_failure_evidence_names_the_exact_cycle_and_mismatched_marker(self) -> None:
        sandbox = _RaceSandbox(corrupt_iteration=2)

        result = execute_scenario(
            title="Pause connect race",
            body=lambda context: pause_connect_race.verify(
                Ecw(
                    context,
                    cast(EcwSandbox, sandbox),
                    _sandbox_timeout=SANDBOX_TIMEOUT_SECONDS,
                )
            ),
        )

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.summary, "sandbox command result did not match")
        phases = result.evidence["phases"]
        self.assertIsInstance(phases, list)
        assert isinstance(phases, list)
        self.assertEqual(
            phases[-1]["name"],
            f"cycle 2/{ITERATIONS}: pause, reconnect, and read marker",
        )
        self.assertEqual(result.evidence["actual"], (0, "corrupt"))


if __name__ == "__main__":
    unittest.main()

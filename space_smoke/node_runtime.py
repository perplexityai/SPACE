"""Opt-in fixture for a node runtime's bounded local JSON request protocol."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any, ClassVar

from space_smoke.runtime import ScenarioContext


@dataclass(frozen=True)
class NodeRuntimeFixture:
    """No VM or ECW sandbox is implicitly allocated by this fixture."""

    fixture_name: ClassVar[str] = "node-runtime"
    timeout_minutes: ClassVar[int] = 3
    context: ScenarioContext
    command: tuple[str, ...]

    def request(self, request: dict[str, Any]) -> dict[str, Any]:
        completed = subprocess.run(
            self.command,
            input=json.dumps(request),
            text=True,
            capture_output=True,
            check=True,
            timeout=150,
        )
        response = json.loads(completed.stdout)
        if not isinstance(response, dict):
            raise ValueError("node runtime response must be a JSON object")
        return response

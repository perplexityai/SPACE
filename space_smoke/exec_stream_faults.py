"""ECW process streams through faults on the data plane's node-api hop.

An ECW `Process.Start` answers HTTP 200 once the data plane has the guest's
stream open, so a fault after that is one the client cannot retry around: the
data plane must carry the same client stream across it. The data plane keeps
one idempotency key per command (the client's `Idempotency-Key`, or one it
generates for the request) and one deadline (the client's
`Connect-Timeout-Ms`) for every reopen, so the guest replays its record of the
command instead of running it again.

Each scenario runs a command that appends a line to a marker file before
anything else. The marker's line count is how many times the guest ran it.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    EcwProcessStartStream,
    EcwProcessStreamEvent,
    ExecStreamFaultControl,
)
from space_smoke.runtime import ResourceScope, SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# The client gives up on a stream that has read nothing for this long.
READ_TIMEOUT_SECONDS = 90.0


@dataclass
class OpenCommand:
    """A command's client stream and what the client has read from it."""

    stream: EcwProcessStartStream
    opened_at: float
    events: list[EcwProcessStreamEvent] = field(default_factory=list)
    _iterator: Iterator[EcwProcessStreamEvent] | None = None

    def read_until(self, done: Callable[[OpenCommand], bool]) -> None:
        if self._iterator is None:
            self._iterator = self.stream.events()
        for event in self._iterator:
            self.events.append(event)
            if done(self):
                return

    def read_to_end(self) -> None:
        self.read_until(lambda _command: False)
        self.stream.close()

    @property
    def stdout(self) -> str:
        return b"".join(event.data for event in self.events if event.kind == "stdout").decode(
            errors="replace"
        )

    @property
    def ends(self) -> list[EcwProcessStreamEvent]:
        return [event for event in self.events if event.kind == "end"]

    @property
    def errors(self) -> list[str]:
        return [event.error or "" for event in self.events if event.kind == "error"]

    def summary(self) -> dict[str, object]:
        return {
            "http_status": self.stream.http_status,
            "stdout": self.stdout[-2_000:],
            "starts": [event.pid for event in self.events if event.kind == "start"],
            "ends": [
                {
                    "exit_code": event.exit_code,
                    "status": event.status,
                    "after_seconds": round(event.at - self.opened_at, 2),
                }
                for event in self.ends
            ],
            "errors": self.errors,
        }


class ExecStreamFaults:
    def __init__(
        self, test: KindEcw, control: ExecStreamFaultControl, scope: ResourceScope
    ) -> None:
        self._test = test
        self._control = control
        self._scope = scope
        self.marker = f"/tmp/space-smoke-exec-{secrets.token_hex(6)}"

    def node(self) -> str:
        placement = self._control.sandbox_pod_placement(self._test.sandbox.sandbox_id)
        if placement is None:
            raise SmokeConfigurationError("the sandbox has no bound pod")
        self._test.record("sandbox_node", placement.node_name)
        return placement.node_name

    def command(self, body: str) -> str:
        """`body`, run after one line is appended to the marker."""

        return f"echo run >> {self.marker}; {body}"

    def start(
        self, command: str, *, timeout_ms: int, idempotency_key: str | None = None
    ) -> OpenCommand:
        opened_at = time.monotonic()
        stream = self._test.compatibility.open_process_start(
            command,
            timeout_ms=timeout_ms,
            idempotency_key=idempotency_key,
            request_timeout=READ_TIMEOUT_SECONDS,
        )
        self._test.verify(
            stream.http_status == 200,
            "Process.Start did not answer 200",
            http_status=stream.http_status,
        )
        return OpenCommand(stream, opened_at)

    def cut_backend_streams(self, node: str) -> None:
        cut = self._control.cut_node_api_streams(node)
        self._test.record("node_api_connections_cut", cut)
        self._test.verify(cut >= 1, "no data plane connection to node-api was open to cut")

    @contextmanager
    def frozen_backend(self, node: str) -> Iterator[None]:
        """node-api stopped for the block. Continuing it is armed before the
        stop, so a failure anywhere still continues it at cleanup."""

        resume = self._scope.defer(
            f"continue node-api on {node}",
            lambda: self._control.continue_node_api(node),
            critical=True,
        )
        try:
            self._control.stop_node_api(node)
            yield
        finally:
            resume.release()

    def replace_backend(self, node: str, *, grace_seconds: int) -> None:
        started = time.monotonic()
        pod = self._control.replace_node_api(node, grace_seconds=grace_seconds, timeout=120)
        self._test.record("replacement_node_api_pod", pod)
        self._test.record("node_api_replacement_seconds", round(time.monotonic() - started, 1))

    def runs(self) -> int:
        result = self._test.run_command_when_ready(f"wc -l < {self.marker}", timeout=30)
        return int(result.stdout.strip() or "0")

    def expect_one_run(self) -> None:
        runs = self.runs()
        self._test.verify(runs == 1, "the guest ran the command more than once", runs=runs)

    def expect_completed(
        self, command: OpenCommand, *, stdout: str, exit_code: int, status: str = "exited"
    ) -> EcwProcessStreamEvent:
        """The client read `stdout` exactly, then one exit, then a clean end."""

        evidence = command.summary()
        self._test.verify(
            not command.errors and command.stdout == stdout and len(command.ends) == 1,
            "the client stream did not carry the command's output once and its exit",
            expected_stdout=stdout,
            **evidence,
        )
        end = command.ends[0]
        self._test.verify(
            end.exit_code == exit_code and (end.status or "exited") == status,
            "the client stream ended with the wrong exit",
            expected_exit_code=exit_code,
            expected_status=status,
            **evidence,
        )
        self._test.record("client_stream", evidence)
        return end


def ticks(count: int) -> str:
    return "".join(f"tick-{index}\n" for index in range(1, count + 1))


def tick_command(count: int, *, interval: float, then: str) -> str:
    return f"for i in $(seq 1 {count}); do echo tick-$i; sleep {interval}; done; {then}"

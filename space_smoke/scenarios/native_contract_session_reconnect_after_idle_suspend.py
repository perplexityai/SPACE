"""A command attached while its sandbox ran, then reattached after an idle
suspend, wakes the sandbox only when its wake policy allows access: the node
judges a reconnect to a process that already started as access, not as a
command."""
import base64
import time

from space_smoke.native_hardening import NativeEvents, NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_contract_command_wakes_idle_suspended_sandbox import (
    OUTPUT_BUDGET_SECONDS, POLICY, SUSPEND_BUDGET_SECONDS, _retry_delay,
)

# Prints once, then a tick a second for as long as the sandbox runs it, so a
# reattach after the suspend reads output only the restored process wrote.
TICKER = ["sh", "-c", "printf start; while sleep 1; do printf .; done"]
# Long enough to outlast the idle suspend and the restore.
TICKER_TIMEOUT_SECONDS = 1800


def _stdout(events: NativeEvents) -> bytes:
    """The stdout an attach read; a reattach may also carry resync frames."""
    return b"".join(
        base64.b64decode(str(event.data.get("data_base64", "")), validate=True)
        for event in events.events
        if event.data.get("type") == "stdout"
    )


def _started(test: NativeHardeningFixture, wake_on: list[str]) -> tuple[str, dict[str, object], str]:
    """A sandbox under `wake_on` whose command was attached, started and left
    running: its id, the command, and the last event id the attach read."""
    sandbox_id = test.create_ready({"auto_lifecycle": {**POLICY, "wake": {"on": wake_on}}})
    command = test.command(sandbox_id, TICKER, timeout_seconds=TICKER_TIMEOUT_SECONDS)
    command.require(201)
    first = test.events(command.body, timeout=60, max_events=5)
    stdout = _stdout(first)
    test.context.verify(
        first.status == 200 and stdout.startswith(b"start") and first.events[-1].id is not None,
        "the first attach did not start the command",
        wake_on=wake_on, status=first.status, stdout=stdout.decode(errors="replace"),
    )
    return sandbox_id, command.body, str(first.events[-1].id)


def _not_running(sandbox_id: str, events: NativeEvents) -> bool:
    error = events.body.get("error")
    return (
        events.status == 409
        and isinstance(error, dict)
        and error.get("code") == "conflict"
        and error.get("retryable") is False
        and error.get("details") == [
            {"key": "reason", "value": "sandbox_not_running"},
            {"key": "state", "value": "suspended"},
            {"key": "wake", "value": f"POST /v1/sandboxes/{sandbox_id}:wake"},
        ]
    )


@scenario(
    "native_contract.session_reconnect_after_idle_suspend",
    api_surfaces=("native",),
)
def session_reconnect_after_idle_suspend(test: NativeHardeningFixture) -> None:
    """Under `wake.on=["access"]` the reattach wakes the sandbox and reads the
    running command's later output; under `wake.on=["command"]` it is the
    final `sandbox_not_running` conflict, and the sandbox stays suspended."""
    access_id, access_command, access_last = _started(test, ["access"])
    command_id, command_command, command_last = _started(test, ["command"])
    test.wait_sandbox(access_id, "suspended", timeout=SUSPEND_BUDGET_SECONDS)
    test.wait_sandbox(command_id, "suspended", timeout=SUSPEND_BUDGET_SECONDS)

    refused: list[dict[str, object]] = []
    test.context.record("command_policy_attaches", refused)
    for attempt in range(2):
        events = test.events(command_command, timeout=30, last_event_id=command_last)
        refused.append({"status": events.status, "error": events.body.get("error")})
        test.context.verify(
            _not_running(command_id, events),
            "a reconnect under a command-only wake policy was not the final conflict",
            attempt=attempt,
        )
    sandbox = test.request("GET", f"/v1/sandboxes/{command_id}")
    test.context.verify(
        sandbox.body.get("state") == "suspended",
        "a reconnect woke a sandbox whose policy wakes only for a command",
        state=sandbox.body.get("state"),
    )

    deadline = time.monotonic() + OUTPUT_BUDGET_SECONDS
    woken: list[dict[str, object]] = []
    test.context.record("access_policy_attaches", woken)
    while True:
        events = test.events(
            access_command, timeout=min(65, max(1, deadline - time.monotonic())),
            last_event_id=access_last, max_events=3,
        )
        error = events.body.get("error")
        woken.append({"status": events.status, "code": error.get("code") if isinstance(error, dict) else None})
        delay = _retry_delay(events)
        if events.status == 200 or delay is None or time.monotonic() + delay >= deadline:
            break
        time.sleep(delay)
    stdout = _stdout(events)
    test.context.verify(
        events.status == 200 and b"." in stdout,
        "a reconnect under an access wake policy never read the restored command",
        status=events.status, error=events.body, stdout=stdout.decode(errors="replace"),
    )

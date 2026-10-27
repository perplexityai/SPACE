"""A command wakes a sandbox that idle-suspended under an access wake policy."""
import time

from space_smoke.native_concurrency import command_output
from space_smoke.native_hardening import NativeEvents, NativeHardeningFixture
from space_smoke.registry import scenario

MARKER = "space-smoke-command-wake-ok"
IDLE_SECONDS = 60
# The idle deadline is checked on reconciler ticks, and the suspend uploads a
# snapshot before the sandbox reports suspended.
SUSPEND_BUDGET_SECONDS = 300
# Restoring onto a node and starting the command, retried as the attach says.
OUTPUT_BUDGET_SECONDS = 180
POLICY = {
    "wake": {"on": ["access"]},
    "condition_sets": {
        "access_idle": {"kind": "inactivity", "activity_source": "access"},
    },
    "stages": [
        {"condition_set": "access_idle", "at": f"{IDLE_SECONDS}s", "action": "suspend"},
    ],
}


def _header(headers: dict[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name.lower()), None)


def _retry_delay(events: NativeEvents) -> float | None:
    """The attach's own retry advice, or None when it says not to retry."""
    error = events.body.get("error")
    if not isinstance(error, dict) or error.get("retryable") is not True:
        return None
    try:
        return min(float(_header(events.headers, "Retry-After") or "1"), 5.0)
    except ValueError:
        return 1.0


@scenario(
    "native_contract.command_wakes_idle_suspended_sandbox",
    api_surfaces=("native",),
)
def command_wakes_idle_suspended_sandbox(test: NativeHardeningFixture) -> None:
    """With `wake.on=["access"]`, a command sent after an idle suspend runs
    without a wake or any other call first."""
    sandbox_id = test.create_ready({"auto_lifecycle": POLICY})
    test.wait_sandbox(sandbox_id, "suspended", timeout=SUSPEND_BUDGET_SECONDS)

    command = test.command(sandbox_id, ["printf", MARKER])
    test.context.verify(
        command.status == 201,
        "a command the wake policy allows was refused",
        status=command.status, code=command.body.get("code"), retryable=command.body.get("retryable"),
    )
    deadline = time.monotonic() + OUTPUT_BUDGET_SECONDS
    attaches: list[dict[str, object]] = []
    test.context.record("attaches", attaches)
    while True:
        events = test.events(command.body, timeout=min(65, max(1, deadline - time.monotonic())))
        error = events.body.get("error")
        attaches.append({
            "status": events.status,
            "code": error.get("code") if isinstance(error, dict) else None,
            "retry_after": _header(events.headers, "Retry-After"),
        })
        delay = _retry_delay(events)
        if events.status == 200 or delay is None or time.monotonic() + delay >= deadline:
            break
        time.sleep(delay)
    test.context.verify(
        events.status == 200,
        "the command's attach never reached the woken sandbox",
        status=events.status, error=events.body,
    )
    stdout, _ = command_output(events.events, limit=1024)
    test.context.verify(stdout == MARKER.encode(), "the woken sandbox ran the wrong output",
                        stdout=stdout.decode(errors="replace"))

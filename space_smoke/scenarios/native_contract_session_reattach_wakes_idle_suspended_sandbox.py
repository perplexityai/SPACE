"""A session created while its sandbox ran wakes it when reattached after an
idle suspend, under an access wake policy."""
import time
from urllib.parse import quote, urljoin, urlsplit

from space_smoke.native_concurrency import command_output
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_contract_command_wakes_idle_suspended_sandbox import (
    OUTPUT_BUDGET_SECONDS, POLICY, SUSPEND_BUDGET_SECONDS, _retry_delay,
)
from space_smoke.scenarios.native_contract_process_session_wakes_idle_suspended_sandbox import (
    _attach,
)

MARKER = "space-smoke-session-reattach-ok"


@scenario(
    "native_contract.session_reattach_wakes_idle_suspended_sandbox",
    api_surfaces=("native",),
)
def session_reattach_wakes_idle_suspended_sandbox(test: NativeHardeningFixture) -> None:
    """With `wake.on=["access"]`, a command attached over SSE and a process
    session attached over WebSocket, both created while the sandbox ran,
    each wake it after an idle suspend and deliver their output."""
    sandbox_id = test.create_ready({"auto_lifecycle": POLICY})
    path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}"
    command = test.command(sandbox_id, ["printf", MARKER])
    session = test.request(
        "POST", f"{path}/process-sessions",
        body={"command": ["printf", MARKER]}, idempotency_key=test.new_key("process-session"),
    )
    test.context.verify(
        command.status == 201 and session.status == 201,
        "sessions on a running sandbox were refused",
        command_status=command.status, session_status=session.status,
    )
    test.wait_sandbox(sandbox_id, "suspended", timeout=SUSPEND_BUDGET_SECONDS)

    deadline = time.monotonic() + OUTPUT_BUDGET_SECONDS
    sse: list[dict[str, object]] = []
    test.context.record("sse_attaches", sse)
    while True:
        events = test.events(command.body, timeout=min(65, max(1, deadline - time.monotonic())))
        error = events.body.get("error")
        sse.append({"status": events.status, "code": error.get("code") if isinstance(error, dict) else None})
        delay = _retry_delay(events)
        if events.status == 200 or delay is None or time.monotonic() + delay >= deadline:
            break
        time.sleep(delay)
    test.context.verify(events.status == 200, "the command's reattach never reached the woken sandbox",
                        status=events.status, error=events.body)
    stdout, _ = command_output(events.events, limit=1024)
    test.context.verify(stdout == MARKER.encode(), "the command's reattach ran the wrong output",
                        stdout=stdout.decode(errors="replace"))

    # The WebSocket reattach starts from rest too, not from the sandbox the
    # SSE reattach just woke.
    test.wait_sandbox(sandbox_id, "suspended", timeout=SUSPEND_BUDGET_SECONDS)
    deadline = time.monotonic() + OUTPUT_BUDGET_SECONDS
    base = test.config.data_plane_url.rstrip("/") + "/"
    attach = session.body["attach"]
    url = urljoin(base, str(attach["url"]))
    test.context.verify(urlsplit(url).netloc == urlsplit(base).netloc,
                        "the attach URL left the data-plane origin")
    url = url.replace("http://", "ws://", 1).replace("https://", "wss://", 1)
    websocket: list[object] = []
    test.context.record("websocket_attaches", websocket)
    output: list[str] = []
    while time.monotonic() < deadline:
        output, frames = _attach(url, str(attach["token"]), deadline - time.monotonic())
        websocket.append(frames)
        last = frames[-1] if frames else {}
        if last.get("type") == "process_exited" or last.get("retryable") is not True:
            break
        time.sleep(2)
    test.context.verify("".join(output) == MARKER, "the process session's reattach never ran",
                        stdout_bytes=len("".join(output)))

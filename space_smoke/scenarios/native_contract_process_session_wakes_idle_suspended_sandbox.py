"""A WebSocket process session wakes a sandbox that idle-suspended under an
access wake policy."""
import base64
import json
import time
from urllib.parse import quote, urljoin, urlsplit

from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_contract_command_wakes_idle_suspended_sandbox import (
    OUTPUT_BUDGET_SECONDS, POLICY, SUSPEND_BUDGET_SECONDS,
)

MARKER = "space-smoke-process-session-wake-ok"


def _attach(url: str, token: str, budget: float) -> tuple[list[str], list[dict[str, object]]]:
    """One WebSocket attachment: the frame types seen and any error frames.
    Stdout is decoded into the returned list, never logged."""
    # Imported here so the registry imports without the smoke-only client:
    # inventories such as the quarantine backlog load every scenario module.
    from websockets.exceptions import ConnectionClosed
    from websockets.sync.client import connect

    stdout: list[str] = []
    frames: list[dict[str, object]] = []
    with connect(url, subprotocols=[token], open_timeout=min(30, budget)) as socket:
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            try:
                frame = json.loads(socket.recv(timeout=max(1, deadline - time.monotonic())))
            except (ConnectionClosed, TimeoutError):
                break
            kind = frame.get("type")
            if kind == "stdout":
                stdout.append(base64.b64decode(str(frame.get("data_base64", ""))).decode())
            elif kind in {"error", "process_exited", "closed"}:
                frames.append({key: frame.get(key) for key in ("type", "code", "retryable", "exit_code")})
                break
    return stdout, frames


@scenario(
    "native_contract.process_session_wakes_idle_suspended_sandbox",
    api_surfaces=("native",),
)
def process_session_wakes_idle_suspended_sandbox(test: NativeHardeningFixture) -> None:
    """With `wake.on=["access"]`, a process session created after an idle
    suspend and attached over WebSocket runs without a wake first."""
    sandbox_id = test.create_ready({"auto_lifecycle": POLICY})
    test.wait_sandbox(sandbox_id, "suspended", timeout=SUSPEND_BUDGET_SECONDS)

    session = test.request(
        "POST", f"/v1/sandboxes/{quote(sandbox_id, safe='')}/process-sessions",
        body={"command": ["printf", MARKER]}, idempotency_key=test.new_key("process-session"),
    )
    test.context.verify(
        session.status == 201,
        "a process session the wake policy allows was refused",
        status=session.status, code=session.body.get("code"), retryable=session.body.get("retryable"),
    )
    attach = session.body.get("attach")
    test.context.verify(isinstance(attach, dict), "the process session has no attach info")
    base = test.config.data_plane_url.rstrip("/") + "/"
    url = urljoin(base, str(attach["url"]))
    test.context.verify(urlsplit(url).netloc == urlsplit(base).netloc,
                        "the attach URL left the data-plane origin")
    url = url.replace("http://", "ws://", 1).replace("https://", "wss://", 1)

    deadline = time.monotonic() + OUTPUT_BUDGET_SECONDS
    attaches: list[object] = []
    test.context.record("attaches", attaches)
    stdout: list[str] = []
    while time.monotonic() < deadline:
        stdout, frames = _attach(url, str(attach["token"]), deadline - time.monotonic())
        attaches.append(frames)
        last = frames[-1] if frames else {}
        if last.get("type") == "process_exited" or last.get("retryable") is not True:
            break
        time.sleep(2)
    test.context.verify(
        "".join(stdout) == MARKER,
        "the process session never ran on the woken sandbox",
        stdout_bytes=len("".join(stdout)),
    )

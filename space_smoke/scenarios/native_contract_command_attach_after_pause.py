"""An attach after the sandbox paused is a final conflict, not a retry loop."""
from urllib.parse import quote

from space_smoke.native_hardening import NativeEvents, NativeHardeningFixture
from space_smoke.registry import scenario


def _not_running(sandbox_id: str, events: NativeEvents) -> bool:
    error = events.body.get("error")
    return (
        events.status == 409
        and isinstance(error, dict)
        and error.get("code") == "conflict"
        and error.get("retryable") is False
        and error.get("details") == [
            {"key": "reason", "value": "sandbox_not_running"},
            {"key": "state", "value": "paused"},
            {"key": "wake", "value": f"POST /v1/sandboxes/{sandbox_id}:wake"},
        ]
    )


@scenario(
    "native_contract.command_attach_after_pause",
    api_surfaces=("native",),
)
def command_attach_after_pause(test: NativeHardeningFixture) -> None:
    """A command created while the sandbox runs, then attached after it
    paused without a command or access wake source, answers the same
    non-retryable conflict as creating it would."""
    sandbox_id = test.create_ready()
    command = test.command(sandbox_id, ["true"])
    command.require(201)

    path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}"
    pause = test.request("POST", f"{path}:pause", idempotency_key=test.new_key("pause"))
    test.wait_operation(str(pause.require(202).get("operation_id")))
    test.wait_sandbox(sandbox_id, "paused")

    # Attach twice: the conflict is final, so a retry gets it again rather
    # than a retryable answer on either attach.
    attaches = []
    test.context.record("attaches", attaches)
    for attempt in range(2):
        events = test.events(command.body, timeout=30)
        attaches.append({"status": events.status, "error": events.body.get("error")})
        test.context.verify(
            _not_running(sandbox_id, events),
            "an attach to a paused sandbox the command cannot wake was not a final conflict",
            attempt=attempt,
        )

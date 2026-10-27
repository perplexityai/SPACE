"""A command that cannot wake its sandbox is a final conflict, not a retry loop."""
import time
from urllib.parse import quote

from space_smoke.native_hardening import NativeHardeningFixture, NativeResponse
from space_smoke.registry import scenario

# Long enough to observe the delete before the sandbox row is gone.
DELETING_BUDGET_SECONDS = 30


def _not_running(sandbox_id: str, state: str, response: NativeResponse) -> bool:
    details = [
        {"key": "reason", "value": "sandbox_not_running"},
        {"key": "state", "value": state},
    ]
    # No action returns a deleting sandbox to running, so none is named.
    if state != "deleting":
        details.append({"key": "wake", "value": f"POST /v1/sandboxes/{sandbox_id}:wake"})
    return (
        response.status == 409
        and response.body.get("code") == "conflict"
        and response.body.get("retryable") is False
        and all(key.lower() != "retry-after" for key in response.headers)
        and response.body.get("details") == details
    )


def _outcome(test: NativeHardeningFixture, command: NativeResponse) -> dict[str, object]:
    """Credential-free evidence: the answer, and for an accepted command what
    its attach answers instead."""
    outcome: dict[str, object] = {
        "status": command.status,
        "code": command.body.get("code"),
        "retryable": command.body.get("retryable"),
        "details": command.body.get("details"),
    }
    if command.status == 201:
        attaches = []
        for _ in range(3):
            events = test.events(command.body, timeout=15)
            attaches.append({"status": events.status, "error": events.body.get("error")})
            time.sleep(1)
        outcome["attaches"] = attaches
    return outcome


@scenario(
    "native_contract.command_on_sandbox_not_running",
    api_surfaces=("native",),
)
def command_on_sandbox_not_running(test: NativeHardeningFixture) -> None:
    """Without a command or access wake source, a command on a suspended or
    deleting sandbox is a non-retryable conflict naming its state and wake."""
    sandbox_id = test.create_ready()
    path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}"

    suspend = test.request("POST", f"{path}:suspend", idempotency_key=test.new_key("suspend"))
    test.wait_operation(str(suspend.require(202).get("operation_id")))
    test.wait_sandbox(sandbox_id, "suspended")
    command = test.command(sandbox_id, ["true"])
    suspended = _not_running(sandbox_id, "suspended", command)
    test.context.record("suspended", _outcome(test, command))

    delete = test.request("DELETE", path, idempotency_key=test.new_key("delete"))
    operation_id = str(delete.require(202).get("operation_id"))
    observed: list[dict[str, object]] = []
    deadline = time.monotonic() + DELETING_BUDGET_SECONDS
    deleting = False
    while not deleting and time.monotonic() < deadline:
        command = test.command(sandbox_id, ["true"])
        deleting = _not_running(sandbox_id, "deleting", command)
        observed.append({"status": command.status, "details": command.body.get("details")})
        if command.status == 404:
            break
    test.context.record("deleting", observed[-5:])
    test.wait_operation(operation_id)
    test.context.verify(suspended, "a command on a suspended sandbox was not a final conflict")
    test.context.verify(deleting, "a command on a deleting sandbox was not a final conflict")

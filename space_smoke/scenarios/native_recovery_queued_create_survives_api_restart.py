"""Native recovery behavior on an exclusively owned Kind cluster."""

import time
import uuid

from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    attach_after_recovery,
    expect_response,
    expect_success,
    stdout_bytes,
)
from space_smoke.native_hardening import NativeTransportError
from space_smoke.registry import scenario


@scenario(
    "native_recovery.queued_create_survives_api_restart",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def queued_create_survives_api_restart(fixture: KindNativeHardeningFixture) -> None:
    """Persisted acceptance and same-key replay survive API loss while work is queued."""

    control = fixture.recovery
    # The supplied forwards must target the acknowledged Kind cluster. Probe
    # the authenticated public surface before stopping any controller.
    expect_response(fixture, fixture.request("GET", "/v1/sandboxes?limit=1"), 200)
    with control.owned_faults():
        with fixture.context.phase("queue a create while the controller is stopped"):
            control.stop_controller()
            key = uuid.uuid4().hex
            accepted = expect_response(fixture, fixture.create(idempotency_key=key), 202)
            operation_id, sandbox_id = accepted["operation_id"], accepted["sandbox_id"]
            operation = expect_response(fixture, fixture.request("GET", "/v1/operations/" + operation_id), 200)
            fixture.context.verify(operation["state"] == "queued", "create must remain queued without a controller")
        with fixture.context.phase("restart API and replay persisted acceptance"):
            control.restart("space-api")
            deadline = time.monotonic() + 45
            replay = None
            while time.monotonic() < deadline:
                try:
                    replay = fixture.create(idempotency_key=key)
                except NativeTransportError:
                    time.sleep(0.5)
                    continue
                if replay.status not in {502, 503, 504}:
                    break
                time.sleep(0.5)
            fixture.context.verify(replay is not None, "API did not recover before replay deadline")
            body = expect_response(fixture, replay, 202)
            fixture.context.verify(
                body.get("operation_id") == operation_id and body.get("sandbox_id") == sandbox_id,
                "same-key replay after API restart must retain operation and sandbox identity",
            )
            control.restore_controller()
        with fixture.context.phase("await queued operation and execute in recovered sandbox"):
            fixture.wait_operation(operation_id)
            fixture.wait_sandbox(sandbox_id)
            command = expect_response(fixture, fixture.command(sandbox_id, ["sh", "-c", "printf recovered"]), 201)
            events = attach_after_recovery(fixture, command)
            expect_success(fixture, events)
            fixture.context.verify(stdout_bytes(events) == b"recovered", "recovered sandbox must execute commands")

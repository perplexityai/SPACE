"""Native recovery behavior on an exclusively owned Kind cluster."""

import uuid

from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    attach_after_recovery,
    expect_response,
    expect_success,
    stdout_bytes,
)
from space_smoke.registry import scenario


@scenario(
    "native_recovery.command_survives_data_plane_restart",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def command_survives_data_plane_restart(fixture: KindNativeHardeningFixture) -> None:
    """A running command reconnects after data-plane replacement without reexecution."""

    control = fixture.recovery
    expect_response(fixture, fixture.request("GET", "/v1/sandboxes?limit=1"), 200)
    with control.owned_faults():
        sandbox_id = fixture.create_ready()
        marker = "/tmp/space-smoke-recovery-" + uuid.uuid4().hex
        release = marker + "-release"
        script = (
            f"echo once >> {marker}; echo begun; "
            f"while ! test -f {release}; do sleep 0.2; done; echo recovered"
        )
        command = expect_response(
            fixture, fixture.command(sandbox_id, ["sh", "-c", script], timeout_seconds=180), 201
        )
        with fixture.context.phase("start command and replace data-plane pods"):
            # The SSE contract starts with attached, then process_started; the
            # first output ("begun") is the first frame with an id.
            initial = attach_after_recovery(fixture, command, max_events=3)
            fixture.context.verify(
                any(event.data.get("type") == "process_started" for event in initial.events),
                "fault requires an observed running guest process",
            )
            cursor = next((event.id for event in reversed(initial.events) if event.id), None)
            fixture.context.verify(cursor is not None, "initial stream must supply a reconnect cursor")
            control.restart("space-data-plane")
        with fixture.context.phase("release process and reconnect with the original command session"):
            release_command = expect_response(
                fixture, fixture.command(sandbox_id, ["sh", "-c", f"touch {release}"]), 201
            )
            expect_success(fixture, attach_after_recovery(fixture, release_command))
            resumed = attach_after_recovery(fixture, command, last_event_id=cursor)
            expect_success(fixture, resumed)
            count_command = expect_response(
                fixture, fixture.command(sandbox_id, ["sh", "-c", f"wc -l < {marker}"]), 201
            )
            count_events = attach_after_recovery(fixture, count_command)
            expect_success(fixture, count_events)
            fixture.context.verify(
                stdout_bytes(count_events).strip() == b"1",
                "reconnecting a session after data-plane restart must not repeat guest side effects",
            )
            fixture.context.record("recovery_side_effect_count", 1)

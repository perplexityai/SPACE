"""Native recovery behavior on an exclusively owned Kind cluster."""

import base64
import threading
import time
import uuid

from space_smoke.native_hardening import NativeEvent, NativeEvents
from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    attach_after_recovery,
    expect_response,
    expect_success,
    stdout_bytes,
)
from space_smoke.registry import scenario

# How long the attached stream may stay open in all. The command waits for the
# scenario's markers, so this bounds the scenario, not the guest.
STREAM_TIMEOUT = 300.0
# How soon after its last stream ends the old data plane must be gone. Its
# drain returns at once then; this leaves the kubelet time to see the exit,
# and is well short of the drain deadline.
EXIT_AFTER_STREAM = 30.0


@scenario(
    "native_recovery.attached_stream_drains_through_data_plane_restart",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def attached_stream_drains_through_data_plane_restart(fixture: KindNativeHardeningFixture) -> None:
    """A stream attached before SIGTERM keeps delivering, and the old replica exits when it ends."""

    control = fixture.recovery
    expect_response(fixture, fixture.request("GET", "/v1/sandboxes?limit=1"), 200)
    with control.owned_faults():
        deadline = control.data_plane_shutdown_grace()
        fixture.context.record("data_plane_shutdown_grace_seconds", deadline)
        sandbox_id = fixture.create_ready()
        marker = "/tmp/space-smoke-drain-" + uuid.uuid4().hex
        go, release = marker + "-go", marker + "-release"
        # A line a second while waiting keeps the stream busy between markers.
        script = (
            f"echo begun; while ! test -f {go}; do echo waiting; sleep 1; done; echo after-sigterm; "
            f"while ! test -f {release}; do echo waiting; sleep 1; done; echo released"
        )
        command = expect_response(
            fixture,
            fixture.command(sandbox_id, ["sh", "-c", script], timeout_seconds=int(STREAM_TIMEOUT)),
            201,
        )

        output = bytearray()
        output_lock = threading.Lock()
        begun, after_sigterm = threading.Event(), threading.Event()

        def on_event(event: NativeEvent) -> None:
            if event.data.get("type") != "stdout":
                return
            with output_lock:
                output.extend(base64.b64decode(event.data["data_base64"], validate=True))
                seen = bytes(output)
            if b"begun" in seen:
                begun.set()
            if b"after-sigterm" in seen:
                after_sigterm.set()

        # One attachment, never reconnected: the scenario is about this stream
        # surviving the drain, so a reconnect would hide the failure.
        outcome: dict[str, NativeEvents | BaseException] = {}

        def read_stream() -> None:
            try:
                outcome["events"] = fixture.events(command, timeout=STREAM_TIMEOUT, on_event=on_event)
            except BaseException as error:  # noqa: BLE001 - re-raised on the scenario thread
                outcome["error"] = error

        # A daemon thread, so a failed verification doesn't wait out the stream.
        reader = threading.Thread(target=read_stream, name="attached-stream", daemon=True)
        reader.start()

        with fixture.context.phase("attach the command's stream before the restart"):
            fixture.context.verify(begun.wait(60), "the command's stream must attach and deliver output")
        with fixture.context.phase("SIGTERM the data plane while the stream is attached"):
            replaced = control.begin_restart("space-data-plane")
            sigterm_at = time.monotonic()
            draining = control.await_replacements("space-data-plane", replaced)
            fixture.context.verify(
                bool(draining),
                "a replaced data-plane pod serving an open stream must keep running after its replacements are ready",
                replaced_pods=len(replaced),
            )
        with fixture.context.phase("the attached stream keeps delivering during the drain"):
            # A new command attaches through a replacement; the old pod no
            # longer takes new streams.
            go_command = expect_response(fixture, fixture.command(sandbox_id, ["sh", "-c", f"touch {go}"]), 201)
            expect_success(fixture, attach_after_recovery(fixture, go_command))
            fixture.context.verify(
                after_sigterm.wait(30),
                "the stream attached before SIGTERM must deliver output the command produced after it",
            )
            fixture.context.verify(
                bool(control.remaining("space-data-plane", draining)),
                "the draining pod must still be running while its stream is open",
            )
        with fixture.context.phase("the stream ends with the command, then the old pod exits"):
            release_command = expect_response(
                fixture, fixture.command(sandbox_id, ["sh", "-c", f"touch {release}"]), 201
            )
            expect_success(fixture, attach_after_recovery(fixture, release_command))
            reader.join(60)
            fixture.context.verify(not reader.is_alive(), "the attached stream must end when its command exits")
            stream_ended_at = time.monotonic()
            if "error" in outcome:
                raise outcome["error"]
            events = outcome["events"]
            assert isinstance(events, NativeEvents)
            fixture.context.verify(events.status == 200, "the attached stream must have opened")
            expect_success(fixture, events)
            fixture.context.verify(
                b"released" in stdout_bytes(events),
                "the stream attached before SIGTERM must carry the command through to its exit",
            )
            try:
                control.await_exit("space-data-plane", replaced, timeout=EXIT_AFTER_STREAM)
            except TimeoutError:
                fixture.context.verify(
                    False,
                    "the old data plane must exit once its last stream ends, not wait for the drain deadline",
                    deadline_seconds=deadline,
                )
            fixture.context.record(
                "data_plane_exit_after_stream_seconds", round(time.monotonic() - stream_ended_at, 1)
            )
            fixture.context.record("data_plane_drain_seconds", round(time.monotonic() - sigterm_at, 1))

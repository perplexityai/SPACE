"""A wake behind a queued suspend is not run inline ahead of it."""

import time

from space_smoke.native_inline import lifecycle, operation, require_inline
from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    attach_after_recovery,
    expect_response,
    expect_success,
    stdout_bytes,
)
from space_smoke.registry import scenario

# Long enough for an inline wake to have been claimed had admission let it.
HOLD_SECONDS = 5


@scenario(
    "native_recovery.inline_wake_waits_behind_queued_suspend",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def inline_wake_waits_behind_queued_suspend(fixture: KindNativeHardeningFixture) -> None:
    """With a suspend queued for a running sandbox, a later wake stays queued
    behind it; the dispatcher then runs the suspend and the wake in request
    order, so the sandbox ends running.
    """

    control = fixture.recovery
    with control.owned_faults():
        require_inline(control, "wake_sandbox")
        with fixture.context.phase("prepare a running sandbox"):
            sandbox_id = fixture.create_ready(timeout=120)
        with fixture.context.phase("queue a suspend, then a wake, with no dispatcher running"):
            control.stop_controller()
            suspend = lifecycle(fixture, sandbox_id, "suspend")
            wake = lifecycle(fixture, sandbox_id, "wake")
            deadline = time.monotonic() + HOLD_SECONDS
            while time.monotonic() < deadline:
                states = {"suspend": operation(fixture, suspend)["state"], "wake": operation(fixture, wake)["state"]}
                fixture.context.verify(
                    states == {"suspend": "queued", "wake": "queued"},
                    "a wake behind a queued suspend must stay queued",
                    inline_ordering_states=states,
                )
                time.sleep(0.5)
        with fixture.context.phase("restore the dispatcher and await both in order"):
            control.restore_controller()
            fixture.wait_operation(suspend, timeout=240)
            fixture.wait_operation(wake, timeout=240)
            fixture.wait_sandbox(sandbox_id, timeout=120)
            command = expect_response(fixture, fixture.command(sandbox_id, ["sh", "-c", "printf ordered"]), 201)
            events = attach_after_recovery(fixture, command)
            expect_success(fixture, events)
            fixture.context.verify(
                stdout_bytes(events) == b"ordered", "the wake must run after the suspend, leaving the sandbox running"
            )

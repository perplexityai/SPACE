"""An inline wake abandoned by a killed API pod is re-driven by the dispatcher."""

from space_smoke.native_inline import claims, lifecycle, operation, require_inline, suspended_sandbox, wait_state
from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    attach_after_recovery,
    expect_response,
    expect_success,
    stdout_bytes,
)
from space_smoke.registry import scenario
from space_smoke.runtime import InconclusiveSmoke

# The dispatcher's default claim staleness is 60 s; the re-driven restore
# itself takes seconds.
REDRIVE_TIMEOUT = 300


@scenario(
    "native_recovery.inline_wake_survives_api_pod_kill",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def inline_wake_survives_api_pod_kill(fixture: KindNativeHardeningFixture) -> None:
    """A wake the API is running when every API pod is killed is not lost:
    once its claim goes stale the dispatcher takes the same operation over,
    and the sandbox runs commands.
    """

    control = fixture.recovery
    with control.owned_faults():
        require_inline(control, "wake_sandbox")
        with fixture.context.phase("suspend a sandbox"):
            sandbox_id = suspended_sandbox(fixture)
        with fixture.context.phase("start an inline wake with no dispatcher running"):
            control.stop_controller()
            wake = lifecycle(fixture, sandbox_id, "wake")
            state = wait_state(fixture, wake, {"running"}, timeout=15)
            fixture.context.record("inline_wake_state_before_kill", state)
            if state == "queued":
                raise InconclusiveSmoke("the wake was queued, not run inline")
            if state != "running":
                raise InconclusiveSmoke(f"the wake ended ({state}) before the API pod could be killed")
        with fixture.context.phase("kill every API pod mid-wake"):
            control.kill("space-api")
            after = operation(fixture, wake)["state"]
            fixture.context.record("inline_wake_state_after_kill", after)
            if after != "running":
                raise InconclusiveSmoke(f"the wake ended ({after}) before its API pod died")
        with fixture.context.phase("restore the dispatcher and await the re-driven wake"):
            control.restore_controller()
            fixture.wait_operation(wake, timeout=REDRIVE_TIMEOUT)
            claimed = claims(fixture, wake)
            fixture.context.verify(
                claimed >= 2,
                "the abandoned wake must be claimed again, by the dispatcher",
                inline_wake_claims=claimed,
            )
            fixture.wait_sandbox(sandbox_id, timeout=120)
            command = expect_response(fixture, fixture.command(sandbox_id, ["sh", "-c", "printf woken"]), 201)
            events = attach_after_recovery(fixture, command)
            expect_success(fixture, events)
            fixture.context.verify(stdout_bytes(events) == b"woken", "the re-driven wake must leave a running sandbox")

"""An inline wake abandoned by a killed API pod is re-driven by the dispatcher."""

from space_smoke.native_inline import (
    claims,
    inline_execution,
    lifecycle,
    operation_after_api_loss,
    suspended_sandbox,
    terminal_events,
    wait_state,
)
from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    attach_after_recovery,
    expect_response,
    expect_success,
    stdout_bytes,
)
from space_smoke.registry import scenario

# The inline wake waits this long before it starts, so it is still running
# when its API pod dies however fast the runner restores: the kill, which
# waits out the replacement pods, takes seconds. Only the API pods' own
# executions are held; the dispatcher's re-drive is not.
WAKE_HOLD_SECONDS = 300
# The dispatcher's default claim staleness is 60 s; the re-driven restore
# itself takes seconds.
REDRIVE_TIMEOUT = 300


@scenario(
    "native_recovery.inline_wake_survives_api_pod_kill",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
    test_hooks="SPACE_NATIVE_INLINE_TEST_HOLD_SECS, which holds the inline wake running until its pod dies",
)
def inline_wake_survives_api_pod_kill(fixture: KindNativeHardeningFixture) -> None:
    """A wake the API is running when every API pod is killed is not lost:
    once its claim goes stale the dispatcher takes the same operation over,
    ends it once, and the sandbox runs commands.
    """

    control = fixture.recovery
    with inline_execution(fixture, control, ("wake_sandbox",), hold=WAKE_HOLD_SECONDS), control.owned_faults():
        with fixture.context.phase("suspend a sandbox"):
            sandbox_id = suspended_sandbox(fixture)
        with fixture.context.phase("start an inline wake with no dispatcher running"):
            control.stop_controller()
            wake = lifecycle(fixture, sandbox_id, "wake")
            state = wait_state(fixture, wake, {"running"}, timeout=15)
            fixture.context.record("inline_wake_state_before_kill", state)
            fixture.context.verify(
                state == "running",
                "with inline execution on and no dispatcher running, the API must claim the wake and hold it running",
                inline_wake_state_before_kill=state,
            )
        with fixture.context.phase("kill every API pod mid-wake"):
            control.kill("space-api")
            after = operation_after_api_loss(fixture, wake)["state"]
            fixture.context.record("inline_wake_state_after_kill", after)
            fixture.context.verify(
                after == "running",
                "the wake must still be running, abandoned by its killed API pod",
                inline_wake_state_after_kill=after,
            )
        with fixture.context.phase("restore the dispatcher and await the re-driven wake"):
            control.restore_controller()
            fixture.wait_operation(wake, timeout=REDRIVE_TIMEOUT)
            ran = {"claims": claims(fixture, wake), "ends": terminal_events(fixture, wake)}
            fixture.context.record("inline_wake_run", ran)
            fixture.context.verify(
                ran["claims"] >= 2 and ran["ends"] == 1,
                "the abandoned wake must be claimed again, by the dispatcher, and end once",
                inline_wake_run=ran,
            )
            fixture.wait_sandbox(sandbox_id, timeout=120)
            command = expect_response(fixture, fixture.command(sandbox_id, ["sh", "-c", "printf woken"]), 201)
            events = attach_after_recovery(fixture, command)
            expect_success(fixture, events)
            fixture.context.verify(stdout_bytes(events) == b"woken", "the re-driven wake must leave a running sandbox")

"""With one inline permit per API pod, concurrent creates past it are queued, not lost."""

from concurrent.futures import ThreadPoolExecutor

from space_smoke.native_inline import inline_execution, operation
from space_smoke.native_recovery import RECOVERY_ENVIRONMENT, KindNativeHardeningFixture, expect_response
from space_smoke.registry import scenario
from space_smoke.runtime import InconclusiveSmoke

CREATES = 4
# Fail-safe deadline only: the run releases the held permits after reading
# every admission's committed state, without waiting out this duration.
PERMIT_HOLD_SECONDS = 120


@scenario(
    "native_recovery.inline_fallback_at_one_permit_loses_no_operation",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
    test_hooks="SPACE_NATIVE_INLINE_TEST_HOLD_SECS, which keeps each inline create's permit taken while the rest "
    "are admitted",
)
def inline_fallback_at_one_permit_loses_no_operation(fixture: KindNativeHardeningFixture) -> None:
    """At `SPACE_NATIVE_INLINE_MAX_CONCURRENT=1`, concurrent creates with no
    dispatcher running start inline at most once per API pod; the rest stay
    queued, and every one succeeds once the dispatcher is back.
    """

    control = fixture.recovery
    with (
        inline_execution(fixture, control, ("create_sandbox",), permits=1, hold=PERMIT_HOLD_SECONDS) as release_hold,
        control.owned_faults(),
    ):
        api_pods = control._captures["space-api"].replicas
        if api_pods >= CREATES:
            raise InconclusiveSmoke(f"{api_pods} API pods leave no create past their permits")
        with fixture.context.phase("admit concurrent creates with no dispatcher running"):
            control.stop_controller()
            with ThreadPoolExecutor(max_workers=CREATES) as pool:
                responses = list(pool.map(lambda _: fixture.create(), range(CREATES)))
            accepted = [expect_response(fixture, response, 202) for response in responses]
            operations = [body["operation_id"] for body in accepted]
            # Admission commits its inline claim before answering, so every
            # concurrent response is sufficient synchronization for this read.
            started = {op for op in operations if operation(fixture, op)["state"] != "queued"}
            fixture.context.record("inline_started_while_stopped", len(started))
            fixture.context.verify(
                bool(started),
                "with a permit free on each API pod and no dispatcher running, the API must run a create",
                inline_started=len(started),
            )
            fixture.context.verify(
                len(started) <= api_pods,
                "creates past each API pod's one permit must be queued for the dispatcher",
                inline_started=len(started),
                api_pods=api_pods,
            )
        with fixture.context.phase("restore the dispatcher and await every create"):
            release_hold()
            control.restore_controller()
            for body in accepted:
                fixture.wait_operation(body["operation_id"], timeout=300)
                fixture.wait_sandbox(body["sandbox_id"], timeout=120)

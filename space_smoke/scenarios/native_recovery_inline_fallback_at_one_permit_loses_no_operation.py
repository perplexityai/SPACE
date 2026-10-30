"""With one inline permit per API pod, concurrent creates past it are queued, not lost."""

import time
from concurrent.futures import ThreadPoolExecutor

from space_smoke.native_inline import inline_permits, operation, require_inline
from space_smoke.native_recovery import RECOVERY_ENVIRONMENT, KindNativeHardeningFixture, expect_response
from space_smoke.registry import scenario
from space_smoke.runtime import InconclusiveSmoke

CREATES = 4
# Long enough for every admitted create to have been claimed had admission
# let it run inline.
HOLD_SECONDS = 8


@scenario(
    "native_recovery.inline_fallback_at_one_permit_loses_no_operation",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def inline_fallback_at_one_permit_loses_no_operation(fixture: KindNativeHardeningFixture) -> None:
    """At `SPACE_NATIVE_INLINE_MAX_CONCURRENT=1`, concurrent creates with no
    dispatcher running start inline at most once per API pod; the rest stay
    queued, and every one succeeds once the dispatcher is back.
    """

    control = fixture.recovery
    require_inline(control, "create_sandbox")
    with inline_permits(fixture, control, 1), control.owned_faults():
        api_pods = control._captures["space-api"].replicas
        if api_pods >= CREATES:
            raise InconclusiveSmoke(f"{api_pods} API pods leave no create past their permits")
        with fixture.context.phase("admit concurrent creates with no dispatcher running"):
            control.stop_controller()
            with ThreadPoolExecutor(max_workers=CREATES) as pool:
                responses = list(pool.map(lambda _: fixture.create(), range(CREATES)))
            accepted = [expect_response(fixture, response, 202) for response in responses]
            operations = [body["operation_id"] for body in accepted]
            started = set()
            deadline = time.monotonic() + HOLD_SECONDS
            while time.monotonic() < deadline:
                started |= {op for op in operations if operation(fixture, op)["state"] != "queued"}
                time.sleep(0.5)
            fixture.context.record("inline_started_while_stopped", len(started))
            if not started:
                raise InconclusiveSmoke("no create ran inline")
            fixture.context.verify(
                len(started) <= api_pods,
                "creates past each API pod's one permit must be queued for the dispatcher",
                inline_started=len(started),
                api_pods=api_pods,
            )
        with fixture.context.phase("restore the dispatcher and await every create"):
            control.restore_controller()
            for body in accepted:
                fixture.wait_operation(body["operation_id"], timeout=300)
                fixture.wait_sandbox(body["sandbox_id"], timeout=120)

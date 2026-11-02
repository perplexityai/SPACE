"""Inline work under a newly elected controller leader runs once and is not lost."""

from concurrent.futures import ThreadPoolExecutor

from space_smoke.native_inline import (
    claims,
    inline_execution,
    lifecycle,
    operation,
    suspended_sandbox,
    terminal_events,
    wait_state,
)
from space_smoke.native_recovery import RECOVERY_ENVIRONMENT, KindNativeHardeningFixture, expect_response
from space_smoke.registry import scenario

CREATES = 2
WAKES = 2
# Long enough for every create and wake to be claimed, had admission let it
# run inline.
INLINE_START_TIMEOUT = 15


@scenario(
    "native_recovery.inline_load_survives_leader_failover",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def inline_load_survives_leader_failover(fixture: KindNativeHardeningFixture) -> None:
    """Creates and wakes the API runs inline while no controller runs, with a
    suspend queued behind them for the dispatcher, then a new controller
    leader elected while their claims may still be live: each create and wake
    is claimed once and ends once, and every operation succeeds as requested.
    """

    control = fixture.recovery
    with inline_execution(fixture, control, ("create_sandbox", "wake_sandbox")), control.owned_faults():
        with fixture.context.phase("prepare suspended and running sandboxes"):
            suspended = [suspended_sandbox(fixture) for _ in range(WAKES)]
            running = fixture.create_ready(timeout=120)
        with fixture.context.phase("run creates and wakes inline with no controller running"):
            control.stop_controller()

            def submit(index: int) -> tuple[str, str]:
                if index < CREATES:
                    body = expect_response(fixture, fixture.create(), 202)
                    return body["operation_id"], body["sandbox_id"]
                sandbox_id = suspended[index - CREATES]
                return lifecycle(fixture, sandbox_id, "wake"), sandbox_id

            with ThreadPoolExecutor(max_workers=CREATES + WAKES) as pool:
                inline_work = list(pool.map(submit, range(CREATES + WAKES)))
            # Only the API can move an operation past `queued` now.
            started = {
                operation_id: wait_state(fixture, operation_id, {"running"}, timeout=INLINE_START_TIMEOUT)
                for operation_id, _ in inline_work
            }
            fixture.context.record("inline_states_while_stopped", started)
            fixture.context.verify(
                all(state in {"running", "succeeded"} for state in started.values()),
                "with no controller running, the API must run every create and wake",
                inline_states_while_stopped=started,
            )
            suspend = lifecycle(fixture, running, "suspend")
            fixture.context.verify(
                operation(fixture, suspend)["state"] == "queued",
                "the suspend must wait for a dispatcher",
            )
        with fixture.context.phase("elect a controller leader under the inline claims"):
            fixture.context.record(
                "inline_states_at_election",
                {operation_id: operation(fixture, operation_id)["state"] for operation_id, _ in inline_work},
            )
            control.restore_controller()
        with fixture.context.phase("await every accepted operation"):
            for operation_id, sandbox_id in inline_work:
                fixture.wait_operation(operation_id, timeout=300)
                fixture.wait_sandbox(sandbox_id, "running", timeout=180)
            fixture.wait_operation(suspend, timeout=300)
            fixture.wait_sandbox(running, "suspended", timeout=180)
            ran = {
                operation_id: {"claims": claims(fixture, operation_id), "ends": terminal_events(fixture, operation_id)}
                for operation_id, _ in inline_work
            }
            fixture.context.record("inline_runs", ran)
            fixture.context.verify(
                all(run == {"claims": 1, "ends": 1} for run in ran.values()),
                "each inline create and wake must be claimed once and end once: no dispatcher may take over a live "
                "inline claim",
                inline_runs=ran,
            )

"""A storage-bound operation must leave idle dispatcher capacity usable."""

import time

from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    expect_response,
)
from space_smoke.registry import scenario
from space_smoke.runtime import InconclusiveSmoke

TERMINAL = {"succeeded", "failed", "canceled", "expired"}
FAIRNESS_WINDOW_SECONDS = 15


@scenario(
    "native_recovery.slow_operation_does_not_block_later_work",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def slow_operation_does_not_block_later_work(test: KindNativeHardeningFixture) -> None:
    """With an idle dispatcher slot, a later pause finishes while an earlier
    sandbox suspend is held running by a reversible object-storage outage.
    """
    control = test.recovery
    operations = []
    observation = []
    fast_completed_while_slow_running = False
    inconclusive = None
    primary = None

    def claim_history(operation_id: str) -> list[dict]:
        page = expect_response(
            test, test.request("GET", f"/v1/operations/{operation_id}/events?limit=1000"), 200
        )
        if page.get("next_cursor"):
            raise InconclusiveSmoke("operation claim history exceeded the bounded complete page")
        events = page.get("data")
        if not isinstance(events, list) or not events:
            raise InconclusiveSmoke("operation claim history was unavailable")
        return events
    with control.owned_faults(object_storage=True):
        control.require_dispatcher_capacity()
        with test.context.phase("prepare two ready sandboxes before blocking object storage"):
            slow = test.create_ready(timeout=90)
            fast = test.create_ready(timeout=90)
        try:
            with test.context.phase("hold a native suspend in the current dispatcher batch"):
                control.stop_object_storage()
                accepted = expect_response(
                    test,
                    test.request("POST", f"/v1/sandboxes/{slow}:suspend", idempotency_key=test.key("slow-suspend")),
                    202,
                )
                slow_operation = accepted["operation_id"]
                operations.append(slow_operation)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    state = expect_response(test, test.request("GET", f"/v1/operations/{slow_operation}"), 200)["state"]
                    if state == "running":
                        break
                    if state in TERMINAL:
                        raise InconclusiveSmoke("slow suspend became terminal before the fairness window")
                    time.sleep(0.25)
                else:
                    raise InconclusiveSmoke("slow suspend was not claimed before the fairness window")

            with test.context.phase("submit later independent pause while the suspend is running"):
                history = claim_history(slow_operation)
                if history[-1].get("event_type") != "operation.claimed":
                    raise InconclusiveSmoke("slow operation did not retain a current claim before canary submission")
                claim_sequence = history[-1]["sequence"]
                test.context.record("dispatcher_slow_claim_sequence", claim_sequence)
                accepted = expect_response(
                    test,
                    test.request("POST", f"/v1/sandboxes/{fast}:pause", idempotency_key=test.key("fast-pause")),
                    202,
                )
                fast_operation = accepted["operation_id"]
                operations.append(fast_operation)
                started = time.monotonic()
                deadline = started + FAIRNESS_WINDOW_SECONDS
                while time.monotonic() < deadline:
                    # Observe the slow operation AFTER the canary: a canary
                    # completing only after the slow operation drains is not a pass.
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    fast_state = expect_response(
                        test, test.request(
                            "GET", f"/v1/operations/{fast_operation}",
                            timeout=min(5, remaining),
                        ), 200
                    )["state"]
                    fast_observed_at = time.monotonic()
                    if fast_observed_at >= deadline:
                        break
                    slow_state = expect_response(
                        test, test.request(
                            "GET", f"/v1/operations/{slow_operation}",
                            timeout=min(5, deadline - fast_observed_at),
                        ), 200
                    )["state"]
                    observation.append({
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                        "fast_observed_seconds": round(fast_observed_at - started, 3),
                        "slow_state": slow_state, "fast_state": fast_state,
                    })
                    if slow_state != "running":
                        inconclusive = "slow operation stopped running before independent completion was observed"
                        break
                    if fast_state == "succeeded":
                        # Running on both polls can hide a requeue and reclaim.
                        # Durable event sequence proves this is the SAME claim.
                        later_events = [
                            event for event in claim_history(slow_operation)
                            if event["sequence"] > claim_sequence
                        ]
                        test.context.record("dispatcher_slow_later_events", [
                            {"sequence": event["sequence"], "event_type": event["event_type"]}
                            for event in later_events
                        ])
                        if later_events:
                            inconclusive = "slow operation changed claim during the fairness window"
                        else:
                            fast_completed_while_slow_running = True
                        break
                    if fast_state in TERMINAL:
                        break
                    time.sleep(0.25)
                test.context.record("dispatcher_fairness_observations", observation)
                test.context.record("dispatcher_fairness_window_seconds", FAIRNESS_WINDOW_SECONDS)
        except BaseException as error:
            primary = error
            raise
        finally:
            # The storage outage must end before an assertion can escape.
            # Drain within the scenario budget: generic fixture cleanup cannot
            # cover the upload worker's possible 600 s retry ladder.
            try:
                with test.context.phase("restore object storage and drain accepted operations"):
                    control.restore_object_storage()
                    deadline = time.monotonic() + 600
                    terminal_states = {}
                    for operation_id in operations:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("accepted fairness operations did not drain after storage restoration")
                        result = test.wait_operation(operation_id, timeout=remaining, require_success=False)
                        terminal_states[operation_id] = result["state"]
                    test.context.record("dispatcher_fairness_terminal_states", terminal_states)
            except Exception as error:
                test.context.record("dispatcher_fairness_restore_or_drain_failure", str(error))
                if primary is None:
                    raise
                primary.add_note(f"Fairness restoration or drain also failed: {error}")
        if inconclusive:
            raise InconclusiveSmoke(inconclusive)
        test.context.verify(
            fast_completed_while_slow_running,
            "later independent work did not complete while a slow operation occupied only one dispatcher slot",
            dispatcher_fairness_passed=fast_completed_while_slow_running,
        )

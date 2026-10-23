"""Work queued through a controller outage longer than its deadline expires."""

import time

from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
    expect_response,
)
from space_smoke.registry import scenario

DEADLINE_SECONDS = 20
# How far past the shortened deadline the outage lasts, so every queued
# operation is over it when the controller comes back.
OUTAGE_MARGIN_SECONDS = 10


@scenario(
    "native_recovery.stale_queued_work_expires_after_controller_outage",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def stale_queued_work_expires_after_controller_outage(test: KindNativeHardeningFixture) -> None:
    """A create and a pause queued through an outage longer than the operation
    deadline expire without running; a delete queued with them still runs.
    """
    control = test.recovery
    with control.owned_faults():
        with test.context.phase("prepare a sandbox to pause and one to delete"):
            pause_target = test.create_ready(timeout=120)
            delete_target = test.create_ready(timeout=120)
        with control.operation_age_limit(seconds=DEADLINE_SECONDS):
            with test.context.phase("queue a create, a pause and a delete while the controller is stopped"):
                control.stop_controller()
                create = expect_response(test, test.create(idempotency_key=test.key("late-create")), 202)
                pause = expect_response(
                    test,
                    test.request("POST", f"/v1/sandboxes/{pause_target}:pause", idempotency_key=test.key("late-pause")),
                    202,
                )
                delete = expect_response(
                    test,
                    test.request("DELETE", f"/v1/sandboxes/{delete_target}", idempotency_key=test.key("late-delete")),
                    202,
                )
                queued_at = time.monotonic()
                operations = {
                    "create": create["operation_id"],
                    "pause": pause["operation_id"],
                    "delete": delete["operation_id"],
                }
                test.context.record("late_operations", operations)
                for name, operation_id in operations.items():
                    state = expect_response(test, test.request("GET", f"/v1/operations/{operation_id}"), 200)["state"]
                    test.context.verify(state == "queued", f"the {name} must stay queued without a controller")
            with test.context.phase("keep the controller stopped past the operation deadline"):
                # Only wall-clock time ages the queued operations; there is no
                # state to poll until the controller is back.
                time.sleep(max(0.0, queued_at + DEADLINE_SECONDS + OUTAGE_MARGIN_SECONDS - time.monotonic()))
                control.restore_controller()
            with test.context.phase("await the queued operations"):
                results = {
                    name: test.wait_operation(operation_id, timeout=180, require_success=False)
                    for name, operation_id in operations.items()
                }
                test.context.record("late_operation_outcomes", {
                    name: {
                        "state": result.get("state"),
                        "terminal_error_code": result.get("terminal_error_code"),
                        "reason": (result.get("terminal_error") or {}).get("reason"),
                    }
                    for name, result in results.items()
                })
        with test.context.phase("inspect the targets"):
            created = test.request("GET", f"/v1/sandboxes/{create['sandbox_id']}")
            paused = expect_response(test, test.request("GET", f"/v1/sandboxes/{pause_target}"), 200)
            test.wait_sandbox(delete_target, "deleted", timeout=120)
            test.context.record("late_targets", {
                "created_status": created.status,
                "pause_target_state": paused.get("state"),
            })

    for name in ("create", "pause"):
        result = results[name]
        test.context.verify(
            result.get("state") == "expired"
            and (result.get("terminal_error") or {}).get("reason") == "operation_deadline_exceeded",
            f"the {name} queued past its deadline must expire instead of running",
        )
    test.context.verify(created.status == 404, "the expired create must not build its sandbox")
    test.context.verify(paused.get("state") == "running", "the expired pause must not pause its sandbox")
    test.context.verify(results["delete"].get("state") == "succeeded", "the delete queued past its deadline must still run")

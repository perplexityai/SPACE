"""Completed DELETE response recovery."""
from space_smoke.registry import scenario
from space_smoke.native_hardening import NativeHardeningFixture

@scenario(
    "native_contract.completed_delete_replay",
    api_surfaces=("native",),
)
def completed_delete_replay(test: NativeHardeningFixture) -> None:
    """A completed DELETE replay returns its original retained operation."""
    sid = test.create_ready()
    key = test.new_key("delete")
    first = test.request("DELETE", f"/v1/sandboxes/{sid}", idempotency_key=key)
    test.context.verify(first.status == 202, "DELETE was not accepted", status=first.status)
    operation = first.body["operation_id"]
    test.wait_operation(operation)
    test.wait_sandbox(sid, state="deleted")
    replay = test.request("DELETE", f"/v1/sandboxes/{sid}", idempotency_key=key)
    test.context.verify(replay.status == 202 and replay.body.get("operation_id") == operation,
                        "completed DELETE did not replay its original operation",
                        original_operation=operation, replay_status=replay.status)

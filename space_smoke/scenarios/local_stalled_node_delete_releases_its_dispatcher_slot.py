"""A Delete whose node-local delete stalls releases its dispatcher slot."""

import space_smoke as smoke
from space_smoke.node_delete_stall import (
    NODE_DELETE_BUDGET_SECONDS,
    NODE_MANAGER,
    SANDBOX_MANAGER,
)

# Each stall stays under the frozen service's liveness window: three failed
# probes 20 s apart restart it 40-60 s after the freeze.
NOT_ADMITTED_ATTEMPTS = 2


# Two sandboxes, one per branch. Not admitted: with sandbox-manager frozen,
# the Delete and the admission probe both hang, so each attempt ends requeued
# after the teardown's budget while the sandbox stays, and once thawed the
# delete succeeds. Admitted: with node-manager frozen, sandbox-manager admits
# the delete (its row reads `deleting`) and its cleanup hangs, so the teardown
# continues after the budget and the operation succeeds on its first attempt
# while node-manager is still frozen; the node's row goes once thawed. Each attempt's slot
# hold is read from the operation's public events (`space_smoke.node_delete_stall`).
# Do not run concurrently with other scenarios on the same workers.
@smoke.scenario(
    "local.stalled_node_delete_releases_its_dispatcher_slot",
    # Two creates, two stalls of under 40 s, the delete that follows each
    # thaw (up to 120 s) and the node cleanup after the second (120 s).
    sandbox=smoke.SandboxConfig(timeout=900),
)
def stalled_node_delete_releases_its_dispatcher_slot(test: smoke.KindEcw) -> None:
    """A Delete whose node does not answer holds a dispatcher slot for one
    bounded attempt at a time: it keeps the sandbox and retries until the node
    admits the delete, and continues as soon as the node has admitted it."""

    stall = test.node_delete_stall

    with test.phase("create one sandbox per branch"):
        admitted = test.sandbox
        ready = test.run_command_when_ready("true")
        test.verify(ready.exit_code == 0, "the sandbox did not become ready")
        not_admitted = test.sandboxes.create()

    with test.phase("not admitted: freeze sandbox-manager under the delete"):
        node = stall.node_of(not_admitted.sandbox_id)
        test.record("not_admitted_node", node)
        stall.freeze(node, SANDBOX_MANAGER)
        operation_id = stall.delete(not_admitted.sandbox_id)
        stall.expect_service_hangs(node, SANDBOX_MANAGER)

    with test.phase("not admitted: each attempt is requeued and the sandbox is kept"):
        holds = stall.expect_requeued_while_kept(
            operation_id, not_admitted, attempts=NOT_ADMITTED_ATTEMPTS, timeout=30
        )
        test.verify(
            all(held >= NODE_DELETE_BUDGET_SECONDS - 2 for held, _ in holds),
            "an attempt ended before the teardown's budget, so the node answered",
            holds=holds,
        )
        stall.thaw()

    with test.phase("not admitted: once thawed, the retried delete succeeds"):
        stall.expect_service_answers(node, SANDBOX_MANAGER)
        stall.expect_succeeded(operation_id, timeout=120)
        stall.expect_gone(not_admitted)

    with test.phase("admitted: freeze node-manager under the delete"):
        node = stall.node_of(admitted.sandbox_id)
        test.record("admitted_node", node)
        stall.freeze(node, NODE_MANAGER)
        operation_id = stall.delete(admitted.sandbox_id)
        stall.expect_service_hangs(node, NODE_MANAGER)

    with test.phase("admitted: the teardown continues after its budget"):
        # sandbox-manager's own reads wait on the frozen node-manager, so the
        # node's row is read only after the thaw.
        holds = stall.expect_succeeded(operation_id, timeout=30)
        test.record("admitted_slot_holds", holds)
        test.verify(
            len(holds) == 1 and holds[0][0] >= NODE_DELETE_BUDGET_SECONDS - 2,
            "the admitted delete did not succeed on one attempt cut at the budget",
            holds=holds,
        )
        stall.expect_gone(admitted)
        stall.thaw()

    with test.phase("admitted: once thawed, the node's cleanup finishes"):
        stall.expect_service_answers(node, NODE_MANAGER)
        stall.expect_node_row_gone(node, admitted.sandbox_id, timeout=120)

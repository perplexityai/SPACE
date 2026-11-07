"""The dead-node verb in live mode reconciles a dead Kind worker's rows and node, and leaves a same-name replacement's sandboxes alone."""

import space_smoke as smoke
from space_smoke.dead_node import (
    ABSENCE_WINDOW_ENV,
    EXPECTED_OUTCOME,
    INSTANCE_AUTHORITY_ENV,
    MODE_ENV,
    PAUSED,
    STOPPED,
    SUSPENDING_LOCAL,
    SUSPENDED_REMOTE,
    TICK_ENV,
    UPLOAD_SETTLE_ENV,
    created_states,
)

# The verb's ten-minute absence and upload windows, shortened to seconds by
# the test hooks; its five-minute tick to five seconds.
WINDOW_SECONDS = "20"
TICK_SECONDS = 5
# Live ticks the replacement's sandbox must survive untouched.
REPLACEMENT_TICKS = 6


# The row rules, the guards and the races are crate tests
# (`dead_node/tests/live.rs`, `dead_node/store/tests.rs`). This scenario is
# the seam: a worker that really died, its rows written and stamped by the
# product's own lifecycle paths, judged through Kind's fake EC2 by instance
# ID, reconciled live, and then replaced under the same name by a worker
# whose sandboxes the verb must not touch. It stops a worker, so it runs only
# by name.
@smoke.scenario(
    "local.dead_node_live_reconciles_a_dead_worker",
    # Two worker re-registrations, six sandboxes, two suspends (one with
    # uploads blocked), a worker stop, the heartbeat going stale, three
    # controller rollouts and a worker start under a new instance.
    sandbox=smoke.SandboxConfig(timeout=3600),
    timeout_minutes=60,
    test_hooks=(
        "SPACE_DEAD_NODE_TEST_TICK_SECS, SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS and "
        "SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS, which shorten the verb's tick and windows "
        "to seconds, and SPACE_DEAD_NODE_TEST_INSTANCE_AUTHORITY, which selects its EC2 arm"
    ),
    ignored="stops a Kind worker and replaces it, too invasive for the per-merge set",
)
def dead_node_live_reconciles_a_dead_worker(test: smoke.KindEcw) -> None:
    """With a worker's container stopped, its Node deleted and its fake AWS
    instance terminated, the dead-node verb against Kind's fake EC2 reports
    the worker `lost` in `dry-run` with every row's predicted outcome and no
    row changed; in `live` it leaves each row in its rule's state (started,
    paused, stopped and suspending with the node's only copy `failed`, the
    worker kept as their last target; suspended with a durable snapshot
    `suspended` and unplaced) and deletes the worker's node row. Started
    again under the same name with a new instance, the replacement registers
    again, and a sandbox placed on it, stamped with the new instance, keeps
    every column while the verb keeps ticking live."""

    dead = test.dead_node

    with test.phase("choose a worker that runs no stack pod, and a reference"):
        dead.choose_worker()

    with test.phase("re-register both under fake AWS providerIDs"):
        dead.give_fake_instances()

    with test.phase("create one sandbox per state on that worker"):
        with dead.only_worker_schedulable():
            created = {state: test.sandboxes.create().sandbox_id for state in created_states()}
        dead.adopt_fixture_sandbox_if_resident()
        for state, sandbox_id in created.items():
            dead.adopt(sandbox_id, state)

    with test.phase("suspend one durably, stop one, pause one, suspend one with uploads blocked"):
        dead.suspend_remote(created[SUSPENDED_REMOTE])
        dead.act(created[STOPPED], "stop", "stopped")
        dead.act(created[PAUSED], "pause", "paused")
        dead.block_uploads()
        dead.suspend_local_only(created[SUSPENDING_LOCAL])

    with test.phase("stop the worker, delete its Node, terminate its instance"):
        dead.kill_worker()

    with dead.scoped_config() as configure:
        with test.phase("dry-run against Kind's fake EC2: lost, each row as predicted, none changed"):
            configure(
                {
                    MODE_ENV: "dry-run",
                    TICK_ENV: str(TICK_SECONDS),
                    ABSENCE_WINDOW_ENV: WINDOW_SECONDS,
                    UPLOAD_SETTLE_ENV: WINDOW_SECONDS,
                    INSTANCE_AUTHORITY_ENV: "ec2",
                }
            )
            dead.expect_status(
                mode="dry-run",
                verdict="lost",
                reason="instance_and_node_gone",
                outcome_of=EXPECTED_OUTCOME.__getitem__,
            )
            dead.expect_rows_unchanged()

        with test.phase("live: each row in its rule's state, the node row deleted"):
            configure({MODE_ENV: "live"})
            dead.expect_live_reconciled()

        with test.phase("replace the worker under the same name with a new instance"):
            dead.replace_worker()

        with test.phase("a sandbox on the replacement survives the live verb untouched"):
            with dead.only_worker_schedulable():
                replacement_sandbox = test.sandboxes.create().sandbox_id
            dead.adopt_replacement_sandbox(replacement_sandbox)
            dead.expect_replacement_untouched(
                ticks=REPLACEMENT_TICKS, tick_seconds=TICK_SECONDS
            )

    with test.phase("delete the sandbox whose suspend the dead worker held"):
        dead.delete_suspending_local()

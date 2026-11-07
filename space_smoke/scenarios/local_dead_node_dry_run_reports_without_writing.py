"""The dead-node verb's dry run judges a dead Kind worker lost and reports every row's outcome without writing."""

import space_smoke as smoke
from space_smoke.dead_node import (
    ABSENCE_WINDOW_ENV,
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
TICK_SECONDS = "5"


# The verdict and outcome rules are crate tests (`dead_node/verdict/tests.rs`,
# `dead_node/outcome/tests.rs`, and `dead_node/tests.rs`, which also checks
# that a dry-run tick changes no table but its report). This scenario is the
# seam: a worker that really died, judged from the reflector, the API server,
# the heartbeat and Kind's fake EC2, over rows the product's own lifecycle
# paths wrote. It stops a worker, so it runs only by name.
@smoke.scenario(
    "local.dead_node_dry_run_reports_without_writing",
    # Five sandboxes, two suspends (one with uploads blocked), a worker stop,
    # the heartbeat going stale, two controller rollouts and a worker start.
    sandbox=smoke.SandboxConfig(timeout=3600),
    timeout_minutes=45,
    test_hooks=(
        "SPACE_DEAD_NODE_TEST_TICK_SECS, SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS and "
        "SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS, which shorten the verb's tick and windows "
        "to seconds, and SPACE_DEAD_NODE_TEST_INSTANCE_AUTHORITY, which selects its EC2 arm"
    ),
    ignored="stops a Kind worker and starts it again, too invasive for the per-merge set",
)
def dead_node_dry_run_reports_without_writing(test: smoke.KindEcw) -> None:
    """With a worker's container stopped and its Node deleted, the dead-node
    verb in `dry-run` judges the worker lost, by `node_deleted` on Kind's
    `kube-delete` terminator and by `instance_and_node_gone` against its fake
    EC2, and the admin route lists every sandbox the scenario put on it under
    the outcome its state predicts (started, paused, stopped, and suspending
    with its upload blocked so the node holds the only copy, `lost`;
    suspended with a durable snapshot `stays_suspended`),
    with counts for exactly those rows; and no sandbox row changes."""

    dead = test.dead_node

    with test.phase("choose a worker that runs no stack pod"):
        dead.choose_worker()

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

    with test.phase("stop the worker's container and delete its Node"):
        dead.kill_worker()

    with dead.scoped_dry_run() as configure:
        with test.phase("dry-run with the kube-delete terminator: lost, each row as predicted"):
            configure(
                {
                    MODE_ENV: "dry-run",
                    TICK_ENV: TICK_SECONDS,
                    ABSENCE_WINDOW_ENV: WINDOW_SECONDS,
                    UPLOAD_SETTLE_ENV: WINDOW_SECONDS,
                }
            )
            dead.expect_dry_run(reason="node_deleted")
            dead.expect_rows_unchanged()

        with test.phase("dry-run against Kind's fake EC2: lost by the EC2 arm, each row as predicted"):
            configure({INSTANCE_AUTHORITY_ENV: "ec2"})
            dead.expect_dry_run(reason="instance_and_node_gone")
            dead.expect_rows_unchanged()

    with test.phase("start the worker again"):
        dead.restart_worker()

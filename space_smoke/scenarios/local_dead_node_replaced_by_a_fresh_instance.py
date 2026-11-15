"""The dead-node verb reconciles a dead Kind worker, and a same-name replacement booted without the dead instance's node-local state knows none of its sandboxes when it registers."""

import space_smoke as smoke
from space_smoke.dead_node import (
    ABSENCE_WINDOW_ENV,
    EXPECTED_OUTCOME,
    MODE_ENV,
    PAUSED,
    STARTED_ONCE_SUSPENDED,
    STOPPED,
    SUSPENDING_LOCAL,
    SUSPENDED_REMOTE,
    TERMINATOR_KIND_ENV,
    TICK_ENV,
    UNSELECTED,
    UPLOAD_SETTLE_ENV,
    fresh_replacement_states,
)

# The verb's ten-minute absence and upload windows, shortened to seconds by
# the test hooks; its five-minute tick to five seconds.
WINDOW_SECONDS = "20"
TICK_SECONDS = 5
# Live ticks the replacement's sandbox must survive untouched.
REPLACEMENT_TICKS = 6
# A delete on the new instance has no dead suspend to wait behind.
DELETE_SECONDS = 300


# The row rules, the guards and the races are crate tests
# (`dead_node/tests/live.rs`, `dead_node/store/tests.rs`). This scenario is
# the seam: a worker that really died, its rows written and stamped by the
# product's own lifecycle paths, judged through Kind's fake EC2 by instance
# ID in `dry-run` and then reconciled `live`, and replaced under the same
# name. A dead instance never comes back with its disk, so the worker comes
# back under the same name with a new instance and fresh node-local state
# (the node layer's space-kind-fresh-instance.service): nothing under
# /space/run (no node service database, run directory or socket), an empty
# node artifact store and a freshly formatted cache. It keeps what the dead
# instance's node-manager installed (the service binaries and releases under
# /space/bin, the env under /space/config and the enabled runtime-supervisor
# unit) and the kubelet's and containerd's state, which a real new instance
# would not have. It stops a worker, so it runs only by name.
@smoke.scenario(
    "local.dead_node_replaced_by_a_fresh_instance",
    # Two worker re-registrations, seven sandboxes, three suspends (one with
    # uploads blocked) and a wake, a worker stop, the heartbeat going stale,
    # four controller rollouts, a worker start as a new instance and a wake
    # there.
    sandbox=smoke.SandboxConfig(timeout=3600),
    timeout_minutes=60,
    test_hooks=(
        "SPACE_DEAD_NODE_TEST_TICK_SECS, SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS and "
        "SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS, which shorten the verb's tick and windows "
        "to seconds"
    ),
    tier="manual",
    manual_reason="stops a Kind worker and replaces it, too invasive for the per-merge set",
)
def dead_node_replaced_by_a_fresh_instance(test: smoke.KindEcw) -> None:
    """With a worker's container stopped, its Node deleted and its fake AWS
    instance terminated, the dead-node verb in `dry-run` judges the worker
    `unknown` (`no_instance_authority`) on Kind's `kube-delete` terminator,
    selecting no row; with the `aws` terminator, against Kind's fake EC2, it
    reports the worker `lost` with every row's predicted outcome, and no row
    changes in either. In `live`, against the fake EC2, it deletes the
    worker's node row with no tombstone: the durably
    suspended sandbox stays `suspended`, unplaced; the started, paused,
    stopped and still-suspending ones are written off `failed`, and the
    started one with a snapshot from an earlier suspend keeps it. The worker
    then comes back under the same name as a new instance, with fresh
    node-local state: it registers again, its sandbox-manager knows none of
    the dead instance's sandboxes, a sandbox placed on it keeps every column
    while the verb keeps ticking live, and the suspended sandbox wakes on it.
    The still-suspending sandbox deletes with nothing to wait behind; no
    worker's sandbox-manager holds a row of a sandbox the verb wrote off or
    the scenario deleted, or a quarantined row of the scenario's sandboxes;
    none of their control-plane rows is quarantined; and no live
    control-plane row but the woken sandbox's, the replacement's and the
    fixture's own is placed on or targets the node."""

    dead = test.dead_node

    with test.phase("choose a worker that runs no stack pod, and a reference"):
        dead.choose_worker()
        dead.require_fresh_instance_support()

    with test.phase("re-register both under fake AWS providerIDs"):
        dead.give_fake_instances()

    with test.phase("create one sandbox per state on that worker"):
        with dead.only_worker_schedulable():
            created = {state: test.sandboxes.create() for state in fresh_replacement_states()}
        dead.adopt_fixture_sandbox_if_resident()
        for state, sandbox in created.items():
            dead.adopt(sandbox.sandbox_id, state)

    with test.phase("suspend and wake one, suspend one durably, stop one, pause one"):
        with dead.only_worker_schedulable():
            dead.suspend_and_wake(created[STARTED_ONCE_SUSPENDED])
        dead.suspend_remote_settled(created[SUSPENDED_REMOTE].sandbox_id)
        dead.act_settled(created[STOPPED].sandbox_id, "stop", "stopped")
        dead.act_settled(created[PAUSED].sandbox_id, "pause", "paused")

    with test.phase("suspend one with uploads blocked"):
        dead.block_uploads()
        dead.suspend_local_only(created[SUSPENDING_LOCAL].sandbox_id)
        dead.note_snapshots()

    with test.phase("stop the worker, delete its Node, terminate its instance"):
        dead.kill_worker(fresh_node_state=True)

    with dead.scoped_config() as configure:
        with test.phase("dry-run with the kube-delete terminator: unknown, no row selected"):
            configure(
                {
                    MODE_ENV: "dry-run",
                    TICK_ENV: str(TICK_SECONDS),
                    ABSENCE_WINDOW_ENV: WINDOW_SECONDS,
                    UPLOAD_SETTLE_ENV: WINDOW_SECONDS,
                    TERMINATOR_KIND_ENV: "kube-delete",
                }
            )
            dead.expect_status(
                mode="dry-run",
                verdict="unknown",
                reason="no_instance_authority",
                outcome_of=lambda _state: UNSELECTED,
            )
            dead.expect_rows_unchanged()

        with test.phase("dry-run against Kind's fake EC2: lost, each row as predicted, none changed"):
            configure({TERMINATOR_KIND_ENV: "aws"})
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
            dead.expect_no_tombstone()
            dead.expect_snapshots_kept()

        with test.phase("replace the worker with a new instance under the same name"):
            dead.replace_worker()
            dead.expect_fresh_replacement()

        with test.phase("a sandbox on the new instance survives the live verb untouched"):
            with dead.only_worker_schedulable():
                replacement_sandbox = test.sandboxes.create().sandbox_id
            dead.adopt_replacement_sandbox(replacement_sandbox)
            dead.expect_replacement_untouched(
                ticks=REPLACEMENT_TICKS, tick_seconds=TICK_SECONDS
            )

        with test.phase("the suspended sandbox wakes on the new instance"):
            with dead.only_worker_schedulable():
                dead.wake_on_worker(created[SUSPENDED_REMOTE])

    with test.phase("delete the sandbox whose suspend the dead worker held"):
        dead.delete_suspending_local(delete_timeout=DELETE_SECONDS)

    with test.phase("nothing of the dead worker lingers"):
        dead.expect_nothing_lingers(kept=[created[SUSPENDED_REMOTE].sandbox_id])

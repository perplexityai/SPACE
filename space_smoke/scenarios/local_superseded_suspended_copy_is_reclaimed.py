"""A suspended copy left behind by a cross-node restore is reclaimed."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-superseded-copy.txt"
MARKER = "space-smoke-superseded-copy-ok"


# When a suspended sandbox restores on another node, the old node keeps its
# suspended copy. Restores read only the newest execution's node and the newest
# remote snapshot, so nothing ever reads that copy again. Before the superseded
# partition, only cold-sandbox eviction (disk pressure plus a long idle floor)
# or deleting the sandbox removed it.
#
# This scenario forces the move with the same cordon as
# `local.suspend_restore_cross_node_from_object_storage`, then requires the
# copy on the source node to disappear through node-ctl's facet sync. Kind
# also runs cold-sandbox eviction with a short minimum idle, so the fixture
# reads sbx-manager's log to make sure that loop is not what removed it.
# The sandbox must keep its state and restore again afterwards.
@smoke.scenario(
    "local.superseded_suspended_copy_is_reclaimed",
    sandbox=smoke.SandboxConfig(timeout=900),
)
def superseded_suspended_copy_is_reclaimed(test: smoke.KindEcw) -> None:
    """After a cross-node restore, the source node's suspended copy is
    evicted by node-ctl, and the sandbox keeps its state and still restores."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    restore = test.cross_node_restore
    eviction = test.cold_eviction

    with test.phase("write state and record the source node before suspend"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before suspending the sandbox",
            command_stderr=written.stderr,
        )
        source = restore.current_placement()

    with test.phase("suspend durably"):
        test.pause()
        restore.wait_until_restore_portable()
        eviction.expect_local_presence_present(source.node_name)

    with restore.cordoned(source.node_name):
        with test.phase("restore while the source worker is unschedulable"):
            test.reconnect_by_id()
            landed = restore.current_placement()
            restore.expect_restored_from_object_storage(source=source, landed=landed)

    with test.phase("node-ctl reclaims the suspended copy on the source node"):
        eviction.wait_until_superseded_copy_evicted(source.node_name)

    with test.phase("the copy on the landing node is untouched"):
        eviction.expect_local_presence_present(landed.node_name)
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

    with test.phase("the sandbox still suspends and restores with its state"):
        test.pause()
        test.reconnect_by_id()
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

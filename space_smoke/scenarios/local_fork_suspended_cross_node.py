"""A suspended source forks onto another worker with intact, independent disk data."""

import space_smoke as smoke

PATH = "/tmp/space-smoke-cold-fork.txt"
MARKER = "durable-fork-state"


@smoke.scenario(
    "local.fork_suspended_cross_node", sandbox=smoke.SandboxConfig(timeout=900)
)
def fork_suspended_cross_node(test: smoke.KindEcw) -> None:
    """Off-source placement preserves disk data without resuming the suspended source."""

    restore = test.cross_node_restore
    with test.phase("write state and suspend the source durably"):
        test.sandbox.files.write(PATH, MARKER)
        source = restore.current_placement()
        test.pause()
        restore.wait_until_restore_portable()

    with restore.cordoned(source.node_name):
        with test.phase("fork the suspended source onto another worker"):
            child = test.sandboxes.fork(test.sandbox).sandbox
            landed = restore.current_placement(child)
            test.verify(
                landed.node_name != source.node_name,
                "fork did not move off the cordoned source worker",
                source_node=source.node_name,
                child_node=landed.node_name,
            )
            test.verify(child.files.read(PATH) == MARKER, "cross-node fork lost disk state")
            child.files.write(PATH, "child-only")
            test.verify(
                test.sandbox.get_info().state == smoke.SandboxState.PAUSED,
                "fork unexpectedly resumed its suspended source",
            )

    with test.phase("the source restores with its original data"):
        test.reconnect_by_id()
        test.verify(test.sandbox.files.read(PATH) == MARKER, "fork changed suspended source data")
        test.verify(child.files.read(PATH) == "child-only", "source restore changed child data")

"""A sandbox bound to Firecracker 1.17.0 keeps running it through suspend, restore and cross-node restore."""

import shlex

import space_smoke as smoke

ENVIRONMENT = "fc-1.17.0-linux-6.1.155"
PATH = "/tmp/space-smoke-selected-environment.txt"


# Setup admits the environment on every node and builds seed bound to it
# under no tag (`space_smoke/execution_environment.py`); the sandbox is the
# ECW create of that build's digest, which records the build's binding. Each
# step reads what the running VM's own Firecracker executable reports, on
# whichever node it runs.
@smoke.scenario(
    "local.selected_environment_survives_suspend_and_cross_node_restore",
    sandbox=smoke.SandboxConfig(timeout=900, kind_execution_environment=ENVIRONMENT),
    tier="manual",
    manual_reason=(
        "Rolls every node-manager to admit Firecracker 1.17.0 / Linux 6.1.155, installs that "
        "runtime on every node from its upstream release, and builds a seed: run it on a bench "
        "no other scenario shares while it runs."
    ),
    # Worst case: admission up to 38 minutes (two DaemonSet rollouts of up to
    # about 662 s each, 600 s of installs, 300 s of evidence), a 20-minute seed
    # build, about 21 minutes of lifecycle (three runtime reads of up to about
    # three minutes each included), and the withdrawal's two rollouts:
    # about 102 minutes, inside the 115 the budget leaves after cleanup.
    timeout_minutes=150,
    # On a failure, cleanup deletes the sandbox, waits up to five minutes for a
    # running seed build before deleting its release, and owns both rollouts.
    cleanup_timeout_seconds=2100,
)
def selected_environment_survives_suspend_and_cross_node_restore(test: smoke.KindEcw) -> None:
    """The sandbox runs the selected environment after create, after a
    suspend and restore, and after a restore onto another node from object
    storage, with its filesystem intact each time. The bench's node-manager
    allowlist is back to what it was once the sandbox and seed are gone."""

    environment = test.execution_environment
    restore = test.cross_node_restore
    sandbox_id = test.sandbox.sandbox_id
    marker = f"selected-environment-{sandbox_id}"

    with test.phase("the created sandbox runs the selected environment"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(written.exit_code == 0, "failed to write state before suspending", command_stderr=written.stderr)
        environment.expect_running(sandbox_id, step="after create")
        first = restore.current_placement()
        test.record("placement_after_create", first.node_name)

    with test.phase("suspend and restore"):
        test.pause()
        test.reconnect_by_id()

    with test.phase("the restored sandbox runs the selected environment with its state"):
        test.expect_command(test.run_command_when_ready(f"cat {shlex.quote(PATH)}"), stdout=marker)
        environment.expect_running(sandbox_id, step="after restore")
        source = restore.current_placement()
        test.record("placement_after_restore", source.node_name)

    with test.phase("suspend until the snapshot is durable"):
        test.pause()
        restore.wait_until_restore_portable()

    with restore.cordoned(source.node_name):
        with test.phase("restore while the source worker is unschedulable"):
            test.reconnect_by_id()

        with test.phase("the restore pulled its snapshot from object storage onto another node"):
            landed = restore.current_placement()
            restore.expect_restored_from_object_storage(source=source, landed=landed)

    with test.phase("the cross-node restore runs the selected environment with its state"):
        test.expect_command(test.run_command_when_ready(f"cat {shlex.quote(PATH)}"), stdout=marker)
        environment.expect_running(sandbox_id, step="after cross-node restore")

    with test.phase("delete the sandbox"):
        test.delete_sandbox()

    with test.phase("withdraw the environment from the bench"):
        environment.withdraw()

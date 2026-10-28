"""A sandbox-manager restart leaves a teardown suspend that waits on its backup to that teardown."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-teardown-restart.txt"
MARKER = "space-smoke-teardown-restart-ok"


# The window is a teardown suspend whose capture is staged but not backed up:
# its row is `suspending` with the capture pinned while the runtime waits for
# the upload. Uploads are held by scaling the object store to zero, with its
# scale-back armed first (`space_smoke.pending_backup`), and the suspend runs
# as a native operation because the ECW pause answers only once durable. The
# restart replaces only sandbox-manager's pod, so the VM keeps running under
# the host runtime-supervisor, as in a sandbox-manager rollout. The store
# returns once the restart's hold check is done, and the transfer's retry
# ladder decides when the upload lands. Do not run concurrently with another
# suspend-bearing smoke.
@smoke.scenario(
    "local.sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown",
    # The phases' worst cases add to 2610 s: the object store scale-down
    # 120 s, the snapshot in flight 180 s, the row reaching `suspending`
    # 120 s, the restart 1200 s (the pod delete and its replacement's
    # readiness are each bounded at 600 s), the hold 30 s, the object store's
    # rollout back 180 s, the upload's retry ladder 600 s and the row reaching
    # `suspended`, or its eviction once backed, 180 s. Create, the wake and
    # each poll's own read come on top.
    sandbox=smoke.SandboxConfig(timeout=3000),
)
def sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown(test: smoke.KindEcw) -> None:
    """With the suspend's backup held, restarting the worker's sandbox-manager
    leaves the sandbox's row `suspending`, rather than marking it suspended
    behind its teardown; once uploads return, the suspend operation succeeds
    and the sandbox wakes with the state it had."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    uploads = test.pending_backup
    restart = test.teardown_restart

    with test.phase("write state before the suspend"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before suspending the sandbox",
            command_stderr=written.stderr,
        )

    with test.phase("block uploads: scale the object store to zero"):
        uploads.block_uploads()

    with test.phase("suspend as an operation; the teardown waits on its backup"):
        uploads.suspend_async()
        uploads.expect_snapshot_in_flight()
        restart.expect_row("suspending")

    with test.phase("restart the worker's sandbox-manager"):
        restart.restart_sandbox_manager()

    with test.phase("the restart leaves the row to its teardown"):
        restart.expect_row_holds("suspending")

    with test.phase("restore uploads; the teardown finishes the suspend"):
        uploads.restore_uploads()
        uploads.expect_suspend_succeeded(timeout=600)
        restart.expect_row_finished(timeout=180)

    with test.phase("the sandbox wakes with its state"):
        test.wake_from_durable_suspend()
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

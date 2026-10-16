"""A late kubelet retry of a rolled-back restore keeps the original snapshot."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-restore-rollback.txt"
MARKER = "space-smoke-restore-rollback-v1"


# The contract, end to end: a restore whose prepare fails after its
# activation claim is rolled back, and kubelet's later retries of the same
# activation attempt are refused. So the attempt never goes live, deleting its
# pod publishes no snapshot, and the next connect restores the original
# snapshot. Unit tests cover each refusal; only a real kubelet retries late.
# See `space_smoke.restore_rollback` for why the snapshot rows and the restored
# artifact, not the user's file, are the verdict, and why the refusals
# themselves are evidence only.
@smoke.scenario(
    "local.restore_rollback_late_retry_keeps_original_snapshot",
    sandbox=smoke.SandboxConfig(timeout=900),
)
def restore_rollback_late_retry_keeps_original_snapshot(test: smoke.KindEcw) -> None:
    """After a faulted restore rolls back and kubelet retries it late, the
    sandbox still has exactly its original snapshot, the next restore is
    planned from that snapshot and activates its artifact, and the user's
    file and exec work."""

    rollback = test.restore_rollback

    with test.phase("write a marker and suspend"):
        written = test.run_command_when_ready(f"printf %s {MARKER} > {shlex.quote(PATH)}")
        test.verify(written.exit_code == 0, "failed to write the marker", command_stderr=written.stderr)
        test.pause()
        original = rollback.original_snapshot()

    with test.phase("arm one restore-prepare fault"):
        rollback.arm_fault()

    with test.phase("connect: the restore prepare fails and rolls back"):
        attempt = rollback.connect_through_fault()

    with test.phase("kubelet retries the rolled-back attempt late"):
        rollback.hold_for_late_retry(attempt)

    with test.phase("tear down the rolled-back attempt's pod"):
        rollback.tear_down_attempt(attempt)

    with test.phase("connect again"):
        test.reconnect_by_id()
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=MARKER)
        exec_check = test.run_command_when_ready("echo exec-ok")
        test.expect_command(exec_check, stdout="exec-ok\n")

    with test.phase("the original snapshot is the only one, and it was restored"):
        rollback.expect_original_snapshot_restored(original, attempt)

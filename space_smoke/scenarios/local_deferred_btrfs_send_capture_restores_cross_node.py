"""A deferred btrfs-send capture writes no send file and still restores elsewhere."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-deferred-capture.txt"
MARKER = "space-smoke-deferred-capture-ok"


# With `SPACE_SNAPSHOT_STREAM_BTRFS_SEND=true` (the Kind chart sets it) a
# btrfs-send suspend retains its source as read-only btrfs snapshots and
# volume-manager streams the upload from them under node-manager leases; the
# node never writes `rootfs.btrfs-send`. That needs volume-manager's own
# identity (`space-volmgr`), which node-manager's lease RPCs authorize and
# which the Kind chart now runs, and sandbox-manager never traverses the
# bundle. The proof is sandbox-manager's own encode line (`deferred=true`,
# no stream bytes), volume-manager's completed `push_btrfs_send` job for the
# snapshot (a materialized fallback that bookkeeping later reclaims leaves the
# same directory behind), the canonical snapshot directory holding the bundle
# and no send file before and after the upload completes, and the durable path
# end to end: the same cross-node restore
# `local.suspend_restore_cross_node_from_object_storage` proves, now sourced
# from a stream that was never a file on the source node.
@smoke.scenario(
    "local.deferred_btrfs_send_capture_restores_cross_node",
    # A cross-node restore pulls the full snapshot from the object store; give
    # a slow KVM bench room.
    sandbox=smoke.SandboxConfig(timeout=900),
)
def deferred_btrfs_send_capture_restores_cross_node(test: smoke.KindEcw) -> None:
    """A suspend under deferred capture retains its source and writes no
    `rootfs.btrfs-send`, the backup streams from the retained source, and the
    sandbox restores from object storage onto another node with its state
    intact."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    codec = test.suspend_codec
    restore = test.cross_node_restore

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

    with test.phase("the worker resolves an omitted suspend format to the stream codec"):
        codec.require_btrfs_send_default(source.node_name)

    with test.phase("suspend without naming a format"):
        suspended_at = codec.mark_since()
        test.pause()

    with test.phase("the suspend was sealed as btrfs-send-v1 and retained its source"):
        codec.expect_suspend_codec(since_time=suspended_at)
        codec.expect_deferred_capture(source.node_name, since_time=suspended_at)
        codec.expect_retained_source_without_send_file(source.node_name)

    with test.phase("wait until the durable snapshot is what a restore would select"):
        restore.wait_until_restore_portable()

    with test.phase("volume-manager streamed the backup from the retained source"):
        codec.expect_streamed_upload(source.node_name, since_time=suspended_at)

    with test.phase("the upload left no send file behind"):
        codec.expect_retained_source_without_send_file(source.node_name)

    with restore.cordoned(source.node_name):
        with test.phase("restore while the source worker is unschedulable"):
            test.reconnect_by_id()

        with test.phase("the restore pulled its snapshot from object storage"):
            landed = restore.current_placement()
            restore.expect_restored_from_object_storage(source=source, landed=landed)

    with test.phase("state survives the cross-node restore of a deferred capture"):
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

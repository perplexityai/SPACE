"""A corrupted btrfs-send stream in object storage is refused, not restored."""

import space_smoke as smoke


# `btrfs receive` is the codec's integrity check on the landing worker: a
# stream whose bytes do not match what `btrfs send` produced aborts the
# receive, and sbx-manager must fail the restore with nothing left behind —
# no `instance`, and no half-received `instance.ro` for a later attempt to
# trip over.
#
# Forcing, kind-only: one byte of the durable `rootfs.btrfs-send` object is
# flipped in the bench's loopback object store (the same guard the retention
# scenario's storage delete uses refuses a remote endpoint). The source is
# cordoned so the restore cannot reuse its retained local artifact and has to
# pull the corrupted copy. The sandbox is deleted at scope end, and the object
# with it.
@smoke.scenario(
    "local.btrfs_send_restore_rejects_corrupted_stream",
    # The corruption goes through the AWS CLI against the loopback store;
    # declare it so an isolated run refuses in preflight, not after it has
    # created and suspended a sandbox.
    required_tools=("aws",),
    sandbox=smoke.SandboxConfig(timeout=1200),
)
def btrfs_send_restore_rejects_corrupted_stream(test: smoke.KindEcw) -> None:
    """A sandbox suspended with the production stream codec whose durable
    stream is corrupted in object storage fails to restore onto another worker:
    the client sees the failure, sbx-manager reports the restore failed at the
    receive with node-manager's aborted `btrfs receive`,
    and the target holds no instance or partial receive."""

    codec = test.suspend_codec
    restore = test.cross_node_restore

    with test.phase("locate the source worker and the target"):
        source = restore.current_placement()
        source_worker = source.node_name
        target_node = codec.other_sandbox_worker(source.node_name)
        test.record("target_node", target_node)

    with test.phase("the worker resolves an omitted suspend format to the stream codec"):
        codec.require_btrfs_send_default(source_worker)

    with test.phase("suspend without naming a format"):
        suspended_at = codec.mark_since()
        test.pause()

    with test.phase("the suspend was sealed as btrfs-send-v1"):
        codec.expect_suspend_codec(since_time=suspended_at)

    with test.phase("wait until the durable snapshot is what a restore would select"):
        restore.wait_until_restore_portable()

    with test.phase("corrupt one byte of the durable stream"):
        codec.corrupt_stream()

    with restore.cordoned(source.node_name):
        since = codec.mark_since()
        with test.phase("the restore fails at the client"):
            codec.expect_reconnect_fails()

        with test.phase("sbx-manager failed the restore at the receive, per its own event"):
            # The stage names the pull; the error kind says it failed at
            # node-manager's btrfs receive, not at the parent-template lookup.
            codec.expect_restore_failed(
                reason="pull_rootfs", error_kind="btrfs_receive", since_time=since
            )

        with test.phase("no instance and no partial receive remain on the target"):
            codec.expect_no_instance(target_node)

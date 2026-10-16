"""A btrfs send the kernel restarts mid-pipe is rejected, retried, and restores."""

import shlex

import space_smoke as smoke

PAYLOAD = "/var/tmp/space-smoke-restarted-send.bin"
# Incompressible bytes so the deferred capture's send outlasts the
# interrupter's poll and blocks on the pipe while volume-manager is stopped.
PAYLOAD_MIB = 64
MIN_PAYLOAD_MIB = 16


# A deferred capture's upload reads `BTRFS_IOC_SEND` output from a pipe that
# node-manager relays to volume-manager. When the pipe is full and the sending
# thread is stopped and continued (a stop signal or cgroup freeze does the
# same), `pipe_write` returns -ERESTARTSYS and the kernel restarts the whole
# ioctl. The restart appends a complete stream after the partial one and still
# reports success, so without a check the upload stores a stream that
# `btrfs receive` rejects with "crc32 mismatch in command".
#
# Forcing, kind-only: the node's operator shell stops volume-manager, so the
# send fills the pipe and blocks; stops and continues node-manager; then
# continues volume-manager, once, while the send runs. node-manager's relay
# checks the stream's framing and refuses it; volume-manager's attempt fails
# retriable and the retry streams a clean send. The proof is that failed
# attempt and a cross-node restore from object storage, which runs
# `btrfs receive` on the stored stream, with the payload's digest intact.
@smoke.scenario(
    "local.restarted_btrfs_send_is_rejected_and_retried",
    # The upload retries after the rejection and a cross-node restore pulls
    # the payload from the object store; give a slow KVM bench room.
    sandbox=smoke.SandboxConfig(timeout=1200),
)
def restarted_btrfs_send_is_rejected_and_retried(test: smoke.KindEcw) -> None:
    """A deferred capture whose kernel send is interrupted while blocked on its
    pipe fails that upload attempt at node-manager's framing check instead of
    storing a concatenated stream; the retried upload restores onto another
    worker with the sandbox's data intact."""

    codec = test.suspend_codec
    restore = test.cross_node_restore

    with test.phase("write an incompressible payload and record its digest"):
        free = test.run_command_when_ready(
            f"df -Pm {shlex.quote('/var/tmp')} | awk 'NR==2 {{print $4}}'"
        )
        test.verify(
            free.exit_code == 0 and free.stdout.strip().isdigit(),
            "could not measure the guest's free space",
            command_stdout=free.stdout,
            command_stderr=free.stderr,
        )
        payload_mib = min(PAYLOAD_MIB, int(free.stdout.strip()) * 7 // 10)
        test.verify(
            payload_mib >= MIN_PAYLOAD_MIB,
            "the guest has too little free space for a payload the send blocks on",
            free_mib=free.stdout.strip(),
        )
        test.record("payload_mib", payload_mib)
        written = test.run_command_when_ready(
            f"head -c {payload_mib}M /dev/urandom > {shlex.quote(PAYLOAD)} && sync "
            f"&& sha256sum {shlex.quote(PAYLOAD)} | cut -d' ' -f1",
            timeout=300,
        )
        test.verify(
            written.exit_code == 0 and len(written.stdout.strip()) == 64,
            "failed to write the payload before suspending the sandbox",
            command_stderr=written.stderr,
        )
        digest = written.stdout.strip()
        source = restore.current_placement()

    with test.phase("the worker resolves an omitted suspend format to the stream codec"):
        codec.require_btrfs_send_default(source.node_name)

    with test.phase("suspend while node-manager is stopped and continued mid-send"):
        suspended_at = codec.mark_since()
        with codec.interrupting_btrfs_send(source.node_name):
            test.pause()
            codec.expect_deferred_capture(source.node_name, since_time=suspended_at)
            restore.wait_until_restore_portable(timeout=900)
            codec.expect_streamed_upload(source.node_name, since_time=suspended_at)

    with test.phase("node-manager rejected the restarted stream and the upload retried"):
        codec.expect_rejected_send_stream(source.node_name, since_time=suspended_at)

    with restore.cordoned(source.node_name):
        with test.phase("restore while the source worker is unschedulable"):
            test.reconnect_by_id()

        with test.phase("the restore pulled its snapshot from object storage"):
            landed = restore.current_placement()
            restore.expect_restored_from_object_storage(source=source, landed=landed)

    with test.phase("the payload survives the cross-node restore"):
        observed = test.run_command_when_ready(
            f"sha256sum {shlex.quote(PAYLOAD)} | cut -d' ' -f1", timeout=120
        )
        test.expect_command(observed, stdout=f"{digest}\n")

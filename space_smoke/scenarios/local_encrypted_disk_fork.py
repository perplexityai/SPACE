"""Encrypted disk forks retain data without depending on the source's key lifetime."""

import secrets

import space_smoke as smoke

DISK_PATH = "/tmp/space-smoke-encrypted-fork.bin"
MEMORY_PATH = "/dev/shm/space-smoke-encrypted-fork.txt"
CONTENT = bytes(range(256)) * 4096
CHILD_CONTENT = "independent encrypted child"
BOOT_ID = "cat /proc/sys/kernel/random/boot_id"


@smoke.scenario(
    "local.encrypted_disk_fork",
    sandbox=smoke.SandboxConfig(timeout=900, template="seed", kind_byok=True),
)
def encrypted_disk_fork(test: smoke.KindEcw) -> None:
    """Fork a sealed encrypted disk, replay it, and restore independently of its source."""
    verify_encrypted_disk_fork(test, suspended=True)


def verify_encrypted_disk_fork(test: smoke.KindEcw, *, suspended: bool) -> None:
    source = test.sandbox
    key = f"space-smoke-encrypted-fork-{secrets.token_hex(16)}"

    with test.phase("write disk and memory state before encrypted suspension"):
        source.files.write(DISK_PATH, CONTENT)
        source.files.write(MEMORY_PATH, "source memory only")
        original_boot = source.run(BOOT_ID)
        test.verify(original_boot.exit_code == 0, "source boot identity is unavailable")
        source_placement = test.cross_node_restore.current_placement()
        if suspended:
            test.pause()
            test.cross_node_restore.wait_until_restore_portable()

    with test.phase("fork only the encrypted source disk into a fresh guest"):
        with test.cross_node_restore.cordoned(source_placement.node_name):
            child = test.sandboxes.fork(source, idempotency_key=key).sandbox
            child_placement = test.cross_node_restore.current_placement(child)
            test.verify(
                child_placement.node_name != source_placement.node_name,
                "encrypted disk fork did not restore onto another worker",
            )
        test.verify(child.files.read_bytes(DISK_PATH) == CONTENT, "encrypted fork lost disk bytes")
        child_boot = child.run(BOOT_ID)
        test.verify(
            child_boot.exit_code == 0 and child_boot.stdout != original_boot.stdout,
            "disk fork did not boot with a new guest identity",
        )
        no_memory = child.run(f"test ! -e {MEMORY_PATH}")
        test.verify(no_memory.exit_code == 0, "disk fork copied the source's memory state")
        test.verify(
            source.get_info().state == (
                smoke.SandboxState.PAUSED if suspended else smoke.SandboxState.RUNNING
            ),
            "fork changed the encrypted source lifecycle",
        )

    with test.phase("retain child writes and identity on completed replay"):
        child.files.write(DISK_PATH, CHILD_CONTENT)
        replay = test.sandboxes.fork(source, idempotency_key=key).sandbox
        test.verify(replay.sandbox_id == child.sandbox_id, "encrypted replay changed identity")
        test.verify(replay.files.read(DISK_PATH) == CHILD_CONTENT, "replay reset child data")

    with test.phase("restore the original source and verify isolation"):
        if suspended:
            test.reconnect_by_id()
        test.verify(source.files.read_bytes(DISK_PATH) == CONTENT, "child changed source disk bytes")
        test.verify(source.files.read(MEMORY_PATH) == "source memory only", "source memory changed")
        restored_boot = source.run(BOOT_ID)
        test.verify(restored_boot.stdout == original_boot.stdout, "fork restarted its source")

    with test.phase("retire the source before exercising the child's encryption again"):
        test.delete_sandbox(source)
        test.wait_until_deleted(source)

    with test.phase("suspend and restore the child without the source sandbox"):
        child.pause()
        with test.cross_node_restore.cordoned(child_placement.node_name):
            restored = child.connect(timeout=900)
        test.verify(restored.files.read(DISK_PATH) == CHILD_CONTENT, "child restore lost disk data")
        restored_boot = restored.run(BOOT_ID)
        test.verify(restored_boot.stdout == child_boot.stdout, "child restore lost its VM state")

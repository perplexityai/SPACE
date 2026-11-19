"""An encrypted restore can be suspended again without losing its memory base."""

import shlex

import space_smoke as smoke

PATH = "/dev/shm/space-smoke-encrypted-resuspend.txt"
MARKER = "space-smoke-encrypted-resuspend-ok"
BOOT_ID_COMMAND = "tr -d '\\n' < /proc/sys/kernel/random/boot_id"


# The fixture enrolls a disposable BYOK organization before sandbox creation.
# Its first sealed `snapshot.luks` is the base Firecracker loads; the second suspend must update that same sparse LUKS volume. Reformatting
# it makes a Diff snapshot with no base, which resumes into broken userspace and
# surfaces later as an unreachable vsock bridge.
@smoke.scenario(
    "local.encrypted_resuspend_preserves_cow",
    sandbox=smoke.SandboxConfig(timeout=900, template="seed", kind_byok=True),
)
def encrypted_resuspend_preserves_cow(test: smoke.KindEcw) -> None:
    """A BYOK sandbox survives suspend, restore, re-suspend, and a second
    restore with one stable sparse LUKS memory base and intact guest state."""

    encrypted = test.encrypted_resuspend
    marker = f"{MARKER}-{test.sandbox.sandbox_id}"

    with test.phase("write memory-only guest state before the first suspend"):
        boot_id_result = test.run_command_when_ready(BOOT_ID_COMMAND)
        test.verify(
            boot_id_result.exit_code == 0 and bool(boot_id_result.stdout),
            "failed to read guest boot identity before encrypted suspend",
            command_stderr=boot_id_result.stderr,
        )
        boot_id = boot_id_result.stdout
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write guest state before encrypted suspend",
            command_stderr=written.stderr,
        )

    with test.phase("seal the first encrypted snapshot volume"):
        first_node = encrypted.current_node()
        test.pause()
        first = encrypted.sealed_snapshot(first_node)

    with test.phase("restore from the first encrypted snapshot"):
        test.reconnect_by_id()
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)
        restored_boot_id = test.run_command_when_ready(BOOT_ID_COMMAND)
        test.expect_command(restored_boot_id, stdout=boot_id)
        encrypted.expect_prior_executions_retired()

    with test.phase("re-suspend into the existing sparse encrypted base"):
        second_node = encrypted.current_node()
        test.pause()
        second = encrypted.sealed_snapshot(second_node)
        encrypted.expect_base_reused(first, second)

    with test.phase("restore the re-suspended guest through the vsock bridge"):
        test.reconnect_by_id()
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)
        restored_boot_id = test.run_command_when_ready(BOOT_ID_COMMAND)
        test.expect_command(restored_boot_id, stdout=boot_id)
        encrypted.expect_prior_executions_retired()

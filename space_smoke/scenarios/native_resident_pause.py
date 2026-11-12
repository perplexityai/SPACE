"""Native resident-pause lifecycle coverage."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-resident-pause.txt"
MARKER = "space-smoke-resident-pause-ok"
BOOT_ID_PATH = f"{PATH}.boot-id"


@smoke.scenario("native.resident_pause", api_surfaces=("native",))
def resident_pause(test: smoke.CommandSandboxFixture) -> None:
    """A resident pause and wake preserve filesystem state without rebooting."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"

    with test.phase("write state before resident pause"):
        written = test.run_command_when_ready(
            f"cat /proc/sys/kernel/random/boot_id > {shlex.quote(BOOT_ID_PATH)} && "
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before resident pause",
            command_stderr=written.stderr,
        )

    with test.phase("resident pause and wake sandbox"):
        test.pause_resident()
        test.wake_from_resident_pause()

    with test.phase("read state after resident wake"):
        observed = test.run_command_when_ready(
            f"cmp /proc/sys/kernel/random/boot_id {shlex.quote(BOOT_ID_PATH)} && "
            f"cat {shlex.quote(PATH)}"
        )
        test.expect_command(observed, stdout=marker)

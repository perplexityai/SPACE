"""Adopt an existing pool VM for a BYOK organization without changing pods."""

import space_smoke as smoke


@smoke.scenario(
    "local.encrypted_warm_pool",
    sandbox=smoke.SandboxConfig(
        timeout=900,
        template="seed",
        kind_byok=True,
        kind_warm_pool=smoke.KindWarmPoolConfig(vcpu=2, memory_mib=1024),
        envs={"SPACE_ENCRYPTED_POOL_PROBE": "adopted"},
    ),
)
def encrypted_warm_pool(test: smoke.KindEcw) -> None:
    """The same warm VM becomes encrypted and preserves guest state through suspend and restore."""
    with test.phase("verify warm VM adoption and write encrypted disk state"):
        test.expect_warm_pool_claim()
        test.expect_command(
            test.run_command_when_ready('printf %s "$SPACE_ENCRYPTED_POOL_PROBE"'),
            stdout="adopted",
        )
        test.expect_command(
            test.run_command_when_ready(
                "printf encrypted-pool > /home/user/pool-proof && sync && cat /home/user/pool-proof"
            ),
            stdout="encrypted-pool",
        )
        boot = test.run_command_when_ready("cat /proc/sys/kernel/random/boot_id")
        test.verify(boot.exit_code == 0, "cannot read guest boot identity")

    with test.phase("seal the adopted VM's encrypted snapshot"):
        node = test.encrypted_resuspend.current_node()
        test.pause()
        test.encrypted_resuspend.clear_completed_pods()
        test.encrypted_resuspend.sealed_snapshot(node)

    with test.phase("restore encrypted disk and memory state"):
        test.reconnect_by_id()
        test.expect_command(
            test.run_command_when_ready("cat /home/user/pool-proof"), stdout="encrypted-pool"
        )
        test.expect_command(
            test.run_command_when_ready("cat /proc/sys/kernel/random/boot_id"), stdout=boot.stdout
        )

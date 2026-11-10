"""Automatic maintenance on a qualified Linux or Windows direct-node fixture."""

import time

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.guest_auto_update",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires a dedicated node with automation enabled and a newer qualified platform target",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH", "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def guest_auto_update(test: DirectNodeRecoveryFixture) -> None:
    """Grace yields to pause; automatic trial delays one customer command without replay."""
    with test.context.phase("activate then pause before automatic consideration"):
        windows = test.config.firmware_lifecycle
        test.prepare(firmware=windows, rootfs_size_mib=None if windows else 4096)
        execution = test.start(memory_mib=8192 if windows else 1024)
        original = test.guest_maintenance_status()
        test.memory_lifecycle("pause")
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            paused = test.guest_maintenance_status()
            test.context.verify(paused.operation_id is None and paused.incarnation is None,
                                "automatic maintenance contacted or admitted a paused guest")
            test.context.wait(5)
        test.memory_lifecycle("resume")
    with test.context.phase("wait for automatic trial and issue a customer command once"):
        deadline = time.monotonic() + 420
        while True:
            seen = test.guest_maintenance_status()
            if seen.operation_id and seen.admission_held and seen.phase == "GUEST_UPDATE_OPERATION_PHASE_ACTIVATING":
                operation = seen.operation_id
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"automatic trial never appeared: {seen}")
            test.context.wait(0.1)
        command = "Write-Output 'automatic guest update ready'" if windows else "printf 'automatic guest update ready\\n'"
        test.context.verify(test.agent_command(command, retry=False) == "automatic guest update ready",
                            "customer command during maintenance did not complete")
        installed = test.await_guest_maintenance(operation)
        test.context.verify(installed.outcome == "committed", f"automatic update did not commit: {installed}")
        test.context.verify(installed.committed_digest != original.committed_digest,
                            "automatic target did not replace the template release")
        test.context.verify(test.observe() == execution, "automatic guest update replaced the VM execution")


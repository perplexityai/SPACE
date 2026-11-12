"""Node-local automatic guest-update kill switch."""

import time

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.guest_auto_update_disabled",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    tier="manual",
    manual_reason="requires a dedicated node started with SPACE_GUEST_AUTO_UPDATE=0 and a qualified newer target",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH", "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def guest_auto_update_disabled(test: DirectNodeRecoveryFixture) -> None:
    """The node kill switch prevents automatic admission while manual maintenance works."""
    with test.context.phase("observe an idle qualified guest with automation disabled"):
        windows = test.config.firmware_lifecycle
        test.prepare(firmware=windows, rootfs_size_mib=None if windows else 4096)
        test.start(memory_mib=8192 if windows else 1024)
        original = test.guest_maintenance_status()
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            seen = test.guest_maintenance_status()
            test.context.verify(seen.operation_id is None and seen.incarnation == original.incarnation,
                                "disabled automatic maintenance admitted or restarted a guest")
            test.context.wait(5)
    with test.context.phase("explicit maintenance remains available"):
        installed = test.await_guest_maintenance(test.guest_maintenance(update=True))
        test.context.verify(installed.outcome == "committed", f"manual update failed with automation disabled: {installed}")
        test.context.verify(installed.committed_digest != original.committed_digest,
                            "disabled fixture did not have an eligible newer target")

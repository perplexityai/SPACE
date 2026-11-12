"""Local Windows recovery under a fully hung candidate."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.windows_guest_recovery",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",), required_tools=("ssh",),
    tier="manual",
    manual_reason="requires a disposable Windows updater template and pinned Administrator SSH access",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE", "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT", "SPACE_SMOKE_WINDOWS_ADMIN_SSH_CONFIG",
    ),
)
def windows_guest_recovery(test: DirectNodeRecoveryFixture) -> None:
    """Recover a fully hung synthetic trial and retry a settled stopped service."""
    with test.context.phase("create an isolated Windows recovery execution"):
        test.prepare(firmware=True)
        test.verify_agent_template()
        execution = test.start(memory_mib=8192)
    with test.context.phase("recover hung trial and stopped service through periodic tasks"):
        test.verify_windows_recovery()
        test.context.verify(test.observe() == execution, "recovery replaced the VM execution")
        test.agent_command("Write-Output 'recovery resumed customer dispatch'")

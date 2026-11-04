"""Native Windows process API through managed cold start and restart."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture

@smoke.scenario(
    "node.windows_agent_operations",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires a Windows template provisioned for native spaced process/file operations",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE", "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT", "SPACE_SMOKE_WINDOWS_OPERATIONS_PROBE",
    ),
)
def windows_agent_operations(test: DirectNodeRecoveryFixture) -> None:
    """Managed spaced-host preserves process/file semantics across cold starts."""
    with test.context.phase("create and execute through spaced-host"):
        test.prepare(firmware=True)
        test.verify_agent_template()
        original = test.start(memory_mib=8192)
        test.verify_windows_agent_operations()
    with test.context.phase("cold restart and verify process/file APIs"):
        test.stop()
        restarted = test.start(memory_mib=8192)
        test.context.verify(restarted.execution_id != original.execution_id, "cold start reused execution")
        test.verify_windows_agent_operations()
    with test.context.phase("stop and delete the scenario sandbox"):
        test.stop()
        test.cleanup()
        test.selector = None
        test.create_directory = None

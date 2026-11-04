"""Native Windows agent readiness through normal sandbox create and cold restart."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.windows_agent_readiness",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires a native spaced Windows template with OpenSSH on an authorized standalone worker",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE", "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def windows_agent_readiness(test: DirectNodeRecoveryFixture) -> None:
    """Blocking start must complete through spaced, including after a cold restart."""
    with test.context.phase("create and wait for the native agent"):
        test.prepare(firmware=True)
        test.verify_agent_template()
        original = test.start(memory_mib=8192)
        test.verify_ssh_banner()
    with test.context.phase("cold restart and reconnect to the native agent"):
        test.stop()
        restarted = test.start(memory_mib=8192)
        test.context.verify(restarted.execution_id != original.execution_id, "cold start reused execution")
        test.verify_ssh_banner()
    with test.context.phase("delete only the scenario's sandbox"):
        test.cleanup()
        test.selector = None
        test.create_directory = None

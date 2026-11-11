"""Windows memory lifecycle through standalone SPACE services."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture
from space_smoke.memory_lifecycle import exercise_memory_lifecycle


@smoke.scenario(
    "node.windows_memory_lifecycle",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires a Windows template with native spaced memory lifecycle on a standalone KVM worker",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE", "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT", "SPACE_SMOKE_WINDOWS_OPERATIONS_PROBE",
    ),
)
def windows_memory_lifecycle(test: DirectNodeRecoveryFixture) -> None:
    """Manual and automatic dormancy preserve a live Windows process and its memory."""
    exercise_memory_lifecycle(test, firmware=True)

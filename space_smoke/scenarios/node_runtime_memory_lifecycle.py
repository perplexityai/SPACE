"""Resident and persisted state through runtime-neutral lifecycle operations."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture
from space_smoke.memory_lifecycle import exercise_memory_lifecycle


@smoke.scenario(
    "node.runtime_memory_lifecycle",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires an explicitly selected standalone node and snapshot-capable environment",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY",
        "SPACE_SMOKE_NODE_HOST",
        "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV",
        "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def runtime_memory_lifecycle(test: DirectNodeRecoveryFixture) -> None:
    """Pause and restore preserve RAM and disk; restored executions survive manager restart."""
    exercise_memory_lifecycle(test, firmware=False)

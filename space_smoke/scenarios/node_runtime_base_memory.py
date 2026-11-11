"""Resident and persisted state through runtime-neutral lifecycle operations."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture
from space_smoke.base_memory import exercise_base_memory


@smoke.scenario(
    "node.runtime_base_memory",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    timeout_minutes=25,
    cleanup_timeout_seconds=600,
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
def runtime_base_memory(test: DirectNodeRecoveryFixture) -> None:
    """Base snapshots and pool members restore independent Linux guest processes."""
    exercise_base_memory(test, firmware=False)

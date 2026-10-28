"""Resident and persisted state through runtime-neutral lifecycle operations."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


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
    with test.context.phase("create_and_write_state"):
        test.prepare()
        original = test.start()
        boot = test.write_marker()
        test.guest(f"printf '%s' {test.marker} > /dev/shm/runtime-memory-marker")
    with test.context.phase("resident_pause_resume"):
        test.command(["space-sbxmgr", "pause", test.owned_selector(), "--blocking"])
        test.command(["space-sbxmgr", "resume", test.owned_selector(), "--blocking"])
        test.context.verify(test.verify_marker() == boot, "resume changed guest boot")
    with test.context.phase("named_full_snapshot"):
        test.command(
            ["space-sbxmgr", "snapshot", test.owned_selector(), "--full"], timeout=180
        )
    for iteration in range(2):
        with test.context.phase(f"suspend_restore_{iteration}"):
            test.command(
                ["space-sbxmgr", "suspend", test.owned_selector(), "--blocking"],
                timeout=180,
            )
            if iteration == 0:
                test.restart_managers()
            test.command(
                ["space-sbxmgr", "restore", test.owned_selector(), "--blocking"],
                timeout=180,
            )
            restored = test.observe()
            test.context.verify(
                restored.execution_id != original.execution_id,
                "restore did not acquire a new execution",
            )
            test.context.verify(test.verify_marker() == boot, "restore cold booted")
            test.context.verify(
                test.guest("cat /dev/shm/runtime-memory-marker") == test.marker,
                "restore lost resident guest memory",
            )
            original = restored
    with test.context.phase("adopt_restored_execution"):
        test.restart_managers()
        test.context.verify(
            test.verify_marker() == boot, "manager restart lost guest state"
        )
        test.context.verify(
            test.guest("cat /dev/shm/runtime-memory-marker") == test.marker,
            "manager restart lost restored memory",
        )
        # A resident transition after adoption also checks guest readiness
        # through spaced without requiring the public API route-token authority.
        test.command(["space-sbxmgr", "pause", test.owned_selector(), "--blocking"])
        test.command(["space-sbxmgr", "resume", test.owned_selector(), "--blocking"])

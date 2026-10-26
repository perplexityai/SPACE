"""Recovery of running and stopped executions on an opted-in standalone worker."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.runtime_survives_manager_restarts",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires an explicitly dedicated idle worker and permission to restart its managers",
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
def runtime_survives_manager_restarts(test: DirectNodeRecoveryFixture) -> None:
    """Manager restarts preserve a running VM's execution, slot and guest boot;
    restarting managers while stopped preserves its disk for a fresh start.
    Cleanup deletes only the run-owned sandbox through the ordinary API.
    """
    with test.context.phase("create a pinned sandbox on the dedicated node"):
        test.prepare()
        original = test.start()
        boot_id = test.write_marker()
    with test.context.phase("recover the existing running execution"):
        test.restart_managers()
        test.context.verify(
            test.observe() == original, "manager restart changed the live execution"
        )
        test.context.verify(
            test.verify_marker() == boot_id, "manager restart rebooted the guest"
        )
        test.context.record(
            "running_recovery", "same execution, slot, process and guest boot"
        )
    with test.context.phase("recover stopped state and cold-start its preserved disk"):
        test.stop()
        test.restart_managers()
        restarted = test.start()
        test.context.verify(
            restarted.execution_id != original.execution_id,
            "cold start reused the previous execution generation",
        )
        test.context.verify(
            test.verify_marker() != boot_id,
            "cold start did not create a new guest boot",
        )
        test.context.record(
            "stopped_recovery", "new execution and guest boot; disk marker preserved"
        )

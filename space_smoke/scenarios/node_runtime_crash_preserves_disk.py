"""Fault recovery for an owned runtime execution on a dedicated worker."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.runtime_crash_preserves_disk",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires an explicitly dedicated idle worker and permission to kill its owned runtime and restart managers",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY",
        "SPACE_SMOKE_NODE_FAULT",
        "SPACE_SMOKE_NODE_HOST",
        "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV",
        "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def runtime_crash_preserves_disk(test: DirectNodeRecoveryFixture) -> None:
    """A killed runtime becomes stopped through normal reconciliation, retains
    its disk, and can start a new execution. Deletion reclaims its node lease.
    """
    test.require_crash_opt_in()
    with test.context.phase("create an owned execution and durable disk marker"):
        test.prepare()
        original = test.start()
        original_boot = test.write_marker()
    for restart_managers in (False, True):
        with test.context.phase(
            f"crash and reconcile; manager restart={restart_managers}"
        ):
            test.crash_runtime(original)
            if restart_managers:
                test.restart_managers()
            test.await_crash_cleanup()
            test.await_lease_cleanup()
        with test.context.phase("start a new execution from the retained disk"):
            current = test.start()
            test.context.verify(
                current.execution_id != original.execution_id,
                "crash recovery reused the dead execution identity",
            )
            current_boot = test.verify_marker()
            test.context.verify(
                current_boot != original_boot,
                "crash recovery did not cold-boot a new guest",
            )
            original, original_boot = current, current_boot
    test.context.record(
        "crash_recovery", "repeated cleanup, new executions, preserved disk"
    )

"""Fault recovery for an owned Cloud Hypervisor execution on a dedicated worker."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.runtime_crash_preserves_disk",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    quarantined="a killed VMM remains started with its node lease; automatic crash recovery is incomplete",
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
    """A killed CH process becomes failed through normal reconciliation, retains
    its disk, and can start a new execution. Deletion reclaims its node lease.
    """
    with test.context.phase("create an owned CH execution and durable disk marker"):
        test.prepare()
        original = test.start()
        original_boot = test.write_marker()
    with test.context.phase("kill only the captured VMM and observe failure"):
        test.crash_runtime(original)
        test.await_failed()
    with test.context.phase("start a new execution from the retained disk"):
        current = test.start()
        test.context.verify(
            current.execution_id != original.execution_id,
            "crash recovery reused the dead execution identity",
        )
        test.context.verify(
            test.verify_marker() != original_boot,
            "crash recovery did not cold-boot a new guest",
        )
        test.context.record("crash_recovery", "new execution, preserved disk marker")

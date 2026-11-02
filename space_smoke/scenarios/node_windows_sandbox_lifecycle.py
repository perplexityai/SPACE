"""Local firmware guest boot and stop/start through the normal manager APIs."""
import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.windows_sandbox_lifecycle",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires a prepared local Windows template on an authorized standalone worker",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE", "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def windows_sandbox_lifecycle(test: DirectNodeRecoveryFixture) -> None:
    """No manager restarts or device allocations. Cleanup deletes only its sandbox.
    RDP negotiation is service evidence; authenticated interactive access is separate.
    Agent refusal before wake is covered by the manager's guest-image unit tests.
    """
    with test.context.phase("create and launch the firmware guest"):
        test.prepare(firmware=True)
        original = test.start(memory_mib=4096)
        test.verify_rdp_negotiation()
    with test.context.phase("stop and cold-start with the persisted guest contract"):
        test.stop()
        restarted = test.start(memory_mib=4096)
        test.context.verify(restarted.execution_id != original.execution_id, "cold start reused execution")
        test.verify_rdp_negotiation()
    with test.context.phase("delete only the scenario's sandbox"):
        test.cleanup()
        test.selector = None
        test.create_directory = None

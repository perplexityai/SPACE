"""One explicitly enrolled dedicated GPU through the ordinary sandbox lifecycle."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.gpu_sandbox_lifecycle",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires an authorized idle standalone worker and enrolled development GPU",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY",
        "SPACE_SMOKE_NODE_HOST",
        "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV",
        "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH",
        "SPACE_SMOKE_NODE_ENVIRONMENT",
        "SPACE_SMOKE_NODE_DEVICE_PROFILE",
        "SPACE_SMOKE_NODE_DEVICE_PARENT",
    ),
)
def gpu_sandbox_lifecycle(test: DirectNodeRecoveryFixture) -> None:
    """Checked device computation, stopped-disk restart and asynchronous pool reuse.
    Reset-only development evidence does not establish tenant-data sanitization.
    """
    with test.context.phase("create and boot a sandbox with the requested GPU"):
        test.prepare(with_device=True)
        first = test.start(memory_mib=4096)
        test.verify_device_owner(first)
        boot = test.write_marker()
        test.compute_on_gpu()
    with test.context.phase("release GPU ownership and recover pool capacity"):
        test.stop()
        test.await_device_ready()
    with test.context.phase("cold-start the preserved sandbox with GPU computation"):
        second = test.start(memory_mib=4096)
        test.verify_device_owner(second)
        test.context.verify(
            second.execution_id != first.execution_id, "start reused an execution"
        )
        test.context.verify(
            test.verify_marker() != boot, "cold start reused a guest boot"
        )
        test.compute_on_gpu()
    with test.context.phase("delete the owned sandbox and return the GPU to its pool"):
        test.cleanup()
        test.selector = None
        test.create_directory = None
        test.await_device_ready()

"""Runtime artifacts arrive through ordinary publication and reconciliation."""

import space_smoke as smoke
from space_smoke.runtime_provisioning import RuntimeProvisioningFixture


@smoke.scenario(
    "node.runtime_provisioning",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    timeout_minutes=15,
    cleanup_timeout_seconds=120,
    api_surfaces=("node",),
    required_tools=("docker", "grpcurl"),
    ignored="requires explicit disposable Docker real-binary fixture configuration",
    rerun_environment=(
        "SPACE_SMOKE_LOCAL_RUNTIME_PROVISIONING",
        "SPACE_SMOKE_RUNTIME_BINARY_DIR",
        "SPACE_SMOKE_RUNTIME_SOURCE_DIR",
        "SPACE_SMOKE_RUNTIME_EVIDENCE_DIR",
        "SPACE_SMOKE_RUNTIME_WORKER_IMAGE",
        "SPACE_SMOKE_RUNTIME_POSTGRES_IMAGE",
    ),
)
def runtime_provisioning(test: RuntimeProvisioningFixture) -> None:
    """Known cold-start environments must become installed through the regular recipe loop."""
    test.control.verify_regular_reconciliation(test.context)

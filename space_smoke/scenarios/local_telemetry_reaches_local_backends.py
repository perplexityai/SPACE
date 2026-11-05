"""In `otel` mode the stack's metrics, traces and logs reach the local backends."""

import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig

MARKER = "space-smoke-telemetry-ok"
VCPU = 2
MEMORY_MIB = 1024
# The fixture's ECW client sends no X-Client-ID, which the API's route
# middleware tags `unknown`.
CLIENT_ID = "unknown"


# The reads poll for this scenario's own sandbox; Tempo's search cutoff (5 s
# in the Kind profile) sets the floor (see space_smoke/telemetry_delivery.py).
# The owned cold-create config is enabled with a zero slot target, so the API
# resolves its shape for the create (the Kind seed declares none) while no
# warm slot exists to adopt.
@smoke.scenario(
    "local.telemetry_reaches_local_backends",
    telemetry_mode="otel",
    sandbox=smoke.SandboxConfig(
        kind_cold_create=KindColdCreateConfig(
            vcpu=VCPU, memory_mib=MEMORY_MIB, supply_shape=True
        ),
    ),
)
def telemetry_reaches_local_backends(test: smoke.KindEcw) -> None:
    """Once a cold-created sandbox has run a command, Tempo returns
    containerd-shim-space-v2's create in the space-api trace that created it,
    parented by the API's pod materialization span, Prometheus
    holds create latency from that shim and from space-api since the
    scenario began, Loki holds a SPACE pod log line naming the sandbox, and
    Prometheus holds one accepted, defaulted requested-size sample of the
    create's 2 vCPU and 1024 MiB."""

    test.expect_cold_create()
    with test.phase("run a command in the sandbox"):
        result = test.run_command(f"printf %s {MARKER}")
    test.expect_command(result, stdout=MARKER)

    telemetry = test.telemetry
    with test.phase("Tempo: the shim's create joins its space-api parent for this sandbox"):
        telemetry.expect_traces()

    with test.phase("Prometheus: create latency from this sandbox's shim and the API"):
        telemetry.expect_metrics()

    with test.phase("Loki: a SPACE pod log line names the sandbox"):
        telemetry.expect_logs()

    with test.phase("Prometheus: one requested-size sample of this create's shape"):
        telemetry.expect_requested_create_size(
            template=test.sandbox_template_name(),
            client_id=CLIENT_ID,
            vcpu=VCPU,
            memory_mib=MEMORY_MIB,
        )

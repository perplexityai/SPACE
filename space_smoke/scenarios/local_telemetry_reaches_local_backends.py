"""In `otel` mode the stack's metrics, traces and logs reach the local backends."""

import space_smoke as smoke
from space_smoke.config import SandboxConfig

MARKER = "space-smoke-telemetry-ok"


# Tempo's search cutoff (5 s in the Kind profile) sets the trace delivery floor
# (see space_smoke/telemetry_delivery.py). Memory delivery is checked for the
# template and node, not attributed to this particular sandbox.
@smoke.scenario(
    "local.telemetry_reaches_local_backends",
    telemetry_mode="otel",
    sandbox=SandboxConfig(timeout=600),
)
def telemetry_reaches_local_backends(test: smoke.KindEcw) -> None:
    """Once a sandbox has run a command, Tempo returns
    containerd-shim-space-v2's create in the space-api trace that created it,
    parented by the API's pod materialization span, Prometheus
    holds create latency from that shim and from space-api since the
    scenario began, Loki holds a SPACE pod log line naming the sandbox, and
    fresh guest-memory observations arrive for its template and node."""

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

    with test.phase("Prometheus: fresh guest-memory observations for template and node"):
        telemetry.expect_guest_memory_used()

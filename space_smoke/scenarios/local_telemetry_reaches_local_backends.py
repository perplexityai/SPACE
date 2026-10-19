"""In `otel` mode the stack's metrics, traces and logs reach the local backends."""

import space_smoke as smoke

MARKER = "space-smoke-telemetry-ok"


# The reads poll for this scenario's own sandbox; Tempo's 30 s search cutoff
# sets the floor (see space_smoke/telemetry_delivery.py).
@smoke.scenario("local.telemetry_reaches_local_backends", telemetry_mode="otel")
def telemetry_reaches_local_backends(test: smoke.KindEcw) -> None:
    """Once a sandbox has run a command, Tempo returns
    containerd-shim-space-v2's create trace for it, linked to the space-api
    trace that created it, Prometheus
    holds create latency from that shim and from space-api since the
    scenario began, and Loki holds a SPACE pod log line naming the sandbox."""

    with test.phase("run a command in the sandbox"):
        result = test.run_command(f"printf %s {MARKER}")
    test.expect_command(result, stdout=MARKER)

    telemetry = test.telemetry
    with test.phase("Tempo: the shim's create trace links to space-api's for this sandbox"):
        telemetry.expect_traces()

    with test.phase("Prometheus: create latency from this sandbox's shim and the API"):
        telemetry.expect_metrics()

    with test.phase("Loki: a SPACE pod log line names the sandbox"):
        telemetry.expect_logs()

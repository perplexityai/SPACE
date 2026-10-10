"""In `otel` mode the stack's metrics, traces and logs reach the local backends."""

import space_smoke as smoke

MARKER = "space-smoke-telemetry-ok"


# The reads poll for this scenario's own sandbox; Tempo's 30 s search cutoff
# sets the floor (see space_smoke/telemetry_delivery.py).
@smoke.scenario("local.telemetry_reaches_local_backends")
def telemetry_reaches_local_backends(test: smoke.KindEcw) -> None:
    """Once a sandbox has run a command, Tempo returns one trace in which
    space-api created it and containerd-shim-space-v2 started it, Prometheus
    holds create latency from that shim and from space-api since the
    scenario began, and Loki holds a SPACE pod log line naming the sandbox."""

    with test.phase("run a command in the sandbox"):
        result = test.run_command(f"printf %s {MARKER}")
    test.expect_command(result, stdout=MARKER)

    telemetry = test.telemetry
    with test.phase("Tempo: one trace from space-api to the shim for this sandbox"):
        telemetry.expect_traces()

    with test.phase("Prometheus: create latency from this sandbox's shim and the API"):
        telemetry.expect_metrics()

    with test.phase("Loki: a SPACE pod log line names the sandbox"):
        telemetry.expect_logs()

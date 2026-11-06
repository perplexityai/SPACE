"""In `otel` mode the stack's metrics, traces and logs reach the local backends."""

import space_smoke as smoke
from space_smoke.config import KindColdCreateConfig, SandboxConfig

MARKER = "space-smoke-telemetry-ok"


# The reads poll for this scenario's own sandbox; Tempo's search cutoff (5 s
# in the Kind profile) sets the floor (see space_smoke/telemetry_delivery.py).
@smoke.scenario(
    "local.telemetry_reaches_local_backends",
    telemetry_mode="otel",
    sandbox=SandboxConfig(
        timeout=600,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def telemetry_reaches_local_backends(test: smoke.KindEcw) -> None:
    """Once a sandbox has run a command, Tempo returns
    containerd-shim-space-v2's create in the space-api trace that created it,
    parented by the API's pod materialization span, Prometheus
    holds create latency from that shim and from space-api since the
    scenario began, Loki holds a SPACE pod log line naming the sandbox, and
    fresh guest-memory observations agree with the guest's meminfo."""

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

    with test.phase("Prometheus: fresh guest-memory bytes agree with guest meminfo"):
        result = test.run_command(
            "awk '/^MemTotal:/ { total=$2 } "
            "/^MemAvailable:/ { available=$2 } "
            'END { printf "%.0f %.0f\\n", total*1024, (total-available)*1024 }\' '
            "/proc/meminfo"
        )
        test.verify(result.exit_code == 0, "reading guest meminfo failed")
        total, used = map(int, result.stdout.split())
        test.verify(0 < used <= total, "guest meminfo is not a valid memory observation")
        test.record("guest_meminfo_bytes", {"total": total, "used": used})
        telemetry.expect_guest_memory_used(
            total_bytes=total,
            used_bytes=used,
        )

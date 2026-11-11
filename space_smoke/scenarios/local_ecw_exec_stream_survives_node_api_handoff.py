"""An ECW process stream survives node-api being replaced mid-output."""

import secrets

import space_smoke as smoke
from space_smoke.exec_stream_faults import tick_command, ticks

TICKS = 40
TIMEOUT_MS = 120_000
GRACE_SECONDS = 3


@smoke.scenario(
    "local.ecw_exec_stream_survives_node_api_handoff",
    sandbox=smoke.SandboxConfig(timeout=600),
)
def ecw_exec_stream_survives_node_api_handoff(test: smoke.KindEcw) -> None:
    """With the sandbox node's node-api pod deleted after the first line of
    output, its streams cut at the end of a short termination grace and a new
    pod taking its place, the same client stream reads every line once and in
    order and then the command's exit; the guest ran it once."""

    faults = test.exec_stream_faults
    node = faults.node()
    command = faults.command(tick_command(TICKS, interval=0.5, then="exit 3"))

    with test.phase("start a ticking command; read its first tick"):
        stream = faults.start(
            command, timeout_ms=TIMEOUT_MS, idempotency_key=f"space-smoke-{secrets.token_hex(8)}"
        )
        stream.read_until(lambda read: "tick-1\n" in read.stdout)

    with test.phase("replace the node's node-api pod"):
        faults.replace_backend(node, grace_seconds=GRACE_SECONDS)

    with test.phase("the stream carries every tick once and the exit"):
        stream.read_to_end()
        faults.expect_completed(stream, stdout=ticks(TICKS), exit_code=3)
        faults.expect_one_run()

"""An ECW process stream survives its node-api connection ending after the 200."""

import space_smoke as smoke
from space_smoke.exec_stream_faults import tick_command, ticks

TICKS = 8
TIMEOUT_MS = 8_000


@smoke.scenario(
    "local.ecw_exec_stream_survives_backend_eof",
    sandbox=smoke.SandboxConfig(timeout=600),
)
def ecw_exec_stream_survives_backend_eof(test: smoke.KindEcw) -> None:
    """With the data plane's connections to the sandbox node's node-api reset
    after the first line of output, the same client stream, opened without an
    `Idempotency-Key`, reads every line once and in order and then the
    command's timeout at its original deadline; the guest ran it once."""

    faults = test.exec_stream_faults
    node = faults.node()
    command = faults.command(tick_command(TICKS, interval=0.5, then="sleep 60"))

    with test.phase("start a ticking command; read its first tick"):
        stream = faults.start(command, timeout_ms=TIMEOUT_MS)
        stream.read_until(lambda read: "tick-1\n" in read.stdout)

    with test.phase("reset the data plane's node-api connections"):
        faults.cut_backend_streams(node)

    with test.phase("the stream carries every tick once and the original deadline"):
        stream.read_to_end()
        end = faults.expect_completed(
            stream, stdout=ticks(TICKS), exit_code=124, status="timed_out"
        )
        elapsed = end.at - stream.opened_at
        test.verify(
            elapsed < TIMEOUT_MS / 1000 + 3,
            "the reopened stream did not keep the command's deadline",
            seconds_to_timeout=round(elapsed, 2),
        )
        faults.expect_one_run()

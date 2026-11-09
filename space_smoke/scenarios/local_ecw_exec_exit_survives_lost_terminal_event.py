"""An ECW process's exit reaches the client when its first delivery is lost."""

import time

import space_smoke as smoke

TIMEOUT_MS = 60_000


@smoke.scenario(
    "local.ecw_exec_exit_survives_lost_terminal_event",
    sandbox=smoke.SandboxConfig(timeout=600),
)
def ecw_exec_exit_survives_lost_terminal_event(test: smoke.KindEcw) -> None:
    """With node-api stopped after the command's last line, the command exiting
    while it is stopped, and the data plane's connections to it reset before it
    continues, the exit that node-api never forwarded still reaches the same
    client stream, after the last line and with no output repeated; the guest
    ran the command once."""

    faults = test.exec_stream_faults
    node = faults.node()
    command = faults.command("echo last; sleep 2; exit 7")

    with test.phase("start the command; read its last line"):
        stream = faults.start(command, timeout_ms=TIMEOUT_MS)
        stream.read_until(lambda read: "last\n" in read.stdout)

    with test.phase("stop node-api past the exit; reset its connections; continue it"):
        with faults.frozen_backend(node):
            time.sleep(4)
            faults.cut_backend_streams(node)
        continued_at = time.monotonic()

    with test.phase("the stream ends with the exit, no output repeated"):
        stream.read_to_end()
        end = faults.expect_completed(stream, stdout="last\n", exit_code=7)
        # The command exited while node-api was stopped and its connections
        # were reset, so the exit can only come over a reopened stream.
        test.verify(
            end.at >= continued_at,
            "the exit reached the client while node-api was stopped",
            seconds_after_continue=round(end.at - continued_at, 2),
        )
        faults.expect_one_run()

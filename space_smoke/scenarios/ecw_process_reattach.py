"""ECW process reattachment happy path."""

import space_smoke as smoke

MARKER = "space-smoke-process-reattach"
RELEASE = "/tmp/space-smoke-process-reattach-release"


@smoke.scenario("ecw.process_reattach")
def process_reattach(test: smoke.Ecw) -> None:
    """A client can disconnect from a running ECW process, reattach by its
    process ID, and receive its final output and exit status.
    """

    with test.phase("start and disconnect from background process"):
        process = test.sandbox.start_background(
            f"while ! test -f {RELEASE}; do sleep 0.05; done; printf %s {MARKER}",
            timeout=20,
        )
        pid = process.pid
        process.disconnect()

    with test.phase("reattach and verify process result"):
        attached = test.reattach_process(pid, timeout=20)
        # The process cannot finish before the new attachment exists.
        test.sandbox.files.write(RELEASE, "release")
        result = attached.wait()
        test.verify(
            result.exit_code == 0 and result.stdout == MARKER,
            "reattached ECW process did not return its final result",
            observed_pid=pid,
            command_exit_code=result.exit_code,
            command_stdout=result.stdout,
            command_stderr=result.stderr,
        )

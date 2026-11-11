"""A Process.Start retried with its idempotency key and deadline runs once."""

import secrets
import time

import space_smoke as smoke

TIMEOUT_MS = 8_000
RETRY_AFTER_SECONDS = 3.0


@smoke.scenario(
    "local.ecw_exec_ambiguous_start_retry_runs_once",
    sandbox=smoke.SandboxConfig(timeout=600),
)
def ecw_exec_ambiguous_start_retry_runs_once(test: smoke.KindEcw) -> None:
    """A client that drops a `Process.Start` right after its HTTP 200 and
    retries with the same `Idempotency-Key` and `Connect-Timeout-Ms` reads the
    first attempt's output and its timeout at the first attempt's deadline,
    not a deadline restarted by the retry; the guest ran the command once. A
    different key runs it again."""

    faults = test.exec_stream_faults
    command = faults.command("echo begun; sleep 30")
    key = f"space-smoke-{secrets.token_hex(8)}"

    with test.phase("start the command and drop the stream after its 200"):
        first = faults.start(command, timeout_ms=TIMEOUT_MS, idempotency_key=key)
        first.stream.close()
        time.sleep(RETRY_AFTER_SECONDS)

    with test.phase("the retry replays the first run and keeps its deadline"):
        retry = faults.start(command, timeout_ms=TIMEOUT_MS, idempotency_key=key)
        retry.read_to_end()
        end = faults.expect_completed(retry, stdout="begun\n", exit_code=124, status="timed_out")
        since_first = end.at - first.opened_at
        test.verify(
            since_first < TIMEOUT_MS / 1000 + RETRY_AFTER_SECONDS - 1,
            "the retry's deadline started over instead of keeping the first attempt's",
            seconds_since_first_start=round(since_first, 2),
        )
        faults.expect_one_run()

    with test.phase("control: a different key runs the command again"):
        other = faults.start(faults.command("true"), timeout_ms=TIMEOUT_MS)
        other.read_to_end()
        faults.expect_completed(other, stdout="", exit_code=0)
        runs = faults.runs()
        test.verify(runs == 2, "a new key did not run the command", runs=runs)

"""A start of a resident a v2 drain is still making durable is told to retry, then lands elsewhere."""

import time
from collections.abc import Callable
from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import OperationLifecycleSandbox

_TERMINAL = {"succeeded", "failed", "canceled", "expired"}
_RETRY = "still making it durable"


# The 503, its `Retry-After` and the 409 it replaces only while the node's
# report lists the sandbox pending are the drain gate's tests
# (`drain_guard_postgres_tests.rs`). This scenario is the seam on a real
# drain: with the drained worker's path to the object store cut, the drain
# publishes a stopped resident's disk but can't upload it, so a start meets
# the sandbox still `stopped` on the drained worker and not yet durable. A
# started resident is no subject: until its upload lands the control plane
# still reads it `started`, so a wake of it is answered as already running.
@smoke.scenario(
    "native.v2_drain_start_waits_for_durability",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    ignored="holds a drain's upload to meet a start mid-evacuation, a bench run",
)
def v2_drain_start_waits_for_durability(test: smoke.CommandSandboxFixture) -> None:
    """With the drained worker's uploads blocked, a start of a stopped
    resident whose disk the v2 drain has not yet uploaded is retried with
    the drain's 503 rather than refused with a 409. Once the path is back,
    the same start operation succeeds and the sandbox runs on the other
    worker."""

    drain = test.drain_v2

    def wait_until(accept: Callable[[], Any], failure: str, *, timeout: float, **detail: Any) -> Any:
        deadline = time.monotonic() + timeout
        while True:
            value = accept()
            if value:
                return value
            test.verify(time.monotonic() < deadline, failure, **detail)
            test.wait(2)

    with drain.v2_controller():
        with test.phase("pin a stopped resident to one worker"):
            sandboxes, lifecycles, _digests = drain.pin_residents({"stopped": 1})
            (sandbox_id,) = lifecycles
            subject = cast(OperationLifecycleSandbox, sandboxes[sandbox_id])
            bare = drain.bare_id(sandbox_id)

        with test.phase("uploads blocked: the drain can't make the resident durable"):
            uploads = drain.block("object-store")
            epoch = drain.drain()
            report = drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and bare in drain.pending_actions(report)
                and drain.placements([bare])[bare][0] == "stopped",
                "the node never reported the stopped resident pending",
                timeout=300,
            )
            test.record("pending_report", report)

        with test.phase("a start is told to retry, not refused"):
            start = subject.request_lifecycle("start")
            progress: list[str] = []

            def retried() -> dict[str, object] | None:
                operation = subject.operation(start)
                message = str(operation.get("progress_message") or "")
                if message and (not progress or progress[-1] != message[:300]):
                    progress.append(message[:300])
                test.verify(
                    operation.get("state") not in _TERMINAL,
                    "the start ended while the drain was still making the sandbox durable",
                    operation=operation,
                    progress=progress,
                )
                return operation if _RETRY in message else None

            waiting = wait_until(retried, "the start was never told to retry", timeout=120)
            test.record("retried_start", waiting)
            test.record("progress_while_blocked", progress)
            test.verify(
                not any("409" in message or "migrating it off" in message for message in progress),
                "the start met the drain's 409",
                progress=progress,
            )

        with test.phase("the path back: the same start lands the sandbox elsewhere"):
            uploads.release()
            operation = wait_until(
                lambda: (lambda op: op if op.get("state") in _TERMINAL else None)(subject.operation(start)),
                "the start never ended",
                timeout=900,
            )
            test.record("start", operation)
            test.verify(
                operation.get("state") == "succeeded",
                "the retried start did not succeed once the sandbox was durable",
                operation=operation,
            )
            drain.expect_back_elsewhere(sandbox_id, "started")

        with test.phase("the node drains; undrain"):
            drain.expect_drained()
            drain.undrain_and_expect_cancelled()

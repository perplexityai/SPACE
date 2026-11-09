"""A v2 drain past the cluster's evacuation slots waits, unsealed, for one to free."""

import time
from collections.abc import Callable
from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox

FILL = "head -c 256M /dev/urandom > /dev/shm/space-smoke-fill && echo ok"


# The admission order and the cap are the controller loop's tests
# (`node_drain/v2/tests.rs`), the waiting gate the drain gate's
# (`drain_guard_postgres_tests.rs`). This scenario is the seam on real
# workers: with the controller's cap at one, a worker drained while another
# evacuates is cordoned but not sealed, and its resident keeps serving.
@smoke.scenario(
    "native.v2_drain_waits_for_an_evacuation_slot",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=60,
)
def v2_drain_waits_for_an_evacuation_slot(test: smoke.CommandSandboxFixture) -> None:
    """With at most one v2 drain evacuating, a worker drained while another
    evacuates waits: its `drain_status` says `waiting`, the node reports
    nothing because nothing sealed it, and its resident runs commands and
    pauses and resumes. Once the first worker is `drained`, the second is
    admitted, seals and evacuates its resident."""

    first = test.drain_v2

    def wait_until(accept: Callable[[], Any], failure: str, *, timeout: float) -> Any:
        deadline = time.monotonic() + timeout
        while True:
            value = accept()
            if value:
                return value
            test.verify(time.monotonic() < deadline, failure)
            test.wait(2)

    with first.v2_controller(max_evacuating_nodes=1):
        with test.phase("a resident on each worker, each worker under a fake instance"):
            first.pin_to_resident_worker(away_from_fixture=True)
            test.sandbox.suspend()
            second = first.companion()
            second.give_fake_instance()
            first.give_fake_instance()
            evacuating = [first.create_resident() for _ in range(4)]
            for sandbox in evacuating:
                filled = first.run_on(sandbox, FILL)
                test.verify(filled.exit_code == 0, "a resident could not fill its memory")
            first.cordon()
            first.release_other_worker()
            waiting = second.create_resident()

        with test.phase("drain both: the first is admitted, the second waits unsealed"):
            first.drain()
            second.drain()
            wait_until(
                lambda: first.status().get("waiting") is False,
                "the first v2 drain was never admitted",
                timeout=120,
            )
            held = second.status()
            test.record("waiting_status", held)
            test.verify(
                held.get("waiting") is True and "report" not in held,
                "the second v2 drain did not wait unsealed",
                status=held,
            )

        with test.phase("the waiting worker's resident serves user calls"):
            served = second.run_on(waiting, "echo served")
            test.verify(
                served.exit_code == 0 and served.stdout.strip() == "served",
                "the waiting worker's resident did not run a command",
                stdout=served.stdout,
            )
            cast(ResidentLifecycleSandbox, waiting).pause()
            cast(ResidentLifecycleSandbox, waiting).wake()
            after = second.run_on(waiting, "echo resumed")
            test.verify(after.exit_code == 0, "the resumed resident did not run a command")
            still = second.status()
            test.record("status_after_user_calls", still)
            test.verify(
                still.get("waiting") is True and first.drain_phase() == "draining",
                "the calls did not all land while the second drain waited",
                second=still,
                first_phase=first.drain_phase(),
            )

        with test.phase("once the first is drained, the second seals and evacuates"):
            first.expect_drained()
            drained_at = time.monotonic()
            report = second.wait_for_report(
                lambda report: report.get("sealed_epoch") == second.epoch
                and any(
                    entry.get("sandbox_id") == second.bare_id(waiting.sandbox_id)
                    for entry in report.get("evacuated", [])
                ),
                "the second drain never sealed and evacuated its resident",
                timeout=600,
            )
            test.record("second_sealed_after_seconds", round(time.monotonic() - drained_at, 1))
            test.record("second_report", report)
            test.verify(second.status().get("waiting") is False, "the second drain still waits")

        with test.phase("undrain both and delete the residents"):
            second.undrain_and_expect_cancelled()
            first.undrain_and_expect_cancelled()
            for sandbox in [*evacuating, waiting]:
                test.delete_sandbox(sandbox)

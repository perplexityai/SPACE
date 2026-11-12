"""A v2 drain past the cluster's evacuation slots waits, unsealed, for one to free."""

import time
from collections.abc import Callable
from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox

FILL = "head -c 256M /dev/urandom > /dev/shm/space-smoke-fill && echo ok"
# More residents than `SPACE_DRAIN_NODE_WINDOW`'s default of 8, so the first
# worker's drain queues some behind its window.
EVACUATING = 12


# The admission order and the cap are the controller loop's tests
# (`node_drain/v2/tests.rs`), the waiting gate the drain gate's
# (`drain_guard_postgres_tests.rs`), the window and the cancel's hand-back
# the planner's (`service/drain_node/tests.rs`). This scenario is the seam on
# real workers: with the controller's cap at one, a worker drained while
# another evacuates is cordoned but not sealed, and its resident keeps
# serving; its cancel finishes at once. A cancel of the evacuating worker
# starts none of the suspends its window queued, hands back the ones that
# ran, and holds its slot until it finishes; a drained worker frees its slot.
@smoke.scenario(
    "native.v2_drain_waits_for_an_evacuation_slot",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=90,
)
def v2_drain_waits_for_an_evacuation_slot(test: smoke.CommandSandboxFixture) -> None:
    """With at most one v2 drain evacuating, a worker drained while another
    evacuates waits: its `drain_status` says `waiting`, the node reports
    nothing because nothing sealed it, and its resident runs commands and
    pauses and resumes. An undrain of the waiting worker is cancelled at
    once, its resident still serving. An undrain of the evacuating worker,
    while its window queues suspends, starts none of them, hands back each
    resident whose suspend ran, one restore each, and keeps the second
    worker's new drain waiting until its cancel finishes; then the second is
    admitted, seals and evacuates its resident, and once it is `drained` the
    first worker's new drain is admitted in turn."""

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
            # Re-registering a worker under a fake instance recreates its
            # Node without the cordon that kept residents off it.
            second.cordon()
            evacuating = [first.create_resident() for _ in range(EVACUATING)]
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

        with test.phase("undrain the waiting worker: its cancel finishes at once"):
            undrained_at = time.monotonic()
            second.undrain_and_expect_cancelled(timeout=90)
            test.record("waiting_cancel_seconds", round(time.monotonic() - undrained_at, 1))
            served = second.run_on(waiting, "echo served")
            test.verify(
                served.exit_code == 0 and first.drain_phase() == "draining",
                "the cancelled waiting worker's resident did not serve, or the first drain moved",
                stdout=served.stdout,
                first_phase=first.drain_phase(),
            )
            second.drain()
            held = second.status()
            test.verify(
                held.get("waiting") is True,
                "the second worker's new drain did not wait for the held slot",
                status=held,
            )

        bare = {first.bare_id(sandbox.sandbox_id): sandbox for sandbox in evacuating}
        with test.phase("undrain the first while its window queues suspends"):
            report = first.wait_for_report(
                lambda report: report.get("sealed_epoch") == first.epoch
                and "suspend" in first.pending_actions(report).values()
                and "queued" in first.pending_actions(report).values(),
                "the first worker's drain never queued a suspend behind its window",
                timeout=300,
            )
            queued = sorted(
                sandbox_id
                for sandbox_id, action in first.pending_actions(report).items()
                if action == "queued" and sandbox_id in bare
            )
            test.record("report_at_undrain", report)
            test.record("queued_at_undrain", queued)
            first.undrain()
            # The run's own cordon kept the waiting resident off this worker;
            # lifted, the hand-back's restores can land here again.
            first.uncordon()
            cancel_started = time.monotonic()
            deadline = cancel_started + 900
            while True:
                # The second worker's status first: an admission seen before
                # the first's cancel is seen unfinished would have taken the
                # slot that cancel holds.
                second_waits = second.status().get("waiting") is True
                cancelled = first.status() == {}
                if cancelled:
                    break
                test.verify(
                    second_waits,
                    "the second drain was admitted before the first's cancel finished",
                    first=first.status(),
                    second=second.status(),
                )
                test.verify(time.monotonic() < deadline, "the first worker's cancel never finished")
                test.wait(2)
            test.record("evacuating_cancel_seconds", round(time.monotonic() - cancel_started, 1))

        with test.phase("the queued residents never suspended; the rest came back once"):
            lifecycles = {sandbox.sandbox_id: "started" for sandbox in evacuating}
            outcome = first.settled_lifecycles(lifecycles, timeout=900)
            test.record("lifecycles_after_cancel", outcome)
            restores = {
                sandbox_id: first.drain_restores(sandbox.sandbox_id)
                for sandbox_id, sandbox in bare.items()
            }
            test.record("restores_after_cancel", restores)
            test.verify(
                all(lifecycle == "started" for lifecycle, _ in outcome.values()),
                "the cancel left a resident out of its lifecycle",
                observed=outcome,
                restores=restores,
            )
            untouched = [sandbox_id for sandbox_id in queued if restores[sandbox_id] == []]
            test.record("queued_never_suspended", untouched)
            test.verify(
                bool(untouched) and any(restores.values()),
                "the cancel started a queued suspend, or handed back none it ran",
                queued=queued,
                restores=restores,
            )
            for sandbox in evacuating:
                first.expect_single_vm(sandbox.sandbox_id, "started")
                if restores[first.bare_id(sandbox.sandbox_id)]:
                    first.expect_landed_once(sandbox.sandbox_id)

        with test.phase("the first's slot freed: the second evacuates while the first waits"):
            first.drain()
            deadline = time.monotonic() + 900
            while True:
                # The first's status first: an admission seen while the
                # second is seen still evacuating took a slot it doesn't hold.
                first_waits = first.status().get("waiting") is True
                if second.drain_phase() == "drained":
                    break
                test.verify(
                    first_waits,
                    "the first worker's new drain was admitted before the second was drained",
                    first=first.status(),
                    second=second.status(),
                )
                test.verify(time.monotonic() < deadline, "the second v2 drain never reached drained")
                test.wait(2)
            report = second.expect_drained()
            second.expect_evacuated(report, {waiting.sandbox_id: "started"})
            test.record("second_report", report)

        with test.phase("the second is drained: the first is admitted and seals"):
            drained_at = time.monotonic()
            first.wait_for_report(
                lambda report: report.get("sealed_epoch") == first.epoch,
                "the first worker's new drain never sealed",
                timeout=300,
            )
            test.record("first_sealed_after_seconds", round(time.monotonic() - drained_at, 1))
            test.verify(first.status().get("waiting") is False, "the first drain still waits")

        with test.phase("undrain both and delete the residents"):
            second.undrain_and_expect_cancelled()
            first.undrain_and_expect_cancelled(timeout=1200)
            for sandbox in [*evacuating, waiting]:
                test.delete_sandbox(sandbox)

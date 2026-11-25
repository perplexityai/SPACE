"""A v2 drain past the cluster's evacuation cap is refused, retryably, until a slot frees."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox
from space_smoke.drain_v2 import (
    CAP_REFUSAL_CODE,
    CAP_REFUSAL_CODE_FIELD,
    CAP_REFUSAL_STATUS,
    CAP_RETRY_AFTER,
    DRAIN_HOLD_FIELD,
)

PATH = "/tmp/space-smoke-v2-cap.txt"
# The first drain's hold: it holds its running resident, and so its slot,
# for the whole run.
HOLD_SECONDS = 60 * 60


# The cap's count under its advisory lock, and the refusal of a node with a
# stale heartbeat, are the admin route's tests (`nodes/drain_handler_tests.rs`).
# This scenario is the seam on real workers: with the API's cap at one, a `:drain` of a second worker while the first evacuates is refused
# with the cap's stable code and a `Retry-After`, writes nothing, and its
# resident keeps serving; once the first is undrained and its cancel
# answered, the same `:drain` is accepted and drains the second.
@smoke.scenario(
    "native.v2_drain_refused_at_the_cap",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    tier="manual",
    manual_reason="two public API rollouts, two re-registered workers and a full drain, too slow for the per-merge set",
)
def v2_drain_refused_at_the_cap(test: smoke.CommandSandboxFixture) -> None:
    """With at most one v2 drain evacuating, a `:drain` of a worker while
    another evacuates is a 409 with the cap's code and a `Retry-After`; the
    refused worker has no drain phase and stays schedulable, and its resident
    runs commands, pauses and wakes. Once the first worker is undrained and
    its drain cancelled, the same `:drain` of the second is accepted: it
    seals the worker and reaches `drained`, its resident left suspended and
    unplaced until a wake lands it on the first worker with its file."""

    first = test.drain_v2

    with first.v2_controller(), first.evacuation_cap(1):
        with test.phase("a resident on each worker, each worker under a fake instance"):
            first.pin_to_resident_worker(away_from_fixture=True)
            test.sandbox.suspend()
            second = first.companion()
            second.give_fake_instance()
            first.give_fake_instance()
            # Re-registering a worker under a fake instance recreates its
            # Node without the cordon that kept residents off it.
            second.cordon()
            evacuating = first.create_resident()
            first.cordon()
            second.uncordon()
            first.release_other_worker()
            waiting = second.create_resident()
            second.write_file(waiting, PATH)

        with test.phase("drain the first, holding its resident"):
            first.drain(hold=HOLD_SECONDS)
            first.wait_for_reported()

        with test.phase("a :drain of the second is refused at the cap, writing nothing"):
            refused = second.drain_answer({"engine": "v2", DRAIN_HOLD_FIELD: 0})
            test.verify(
                refused.status == CAP_REFUSAL_STATUS
                and refused.body.get(CAP_REFUSAL_CODE_FIELD) == CAP_REFUSAL_CODE
                and refused.retry_after == CAP_RETRY_AFTER,
                "a :drain past the cap was not the cap's 409 with Retry-After",
                status=refused.status,
                body=refused.body,
                retry_after=refused.retry_after,
            )
            phase = second.drain_phase()
            test.verify(
                phase is None and second.status() == {} and not second.unschedulable(),
                "a :drain refused at the cap left a drain, or a cordon, on its worker",
                drain_phase=phase,
                status=second.status(),
                unschedulable=second.unschedulable(),
            )

        with test.phase("the refused worker's resident serves user calls"):
            second.expect_file(waiting, PATH)
            cast(ResidentLifecycleSandbox, waiting).pause()
            cast(ResidentLifecycleSandbox, waiting).wake()
            after = second.run_on(waiting, "echo resumed")
            test.verify(after.exit_code == 0, "the woken resident did not run a command")
            test.verify(
                second.drain_phase() is None and first.drain_phase() == "draining",
                "the calls did not all land while the second worker was refused",
                second_phase=second.drain_phase(),
                first_phase=first.drain_phase(),
            )

        with test.phase("undrain the first; once cancelled, the second's :drain is accepted"):
            first.undrain_and_expect_cancelled()
            second.drain()
            second.wait_for_reported()

        with test.phase("the second drains; a wake lands its resident on the first worker"):
            second.expect_drained()
            second.expect_suspended_unplaced(waiting.sandbox_id)
            first.uncordon()
            second.wake_elsewhere(waiting)
            second.expect_file(waiting, PATH)

        with test.phase("undrain the second and delete the residents"):
            second.undrain_and_expect_cancelled()
            for sandbox in (evacuating, waiting):
                test.delete_sandbox(sandbox)

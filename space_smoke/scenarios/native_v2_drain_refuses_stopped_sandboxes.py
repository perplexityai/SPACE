"""A v2 drain never moves a stopped sandbox: it is refused on a worker holding one, and refuses stops while it lasts."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import StoppableLifecycleSandbox
from space_smoke.drain_v2 import STOP_VERBS

# The drain's hold: the running resident is held through the refused stops,
# each of which the dispatcher retries until its budget runs out.
HOLD_SECONDS = 60 * 60


# The up-front refusal, its preview and its skip when the preview can't be
# read are the admin route's tests (`nodes/drain_handler_tests.rs`); the
# stop and kill refusal under a v2 drain in any phase is the lifecycle
# gate's (`lifecycle/drain_guard_postgres_tests.rs`); the planner listing
# every stopped sandbox pending, `external`, so a drain that meets one never
# finishes, is sandbox-manager's (`service/drain_node/tests.rs`). This is
# each seam on a real worker. A stopped sandbox that appears on a drained
# worker anyway is not reproduced here: on Kind a VM that exits on its own
# has its execution retired, its node row cleaned up, rather than left
# stopped on the worker. The other worker stays cordoned for the whole run,
# so the resident stays on the drained one.
@smoke.scenario(
    "native.v2_drain_refuses_stopped_sandboxes",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Four refused stops, each retried for its whole budget, on a slow KVM
    # bench.
    timeout_minutes=45,
    tier="manual",
    manual_reason="waits out four refused stops' retry budgets, too slow for the per-merge set",
)
def v2_drain_refuses_stopped_sandboxes(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` of a worker holding a stopped sandbox is refused with
    the stopped refusal's 409 naming it, and writes nothing; once it is
    deleted the same `:drain` is accepted and holds the worker's running
    sandbox. While the drain lasts, a `:stop`, `:kill`, `:force-stop` and
    `:force-kill` of it is each refused with the drain's retryable 503, and
    it keeps running there. Once undrained, its `:stop` goes through."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a stopped and a running sandbox to one worker"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone, and stays cordoned.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            stopped = cast(StoppableLifecycleSandbox, drain.create_resident())
            held = drain.create_resident()
            stopped.stop()
            residency = drain.residency(stopped.sandbox_id)
            test.verify(
                residency == ("stopped", drain.node_id),
                "the stopped sandbox is not stopped on the worker",
                residency=residency,
                node_id=drain.node_id,
            )

        with test.phase("a v2 :drain of the worker is refused for its stopped sandbox, writing nothing"):
            drain.expect_drain_refused_for_stopped([stopped.sandbox_id], hold=HOLD_SECONDS)

        with test.phase("once it is deleted, the same :drain is accepted and holds the running one"):
            test.delete_sandbox(stopped)
            drain.expect_deleted(stopped.sandbox_id, timeout=300)
            drain.drain(hold=HOLD_SECONDS)
            test.verify(
                drain.drain_phase() == "draining" and drain.seconds_to_deadline() > 0,
                "the accepted v2 drain is not draining before its deadline",
                drain_phase=drain.drain_phase(),
                seconds_to_deadline=round(drain.seconds_to_deadline()),
            )
            drain.wait_for_reported()
            drain.expect_held(held.sandbox_id)

        with test.phase("each stop and kill of the running one is refused with a 503; it keeps running"):
            for verb in STOP_VERBS:
                drain.expect_stop_refused(held, verb)
            test.verify(
                drain.seconds_to_deadline() > 0,
                "the refused stops outlasted the hold",
                seconds_to_deadline=round(drain.seconds_to_deadline()),
            )

        with test.phase("undrained, the running sandbox's :stop goes through"):
            drain.undrain_and_expect_cancelled()
            cast(StoppableLifecycleSandbox, held).stop()
            residency = drain.residency(held.sandbox_id)
            test.verify(
                residency[0] == "stopped",
                "the undrained worker's sandbox did not stop",
                residency=residency,
            )
            test.delete_sandbox(held)
            drain.expect_deleted(held.sandbox_id, timeout=300)

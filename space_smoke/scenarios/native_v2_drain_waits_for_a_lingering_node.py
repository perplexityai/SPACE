"""A terminated v2 worker's row waits for its lingering Node, then goes once the Node is deleted."""

import space_smoke as smoke


# `native.v2_drain_terminates_its_worker` with the worker's Node left behind
# its machine, as behind a cloud controller that lags: the harness stops the
# container and keeps the Node. Once EC2 says the fenced instance is
# terminated the drain still doesn't delete the row, and says it waits for the
# Node; the runbook's delete of that Node, with a uid precondition, lets it
# delete the row with no tombstone. It stops a worker, so it runs only by name.
@smoke.scenario(
    "native.v2_drain_waits_for_a_lingering_node",
    drain_v2=True,
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    tier="manual",
    manual_reason="stops a Kind worker and replaces it, too invasive for the per-merge set",
)
def v2_drain_waits_for_a_lingering_node(test: smoke.CommandSandboxFixture) -> None:
    """A v2-drained worker is `:terminate`d and stopped with its Node kept.
    With its fenced instance terminated, the node's row stays and its
    `drain_last_error` says it waits for the Node; once the Node is deleted
    with a uid precondition, the drain deletes the row with no tombstone. The
    worker comes back under the same name with a new instance."""

    drain = test.drain_v2

    with drain.v2_controller(aws=True):
        with test.phase("pin the worker the fixture is not on, under a fake AWS instance"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain with engine v2 until drained"):
            drain.drain()
            drain.expect_drained()

        with test.phase(":terminate; the row waits for the lingering Node, then goes"):
            drain.terminate_keeping_the_node()

        with test.phase("the fixture sandbox still wakes"):
            test.sandbox.wake()

"""A drained v2 worker is terminated through its instance, and its row goes with no tombstone."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-v2-terminate.txt"


# `native.v2_drain_evacuates_every_resident` is the drain. This is what follows
# it, as production runs it (`space_smoke.drain_v2`): `:terminate` checks the
# node's `done` report with a fresh `DrainNode` call, the controller's `aws`
# terminator terminates the instance through Kind's fake EC2 and flags it, the
# harness stops the worker, and the dead-node verb, `live`, judges the node
# lost by its instance and deletes its row, with no tombstone. The worker
# comes back as a same-name replacement; what it found is recorded as
# `after_terminate`. It stops a worker, so it runs only by name.
@smoke.scenario(
    "native.v2_drain_terminates_its_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # A re-registration, one suspend and restore, the drained edge, the
    # terminate and the replacement on a slow KVM bench.
    timeout_minutes=45,
    test_hooks=(
        "SPACE_DEAD_NODE_TEST_TICK_SECS and SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS, which "
        "shorten the dead-node verb's tick and Node-absence window to seconds"
    ),
    ignored="stops a Kind worker and replaces it, too invasive for the per-merge set",
)
def v2_drain_terminates_its_worker(test: smoke.CommandSandboxFixture) -> None:
    """A worker under a fake AWS instance, v2-drained of a started resident
    that comes back on the other worker, is `:terminate`d: its instance is
    terminated through the fake EC2 and the termination flagged under the
    drain's epoch; once the worker is stopped the dead-node verb deletes the
    node's row with no tombstone. The worker comes back under the same name
    with a new instance. The resident's file is intact."""

    drain = test.drain_v2

    with drain.v2_controller(terminating=True):
        with test.phase("pin the worker the fixture is not on, under a fake AWS instance"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()

        with test.phase("pin a started resident to it"):
            resident = drain.create_resident()
            written = drain.run_on(
                resident, f"printf %s {shlex.quote(resident.sandbox_id)} > {shlex.quote(PATH)}"
            )
            test.verify(written.exit_code == 0, "failed to write the resident's file")
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain with engine v2 until drained; the resident is back elsewhere"):
            drain.drain()
            drain.expect_drained()
            drain.expect_back_elsewhere(resident.sandbox_id, "started")

        with test.phase(":terminate: the instance goes, the verb deletes the row, a replacement returns"):
            drain.terminate()

        with test.phase("the resident's file survived"):
            observed = drain.run_on(resident, f"cat {shlex.quote(PATH)}")
            test.verify(
                observed.exit_code == 0 and observed.stdout == resident.sandbox_id,
                "the resident's file did not survive the drain and terminate",
                stdout=observed.stdout,
            )
            test.sandbox.wake()

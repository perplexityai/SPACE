"""A drained v2 worker is terminated through its instance, and the drain deletes its row with no tombstone."""

import space_smoke as smoke

PATH = "/tmp/space-smoke-v2-terminate.txt"


# `native.v2_drain_evacuates_every_resident` is the drain. This is what follows
# it, as production runs it (`space_smoke.drain_v2`): `:terminate` checks the
# node's `done` report with a fresh `DrainNode` call and that nothing names the
# node; the controller fences the instance, and its `aws` terminator asks
# Kind's fake EC2 to terminate exactly it and records the dispatch. The harness
# then stops the worker and deletes its Node; once EC2 says the instance is
# terminated and a GET finds the Node gone, the drain deletes the node's row,
# with no tombstone. The worker comes back as a same-name replacement; what it
# found is recorded as `after_terminate`. A worker holding a stopped sandbox
# never gets here: a v2 drain neither moves one nor terminates its node
# (`native.v2_drain_refuses_stopped_sandboxes`). It stops a worker, so it runs
# only by name.
@smoke.scenario(
    "native.v2_drain_terminates_its_worker",
    drain_v2=True,
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # A re-registration, one suspend and wake, the drained edge, the
    # terminate and the replacement on a slow KVM bench.
    timeout_minutes=45,
    tier="manual",
    manual_reason="stops a Kind worker and replaces it, too invasive for the per-merge set",
)
def v2_drain_terminates_its_worker(test: smoke.CommandSandboxFixture) -> None:
    """A worker under a fake AWS instance, v2-drained of a started resident
    the drain leaves suspended and unplaced until a wake lands it on the
    other worker, is `:terminate`d: the controller
    fences its instance and records the dispatch of its termination through
    the fake EC2; once the worker is stopped and EC2 says the instance is
    terminated, the drain deletes the node's row with no tombstone. The
    worker comes back under the same name with a new instance. The
    resident's file is intact."""

    drain = test.drain_v2

    with drain.v2_controller(aws=True):
        with test.phase("pin the worker the fixture is not on, under a fake AWS instance"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()

        with test.phase("pin a started resident to it"):
            resident = drain.create_resident()
            drain.write_file(resident, PATH)
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain with engine v2 until drained; a wake lands the resident elsewhere"):
            drain.drain()
            report = drain.expect_drained()
            drain.expect_evacuated(report, [resident.sandbox_id])
            drain.expect_suspended_unplaced(resident.sandbox_id)
            drain.wake_elsewhere(resident)

        with test.phase(":terminate: the instance goes, the drain deletes the row, a replacement returns"):
            drain.terminate()

        with test.phase("the resident's file survived"):
            drain.expect_file(resident, PATH)
            test.sandbox.wake()

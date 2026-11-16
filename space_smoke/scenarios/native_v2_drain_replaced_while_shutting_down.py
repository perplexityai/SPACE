"""A same-name replacement that registers while the fenced instance shuts down takes the row."""

import space_smoke as smoke

# How long Kind's fake EC2 keeps the fenced instance `shutting-down`.
SHUTDOWN_SECONDS = 120


# `native.v2_drain_terminates_its_worker` with the cloud slower than the
# replacement: Kind's fake EC2 keeps the fenced instance `shutting-down` for
# minutes (`fake_ec2_delay`), and the worker comes back under the same name
# with a new instance meanwhile. Its registration, a new uid with a known and
# different instance, takes the row and clears the drain and its fence, so the
# drain never deletes the replacement's row once the old instance is
# terminated, and nothing is tombstoned. It stops a worker, so it runs only by
# name.
@smoke.scenario(
    "native.v2_drain_replaced_while_shutting_down",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    tier="manual",
    manual_reason="stops a Kind worker and replaces it, too invasive for the per-merge set",
)
def v2_drain_replaced_while_shutting_down(test: smoke.CommandSandboxFixture) -> None:
    """A v2-drained worker is `:terminate`d, and while its fenced instance is
    still `shutting-down` the worker comes back under the same name with a
    new instance. The replacement's registration takes the row and clears
    the drain, fence included; the row stays, holding the new instance, with
    no tombstone, after the old instance is terminated."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin the worker the fixture is not on, under a fake AWS instance"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain with engine v2 until drained"):
            drain.drain()
            drain.expect_drained()

        with drain.fake_ec2_delay(SHUTDOWN_SECONDS):
            with test.phase(":terminate; the replacement registers while the instance shuts down"):
                drain.terminate_replaced_while_shutting_down()

        with test.phase("the fixture sandbox still wakes"):
            test.sandbox.wake()

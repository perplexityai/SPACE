"""A re-placed stopped sandbox, drained again before anyone started it, is released on its own backup."""

import space_smoke as smoke

# The claim and teardown rules are Postgres-backed crate tests
# (`node_drain/store/stopped_re_drain_tests.rs`); this scenario is the seam on
# real workers. The first forced `:drain` re-places a `stopped` sandbox onto
# the other worker, as `native.forced_drain_re_places_stopped_sandbox` does.
# Then the destination is drained too, before any start. Its cluster row and its
# node both pin the artifact the re-placement staged from, whose backup is
# already durable, so the evacuation takes that pin and releases the node with
# nothing published. On a two-worker bench both workers are then prior hosts,
# so the next re-placement must wait `Unschedulable`, kept off both.
#
# Kind-only, like its sibling: on a deployed backend `test.stopped_replacement`
# raises a configuration error before it touches the controller or any node.
# The sandbox ends unplaced and is deleted by the fixture. Run it on a fleet with
# no node mid-drain and nothing else of yours on either worker.
@smoke.scenario(
    "native.forced_drain_re_drains_re_placed_stopped_sandbox",
    api_surfaces=("native",),
    # Two controller rollouts, a disk-artifact upload and its durability wait
    # on a slow KVM bench, the re-placement, two drains and the second release.
    timeout_minutes=50,
)
def forced_drain_re_drains_re_placed_stopped_sandbox(test: smoke.CommandSandboxFixture) -> None:
    """With the three stopped-evacuation flags on, a forced `:drain` re-places a
    `stopped` sandbox on the other worker. Draining that worker forced, before
    any start, releases the sandbox from it without a new disk backup, and its
    next re-placement pod waits `Unschedulable`, excluding both workers by name
    in every required node-affinity term, while the row stays a claimed
    re-placement."""

    replacement = test.stopped_replacement

    with test.phase("preconditions: no node mid-drain, at least two workers"):
        replacement.require_preconditions()

    with test.phase("record the resident worker"):
        test.run_command_when_ready("true")
        replacement.record_resident()

    with replacement.scoped_flags():
        with test.phase("turn the three stopped-evacuation flags on"):
            replacement.enable_flags()

        with test.phase("stop: a stopped resident of its worker"):
            replacement.stop_on_resident()

        with test.phase("guard the node and drain it forced"):
            replacement.drain_forced()

        with test.phase("the drain re-places the sandbox, stopped, on the other worker"):
            replacement.expect_replaced_stopped()

        with test.phase("the destination was asked for a stopped materialization and holds it stopped"):
            replacement.expect_materialized_without_boot()

        with test.phase("undrain the first worker"):
            replacement.undrain()

        with test.phase("drain the destination forced, before any start"):
            replacement.re_drain_destination()

        with test.phase("the destination is released on the re-placement's own backup"):
            replacement.expect_released_from_destination(new_backup=False)

        with test.phase("the next re-placement waits unschedulable, kept off both prior hosts"):
            replacement.expect_re_placement_waits_unschedulable()

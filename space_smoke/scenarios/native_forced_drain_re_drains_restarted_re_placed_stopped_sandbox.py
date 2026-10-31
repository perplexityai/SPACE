"""A re-placed stopped sandbox that was started and stopped again drains again."""

import space_smoke as smoke

# The rules are crate tests: the post-start pin clear
# (`controllers/sandbox/lifecycle/start_pin_postgres_tests.rs`) and the drain's
# fresh path after it (`node_drain/store/stopped_re_drain_tests.rs`). This is
# the seam on real workers. A forced `:drain` re-places a `stopped` sandbox
# onto the other worker. A user start then runs it there, pre-bound to that
# worker, and drops the disk pin the re-placement left on the row; a stop
# mints a new one. Draining that worker forced releases the sandbox through
# the ordinary path, publishing the new pin's backup, where a stale pin would
# park the node instead. On a two-worker bench both workers are then prior
# hosts, so the next re-placement waits `Unschedulable`.
#
# Kind-only, like its siblings. The sandbox ends unplaced and is deleted by the
# fixture. Run it on a fleet with no node mid-drain and nothing else of yours
# on either worker.
@smoke.scenario(
    "native.forced_drain_re_drains_restarted_re_placed_stopped_sandbox",
    api_surfaces=("native",),
    # Two controller rollouts, two disk-artifact uploads and their durability
    # waits on a slow KVM bench, the re-placement, a cold start, two drains.
    timeout_minutes=60,
)
def forced_drain_re_drains_restarted_re_placed_stopped_sandbox(
    test: smoke.CommandSandboxFixture,
) -> None:
    """With the three stopped-evacuation flags on, a forced `:drain` re-places a
    `stopped` sandbox on the other worker. A user start runs it there in a pod
    pre-bound to that worker, and it's stopped again. Draining that worker
    forced releases the sandbox from it with a new disk backup published, and
    its next re-placement pod waits `Unschedulable`, excluding both workers in
    every required node-affinity term."""

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

        with test.phase("a user start runs it on the destination, pre-bound there"):
            replacement.start_on_destination()

        with test.phase("stop it again on the destination"):
            replacement.stop_on_destination()

        with test.phase("drain the destination forced"):
            replacement.re_drain_destination()

        with test.phase("the destination is released, publishing the new stop's backup"):
            replacement.expect_released_from_destination(new_backup=True)

        with test.phase("the next re-placement waits unschedulable, kept off both prior hosts"):
            replacement.expect_re_placement_waits_unschedulable()

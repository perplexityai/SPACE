"""A forced drain re-places a stopped sandbox, still stopped, on another worker."""

import shlex

import space_smoke as smoke

PATH = "/home/user/data/forced-drain-stopped.txt"
MARKER = "space-smoke-forced-drain-stopped-ok"


# The drain's stopped-resident evacuation, the re-placement claim and its
# finalize are Postgres-backed crate tests (`node_drain/store/`); the shim's
# materialization request is `shim/space_test.go`. This scenario is the seam
# between them on real workers: a forced `:drain` has to publish a real disk
# artifact, a real shim has to stage it without a microVM, and a later user
# start has to boot from what it staged. `local.forced_drain_evacuates_sandbox`
# is the `started` counterpart.
#
# A native scenario because stop/start is a native-API lifecycle. The drain and
# env controls are Kind-only: on a deployed backend `test.stopped_replacement`
# raises a configuration error, so the run ends inconclusive before it touches
# the controller or any node, and only the fixture's own sandbox is created and
# deleted. The drain-keep override is
# set before the drain, as in the started counterpart, so the kind terminator
# never deletes the emptied node. A forced drain moves every evacuable resident
# of that worker, so run it on a fleet with no node mid-drain and nothing else
# of yours on the worker.
@smoke.scenario(
    "native.forced_drain_re_places_stopped_sandbox",
    api_surfaces=("native",),
    # Two controller rollouts, a disk-artifact upload and its durability wait
    # on a slow KVM bench, the re-placement, the drained hold, and a cold start.
    timeout_minutes=45,
)
def forced_drain_re_places_stopped_sandbox(test: smoke.CommandSandboxFixture) -> None:
    """With the three stopped-evacuation flags on, a forced `:drain` of the
    worker holding a `stopped` sandbox re-places it `stopped` on the other
    worker: the row comes back `stopped`, `on_node`, its evacuation marker
    cleared and pinned to the other worker. Every pod the re-placement
    created was a `stopped-materialization` pod, one of them on the other
    worker, and none is left running once the row settles; that worker's
    sandbox-manager reports the sandbox `stopped`, and no lifecycle read in
    between sees it `started`. The emptied
    node reaches `drained` and is held, `:undrain` returns it to schedulable,
    and a user start then runs the sandbox on the new worker, in a fresh pod,
    with a file written before the stop intact."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    replacement = test.stopped_replacement

    with test.phase("preconditions: no node mid-drain, at least two workers"):
        replacement.require_preconditions()

    with test.phase("write state and record the resident worker"):
        written = test.run_command_when_ready(
            f"mkdir -p {shlex.quote(PATH.rsplit('/', 1)[0])} && "
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before stopping the sandbox",
            command_stderr=written.stderr,
        )
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

        with test.phase("the emptied node reaches drained and is held"):
            replacement.expect_drained_and_held()

        with test.phase("undrain: schedulable again, no drain phase"):
            replacement.undrain()

    with test.phase("never observed started between the stop and the user start"):
        replacement.expect_never_started()

    with test.phase("a user start runs it on the destination worker"):
        replacement.start_on_destination()

    with test.phase("the file written before the stop survives the re-placement"):
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

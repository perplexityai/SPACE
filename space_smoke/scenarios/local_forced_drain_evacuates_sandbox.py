"""A forced node drain evacuates the resident sandbox to another worker, state intact."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-forced-drain.txt"
MARKER = "space-smoke-forced-drain-ok"


# The drain reconciler's evacuation (suspend the resident, restore it off the
# draining node) is fake-backed in `node_drain/tests.rs`. The cross-node
# restore is `local.suspend_restore_cross_node_from_object_storage`. This
# scenario is the seam between them: a `:drain` with `mode: forced` has to
# drive a real suspend and a restore that lands on the other worker, which is
# also the path `space-rollout-mng.py` selects by sending `forced`.
# `local.gentle_drain_leaves_sandbox_resident` proves the opposite default.
#
# The drain-keep override is set before the drain, so the kind terminator (1 s
# tick, `kube-delete` backend) never deletes the emptied node, and no tombstone
# or kubelet restart is left behind. `smoke-kind-node-drain.sh` let the node be
# deleted instead; the terminator's contracts are crate tests
# (`drained_node_terminator/tests.rs`, `node_drain/kube_delete/kube_delete_tests.rs`).
# A forced drain moves every evacuable resident of that worker, so run it on a
# fleet with no node mid-drain and nothing else of yours on the worker.
@smoke.scenario(
    "local.forced_drain_evacuates_sandbox",
    # A suspend, a durable upload, and a restore pulled from object storage on
    # a slow KVM bench, then the drained hold and the undrain.
    sandbox=smoke.SandboxConfig(timeout=900),
)
def forced_drain_evacuates_sandbox(test: smoke.KindEcw) -> None:
    """A `:drain` with `mode: forced` parks the resident's worker in `draining`
    in forced mode and cordons it; the drain reconciler suspends the sandbox and
    restores it onto the other worker, from object storage, as `started`, with
    its filesystem intact and its data plane executing; the emptied node then
    reaches `drained` on its own and the drain-keep override holds it there;
    `:undrain` returns it to schedulable with no drain phase."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    drain = test.node_drain
    # Opened before the drain: its sandbox-manager event window starts here, so the
    # evacuation's `runtime_activated` event falls inside it.
    restore = test.cross_node_restore

    with test.phase("preconditions: no node mid-drain, at least two workers"):
        drain.require_clean_fleet()

    with test.phase("write state and record the resident worker"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before draining the sandbox's worker",
            command_stderr=written.stderr,
        )
        drain.resident_node()
        source = restore.current_placement()

    with test.phase("guard the node and drain it forced"):
        drain.drain_forced()

    with test.phase("the drain evacuates the resident, from object storage, to the other worker"):
        landed = drain.expect_evacuated()
        restore.expect_restored_from_object_storage(source=source, landed=landed)

    with test.phase("state and execution survive the evacuation"):
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

    with test.phase("the emptied node reaches drained and is held"):
        drain.expect_drained_and_held()

    with test.phase("undrain: schedulable again, no drain phase, no tombstone"):
        drain.undrain()

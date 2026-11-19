"""A bodiless node drain is gentle: the resident sandbox is left alone until it suspends."""

import space_smoke as smoke


# `local.forced_drain_evacuates_sandbox` proves evacuation; this scenario proves
# the opposite default. It parks the worker hosting the fixture sandbox in a gentle
# drain for twenty seconds and requires nothing about the sandbox to move
# while the user API keeps serving it; then it suspends the
# sandbox by hand (the "resident leaves naturally" path production reaches via
# auto-suspend) and requires the node to converge to drained on its own. The
# drain-keep override guards the node from the kind terminator, so no node is
# terminated and no tombstone is written; run it on a fleet with no node
# mid-drain (`smoke-kind-node-groups.sh` leaks sandboxes that would break the
# convergence half on the same node).
@smoke.scenario(
    "local.gentle_drain_leaves_sandbox_resident",
    # A 20 s hold, a suspend, up to 120 s of convergence, and the undrain.
    sandbox=smoke.SandboxConfig(timeout=900),
)
def gentle_drain_leaves_sandbox_resident(test: smoke.KindEcw) -> None:
    """A `:drain` with no body parks the resident's worker in `draining` with
    `drain_mode = gentle` and cordons it; through a twenty-second observation the
    sandbox keeps its lifecycle, pod, pod UID and node pin, gains no snapshot,
    and the user API keeps serving it; once the sandbox suspends the node
    reaches `drained` on its own with the guarded Kubernetes node still present;
    `:undrain` returns it to schedulable with no drain phase."""

    drain = test.node_drain

    with test.phase("preconditions: no node mid-drain, at least two workers"):
        drain.require_clean_fleet()

    with test.phase("locate the resident worker and record the pod identity"):
        drain.resident_node()

    with test.phase("guard the node and drain it with no body"):
        drain.drain_gently()

    with test.phase("hold twenty seconds: nothing about the resident may move"):
        drain.expect_resident_untouched()

    with test.phase("suspend the resident; the drain converges to drained with its Kubernetes node present"):
        test.pause()
        drain.expect_drained_node_present()

    with test.phase("undrain: schedulable again, no drain phase, no tombstone"):
        drain.undrain()

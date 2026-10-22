"""The reconcile-queue GC prunes a gone node's day-old refs and day-old dead letters, nothing else."""

import space_smoke as smoke


# The GC runs at the binary's fixed timers (a 300 s tick, 24 h retention), so
# the body seeds synthetic non-ready nodes with backdated refs and waits out
# one tick; its EC2 lookups reach Kind's fake EC2 endpoint, which answers
# from the Nodes (see `space_smoke.reconcile_queue_gc`). The sandbox is the
# fixture's and unused; its timeout outlasts the wait.
@smoke.scenario(
    "local.reconcile_queue_gc_prunes_only_gone_nodes",
    sandbox=smoke.SandboxConfig(timeout=1200),
)
def reconcile_queue_gc_prunes_only_gone_nodes(test: smoke.KindEcw) -> None:
    """With the reconcile-queue GC on against the fake EC2 endpoint, one tick
    deletes a 25 h-old dead letter and the 25 h-old live ref of a non-ready
    node whose Kubernetes Node does not exist, and keeps a 1 h-old dead
    letter and the 25 h-old live ref of a non-ready node named after a Ready
    Node."""

    gc = test.reconcile_queue_gc

    with test.phase("the controller runs the GC against the fake EC2 endpoint"):
        gc.expect_gc_on_against_the_fake()

    with test.phase("seed a live and a gone node with day-old refs"):
        gc.seed()

    with test.phase("one GC tick prunes only the gone node's ref and the day-old dead letter"):
        gc.expect_one_tick_prunes_only_the_gone()

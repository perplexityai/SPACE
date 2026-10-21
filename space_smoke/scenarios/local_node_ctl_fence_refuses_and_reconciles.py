"""node-ctl's `patch nodes` fence refuses every cross-boundary write and still admits node-ctl's own."""

import space_smoke as smoke


# Every phase but one takes under a second: the two convergence canaries ride
# node-ctl's facet poll (60 s deployed, 20 s in the kind chart) and run
# concurrently, so that poll is the stated reason for the time; the 180 s
# budget is a ceiling for a stalled run. Node writes are serialized with the
# drain scenario through the worker cordon lease; the boundary label is live
# on the worker for one probe.
@smoke.scenario(
    "local.node_ctl_fence_refuses_and_reconciles",
    sandbox=smoke.SandboxConfig(timeout=900),
)
def node_ctl_fence_refuses_and_reconciles(test: smoke.KindEcw) -> None:
    """With the token kubelet projects into the node-ctl pod on this sandbox's
    worker, every forbidden Node write (taints, cordon, finalizers, the
    node-group boundary and cred-authority labels, foreign or inflated status
    resources, forged conditions, a template-write pull request, and anything
    on another node) is refused by the fence policy and by the named rule,
    leaves no residue, and the boundary label is otherwise writable; every
    write shape node-ctl has (SSD placement apply, host-CPU label apply,
    template-presence apply and prune, template-write clear, capacity apply)
    is admitted; and the running
    node-ctl writes back a stripped owned label and the stripped slot capacity
    through its own credential."""

    fence = test.node_ctl_fence

    with test.phase("locate the worker's node-ctl pod and a second node"):
        fence.locate()

    with test.phase("the fence is installed, bound Deny, and type-checks clean"):
        fence.expect_fence_enforcing()

    with test.phase("read the credential a guest escape would reach; it is the SA, bound to this node, granted patch"):
        fence.read_credential()

    with test.phase("snapshot both nodes; arm the reverts and the slots repair"):
        fence.snapshot_and_arm()

    with test.phase("refusals: each forbidden write is refused by the named rule"):
        fence.expect_refusals()

    with test.phase("the refused writes left nothing behind"):
        fence.expect_no_residue()

    with test.phase("the refused boundary-label write is otherwise possible"):
        fence.expect_control_write_then_refused_strip()

    with test.phase("node-ctl's own writes still succeed"):
        fence.expect_owned_writes_succeed()

    with test.phase("the live node-ctl reconcile converges through the fence (label and slots, concurrently)"):
        fence.expect_node_ctl_reconciles()

    with test.phase("revert the probes' fields and verify both nodes are clean"):
        fence.retire()

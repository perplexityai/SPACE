"""Admin `:terminate` refuses a node the node-drain reconciler still holds."""

import space_smoke as smoke


@smoke.scenario(
    "local.terminate_gate_refuses_a_held_node",
    # No sandbox of its own is needed beyond the fixture's: the scenario seeds
    # one control-plane row, POSTs once, and undoes both writes.
    sandbox=smoke.SandboxConfig(timeout=900),
)
def terminate_gate_refuses_a_held_node(test: smoke.KindEcw) -> None:
    """A `suspending` sandbox pinned to a drained node by `target_node_id`
    alone makes admin `:terminate` answer 409, counting that row as a drain
    blocker and no resident. That is the shape the guard used to miss: it is
    the same predicate the node-drain reconciler holds the node on, so the
    endpoint and the reconciler now agree about whether the node is free."""

    with test.phase("seed a suspending row pinned to an otherwise idle node"):
        test.drain_gate.seed_blocker()

    with test.phase("POST :terminate and read the refusal"):
        test.drain_gate.expect_terminate_refused()

    with test.phase("undrain the node and delete the seeded row"):
        test.drain_gate.release_blocker()

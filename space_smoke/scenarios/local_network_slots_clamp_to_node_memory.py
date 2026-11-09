"""With the clamp on, a worker that refuses new sandboxes advertises no free network slots."""

import space_smoke as smoke

PLACED_CREATES = 3


# node-manager on one worker runs with a memory admission reserve no node
# can meet, so its real admission check refuses every new sandbox there; the
# other worker admits as before (see `space_smoke.node_memory_admission`).
# The API's fail-fast flag stays off, so the scenario sees placement alone.
# With the clamp off, a create the scheduler can only place on the refusing
# worker (the other one cordoned) is bound there and fails. With it on, the
# refusing worker advertises no network slots past those its bound pods hold,
# and creates are placed on the other worker. The observables are each
# worker Node's allocatable network slots and the pods' `nodeName`.
@smoke.scenario(
    "local.network_slots_clamp_to_node_memory",
    # Three node-manager and two node-ctl rollouts, one create that waits out the ready budget.
    sandbox=smoke.SandboxConfig(timeout=900),
)
def network_slots_clamp_to_node_memory(test: smoke.KindEcw) -> None:
    """With one worker's node-manager refusing new sandboxes at memory
    admission and node-ctl's slot clamp off, a create placed on that worker
    is bound there and fails. With the clamp on, that worker advertises no
    network slots beyond those its bound pods hold, the other worker still
    advertises free slots (clamped to its own memory, never above standing),
    and creates succeed on the other worker. Once the reserve is back, with
    the clamp still on, the refusing worker advertises free slots again."""

    admission = test.node_memory_admission
    workers = admission.workers()
    test.verify(len(workers) >= 2, "the scenario needs two sandbox workers", workers=workers)
    refusing, admitting = workers[0], workers[1]
    test.record("refusing_worker", refusing)
    test.record("admitting_worker", admitting)

    with admission.scoped_knobs():
        with test.phase("read each worker's standing network-slot capacity"):
            standing = admission.capacity()
            test.record("standing_capacity", standing)
            test.verify(
                all(c["advertised"] for c in standing.values()),
                "a worker advertises no network slots before the scenario",
                capacity=standing,
            )

        with test.phase(f"node-manager on {refusing} refuses new sandboxes at memory admission"):
            admission.refuse_new_sandboxes(nodes=(refusing,))

        with test.phase(f"clamp off: a create placed on {refusing} is bound there and fails"):
            with admission.cordoned(admitting):
                off = admission.timed_create("clamp_off")
            test.verify(
                not 200 <= off.attempt.status_code < 300,
                "a create on the refusing worker succeeded",
                status=off.attempt.status_code,
            )
            test.verify(
                set(off.new_pod_nodes.values()) == {refusing},
                "with the clamp off the create's pod was not bound to the refusing worker",
                new_pod_nodes=off.new_pod_nodes,
            )

        with test.phase(f"clamp on: {refusing} advertises no slot past its bound pods"):
            admission.set_clamp(True)
            admission.expect_capacity(
                lambda seen: (
                    (seen[refusing]["advertised"] or 0) <= (seen[refusing]["bound"] or 0)
                    and (seen[admitting]["bound"] or 0) + PLACED_CREATES
                    <= (seen[admitting]["advertised"] or 0)
                    <= (standing[admitting]["advertised"] or 0)
                ),
                "the refusing worker still advertises free network slots, or the admitting one has no room",
            )

        with test.phase(f"clamp on: creates are placed on {admitting}"):
            for attempt in range(PLACED_CREATES):
                placed = admission.timed_create(f"clamp_on_{attempt}")
                test.verify(
                    200 <= placed.attempt.status_code < 300 and placed.attempt.sandbox_id is not None,
                    "a create failed with the admitting worker advertising room",
                    status=placed.attempt.status_code,
                    body=placed.attempt.body[:600],
                )
                test.verify(
                    set(placed.new_pod_nodes.values()) == {admitting},
                    "with the clamp on a create's pod was not placed on the admitting worker",
                    new_pod_nodes=placed.new_pod_nodes,
                )

        with test.phase(f"reserve back, clamp on: {refusing} advertises free slots again"):
            admission.admit_new_sandboxes()
            admission.expect_capacity(
                lambda seen: (seen[refusing]["bound"] or 0)
                < (seen[refusing]["advertised"] or 0)
                <= (standing[refusing]["advertised"] or 0),
                "the refusing worker advertised no free slots once admitting",
            )

"""A worker that dies mid-v2-drain: its durable resident comes back, the rest are lost, snapshots kept."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox


# The verb's rule for each row is a crate test (`dead_node/store/tests.rs`:
# a v2 drain's node is judged in every phase). Here the worker dies with
# its drain's suspends local-only (its uploads blocked), as an instance lost
# mid-drain: Kind's fake EC2 marks it terminated and the harness stops it.
# The dead-node verb, `live`, judges it lost: a resident suspended durably
# before the drain stays suspended and wakes again; the drain's own
# suspends never became durable, so those residents are lost, one keeping
# the snapshot it had from before. The worker then comes back as a
# same-name replacement. It stops a worker, so it runs only by name.
@smoke.scenario(
    "native.v2_drain_outlived_by_a_dead_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=60,
    test_hooks=(
        "SPACE_DEAD_NODE_TEST_TICK_SECS, SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS and "
        "SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS, which shorten the dead-node verb's "
        "tick, Node-absence window and upload settle window to seconds"
    ),
    ignored="stops a Kind worker mid-drain and replaces it, too invasive for the per-merge set",
)
def v2_drain_outlived_by_a_dead_worker(test: smoke.CommandSandboxFixture) -> None:
    """A worker holding a durably suspended resident, a started one with a
    snapshot from an earlier suspend, and a started one with none, dies
    while a v2 drain's suspends wait on their blocked uploads. The dead-node
    verb deletes the node's row with no tombstone; the suspended resident
    stays suspended and wakes again; the other two are lost,
    with no VM, the one with an earlier snapshot keeping it. The worker
    comes back under the same name with a new instance."""

    drain = test.drain_v2

    with drain.v2_controller(terminating=True):
        with test.phase("pin a suspended, a once-suspended started and a started resident"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            kept = drain.create_resident()
            retained = drain.create_resident()
            lost = drain.create_resident()
            for sandbox in (kept, retained, lost):
                drain.fill_memory(sandbox, mib=64)
            retained.suspend()
            retained.wake()
            drain.expect_back_on_drained(retained.sandbox_id)
            kept.suspend()
            earlier = drain.ready_snapshots(retained.sandbox_id)
            test.verify(bool(earlier), "the once-suspended resident has no durable snapshot")
            cast(DurableLifecycleSandbox, test.sandbox).suspend()
            drain.release_other_worker()

        with test.phase("uploads blocked: drain until both suspends wait on their uploads"):
            drain.block("object-store")
            epoch = drain.drain()
            drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and set(drain.pending_actions(report))
                >= {drain.bare_id(retained.sandbox_id), drain.bare_id(lost.sandbox_id)}
                and set(drain.pending_actions(report).values()) == {"upload"},
                "the node never reported both drain suspends waiting on their uploads",
                timeout=300,
            )

        with test.phase("the worker dies; the verb deletes its row; it comes back"):
            statuses = drain.die()

        with test.phase("the suspended resident stays suspended; the others are lost"):
            outcomes = {
                drain.bare_id(sandbox.sandbox_id): drain.residency(sandbox.sandbox_id)
                for sandbox in (kept, retained, lost)
            }
            snapshots = {
                drain.bare_id(sandbox.sandbox_id): drain.ready_snapshots(sandbox.sandbox_id)
                for sandbox in (kept, retained, lost)
            }
            test.record(
                "after_death",
                {
                    "residency": outcomes,
                    "ready_snapshots": {
                        sandbox_id: [record.snapshot_id for record in records]
                        for sandbox_id, records in snapshots.items()
                    },
                },
            )
            kept_lifecycle, _ = outcomes[drain.bare_id(kept.sandbox_id)]
            test.verify(
                kept_lifecycle == "suspended",
                "the durably suspended resident did not stay suspended",
                outcomes=outcomes,
                statuses=statuses,
            )
            for sandbox in (retained, lost):
                lifecycle, node = outcomes[drain.bare_id(sandbox.sandbox_id)]
                test.verify(
                    lifecycle not in ("started", "paused", "suspended", "restoring")
                    and node is None,
                    "a resident whose drain suspend never became durable was not written off",
                    sandbox_id=sandbox.sandbox_id,
                    lifecycle=lifecycle,
                    active_node_id=node,
                    statuses=statuses,
                )
                drain.expect_single_vm(sandbox.sandbox_id, "lost")
            test.verify(
                [record.snapshot_id for record in snapshots[drain.bare_id(retained.sandbox_id)]]
                == [record.snapshot_id for record in earlier],
                "the lost resident did not keep the snapshot it had before the drain",
                earlier=[record.snapshot_id for record in earlier],
                after=[record.snapshot_id for record in snapshots[drain.bare_id(retained.sandbox_id)]],
            )

        with test.phase("the suspended resident wakes"):
            kept.wake()
            drain.expect_lifecycle(kept.sandbox_id, "started")
            drain.expect_single_vm(kept.sandbox_id, "started")
            test.sandbox.wake()

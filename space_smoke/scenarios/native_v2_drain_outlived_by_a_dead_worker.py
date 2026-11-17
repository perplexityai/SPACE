"""A worker that dies mid-v2-drain is let go, and the gone-node loops take its rows as any crashed node's."""

import time
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox, SandboxRowState

# How long Kind's fake EC2 keeps the dead worker's instance `shutting-down`:
# longer than `lost_started_sandboxes` needs to act on a crashed node (its
# 10-minute Node-absence grace and its 5-minute tick), so a write-off that
# could happen before the let-go would.
SHUTDOWN_SECONDS = 1200
# The gone-node loops' tick, and margin: how long after the let-go each one
# has to act.
LOOP_SECONDS = 420
# More started residents than `SPACE_DRAIN_NODE_WINDOW`'s default of 8. With
# uploads blocked, the window holds eight suspends whose uploads never finish
# and queues the rest, which the drain never suspends: their rows are still
# published `started` on the worker when it dies.
STARTED = 10


# The drain's let-go is a crate test (`node_drain::v2`: every unfenced state
# lets go once EC2 says the instance is terminated). Here the worker dies with
# its drain's suspends waiting on blocked uploads, as an instance lost
# mid-drain: Kind's fake EC2 marks it terminated after a delay, and the harness
# stops it and deletes its Node. While EC2 says `shutting-down` the drain still
# owns the node, so a failed wake writes nothing off; once EC2 says
# `terminated` the drain lets the node go, writing no row off itself, and the
# gone-node loops the scenario turns on for itself (`lost_started_sandboxes`,
# `orphaned_nodes`, `orphaned_sandboxes` writing) take the node as any crashed
# node's. A row still published `started` on the dead node leaves it as any
# crashed node's does: `lost_started_sandboxes` writes it off once its started
# recovery fails, or the recovery starts it again on another worker once its
# pod's execution was forgotten; the scenario records which. A row parked
# mid-suspend still names the node, so `orphaned_nodes` can't delete it while
# one does; the scenario deletes those rows itself before it expects the node
# row gone. It stops a worker, so it runs only by name.
@smoke.scenario(
    "native.v2_drain_outlived_by_a_dead_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=120,
    tier="manual",
    manual_reason="stops a Kind worker mid-drain and replaces it, too invasive for the per-merge set",
)
def v2_drain_outlived_by_a_dead_worker(test: smoke.CommandSandboxFixture) -> None:
    """A worker holding a durably suspended resident and more started ones
    than the drain's window dies while the window's suspends wait on their
    blocked uploads. While its instance is `shutting-down` the drain keeps
    the node and nothing is written off, even after a failed wake; once EC2
    says `terminated` the drain lets the node go with its trace, writing no
    row. Then each resident the window queued, still published `started`,
    leaves the dead node as a crashed node's does, written off or started
    again elsewhere, each one mid-suspend is left parked on the node until
    the run deletes it, the
    suspended resident stays suspended and wakes on the other worker, and
    once nothing pins the node `orphaned_nodes` deletes its row, with no
    tombstone."""

    drain = test.drain_v2

    with drain.v2_controller(legacy_loops=True):
        with test.phase("pin a suspended resident and more started ones than the window"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            kept = drain.create_resident()
            started = [drain.create_resident() for _ in range(STARTED)]
            for sandbox in (kept, *started):
                drain.fill_memory(sandbox, mib=64)
            kept.suspend()
            cast(DurableLifecycleSandbox, test.sandbox).suspend()
            drain.release_other_worker()

        bare = {drain.bare_id(sandbox.sandbox_id): sandbox for sandbox in started}
        with test.phase("uploads blocked: drain until the window holds its suspends and queues the rest"):
            drain.block("object-store")
            epoch = drain.drain()
            report = drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and set(drain.pending_actions(report)) >= set(bare)
                and "queued" in drain.pending_actions(report).values(),
                "the node never reported every started resident pending, some queued",
                timeout=300,
            )
            queued = sorted(
                sandbox_id
                for sandbox_id, action in drain.pending_actions(report).items()
                if action == "queued" and sandbox_id in bare
            )
            test.record("queued_before_death", queued)

        def wake_fails(sandbox: DurableLifecycleSandbox) -> str:
            try:
                sandbox.wake()
            except Exception as error:  # noqa: BLE001 - the wake is expected to fail
                return f"{type(error).__name__}: {error}"
            return "woke"

        def lifecycles() -> dict[str, tuple[str, str | None]]:
            return {
                drain.bare_id(sandbox.sandbox_id): drain.residency(sandbox.sandbox_id)
                for sandbox in (kept, *started)
            }

        with drain.fake_ec2_delay(SHUTDOWN_SECONDS):
            with test.phase("the worker dies; while it shuts down the drain keeps it"):
                restart = drain.die()
                deadline = time.monotonic() + SHUTDOWN_SECONDS - 120
                wakes: list[dict[str, str]] = []
                while time.monotonic() < deadline:
                    wakes.append({sandbox_id: wake_fails(bare[sandbox_id]) for sandbox_id in queued})
                    seen = lifecycles()
                    test.verify(
                        all(lifecycle != "failed" for lifecycle, _ in seen.values())
                        and drain.drain_phase() == "draining",
                        "a row was written off, or the node left the drain, before the let-go",
                        lifecycles=seen,
                        drain_phase=drain.drain_phase(),
                    )
                    test.wait(60)
                test.record("wakes_before_let_go", wakes)

            with test.phase("EC2 says terminated: the drain lets the node go"):
                trace = drain.expect_let_go(timeout=900)
                at_let_go = drain.row_states(started)
                test.record(
                    "at_let_go",
                    {"trace": trace, "rows": {key: row.__dict__ for key, row in at_let_go.items()}},
                )

        published = [
            sandbox_id
            for sandbox_id in bare
            if at_let_go[sandbox_id].lifecycle in ("started", "paused")
        ]
        parked = [sandbox_id for sandbox_id in bare if sandbox_id not in published]
        with test.phase("the queued residents are published started; the rest are parked on the node"):
            test.record("published_at_let_go", published)
            test.record("parked_at_let_go", parked)
            for sandbox_id in queued:
                row = at_let_go[sandbox_id]
                test.verify(
                    row.lifecycle == "started" and row.active_node_id == drain.node_id,
                    "a resident the window queued was not published started on the dead node",
                    sandbox_id=sandbox_id,
                    row=row.__dict__,
                )
            for sandbox_id in parked:
                row = at_let_go[sandbox_id]
                test.verify(
                    row.lifecycle in ("suspending", "suspended")
                    and drain.node_id in (row.active_node_id, row.target_node_id),
                    "a resident mid-suspend is not parked on the dead node",
                    sandbox_id=sandbox_id,
                    row=row.__dict__,
                )

        def left_the_node(row: SandboxRowState) -> str | None:
            if row.lifecycle == "failed":
                return "written off"
            if row.lifecycle == "started" and row.active_node_id not in (None, drain.node_id):
                return f"started on {row.active_node_id}"
            return None

        with test.phase("the published rows leave the dead node as a crashed node's"):
            deadline = time.monotonic() + 2 * LOOP_SECONDS
            while True:
                wakes_after = {sandbox_id: wake_fails(bare[sandbox_id]) for sandbox_id in published}
                rows = drain.row_states([bare[sandbox_id] for sandbox_id in published])
                outcomes = {key: left_the_node(row) for key, row in rows.items()}
                if len(outcomes) == len(published) and all(outcomes.values()):
                    break
                test.verify(
                    time.monotonic() < deadline,
                    "a published row stayed on the dead node after the let-go",
                    wakes_after_let_go=wakes_after,
                    rows_after_let_go={key: row.__dict__ for key, row in rows.items()},
                )
                test.wait(30)
            test.record("published_after_let_go", outcomes)
            if parked:
                test.record(
                    "parked_before_delete",
                    {
                        key: row.__dict__
                        for key, row in drain.row_states([bare[sandbox_id] for sandbox_id in parked]).items()
                    },
                )
            kept_lifecycle, _ = drain.residency(kept.sandbox_id)
            test.verify(
                kept_lifecycle == "suspended",
                "the durably suspended resident did not stay suspended",
                lifecycles=lifecycles(),
            )

        with test.phase("delete the parked rows; the suspended resident wakes elsewhere; the node row goes"):
            for sandbox_id in parked:
                test.delete_sandbox(bare[sandbox_id])
                drain.expect_lifecycle_back(bare[sandbox_id].sandbox_id, "deleted", timeout=600)
            kept.wake()
            drain.expect_lifecycle(kept.sandbox_id, "started")
            _, node = drain.residency(kept.sandbox_id)
            test.verify(
                node is not None and node != drain.node_id,
                "the suspended resident did not wake on the other worker",
                active_node_id=node,
            )
            drain.expect_node_row_deleted_without_tombstone(timeout=2 * LOOP_SECONDS)

        with test.phase("the worker comes back under the same name"):
            drain.come_back_after_death(restart)
            test.sandbox.wake()

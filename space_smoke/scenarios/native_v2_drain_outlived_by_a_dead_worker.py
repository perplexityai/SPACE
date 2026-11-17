"""A worker that dies mid-v2-drain is let go, and the gone-node loops take its rows as any crashed node's."""

import time
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox, SandboxRowState

# How long Kind's fake EC2 keeps the dead worker's instance `shutting-down`:
# long enough for failed wakes of the queued residents and many drain ticks
# while it does. It need not outlast `lost_started_sandboxes`' 10-minute
# Node-absence grace and 5-minute tick: no row becomes that loop's candidate
# on Kind (see below), so a longer wait could not show a write-off either.
# What it shows is that the drain itself writes nothing off and keeps the
# node while EC2 says the instance is shutting down.
SHUTDOWN_SECONDS = 180
# A reading this close to the end of the window may already see the let-go.
SHUTDOWN_MARGIN_SECONDS = 30
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
# stops it and deletes its Node. The run shows that while EC2 says
# `shutting-down` the drain keeps the node and no row is written off, even
# after a failed wake; that once EC2 says `terminated` the drain lets the node
# go, writing no row itself; and that each row still published `started` on
# the dead node then leaves it the way a crashed node's does, under the
# gone-node loops the scenario turns on for itself (`lost_started_sandboxes`,
# `orphaned_nodes`, `orphaned_sandboxes` writing): `failed`, or `started` on
# another worker, and the scenario records which.
#
# The write-off branch and the parked-row branch are not exercised on Kind. A
# never-suspended `started` resident of the dead worker is started again on
# the other worker: a request-path pod resolve forgets its execution and
# unplaces the row, and with no prior state the recovery boots the template,
# so `lost_started_sandboxes` never gets a candidate. The run still accepts a
# written-off row, and deletes a row parked mid-suspend itself before it
# expects the node row gone, since a row that names the node keeps
# `orphaned_nodes` from deleting it. It stops a worker, so it runs only by
# name.
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
    the node and no row is written off, even after a failed wake; once EC2
    says `terminated` the drain lets the node go with its trace, writing no
    row. Then each resident still published `started` on the dead node
    leaves it as a crashed node's does, `failed` or started again
    elsewhere, recorded; the suspended resident stays suspended and wakes
    on the other worker; and once nothing pins the node `orphaned_nodes`
    deletes its row, with no tombstone. The write-off branch and the
    parked-row branch are not exercised on Kind."""

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
                # The fake EC2 reads `shutting-down` from the terminate `die`
                # starts with, so the window starts no later than this.
                window_end = time.monotonic() + SHUTDOWN_SECONDS - SHUTDOWN_MARGIN_SECONDS
                restart = drain.die()
                wakes: list[dict[str, str]] = []
                checked = 0
                while time.monotonic() < window_end:
                    wakes.append({sandbox_id: wake_fails(bare[sandbox_id]) for sandbox_id in queued})
                    # Only a reading started inside the window says anything:
                    # a failed wake can take long enough to end past it.
                    if time.monotonic() >= window_end:
                        break
                    seen = lifecycles()
                    phase = drain.drain_phase()
                    test.verify(
                        all(lifecycle != "failed" for lifecycle, _ in seen.values())
                        and phase == "draining",
                        "a row was written off, or the node left the drain, before the let-go",
                        lifecycles=seen,
                        drain_phase=phase,
                    )
                    checked += 1
                    test.wait(20)
                test.record("wakes_before_let_go", wakes)
                test.verify(
                    checked > 0 and bool(wakes),
                    "the shutdown window ended before a failed wake and a reading of the drain",
                    wakes=wakes,
                    checked=checked,
                )

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

        # `failed` is the write-off branch, which Kind doesn't reach: there a
        # published row is started again on the other worker.
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

"""A worker that dies mid-v2-drain is let go, and the drain hands its rows to the gone-node loops."""

import time
from typing import Any, cast

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
WINDOW = 8


# The drain's let-go is a crate test (`node_drain::v2`: every unfenced state
# lets go once EC2 says the instance is terminated). Here the worker dies with
# its drain's suspends waiting on blocked uploads, as an instance lost
# mid-drain: Kind's fake EC2 marks it terminated after a delay, and the harness
# stops it and deletes its Node. The run shows that while EC2 says
# `shutting-down` the drain keeps the node and no row is written off, even
# after a failed wake; that once EC2 says `terminated` the drain lets the node
# go, writing no row itself; and that the drain's handover holds: its columns
# stay cleared and no row still published `started` on the dead node names it,
# active or target, under the gone-node loops the scenario turns on for itself
# (`lost_started_sandboxes`, `orphaned_nodes`, `orphaned_sandboxes` writing).
# What those loops make of each row afterwards is the crashed-node handling,
# not the drain's: the run records each row's lifecycle and node without
# judging it.
#
# The parked-row branch is not exercised on Kind. The run deletes a row parked
# mid-suspend itself before it records whether the node row went, since a row
# that names the node keeps `orphaned_nodes` from deleting it, and so can a
# snapshot the dead worker left `creating`. It stops a worker, so it runs
# only by name.
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
    row. Then the drain's columns stay cleared and no resident still
    published `started` on the dead node names it, active or target; where
    each ends up is the crashed-node handling's, recorded; the suspended
    resident stays suspended and wakes on the other worker; and nothing
    tombstones the node, while whether `orphaned_nodes` deletes its row, and
    which `creating` snapshots still name it, is recorded. The parked-row
    branch is not exercised on Kind."""

    drain = test.drain_v2

    with drain.v2_controller(aws=True, legacy_loops=True):
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
        # The node's report says only that each started resident is moving:
        # one the window took is no longer `started` on the worker, its
        # upload waiting, and one waiting for a slot still is.
        def window_full(report: dict[str, Any]) -> bool:
            if not drain.reported(report) or set(drain.pending_waits(report)) < set(bare):
                return False
            states = drain.node_states([bare[sandbox_id].sandbox_id for sandbox_id in bare])
            return list(states.values()).count("started") == STARTED - WINDOW

        with test.phase("uploads blocked: drain until the window holds its suspends and the rest wait"):
            drain.block("object-store")
            drain.drain()
            report = drain.wait_for_report(
                window_full,
                "the node never reported every started resident pending, the window's taken and the rest waiting",
                timeout=180,
            )
            states = drain.node_states([sandbox.sandbox_id for sandbox in started])
            queued = sorted(sandbox_id for sandbox_id, state in states.items() if state == "started")
            test.record("node_states_before_death", states)
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

        # The drain's handover: the row names the dead node neither active nor
        # target. What it becomes then is the gone-node loops' to decide.
        def off_the_node(row: SandboxRowState) -> bool:
            return drain.node_id not in (row.active_node_id, row.target_node_id)

        published = [
            sandbox_id
            for sandbox_id in bare
            if at_let_go[sandbox_id].lifecycle in ("started", "paused")
        ]
        parked = [sandbox_id for sandbox_id in bare if sandbox_id not in published]
        with test.phase("the queued residents are published started; the rest are parked on the node"):
            test.record("published_at_let_go", published)
            test.record("parked_at_let_go", parked)
            # Once the node is let go its rows are a crashed node's, so a call
            # may already have started one again elsewhere by this read.
            for sandbox_id in queued:
                row = at_let_go[sandbox_id]
                test.verify(
                    (row.lifecycle == "started" and row.active_node_id == drain.node_id)
                    or off_the_node(row),
                    "a resident the window queued was neither published started on the dead node "
                    "nor off it",
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

        with test.phase("the drain hands the published rows over: its columns stay cleared, none names the node"):
            deadline = time.monotonic() + 2 * LOOP_SECONDS
            while True:
                wakes_after = {sandbox_id: wake_fails(bare[sandbox_id]) for sandbox_id in published}
                rows = drain.row_states([bare[sandbox_id] for sandbox_id in published])
                if len(rows) == len(published) and all(off_the_node(row) for row in rows.values()):
                    break
                test.verify(
                    time.monotonic() < deadline,
                    "a published row stayed on the dead node after the let-go",
                    wakes_after_let_go=wakes_after,
                    rows_after_let_go={key: row.__dict__ for key, row in rows.items()},
                )
                test.wait(30)
            test.record(
                "published_after_let_go",
                {key: {"lifecycle": row.lifecycle, "active_node_id": row.active_node_id} for key, row in rows.items()},
            )
            test.record("wakes_after_let_go", wakes_after)
            drain.expect_still_let_go()
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

        with test.phase("delete the parked rows; the suspended resident wakes elsewhere; the node row is recorded"):
            for sandbox_id in parked:
                test.delete_sandbox(bare[sandbox_id])
                drain.expect_deleted(bare[sandbox_id].sandbox_id, timeout=600)
            kept.wake()
            drain.expect_lifecycle(kept.sandbox_id, "started")
            _, node = drain.residency(kept.sandbox_id)
            test.verify(
                node is not None and node != drain.node_id,
                "the suspended resident did not wake on the other worker",
                active_node_id=node,
            )
            # Deleting the node row is `orphaned_nodes`', the crashed-node
            # handling's, which a `creating` snapshot naming the node can hold
            # back: recorded, not judged.
            drain.record_node_row_after_let_go(timeout=2 * LOOP_SECONDS)

        with test.phase("the worker comes back under the same name"):
            drain.come_back_after_death(restart)
            test.sandbox.wake()

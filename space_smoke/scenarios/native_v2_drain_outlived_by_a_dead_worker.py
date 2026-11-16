"""A worker that dies mid-v2-drain is let go, and the gone-node loops take its rows as any crashed node's."""

import time
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox

# How long Kind's fake EC2 keeps the dead worker's instance `shutting-down`:
# longer than `lost_started_sandboxes` needs to act on a crashed node (its
# 10-minute Node-absence grace and its 5-minute tick), so a write-off that
# could happen before the let-go would.
SHUTDOWN_SECONDS = 1200
# The gone-node loops' tick, and margin: how long after the let-go each one
# has to act.
LOOP_SECONDS = 420


# The drain's let-go is a crate test (`node_drain::v2`: every unfenced state
# lets go once EC2 says the instance is terminated). Here the worker dies with
# its drain's suspends waiting on blocked uploads, as an instance lost
# mid-drain: Kind's fake EC2 marks it terminated after a delay, and the harness
# stops it and deletes its Node. While EC2 says `shutting-down` the drain still
# owns the node, so a failed wake writes nothing off; once EC2 says
# `terminated` the drain lets the node go, writing no row off itself, and the
# gone-node loops the scenario turns on for itself (`lost_started_sandboxes`,
# `orphaned_nodes`, `orphaned_sandboxes` writing) take the node as any crashed
# node's. It stops a worker, so it runs only by name.
@smoke.scenario(
    "native.v2_drain_outlived_by_a_dead_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=120,
    tier="manual",
    manual_reason="stops a Kind worker mid-drain and replaces it, too invasive for the per-merge set",
)
def v2_drain_outlived_by_a_dead_worker(test: smoke.CommandSandboxFixture) -> None:
    """A worker holding a durably suspended resident and two started ones
    dies while a v2 drain's suspends wait on their blocked uploads. While its
    instance is `shutting-down` the drain keeps the node and nothing is
    written off, even after a failed wake; once EC2 says `terminated` the
    drain lets the node go with its trace, writing no row. Then a resident
    still published `started` is written off by `lost_started_sandboxes`
    after a failed wake, one mid-suspend is left parked, the suspended
    resident stays suspended and wakes on the other worker, and once nothing
    pins the node `orphaned_nodes` deletes its row, with no tombstone."""

    drain = test.drain_v2

    with drain.v2_controller(legacy_loops=True):
        with test.phase("pin a suspended and two started residents"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            kept = drain.create_resident()
            started = [drain.create_resident(), drain.create_resident()]
            for sandbox in (kept, *started):
                drain.fill_memory(sandbox, mib=64)
            kept.suspend()
            cast(DurableLifecycleSandbox, test.sandbox).suspend()
            drain.release_other_worker()

        with test.phase("uploads blocked: drain until the node reports its suspends waiting"):
            drain.block("object-store")
            epoch = drain.drain()
            drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and set(drain.pending_actions(report))
                >= {drain.bare_id(sandbox.sandbox_id) for sandbox in started},
                "the node never reported both drain suspends pending",
                timeout=300,
            )

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
                    wakes.append({drain.bare_id(s.sandbox_id): wake_fails(s) for s in started})
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
                at_let_go = lifecycles()
                test.record("at_let_go", {"trace": trace, "lifecycles": at_let_go})

        with test.phase("the gone-node loops take the rows as a crashed node's"):
            published = [
                sandbox
                for sandbox in started
                if at_let_go[drain.bare_id(sandbox.sandbox_id)][0] in ("started", "paused")
            ]
            for sandbox in started:
                if sandbox not in published:
                    test.record(f"parked_{drain.bare_id(sandbox.sandbox_id)}", at_let_go)
            deadline = time.monotonic() + 2 * LOOP_SECONDS
            while published:
                for sandbox in published:
                    wake_fails(sandbox)
                seen = lifecycles()
                if all(seen[drain.bare_id(s.sandbox_id)][0] == "failed" for s in published):
                    break
                test.verify(
                    time.monotonic() < deadline,
                    "lost_started_sandboxes never wrote off a published row after the let-go",
                    lifecycles=seen,
                )
                test.wait(30)
            for sandbox in started:
                lifecycle, _ = drain.residency(sandbox.sandbox_id)
                test.verify(
                    lifecycle not in ("started", "suspended", "restoring")
                    or sandbox not in published,
                    "a published row was not written off after the let-go",
                    sandbox_id=sandbox.sandbox_id,
                    lifecycle=lifecycle,
                )
            kept_lifecycle, _ = drain.residency(kept.sandbox_id)
            test.verify(
                kept_lifecycle == "suspended",
                "the durably suspended resident did not stay suspended",
                lifecycles=lifecycles(),
            )

        with test.phase("the suspended resident wakes elsewhere; the node row goes"):
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

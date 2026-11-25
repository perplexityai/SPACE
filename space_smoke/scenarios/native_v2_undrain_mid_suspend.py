"""An undrain while a v2 drain's suspends run starts no further suspend and hands nothing back."""

from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox

# More started residents than `SPACE_DRAIN_NODE_WINDOW`'s default of 8, so
# some wait for a slot in the drain's window.
STARTED = 12
FILL_MIB = 256


# `native.v2_undrain_unseals_the_worker` undrains a sealed worker whose
# resident the drain has not touched. Here the undrain lands while the
# drain's suspends run and the rest still wait their turn behind its window:
# the node's report says only that each is moving, so the run tells them
# apart by their rows on the worker, `suspending` or still `started`. The
# cancel unseals the node and starts no further suspend; a suspend already
# past its fence may finish after it. There is no hand-back, so a resident
# whose suspend ran stays suspended until a user call wakes it, on any
# worker, the undrained one included. The planner's cancel and the loop's
# are crate tests (`service/drain_node/tests.rs`, `node_drain/v2/tests.rs`).
@smoke.scenario(
    "native.v2_undrain_mid_suspend",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=40,
    tier="manual",
    manual_reason="drain v2 is off by default outside Kind, so its scenarios run by name, not in the per-merge set",
)
def v2_undrain_mid_suspend(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain`, due now, of a worker holding twelve started residents,
    each VM holding memory, is `:undrain`ed while the node reports them
    moving, some of their suspends running and the rest still waiting. The
    drain is cancelled on the node. No resident still started on the worker
    once the cancel cleared was suspended after it: each still runs there,
    with no snapshot. Each resident whose suspend ran stays suspended,
    handed back to nothing, until a wake starts it. Every resident's memory
    is intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin twelve started residents to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents({"started": STARTED}, mib=FILL_MIB)
            bare = {drain.bare_id(sandbox_id): sandbox_id for sandbox_id in sandboxes}

        def mid_suspend(report: dict[str, Any]) -> bool:
            if not drain.reported(report) or set(drain.pending_waits(report)) < set(bare):
                return False
            states = set(drain.node_states(list(sandboxes)).values())
            return "suspending" in states and "started" in states

        with test.phase("drain, and undrain while suspends run and others wait"):
            drain.drain()
            report = drain.wait_for_report(
                mid_suspend,
                "the drain never had suspends running while other residents waited",
                timeout=180,
            )
            test.record("report_at_undrain", report)
            test.record("node_states_at_undrain", drain.node_states(list(sandboxes)))
            drain.undrain_and_expect_cancelled()
            after_cancel = drain.node_states(list(sandboxes))
            untouched = sorted(sandbox_id for sandbox_id, state in after_cancel.items() if state == "started")
            test.record("node_states_after_cancel", after_cancel)
            test.record("started_after_cancel", untouched)

        with test.phase("no resident started after the cancel was suspended; the rest stay suspended"):
            states = drain.wait_for_in_flight_suspends(list(sandboxes))
            # A suspend the node made reaches the row once its snapshot is
            # durable; `started` is a resting lifecycle, so wait for that
            # first rather than read a row that hasn't caught up.
            for sandbox_id, state in states.items():
                if state == "suspended":
                    drain.expect_lifecycle_back(bare[sandbox_id], "suspended", timeout=300)
            outcome = drain.settled_lifecycles(lifecycles, timeout=300)
            test.record("lifecycles_after_undrain", outcome)
            snapshots = {sandbox_id: drain.ready_snapshots(bare[sandbox_id]) for sandbox_id in untouched}
            test.verify(
                bool(untouched)
                and all(
                    outcome[sandbox_id] == ("started", drain.node_id) and not snapshots[sandbox_id]
                    for sandbox_id in untouched
                ),
                "a resident still started when the cancel cleared was suspended after it",
                untouched=untouched,
                observed={sandbox_id: outcome[sandbox_id] for sandbox_id in untouched},
            )
            suspended = [sandbox_id for sandbox_id, (lifecycle, _) in outcome.items() if lifecycle == "suspended"]
            test.record("suspended_at_undrain", suspended)
            test.verify(
                bool(suspended)
                and all(
                    lifecycle in ("started", "suspended") for lifecycle, _ in outcome.values()
                ),
                "the undrain handed a suspended resident back, or left one out of both lifecycles",
                observed=outcome,
            )

        with test.phase("a wake starts each suspended one; every resident's memory is intact"):
            for sandbox_id in suspended:
                cast(ResidentLifecycleSandbox, sandboxes[bare[sandbox_id]]).wake()
                drain.expect_lifecycle_back(bare[sandbox_id], "started", timeout=300)
            for sandbox_id, sandbox in sandboxes.items():
                drain.expect_memory(sandbox, digests[sandbox_id])
            test.sandbox.wake()

        with test.phase("delete the residents"):
            for sandbox in sandboxes.values():
                test.delete_sandbox(sandbox)

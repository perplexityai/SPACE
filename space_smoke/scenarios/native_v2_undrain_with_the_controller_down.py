"""An undrain while the controller is down is cancelled at once when it is back, and leaves the residents suspended."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox

# How soon after the controller is back its cancel must land: a few loop
# ticks, with nothing to hand back first.
CANCEL_SECONDS = 120


# `native.v2_undrain_mid_suspend` undrains while the suspends run. Here the
# controller is down while the node, on its own driver, makes both residents
# durable, and the worker is undrained before the controller has seen any of
# it. The report is rebuilt from the node's rows and there is nothing to
# hand back, so the controller that comes back cancels the drain at once;
# the residents stay suspended until a wake starts each.
@smoke.scenario(
    "native.v2_undrain_with_the_controller_down",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
    tier="manual",
    manual_reason="scales the cluster-controller to zero, too invasive for the per-merge set",
)
def v2_undrain_with_the_controller_down(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain`, due now, seals a worker holding a started and a paused
    resident; with the controller down the node makes both durable, and the
    worker is `:undrain`ed. Once the controller is back it cancels the drain
    within a few ticks and uncordons the worker; both residents stay
    suspended, and a wake starts each with its memory intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started and a paused resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents({"started": 1, "paused": 1})

        with test.phase("drain; with the controller down, the node makes both durable; undrain"):
            # Uploads are held until the controller is down, so it can't
            # see either resident durable first.
            uploads = drain.block("object-store")
            drain.drain()
            drain.wait_for_reported()
            with drain.controller_down():
                uploads.release()
                for sandbox_id, lifecycle in lifecycles.items():
                    drain.expect_durable(sandbox_id, lifecycle, timeout=600)
                drain.undrain()

        with test.phase("the controller back: the drain is cancelled at once"):
            drain.expect_cancelled(timeout=CANCEL_SECONDS)
            outcome = drain.settled_lifecycles(lifecycles, timeout=300)
            test.record("lifecycles_after_undrain", outcome)
            test.verify(
                all(lifecycle == "suspended" for lifecycle, _ in outcome.values()),
                "an undrain handed a resident back instead of leaving it suspended",
                observed=outcome,
            )

        with test.phase("a wake starts each, its memory intact"):
            for sandbox_id, sandbox in sandboxes.items():
                cast(ResidentLifecycleSandbox, sandbox).wake()
                drain.expect_lifecycle_back(sandbox_id, "started", timeout=600)
                drain.expect_memory(sandbox, digests[sandbox_id])
            test.sandbox.wake()

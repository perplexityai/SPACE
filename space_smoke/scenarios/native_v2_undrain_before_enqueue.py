"""An undrain after a v2 drain made its residents durable, before any restore was enqueued, records each."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox


# `native.v2_undrain_mid_suspend` undrains while the suspends run. Here
# uploads are held until the controller is down, and the node then makes
# both residents durable with nothing recording them, so no restore exists
# when the undrain lands; the controller that comes back finds the undrain,
# and its cancel takes each resident back from the node and records it, the
# started one restored and the paused one left suspended, before it
# finishes the cancel.
@smoke.scenario(
    "native.v2_undrain_before_enqueue",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
    tier="manual",
    manual_reason="scales the cluster-controller to zero, too invasive for the per-merge set",
)
def v2_undrain_before_enqueue(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` seals a worker holding a started and a paused resident;
    with the controller down the node makes both durable, and the worker is
    `:undrain`ed before any drain restore exists. Once the controller is
    back it cancels the epoch only after recording both: the started one is
    restored once, started with exactly one VM, its memory intact; the
    paused one is left suspended, with no drain restore, and a wake starts
    it."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started and a paused resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents({"started": 1, "paused": 1})

        with test.phase("drain; with the controller down, the node makes both durable"):
            # Uploads are held until the controller is down, so it can't
            # record either resident first.
            uploads = drain.block("object-store")
            epoch = drain.drain()
            drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch,
                "the node never reported sealed under the drain's epoch",
                timeout=180,
            )
            with drain.controller_down():
                uploads.release()
                for sandbox_id, lifecycle in lifecycles.items():
                    drain.expect_durable(sandbox_id, lifecycle, timeout=600)
                drain.undrain()
                waiting = {
                    drain.bare_id(sandbox_id): drain.drain_restores(sandbox_id)
                    for sandbox_id in lifecycles
                }
                test.record("restores_at_undrain", waiting)
                test.verify(
                    not any(waiting.values()),
                    "a drain restore existed before the undrain",
                    restores=waiting,
                )

        with test.phase("the controller back: each resident recorded, then the cancel"):
            drain.expect_cancelled()
            after = drain.after_drain(lifecycles)
            outcome = drain.settled_lifecycles(lifecycles, timeout=600)
            test.record("lifecycles_after_undrain", outcome)
            test.verify(
                all(
                    outcome[drain.bare_id(sandbox_id)][0] == lifecycle
                    for sandbox_id, lifecycle in after.items()
                ),
                "an undrain before the enqueue left a resident somewhere the drain doesn't",
                observed=outcome,
            )
            for sandbox_id, lifecycle in lifecycles.items():
                if lifecycle == "paused":
                    drain.expect_suspended_unrestored(sandbox_id)
                else:
                    drain.expect_single_vm(sandbox_id, lifecycle)
                    drain.expect_landed_once(sandbox_id)
            drain.expect_started_memory(sandboxes, lifecycles, digests)

        with test.phase("a wake starts the paused one"):
            for sandbox_id, sandbox in sandboxes.items():
                if lifecycles[sandbox_id] == "paused":
                    cast(ResidentLifecycleSandbox, sandbox).wake()
                    drain.expect_lifecycle_back(sandbox_id, "started", timeout=600)
                    drain.expect_memory(sandbox, digests[sandbox_id])
            test.sandbox.wake()

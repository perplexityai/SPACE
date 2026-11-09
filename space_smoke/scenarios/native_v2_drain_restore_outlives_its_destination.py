"""A drain restore whose destination fails mid-restore is reverted and lands on a later attempt."""

import time

import space_smoke as smoke

HOLD_SECONDS = 20


# A failed attempt reverting the row and backing off is a crate test
# (`drain_restore_operations/tests`). Here the claimed restores of a v2
# drain hold, and while they hold the destination worker loses its path to
# the object store, so each attempt's download fails on the node: the row
# goes back to `suspended`, the operation waits for its retry, and once the
# path is back the retry lands. The bench has two sandbox workers, so the
# retry lands on the same destination once it recovers.
@smoke.scenario(
    "native.v2_drain_restore_outlives_its_destination",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # A timed-out attempt is not reverted while its restore may still be
    # live: the sweeper reverts it once its lease and the restoring age
    # have passed.
    timeout_minutes=75,
    test_hooks="SPACE_DRAIN_RESTORE_TEST_HOLD_SECS, which holds each claimed drain restore",
    ignored="cuts the destination's object store path under a drain, too invasive for the per-merge set",
)
def v2_drain_restore_outlives_its_destination(test: smoke.CommandSandboxFixture) -> None:
    """A v2 drain of a started and a paused resident enqueues a restore for
    each; while the claimed restores hold, the destination's path to the
    object store is cut. Each attempt fails, its sandbox is `suspended`
    again with its operation queued for a retry; once the path is back each
    retry lands it in its lifecycle on the destination, with one VM, landed
    by exactly one drain restore."""

    drain = test.drain_v2

    with drain.v2_controller(hold_seconds=HOLD_SECONDS):
        with test.phase("pin a started and a paused resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents({"started": 1, "paused": 1})

        with test.phase("drain; cut the destination's object store while the restores hold"):
            drain.drain()
            ids = list(lifecycles)
            drain.wait_for_restores(
                ids,
                lambda restores: any(state == "running" for state, _ in restores),
                "no drain restore was claimed",
                timeout=300,
            )
            destination = drain.block("object-store", drain.other_node_name)

        with test.phase("each attempt fails: suspended again, its restore queued for a retry"):
            for sandbox_id in ids:
                drain.wait_for_restores(
                    [sandbox_id],
                    lambda restores: restores == [("queued", 1)],
                    "a failed drain restore attempt was not queued for its retry",
                    timeout=900,
                )
                failed = time.monotonic()
                drain.expect_lifecycle_unplaced(sandbox_id, "suspended", timeout=1800)
                test.record(
                    f"seconds_to_suspended_{drain.bare_id(sandbox_id)}",
                    round(time.monotonic() - failed),
                )

        with test.phase("the path back: each retry lands"):
            destination.release()
            drain.expect_drained()
            restores = drain.expect_all_back(lifecycles)
            test.verify(
                all(any(attempts >= 2 for state, attempts in each if state == "succeeded") for each in restores.values()),
                "a drain restore landed without a retry",
                restores=restores,
            )
            drain.expect_started_memory(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()

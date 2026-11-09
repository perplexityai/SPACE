"""A SPACE resume of a suspended, drain-evacuated sandbox waits for its drain restore."""

import time
from collections.abc import Callable
from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import OperationLifecycleSandbox, ResidentLifecycleSandbox

HOLD_SECONDS = 90
# More paused residents than the controller's four drain restore slots, so
# some restores wait unclaimed while the first ones are held.
PAUSED = 6
_TERMINAL = {"succeeded", "failed", "canceled", "expired"}


# Which calls supersede a waiting drain restore is the supersession tests'
# (`drain_restore_supersession/tests.rs`, `drain_restore_operations/tests/
# supersession.rs`). This scenario is the seam on a real drain: with each
# claimed restore held, a `resume` meets a paused resident's restore still
# waiting for a slot.
@smoke.scenario(
    "native.v2_drain_resume_waits_for_its_restore",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    test_hooks="SPACE_DRAIN_RESTORE_TEST_HOLD_SECS, which holds each claimed drain restore",
    ignored="holds every drain restore to meet one waiting, a bench run",
)
def v2_drain_resume_waits_for_its_restore(test: smoke.CommandSandboxFixture) -> None:
    """A SPACE `resume` of a paused resident a v2 drain suspended, whose
    drain restore still waits unclaimed, leaves that restore queued: the
    restore brings the sandbox back paused on the other worker, and the
    resume, queued behind it, then resumes it there."""

    drain = test.drain_v2

    def wait_until(accept: Callable[[], Any], failure: str, *, timeout: float) -> Any:
        deadline = time.monotonic() + timeout
        while True:
            value = accept()
            if value:
                return value
            test.verify(time.monotonic() < deadline, failure)
            test.wait(2)

    with drain.v2_controller(hold_seconds=HOLD_SECONDS):
        with test.phase("pin paused residents to one worker"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            paused = [drain.create_resident() for _ in range(PAUSED)]
            for sandbox in paused:
                cast(ResidentLifecycleSandbox, sandbox).pause()
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain; a resume meets a restore still waiting for a slot"):
            drain.drain()
            ids = {drain.bare_id(sandbox.sandbox_id): sandbox for sandbox in paused}

            def unclaimed() -> str | None:
                placements = drain.placements(list(ids))
                for sandbox_id in ids:
                    restores = drain.drain_restores(sandbox_id)
                    if (
                        placements[sandbox_id][0] == "suspended"
                        and restores
                        and restores[-1][0] == "queued"
                    ):
                        return sandbox_id
                return None

            target_id = wait_until(unclaimed, "no drain restore waited unclaimed", timeout=600)
            target = cast(OperationLifecycleSandbox, ids[target_id])
            resume = target.request_lifecycle("resume")
            test.wait(5)
            restores = drain.drain_restores(target_id)
            test.record("restores_after_resume", restores)
            test.verify(
                bool(restores) and restores[-1][0] in ("queued", "running", "succeeded"),
                "the resume canceled the drain restore",
                restores=restores,
            )

        with test.phase("the restore lands the sandbox paused, then the resume resumes it"):
            operation = wait_until(
                lambda: (
                    lambda op: op if op.get("state") in _TERMINAL else None
                )(target.operation(resume)),
                "the resume never ended",
                timeout=900,
            )
            test.record("resume", operation)
            test.record("restores_after", drain.drain_restores(target_id))
            test.verify(
                operation.get("state") == "succeeded" and drain.drain_restores(target_id)[-1][0] == "succeeded",
                "the resume did not resume the sandbox its restore brought back",
                resume=operation,
            )
            drain.expect_back_elsewhere(target.sandbox_id, "started")

        with test.phase("the node drains; undrain and delete the residents"):
            drain.expect_drained()
            drain.undrain_and_expect_cancelled()
            for sandbox in paused:
                test.delete_sandbox(sandbox)

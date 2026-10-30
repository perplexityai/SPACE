"""A worker deletes an org template the cluster stopped answering for, after the wait."""

import time

import space_smoke as smoke
from space_smoke.unanswered_template_eviction import FULL_SYNC_SECONDS

# Long enough that the two syncs after the sandbox delete and the check of
# the copy finish inside it, which `expect_held_before_wait` verifies. On
# the Kind bench they take about 4 s; the rest is headroom for slower CI
# runners.
WAIT_SECONDS = 20
MARKER = "space-smoke-unanswered-template-ok"


# The template is a minimal run-owned build no worker prefetches, so only
# this scenario's sandboxes give a worker a reason to hold it, and the
# cluster releases it once they are gone. The wait is node-ctl's one-hour
# default shortened through an env override on the worker node-ctl
# DaemonSet, scoped to the body and put back in its own phase, and each
# check follows a full sync run on demand rather than node-ctl's poll (see
# `space_smoke.unanswered_template_eviction`). The other worker is cordoned
# for the second sandbox so it lands on the worker that deleted the copy.
@smoke.scenario(
    "local.unanswered_template_deleted_after_wait",
    # The template build, two worker node-ctl rollouts and the hold past the
    # wait with the sandbox running.
    sandbox=smoke.SandboxConfig(timeout=900, kind_dedicated_template=True),
)
def unanswered_template_deleted_after_wait(test: smoke.KindEcw) -> None:
    """With node-ctl's unanswered-template wait shortened, a worker keeps an
    org template through a full sync past the wait while a sandbox of it runs
    there, keeps it through two full syncs after the sandbox is deleted but
    before the wait has passed, and deletes it (unlisted, its bytes and then
    its Node template-build metadata gone) in the first full sync after the
    wait; a sandbox of the same template placed on that worker afterwards
    pulls it again and boots."""

    unanswered = test.unanswered_templates
    restore = test.cross_node_restore
    template = test.sandbox_template_name()

    with test.phase("locate the worker running the template's sandbox"):
        worker = restore.current_placement().node_name
        other = unanswered.other_worker(worker)
        test.record("worker", worker)

    with unanswered.scoped_wait():
        with test.phase(f"shorten worker node-ctl's unanswered-template wait to {WAIT_SECONDS} s"):
            unanswered.shorten_wait(seconds=WAIT_SECONDS, workers=(worker, other))

        with test.phase("the worker keeps the template through a sync past the wait while its sandbox runs"):
            held = unanswered.expect_held(worker, template)
            unanswered.expect_held_past_wait(held)
            test.expect_command(test.run_command(f"printf %s {MARKER}"), stdout=MARKER)

        with test.phase("delete the sandbox"):
            # Taken first: no answer that leaves the template out comes sooner.
            deleted_at = time.monotonic()
            test.delete_sandbox()
            test.wait_until_deleted()

        with test.phase("the worker keeps the template through two syncs before the wait has passed"):
            recorded = unanswered.expect_held_before_wait(held, since=deleted_at)

        with test.phase("the first sync after the wait deletes the template"):
            unanswered.expect_deleted_after_wait(held, recorded=recorded, since=deleted_at)

        # A failed body still restores as it unwinds; this phase accounts for
        # the node-ctl rollout on the passing path.
        with test.phase("restore the standing node-ctl wait"):
            unanswered.restore_standing()

    with restore.cordoned(other):
        with test.phase("a new sandbox of the template on that worker pulls it again and boots"):
            sandbox = test.sandboxes.create()
            landed = restore.current_placement(sandbox).node_name
            test.verify(landed == worker, "the new sandbox landed off the worker", landed=landed, worker=worker)
            test.expect_command(sandbox.run(f"printf %s {MARKER}"), stdout=MARKER)
            unanswered.expect_held_within(worker, template, timeout=FULL_SYNC_SECONDS)

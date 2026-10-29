"""A worker deletes an org template the cluster stopped answering for, after the wait."""

import time

import space_smoke as smoke
from space_smoke.unanswered_template_eviction import FULL_SYNC_SECONDS

# Short enough to watch, and a whole sync under two syncs, so a delete lands
# on the second full sync after the first omitted answer rather than on a
# sync whose clock reads just under the wait.
WAIT_SECONDS = 90
MARKER = "space-smoke-unanswered-template-ok"


# The template is a minimal run-owned build no worker prefetches, so only
# this scenario's sandboxes give a worker a reason to hold it, and the
# cluster stops answering for it once they are gone. The wait is node-ctl's
# one-hour default shortened by a test hook on the worker node-ctl
# DaemonSet, scoped to the body and put back in its own phase (see
# `space_smoke.unanswered_template_eviction`). The other worker is cordoned
# for the second sandbox so it lands on the worker that deleted the copy.
@smoke.scenario(
    "local.unanswered_template_deleted_after_wait",
    # The template build, two worker node-ctl rollouts, the hold past the wait
    # with the sandbox running, then the wait and up to two syncs after it.
    sandbox=smoke.SandboxConfig(timeout=900, kind_dedicated_template=True),
    test_hooks="SPACE_NODECTL_UNANSWERED_TEMPLATE_EVICT_AFTER_SECS; release waits an hour",
)
def unanswered_template_deleted_after_wait(test: smoke.KindEcw) -> None:
    """With node-ctl's unanswered-template wait shortened, a worker keeps an
    org template while a sandbox of it runs there, keeps it right after the
    sandbox is deleted, and deletes it (unlisted, its bytes and its Node
    template-build metadata gone) once the wait has passed; a sandbox of the
    same template placed on that worker afterwards pulls it again and boots."""

    unanswered = test.unanswered_templates
    restore = test.cross_node_restore
    template = test.sandbox_template_name()

    with test.phase("locate the worker running the template's sandbox"):
        worker = restore.current_placement().node_name
        other = unanswered.other_worker(worker)
        test.record("worker", worker)

    with unanswered.scoped_wait():
        with test.phase(f"shorten worker node-ctl's unanswered-template wait to {WAIT_SECONDS} s"):
            rolled_at = unanswered.shorten_wait(seconds=WAIT_SECONDS, workers=(worker, other))

        with test.phase("the worker keeps the template past the wait while its sandbox runs"):
            held = unanswered.expect_held(worker, template)
            # Past the first full sync a delete could land on, and one more.
            unanswered.expect_held_until(held, rolled_at + WAIT_SECONDS + FULL_SYNC_SECONDS + 30)
            test.expect_command(test.run_command(f"printf %s {MARKER}"), stdout=MARKER)

        with test.phase("delete the sandbox; the worker still holds the template"):
            # Taken first, so the earliest-delete bound only loosens.
            deleted_at = time.monotonic()
            test.delete_sandbox()
            test.wait_until_deleted()
            unanswered.expect_held(worker, template)

        with test.phase("the worker deletes the template once the wait has passed"):
            unanswered.expect_deleted_after_wait(held, since=deleted_at)

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
            unanswered.expect_held(worker, template)

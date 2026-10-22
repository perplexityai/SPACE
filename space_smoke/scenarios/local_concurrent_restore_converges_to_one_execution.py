"""Concurrent restore callers across API replicas converge to one execution."""

import shlex
import time

import space_smoke as smoke
from space_smoke.capabilities import RestoreCallerOutcome
from space_smoke.runtime import background

PATH = "/tmp/space-smoke-concurrent-restore.txt"
MARKER = "space-smoke-concurrent-restore-ok"
CALLERS = 20


# The compose hammer proved claim-before-create in the restore path under real
# multi-process contention, against a stub apiserver whose knobs opened the
# windows: a pod-POST delay, a forced first-POST failure, and a POST counter.
# This is its real-cluster form. Three API replicas serve twenty gated
# `connect` callers; the counters are the API's own `sandbox_k8s_executions`
# rows and the sandbox's pods by label; the windows are a namespace pod-count
# quota (pod POSTs are refused at admission until it is lifted — the stub
# failed exactly one, so the verdict counts refusals instead of assuming one)
# and, for the teardown fence, the same quota keeping the callers' claims
# refused — waiting callers re-claim as each refused claim is retired — so the
# delete aims at that window, landing on a `suspended` sandbox if every caller
# was refused first. (Cordoning every worker is not a stand-in for the stub's
# POST delay: the API's placement admission then answers 429 before any POST,
# so no restore is in flight at all.) The one thing the stub could do that a
# real cluster cannot is hold the POST itself open, so the hammer's "teardown
# waited on the restore's own lock" timing assertion has no equivalent here;
# the fence's outcome does. In that phase 404/409 dominate the callers'
# outcomes — the delete's fence turns every racing connect into a conflict or
# a not-found — and callers whose claims the cap retired before the fence see
# the refused-claim 500; every 500 must have finished before the delete
# returned.
@smoke.scenario(
    "local.concurrent_restore_converges_to_one_execution",
    # Four restore windows, an API scale-up rollout, and a pod deletion with
    # its grace period; each restore itself is seconds on kind.
    sandbox=smoke.SandboxConfig(timeout=1800),
)
def concurrent_restore_converges_to_one_execution(test: smoke.KindEcw) -> None:
    """Twenty concurrent connects to a suspended sandbox, spread over three API
    replicas, produce exactly one execution and one pod with every caller
    landing on it; a started sandbox whose pod is deleted recovers under the
    same contention onto one new generation restored from its suspend
    checkpoint; pod POSTs refused at admission retire their generations and
    the callers converge on the one that was admitted, the refused callers
    seeing the failure; and a delete issued while the pod cap is refusing the
    callers' claims leaves no pod that ever runs, no live sandbox, and no hung or crashed
    caller behind."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    race = test.concurrent_restore
    restore = test.cross_node_restore  # portability wait only

    with test.phase("write state and run the public API at three replicas"):
        written = test.run_command_when_ready(f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}")
        test.verify(written.exit_code == 0, "failed to write state", command_stderr=written.stderr)
        race.scale_api_for_contention(3)

    with test.phase("happy: suspend, then twenty concurrent connects"):
        test.pause()
        restore.wait_until_restore_portable()
        before = len(race.executions())
        outcomes = race.race_connects(CALLERS)
        race.expect_converged(outcomes, executions_total=before + 1, label="happy")
        # Every caller landed, so the slowest waited out the whole restore:
        # the claim, the pod POST, the pod reaching Running and the VM serving.
        restore_seconds = max(o.elapsed_seconds for o in outcomes)
        test.reconnect_by_id()
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

    with test.phase("started_local_handoff: the running pod vanishes; connects recover one new generation"):
        before_rows = race.executions()
        deleted = race.delete_current_pod()
        # The API learns of the loss only through a connect's failed keepalive
        # touch, and the VM outlives the pod delete by its grace period; wait
        # until the runtime is really gone so all twenty callers hit the
        # recovery path together, as the hammer's SQL-seeded lost pod did.
        race.wait_until_runtime_gone()
        # The probe above must not itself have opened a recovery generation,
        # or "the callers opened one" and "the callers joined one" would read
        # the same.
        test.verify(
            len(race.executions()) == len(before_rows),
            "a recovery generation was already open before the callers fired",
            executions=[(r.generation, r.state, r.active) for r in race.executions()],
        )
        outcomes = race.race_connects(CALLERS)
        active = race.expect_converged(outcomes, executions_total=len(before_rows) + 1, label="handoff")
        pods = [p for p in race.pods() if not p.deleting]
        test.verify(
            len(pods) == 1
            and pods[0].pod_name != deleted.pod_name
            and pods[0].execution_generation == str(active.generation)
            and pods[0].checkpoint == "suspend"
            and pods[0].restore_source_node != "",
            "the recovered pod is not a fresh generation restored from the suspend checkpoint",
            pods=[(p.pod_name, p.execution_generation, p.checkpoint, p.restore_source_node) for p in pods],
            deleted_pod=deleted.pod_name,
        )
        test.reconnect_by_id()

    with test.phase("failed_post: pod POSTs refused at admission retire their generations; one wins"):
        test.pause()
        restore.wait_until_restore_portable()
        before = len(race.executions())
        race.cap_namespace_pods()
        # The cap is lifted once the first refusal is visible (a retired,
        # inactive row). Unlike the stub, admission keeps refusing until then,
        # so several callers may see it; the verdict counts them against the
        # retired generations instead of assuming exactly one.
        def lift_after_first_refusal() -> None:
            race.wait_for_abandoned_execution(after=before, timeout=90)
            race.lift_pod_cap()

        lifter = background("the cap lifter", lift_after_first_refusal)
        outcomes = race.race_connects(CALLERS)
        lifter.join(timeout=120)
        race.expect_converged_through_refusals(outcomes, before=before, label="failed_post")
        test.reconnect_by_id()

    with test.phase("teardown: delete while the pod cap refuses the restore; nothing survives"):
        test.pause()
        restore.wait_until_restore_portable()
        # The previous pod is still Terminating after the pause; the quota
        # controller charges it until it is gone, so a cap pinned now would be
        # off by one once it leaves and one create would slip through.
        # `cap_namespace_pods` waits for the drain and fails if it does not
        # complete.
        before = len(race.executions())
        # The quota refuses every pod POST, so no restore pod ever exists.
        # Waiting callers re-claim as each refused claim is retired, until
        # every caller has had one: a refused owner returns 500 and its
        # joiners re-race on a capped backoff. The delete follows the first
        # claim, aiming inside that window rather than the restore's own few
        # seconds; if every caller was refused first, it lands on a
        # `suspended` sandbox, which the verdict also accepts.
        # Cordoning every worker is not that window — the API's placement
        # admission answers 429 before any POST (rev6 on the bench).
        race.cap_namespace_pods()
        outcomes: list[RestoreCallerOutcome] = []
        firing = background(
            "the twenty concurrent callers",
            lambda: outcomes.extend(race.race_connects(CALLERS, timeout=180)),
        )
        race.wait_for_restore_in_flight(before=before)
        # Snapshot the generations the cap has retired right before the
        # delete — a lower bound on the refused-claim 500s, since more can be
        # retired between here and the fence and the cascade erases the rows.
        refused = len([r for r in race.executions()[before:] if r.state == "replaced"])
        test.sandbox.kill()
        delete_returned_at = time.monotonic()
        # The "no pod ever runs" window opens at the lift, so observe from
        # before it: a pod admitted right after the lift could reach Running
        # and be reaped before the callers drain, and a poll started after
        # the join would miss it.
        pod_watch = race.watch_for_running_pods()
        race.lift_pod_cap()
        # The join is the no-hang contract: it fails if a caller is still
        # running at the deadline and re-raises if the callers died.
        firing.join(timeout=200)
        joined = time.monotonic()
        # Nothing re-drives a refused pod POST once its caller is gone (the
        # restore abandons the claim), so a pod that could still run was
        # admitted before the callers drained and is listed by now. Keep
        # watching until every such pod is gone, however long one sat Pending
        # first, and for at least one restore as measured on this runner above
        # plus the watch's polls. A pod still listed when the drain gives up
        # fails here: the watch cannot close while one remains.
        remaining = race.wait_for_pods_gone()
        window = restore_seconds + 2 * pod_watch.interval_seconds
        watched = round(max(window, time.monotonic() - joined + pod_watch.interval_seconds), 1)
        test.record("teardown_no_running_window_seconds", watched)
        running_after_lift = pod_watch.stop_after(
            seconds=max(pod_watch.interval_seconds, joined + window - time.monotonic())
        )
        test.verify(
            not remaining,
            "teardown: an admitted pod never drained, so whether it ran is unknown",
            pods=[(p.pod_name, p.phase, p.deleting) for p in remaining],
            pods_reached_running_after_lift=[
                (p.pod_name, p.execution_generation) for p in running_after_lift
            ],
        )
        race.expect_torn_down(
            outcomes,
            label="teardown",
            running_after_lift=running_after_lift,
            refused_before_delete=refused,
            delete_returned_at=delete_returned_at,
        )

    with test.phase("scale the public API back"):
        race.restore_api_scale()

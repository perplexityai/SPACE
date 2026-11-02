"""The orphaned-restore-pod GC reaps superseded restore pods and nothing a live restore needs."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-orphaned-restore-pod-gc.txt"
MARKER = "space-smoke-orphaned-restore-pod-gc-ok"
# The threshold is how long a pod must classify as superseded on every tick
# before it is reaped, and so also the margin for the reaper's pod cache and
# its Postgres read disagreeing. A live restore pod does not classify as
# superseded at all: its execution claim is committed at the pod's generation
# before the pod is created, and the reaper re-reads the pod before deleting.
# Ten seconds is five ticks; the reap budget and the survival window are
# derived from it.
THRESHOLD_SECONDS = 10
INTERVAL_SECONDS = 2


# The reaper ships default-off and dry-run and reads its config at startup,
# so the body runs inside a scoped live config (short threshold, 2 s ticks)
# whose standing env is put back in the body's last phase. The orphans are
# unschedulable clones of this sandbox's own restore pod at retired
# generations (see `space_smoke.orphaned_restore_pod_gc`): Pending, never a
# VM, exactly what a stranded pre-bind restore pod looks like to the reaper.
@smoke.scenario(
    "local.orphaned_restore_pod_gc_reaps_only_superseded",
    # Two controller rollouts, three suspend/restore cycles, three reaps of
    # about threshold plus two ticks each, and two survival windows.
    sandbox=smoke.SandboxConfig(timeout=1800),
)
def orphaned_restore_pod_gc_reaps_only_superseded(test: smoke.KindEcw) -> None:
    """With the GC live, a restore pod below the started row's active
    generation and one whose sandbox row does not exist are each deleted,
    no sooner than the threshold; a restore done while the GC is live leaves
    one execution whose pod keeps its uid well past the threshold plus
    several ticks; and a suspended sandbox with an orphan on its own row at
    its retired generation, which suspend retired as `suspended` rather than
    `replaced`, keeps that orphan through the survival window (the backup
    fence would admit its teardown capture) while a row-less control planted
    beside it is reaped, then restores with its state and the orphan is gone
    once the restore supersedes it."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    gc = test.orphaned_restore_pod_gc

    with test.phase("write state; suspend and restore once for a real restore pod"):
        written = test.run_command_when_ready(f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}")
        test.verify(written.exit_code == 0, "failed to write state", command_stderr=written.stderr)
        test.pause()
        gc.wait_until_suspended()
        test.reconnect_by_id()
        test.expect_command(test.run_command_when_ready(f"cat {shlex.quote(PATH)}"), stdout=marker)
        restored = gc.capture_restore_pod()

    with gc.scoped_live_gc():
        # 2 s ticks keep the reaps and the survival window short.
        with test.phase(
            f"run the GC live with a {THRESHOLD_SECONDS} s threshold and {INTERVAL_SECONDS} s ticks"
        ):
            gc.go_live(threshold_seconds=THRESHOLD_SECONDS, interval_seconds=INTERVAL_SECONDS)

        with test.phase("superseded: an older generation on the started row and a row-less pod are reaped"):
            superseded = gc.plant(sandbox_id=test.sandbox.sandbox_id, generation=restored.generation - 1)
            rowless = gc.plant(generation=1)
            # Watched together, so each plant's age is judged from its own create.
            gc.expect_reaped(superseded, rowless)
            # The started row's own restore pod sat beside them the whole time.
            current = gc.expect_one_live_execution(above=superseded.generation)
            test.verify(
                current.uid == restored.uid,
                "the started sandbox's restore pod was replaced while its superseded sibling was reaped",
                before=restored.name,
                after=current.name,
            )
            test.expect_command(test.run_command(f"cat {shlex.quote(PATH)}"), stdout=marker)

        with test.phase("restored: a pod restored under the live GC is left alone"):
            test.pause()
            rows = gc.wait_until_suspended()
            # Restored while the GC ticks. Its current-generation pod on a
            # started row must outlast the threshold plus several ticks. The skip for an in-flight `restoring`
            # row is unit-tested in the reconciler instead: on kind a restore
            # is `restoring` for about a second, far shorter than the
            # threshold a misclassified pod would need to age into a delete.
            test.reconnect_by_id()
            test.expect_command(test.run_command_when_ready(f"cat {shlex.quote(PATH)}"), stdout=marker)
            live = gc.expect_one_live_execution(above=max(r.generation for r in rows))
            gc.expect_undisturbed(live)

        with test.phase(
            "suspended: an orphan at the row's unreplaced retired generation is left alone until a restore"
        ):
            test.pause()
            rows = gc.wait_until_suspended()
            retired = max(r.generation for r in rows)
            # The phase rests on the suspend retiring its execution as
            # something other than `replaced`: that's what the fence admits.
            retired_states = [r.state for r in rows if r.generation == retired]
            test.verify(
                "replaced" not in retired_states,
                "the suspend retired its execution as `replaced`, so the GC would rightly reap the plant",
                generation=retired,
                states=retired_states,
            )
            own = gc.plant(sandbox_id=test.sandbox.sandbox_id, generation=retired)
            # A row-less control planted beside it shows the GC is acting
            # while `own` ages: the control is reaped, then `own` must still
            # be there through a further survival window.
            control = gc.plant(generation=1)
            gc.expect_reaped(control)
            # A suspend retires its execution as `suspended`, not `replaced`,
            # and nothing newer has run, so the fence would admit the teardown
            # capture a reap of a bound pod starts: the GC must leave it alone.
            gc.expect_left_alone(own)
            test.reconnect_by_id()
            test.expect_command(test.run_command_when_ready(f"cat {shlex.quote(PATH)}"), stdout=marker)
            # The restore's execution supersedes the plant. Its lingering-pod
            # sweep or the GC removes it; either is correct.
            gc.expect_gone(own)
            gc.expect_one_live_execution(above=retired)

        # A failed body still restores as it unwinds; this phase accounts for
        # the controller rollout on the passing path.
        with test.phase("restore the standing GC config"):
            gc.restore_standing()

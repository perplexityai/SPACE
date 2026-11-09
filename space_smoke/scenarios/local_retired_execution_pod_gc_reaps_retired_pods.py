"""The retired-execution-pod GC reaps the pods a retired execution leaves, and nothing live."""

import space_smoke as smoke

# The threshold is how long a pod must classify reapable on every tick before
# it is deleted. Twenty seconds is four ticks; the survival windows and the
# reap budget are derived from it.
THRESHOLD_SECONDS = 20
INTERVAL_SECONDS = 5
# With the GC off, the retired pods outlast the longest window the GC phases
# wait, so their removal below is the GC's doing.
GC_OFF_WINDOW_SECONDS = 60
# A cold create stranded in its Pending pod ends when the client's 90 s
# request timeout or the API's own create deadline fires.
CREATE_END_SECONDS = 240


# See `space_smoke.retired_execution_pod_gc` for how each pod is produced and
# which state is judged.
@smoke.scenario(
    "local.retired_execution_pod_gc_reaps_retired_pods",
    # A cold create, an idle suspend, two stranded creates (each up to three
    # attempts), an API restart, two controller rollouts there and back, and
    # the survival windows.
    sandbox=smoke.SandboxConfig(timeout=1800),
)
def retired_execution_pod_gc_reaps_retired_pods(test: smoke.KindEcw) -> None:
    """A node-side idle suspend leaves a Succeeded pod, and a cold create
    retired as `replaced` whose API was killed leaves a Pending pod with no
    IP; both outlast a window with the GC off. A stranded create whose create
    ends has its Pending pod deleted by the create's own cleanup. In dry-run
    the GC reports would-reap for exactly the reapable pods and deletes
    nothing past its threshold; live, it deletes both, none before the
    threshold, while a Pending pod with a pod IP on a retired `replaced` row
    and the live sandbox keep their pods."""

    gc = test.retired_execution_pod_gc

    with test.phase("a node-side idle suspend leaves its Succeeded pod"):
        suspended = gc.idle_suspended_pod()

    with test.phase("a stranded create whose create ends: its cleanup deletes the Pending pod"):
        cancelled, create = gc.strand_cold_create()
        create.join(timeout=CREATE_END_SECONDS)
        took = gc.expect_gone(
            cancelled, timeout=60, message="the ended create left its retired Pending pod behind"
        )
        test.record("create_cleanup_deleted_pod_after_seconds", took)

    with test.phase("a stranded create whose API is killed leaves its Pending pod"):
        stranded, create = gc.strand_cold_create()
        gc.kill_public_api()
        create.join(timeout=CREATE_END_SECONDS)

    with test.phase("plant a Pending pod with a pod IP on the deleted pod's retired row"):
        with_ip = gc.plant_pending_with_ip(cancelled, template_pod=stranded.name)

    with test.phase("GC off: the retired pods stay"):
        gc.expect_persist(suspended, stranded, with_ip, seconds=GC_OFF_WINDOW_SECONDS, record_key="gc_off_kept")

    with gc.scoped_gc():
        with test.phase("dry-run: would-reap for exactly the retired pods, and none deleted"):
            gc.configure(dry_run=True, threshold_seconds=THRESHOLD_SECONDS, interval_seconds=INTERVAL_SECONDS)
            gc.expect_dry_run_verdicts(suspended, stranded)

        with test.phase("live: the retired pods are reaped; the pod with an IP and the live sandbox stay"):
            settings = gc.configure(
                dry_run=False, threshold_seconds=THRESHOLD_SECONDS, interval_seconds=INTERVAL_SECONDS
            )
            gc.expect_reaped(suspended, stranded)
            gc.expect_persist(with_ip, seconds=settings.survival_window_seconds, record_key="with_ip_kept")
            gc.expect_live_sandbox_untouched(seconds=settings.interval_seconds * 2)
            gc.expect_live_reap_counts(suspended, stranded)

        # A failed body still restores as it unwinds; this phase accounts for
        # the controller rollout on the passing path.
        with test.phase("restore the standing GC config"):
            gc.restore_standing()
            gc.release_plants()

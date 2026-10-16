//! The restore and warm-pool exclusions keyed on **live ownership** rather than
//! on the pod markers, which are stamped at build time and never cleared.
//!
//! Read off the pod alone, both markers are permanent: a restored sandbox's pod
//! keeps its restore annotation forever, and a claimed slot's pod keeps
//! `pool-role=warm` forever. A pod that later wedges in `ContainerCreating` was
//! therefore excluded for the rest of its life, however long it stayed stuck.
//! These tests pin the pairing in both directions — the exclusion holds while
//! the owner is live, and lifts once it isn't — plus the fail-closed behavior on
//! every answer the database can't give.

use super::*;

/// A stuck pod carrying a restore annotation.
fn restore_snapshot(name: &str) -> PodSnapshot {
    let mut pod = stuck_snapshot(name);
    pod.is_restore = true;
    pod
}

/// A stuck pod carrying `pool-role=warm`.
fn warm_snapshot(name: &str) -> PodSnapshot {
    let mut pod = stuck_snapshot(name);
    pod.is_warm_pool = true;
    pod
}

// ---- pure ownership predicates -------------------------------------------

/// Build a one-pod ownership view so the predicates can be exercised over each
/// `Lookup` state without a tick.
fn ownership_for(pod: &PodSnapshot, owner: Lookup<Owner>) -> MarkerOwnership {
    MarkerOwnership {
        restores: pod.sandbox_id.map(|id| (id, owner)).into_iter().collect(),
        slots: HashMap::new(),
    }
}

fn slot_ownership_for(pod: &PodSnapshot, owner: Lookup<Owner>) -> MarkerOwnership {
    MarkerOwnership {
        restores: HashMap::new(),
        slots: [(pod.pod_name.clone(), owner)].into_iter().collect(),
    }
}

#[test]
fn restore_marker_excludes_only_while_a_restore_is_in_flight() {
    let pod = restore_snapshot("p");
    let excludes = |owner| ownership_for(&pod, owner).restore_excludes(&pod);

    // A `restoring` row, or a started recovery holding a fresh lease while
    // deliberately leaving the row in `started`.
    assert!(excludes(Lookup::Known(Owner::InFlight)));
    // The restore finished, or the sandbox was retired: the annotation is a
    // leftover.
    assert!(!excludes(Lookup::Known(Owner::Released)));
    // No row at all: nothing to strand, so nothing to protect.
    assert!(!excludes(Lookup::Absent));
    // Unreadable row / failed query / never asked: fall back to the marker.
    assert!(excludes(Lookup::Unknown));

    // An unmarked pod is never excluded on this axis, whatever the row says.
    let plain = stuck_snapshot("plain");
    assert!(!ownership_for(&plain, Lookup::Known(Owner::InFlight)).restore_excludes(&plain));
}

#[test]
fn warm_pool_marker_excludes_while_the_pool_still_owns_the_pod() {
    let pod = warm_snapshot("p");
    let excludes = |owner| slot_ownership_for(&pod, owner).warm_pool_excludes(&pod);

    assert!(excludes(Lookup::Known(Owner::InFlight)));
    assert!(!excludes(Lookup::Known(Owner::Released)));
    assert!(!excludes(Lookup::Absent));
    assert!(excludes(Lookup::Unknown));

    let plain = stuck_snapshot("plain");
    assert!(!slot_ownership_for(&plain, Lookup::Known(Owner::InFlight)).warm_pool_excludes(&plain));
}

/// Tick one pod against `store` and assert it was excluded for `reason`.
async fn assert_excluded(pod: PodSnapshot, store: FakeStore, reason: SkipReason) {
    let (mut reconciler, _clock) =
        build_with_store(test_config(60), FakeBackend::new(vec![pod]), store);

    let report = reconciler.tick().await;

    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, reason), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
    // Excluded before any apiserver work, as both markers always were.
    assert_eq!(reconciler.backend.fresh_read_count(), 0);
}

/// Tick one pod against `store` and assert the stale marker released it.
async fn assert_released(pod: PodSnapshot, store: FakeStore) {
    let pod_name = pod.pod_name.clone();
    let (mut reconciler, _clock) =
        build_with_store(test_config(60), FakeBackend::new(vec![pod]), store);

    let report = reconciler.tick().await;

    assert_eq!(report.deleted, 1);
    assert_eq!(reconciler.backend.deleted_names(), vec![pod_name]);
}

// ---- restore marker, end to end ------------------------------------------

/// The exclusion still applies while the restore is genuinely in flight.
#[tokio::test]
async fn restore_pod_is_excluded_while_its_sandbox_is_restoring() {
    let pod = restore_snapshot("p");
    let sandbox_id = pod.sandbox_id.expect("fixture sets a sandbox id");
    let store = FakeStore::default().with_restore_owner(sandbox_id, Owner::InFlight);

    assert_excluded(pod, store, SkipReason::RestorePod).await;
}

/// The case the permanent annotation used to make uncollectable forever: the
/// restore completed, the sandbox is `started`, and its pod later re-wedged.
/// Ownership has moved to the orphaned-sandbox reconciler, so the GC may act. A
/// row already retired to a dormant state reads the same way: nothing will ever
/// converge the pod away, and there is nothing left to strand.
#[tokio::test]
async fn restore_pod_is_released_once_its_sandbox_left_restoring() {
    let pod = restore_snapshot("p");
    let sandbox_id = pod.sandbox_id.expect("fixture sets a sandbox id");
    let store = FakeStore::default().with_restore_owner(sandbox_id, Owner::Released);

    assert_released(pod, store).await;
}

/// No sandbox row at all — the pod outlived its sandbox entirely.
#[tokio::test]
async fn restore_pod_with_no_sandbox_row_is_released() {
    assert_released(restore_snapshot("p"), FakeStore::default()).await;
}

/// A failed lifecycle query must not read as "no row, therefore stale marker,
/// therefore delete". It falls back to the annotation.
#[tokio::test]
async fn restore_pod_is_excluded_when_the_lifecycle_lookup_fails() {
    assert_excluded(
        restore_snapshot("p"),
        FakeStore::failing_lifecycles(),
        SkipReason::RestorePod,
    )
    .await;
}

/// Same for a row whose `lifecycle` string this build doesn't recognize: the
/// column is wider than the enum, and an unparseable value is "unknown", not
/// "absent".
#[tokio::test]
async fn restore_pod_is_excluded_when_the_lifecycle_is_unreadable() {
    let pod = restore_snapshot("p");
    let sandbox_id = pod.sandbox_id.expect("fixture sets a sandbox id");
    let store = FakeStore::default().with_unreadable_lifecycle(sandbox_id);

    assert_excluded(pod, store, SkipReason::RestorePod).await;
}

/// A pod with no sandbox id can't be resolved against any row, so its marker
/// stays authoritative — the pod is excluded as a restore before the
/// missing-sandbox-id skip is even reached.
#[tokio::test]
async fn restore_pod_without_a_sandbox_id_is_excluded() {
    let mut pod = restore_snapshot("p");
    pod.sandbox_id = None;

    assert_excluded(pod, FakeStore::default(), SkipReason::RestorePod).await;
}

// ---- warm-pool marker, end to end ----------------------------------------

#[tokio::test]
async fn warm_pool_pod_is_excluded_while_the_pool_owns_it() {
    let store = FakeStore::default().with_slot_owner("p", Owner::InFlight);
    assert_excluded(warm_snapshot("p"), store, SkipReason::WarmPool).await;
}

/// Binding a slot updates Postgres but not the pod label, so the label alone
/// excluded a bound-then-wedged pod forever. Ownership having moved is what
/// matters — including for a receipt whose bind predates the `bound` stamp,
/// which the store resolves from the slot's reserved sandbox row.
#[tokio::test]
async fn warm_pool_pod_is_released_once_a_sandbox_owns_it() {
    let store = FakeStore::default().with_slot_owner("p", Owner::Released);
    assert_released(warm_snapshot("p"), store).await;
}

/// The slot row is gone entirely — the pool has finished with this pod.
#[tokio::test]
async fn warm_pool_pod_with_no_slot_row_is_released() {
    assert_released(warm_snapshot("p"), FakeStore::default()).await;
}

#[tokio::test]
async fn warm_pool_pod_is_excluded_when_the_slot_lookup_fails() {
    assert_excluded(
        warm_snapshot("p"),
        FakeStore::failing_slots(),
        SkipReason::WarmPool,
    )
    .await;
}

#[tokio::test]
async fn warm_pool_pod_is_excluded_when_the_slot_state_is_unreadable() {
    let store = FakeStore::default().with_unreadable_slot("p");

    assert_excluded(warm_snapshot("p"), store, SkipReason::WarmPool).await;
}

/// A failing slot query must not also re-freeze the restore exclusion: the two
/// lookups fail independently, so a broken pool query still lets a stale restore
/// marker be resolved.
#[tokio::test]
async fn a_failing_slot_lookup_does_not_freeze_the_restore_exclusion() {
    let restore = restore_snapshot("restore");
    let sandbox_id = restore.sandbox_id.expect("fixture sets a sandbox id");
    let mut store = FakeStore::failing_slots();
    store.restores.insert(sandbox_id, Owner::Released);
    let backend = FakeBackend::new(vec![restore, warm_snapshot("warm")]);
    let (mut reconciler, _clock) = build_with_store(test_config(60), backend, store);

    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 1);
    assert_eq!(
        reconciler.backend.deleted_names(),
        vec!["restore".to_string()]
    );
    assert_eq!(skip_count(&report, SkipReason::WarmPool), 1);
}

// ---- lookup shape ---------------------------------------------------------

/// The lookups are batched once per tick and scoped to the pods that actually
/// carry a marker — a per-candidate query would put the whole stuck backlog on
/// Postgres every tick. Either marker selects the sandbox, since a restore or
/// started recovery can own a pooled pod without annotating it; only the slot
/// half is restricted to pool-labelled pods. Plain pods are never looked up.
#[tokio::test]
async fn marker_lookups_are_batched_and_scoped_to_marked_pods() {
    let restore = restore_snapshot("restore");
    let warm = warm_snapshot("warm");
    let restore_sandbox = restore.sandbox_id.expect("fixture sets a sandbox id");
    let warm_sandbox = warm.sandbox_id.expect("fixture sets a sandbox id");
    let backend = FakeBackend::new(vec![
        restore,
        warm,
        stuck_snapshot("plain-a"),
        stuck_snapshot("plain-b"),
    ]);
    let (mut reconciler, _clock) = build(test_config(60), backend);

    reconciler.tick().await;

    assert_eq!(reconciler.store.lifecycle_query_count(), 1);
    let mut queried = reconciler.store.last_lifecycle_query();
    queried.sort();
    let mut expected = vec![restore_sandbox, warm_sandbox];
    expected.sort();
    assert_eq!(queried, expected, "both marked pods, neither plain one");
    assert_eq!(reconciler.store.last_slot_query(), vec!["warm".to_string()]);
}

/// Same shape, opposite row: the restore finished, so the late-revealed marker
/// is stale and the pod is an ordinary candidate.
#[tokio::test]
async fn a_stale_marker_revealed_only_on_recheck_does_not_block_the_delete() {
    let pod = stuck_snapshot("p");
    let sandbox_id = pod.sandbox_id.expect("fixture sets a sandbox id");
    let mut fresh = pod.clone();
    fresh.is_restore = true;
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(fresh));
    let store = FakeStore::default().with_restore_owner(sandbox_id, Owner::Released);
    let (mut reconciler, _clock) = build_with_store(test_config(60), backend, store);

    assert_eq!(reconciler.tick().await.deleted, 1);
}

/// The fail-closed direction survives the re-read: when the live lookup itself
/// fails, a late-revealed marker still excludes the pod.
#[tokio::test]
async fn a_marker_revealed_only_on_recheck_excludes_when_the_re_read_fails() {
    let pod = stuck_snapshot("p");
    let mut fresh = pod.clone();
    fresh.is_restore = true;
    let backend =
        FakeBackend::new(vec![pod]).with_fresh_override("p", FreshOverride::Snapshot(fresh));
    let (mut reconciler, _clock) =
        build_with_store(test_config(60), backend, FakeStore::failing_lifecycles());

    let report = reconciler.tick().await;

    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::RestorePod), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
    // The re-read is what asked — and failed — for this pod; without it the
    // batch's empty map would have produced the same skip for the wrong reason.
    assert_eq!(reconciler.store.lifecycle_query_count(), 2);
}

// ---- ownership is fenced at the destructive act --------------------------

/// Ownership is re-read immediately before the delete, not just once per tick.
/// Both transitions that matter are Postgres-only — a claimed slot released back
/// to `ready` (`release_claimed_pool_slot` writes that directly), or a sandbox
/// entering `restoring` — so they leave the pod object untouched and neither the
/// verify GET nor the UID/resourceVersion delete precondition can catch them.
#[tokio::test]
async fn a_slot_reclaimed_after_the_batch_lookup_stops_the_delete() {
    // Claimed at the tick's batch lookup, released back to the pool before the
    // delete.
    let store = FakeStore::default()
        .with_slot_owner("p", Owner::Released)
        .with_slot_owner_after_first_read("p", Owner::InFlight);
    let backend = FakeBackend::new(vec![warm_snapshot("p")]);
    let (mut reconciler, _clock) = build_with_store(test_config(60), backend, store);

    let report = reconciler.tick().await;

    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::WarmPool), 1);
    assert_eq!(reconciler.backend.delete_count(), 0);
    // The batch lookup and the pre-delete re-read are separate queries.
    assert_eq!(reconciler.store.slot_query_count(), 2);
}

/// The same fence on the restore side: the sandbox re-enters `restoring` after
/// the tick's batch lookup released its pod.
#[tokio::test]
async fn a_sandbox_re_entering_restoring_after_the_batch_lookup_stops_the_delete() {
    let pod = restore_snapshot("p");
    let sandbox_id = pod.sandbox_id.expect("fixture sets a sandbox id");
    let store = FakeStore::default()
        .with_restore_owner(sandbox_id, Owner::Released)
        .with_restore_owner_after_first_read(sandbox_id, Owner::InFlight);
    let (mut reconciler, _clock) =
        build_with_store(test_config(60), FakeBackend::new(vec![pod]), store);

    let report = reconciler.tick().await;

    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::RestorePod), 1);
    assert_eq!(reconciler.store.lifecycle_query_count(), 2);
}

/// An unmarked pod — the overwhelming majority — must not pay the extra query.
#[tokio::test]
async fn an_unmarked_pod_is_not_re_read_before_the_delete() {
    let (mut reconciler, _clock) = build_with_store(
        test_config(60),
        FakeBackend::new(vec![stuck_snapshot("p")]),
        FakeStore::default(),
    );

    assert_eq!(reconciler.tick().await.deleted, 1);

    // One batch lookup for the tick, and nothing more.
    assert_eq!(reconciler.store.lifecycle_query_count(), 1);
    assert_eq!(reconciler.store.slot_query_count(), 1);
}

/// A stalled ownership lookup must not halt the whole loop. The lookup has its
/// own budget precisely so the action phase keeps its deadline: before that, a
/// slow Postgres consumed the entire tick budget, the candidate loop exited at
/// its first deadline check, and *every* pod deferred — including the unmarked
/// majority that need no lookup at all. This GC exists to relieve a storm, so it
/// has to keep clearing the ordinary backlog while the database is unhappy.
#[tokio::test(start_paused = true)]
async fn a_stalled_ownership_lookup_still_lets_unmarked_pods_be_collected() {
    let mut config = test_config(60);
    config.tick_budget = Duration::from_secs(45);
    // Longer than the lookup budget, shorter than the tick budget.
    let store = FakeStore::stalling(Duration::from_secs(20));
    let backend = FakeBackend::new(vec![restore_snapshot("marked"), stuck_snapshot("plain")]);
    let (mut reconciler, _clock) = build_with_store(config, backend, store);

    let report = reconciler.tick().await;

    // The unmarked pod is deleted despite the stall.
    assert_eq!(report.deleted, 1);
    assert_eq!(
        reconciler.backend.deleted_names(),
        vec!["plain".to_string()]
    );
    // The marked one keeps its exclusion, since the lookup never answered.
    assert_eq!(skip_count(&report, SkipReason::RestorePod), 1);
    assert_eq!(report.deferred, 0);
}

/// Same protection on the *pre-delete* re-read. A marker the scan never saw
/// reaches the live lookup only here, so a stall lands on this read instead of
/// the batch one — and outrunning the lookup budget while the action deadline
/// still has time is this pod's problem, not the tick's. Ending the loop there
/// would defer the unmarked candidates behind it, which need no lookup at all.
#[tokio::test(start_paused = true)]
async fn a_stalled_pre_delete_re_read_still_lets_later_pods_be_collected() {
    let mut config = test_config(60);
    config.tick_budget = Duration::from_secs(45);
    // Marked only on the authoritative read, so nothing to batch: the stall can
    // only land on the pre-delete re-read.
    let mut fresh = stuck_snapshot("marked-late");
    fresh.is_warm_pool = true;
    let backend = FakeBackend::new(vec![stuck_snapshot("marked-late"), stuck_snapshot("plain")])
        .with_fresh_override("marked-late", FreshOverride::Snapshot(fresh));
    // Longer than the lookup budget, shorter than the tick budget.
    let store = FakeStore::stalling(Duration::from_secs(20));
    let (mut reconciler, _clock) = build_with_store(config, backend, store);

    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.backend.deleted_names(),
        vec!["plain".to_string()],
        "the candidate behind the stalled re-read is still collected"
    );
    assert_eq!(report.deleted, 1);
    // Fail-closed on the pod whose ownership went unanswered — and only on it.
    assert_eq!(skip_count(&report, SkipReason::TimedOut), 1);
    assert_eq!(report.deferred, 0);
}

/// The other side of that distinction: when the *action deadline* is what the
/// stalled re-read runs into, there is no tick time left for the tail either, so
/// the loop does stop and defer it.
#[tokio::test(start_paused = true)]
async fn a_re_read_that_outruns_the_action_deadline_defers_the_tail() {
    let mut config = test_config(60);
    // Shorter than the lookup budget, so the action deadline expires first.
    config.tick_budget = MARKER_LOOKUP_BUDGET / 2;
    let mut fresh = stuck_snapshot("marked-late");
    fresh.is_warm_pool = true;
    let backend = FakeBackend::new(vec![stuck_snapshot("marked-late"), stuck_snapshot("plain")])
        .with_fresh_override("marked-late", FreshOverride::Snapshot(fresh));
    let store = FakeStore::stalling(Duration::from_secs(20));
    let (mut reconciler, _clock) = build_with_store(config, backend, store);

    let report = reconciler.tick().await;

    assert_eq!(report.deleted, 0);
    assert_eq!(skip_count(&report, SkipReason::TimedOut), 1);
    assert_eq!(report.deferred, 1, "the tail waits for the next tick");
}

/// The slot-state rules are the pool crate's (`SlotOwnership::pool_owns_pod`);
/// what this side owns is the fail-closed mapping: a state this build can't
/// decode must keep the exclusion, never read as released.
#[test]
fn a_slot_row_maps_to_an_owner_and_fails_closed_on_an_unreadable_state() {
    let row = |state, reserved_sandbox_exists| SlotOwnership {
        pod_name: "p".to_string(),
        state,
        reserved_sandbox_exists,
    };

    assert_eq!(
        warm_pool_owner(&row(Ok(PoolSlotState::Bound), false)),
        Lookup::Known(Owner::Released)
    );
    assert_eq!(
        warm_pool_owner(&row(Ok(PoolSlotState::Ready), false)),
        Lookup::Known(Owner::InFlight)
    );
    assert_eq!(
        warm_pool_owner(&row(
            Err(PoolError::InvalidSlotState(
                "a_state_from_the_future".into()
            )),
            false
        )),
        Lookup::Unknown
    );
}

/// A pooled pod's slot releasing it is not the same as *nothing* owning it. A
/// started recovery reuses the wedged pod under its existing generation
/// (`claim_started_recovery` moves that row to `recovering` in place), and a
/// pooled pod carries no restore annotation to select it into the lifecycle
/// lookup — so keying the pool half on the slot row alone would hand the GC a
/// pod a live recovery is mid-way through adopting. Ownership is a question
/// about the sandbox, not about which marker happens to be on the pod.
#[tokio::test]
async fn a_released_pool_pod_is_still_excluded_by_a_live_recovery_lease() {
    let pod = warm_snapshot("p");
    let sandbox_id = pod.sandbox_id.expect("fixture sets a sandbox id");
    let store = FakeStore::default()
        .with_slot_owner("p", Owner::Released)
        .with_restore_owner(sandbox_id, Owner::InFlight);
    assert_excluded(pod, store, SkipReason::RestorePod).await;
}

/// The same pod with no recovery in flight stays collectible — the lease check
/// must not resurrect the permanent exclusion it is guarding against.
#[tokio::test]
async fn a_released_pool_pod_with_no_recovery_is_collected() {
    let pod = warm_snapshot("p");
    let sandbox_id = pod.sandbox_id.expect("fixture sets a sandbox id");
    let store = FakeStore::default()
        .with_slot_owner("p", Owner::Released)
        .with_restore_owner(sandbox_id, Owner::Released);
    assert_released(pod, store).await;
}

//! `claim_placement` over the real migrated schema: fence minting, contention,
//! re-drives, the evacuation marker protocol, backoff, and miss
//! classification.

use pplx_space_common_core::types::NodeId;

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn user_claim_mints_a_generation_and_lease_fence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;

    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_eq!(attempt.fence.sandbox, sandbox);
    assert_eq!(attempt.fence.generation.get(), 1);
    assert_eq!(attempt.attempt, 0);
    assert_eq!(attempt.target_node, None);
    assert_eq!(
        attempt.origin,
        ClaimOrigin::FreshFromSuspended,
        "a claim on a suspended row is a fresh wake, not a re-drive"
    );
    assert!(
        attempt.lease_expires_at > jiff::Timestamp::now(),
        "a fresh claim's lease must be unexpired"
    );

    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring");
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(attempt.fence.lease.as_uuid()),
        "the row must carry the fence's lease token"
    );
    let (pod_name, pod_namespace, state, active): (String, String, String, bool) =
        sqlx::query_as(sql!(
            "SELECT pod_name, pod_namespace, state, active
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        pod_name,
        format!("space-sbx-{}-g1", sandbox.as_uuid().simple())
    );
    assert_eq!(pod_namespace, NAMESPACE);
    assert_eq!(state, "creating");
    assert!(active);
    Ok(())
}

/// Fence uniqueness under contention: one suspended row, many concurrent
/// claimants, exactly one minted fence — the others observe the winner's
/// ownership. The candidate `FOR UPDATE` serializes the claim statements and
/// the re-checked WHERE makes every loser match zero rows.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_user_claims_mint_exactly_one_fence() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(10).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;

    let claims = (0..8).map(|_| {
        let store = store.clone();
        tokio::spawn(async move { store.claim_placement(sandbox, ClaimKind::UserRestore).await })
    });
    let outcomes = join_all(claims).await;

    let mut claimed = Vec::new();
    let mut already_owned = 0;
    for outcome in outcomes {
        match outcome?? {
            ClaimOutcome::Claimed(attempt) => claimed.push(attempt),
            ClaimOutcome::AlreadyOwned { .. } => already_owned += 1,
            other => panic!("unexpected concurrent claim outcome: {other:?}"),
        }
    }
    assert_eq!(claimed.len(), 1, "exactly one claimant may mint the fence");
    assert_eq!(already_owned, 7);
    let winner = claimed.first().expect("one claimed attempt");
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(winner.fence.lease.as_uuid()),
        "the row's lease must be the winner's fence"
    );
    Ok(())
}

/// A `Placing` row whose lease expired *is* reclaimable by derivation: the
/// next claim replaces the stale attempt with a fresh fence, and the stale
/// fence can no longer complete anything.
#[tokio::test(flavor = "current_thread")]
async fn expired_lease_reclaim_replaces_the_stale_attempt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let stale = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    expire_attempt_ownership(pool, sandbox).await?;
    let fresh = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_ne!(
        fresh.fence.lease, stale.fence.lease,
        "the reclaim must mint a new lease"
    );
    assert_eq!(
        fresh.fence.generation, stale.fence.generation,
        "the reclaim re-drives the still-active execution generation"
    );
    assert_eq!(
        (stale.origin, fresh.origin),
        (
            ClaimOrigin::FreshFromSuspended,
            ClaimOrigin::RedroveRestoring
        ),
        "the reclaim's origin tells its caller to reconcile the lapsed attempt's leavings"
    );
    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&stale.fence))
            .await?,
        PlacementOutcome::CasLost,
        "the replaced attempt's fence is stale and must not complete"
    );
    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring", "the stale fence wrote nothing");
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&fresh.fence))
            .await?,
        PlacementOutcome::Completed
    );
    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("started", "on_node")
    );
    Ok(())
}

/// A claimed row that fails the decode boundary must leave no live claim
/// behind. `restore_attempt_count` has no non-negative CHECK, so a corrupt
/// negative value is representable while the evacuation marker holds (the
/// marker-clear trigger resets the count only once the marker is gone, and
/// the evacuation claim keeps the marker); the decode into [`Attempt`] runs
/// inside the claim transaction, and its failure must roll the minted
/// lease/lifecycle/execution writes back rather than stamp an owner the
/// caller never received — an error-with-live-claim blocks every later
/// claimant for a full lease TTL with nothing driving the row.
#[tokio::test(flavor = "current_thread")]
async fn undecodable_claimed_row_rolls_back_and_stamps_no_owner() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET restore_attempt_count = -1 WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;

    let err = store
        .claim_placement(sandbox, ClaimKind::EvacuationRestore)
        .await
        .expect_err("a negative attempt count must fail the decode boundary");
    assert!(
        matches!(
            &err,
            PlacementError::UndecodableRow { column, .. } if *column == "restore_attempt_count"
        ),
        "unexpected error: {err}"
    );

    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        lifecycle, "suspended",
        "a failed claim must not leave the row restoring"
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        None,
        "a failed claim must not stamp a live lease"
    );
    let executions: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        executions, 0,
        "a failed claim must not leave an allocated execution generation behind"
    );
    Ok(())
}

/// The spec's evacuation-defers-to-user-wake narrative: a draining node wants
/// to move a sandbox at the same moment its owner wakes it. Both sides run
/// the same claim; there is one lease, so exactly one wins. The user's claim
/// clears the drain marker, the drain's marker-required claim then matches
/// zero rows, and the supersession is a typed drop-it outcome — the user's
/// restore is already moving the sandbox off the node, which is what the
/// drain wanted, so the drain must not wait on it.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_defers_when_a_user_wake_takes_the_row_over() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    let user = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    assert_eq!(
        evacuation_marker(pool, sandbox).await?,
        None,
        "the user takeover clears the drain marker"
    );

    let evacuation = store
        .claim_placement(sandbox, ClaimKind::EvacuationRestore)
        .await?;
    assert_eq!(
        evacuation,
        ClaimOutcome::Blocked(BlockedReason::EvacuationSuperseded),
        "a marker consumed by the user's takeover supersedes the evacuation"
    );

    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&user.fence))
            .await?,
        PlacementOutcome::Completed
    );
    Ok(())
}

/// The supersession must not decay into a hot-loop: with the marker gone
/// and the takeover's lease lapsed (nothing owns the row), an
/// `AlreadyOwned` answer would carry an already-elapsed deadline and the
/// drain would retry instantly forever — the legacy path surfaced this as
/// `EvacuationClaimLost`. The classification is drop-it even when no owner
/// remains, because a marker-required claim can never succeed again.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_is_superseded_even_after_the_takeovers_lease_lapses() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    expire_attempt_ownership(pool, sandbox).await?;

    let outcome = store
        .claim_placement(sandbox, ClaimKind::EvacuationRestore)
        .await?;

    assert_eq!(
        outcome,
        ClaimOutcome::Blocked(BlockedReason::EvacuationSuperseded),
        "an ownerless markerless row still supersedes the evacuation — never an elapsed deadline"
    );
    Ok(())
}

/// Fence uniqueness under contention, evacuation kind: the mirror of
/// `concurrent_user_claims_mint_exactly_one_fence`. The lease-expiry gate on
/// the marked-row claim is what makes the losers observe the winner instead
/// of overwriting its token mid-flight.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_evacuation_claims_mint_exactly_one_fence() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(10).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    let claims = (0..8).map(|_| {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .claim_placement(sandbox, ClaimKind::EvacuationRestore)
                .await
        })
    });
    let outcomes = join_all(claims).await;

    let mut claimed = Vec::new();
    let mut already_owned = 0;
    for outcome in outcomes {
        match outcome?? {
            ClaimOutcome::Claimed(attempt) => claimed.push(attempt),
            ClaimOutcome::AlreadyOwned { .. } => already_owned += 1,
            other => panic!("unexpected concurrent claim outcome: {other:?}"),
        }
    }
    assert_eq!(claimed.len(), 1, "exactly one claimant may mint the fence");
    assert_eq!(already_owned, 7);
    let winner = claimed.first().expect("one claimed attempt");
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(winner.fence.lease.as_uuid()),
        "the row's lease must be the winner's fence"
    );
    Ok(())
}

/// The same race with the drain ahead: its claim holds the marked
/// `restoring` row, which the user claim's own candidate WHERE excludes —
/// so the user's wake is Blocked (the api surfaces a conflict), never an
/// AlreadyOwned deadline inviting a retry that cannot succeed while the
/// marker holds.
#[tokio::test(flavor = "current_thread")]
async fn user_claim_is_blocked_while_the_evacuation_restore_owns_the_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;

    let evacuation = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );
    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring");
    assert!(
        evacuation_marker(pool, sandbox).await?.is_some(),
        "the drain claim keeps its marker for the finalize leg"
    );

    let user = store
        .claim_placement(sandbox, ClaimKind::UserRestore)
        .await?;
    assert_eq!(
        user,
        ClaimOutcome::Blocked(BlockedReason::Lifecycle(StoredSandboxLifecycle::Live(
            SandboxLifecycle::Restoring
        ))),
        "the user claim must not grab a marked drain restore"
    );

    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&evacuation.fence))
            .await?,
        PlacementOutcome::Completed
    );
    Ok(())
}

/// A second evacuation re-drive while the first attempt's lease is unexpired
/// must not steal the live owner's fence: the marked `restoring` row is
/// accepted for crash recovery only once its lease has lapsed.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_reclaim_defers_while_the_first_attempts_lease_is_unexpired() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    let first = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );

    let second = store
        .claim_placement(sandbox, ClaimKind::EvacuationRestore)
        .await?;

    let deadline = expect_already_owned(second);
    // The deadline mirrors the evacuation gate: the lease alone. The fresh
    // execution minted by the first claim carries a longer ownership window,
    // but the evacuation claim's SQL never reads it — reporting it would
    // tell the re-drive to wait ~150s past its actual eligibility.
    assert_eq!(
        deadline,
        stored_lease_deadline(pool, sandbox).await?,
        "an evacuation miss must report the lease deadline, not the execution window"
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(first.fence.lease.as_uuid()),
        "the row must keep the first attempt's lease token"
    );
    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&first.fence))
            .await?,
        PlacementOutcome::Completed,
        "the live owner's fence must still complete"
    );
    Ok(())
}

/// The crash-recovery re-drive the marked-`restoring` acceptance exists for:
/// once the abandoned attempt's lease lapses, the sweep's next claim replaces
/// the stale fence and resumes the marked restore.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_reclaim_replaces_the_stale_attempt_once_the_lease_expires() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    let stale = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );

    expire_lease(pool, sandbox).await?;
    let fresh = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );

    assert_ne!(
        fresh.fence.lease, stale.fence.lease,
        "the reclaim must mint a new lease"
    );
    assert_eq!(
        (stale.origin, fresh.origin),
        (
            ClaimOrigin::FreshFromSuspended,
            ClaimOrigin::RedroveRestoring
        ),
        "the marked re-drive reports its resumed shape"
    );
    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&stale.fence))
            .await?,
        PlacementOutcome::CasLost,
        "the replaced attempt's fence is stale and must not complete"
    );
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&fresh.fence))
            .await?,
        PlacementOutcome::Completed
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn claim_reuses_the_active_execution_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    // An active-but-not-fresh execution (a prior failed attempt) does not
    // block the claim and stays the fence's generation.
    insert_execution(pool, sandbox, 3, "recovery_failed", 0).await?;

    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_eq!(attempt.fence.generation.get(), 3);
    let executions: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        executions, 1,
        "the claim reuses the row instead of allocating"
    );
    Ok(())
}

/// A fresh working execution owns the restore gate even when the lease
/// columns are empty — the guard that keeps a claim from double-driving an
/// execution the node is still acting on.
#[tokio::test(flavor = "current_thread")]
async fn user_claim_defers_to_a_fresh_working_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    insert_execution(pool, sandbox, 1, "running", 0).await?;

    let outcome = store
        .claim_placement(sandbox, ClaimKind::UserRestore)
        .await?;
    assert!(
        matches!(outcome, ClaimOutcome::AlreadyOwned { .. }),
        "a fresh active execution owns the row, got {outcome:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn claims_outside_the_restore_window_are_blocked() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    // Every stored lifecycle except the two claimable ones (`suspended` and
    // the re-drivable `restoring`) must classify as Blocked for both fenced
    // claim kinds. The one exception is a markerless `stopped` row under the
    // evacuation claim: `stopped` is inside that claim's window, so the miss
    // reports the missing marker instead (see
    // `evacuation_claim_takes_only_a_released_stopped_evacuation`).
    const OUTSIDE_WINDOW: [StoredSandboxLifecycle; 16] = [
        StoredSandboxLifecycle::Live(SandboxLifecycle::Created),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Creating),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Starting),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Started),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Pausing),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Paused),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Resuming),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Suspending),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Stopping),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Stopped),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Deleting),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Checkpointing),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Failed),
        StoredSandboxLifecycle::Live(SandboxLifecycle::Quarantined),
        StoredSandboxLifecycle::Deleted,
        StoredSandboxLifecycle::CleanupRequired,
    ];

    for lifecycle in OUTSIDE_WINDOW {
        let sandbox = insert_sandbox(pool, lifecycle.as_str()).await?;
        for kind in [ClaimKind::UserRestore, ClaimKind::EvacuationRestore] {
            let outcome = store.claim_placement(sandbox, kind).await?;
            let expected = if kind == ClaimKind::EvacuationRestore
                && lifecycle == StoredSandboxLifecycle::Live(SandboxLifecycle::Stopped)
            {
                BlockedReason::EvacuationSuperseded
            } else {
                BlockedReason::Lifecycle(lifecycle)
            };
            assert_eq!(
                outcome,
                ClaimOutcome::Blocked(expected),
                "a {kind} claim on a {lifecycle} row must be blocked"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn claim_on_a_missing_sandbox_is_not_found() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let store = store_for(&db);
    let missing = SandboxId::new();

    let err = store
        .claim_placement(missing, ClaimKind::UserRestore)
        .await
        .expect_err("claiming a missing sandbox must fail");
    assert!(
        matches!(err, PlacementError::SandboxNotFound(sandbox) if sandbox == missing),
        "unexpected error: {err}"
    );
    Ok(())
}

/// Claim-miss classification, lease arm: while the lease alone owns a
/// `restoring` row (its execution has aged out of the ownership window),
/// the miss reports the lease's exact stored deadline — not a rounded or
/// re-derived instant.
#[tokio::test(flavor = "current_thread")]
async fn missed_claim_reports_the_exact_lease_deadline_while_the_lease_owns_the_row() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    age_execution_past_ownership(pool, sandbox).await?;

    let deadline = expect_already_owned(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_eq!(
        deadline,
        stored_lease_deadline(pool, sandbox).await?,
        "the reported deadline is the stored lease expiry, ceil-to-millisecond"
    );
    assert!(
        deadline > jiff::Timestamp::now(),
        "an unexpired lease owns the row into the future"
    );
    Ok(())
}

/// Claim-miss classification, execution arm: a `restoring` row whose lease
/// has expired is still owned while its execution is fresh, and the miss
/// reports the execution window's exact deadline — the max() half of the
/// ownership derivation.
#[tokio::test(flavor = "current_thread")]
async fn missed_claim_reports_the_execution_deadline_when_it_outlives_the_lease() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    expire_lease(pool, sandbox).await?;

    let deadline = expect_already_owned(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_eq!(
        deadline,
        stored_execution_ownership_deadline(pool, sandbox).await?,
        "the reported deadline is the fresh execution's ownership window"
    );
    assert!(
        deadline > jiff::Timestamp::now(),
        "execution ownership outlives the expired lease"
    );
    Ok(())
}

/// Claim-miss classification, suspended arm: a `suspended` row with no
/// lease at all is still owned while a fresh active execution works it —
/// the same guard that made the claim miss — so the miss reports that
/// execution window's deadline, not an already-expired NOW() that sends
/// callers into an immediate-retry spin for the whole ownership window.
#[tokio::test(flavor = "current_thread")]
async fn missed_claim_on_a_suspended_row_reports_the_execution_deadline() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    insert_execution(pool, sandbox, 1, "running", 0).await?;

    let deadline = expect_already_owned(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_eq!(
        deadline,
        stored_execution_ownership_deadline(pool, sandbox).await?,
        "the reported deadline is the fresh execution's ownership window"
    );
    assert!(
        deadline > jiff::Timestamp::now(),
        "a fresh execution owns the suspended row into the future"
    );
    Ok(())
}

/// The evacuation claim honors the cohort restore-backoff schedule: a
/// marked row inside its `next_restore_attempt_at` window classifies as
/// Backoff with the exact stored deadline instead of minting a fence. The
/// legacy sweep enforced this at its selection query; the claim is now the
/// single gate, so it must own the predicate.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_backs_off_until_the_scheduled_retry_instant() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    set_restore_backoff(pool, sandbox, 60).await?;

    let deadline = expect_backoff(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );

    assert_eq!(
        deadline,
        stored_backoff_deadline(pool, sandbox).await?,
        "the reported deadline is the stored backoff instant, ceil-to-millisecond"
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        None,
        "a backed-off claim must not mint a fence"
    );
    Ok(())
}

/// Once the backoff deadline lapses the marked row is claimable again.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_readmits_once_the_backoff_deadline_passes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    set_restore_backoff(pool, sandbox, -1).await?;

    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );
    Ok(())
}

/// The backoff schedule throttles the cohort sweep, not the user: a user
/// wake takes over a marked row immediately regardless of the schedule —
/// user demand outranks cohort throttling, and the takeover clears the
/// marker (and with it the backoff state, via the marker-clear trigger).
#[tokio::test(flavor = "current_thread")]
async fn user_claim_ignores_the_evacuation_backoff_schedule() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "started").await?;
    set_restore_backoff(pool, sandbox, 60).await?;

    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_eq!(
        evacuation_marker(pool, sandbox).await?,
        None,
        "the user takeover consumes the evacuation marker"
    );
    Ok(())
}

/// A `restoring -> restoring` re-drive must preserve the first claim's source
/// boundary. Moving it would admit artifacts published by a failed restore
/// branch into the retry's pre-restore selection window.
#[tokio::test(flavor = "current_thread")]
async fn restoring_reclaim_preserves_the_restore_source_boundary() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    expire_attempt_ownership(pool, sandbox).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    let original_boundary: String = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at::text FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;

    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        lifecycle, "restoring",
        "the re-drive is restoring -> restoring, so the lifecycle-change trigger cannot have bumped the clock"
    );
    let source_boundary: String = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at::text FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        source_boundary, original_boundary,
        "the re-claim must not widen the restore source window"
    );
    Ok(())
}

/// The lease-expiry comparison is inclusive: `restore_lease_expires_at <=
/// NOW()`. At the exact deadline instant the lease no longer owns the row;
/// one microsecond before, it still does. Pinned by running the claim
/// statement inside one transaction with the deadline set from that
/// transaction's own NOW() — the only clock the comparison uses.
#[tokio::test(flavor = "current_thread")]
async fn lease_expiry_is_inclusive_at_the_exact_deadline_instant() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    // Only the lease may gate the re-claim.
    age_execution_past_ownership(pool, sandbox).await?;

    for (offset_secs, reclaimable) in [(0.0_f64, true), (0.000_001_f64, false)] {
        let mut tx = pool.begin().await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET restore_lease_expires_at = NOW() + make_interval(secs => $2)
             WHERE id = $1"
        ))
        .bind(sandbox)
        .bind(offset_secs)
        .execute(&mut *tx)
        .await?;
        let claimed = sqlx::query_as::<_, (i64, i32, Option<Uuid>)>(sqlx::AssertSqlSafe(sql!(
            USER_RESTORE_CLAIM_SQL
        )))
        .bind(sandbox)
        .bind(recovery_lease_ttl_secs())
        .bind(Uuid::now_v7())
        .bind(restore_execution_ownership_ttl_secs())
        .fetch_optional(&mut *tx)
        .await?;
        assert_eq!(
            claimed.is_some(),
            reclaimable,
            "a lease expiring {offset_secs}s after the claim's NOW() must {}",
            if reclaimable {
                "be reclaimable: the deadline is inclusive"
            } else {
                "still own the row"
            }
        );
        tx.rollback().await?;
    }
    Ok(())
}

/// The evacuation claim admits one `stopped` shape — a stopped evacuation its
/// teardown has released — and mints the same fence for it as for a
/// suspended row. Every near miss sits beside that claimed row, so a refusal
/// can't pass because no `stopped` row is claimable at all, and each leaves
/// its row untouched.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_takes_only_a_released_stopped_evacuation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let released = insert_sandbox(pool, "stopped").await?;
    set_evacuation_marker(pool, released, "stopped").await?;
    let markerless = insert_sandbox(pool, "stopped").await?;
    let still_on_node = insert_sandbox(pool, "stopped").await?;
    set_evacuation_marker(pool, still_on_node, "stopped").await?;
    set_active_node(pool, still_on_node, Some(NodeId::new())).await?;
    let foreign_marker = insert_sandbox(pool, "stopped").await?;
    set_evacuation_marker(pool, foreign_marker, "started").await?;

    let attempt = expect_claimed(
        store
            .claim_placement(released, ClaimKind::EvacuationRestore)
            .await?,
    );
    assert_eq!(attempt.origin, ClaimOrigin::FreshFromStopped);
    assert_eq!(
        lifecycle_and_placement(pool, released).await?.0,
        "restoring"
    );
    assert_eq!(
        evacuation_marker(pool, released).await?.as_deref(),
        Some("stopped"),
        "the marker survives the claim, which re-drives on it"
    );
    assert_eq!(
        lease_token(pool, released).await?,
        Some(attempt.fence.lease.as_uuid())
    );

    let stopped = StoredSandboxLifecycle::Live(SandboxLifecycle::Stopped);
    for (sandbox, expected, why) in [
        (
            markerless,
            ClaimOutcome::Blocked(BlockedReason::EvacuationSuperseded),
            "a markerless stopped row is user-owned",
        ),
        (
            still_on_node,
            ClaimOutcome::Blocked(BlockedReason::StoppedEvacuationNotReleased),
            "a stopped evacuation still on its node would be placed twice",
        ),
        (
            foreign_marker,
            ClaimOutcome::Blocked(BlockedReason::Lifecycle(stopped)),
            "only the stopped evacuation's own marker admits a stopped row",
        ),
    ] {
        let outcome = store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?;
        assert_eq!(outcome, expected, "{why}");
        assert_eq!(lifecycle_and_placement(pool, sandbox).await?.0, "stopped");
        assert_eq!(lease_token(pool, sandbox).await?, None);
    }

    // The user claim never serves a stopped row, marked or not.
    let user_target = insert_sandbox(pool, "stopped").await?;
    set_evacuation_marker(pool, user_target, "stopped").await?;
    assert_eq!(
        store
            .claim_placement(user_target, ClaimKind::UserRestore)
            .await?,
        ClaimOutcome::Blocked(BlockedReason::Lifecycle(stopped))
    );
    assert_eq!(
        evacuation_marker(pool, user_target).await?.as_deref(),
        Some("stopped")
    );

    // Once its lease lapses, the claimed row re-drives as `restoring`.
    expire_lease(pool, released).await?;
    let redrive = expect_claimed(
        store
            .claim_placement(released, ClaimKind::EvacuationRestore)
            .await?,
    );
    assert_eq!(redrive.origin, ClaimOrigin::RedroveRestoring);
    Ok(())
}

/// The two conjuncts the machine adds over the api claim, the restore lease
/// and the cohort backoff, gate a stopped evacuation exactly as they gate a
/// suspended one: the same outcome, carrying the same stored deadline.
#[tokio::test(flavor = "current_thread")]
async fn stopped_evacuation_claim_honors_the_lease_and_backoff_gates() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);

    // Backoff: inside the window it reports the stored instant; past it the
    // same row is claimed.
    let backed_off = insert_sandbox(pool, "stopped").await?;
    set_evacuation_marker(pool, backed_off, "stopped").await?;
    set_restore_backoff(pool, backed_off, 60).await?;
    let deadline = expect_backoff(
        store
            .claim_placement(backed_off, ClaimKind::EvacuationRestore)
            .await?,
    );
    assert_eq!(deadline, stored_backoff_deadline(pool, backed_off).await?);
    assert_eq!(
        lifecycle_and_placement(pool, backed_off).await?.0,
        "stopped"
    );
    set_restore_backoff(pool, backed_off, -1).await?;
    let attempt = expect_claimed(
        store
            .claim_placement(backed_off, ClaimKind::EvacuationRestore)
            .await?,
    );
    assert_eq!(attempt.origin, ClaimOrigin::FreshFromStopped);

    // Lease: no ordinary write leaves an unexpired lease on a `stopped` row —
    // the clear-lease trigger drops it, and leaving `restoring` for `stopped`
    // also clears the marker. So claim the row, then put it back to `stopped`
    // with both triggers bypassed. The claim must wait the lease out rather
    // than steal it.
    let leased = insert_sandbox(pool, "stopped").await?;
    set_evacuation_marker(pool, leased, "stopped").await?;
    expect_claimed(
        store
            .claim_placement(leased, ClaimKind::EvacuationRestore)
            .await?,
    );
    let token = lease_token(pool, leased).await?;
    set_lifecycle_bypassing_lease_and_marker_triggers(pool, leased, "stopped").await?;
    assert_eq!(
        evacuation_marker(pool, leased).await?.as_deref(),
        Some("stopped")
    );
    let owned_until = expect_already_owned(
        store
            .claim_placement(leased, ClaimKind::EvacuationRestore)
            .await?,
    );
    assert_eq!(owned_until, stored_lease_deadline(pool, leased).await?);
    assert_eq!(
        lease_token(pool, leased).await?,
        token,
        "a live lease must not be replaced"
    );
    expire_lease(pool, leased).await?;
    let attempt = expect_claimed(
        store
            .claim_placement(leased, ClaimKind::EvacuationRestore)
            .await?,
    );
    assert_eq!(attempt.origin, ClaimOrigin::FreshFromStopped);
    Ok(())
}

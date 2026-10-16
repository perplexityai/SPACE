//! Postgres behaviors for the evacuation restore claim on a `stopped` row
//! (`K8sStore::begin_restore_lifecycle(require_evacuation_marker = true)`).
//!
//! A sibling module for the same reason as `stopped_restore_selector_tests`:
//! the parent is far past the file-size guidance. These take
//! `&PostgresStore` and run from `k8s_store_postgres_behaviors`.

use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;

/// Seed a `stopped` row with an optional evacuation marker and residency.
/// The marker is written while the row stays `stopped`, the shape the
/// migration admitting `'stopped'` describes for a stopped evacuation: the
/// clearing trigger only acts on a lifecycle change, so it keeps the marker.
async fn insert_stopped(
    pool: &PgPool,
    marker: Option<&str>,
    active_node_id: Option<NodeId>,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, "stopped").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = $2,
                active_node_id = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(marker)
    .bind(active_node_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// The columns a stopped evacuation's claim and its rollback can touch.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct StoppedClaimRow {
    lifecycle: String,
    evacuation_restore_target: Option<String>,
    restore_lease_token: Option<Uuid>,
    restore_attempt_count: i32,
    backoff_scheduled: bool,
}

async fn claim_row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<StoppedClaimRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle,
                evacuation_restore_target,
                restore_lease_token,
                restore_attempt_count,
                next_restore_attempt_at IS NOT NULL AS backoff_scheduled
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The evacuation claim takes exactly one `stopped` shape — the stopped
/// evacuation its teardown has released — and every near miss keeps its row
/// untouched and gets the error that sends the drain down the right branch.
///
/// The claimed row is the presence control: every refusal below sits beside a
/// row the same statement does take, so a refusal can't pass because the
/// claim takes no `stopped` row at all.
pub(super) async fn evacuation_claim_takes_only_a_released_stopped_evacuation(
    fixture: &PostgresStore,
) -> TestResult {
    let pool = &fixture.pool;
    let released = insert_stopped(pool, Some("stopped"), None).await?;
    let markerless = insert_stopped(pool, None, None).await?;
    let still_on_node = insert_stopped(pool, Some("stopped"), Some(NodeId::new())).await?;
    let foreign_marker = insert_stopped(pool, Some("started"), None).await?;
    let user_target = insert_stopped(pool, Some("stopped"), None).await?;

    let claim = fixture
        .store
        .begin_restore_lifecycle(released, true)
        .await?;
    assert_eq!(claim, RestoreClaim::FreshFromStopped);
    let row = claim_row(pool, released).await?;
    assert_eq!(row.lifecycle, "restoring");
    assert_eq!(
        row.evacuation_restore_target.as_deref(),
        Some("stopped"),
        "the marker has to survive the claim: the crash-recovery re-claim is \
         gated on it"
    );
    assert_eq!(
        row.restore_lease_token, None,
        "an evacuation claim must not mint a user restore lease"
    );

    // A user who started and stopped the sandbox again has it back; the
    // drain must defer, not fail.
    let err = fixture
        .store
        .begin_restore_lifecycle(markerless, true)
        .await
        .expect_err("a markerless stopped row is user-owned");
    assert!(
        matches!(err, SubstrateError::EvacuationClaimLost { .. }),
        "got {err:?}"
    );

    // Not torn down yet: placing it would give it two homes. Also a deferral,
    // since the drain retries it once the teardown releases the node.
    let err = fixture
        .store
        .begin_restore_lifecycle(still_on_node, true)
        .await
        .expect_err("a stopped evacuation still on its node is not claimable");
    assert!(
        matches!(err, SubstrateError::EvacuationClaimLost { .. }),
        "got {err:?}"
    );

    // Only the stopped evacuation's own marker admits a stopped row.
    let err = fixture
        .store
        .begin_restore_lifecycle(foreign_marker, true)
        .await
        .expect_err("a suspend-path marker must not admit a stopped row");
    assert!(
        matches!(err, SubstrateError::Conflict { .. }),
        "got {err:?}"
    );

    // The user claim never takes a `stopped` row, marked or not.
    let err = fixture
        .store
        .begin_restore_lifecycle(user_target, false)
        .await
        .expect_err("the user restore claim does not serve stopped rows");
    assert!(
        matches!(err, SubstrateError::Conflict { .. }),
        "got {err:?}"
    );

    for (sandbox_id, expected_marker) in [
        (markerless, None),
        (still_on_node, Some("stopped")),
        (foreign_marker, Some("started")),
        (user_target, Some("stopped")),
    ] {
        let row = claim_row(pool, sandbox_id).await?;
        assert_eq!(
            row.lifecycle, "stopped",
            "a refused claim must not move {sandbox_id}"
        );
        assert_eq!(row.evacuation_restore_target.as_deref(), expected_marker);
        assert_eq!(row.restore_lease_token, None);
    }

    // Crash recovery: a controller that died after the claim re-drives the
    // same row, which is now a marked `restoring` one.
    let claim = fixture
        .store
        .begin_restore_lifecycle(released, true)
        .await?;
    assert_evacuation_redrive_restore_claim(claim);
    Ok(())
}

/// Why a fresh stopped claim is never rolled back. The rollback predicate
/// refuses it on the same clean, undispatched failure that rolls a suspended
/// claim back, and the alternative it avoids is pinned directly: moving the
/// row back to `stopped` fires `sandboxes_clear_evacuation_restore_target`,
/// which drops the marker and the restore backoff, leaving a sandbox that is on
/// no node with no marker for the drain to find it by.
pub(super) async fn a_fresh_stopped_claim_is_not_rolled_back_on_a_clean_failure(
    fixture: &PostgresStore,
) -> TestResult {
    use super::super::super::execution::restore_failure_should_revert_to_suspended;
    use super::super::super::pod_ops::WakeDispatchStatus;

    let pool = &fixture.pool;
    let clean_err = SubstrateError::Conflict {
        message: "wake failed before live work landed".to_string(),
    };
    let sandbox_id = insert_stopped(pool, Some("stopped"), None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_attempt_count = 3,
                next_restore_attempt_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;

    let claim = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, true)
        .await?;
    assert_eq!(claim, RestoreClaim::FreshFromStopped);
    assert!(
        !restore_failure_should_revert_to_suspended(
            Some(claim),
            Some(&clean_err),
            WakeDispatchStatus::NotDispatched,
        ),
        "a stopped evacuation never had a snapshot to return to"
    );
    assert!(
        restore_failure_should_revert_to_suspended(
            Some(RestoreClaim::FreshFromSuspended {
                restore_lease_token: None
            }),
            Some(&clean_err),
            WakeDispatchStatus::NotDispatched,
        ),
        "the same failure rolls a fresh suspended claim back"
    );
    assert_eq!(
        claim_row(pool, sandbox_id).await?,
        StoppedClaimRow {
            lifecycle: "restoring".to_string(),
            evacuation_restore_target: Some("stopped".to_string()),
            restore_lease_token: None,
            restore_attempt_count: 3,
            backoff_scheduled: true,
        },
        "the claim keeps the drain's retry bookkeeping, and with no token there \
         is no lease for the failure path to release"
    );

    fixture
        .store
        .revert_lifecycle_after_rollback_best_effort(sandbox_id, "stopped", None)
        .await?;
    assert_eq!(
        claim_row(pool, sandbox_id).await?,
        StoppedClaimRow {
            lifecycle: "stopped".to_string(),
            evacuation_restore_target: None,
            restore_lease_token: None,
            restore_attempt_count: 0,
            backoff_scheduled: false,
        },
        "returning to `stopped` leaves the trigger's exempt set and erases the evacuation"
    );
    Ok(())
}

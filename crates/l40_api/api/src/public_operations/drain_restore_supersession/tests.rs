//! A user's request supersedes a drain restore that waits, at admission and
//! when the restore's attempt ends, and spares one an attempt holds.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_public_api::{OperationId, OperationKind, OperationState, PublicSandboxId};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::super::{
    ApiProduct, ClaimLimits, ClaimPublicOperations, CreatePublicOperation,
    CreatePublicOperationResult, PublicOperationStore, ReleaseClaimedPublicOperation,
    ReleasedClaim, StoredOperationKind, create_operation_tx,
};
use super::supersedes_drain_restore;

const ORG: OrgId = OrgId::from_uuid(Uuid::from_u128(1));

/// A sandbox row in `lifecycle`.
async fn sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<(SandboxId, String)> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, user_id, org_id, lifecycle, placement, managed_by, network_policy_generation)
         VALUES ($1, '00000000-0000-0000-0000-000000000010', $2, $3, 'unplaced', 'k8s', 1)"
    ))
    .bind(id)
    .bind(ORG)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok((id, PublicSandboxId::from(id).to_string()))
}

/// Insert an operation of `kind` for `target_id` through the one insert a
/// sandbox's queue takes.
async fn create(
    pool: &PgPool,
    kind: StoredOperationKind,
    target_id: &str,
) -> TestResult<OperationId> {
    let mut tx = pool.begin().await?;
    let (created, _) = create_operation_tx(
        &mut tx,
        CreatePublicOperation {
            org_id: ORG,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind,
            target_type: Some(pplx_space_public_api::OperationTargetType::Sandbox),
            target_id: Some(target_id),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        },
    )
    .await?;
    tx.commit().await?;
    let CreatePublicOperationResult::Created(operation) = created else {
        return Err(format!("not created: {created:?}").into());
    };
    Ok(operation)
}

async fn state_of(pool: &PgPool, operation: OperationId) -> TestResult<(String, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT state, next_attempt_at <= NOW()
           FROM public_operations
          WHERE operation_id = $1"
    ))
    .bind(operation.as_uuid())
    .fetch_one(pool)
    .await?)
}

/// Push a queued operation's next attempt an hour out, as a retry's backoff
/// does.
async fn back_off(pool: &PgPool, operation: OperationId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE public_operations SET next_attempt_at = NOW() + INTERVAL '1 hour'
          WHERE operation_id = $1"
    ))
    .bind(operation.as_uuid())
    .execute(pool)
    .await?;
    Ok(())
}

/// Admitting each kind on a `suspended` sandbox cancels its waiting drain
/// restore exactly when the kind supersedes one. A client reading the
/// canceled restore sees a `restore_sandbox`.
#[tokio::test(flavor = "current_thread")]
async fn admitting_each_kind_cancels_a_waiting_drain_restore_by_its_rule() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let store = PublicOperationStore::new(pool.clone());
    for kind in OperationKind::ALL {
        let (_, target_id) = sandbox(pool, "suspended").await?;
        let restore = create(pool, StoredOperationKind::DrainRestoreSandbox, &target_id).await?;
        back_off(pool, restore).await?;

        create(pool, StoredOperationKind::Public(*kind), &target_id).await?;

        let supersedes = supersedes_drain_restore(StoredOperationKind::Public(*kind));
        let expected = if supersedes { "canceled" } else { "queued" };
        assert_eq!(state_of(pool, restore).await?.0, expected, "{kind}");
        let record = store.fetch_operation(ORG, restore).await?.expect("visible");
        assert_eq!(record.kind, OperationKind::RestoreSandbox, "{kind}");
        if supersedes {
            assert_eq!(record.state, OperationState::Canceled, "{kind}");
            assert!(
                !record.terminal_error.unwrap().to_string().contains("drain"),
                "{kind}: the record names no system kind"
            );
        }
    }
    Ok(())
}

/// A drain restore a claim holds is left to its attempt, and one that ended
/// stays as it ended.
#[tokio::test(flavor = "current_thread")]
async fn admission_spares_a_claimed_restore() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let (_, target_id) = sandbox(pool, "suspended").await?;
    let restore = create(pool, StoredOperationKind::DrainRestoreSandbox, &target_id).await?;
    let (_, claimed) = claim_drain_restore(pool).await?;
    assert_eq!(claimed, restore);
    create(pool, OperationKind::SuspendSandbox.into(), &target_id).await?;
    assert_eq!(state_of(pool, restore).await?.0, "running");

    // After it landed there is nothing to cancel.
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'succeeded', claim_token = ''
          WHERE operation_id = $1"
    ))
    .bind(restore.as_uuid())
    .execute(pool)
    .await?;
    create(pool, OperationKind::SuspendSandbox.into(), &target_id).await?;
    assert_eq!(state_of(pool, restore).await?.0, "succeeded");
    Ok(())
}

/// Claim the one due drain restore.
async fn claim_drain_restore(pool: &PgPool) -> TestResult<(String, OperationId)> {
    let mut claim = PublicOperationStore::new(pool.clone())
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits {
                drain_restores: 4,
                ..ClaimLimits::any(4)
            },
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[],
        })
        .await?;
    assert_eq!(claim.operations.len(), 1, "one drain restore is due");
    Ok((claim.claim_token, claim.operations.remove(0).id))
}

/// Releasing a drain restore's attempt cancels it when a user's operation
/// that supersedes it queued meanwhile, and requeues it otherwise: when
/// nothing queued, and when what queued doesn't supersede it.
#[tokio::test(flavor = "current_thread")]
async fn release_cancels_a_restore_a_user_operation_queued_behind() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let store = PublicOperationStore::new(pool.clone());
    for (case, queued, canceled) in [
        ("a wake queued", Some(OperationKind::WakeSandbox), true),
        ("nothing queued", None, false),
        (
            "an update queued",
            Some(OperationKind::UpdateSandbox),
            false,
        ),
    ] {
        let (_, target_id) = sandbox(pool, "suspended").await?;
        let restore = create(pool, StoredOperationKind::DrainRestoreSandbox, &target_id).await?;
        let (claim_token, _) = claim_drain_restore(pool).await?;
        let later = match queued {
            Some(kind) => Some(create(pool, kind.into(), &target_id).await?),
            None => None,
        };

        let released = store
            .release_claimed_operation(ReleaseClaimedPublicOperation {
                claim_token,
                operation_id: restore,
                progress_message: "retrying".into(),
                retry_after: Duration::from_secs(60),
                count_attempt: true,
                undo_first_start: false,
                cancel_if_superseded: true,
                carried_error: None,
            })
            .await?;

        if canceled {
            assert_eq!(
                released,
                ReleasedClaim::DrainRestoreSuperseded {
                    by: later.expect("a later operation")
                },
                "{case}"
            );
            assert_eq!(state_of(pool, restore).await?.0, "canceled", "{case}");
        } else {
            assert_eq!(released, ReleasedClaim::Requeued, "{case}");
            assert_eq!(state_of(pool, restore).await?.0, "queued", "{case}");
        }
        // Clear the queue for the next case's claim.
        sqlx::query(sql!(
            "UPDATE public_operations SET state = 'canceled' WHERE target_id = $1"
        ))
        .bind(&target_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// A lifecycle call that records no operation cancels the sandbox's waiting
/// drain restore.
#[tokio::test(flavor = "current_thread")]
async fn a_call_without_an_operation_cancels_a_waiting_restore() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let store = PublicOperationStore::new(pool.clone());
    let (id, target_id) = sandbox(pool, "suspended").await?;
    let restore = create(pool, StoredOperationKind::DrainRestoreSandbox, &target_id).await?;
    let outcome = store
        .cancel_drain_restores_for_action(ORG, id, "pause")
        .await?;
    assert_eq!(outcome.canceled, 1);
    assert_eq!(state_of(pool, restore).await?.0, "canceled");
    Ok(())
}

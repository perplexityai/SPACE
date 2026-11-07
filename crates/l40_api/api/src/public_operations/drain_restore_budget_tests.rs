//! The claim's cap on drain restores ([`ClaimLimits::drain_restores`]).

use std::time::Duration;

use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;

use super::*;

const ORG: OrgId = OrgId::from_uuid(Uuid::from_u128(1));

/// Queue an operation of `kind` for a sandbox of its own.
async fn queue(pool: &PgPool, kind: StoredOperationKind) -> TestResult<OperationId> {
    let target_id = pplx_space_public_api::PublicSandboxId::from(SandboxId::new()).to_string();
    let mut tx = pool.begin().await?;
    let (created, _) = create_operation_tx(
        &mut tx,
        CreatePublicOperation {
            org_id: ORG,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&target_id),
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

/// One dispatcher's claim: up to `total` operations of `kinds`, and up to
/// `drain_restores` drain restores running at once.
async fn claim(
    pool: &PgPool,
    kinds: &[OperationKind],
    total: u32,
    drain_restores: u32,
) -> TestResult<Vec<ClaimedPublicOperation>> {
    Ok(PublicOperationStore::new(pool.clone())
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits {
                drain_restores,
                ..ClaimLimits::any(total)
            },
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: kinds,
        })
        .await?
        .operations)
}

fn drain_restores(claimed: &[ClaimedPublicOperation]) -> usize {
    claimed
        .iter()
        .filter(|operation| operation.kind == StoredOperationKind::DrainRestoreSandbox)
        .count()
}

/// The cap counts drain restores running under any claim: claims that each
/// have slots to spare take no more than the cap between them, and one
/// ending frees one.
#[tokio::test(flavor = "current_thread")]
async fn the_budget_caps_running_drain_restores_across_claims() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for _ in 0..6 {
        queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    }

    let first = claim(pool, &[], 3, 4).await?;
    let second = claim(pool, &[], 16, 4).await?;
    assert_eq!(
        (drain_restores(&first), drain_restores(&second)),
        (3, 1),
        "the second claim takes what the cap leaves"
    );
    assert!(claim(pool, &[], 16, 4).await?.is_empty(), "the cap is full");

    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'succeeded', claim_token = ''
          WHERE operation_id = $1"
    ))
    .bind(first[0].id.as_uuid())
    .execute(pool)
    .await?;
    assert_eq!(drain_restores(&claim(pool, &[], 16, 4).await?), 1);
    Ok(())
}

/// With the cap full, a user's restores are claimed as before; with no
/// budget at all, a drain restore is never claimed and the rest is.
#[tokio::test(flavor = "current_thread")]
async fn the_budget_leaves_user_work_alone() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for _ in 0..5 {
        queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    }
    for _ in 0..3 {
        queue(pool, OperationKind::RestoreSandbox.into()).await?;
    }
    let restores = [OperationKind::RestoreSandbox];

    let unbudgeted = claim(pool, &restores, 16, 0).await?;
    assert_eq!(
        (unbudgeted.len(), drain_restores(&unbudgeted)),
        (3, 0),
        "no budget claims every user restore and no drain restore"
    );
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'queued', claim_token = '' WHERE kind = $1"
    ))
    .bind(OperationKind::RestoreSandbox.as_str())
    .execute(pool)
    .await?;

    let capped = claim(pool, &[], 16, 4).await?;
    assert_eq!(drain_restores(&capped), 4);
    let users = claim(pool, &restores, 16, 4).await?;
    assert_eq!(
        (users.len(), drain_restores(&users)),
        (3, 0),
        "a full cap holds back only drain restores"
    );
    Ok(())
}

/// A claim with no drain-restore budget, as a dispatcher that runs none
/// makes, ignores drain restores entirely, as a dispatcher at the base does:
/// one due at the head of a sandbox's queue holds back nothing behind it.
#[tokio::test(flavor = "current_thread")]
async fn without_a_budget_a_drain_restore_holds_nothing_back() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let target_id = pplx_space_public_api::PublicSandboxId::from(SandboxId::new()).to_string();
    for kind in [
        StoredOperationKind::DrainRestoreSandbox,
        OperationKind::UpdateSandbox.into(),
    ] {
        let mut tx = pool.begin().await?;
        let (created, _) = create_operation_tx(
            &mut tx,
            CreatePublicOperation {
                org_id: ORG,
                actor_user_id: None,
                api_product: ApiProduct::Native,
                kind,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(&target_id),
                payload_json: serde_json::json!({}),
                idempotency: None,
                traceparent: None,
            },
        )
        .await?;
        tx.commit().await?;
        assert!(matches!(created, CreatePublicOperationResult::Created(_)));
    }

    let claimed = claim(pool, &[OperationKind::UpdateSandbox], 16, 0).await?;
    assert_eq!(
        claimed
            .iter()
            .map(|operation| operation.kind)
            .collect::<Vec<_>>(),
        [StoredOperationKind::Public(OperationKind::UpdateSandbox)],
        "the update behind the drain restore is claimed"
    );
    Ok(())
}

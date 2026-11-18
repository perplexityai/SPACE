//! A user's call on a `suspended` sandbox looks for an active drain restore
//! before it cancels any, and goes on cancelling none when the lookup fails.

use pplx_space_accounts::OrgId;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_public_api::{OperationId, OperationKind, OperationTargetType, PublicSandboxId};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{cancels_drain_restores, drain_restore_active};
use crate::public_operations::{
    ApiProduct, CreatePublicOperation, CreatePublicOperationResult, StoredOperationKind,
    create_operation_tx,
};

const ORG: OrgId = OrgId::from_uuid(Uuid::from_u128(1));

/// An operation of `kind` on `sandbox_id`, through the one insert a
/// sandbox's queue takes.
async fn create(
    pool: &PgPool,
    kind: StoredOperationKind,
    sandbox_id: SandboxId,
) -> TestResult<OperationId> {
    let target_id = PublicSandboxId::from(sandbox_id).to_string();
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

async fn set_state(pool: &PgPool, operation: OperationId, state: &str) -> TestResult {
    sqlx::query(sql!(
        "UPDATE public_operations SET state = $2 WHERE operation_id = $1"
    ))
    .bind(operation.as_uuid())
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// The cancel runs only for an active drain restore, and a failed lookup
/// cancels nothing rather than failing the call.
#[test]
fn only_an_active_drain_restore_is_cancelled_and_a_failed_lookup_cancels_none() {
    let sandbox_id = SandboxId::new();
    assert!(cancels_drain_restores(sandbox_id, Ok(true)));
    assert!(!cancels_drain_restores(sandbox_id, Ok(false)));
    assert!(!cancels_drain_restores(
        sandbox_id,
        Err(sqlx::Error::PoolTimedOut)
    ));
}

/// The lookup finds the sandbox's queued or running drain restore, and
/// nothing else: no operation, a user's operation, a finished drain
/// restore, or another sandbox's.
#[tokio::test(flavor = "current_thread")]
async fn the_lookup_finds_only_this_sandboxes_active_drain_restore() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox = SandboxId::new();
    let other = SandboxId::new();
    assert!(!drain_restore_active(pool, ORG, sandbox).await?);

    create(
        pool,
        StoredOperationKind::Public(OperationKind::StopSandbox),
        sandbox,
    )
    .await?;
    create(pool, StoredOperationKind::DrainRestoreSandbox, other).await?;
    assert!(!drain_restore_active(pool, ORG, sandbox).await?);

    let restore = create(pool, StoredOperationKind::DrainRestoreSandbox, sandbox).await?;
    assert!(drain_restore_active(pool, ORG, sandbox).await?, "queued");
    set_state(pool, restore, "running").await?;
    assert!(drain_restore_active(pool, ORG, sandbox).await?, "running");
    for finished in ["succeeded", "failed", "canceled", "expired"] {
        set_state(pool, restore, finished).await?;
        assert!(
            !drain_restore_active(pool, ORG, sandbox).await?,
            "{finished}"
        );
    }
    Ok(())
}

/// A lookup the database can't answer is an error, which the call then
/// treats as finding none.
#[tokio::test(flavor = "current_thread")]
async fn a_lookup_on_a_closed_pool_fails_and_cancels_none() -> TestResult {
    let db = pg_test_db().await?;
    db.pool.close().await;
    let sandbox = SandboxId::new();
    let looked_up = drain_restore_active(&db.pool, ORG, sandbox).await;
    assert!(looked_up.is_err(), "{looked_up:?}");
    assert!(!cancels_drain_restores(sandbox, looked_up));
    Ok(())
}

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_public_api::{OperationId, OperationKind, OperationTargetType};
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use crate::operations;

const SEED_ORG: &str = "00000000-0000-0000-0000-000000000001";
const SEED_USER: &str = "00000000-0000-0000-0000-000000000010";

fn seed_org() -> TestResult<OrgId> {
    Ok(OrgId::from_uuid(Uuid::parse_str(SEED_ORG)?))
}

async fn insert_queued(
    pool: &sqlx::PgPool,
    kind: OperationKind,
    target_id: &str,
) -> TestResult<OperationId> {
    let operation_id = OperationId::new();
    operations::insert(
        pool,
        operations::NewOperation {
            operation_id,
            org_id: seed_org()?,
            actor_user_id: Some(UserId::from_uuid(Uuid::parse_str(SEED_USER)?)),
            api_product: "native",
            kind,
            target_type: Some(&OperationTargetType::Sandbox),
            target_id: Some(target_id),
            payload_json: serde_json::json!({}),
            gc_after_days: 7,
            traceparent: None,
        },
    )
    .await?;
    Ok(operation_id)
}

#[tokio::test(flavor = "current_thread")]
async fn an_operation_is_active_only_until_it_finishes() -> TestResult {
    let db = pg_test_db().await?;
    let target = format!("sbx_{}", Uuid::now_v7());
    let operation_id = insert_queued(&db.pool, OperationKind::CreateSandbox, &target).await?;
    assert!(operations::is_active(&db.pool, operation_id).await?);

    operations::cancel_superseded_create(
        &db.pool,
        operation_id,
        "superseded",
        "operation_failed",
        serde_json::json!({}),
    )
    .await?;
    assert!(!operations::is_active(&db.pool, operation_id).await?);
    assert!(!operations::is_active(&db.pool, OperationId::new()).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn another_operation_on_the_sandbox_counts_unless_its_kind_is_outside_the_queue() -> TestResult
{
    let db = pg_test_db().await?;
    let org_id = seed_org()?;
    let target = format!("sbx_{}", Uuid::now_v7());
    let resume = insert_queued(&db.pool, OperationKind::ResumeSandbox, &target).await?;
    assert!(
        !operations::other_active_on_sandbox(
            &db.pool,
            org_id,
            &target,
            &[OperationKind::ForkSandbox],
            resume,
        )
        .await?,
        "the operation itself does not count"
    );

    insert_queued(&db.pool, OperationKind::ForkSandbox, &target).await?;
    assert!(
        !operations::other_active_on_sandbox(
            &db.pool,
            org_id,
            &target,
            &[OperationKind::ForkSandbox],
            resume,
        )
        .await?,
        "a fork is outside the sandbox's queue"
    );
    assert!(
        operations::other_active_on_sandbox(&db.pool, org_id, &target, &[], resume).await?,
        "with no kinds outside the queue, the fork counts"
    );
    Ok(())
}

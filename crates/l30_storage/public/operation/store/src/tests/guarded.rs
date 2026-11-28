//! The losing case of each guarded transition: the guard matches no row and
//! the row is left as it was. Each test then makes a call the guard admits,
//! so the loss is the guard's and not the setup's.

use pplx_space_accounts::UserId;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_public_api::{OperationId, OperationKind, OperationTargetType};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{SEED_USER, insert_queued, seed_org};
use crate::idempotency::{self, IdempotencyKeyRef};
use crate::{claim_queue, operations};

fn seed_user() -> TestResult<UserId> {
    Ok(UserId::from_uuid(Uuid::parse_str(SEED_USER)?))
}

fn sandbox_target() -> String {
    format!("sbx_{}", Uuid::now_v7())
}

/// The whole `public_operations` row, to compare before and after a loss.
async fn operation_row(pool: &PgPool, operation_id: OperationId) -> TestResult<serde_json::Value> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT to_jsonb(o) FROM public_operations o WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(pool)
    .await?)
}

/// The whole `public_idempotency_records` row `id`, if it exists.
async fn record_row(pool: &PgPool, id: Uuid) -> TestResult<Option<serde_json::Value>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT to_jsonb(r) FROM public_idempotency_records r WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?)
}

/// A queued sandbox operation of `kind`, claimed: its id, token and
/// generation.
async fn claimed(pool: &PgPool, kind: OperationKind) -> TestResult<(OperationId, String, i64)> {
    let operation_id = insert_queued(pool, kind, &sandbox_target()).await?;
    let token = format!("test-claim-{}", Uuid::now_v7());
    let mut tx = pool.begin().await?;
    let row = operations::claim_created(&mut *tx, operation_id, &token).await?;
    tx.commit().await?;
    Ok((operation_id, token, row.claim_generation))
}

fn terminal() -> operations::TerminalError<'static> {
    operations::TerminalError {
        progress_message: "failed",
        code: "operation_failed",
        error: serde_json::json!({}),
    }
}

fn requeue() -> operations::Requeue<'static> {
    operations::Requeue {
        progress_message: "requeued",
        retry_after_secs: 1.0,
        count_attempt: true,
        undo_first_start: false,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_claim_write_under_a_lost_claim_changes_nothing() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (operation_id, token, generation) = claimed(pool, OperationKind::ResumeSandbox).await?;
    let before = operation_row(pool, operation_id).await?;
    let payload = serde_json::json!({"a": 1});

    assert!(
        !operations::set_claimed_payload(
            pool,
            operation_id,
            &token,
            generation + 1,
            payload.clone()
        )
        .await?
    );
    assert!(
        !operations::scrub_claimed_payload_path(pool, operation_id, "other", generation, &["a"])
            .await?
    );
    assert!(!operations::renew_claim(pool, operation_id, &token, generation - 1).await?);
    let mut tx = pool.begin().await?;
    assert_eq!(
        operations::claimed_kind_locked(&mut *tx, operation_id, "other").await?,
        None
    );
    tx.rollback().await?;
    assert!(!operations::requeue_claimed(pool, operation_id, "other", requeue()).await?);
    assert!(
        !operations::complete_claimed(pool, operation_id, "other", "done", serde_json::json!({}))
            .await?
    );
    assert!(!operations::fail_claimed(pool, operation_id, "other", terminal()).await?);
    assert!(!operations::expire_claimed(pool, operation_id, "other", terminal()).await?);
    assert_eq!(operation_row(pool, operation_id).await?, before);

    assert!(operations::renew_claim(pool, operation_id, &token, generation).await?);
    assert!(
        operations::set_claimed_payload(pool, operation_id, &token, generation, payload).await?
    );
    assert!(
        operations::scrub_claimed_payload_path(pool, operation_id, &token, generation, &["a"])
            .await?
    );
    let mut tx = pool.begin().await?;
    assert_eq!(
        operations::claimed_kind_locked(&mut *tx, operation_id, &token)
            .await?
            .as_deref(),
        Some(OperationKind::ResumeSandbox.as_str())
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_finished_operation_takes_no_terminal_write_or_requeue() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (operation_id, token, generation) = claimed(pool, OperationKind::ResumeSandbox).await?;
    assert!(operations::fail_claimed(pool, operation_id, &token, terminal()).await?);
    let before = operation_row(pool, operation_id).await?;

    assert!(!operations::requeue_claimed(pool, operation_id, &token, requeue()).await?);
    assert!(
        !operations::complete_claimed(pool, operation_id, &token, "done", serde_json::json!({}))
            .await?
    );
    assert!(!operations::fail_claimed(pool, operation_id, &token, terminal()).await?);
    assert!(!operations::expire_claimed(pool, operation_id, &token, terminal()).await?);
    assert!(!operations::renew_claim(pool, operation_id, &token, generation).await?);
    assert_eq!(operation_row(pool, operation_id).await?, before);

    let (requeued, token, _) = claimed(pool, OperationKind::ResumeSandbox).await?;
    assert!(operations::requeue_claimed(pool, requeued, &token, requeue()).await?);
    let (completed, token, _) = claimed(pool, OperationKind::ResumeSandbox).await?;
    assert!(
        operations::complete_claimed(pool, completed, &token, "done", serde_json::json!({}))
            .await?
    );
    let (expired, token, _) = claimed(pool, OperationKind::ResumeSandbox).await?;
    assert!(operations::expire_claimed(pool, expired, &token, terminal()).await?);

    // A finished row that still carries its token and generation: only the
    // state guard can refuse these.
    let (finished, token, generation) = claimed(pool, OperationKind::ResumeSandbox).await?;
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'succeeded' WHERE operation_id = $1"
    ))
    .bind(finished.as_uuid())
    .execute(pool)
    .await?;
    let before = operation_row(pool, finished).await?;
    assert!(!operations::renew_claim(pool, finished, &token, generation).await?);
    assert!(
        !operations::set_claimed_payload(
            pool,
            finished,
            &token,
            generation,
            serde_json::json!({"a": 1})
        )
        .await?
    );
    assert!(
        !operations::scrub_claimed_payload_path(pool, finished, &token, generation, &["a"]).await?
    );
    let mut tx = pool.begin().await?;
    assert_eq!(
        operations::claimed_kind_locked(&mut *tx, finished, &token).await?,
        None
    );
    tx.rollback().await?;
    assert!(!operations::requeue_claimed(pool, finished, &token, requeue()).await?);
    assert!(
        !operations::complete_claimed(pool, finished, &token, "done", serde_json::json!({}))
            .await?
    );
    assert!(!operations::fail_claimed(pool, finished, &token, terminal()).await?);
    assert!(!operations::expire_claimed(pool, finished, &token, terminal()).await?);
    assert_eq!(operation_row(pool, finished).await?, before);
    Ok(())
}

/// A dispatcher claim of resumes with claims going stale after
/// `claim_stale_after_secs`.
fn resume_claim(claim_stale_after_secs: i64) -> claim_queue::DueClaim<'static> {
    claim_queue::DueClaim {
        total: 10,
        claim_stale_after_secs,
        claim_token: "test-takeover",
        claimable_kinds: &[OperationKind::ResumeSandbox],
        outside_queue_kinds: &[OperationKind::ForkSandbox],
        keyed: false,
        non_teardown: 10,
        teardown_while_other_due: 10,
        teardown_kinds: &[OperationKind::DeleteSandbox, OperationKind::KillSandbox],
        per_org_non_teardown: 0,
        per_org_headroom: 0,
        admission_claim_token_prefix: "test-admission:",
    }
}

#[tokio::test(flavor = "current_thread")]
async fn the_claim_does_not_take_over_a_live_claim() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (operation_id, _, generation) = claimed(pool, OperationKind::ResumeSandbox).await?;
    let before = operation_row(pool, operation_id).await?;

    let mut tx = pool.begin().await?;
    let live = claim_queue::claim_due(&mut *tx, resume_claim(3_600)).await?;
    assert!(live.operations.is_empty());
    tx.commit().await?;
    assert_eq!(operation_row(pool, operation_id).await?, before);

    let mut tx = pool.begin().await?;
    let stale = claim_queue::claim_due(&mut *tx, resume_claim(0)).await?;
    let taken = stale
        .operations
        .into_iter()
        .map(|row| row.map(|row| row.operation.operation_id))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(taken, vec![operation_id.as_uuid()]);
    tx.commit().await?;
    let after = operation_row(pool, operation_id).await?;
    assert_eq!(after["claim_token"], "test-takeover");
    assert!(
        after["claim_generation"]
            .as_i64()
            .is_some_and(|taken_over| taken_over > generation),
        "the takeover replaces the claim: {after}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_finished_fork_keeps_its_payload() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let org_id = seed_org()?;
    let (fork, token, _) = claimed(pool, OperationKind::ForkSandbox).await?;
    assert!(operations::fail_claimed(pool, fork, &token, terminal()).await?);
    let before = operation_row(pool, fork).await?;

    let payload = serde_json::json!({"leader": true});
    assert!(!operations::set_active_fork_payload(pool, org_id, fork, payload.clone()).await?);
    assert_eq!(operation_row(pool, fork).await?, before);

    let active = insert_queued(pool, OperationKind::ForkSandbox, &sandbox_target()).await?;
    assert!(operations::set_active_fork_payload(pool, org_id, active, payload).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_operation_with_an_actor_is_not_handed_over() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let actor = seed_user()?;
    let owned = insert_queued(pool, OperationKind::WakeSandbox, &sandbox_target()).await?;
    let before = operation_row(pool, owned).await?;

    assert!(!operations::hand_over_queued_system_operation(pool, owned.as_uuid(), actor).await?);
    assert_eq!(operation_row(pool, owned).await?, before);

    let system = OperationId::new();
    operations::insert(
        pool,
        operations::NewOperation {
            operation_id: system,
            org_id: seed_org()?,
            actor_user_id: None,
            api_product: "native",
            kind: OperationKind::WakeSandbox,
            target_type: Some(&OperationTargetType::Sandbox),
            target_id: Some(&sandbox_target()),
            payload_json: serde_json::json!({}),
            gc_after_days: 7,
            traceparent: None,
        },
    )
    .await?;
    assert!(operations::hand_over_queued_system_operation(pool, system.as_uuid(), actor).await?);
    Ok(())
}

/// A queued native create whose payload records a federated grant issued at
/// `issued_at`.
async fn queued_federated_create(pool: &PgPool, issued_at: i64) -> TestResult<OperationId> {
    let operation_id = OperationId::new();
    operations::insert(
        pool,
        operations::NewOperation {
            operation_id,
            org_id: seed_org()?,
            actor_user_id: Some(seed_user()?),
            api_product: "native",
            kind: OperationKind::CreateSandbox,
            target_type: Some(&OperationTargetType::Sandbox),
            target_id: Some(&sandbox_target()),
            payload_json: serde_json::json!({"federated_create": grant(issued_at)}),
            gc_after_days: 7,
            traceparent: None,
        },
    )
    .await?;
    Ok(operation_id)
}

/// A federated create context issued at `issued_at`, expiring far ahead.
fn grant(issued_at: i64) -> serde_json::Value {
    serde_json::json!({
        "issuer": "https://issuer.example.test",
        "org_id": "org-1",
        "subject_id": "subject-1",
        "service_grant_issued_at_unix": issued_at,
        "service_grant_expires_at_unix": 4_000_000_000_i64,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn an_older_federated_grant_does_not_replace_a_newer_one() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let org_id = seed_org()?;
    let actor = seed_user()?;
    let create = queued_federated_create(pool, 2_000).await?;
    let before = operation_row(pool, create).await?;

    assert!(
        !operations::renew_queued_native_create_grant(pool, org_id, actor, create, grant(1_000))
            .await?
    );
    assert_eq!(operation_row(pool, create).await?, before);

    assert!(
        operations::renew_queued_native_create_grant(pool, org_id, actor, create, grant(3_000))
            .await?
    );
    Ok(())
}

const FINGERPRINT_1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const FINGERPRINT_2: &str = "2222222222222222222222222222222222222222222222222222222222222222";

fn key(idempotency_key: &str) -> TestResult<IdempotencyKeyRef<'_>> {
    Ok(IdempotencyKeyRef {
        org_id: seed_org()?,
        method: "POST",
        path_template: "/v1/test",
        idempotency_key,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn a_reply_no_longer_as_recorded_is_neither_replaced_nor_deleted() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let id = Uuid::now_v7();
    let reserved = serde_json::json!({"reservation": "pending"});
    let idempotency_key = Uuid::now_v7().to_string();
    let mut tx = pool.begin().await?;
    idempotency::insert_reply(
        &mut *tx,
        id,
        "native",
        key(&idempotency_key)?,
        FINGERPRINT_1,
        &reserved,
    )
    .await?;
    tx.commit().await?;
    let before = record_row(pool, id).await?;

    let other = serde_json::json!({"reservation": "completed"});
    assert!(!idempotency::replace_recorded_reply(pool, id, &other, &reserved).await?);
    assert!(!idempotency::delete_recorded_reply(pool, id, &other).await?);
    assert_eq!(record_row(pool, id).await?, before);

    assert!(idempotency::replace_recorded_reply(pool, id, &reserved, &other).await?);
    assert!(idempotency::delete_recorded_reply(pool, id, &other).await?);
    assert_eq!(record_row(pool, id).await?, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_live_key_binding_is_not_taken_by_another_request() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let idempotency_key = Uuid::now_v7().to_string();
    let first = Uuid::now_v7();
    assert!(
        idempotency::bind_native_key(pool, first, key(&idempotency_key)?, FINGERPRINT_1, 10)
            .await?
    );
    let before = record_row(pool, first).await?;

    assert!(
        !idempotency::bind_native_key(
            pool,
            Uuid::now_v7(),
            key(&idempotency_key)?,
            FINGERPRINT_2,
            10
        )
        .await?
    );
    assert_eq!(record_row(pool, first).await?, before);

    let renewed = Uuid::now_v7();
    assert!(
        idempotency::bind_native_key(pool, renewed, key(&idempotency_key)?, FINGERPRINT_1, 10)
            .await?
    );
    assert_eq!(record_row(pool, first).await?, None);
    assert!(record_row(pool, renewed).await?.is_some());
    Ok(())
}

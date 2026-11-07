//! The drain restore executor against Postgres: what each attempt ends as,
//! and that ending reverts an episode the operation left open.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_public_api::{OperationState, PublicOperationResult, SandboxLifecycleState};
use pplx_space_sandbox_store::drain_restore::DrainRestoreTarget;
use sqlx::PgPool;
use uuid::Uuid;

use crate::controllers::substrate::kubernetes::KubernetesSubstrate;
use crate::controllers::substrate::kubernetes::tests::{
    insert_ready_full_snapshot_relative_to_lifecycle, insert_test_sandbox_row,
    substrate_with_failed_pod_creates, substrate_with_store,
};
use crate::drain_restore_operations::{
    DrainRestoreOperationExecutor, DrainRestoreOperationPayload, DrainRestoreRetry,
    enqueue_drain_restore,
};
use crate::public_operations::{
    ClaimLimits, ClaimPublicOperations, ClaimedPublicOperation, PublicOperationRequeueReason,
    PublicOperationStore,
};
use crate::public_sandbox_operations::PublicOperationExecutionResult;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ORG: OrgId = OrgId::from_uuid(Uuid::from_u128(1));
const MAX_ATTEMPTS: i32 = 3;

/// A drained sandbox in `lifecycle` with S, its ready suspend snapshot, as
/// its current artifact. Returns S.
async fn drained(pool: &PgPool, id: SandboxId, lifecycle: &str) -> Result<Uuid, BoxError> {
    insert_test_sandbox_row(pool, id, lifecycle).await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        pool,
        id,
        "s3://space-test/suspend-snapshots/drained?region=us-east-1",
        -60,
        Uuid::now_v7(),
    )
    .await?;
    Ok(sqlx::query_scalar(sql!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = sn.local_artifact_id,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
           FROM snapshots sn
          WHERE s.id = $1 AND sn.sandbox_id = s.id
          RETURNING sn.id"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Enqueue a drain restore of `payload` and claim it as the dispatcher
/// does, its earlier attempts set to `earlier_attempts`.
async fn claimed(
    pool: &PgPool,
    payload: serde_json::Value,
    target_id: &str,
    earlier_attempts: i32,
) -> Result<ClaimedPublicOperation, BoxError> {
    let operation = enqueue_payload(pool, payload, target_id).await?;
    sqlx::query(sql!(
        "UPDATE public_operations SET attempts = $2 WHERE operation_id = $1"
    ))
    .bind(operation)
    .bind(earlier_attempts)
    .execute(pool)
    .await?;
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
    assert_eq!(claim.operations.len(), 1, "the drain restore is claimed");
    Ok(claim.operations.remove(0))
}

/// Insert a drain restore carrying `payload` as it is, valid or not.
async fn enqueue_payload(
    pool: &PgPool,
    payload: serde_json::Value,
    target_id: &str,
) -> Result<Uuid, BoxError> {
    if let Ok(payload) = serde_json::from_value::<DrainRestoreOperationPayload>(payload.clone())
        && payload.sandbox_id.to_string() == target_id
    {
        return Ok(enqueue_drain_restore(pool, ORG, payload).await?.as_uuid());
    }
    let operation = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, api_product, kind, state, target_type, target_id,
              payload_json, requested_at)
         VALUES ($1, $2, 'native', 'drain_restore_sandbox', 'queued', 'sandbox', $3, $4,
                 clock_timestamp())"
    ))
    .bind(operation)
    .bind(ORG)
    .bind(target_id)
    .bind(sqlx::types::Json(payload))
    .execute(pool)
    .await?;
    Ok(operation)
}

fn payload(id: SandboxId, s: Uuid) -> serde_json::Value {
    DrainRestoreOperationPayload::new(id, DrainRestoreTarget::Started, s).into_json()
}

fn executor(
    pool: &PgPool,
    kubernetes: Option<KubernetesSubstrate>,
) -> DrainRestoreOperationExecutor {
    DrainRestoreOperationExecutor::new(
        pool.clone(),
        kubernetes.map(Arc::new),
        None,
        DrainRestoreRetry::with_max_attempts(MAX_ATTEMPTS),
    )
}

/// The result's state and, for a terminal one, the reason it records.
fn outcome(result: &PublicOperationExecutionResult) -> (OperationState, Option<String>) {
    let reason = result
        .terminal_error_json
        .as_ref()
        .and_then(|error| error.get("reason"))
        .and_then(|reason| reason.as_str())
        .map(str::to_owned);
    (result.state.clone(), reason)
}

/// An attempt that finds nothing to restore ends without touching the row:
/// canceled when the restore is no longer the system's (the sandbox moved
/// on, or is being deleted), deferred while S uploads, failed when S is lost
/// or the payload is unreadable. A claimable row on a substrate without a
/// drain restore fails.
#[tokio::test(flavor = "current_thread")]
async fn each_gate_ends_the_attempt_as_it_says() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    struct Case {
        name: &'static str,
        lifecycle: &'static str,
        source_state: &'static str,
        valid_payload: bool,
        expected: (OperationState, Option<&'static str>),
    }
    let cases = [
        Case {
            name: "started",
            lifecycle: "started",
            source_state: "ready",
            valid_payload: true,
            expected: (OperationState::Canceled, Some("lifecycle_changed")),
        },
        Case {
            name: "deleting",
            lifecycle: "deleting",
            source_state: "ready",
            valid_payload: true,
            expected: (OperationState::Canceled, Some("sandbox_deleted")),
        },
        Case {
            name: "S uploading",
            lifecycle: "suspended",
            source_state: "creating",
            valid_payload: true,
            expected: (OperationState::Queued, None),
        },
        Case {
            name: "S failed",
            lifecycle: "suspended",
            source_state: "failed",
            valid_payload: true,
            expected: (OperationState::Failed, Some("restore_source_unavailable")),
        },
        Case {
            name: "unreadable payload",
            lifecycle: "suspended",
            source_state: "ready",
            valid_payload: false,
            expected: (OperationState::Failed, Some("invalid_payload")),
        },
        Case {
            name: "claimable without Kubernetes",
            lifecycle: "suspended",
            source_state: "ready",
            valid_payload: true,
            expected: (OperationState::Failed, Some("unsupported_substrate")),
        },
    ];
    for case in cases {
        let id = SandboxId::new();
        let s = drained(pool, id, case.lifecycle).await?;
        sqlx::query(sql!("UPDATE snapshots SET state = $2 WHERE id = $1"))
            .bind(s)
            .bind(case.source_state)
            .execute(pool)
            .await?;
        let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
        let body = if case.valid_payload {
            payload(id, s)
        } else {
            serde_json::json!({ "sandbox_id": target_id, "target": "suspended", "snapshot_id": s })
        };
        let operation = claimed(pool, body, &target_id, 0).await?;
        let before = row_image(pool, id).await?;

        let result = executor(pool, None).execute(&operation).await;

        let (state, reason) = outcome(&result);
        assert_eq!(
            (state, reason.as_deref()),
            case.expected,
            "{}: {result:?}",
            case.name
        );
        if case.expected.0 == OperationState::Queued {
            assert_eq!(
                result.requeue_reason,
                PublicOperationRequeueReason::RetryDeferred,
                "{}",
                case.name
            );
        }
        assert_eq!(
            row_image(pool, id).await?,
            before,
            "{}: row changed",
            case.name
        );
        end(pool, &operation).await?;
    }
    Ok(())
}

/// End a claimed operation, as its driver records the result, so it no
/// longer holds one of the drain restores' slots.
async fn end(pool: &PgPool, operation: &ClaimedPublicOperation) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'failed', claim_token = '' WHERE operation_id = $1"
    ))
    .bind(operation.id.as_uuid())
    .execute(pool)
    .await?;
    Ok(())
}

/// Ending an attempt as superseded or failed reverts the episode the
/// operation left open under its own lease, back to `suspended` with the
/// target cleared; an episode under another lease is left alone.
#[tokio::test(flavor = "current_thread")]
async fn ending_reverts_an_episode_the_operation_left_open() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    for (case, own_lease) in [("own lease", true), ("another lease", false)] {
        let id = SandboxId::new();
        let s = drained(pool, id, "suspended").await?;
        let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
        let operation = claimed(pool, payload(id, s), &target_id, 0).await?;
        let token = if own_lease {
            operation.id.as_uuid()
        } else {
            Uuid::now_v7()
        };
        // An earlier attempt reached the node and released its lease; S
        // has since failed.
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'restoring',
                    restore_episode_target = 'started',
                    restore_lease_token = $2,
                    restore_lease_expires_at = NOW() - INTERVAL '1 minute'
              WHERE id = $1"
        ))
        .bind(id)
        .bind(token)
        .execute(pool)
        .await?;
        sqlx::query(sql!("UPDATE snapshots SET state = 'failed' WHERE id = $1"))
            .bind(s)
            .execute(pool)
            .await?;

        let result = executor(pool, Some(substrate_with_store(pool.clone())))
            .execute(&operation)
            .await;

        assert_eq!(
            outcome(&result),
            (
                OperationState::Failed,
                Some("restore_source_unavailable".to_owned())
            ),
            "{case}"
        );
        let row: (String, Option<String>) = sqlx::query_as(sql!(
            "SELECT lifecycle, restore_episode_target FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await?;
        let expected = if own_lease {
            ("suspended".to_owned(), None)
        } else {
            ("restoring".to_owned(), Some("started".to_owned()))
        };
        assert_eq!(row, expected, "{case}");
        end(pool, &operation).await?;
    }
    Ok(())
}

/// A failed attempt retries with the drain's backoff, spending an attempt;
/// the attempt that reaches the budget fails instead, so the restore stops
/// and says why. The claim the attempt took is reverted either way.
#[tokio::test(flavor = "current_thread")]
async fn a_failing_restore_retries_until_its_attempts_are_spent() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    for (earlier_attempts, expected) in [
        (0, (OperationState::Queued, None)),
        (
            MAX_ATTEMPTS - 1,
            (
                OperationState::Failed,
                Some("retry_attempts_exhausted".to_owned()),
            ),
        ),
    ] {
        let id = SandboxId::new();
        let s = drained(pool, id, "suspended").await?;
        let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
        let operation = claimed(pool, payload(id, s), &target_id, earlier_attempts).await?;
        let (substrate, pod_creates) = substrate_with_failed_pod_creates(pool.clone());

        let result = executor(pool, Some(substrate)).execute(&operation).await;

        assert_eq!(outcome(&result), expected, "attempt {}", operation.attempts);
        assert_eq!(pod_creates.load(std::sync::atomic::Ordering::SeqCst), 1);
        if expected.0 == OperationState::Queued {
            assert_eq!(
                result.requeue_reason,
                PublicOperationRequeueReason::TransientFailure
            );
            assert_eq!(result.retry_after, Some(Duration::from_secs(60)));
        }
        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(id)
                .fetch_one(pool)
                .await?;
        assert_eq!(lifecycle, "suspended", "attempt {}", operation.attempts);
        end(pool, &operation).await?;
    }
    Ok(())
}

/// A landed restore reports what a user's restore of the sandbox would: a
/// `restore` that reached the target state.
#[test]
fn a_landed_restore_reports_a_restore() {
    let id = SandboxId::new();
    let request = super::super::DrainRestoreRequest {
        sandbox_id: id,
        target: DrainRestoreTarget::Paused,
        snapshot_id: Uuid::now_v7(),
        restore_lease_token: Uuid::now_v7(),
    };
    let result = super::super::succeeded(&request);
    assert_eq!(result.state, OperationState::Succeeded);
    let reported: PublicOperationResult =
        serde_json::from_value(result.result_json.expect("a result")).expect("parses");
    let PublicOperationResult::Sandbox(sandbox) = reported else {
        panic!("not a sandbox result: {reported:?}");
    };
    assert_eq!(sandbox.sandbox_id, id.into());
    assert_eq!(sandbox.state, SandboxLifecycleState::Paused);
    assert_eq!(
        sandbox.action,
        Some(pplx_space_public_api::SandboxLifecycleAction::Restore)
    );
}

async fn row_image(pool: &PgPool, id: SandboxId) -> Result<String, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT row_to_json(s)::TEXT FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

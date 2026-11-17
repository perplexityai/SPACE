//! The drain restore executor against Postgres: what each attempt ends as.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_accounts::OrgId;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_public_api::{OperationState, PublicOperationResult, SandboxLifecycleState};
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
    claim(pool).await
}

/// Claim the one due drain restore as the dispatcher does.
async fn claim(pool: &PgPool) -> Result<ClaimedPublicOperation, BoxError> {
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
    DrainRestoreOperationPayload::new(id, s).into_json()
}

fn executor(
    pool: &PgPool,
    kubernetes: Option<KubernetesSubstrate>,
) -> DrainRestoreOperationExecutor {
    DrainRestoreOperationExecutor::new(
        PublicOperationStore::new(pool.clone()),
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

/// An attempt whose claim takes nothing ends without touching the row:
/// canceled when the restore is no longer the system's (the sandbox moved
/// on, or is being deleted), deferred while S uploads, failed when S is lost
/// or the payload is unreadable or names a target other than `started`. A
/// payload with a field this version doesn't know reads as one without it.
/// A claimable row on a substrate without a drain restore fails.
#[tokio::test(flavor = "current_thread")]
async fn each_refused_claim_ends_the_attempt_as_it_says() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    struct Case {
        name: &'static str,
        lifecycle: &'static str,
        source_state: &'static str,
        payload: &'static str,
        kubernetes: bool,
        expected: (OperationState, Option<&'static str>),
    }
    let cases = [
        Case {
            name: "started",
            lifecycle: "started",
            source_state: "ready",
            payload: "valid",
            kubernetes: true,
            expected: (OperationState::Canceled, Some("superseded")),
        },
        Case {
            name: "deleting",
            lifecycle: "deleting",
            source_state: "ready",
            payload: "valid",
            kubernetes: true,
            expected: (OperationState::Canceled, Some("sandbox_deleted")),
        },
        Case {
            name: "S uploading",
            lifecycle: "suspended",
            source_state: "creating",
            payload: "valid",
            kubernetes: true,
            expected: (OperationState::Queued, None),
        },
        Case {
            name: "S failed",
            lifecycle: "suspended",
            source_state: "failed",
            payload: "valid",
            kubernetes: true,
            expected: (OperationState::Failed, Some("restore_source_unavailable")),
        },
        Case {
            name: "unreadable payload",
            lifecycle: "suspended",
            source_state: "ready",
            payload: "suspended",
            kubernetes: true,
            expected: (OperationState::Failed, Some("invalid_payload")),
        },
        Case {
            name: "a stopped target",
            lifecycle: "suspended",
            source_state: "ready",
            payload: "stopped",
            kubernetes: true,
            expected: (OperationState::Failed, Some("invalid_payload")),
        },
        Case {
            name: "a paused target",
            lifecycle: "suspended",
            source_state: "ready",
            payload: "paused",
            kubernetes: true,
            expected: (OperationState::Failed, Some("invalid_payload")),
        },
        Case {
            name: "a field from a newer writer",
            lifecycle: "started",
            source_state: "ready",
            payload: "newer",
            kubernetes: true,
            expected: (OperationState::Canceled, Some("superseded")),
        },
        Case {
            name: "claimable without Kubernetes",
            lifecycle: "suspended",
            source_state: "ready",
            payload: "valid",
            kubernetes: false,
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
        let body = match case.payload {
            "valid" => payload(id, s),
            "newer" => {
                let mut body = payload(id, s);
                body["field_from_a_newer_writer"] = serde_json::json!({ "any": "value" });
                body
            }
            target => {
                serde_json::json!({ "sandbox_id": target_id, "target": target, "snapshot_id": s })
            }
        };
        let operation = claimed(pool, body, &target_id, 0).await?;
        let before = row_image(pool, id).await?;

        let kubernetes = case.kubernetes.then(|| substrate_with_store(pool.clone()));
        let result = executor(pool, kubernetes).execute(&operation).await;

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
                PublicOperationRequeueReason::WorkflowProgress,
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

/// Move `operation`'s enqueue `ago` into the past, as if it had been
/// enqueued then.
async fn enqueued_ago(
    pool: &PgPool,
    operation: &ClaimedPublicOperation,
    ago: Duration,
) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE public_operations
            SET created_at = created_at - make_interval(secs => $2),
                requested_at = requested_at - make_interval(secs => $2)
          WHERE operation_id = $1"
    ))
    .bind(operation.id.as_uuid())
    .bind(ago.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

/// The terminal error's `field`, as text.
fn terminal_field<'a>(result: &'a PublicOperationExecutionResult, field: &str) -> Option<&'a str> {
    result
        .terminal_error_json
        .as_ref()
        .and_then(|error| error.get(field))
        .and_then(|value| value.as_str())
}

/// The window counts from the enqueue, not from an attempt: an attempt that
/// starts with the window spent, however long the restore waited for a slot
/// and with any attempts left, ends it `failed` before any node work, the
/// row left as it was.
#[tokio::test(flavor = "current_thread")]
async fn an_attempt_past_the_window_ends_the_restore_before_any_node_work() -> Result<(), BoxError>
{
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let window = DrainRestoreRetry::with_max_attempts(MAX_ATTEMPTS).window;
    let id = SandboxId::new();
    let s = drained(pool, id, "suspended").await?;
    let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
    let operation = claimed(pool, payload(id, s), &target_id, 0).await?;
    enqueued_ago(pool, &operation, window).await?;
    let before = row_image(pool, id).await?;
    let (substrate, pod_creates) = substrate_with_failed_pod_creates(pool.clone());

    let result = executor(pool, Some(substrate)).execute(&operation).await;

    assert_eq!(
        outcome(&result),
        (
            OperationState::Failed,
            Some("restore_window_exhausted".to_owned())
        ),
        "{result:?}"
    );
    assert_eq!(
        result
            .terminal_error_json
            .as_ref()
            .and_then(|error| error.get("restore_window_ms"))
            .and_then(serde_json::Value::as_u64),
        Some(u64::try_from(window.as_millis())?)
    );
    assert_eq!(pod_creates.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        row_image(pool, id).await?,
        before,
        "the row is left as it was"
    );
    end(pool, &operation).await?;
    Ok(())
}

/// A retry is taken only while it would start inside the window. A
/// converging attempt (S still uploads) or a failed one with less of the
/// window left than its wait ends the restore instead, naming what it waited
/// for or the error it failed with; with more left, each retries.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_that_would_start_past_the_window_ends_the_restore() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let retry = DrainRestoreRetry::with_max_attempts(MAX_ATTEMPTS);
    for converging in [true, false] {
        // Less left than the wait the attempt would take, then more.
        let wait = if converging {
            retry.converging_delay
        } else {
            retry.base_delay
        };
        for (left, ends) in [(wait / 2, true), (wait * 3, false)] {
            let id = SandboxId::new();
            let s = drained(pool, id, "suspended").await?;
            if converging {
                sqlx::query(sql!(
                    "UPDATE snapshots SET state = 'creating' WHERE id = $1"
                ))
                .bind(s)
                .execute(pool)
                .await?;
            }
            let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
            let operation = claimed(pool, payload(id, s), &target_id, 0).await?;
            enqueued_ago(pool, &operation, retry.window - left).await?;
            let substrate = if converging {
                substrate_with_store(pool.clone())
            } else {
                substrate_with_failed_pod_creates(pool.clone()).0
            };

            let result = executor(pool, Some(substrate)).execute(&operation).await;

            let case = format!("converging {converging}, {left:?} left");
            if ends {
                assert_eq!(
                    outcome(&result),
                    (
                        OperationState::Failed,
                        Some("restore_window_exhausted".to_owned())
                    ),
                    "{case}: {result:?}"
                );
                if converging {
                    assert_eq!(
                        terminal_field(&result, "waited_for"),
                        Some("restore_source_pending"),
                        "{case}"
                    );
                } else {
                    assert!(terminal_field(&result, "error").is_some(), "{case}");
                }
            } else {
                assert_eq!(outcome(&result), (OperationState::Queued, None), "{case}");
                assert_eq!(result.retry_after, Some(wait), "{case}");
            }
            end(pool, &operation).await?;
        }
    }
    Ok(())
}

/// An attempt gets no longer than what is left of the window: with a second
/// left and a node that answers the Wake only after it, the attempt times
/// out in about that second, not the per-attempt ten minutes. Its Wake was
/// sent, so the row stays `restoring` under the operation's lease for the
/// node's receipt, and the restore ends: the retry's wait, though shorter
/// than what was left when the attempt began, no longer fits in what the
/// attempt left.
#[tokio::test(flavor = "current_thread")]
async fn an_attempt_gets_no_longer_than_the_window_leaves() -> Result<(), BoxError> {
    use crate::controllers::substrate::kubernetes::tests::drain_restore::drained_subject_without_retirement;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let id = SandboxId::new();
    let (substrate, wake_attempts, worker, s) =
        drained_subject_without_retirement(pool, id).await?;
    worker.delay_wakes(Duration::from_secs(5));
    let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
    let operation = claimed(pool, payload(id, s), &target_id, 0).await?;
    let retry = DrainRestoreRetry {
        base_delay: Duration::from_millis(500),
        ..DrainRestoreRetry::with_max_attempts(MAX_ATTEMPTS)
    };
    enqueued_ago(pool, &operation, retry.window - Duration::from_secs(2)).await?;

    let started = std::time::Instant::now();
    let result = DrainRestoreOperationExecutor::new(
        PublicOperationStore::new(pool.clone()),
        Some(Arc::new(substrate)),
        None,
        retry,
    )
    .execute(&operation)
    .await;

    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the attempt waited {:?}",
        started.elapsed()
    );
    assert_eq!(
        outcome(&result),
        (
            OperationState::Failed,
            Some("restore_window_exhausted".to_owned())
        ),
        "{result:?}"
    );
    assert_eq!(wake_attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    let (lifecycle, token): (String, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT lifecycle, restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        (lifecycle.as_str(), token),
        ("restoring", Some(operation.id.as_uuid())),
        "the claim stays the operation's for the node's receipt"
    );
    Ok(())
}

/// A drain restore the full cap held back until its window was spent is
/// claimed beside the cap, and its attempt ends it at once, with no node
/// work, so the update queued behind it on its sandbox runs next.
#[tokio::test(flavor = "current_thread")]
async fn a_cap_held_restore_ends_once_its_window_is_spent_and_frees_its_queue()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let store = PublicOperationStore::new(pool.clone());
    let claim_with_cap = |cap: u32| {
        store.claim_operations(ClaimPublicOperations {
            limits: ClaimLimits {
                drain_restores: cap,
                ..ClaimLimits::any(16)
            },
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[pplx_space_public_api::OperationKind::UpdateSandbox],
        })
    };
    // One drain restore runs and fills a cap of one.
    let running = SandboxId::new();
    let s = drained(pool, running, "suspended").await?;
    enqueue_drain_restore(pool, ORG, DrainRestoreOperationPayload::new(running, s)).await?;
    assert_eq!(claim_with_cap(1).await?.operations.len(), 1);

    let id = SandboxId::new();
    let s = drained(pool, id, "suspended").await?;
    let held = enqueue_drain_restore(pool, ORG, DrainRestoreOperationPayload::new(id, s)).await?;
    let target_id = pplx_space_public_api::PublicSandboxId::from(id).to_string();
    let update = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, api_product, kind, state, target_type, target_id,
              requested_at)
         VALUES ($1, $2, 'native', 'update_sandbox', 'queued', 'sandbox', $3,
                 clock_timestamp())"
    ))
    .bind(update)
    .bind(ORG)
    .bind(&target_id)
    .execute(pool)
    .await?;
    assert!(
        claim_with_cap(1).await?.operations.is_empty(),
        "the full cap holds the restore, and the update behind it"
    );

    let window = DrainRestoreRetry::with_max_attempts(MAX_ATTEMPTS).window;
    sqlx::query(sql!(
        "UPDATE public_operations
            SET created_at = created_at - make_interval(secs => $2),
                requested_at = requested_at - make_interval(secs => $2)
          WHERE operation_id = $1"
    ))
    .bind(held.as_uuid())
    .bind(window.as_secs_f64())
    .execute(pool)
    .await?;
    let mut claim = claim_with_cap(1).await?;
    assert_eq!(
        claim
            .operations
            .iter()
            .map(|operation| operation.id)
            .collect::<Vec<_>>(),
        [held],
        "past its window the restore is claimed beside the full cap"
    );
    let operation = claim.operations.remove(0);
    let (substrate, pod_creates) = substrate_with_failed_pod_creates(pool.clone());
    let result = executor(pool, Some(substrate)).execute(&operation).await;
    assert_eq!(
        outcome(&result),
        (
            OperationState::Failed,
            Some("restore_window_exhausted".to_owned())
        )
    );
    assert_eq!(pod_creates.load(std::sync::atomic::Ordering::SeqCst), 0);
    end(pool, &operation).await?;

    assert_eq!(
        claim_with_cap(1)
            .await?
            .operations
            .iter()
            .map(|operation| operation.id.as_uuid())
            .collect::<Vec<_>>(),
        [update],
        "the update behind it runs"
    );
    Ok(())
}

/// A landed restore reports what a user's restore of the sandbox would: a
/// `restore` that reached `started`.
#[test]
fn a_landed_restore_reports_a_restore() {
    let id = SandboxId::new();
    let request = super::super::DrainRestoreRequest {
        sandbox_id: id,
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
    assert_eq!(sandbox.state, SandboxLifecycleState::Running);
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

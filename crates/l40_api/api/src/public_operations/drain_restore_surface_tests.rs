//! A client never reads a system kind's stored name: the operation record,
//! its events (the events route and the sandbox's lifecycle stream), and the
//! terminal error the driver writes all render a drain restore as
//! `restore_sandbox`. The admin API reads the stored name.

use std::time::Duration;

use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_public_api::PublicSandboxId;
use pplx_space_test_env::TestResult;

use super::*;
use crate::public_operation_driver::{
    OperationExecutionConfig, OperationExecutionResult, OperationExecutor, drive_claimed_operation,
};

const ORG: OrgId = OrgId::from_uuid(Uuid::from_u128(1));

/// Every JSON value `value` holds that is a string naming a kind.
fn kinds_named(value: &serde_json::Value, found: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, field) in fields {
                if key == "kind"
                    && let Some(kind) = field.as_str()
                {
                    found.push(kind.to_owned());
                }
                kinds_named(field, found);
            }
        }
        serde_json::Value::Array(items) => items.iter().for_each(|item| kinds_named(item, found)),
        serde_json::Value::String(text) => {
            assert!(
                !text.contains(StoredOperationKind::DrainRestoreSandbox.as_str()),
                "a client-visible string names the system kind: {text}"
            );
        }
        _ => {}
    }
}

/// A drain restore created, claimed and failed for its spent retry budget:
/// each client surface names only `restore_sandbox`.
#[tokio::test(flavor = "current_thread")]
async fn a_client_reads_a_drain_restore_as_a_restore() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let store = PublicOperationStore::new(pool.clone());
    let sandbox_id = SandboxId::new();
    let target_id = PublicSandboxId::from(sandbox_id).to_string();
    let mut tx = pool.begin().await?;
    let (created, _) = create_operation_tx(
        &mut tx,
        CreatePublicOperation {
            org_id: ORG,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: StoredOperationKind::DrainRestoreSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&target_id),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        },
    )
    .await?;
    tx.commit().await?;
    let CreatePublicOperationResult::Created(operation_id) = created else {
        return Err("not created".into());
    };
    let mut claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits {
                drain_restores: 1,
                ..ClaimLimits::any(1)
            },
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[],
        })
        .await?;
    let operation = claim.operations.remove(0);
    assert_eq!(operation.kind, StoredOperationKind::DrainRestoreSandbox);
    // The driver fails a requeue past the retry budget, writing its own
    // terminal error.
    drive_claimed_operation(
        store.clone(),
        claim.claim_token,
        OperationExecutionConfig {
            max_retry_attempts: 1,
            ..OperationExecutionConfig::default()
        },
        operation,
        OperationExecutor::Dispatcher,
        std::future::ready(OperationExecutionResult {
            state: OperationState::Queued,
            progress_message: "retry".into(),
            result_json: None,
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }),
    )
    .await?;

    let record = store
        .fetch_operation(ORG, operation_id)
        .await?
        .ok_or("the record is visible")?;
    assert_eq!(record.kind, OperationKind::RestoreSandbox);
    assert_eq!(record.state, OperationState::Failed);
    let mut kinds = Vec::new();
    kinds_named(&serde_json::to_value(&record)?, &mut kinds);

    let events = store
        .fetch_operation_events(ORG, operation_id, None, 100)
        .await?
        .ok_or("the events are visible")?;
    let streamed = store
        .fetch_sandbox_operation_events(ORG, sandbox_id, None, 100)
        .await?;
    assert_eq!(events, streamed, "the stream carries the same events");
    for event in &events {
        kinds_named(&serde_json::to_value(event)?, &mut kinds);
    }
    let event_types = events
        .iter()
        .map(|event| event.event_type)
        .collect::<Vec<_>>();
    assert_eq!(
        event_types,
        [
            PublicOperationEventType::Created,
            PublicOperationEventType::Claimed,
            PublicOperationEventType::Failed,
        ]
    );
    // The record, the created and claimed events, and the terminal error
    // in the record and in the failed event.
    assert_eq!(kinds.len(), 5, "{kinds:?}");
    assert!(
        kinds
            .iter()
            .all(|kind| kind == OperationKind::RestoreSandbox.as_str()),
        "{kinds:?}"
    );

    let admin = store
        .fetch_operation_admin(operation_id)
        .await?
        .ok_or("the admin record is visible")?;
    assert_eq!(admin.kind, StoredOperationKind::DrainRestoreSandbox);
    assert_eq!(
        serde_json::to_value(&admin)?["kind"],
        StoredOperationKind::DrainRestoreSandbox.as_str()
    );
    Ok(())
}

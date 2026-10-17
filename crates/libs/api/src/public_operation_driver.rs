//! Running one claimed public operation to its recorded outcome.
//!
//! The cluster-controller dispatcher and the native create's inline path
//! both drive a claimed operation through [`drive_claimed_operation`]: it
//! renews the claim while the execution runs, then records the result under
//! that claim (complete, fail, or release for a retry) with the same events
//! and metrics either way. Only the `executor` metric tag tells them apart.

use std::future::Future;
use std::time::{Duration, Instant};

use pplx_space_common_core::types::time::format_simple_duration;
use pplx_space_public_api::{OperationKind, OperationState, PublicErrorCode};
use tokio::time::MissedTickBehavior;

use crate::public_operations::{
    ApiProduct, ClaimedPublicOperation, CompleteClaimedPublicOperation, FailClaimedPublicOperation,
    PublicOperationRequeueReason, PublicOperationStore, PublicOperationStoreError,
    ReleaseClaimedPublicOperation,
};
use crate::public_sandbox_operations::PublicOperationExecutionResult;

pub use pplx_space_metrics::operations::OperationExecutor;

/// How long a claim may go unrenewed before the dispatcher reclaims it.
pub const DEFAULT_CLAIM_STALE_AFTER: Duration = Duration::from_secs(60);
pub const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(5);
pub const DEFAULT_MAX_RETRY_ATTEMPTS: i32 = 16;
pub const DEFAULT_M1_MAX_AGE: Duration = Duration::from_secs(1_800);

fn m1_deadline_message(max_age_seconds: u64) -> String {
    let max_age = format_simple_duration(Duration::from_secs(max_age_seconds));
    format!(
        "Operation did not reach a terminal result within {max_age}; \
         inspect the target state before retrying."
    )
}

/// The root span of one claimed attempt of an operation, linked to the
/// request that created it.
///
/// Each attempt is a trace of its own rather than a child of the request.
/// Operations that share a caller's trace, such as parallel deletes or forks
/// one agent request fans out, run concurrently in this process; under the
/// native Datadog tracer a second span parented on a remote context of a trace
/// the process already has open splits that trace. And an operation retried
/// for as long as its admission keeps it waiting would grow one request's
/// trace without bound. The link names the request instead, and the span is
/// sampled as the request was. It is a consumer span, as for any queued
/// message.
///
/// A link is not searchable, so the span records the request's trace id as
/// `origin_trace_id`, with the operation's `operation_id` and `sandbox_id`:
/// a request's operations are found by any of them. An operation saved
/// without context links to nothing.
pub fn execution_span(operation: &ClaimedPublicOperation) -> tracing::Span {
    let operation_name = format!("operation.{}", operation.kind.as_str());
    let span = tracing::info_span!(
        parent: None,
        "public_operation.execute",
        otel.kind = "consumer",
        otel.name = %operation_name,
        "operation.name" = "public_operation.execute",
        "resource.name" = operation.kind.as_str(),
        operation_id = %operation.id,
        operation_kind = operation.kind.as_str(),
        api_product = operation.api_product.as_str(),
        attempt = operation.attempts,
        claim_generation = operation.claim_generation,
        sandbox_id = operation.sandbox_id(),
        origin_trace_id = tracing::field::Empty,
        origin_sampled = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    );
    if let Some(traceparent) = operation.traceparent.as_deref() {
        pplx_space_observability::trace_propagation::link_root_to_origins(&span, [traceparent]);
    }
    span
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationExecutionConfig {
    pub claim_stale_after: Duration,
    pub retry_after: Duration,
    pub max_retry_attempts: i32,
    pub m1_max_age: Option<Duration>,
}

impl Default for OperationExecutionConfig {
    fn default() -> Self {
        Self {
            claim_stale_after: DEFAULT_CLAIM_STALE_AFTER,
            retry_after: DEFAULT_RETRY_AFTER,
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            m1_max_age: Some(DEFAULT_M1_MAX_AGE),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperationExecutionResult {
    pub state: OperationState,
    pub progress_message: String,
    pub result_json: Option<serde_json::Value>,
    pub terminal_error_code: Option<PublicErrorCode>,
    pub terminal_error_json: Option<serde_json::Value>,
    pub retry_after: Option<Duration>,
    pub requeue_reason: PublicOperationRequeueReason,
}

impl From<PublicOperationExecutionResult> for OperationExecutionResult {
    fn from(value: PublicOperationExecutionResult) -> Self {
        Self {
            state: value.state,
            progress_message: value.progress_message,
            result_json: value.result_json,
            terminal_error_code: value.terminal_error_code,
            terminal_error_json: value.terminal_error_json,
            retry_after: value.retry_after,
            requeue_reason: value.requeue_reason,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    pub claimed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub retried: usize,
    pub stale_claims: usize,
}

impl TickReport {
    pub fn merge(&mut self, other: Self) {
        self.claimed += other.claimed;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.retried += other.retried;
        self.stale_claims += other.stale_claims;
    }
}

/// Run `execution` under the claim `claim_token` holds on `operation`, then
/// record its result. The claim is renewed every third of
/// `claim_stale_after` while the execution runs; a lost renewal returns
/// without recording anything, since another claimant now owns the
/// operation. A caller that stops polling the returned future (a crash, a
/// panic, a cancelled task) leaves the claim to go stale, and the
/// dispatcher reclaims and re-drives the same idempotent operation.
pub async fn drive_claimed_operation<F>(
    store: PublicOperationStore,
    claim_token: String,
    config: OperationExecutionConfig,
    operation: ClaimedPublicOperation,
    executor: OperationExecutor,
    execution: F,
) -> Result<TickReport, PublicOperationStoreError>
where
    F: Future<Output = OperationExecutionResult>,
{
    let operation_id = operation.id;
    let api_product = operation.api_product;
    let api_product_label = api_product.as_str();
    let kind = operation.kind;
    let attempts = operation.attempts;
    let executor_label = executor.as_str();
    pplx_space_metrics::operations::claimed_by(api_product_label, kind.as_str(), executor_label);
    let renew_every = (config.claim_stale_after / 3).max(Duration::from_millis(10));
    let mut renewal = tokio::time::interval(renew_every);
    renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
    renewal.tick().await;
    tokio::pin!(execution);
    let result = loop {
        tokio::select! {
            result = &mut execution => break result,
            _ = renewal.tick() => {
                let started = Instant::now();
                let renewed = store
                    .renew_claimed_operation(&claim_token, &operation)
                    .await?;
                tracing::info!(
                    operation_id = %operation_id,
                    claim_generation = operation.claim_generation,
                    phase = "claim_renewal",
                    result = if renewed { "renewed" } else { "lost" },
                    elapsed_ms = started.elapsed().as_millis(),
                    "public operation lease renewal completed"
                );
                if !renewed {
                    return Ok(TickReport::default());
                }
            }
        }
    };
    let mut report = TickReport::default();
    let updated = match result.state {
        OperationState::Succeeded => {
            let updated = store
                .complete_claimed_operation(CompleteClaimedPublicOperation {
                    claim_token,
                    operation_id,
                    progress_message: result.progress_message,
                    result_json: result.result_json.unwrap_or_else(|| serde_json::json!({})),
                })
                .await?;
            if updated {
                report.succeeded += 1;
                pplx_space_metrics::operations::completed_by(
                    api_product_label,
                    kind.as_str(),
                    OperationState::Succeeded.as_str(),
                    executor_label,
                );
            }
            updated
        }
        OperationState::Failed => {
            let terminal_error_code = result
                .terminal_error_code
                .unwrap_or(PublicErrorCode::Internal);
            let terminal_error_label = terminal_error_code.as_str().to_string();
            let span = tracing::Span::current();
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", terminal_error_label.as_str());
            let updated = store
                .fail_claimed_operation(FailClaimedPublicOperation {
                    claim_token,
                    operation_id,
                    progress_message: result.progress_message,
                    terminal_error_code,
                    terminal_error_json: result
                        .terminal_error_json
                        .unwrap_or_else(|| serde_json::json!({})),
                })
                .await?;
            if updated {
                report.failed += 1;
                pplx_space_metrics::operations::failed(
                    api_product_label,
                    kind.as_str(),
                    &terminal_error_label,
                );
                pplx_space_metrics::operations::completed_by(
                    api_product_label,
                    kind.as_str(),
                    OperationState::Failed.as_str(),
                    executor_label,
                );
            }
            updated
        }
        // Executors only produce the known states; an `Other` here would be a
        // programming error, and releasing the claim is the safe reading.
        OperationState::Queued
        | OperationState::Running
        | OperationState::Canceled
        | OperationState::Expired
        | OperationState::Other(_) => {
            let supported_requeue = result.state == OperationState::Queued;
            let requeue_reason = if supported_requeue {
                result.requeue_reason
            } else {
                PublicOperationRequeueReason::TransientFailure
            };
            let retry_reason = if !supported_requeue {
                "unsupported_executor_state"
            } else {
                match requeue_reason {
                    PublicOperationRequeueReason::TransientFailure => "executor_requested_retry",
                    reason => reason.as_str(),
                }
            };
            let last_progress_message = result.progress_message;
            let progress_message = if supported_requeue {
                last_progress_message.clone()
            } else {
                format!("executor returned unsupported state {}", result.state)
            };
            let preserve_retry_budget =
                supported_requeue && requeue_reason.preserves_retry_budget();
            let last_requeue_reason = if supported_requeue {
                requeue_reason.as_str()
            } else {
                "unsupported_executor_state"
            };
            let deadline = if is_native_m1_terminalized_kind(api_product, kind) {
                config.m1_max_age
            } else {
                None
            };
            let deadline_exceeded = if let Some(deadline) = deadline {
                store
                    .remaining_deadline(operation_id, deadline)
                    .await?
                    .is_some_and(|remaining| remaining.is_zero())
            } else {
                false
            };
            if deadline_exceeded {
                let max_age_seconds = deadline
                    .expect("deadline is configured when it is exceeded")
                    .as_secs();
                let terminal_error_code = PublicErrorCode::OperationFailed;
                let updated = store
                    .fail_claimed_operation(FailClaimedPublicOperation {
                        claim_token,
                        operation_id,
                        progress_message: m1_deadline_message(max_age_seconds),
                        terminal_error_code: terminal_error_code.clone(),
                        terminal_error_json: serde_json::json!({
                            "operation_id": operation_id.to_string(),
                            "kind": kind.as_str(),
                            "reason": "operation_deadline_exceeded",
                            "max_age_seconds": max_age_seconds,
                            "last_requeue_reason": last_requeue_reason,
                            "last_progress_message": last_progress_message,
                            "attempts": attempts,
                        }),
                    })
                    .await?;
                if updated {
                    report.failed += 1;
                    pplx_space_metrics::operations::failed(
                        api_product_label,
                        kind.as_str(),
                        terminal_error_code.as_str(),
                    );
                    pplx_space_metrics::operations::completed_by(
                        api_product_label,
                        kind.as_str(),
                        OperationState::Failed.as_str(),
                        executor_label,
                    );
                    pplx_space_metrics::operations::deadline_exceeded(
                        api_product_label,
                        kind.as_str(),
                        last_requeue_reason,
                    );
                    tracing::warn!(
                        operation_id = %operation_id,
                        api_product = api_product_label,
                        kind = kind.as_str(),
                        last_requeue_reason,
                        max_age_seconds,
                        attempts,
                        "public operation exceeded the M1 terminalization deadline"
                    );
                }
                updated
            } else if !preserve_retry_budget
                && retry_budget_exhausted(attempts, config.max_retry_attempts)
            {
                let terminal_error_code = result
                    .terminal_error_code
                    .unwrap_or(PublicErrorCode::Internal);
                let terminal_error_json = result.terminal_error_json.unwrap_or_else(|| {
                    serde_json::json!({
                        "operation_id": operation_id.to_string(),
                        "kind": kind.as_str(),
                        "reason": "retry_attempts_exhausted",
                        "retry_reason": retry_reason,
                        "attempts": attempts,
                            "max_retry_attempts": config.max_retry_attempts.max(1),
                    })
                });
                let updated = store
                    .fail_claimed_operation(FailClaimedPublicOperation {
                        claim_token,
                        operation_id,
                        progress_message: format!(
                            "retry attempts exhausted after {attempts} attempt(s): \
                             {progress_message}"
                        ),
                        terminal_error_code: terminal_error_code.clone(),
                        terminal_error_json,
                    })
                    .await?;
                if updated {
                    report.failed += 1;
                    pplx_space_metrics::operations::failed(
                        api_product_label,
                        kind.as_str(),
                        terminal_error_code.as_str(),
                    );
                    pplx_space_metrics::operations::completed_by(
                        api_product_label,
                        kind.as_str(),
                        OperationState::Failed.as_str(),
                        executor_label,
                    );
                }
                updated
            } else {
                let updated = store
                    .release_claimed_operation(ReleaseClaimedPublicOperation {
                        claim_token,
                        operation_id,
                        progress_message,
                        retry_after: result
                            .retry_after
                            .filter(|retry_after| !retry_after.is_zero())
                            .unwrap_or(config.retry_after),
                        // Expected workflow progress and activation admission
                        // waits do not consume the transient-failure budget.
                        count_attempt: !preserve_retry_budget,
                    })
                    .await?;
                if updated {
                    report.retried += 1;
                    pplx_space_metrics::operations::requeued(
                        api_product_label,
                        kind.as_str(),
                        retry_reason,
                    );
                }
                updated
            }
        }
    };

    if !updated {
        report.stale_claims += 1;
        pplx_space_metrics::operations::stale_claim(api_product_label, kind.as_str());
        tracing::warn!(
            operation_id = %operation_id,
            "public operation claim was lost before completion update"
        );
    }

    Ok(report)
}

fn retry_budget_exhausted(attempts: i32, max_retry_attempts: i32) -> bool {
    attempts >= max_retry_attempts.max(1)
}

/// Whether the driver fails an operation of this product and kind once it
/// outlives its deadline (`OperationExecutionConfig::m1_max_age`).
pub(crate) fn is_native_m1_terminalized_kind(api_product: ApiProduct, kind: OperationKind) -> bool {
    api_product == ApiProduct::Native
        && matches!(
            kind,
            OperationKind::CreateSandbox
                | OperationKind::DeleteSandbox
                | OperationKind::PauseSandbox
                | OperationKind::SuspendSandbox
                | OperationKind::WakeSandbox
        )
}

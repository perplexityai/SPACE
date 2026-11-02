//! Running one claimed public operation to its recorded outcome.
//!
//! The cluster-controller dispatcher and the native create's inline path
//! both drive a claimed operation through [`drive_deferrable_operation`],
//! under the [`OperationExecutionConfig`] both read from the environment: it
//! renews the claim while the execution runs, then records the result under
//! that claim (complete, fail, or release for a retry) with the same events
//! and metrics either way. Only the `executor` metric tag tells them apart.

use std::future::Future;
use std::time::{Duration, Instant};

use pplx_space_common_core::types::time::format_simple_duration;
use pplx_space_public_api::{OperationId, OperationKind, OperationState, PublicErrorCode};
use tokio::time::MissedTickBehavior;

use crate::public_operations::{
    ApiProduct, ClaimedPublicOperation, CompleteClaimedPublicOperation, ExpirePublicOperation,
    FailClaimedPublicOperation, OverdueQueuedPublicOperations, PublicOperationRequeueReason,
    PublicOperationStore, PublicOperationStoreError, ReleaseClaimedPublicOperation, ReleasedClaim,
};
use crate::public_sandbox_operations::PublicOperationExecutionResult;
use crate::retry_deferral::RetryDeferral;

use pplx_space_metrics::operations::ExecutionOutcome;
pub use pplx_space_metrics::operations::OperationExecutor;

/// How long a claim may go unrenewed before the dispatcher reclaims it.
pub const DEFAULT_CLAIM_STALE_AFTER: Duration = Duration::from_secs(60);
pub const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(5);
pub const DEFAULT_MAX_RETRY_ATTEMPTS: i32 = 16;
pub const DEFAULT_M1_MAX_AGE: Duration = Duration::from_secs(1_800);
/// The Native kinds that expire instead of starting once older than the M1
/// deadline. One that already started keeps the deadline it had: it gets
/// its attempt and fails if it requeues past the deadline, so a half-applied
/// change is driven to an end rather than abandoned. A delete is bound by the
/// deadline too, but only while it retries: it still runs however late it
/// starts, because expiring it would leave running a sandbox its owner asked
/// to remove.
pub const NATIVE_M1_EXPIRING_KINDS: &[OperationKind] = &[
    OperationKind::CreateSandbox,
    OperationKind::PauseSandbox,
    OperationKind::SuspendSandbox,
    OperationKind::WakeSandbox,
];
/// How long an operation may keep deferring its retry waits, from its first
/// claim. Deferred retries spend no attempts, so this matches the longest a
/// fenced restore can retry in its slot on the attempt budget: 16 attempts of
/// a 130 s wait plus the 5 s requeue hint.
pub const DEFERRED_RETRY_WINDOW: Duration = Duration::from_secs(16 * 135);
/// Largest spacing ceiling between deferred retries.
const DEFERRED_RETRY_MAX_SPACING: Duration = Duration::from_secs(60);

fn m1_deadline_message(max_age_seconds: u64) -> String {
    let max_age = format_simple_duration(Duration::from_secs(max_age_seconds));
    format!(
        "Operation did not reach a terminal result within {max_age}; \
         inspect the target state before retrying."
    )
}

/// The terminal record of a Native M1 operation that passed its deadline
/// before it ran, with the reason a deadline failure carries.
fn m1_expiry(
    operation_id: OperationId,
    kind: OperationKind,
    attempts: i32,
    max_age: Duration,
) -> ExpirePublicOperation {
    let max_age_seconds = max_age.as_secs();
    ExpirePublicOperation {
        operation_id,
        progress_message: m1_deadline_message(max_age_seconds),
        terminal_error_code: PublicErrorCode::OperationFailed,
        terminal_error_json: serde_json::json!({
            "operation_id": operation_id.to_string(),
            "kind": kind.as_str(),
            "reason": "operation_deadline_exceeded",
            "max_age_seconds": max_age_seconds,
            "attempts": attempts,
        }),
    }
}

fn expires_unrun(api_product: ApiProduct, kind: OperationKind) -> bool {
    api_product == ApiProduct::Native && NATIVE_M1_EXPIRING_KINDS.contains(&kind)
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

/// Test-hook override of [`DEFAULT_CLAIM_STALE_AFTER`], in whole seconds.
pub const CLAIM_STALE_SECS_ENV: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_CLAIM_STALE_SECS";
/// Test-hook override of [`DEFAULT_M1_MAX_AGE`], in whole seconds; `0` turns
/// the deadline off.
pub const M1_MAX_AGE_ENV: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_M1_MAX_AGE_SECS";
/// Kill switch for retry deferral.
pub const RETRY_DEFERRAL_ENABLED_ENV: &str =
    "SPACE_PUBLIC_OPERATION_DISPATCHER_RETRY_DEFERRAL_ENABLED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationExecutionConfig {
    pub claim_stale_after: Duration,
    pub retry_after: Duration,
    pub max_retry_attempts: i32,
    pub m1_max_age: Option<Duration>,
    /// Whether an execution may hand a retry wait back instead of waiting it
    /// out (see [`drive_deferrable_operation`]).
    pub defer_retry_waits: bool,
}

impl Default for OperationExecutionConfig {
    fn default() -> Self {
        Self {
            claim_stale_after: DEFAULT_CLAIM_STALE_AFTER,
            retry_after: DEFAULT_RETRY_AFTER,
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            m1_max_age: Some(DEFAULT_M1_MAX_AGE),
            defer_retry_waits: true,
        }
    }
}

impl OperationExecutionConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    /// The settings every executor of claimed operations runs under: the
    /// cluster-controller dispatcher and an API process executing inline read
    /// the same variables, so an operation is renewed, retried and bounded by
    /// its deadline the same way whichever of them runs it. The staleness
    /// and deadline overrides are test hooks; the retry-deferral switch is a
    /// deployment setting. A malformed override keeps the default.
    pub fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let default = Self::default();
        let test_secs = |name| {
            pplx_space_common_core::env::test_hook(env, name)
                .and_then(|raw| raw.parse::<u64>().ok())
        };
        Self {
            claim_stale_after: test_secs(CLAIM_STALE_SECS_ENV)
                .filter(|secs| *secs > 0)
                .map_or(default.claim_stale_after, Duration::from_secs),
            retry_after: default.retry_after,
            max_retry_attempts: default.max_retry_attempts,
            m1_max_age: match test_secs(M1_MAX_AGE_ENV) {
                Some(0) => None,
                Some(secs) => Some(Duration::from_secs(secs)),
                None => default.m1_max_age,
            },
            defer_retry_waits: match env.var(RETRY_DEFERRAL_ENABLED_ENV).as_deref() {
                Ok("1" | "true" | "True" | "TRUE") => true,
                Ok("0" | "false" | "False" | "FALSE") => false,
                _ => default.defer_retry_waits,
            },
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
    pub expired: usize,
    pub canceled: usize,
}

impl TickReport {
    pub fn merge(&mut self, other: Self) {
        self.claimed += other.claimed;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.retried += other.retried;
        self.stale_claims += other.stale_claims;
        self.expired += other.expired;
        self.canceled += other.canceled;
    }
}

/// [`drive_claimed_operation`] for an executor's work, which may hand its
/// retry waits back. With `config.defer_retry_waits`, `execution` runs under a
/// [`RetryDeferral`] scope, and a transient retry after one of its steps
/// deferred a wait is released as [`PublicOperationRequeueReason::RetryDeferred`],
/// which spends no attempt, so the wait does not hold the executor's slot.
pub async fn drive_deferrable_operation<F>(
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
    let deferral = config.defer_retry_waits.then(RetryDeferral::new);
    let recorded = deferral.clone();
    let execution = crate::retry_deferral::scope(deferral, execution);
    drive_claimed_operation(
        store,
        claim_token,
        config,
        operation,
        executor,
        async move {
            let mut result = execution.await;
            // A transient retry after a step handed its wait back is the
            // deferral; other requeue reasons keep their own accounting.
            if result.state == OperationState::Queued
                && result.requeue_reason == PublicOperationRequeueReason::TransientFailure
                && recorded.as_ref().is_some_and(RetryDeferral::deferred)
            {
                result.requeue_reason = PublicOperationRequeueReason::RetryDeferred;
            }
            result
        },
    )
    .await
}

/// Run `execution` under the claim `claim_token` holds on `operation`, then
/// record its result. The claim is renewed every third of
/// `claim_stale_after` while the execution runs; a lost renewal returns
/// without recording anything, since another claimant now owns the
/// operation. A caller that stops polling the returned future (a crash, a
/// panic, a cancelled task) leaves the claim to go stale, and the
/// dispatcher reclaims and re-drives the same idempotent operation.
///
/// A Native M1 operation of [`NATIVE_M1_EXPIRING_KINDS`] first claimed when
/// already older than the deadline is recorded `expired` without running.
/// Every other claim records `operations.execution_ms`, from here to the
/// recorded outcome, unless the caller stops polling first.
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
    if let Some(max_age) = config.m1_max_age
        && expires_unrun(api_product, kind)
        && let Some(age_secs) = operation.age_at_first_claim_secs
        && Duration::from_secs(age_secs.into()) >= max_age
    {
        let mut report = TickReport::default();
        // The expiry undoes this claim's attempt, as the operation never ran.
        let attempts = (attempts - 1).max(0);
        if store
            .expire_claimed_operation(
                m1_expiry(operation_id, kind, attempts, max_age),
                &claim_token,
            )
            .await?
        {
            report.expired += 1;
            // Not an execution, so counted as the sweep counts an expiry:
            // no claim by this executor, and a completion with no executor.
            pplx_space_metrics::operations::completed(
                api_product_label,
                kind.as_str(),
                OperationState::Expired.as_str(),
            );
            tracing::warn!(
                operation_id = %operation_id,
                api_product = api_product_label,
                kind = kind.as_str(),
                age_secs,
                max_age_seconds = max_age.as_secs(),
                "public operation expired at claim: older than the M1 terminalization deadline"
            );
        } else {
            record_lost_claim(&mut report, api_product_label, kind, executor, operation_id);
        }
        return Ok(report);
    }
    pplx_space_metrics::operations::claimed_by(api_product_label, kind.as_str(), executor);
    let started_at = Instant::now();
    let (report, outcome) = match execute_and_record(
        store,
        claim_token,
        config,
        operation,
        executor,
        execution,
    )
    .await
    {
        Ok((report, outcome)) => (Ok(report), outcome),
        Err(error) => (Err(error), ExecutionOutcome::Error),
    };
    pplx_space_metrics::operations::execution(
        api_product_label,
        kind.as_str(),
        executor,
        outcome,
        started_at,
    );
    report
}

/// The execution half of [`drive_claimed_operation`]: run `execution` under
/// the renewed claim, then record its result. Every exit returns how the
/// execution ended beside the report of what it recorded.
async fn execute_and_record<F>(
    store: PublicOperationStore,
    claim_token: String,
    config: OperationExecutionConfig,
    operation: ClaimedPublicOperation,
    executor: OperationExecutor,
    execution: F,
) -> Result<(TickReport, ExecutionOutcome), PublicOperationStoreError>
where
    F: Future<Output = OperationExecutionResult>,
{
    let operation_id = operation.id;
    let api_product = operation.api_product;
    let api_product_label = api_product.as_str();
    let kind = operation.kind;
    let attempts = operation.attempts;
    let renew_every = claim_renewal_interval(config.claim_stale_after);
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
                // Another claimant owns the operation: nothing to record.
                if !renewed {
                    return Ok((TickReport::default(), ExecutionOutcome::Lost));
                }
            }
        }
    };
    let mut report = TickReport::default();
    // The outcome this claim recorded, or `None` if the claim was lost first.
    let recorded = match result.state {
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
                    executor,
                );
            }
            updated.then_some(ExecutionOutcome::Succeeded)
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
                    executor,
                );
                pplx_space_metrics::operations::completed_by(
                    api_product_label,
                    kind.as_str(),
                    OperationState::Failed.as_str(),
                    executor,
                );
            }
            updated.then_some(ExecutionOutcome::Failed)
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
            let retry_deferred =
                supported_requeue && requeue_reason == PublicOperationRequeueReason::RetryDeferred;
            // Deferred retries spend no attempts; past the deferral window
            // they fail as an exhausted budget would.
            let deferred_for = if retry_deferred {
                store.time_since_first_claim(operation_id).await?
            } else {
                None
            };
            let deferral_exhausted_after =
                deferred_for.filter(|deferred_for| *deferred_for >= DEFERRED_RETRY_WINDOW);
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
                let terminal_error_code = result
                    .terminal_error_code
                    .unwrap_or(PublicErrorCode::OperationFailed);
                let terminal_error_json = with_carried_error(
                    serde_json::json!({
                        "operation_id": operation_id.to_string(),
                        "kind": kind.as_str(),
                        "reason": "operation_deadline_exceeded",
                        "max_age_seconds": max_age_seconds,
                        "last_requeue_reason": last_requeue_reason,
                        "last_progress_message": last_progress_message,
                        "attempts": attempts,
                    }),
                    result.terminal_error_json,
                );
                let updated = store
                    .fail_claimed_operation(FailClaimedPublicOperation {
                        claim_token,
                        operation_id,
                        progress_message: m1_deadline_message(max_age_seconds),
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
                        executor,
                    );
                    pplx_space_metrics::operations::completed_by(
                        api_product_label,
                        kind.as_str(),
                        OperationState::Failed.as_str(),
                        executor,
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
                updated.then_some(ExecutionOutcome::Failed)
            } else if deferral_exhausted_after.is_some()
                || (!preserve_retry_budget
                    && retry_budget_exhausted(attempts, config.max_retry_attempts))
            {
                let terminal_error_code = result
                    .terminal_error_code
                    .unwrap_or(PublicErrorCode::Internal);
                let mut terminal_error_json = with_carried_error(
                    serde_json::json!({
                        "operation_id": operation_id.to_string(),
                        "kind": kind.as_str(),
                        "reason": "retry_attempts_exhausted",
                        "retry_reason": retry_reason,
                        "attempts": attempts,
                        "max_retry_attempts": config.max_retry_attempts.max(1),
                    }),
                    result.terminal_error_json,
                );
                // Deferred retries give their attempts back, so the window,
                // not the attempt count, is what ran out, whichever error
                // the operation fails with.
                if let (Some(elapsed), Some(fields)) = (
                    deferral_exhausted_after,
                    terminal_error_json.as_object_mut(),
                ) {
                    fields.insert(
                        "deferred_retry_window_ms".to_owned(),
                        serde_json::json!(DEFERRED_RETRY_WINDOW.as_millis()),
                    );
                    fields.insert(
                        "elapsed_since_first_claim_ms".to_owned(),
                        serde_json::json!(elapsed.as_millis()),
                    );
                }
                let progress_message = match deferral_exhausted_after {
                    Some(elapsed) => format!(
                        "retry deferral window of {} exhausted {} after the first claim: \
                         {progress_message}",
                        format_simple_duration(DEFERRED_RETRY_WINDOW),
                        format_simple_duration(Duration::from_secs(elapsed.as_secs())),
                    ),
                    None => format!(
                        "retry attempts exhausted after {attempts} attempt(s): \
                         {progress_message}"
                    ),
                };
                let updated = store
                    .fail_claimed_operation(FailClaimedPublicOperation {
                        claim_token,
                        operation_id,
                        progress_message,
                        terminal_error_code: terminal_error_code.clone(),
                        terminal_error_json,
                    })
                    .await?;
                if updated {
                    if let Some(elapsed) = deferral_exhausted_after {
                        tracing::warn!(
                            operation_id = %operation_id,
                            api_product = api_product_label,
                            kind = kind.as_str(),
                            deferred_retry_window_ms = DEFERRED_RETRY_WINDOW.as_millis(),
                            elapsed_since_first_claim_ms = elapsed.as_millis(),
                            "public operation exhausted its retry deferral window"
                        );
                    }
                    report.failed += 1;
                    pplx_space_metrics::operations::failed(
                        api_product_label,
                        kind.as_str(),
                        terminal_error_code.as_str(),
                        executor,
                    );
                    pplx_space_metrics::operations::completed_by(
                        api_product_label,
                        kind.as_str(),
                        OperationState::Failed.as_str(),
                        executor,
                    );
                }
                updated.then_some(ExecutionOutcome::Failed)
            } else {
                let retry_hint = result
                    .retry_after
                    .filter(|retry_after| !retry_after.is_zero())
                    .unwrap_or(config.retry_after);
                let retry_after = match deferred_for {
                    Some(deferred_for) => {
                        deferred_retry_delay(operation_id, deferred_for, retry_hint)
                    }
                    None => retry_hint,
                };
                let released = store
                    .release_claimed_operation(ReleaseClaimedPublicOperation {
                        claim_token,
                        operation_id,
                        progress_message,
                        retry_after,
                        // Expected workflow progress and activation admission
                        // waits do not consume the transient-failure budget.
                        count_attempt: !preserve_retry_budget,
                        undo_first_start: false,
                        // A create a later delete supersedes ends rather than
                        // retrying, so the delete need not wait out its
                        // retries and the create never runs after it.
                        cancel_if_superseded: true,
                        carried_error: Some(with_carried_error(
                            serde_json::json!({ "last_progress_message": last_progress_message }),
                            result.terminal_error_json,
                        )),
                    })
                    .await?;
                match released {
                    ReleasedClaim::Requeued => {
                        report.retried += 1;
                        pplx_space_metrics::operations::requeued(
                            api_product_label,
                            kind.as_str(),
                            retry_reason,
                            executor,
                        );
                        Some(ExecutionOutcome::Requeued)
                    }
                    ReleasedClaim::Superseded { delete_id } => {
                        report.canceled += 1;
                        pplx_space_metrics::operations::completed_by(
                            api_product_label,
                            kind.as_str(),
                            OperationState::Canceled.as_str(),
                            executor,
                        );
                        tracing::info!(
                            operation_id = %operation_id,
                            delete_operation_id = %delete_id,
                            "public create canceled instead of retried: a later delete supersedes it"
                        );
                        Some(ExecutionOutcome::Canceled)
                    }
                    ReleasedClaim::Lost => None,
                }
            }
        }
    };

    let Some(outcome) = recorded else {
        record_lost_claim(&mut report, api_product_label, kind, executor, operation_id);
        return Ok((report, ExecutionOutcome::Lost));
    };
    Ok((report, outcome))
}

/// The error envelope of a requeue the driver fails: its own `evidence`, with
/// any fields of the error the requeue `carried` that the evidence lacks. The
/// carried code replaces the driver's own; see
/// [`PublicOperationExecutionResult`].
pub(crate) fn with_carried_error(
    mut evidence: serde_json::Value,
    carried: Option<serde_json::Value>,
) -> serde_json::Value {
    if let (Some(evidence), Some(serde_json::Value::Object(cause))) =
        (evidence.as_object_mut(), carried)
    {
        for (field, value) in cause {
            evidence.entry(field).or_insert(value);
        }
    }
    evidence
}

fn record_lost_claim(
    report: &mut TickReport,
    api_product_label: &str,
    kind: OperationKind,
    executor: OperationExecutor,
    operation_id: OperationId,
) {
    report.stale_claims += 1;
    pplx_space_metrics::operations::stale_claim(api_product_label, kind.as_str(), executor);
    tracing::warn!(
        operation_id = %operation_id,
        "public operation claim was lost before completion update"
    );
}

/// Expire up to `limit` queued Native M1 operations of
/// [`NATIVE_M1_EXPIRING_KINDS`] that passed the deadline without ever
/// starting: held back behind earlier work on their sandbox, or waiting for
/// a free slot. One first claimed past the deadline expires in
/// [`drive_claimed_operation`] instead. Returns how many expired.
pub async fn expire_overdue_queued_operations(
    store: &PublicOperationStore,
    config: OperationExecutionConfig,
    limit: u32,
) -> Result<usize, PublicOperationStoreError> {
    let Some(max_age) = config.m1_max_age else {
        return Ok(0);
    };
    let expired = store
        .expire_overdue_queued_operations(
            OverdueQueuedPublicOperations {
                api_product: ApiProduct::Native,
                kinds: NATIVE_M1_EXPIRING_KINDS,
                max_age,
                limit,
            },
            |operation| m1_expiry(operation.id, operation.kind, operation.attempts, max_age),
        )
        .await?;
    for operation in &expired {
        pplx_space_metrics::operations::completed(
            ApiProduct::Native.as_str(),
            operation.kind.as_str(),
            OperationState::Expired.as_str(),
        );
        tracing::warn!(
            operation_id = %operation.id,
            kind = operation.kind.as_str(),
            max_age_seconds = max_age.as_secs(),
            last_progress_message = operation.progress_message,
            "queued public operation expired: older than the M1 terminalization deadline"
        );
    }
    Ok(expired.len())
}

/// How often a running execution renews its claim: a third of
/// `claim_stale_after`, so two renewals can be lost before it goes stale.
pub fn claim_renewal_interval(claim_stale_after: Duration) -> Duration {
    (claim_stale_after / 3).max(Duration::from_millis(10))
}

/// The delay before re-driving an operation that deferred its retry wait:
/// the executor's `hint` plus an equal-jittered eighth of `deferred_for`, the
/// wall-clock time since its first claim. The jittered term is capped at a
/// minute, so the longest delay is `hint` plus 60 s. An operation first
/// claimed moments ago is retried within seconds; one first claimed eight
/// minutes or more ago, however long it has been deferring, is retried about
/// once a minute. A native delete waiting on a snapshot upload spaces its
/// workflow progress the same way.
pub fn deferred_retry_delay(
    operation_id: OperationId,
    deferred_for: Duration,
    hint: Duration,
) -> Duration {
    let half = (deferred_for / 8).min(DEFERRED_RETRY_MAX_SPACING) / 2;
    hint + half + half.mul_f64(retry_jitter_fraction(operation_id, deferred_for.as_secs()))
}

fn retry_jitter_fraction(operation_id: OperationId, seed: u64) -> f64 {
    use std::hash::{DefaultHasher, Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    operation_id.to_string().hash(&mut hasher);
    seed.hash(&mut hasher);
    // The top 53 bits fill an f64 mantissa exactly: a fraction in [0, 1).
    (hasher.finish() >> 11) as f64 / (1_u64 << 53) as f64
}

fn retry_budget_exhausted(attempts: i32, max_retry_attempts: i32) -> bool {
    attempts >= max_retry_attempts.max(1)
}

/// The Native kinds bound by the M1 deadline while they retry: the kinds
/// that expire unrun, and a delete.
fn is_native_m1_terminalized_kind(api_product: ApiProduct, kind: OperationKind) -> bool {
    expires_unrun(api_product, kind)
        || (api_product == ApiProduct::Native && kind == OperationKind::DeleteSandbox)
}

#[cfg(test)]
mod tests;

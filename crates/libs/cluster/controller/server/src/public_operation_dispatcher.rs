use std::time::{Duration, Instant};

use async_trait::async_trait;
use pplx_space_api::cluster_admission::{ClusterSandboxAdmission, ClusterSandboxAdmissionConfig};
use pplx_space_api::controllers::sandbox::SandboxController;
use pplx_space_api::controllers::substrate::Substrate;
use pplx_space_api::fork_operation_executor::ForkSandboxOperationExecutor;
use pplx_space_api::public_operations::{
    ApiProduct, ClaimPublicOperations, ClaimedPublicOperation, CompleteClaimedPublicOperation,
    FailClaimedPublicOperation, PublicOperationClaim, PublicOperationRequeueReason,
    PublicOperationStore, ReleaseClaimedPublicOperation,
};
use pplx_space_api::public_sandbox_operations::{
    CreateSandboxOperationExecutor, PublicOperationExecutionResult,
    SandboxLifecycleOperationExecutor, UpdateSandboxOperationExecutor,
};
use pplx_space_api::public_template_operations::DeleteTemplateOperationExecutor;
use pplx_space_api::tls_interception_exemptions::TlsInterceptionExemptions;
use pplx_space_common_core::types::time::format_simple_duration;
use pplx_space_feature_flags::FeatureFlagClient;
use pplx_space_public_api::{OperationKind, OperationState, PublicErrorCode};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::env_config;

/// How often the dispatcher looks for newly queued operations. The claim is
/// one indexed `FOR UPDATE SKIP LOCKED` statement that returns no rows when the
/// queue is empty, so a short tick keeps pickup latency low at little cost.
/// Pickup waits half a tick on average, which at 200 ms was a large share of
/// a warm-pool create's latency.
const DEFAULT_TICK: Duration = Duration::from_millis(25);
/// A failing claim fails on every tick; warn about it at most this often.
const CLAIM_FAILURE_WARN_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CLAIM_LIMIT: u32 = 64;
const DEFAULT_CLAIM_STALE_AFTER: Duration = Duration::from_secs(60);
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(5);
const DEFAULT_EXECUTION_CONCURRENCY: usize = 16;
const MAX_EXECUTION_CONCURRENCY: usize = 128;
const DEFAULT_MAX_RETRY_ATTEMPTS: i32 = 16;
const M1_MAX_AGE_ENV: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_M1_MAX_AGE_SECS";
const DEFAULT_M1_MAX_AGE: Duration = Duration::from_secs(1_800);

fn m1_deadline_message(max_age_seconds: u64) -> String {
    let max_age = format_simple_duration(Duration::from_secs(max_age_seconds));
    format!(
        "Operation did not reach a terminal result within {max_age}; \
         inspect the target state before retrying."
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub tick: Duration,
    pub claim_limit: u32,
    pub execution_concurrency: usize,
    pub claim_stale_after: Duration,
    pub retry_after: Duration,
    pub max_retry_attempts: i32,
    pub m1_max_age: Option<Duration>,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick: DEFAULT_TICK,
            claim_limit: DEFAULT_CLAIM_LIMIT,
            execution_concurrency: DEFAULT_EXECUTION_CONCURRENCY,
            claim_stale_after: DEFAULT_CLAIM_STALE_AFTER,
            retry_after: DEFAULT_RETRY_AFTER,
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            m1_max_age: Some(DEFAULT_M1_MAX_AGE),
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            // Deliberately not a fail-closed gate: this loop is what completes
            // queued create operations, so a malformed value keeps the default
            // (on) instead of leaving every create `queued` forever.
            enabled: config
                .truthy_default("SPACE_PUBLIC_OPERATION_DISPATCHER_ENABLED", default.enabled),
            tick: config
                .test_duration_secs("SPACE_PUBLIC_OPERATION_DISPATCHER_TICK_SECS", DEFAULT_TICK),
            claim_limit: config.test_bounded_u32(
                "SPACE_PUBLIC_OPERATION_DISPATCHER_CLAIM_LIMIT",
                DEFAULT_CLAIM_LIMIT,
                1024,
            ),
            // A deployment setting, unlike the test hooks around it: each
            // execution holds its slot for the whole operation, so this bounds
            // the dispatcher's throughput.
            execution_concurrency: config.bounded_usize(
                "SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY",
                DEFAULT_EXECUTION_CONCURRENCY,
                MAX_EXECUTION_CONCURRENCY,
            ),
            claim_stale_after: config.test_duration_secs(
                "SPACE_PUBLIC_OPERATION_DISPATCHER_CLAIM_STALE_SECS",
                DEFAULT_CLAIM_STALE_AFTER,
            ),
            retry_after: DEFAULT_RETRY_AFTER,
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            m1_max_age: config
                .test_optional_positive_duration_secs(
                    M1_MAX_AGE_ENV,
                    Some(pplx_space_common_core::types::PositiveDuration::new_panic(
                        DEFAULT_M1_MAX_AGE,
                    )),
                )
                .map(pplx_space_common_core::types::PositiveDuration::get),
        }
    }
}

#[derive(Clone, Copy)]
struct OperationExecutionConfig {
    claim_stale_after: Duration,
    retry_after: Duration,
    max_retry_attempts: i32,
    m1_max_age: Option<Duration>,
}

impl From<&ReconcilerConfig> for OperationExecutionConfig {
    fn from(config: &ReconcilerConfig) -> Self {
        Self {
            claim_stale_after: config.claim_stale_after,
            retry_after: config.retry_after,
            max_retry_attempts: config.max_retry_attempts,
            m1_max_age: config.m1_max_age,
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

#[async_trait]
pub trait PublicOperationExecutor: Send + Sync + 'static {
    fn supported_kinds(&self) -> &'static [OperationKind];

    async fn execute(
        &self,
        claim_token: &str,
        operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult;
}

#[derive(Clone)]
pub struct NativePublicOperationExecutor {
    /// The cluster activation limiter the executors below admit through. Held
    /// only so the caller has a limiter to start a liveness heartbeat from
    /// without constructing a second one (which would build a second, lazy
    /// Redis pool that nothing ever checks a connection out of). The heartbeat
    /// itself captures nothing from the limiter, so it does not depend on
    /// getting *this* instance.
    cluster_admission: ClusterSandboxAdmission,
    create_sandbox: CreateSandboxOperationExecutor,
    update_sandbox: UpdateSandboxOperationExecutor,
    sandbox_lifecycle: SandboxLifecycleOperationExecutor,
    delete_template: DeleteTemplateOperationExecutor,
    fork_sandbox: ForkSandboxOperationExecutor,
}

impl NativePublicOperationExecutor {
    pub fn new(
        pool: PgPool,
        substrate: Substrate,
        feature_flags: FeatureFlagClient,
        grant_audience: pplx_space_common_core::types::FederatedGrantAudience,
        tls_interception_exemptions: Option<TlsInterceptionExemptions>,
    ) -> Result<Self, pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error>
    {
        let route_ed25519_authority = Arc::new(
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority::load_required_from_env()?,
        );
        Ok(Self::with_cluster_admission_config(
            pool,
            substrate,
            ClusterSandboxAdmissionConfig::from_env(),
            feature_flags,
            route_ed25519_authority,
            grant_audience,
            tls_interception_exemptions,
        ))
    }

    fn with_cluster_admission_config(
        pool: PgPool,
        substrate: Substrate,
        admission_config: ClusterSandboxAdmissionConfig,
        feature_flags: FeatureFlagClient,
        route_ed25519_authority: Arc<
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
        >,
        grant_audience: pplx_space_common_core::types::FederatedGrantAudience,
        tls_interception_exemptions: Option<TlsInterceptionExemptions>,
    ) -> Self {
        // Absent an SDK key the injected client is disabled and the dynamic
        // gate defaults on, leaving the limiter env-var-gated exactly as
        // before.
        let cluster_admission = ClusterSandboxAdmission::with_config(
            substrate.clone(),
            admission_config,
            feature_flags,
        );
        let controller =
            SandboxController::with_cluster_admission(substrate.clone(), cluster_admission.clone())
                .with_route_ed25519_authority(Some(route_ed25519_authority))
                .with_federated_grant_audience(grant_audience)
                .with_tls_interception_exemptions(tls_interception_exemptions);
        Self {
            cluster_admission,
            create_sandbox: CreateSandboxOperationExecutor::new(pool.clone(), controller.clone()),
            update_sandbox: UpdateSandboxOperationExecutor::new(pool.clone(), controller.clone()),
            sandbox_lifecycle: SandboxLifecycleOperationExecutor::new(
                pool.clone(),
                controller.clone(),
            ),
            delete_template: DeleteTemplateOperationExecutor::new(pool.clone()),
            fork_sandbox: ForkSandboxOperationExecutor::new(pool, substrate, controller),
        }
    }

    /// The cluster activation limiter this executor admits through. The cluster
    /// controller writes `cluster.sandbox_admission.rate.decision` from here,
    /// so this is the handle that makes its liveness heartbeat attributable to
    /// a limiter this process actually built.
    pub fn cluster_admission(&self) -> &ClusterSandboxAdmission {
        &self.cluster_admission
    }
}

const NATIVE_PUBLIC_OPERATION_KINDS: &[OperationKind] = &[
    OperationKind::CreateSandbox,
    OperationKind::UpdateSandbox,
    OperationKind::DeleteSandbox,
    OperationKind::StartSandbox,
    OperationKind::StopSandbox,
    OperationKind::PauseSandbox,
    OperationKind::ResumeSandbox,
    OperationKind::SuspendSandbox,
    OperationKind::RestoreSandbox,
    OperationKind::WakeSandbox,
    OperationKind::KillSandbox,
    OperationKind::CheckpointSandbox,
    OperationKind::EnableSandboxSnapshots,
    OperationKind::DisableSandboxSnapshots,
    OperationKind::RollbackSandbox,
    OperationKind::DeleteTemplate,
    OperationKind::ForkSandbox,
];

#[async_trait]
impl PublicOperationExecutor for NativePublicOperationExecutor {
    fn supported_kinds(&self) -> &'static [OperationKind] {
        NATIVE_PUBLIC_OPERATION_KINDS
    }

    async fn execute(
        &self,
        claim_token: &str,
        operation: ClaimedPublicOperation,
    ) -> OperationExecutionResult {
        match operation.kind {
            OperationKind::CreateSandbox => self.create_sandbox.execute(&operation).await.into(),
            OperationKind::UpdateSandbox => self.update_sandbox.execute(&operation).await.into(),
            OperationKind::DeleteTemplate => self.delete_template.execute(&operation).await.into(),
            OperationKind::ForkSandbox => self
                .fork_sandbox
                .execute(claim_token, &operation)
                .await
                .into(),
            OperationKind::DeleteSandbox
            | OperationKind::StartSandbox
            | OperationKind::StopSandbox
            | OperationKind::PauseSandbox
            | OperationKind::ResumeSandbox
            | OperationKind::SuspendSandbox
            | OperationKind::RestoreSandbox
            | OperationKind::WakeSandbox
            | OperationKind::KillSandbox
            | OperationKind::CheckpointSandbox
            | OperationKind::EnableSandboxSnapshots
            | OperationKind::DisableSandboxSnapshots
            | OperationKind::RollbackSandbox => {
                self.sandbox_lifecycle.execute(&operation).await.into()
            }
            kind => OperationExecutionResult {
                state: OperationState::Failed,
                progress_message: format!("operation kind {kind} is not handled"),
                result_json: None,
                terminal_error_code: Some(PublicErrorCode::Internal),
                terminal_error_json: Some(serde_json::json!({
                    "operation_id": operation.id.to_string(),
                    "kind": kind.as_str(),
                    "reason": "unsupported_operation_kind",
                })),
                retry_after: None,
                requeue_reason: PublicOperationRequeueReason::TransientFailure,
            },
        }
    }
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

pub async fn run<E>(
    config: ReconcilerConfig,
    store: PublicOperationStore,
    executor: E,
    shutdown_token: CancellationToken,
) where
    E: PublicOperationExecutor + Clone,
{
    if !config.enabled {
        tracing::info!(
            "public-operation-dispatcher is disabled \
             (SPACE_PUBLIC_OPERATION_DISPATCHER_ENABLED=false)"
        );
        shutdown_token.cancelled().await;
        return;
    }

    if executor.supported_kinds().is_empty() {
        tracing::info!(
            "public-operation-dispatcher is enabled but has no supported operation kinds"
        );
    }
    // The effective width, after the ceiling clamp. Nothing else exposes it,
    // so without this line an over-ceiling setting looks like it took effect.
    tracing::info!(
        execution_concurrency = config.execution_concurrency,
        "starting public-operation-dispatcher"
    );

    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;

    // Executions run independently of the claim loop: a slow operation holds
    // one of `execution_concurrency` slots but does not delay pickup of newly
    // queued operations into the remaining slots.
    let concurrency = config.execution_concurrency.max(1);
    let execution_config = OperationExecutionConfig::from(&config);
    let mut in_flight = JoinSet::new();
    let mut last_claim_failure_warn: Option<Instant> = None;
    let mut suppressed_claim_failures: u64 = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let free = concurrency.saturating_sub(in_flight.len());
                if free == 0 {
                    continue;
                }
                let started = Instant::now();
                match claim_up_to(&config, &store, &executor, free).await {
                    Ok(claim) => {
                        let claimed = claim.operations.len();
                        for operation in claim.operations {
                            let span = execution_span(&operation);
                            in_flight.spawn(
                                execute_claimed_operation(
                                    store.clone(),
                                    executor.clone(),
                                    claim.claim_token.clone(),
                                    execution_config,
                                    operation,
                                )
                                .instrument(span),
                            );
                        }
                        if claimed > 0 {
                            tracing::debug!(
                                claimed,
                                in_flight = in_flight.len(),
                                elapsed_ms = started.elapsed().as_millis(),
                                "public-operation-dispatcher claimed operations"
                            );
                        }
                    }
                    Err(error) => {
                        if last_claim_failure_warn
                            .is_none_or(|at| at.elapsed() >= CLAIM_FAILURE_WARN_INTERVAL)
                        {
                            tracing::warn!(
                                error = %error,
                                suppressed = suppressed_claim_failures,
                                "public-operation-dispatcher claim failed"
                            );
                            last_claim_failure_warn = Some(Instant::now());
                            suppressed_claim_failures = 0;
                        } else {
                            suppressed_claim_failures += 1;
                        }
                    }
                }
            }
            Some(joined) = in_flight.join_next(), if !in_flight.is_empty() => {
                log_execution_outcome(joined);
            }
            _ = shutdown_token.cancelled() => break,
        }
    }

    // Stop claiming, but let claimed executions finish and record their
    // outcome, matching the previous behavior of completing the in-progress
    // batch before exiting.
    while let Some(joined) = in_flight.join_next().await {
        log_execution_outcome(joined);
    }
}

fn log_execution_outcome(
    joined: Result<
        Result<TickReport, pplx_space_api::public_operations::PublicOperationStoreError>,
        tokio::task::JoinError,
    >,
) {
    match joined {
        Ok(Ok(report)) => tracing::debug!(
            succeeded = report.succeeded,
            failed = report.failed,
            retried = report.retried,
            stale_claims = report.stale_claims,
            "public-operation-dispatcher execution completed"
        ),
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "public-operation-dispatcher execution failed");
        }
        // The claim is no longer renewed, so the operation becomes claimable
        // again once `claim_stale_after` elapses.
        Err(error) => {
            tracing::error!(error = %error, "public-operation-dispatcher execution task panicked");
        }
    }
}

async fn claim_up_to<E>(
    config: &ReconcilerConfig,
    store: &PublicOperationStore,
    executor: &E,
    free_slots: usize,
) -> Result<PublicOperationClaim, pplx_space_api::public_operations::PublicOperationStoreError>
where
    E: PublicOperationExecutor,
{
    let limit = u32::try_from(free_slots)
        .unwrap_or(u32::MAX)
        .min(config.claim_limit);
    store
        .claim_operations(ClaimPublicOperations {
            limit,
            claim_stale_after: config.claim_stale_after,
            claimable_kinds: executor.supported_kinds(),
        })
        .await
}

/// Claim one batch and run it to completion. [`run`] does not use this: it
/// keeps a bounded in-flight set so slow executions never block pickup. This
/// remains as a deterministic single pass for tests.
#[cfg(test)]
pub async fn tick<E>(
    config: &ReconcilerConfig,
    store: &PublicOperationStore,
    executor: &E,
) -> Result<TickReport, pplx_space_api::public_operations::PublicOperationStoreError>
where
    E: PublicOperationExecutor + Clone,
{
    use futures_util::{StreamExt, stream};

    let claim = store
        .claim_operations(ClaimPublicOperations {
            limit: config.claim_limit,
            claim_stale_after: config.claim_stale_after,
            claimable_kinds: executor.supported_kinds(),
        })
        .await?;

    let mut report = TickReport {
        claimed: claim.operations.len(),
        ..TickReport::default()
    };

    let execution_config = OperationExecutionConfig::from(config);
    let mut executions = stream::iter(claim.operations.into_iter().map(|operation| {
        let span = execution_span(&operation);
        execute_claimed_operation(
            store.clone(),
            executor.clone(),
            claim.claim_token.clone(),
            execution_config,
            operation,
        )
        .instrument(span)
    }))
    .buffer_unordered(config.execution_concurrency.max(1));

    while let Some(operation_report) = executions.next().await {
        report.merge(operation_report?);
    }

    Ok(report)
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
fn execution_span(operation: &ClaimedPublicOperation) -> tracing::Span {
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
        pplx_space_common_svc::observability::trace_propagation::link_root_to_origins(
            &span,
            [traceparent],
        );
    }
    span
}

async fn execute_claimed_operation<E>(
    store: PublicOperationStore,
    executor: E,
    claim_token: String,
    config: OperationExecutionConfig,
    operation: ClaimedPublicOperation,
) -> Result<TickReport, pplx_space_api::public_operations::PublicOperationStoreError>
where
    E: PublicOperationExecutor,
{
    let operation_id = operation.id;
    let api_product = operation.api_product;
    let api_product_label = api_product.as_str();
    let kind = operation.kind;
    let attempts = operation.attempts;
    pplx_space_metrics::operations::claimed(api_product_label, kind.as_str());
    let renew_every = (config.claim_stale_after / 3).max(Duration::from_millis(10));
    let mut renewal = tokio::time::interval(renew_every);
    renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
    renewal.tick().await;
    let execution_claim_token = claim_token.clone();
    let execution = executor.execute(&execution_claim_token, operation.clone());
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
                pplx_space_metrics::operations::completed(
                    api_product_label,
                    kind.as_str(),
                    OperationState::Succeeded.as_str(),
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
                pplx_space_metrics::operations::completed(
                    api_product_label,
                    kind.as_str(),
                    OperationState::Failed.as_str(),
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
                    pplx_space_metrics::operations::completed(
                        api_product_label,
                        kind.as_str(),
                        OperationState::Failed.as_str(),
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
                    pplx_space_metrics::operations::completed(
                        api_product_label,
                        kind.as_str(),
                        OperationState::Failed.as_str(),
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

fn is_native_m1_terminalized_kind(api_product: ApiProduct, kind: OperationKind) -> bool {
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

#[cfg(test)]
impl TickReport {
    fn merge(&mut self, other: Self) {
        self.claimed += other.claimed;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.retried += other.retried;
        self.stale_claims += other.stale_claims;
    }
}

#[cfg(test)]
mod tests;

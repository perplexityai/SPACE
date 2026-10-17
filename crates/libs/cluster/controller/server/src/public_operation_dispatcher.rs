use std::time::{Duration, Instant};

use async_trait::async_trait;
use pplx_space_api::cluster_admission::{ClusterSandboxAdmission, ClusterSandboxAdmissionConfig};
use pplx_space_api::controllers::sandbox::SandboxController;
use pplx_space_api::controllers::substrate::Substrate;
use pplx_space_api::egress_profiles::EgressProfiles;
use pplx_space_api::fork_operation_executor::ForkSandboxOperationExecutor;
use pplx_space_api::public_operation_driver::{
    DEFAULT_CLAIM_STALE_AFTER, DEFAULT_M1_MAX_AGE, DEFAULT_MAX_RETRY_ATTEMPTS, DEFAULT_RETRY_AFTER,
    OperationExecutionConfig, OperationExecutor, drive_claimed_operation, execution_span,
};
pub use pplx_space_api::public_operation_driver::{OperationExecutionResult, TickReport};
use pplx_space_api::public_operations::{
    ClaimPublicOperations, ClaimedPublicOperation, PublicOperationClaim,
    PublicOperationRequeueReason, PublicOperationStore,
};
use pplx_space_api::public_sandbox_operations::{
    CreateSandboxOperationExecutor, SandboxLifecycleOperationExecutor,
    UpdateSandboxOperationExecutor,
};
use pplx_space_api::public_template_operations::DeleteTemplateOperationExecutor;
use pplx_space_api::tls_interception_exemptions::TlsInterceptionExemptions;
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
const DEFAULT_EXECUTION_CONCURRENCY: usize = 16;
const MAX_EXECUTION_CONCURRENCY: usize = 128;
const M1_MAX_AGE_ENV: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_M1_MAX_AGE_SECS";

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

fn execution_config(config: &ReconcilerConfig) -> OperationExecutionConfig {
    OperationExecutionConfig {
        claim_stale_after: config.claim_stale_after,
        retry_after: config.retry_after,
        max_retry_attempts: config.max_retry_attempts,
        m1_max_age: config.m1_max_age,
    }
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
    /// Match the native deadline the dispatcher's driver applies (its
    /// `m1_max_age`), so a create waiting on its row knows whether that
    /// deadline will end the wait.
    pub fn with_native_deadline(mut self, m1_max_age: Option<Duration>) -> Self {
        self.create_sandbox = self.create_sandbox.with_native_deadline(m1_max_age);
        self
    }

    pub fn new(
        pool: PgPool,
        substrate: Substrate,
        feature_flags: FeatureFlagClient,
        grant_audience: pplx_space_common_core::types::FederatedGrantAudience,
        tls_interception_exemptions: Option<TlsInterceptionExemptions>,
        egress_profiles: EgressProfiles,
    ) -> Result<Self, pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error>
    {
        let route_ed25519_authority = Arc::new(
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority::load_required_from_env()?,
        );
        Ok(Self::with_cluster_admission(
            pool,
            substrate.clone(),
            ClusterSandboxAdmission::with_config(
                substrate,
                ClusterSandboxAdmissionConfig::from_env(),
                feature_flags,
            ),
            route_ed25519_authority,
            grant_audience,
            tls_interception_exemptions,
            egress_profiles,
        ))
    }

    fn with_cluster_admission(
        pool: PgPool,
        substrate: Substrate,
        cluster_admission: ClusterSandboxAdmission,
        route_ed25519_authority: Arc<
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
        >,
        grant_audience: pplx_space_common_core::types::FederatedGrantAudience,
        tls_interception_exemptions: Option<TlsInterceptionExemptions>,
        egress_profiles: EgressProfiles,
    ) -> Self {
        let controller =
            SandboxController::with_cluster_admission(substrate.clone(), cluster_admission.clone())
                .with_route_ed25519_authority(Some(route_ed25519_authority))
                .with_federated_grant_audience(grant_audience)
                .with_tls_interception_exemptions(tls_interception_exemptions)
                .with_egress_profiles(egress_profiles);
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
            OperationKind::CreateSandbox => self
                .create_sandbox
                .execute(claim_token, &operation)
                .await
                .into(),
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
    let execution_config = execution_config(&config);
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

    let execution_config = execution_config(config);
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
    let execution_claim_token = claim_token.clone();
    let execution_operation = operation.clone();
    drive_claimed_operation(
        store,
        claim_token,
        config,
        operation,
        OperationExecutor::Dispatcher,
        async move {
            executor
                .execute(&execution_claim_token, execution_operation)
                .await
        },
    )
    .await
}

#[cfg(test)]
mod tests;

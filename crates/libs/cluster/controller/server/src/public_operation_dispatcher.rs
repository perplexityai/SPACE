use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pplx_space_api::cluster_admission::{ClusterSandboxAdmission, ClusterSandboxAdmissionConfig};
use pplx_space_api::controllers::sandbox::SandboxController;
use pplx_space_api::controllers::substrate::Substrate;
use pplx_space_api::egress_profiles::EgressProfiles;
use pplx_space_api::fork_operation_executor::ForkSandboxOperationExecutor;
use pplx_space_api::public_operation_driver::{
    DEFAULT_CLAIM_STALE_AFTER, DEFAULT_M1_MAX_AGE, DEFAULT_MAX_RETRY_ATTEMPTS, DEFAULT_RETRY_AFTER,
    OperationExecutionConfig, OperationExecutor, claim_renewal_interval, drive_claimed_operation,
    execution_span, expire_overdue_queued_operations,
};
pub use pplx_space_api::public_operation_driver::{OperationExecutionResult, TickReport};
use pplx_space_api::public_operations::{
    ClaimPublicOperations, ClaimedPublicOperation, PublicOperationClaim,
    PublicOperationRequeueReason, PublicOperationStore, ReleaseClaimedPublicOperation,
};
use pplx_space_api::public_sandbox_operations::{
    CreateSandboxOperationExecutor, SandboxLifecycleOperationExecutor,
    UpdateSandboxOperationExecutor,
};
use pplx_space_api::public_template_operations::DeleteTemplateOperationExecutor;
use pplx_space_api::retry_deferral::RetryDeferral;
use pplx_space_api::tls_interception_exemptions::TlsInterceptionExemptions;
use pplx_space_feature_flags::FeatureFlagClient;
use pplx_space_public_api::{OperationId, OperationKind, OperationState, PublicErrorCode};
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
/// How often the loop expires queued operations past their age deadline that
/// nothing claims, such as one held back behind its sandbox's running work.
/// A claimed operation is checked as it is claimed, so this bounds only how
/// long such an operation can read `queued` past the deadline.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CLAIM_LIMIT: u32 = 64;
const DEFAULT_EXECUTION_CONCURRENCY: usize = 16;
const MAX_EXECUTION_CONCURRENCY: usize = 128;
const M1_MAX_AGE_ENV: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_M1_MAX_AGE_SECS";
/// How soon an operation handed back because its key is already executing
/// in this process becomes claimable again.
const BUSY_KEY_RETRY_AFTER: Duration = Duration::from_secs(1);
const KEYED_CLAIM_ENABLED_ENV: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_KEYED_CLAIM_ENABLED";
const RETRY_DEFERRAL_ENABLED_ENV: &str = "SPACE_PUBLIC_OPERATION_DISPATCHER_RETRY_DEFERRAL_ENABLED";

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
    /// Kill switch for the keyed work queue: off restores the unkeyed claim.
    pub keyed_claim_enabled: bool,
    /// Kill switch for retry deferral, independent of the keyed claim: off
    /// keeps every retry wait inside the execution's slot.
    pub retry_deferral_enabled: bool,
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
            keyed_claim_enabled: true,
            retry_deferral_enabled: true,
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
            // A deployment setting, unlike the test hooks around it: it caps
            // operations executing at once (see `retry_deferral` for waits).
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
            keyed_claim_enabled: config
                .truthy_default(KEYED_CLAIM_ENABLED_ENV, default.keyed_claim_enabled),
            retry_deferral_enabled: config
                .truthy_default(RETRY_DEFERRAL_ENABLED_ENV, default.retry_deferral_enabled),
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
        keyed_claim_enabled = config.keyed_claim_enabled,
        retry_deferral_enabled = config.retry_deferral_enabled,
        "starting public-operation-dispatcher"
    );

    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;

    // Executions run independently of the claim loop: a slow operation holds
    // one of `execution_concurrency` slots but does not delay pickup into the
    // others. `busy_keys` is this process's view of the claim's one-per-
    // sandbox rule: a claim that would overlap a local execution of its key
    // is handed back, beside the loop, instead of running twice.
    let concurrency = config.execution_concurrency.max(1);
    let execution_config = execution_config(&config);
    let mut in_flight = JoinSet::new();
    let mut busy_keys: HashMap<String, OperationId> = HashMap::new();
    let mut task_keys: HashMap<tokio::task::Id, String> = HashMap::new();
    let mut hand_backs = JoinSet::new();
    // Reclaiming an operation still executing here means its execution lost
    // the claim and notices only at its next renewal. Any other collision is
    // an execution whose result reached the store before its task returned.
    let stale_reclaim_retry_after = claim_renewal_interval(execution_config.claim_stale_after);
    let mut last_claim_failure_warn: Option<Instant> = None;
    let mut suppressed_claim_failures: u64 = 0;
    let mut last_expiry_sweep: Option<Instant> = None;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Free finished executions' slots and keys before claiming.
                while let Some(joined) = in_flight.try_join_next_with_id() {
                    finish_execution(joined, &mut busy_keys, &mut task_keys);
                }
                pplx_space_metrics::operations::dispatcher_slots(in_flight.len(), concurrency);
                if last_expiry_sweep.is_none_or(|at| at.elapsed() >= EXPIRY_SWEEP_INTERVAL) {
                    last_expiry_sweep = Some(Instant::now());
                    if let Err(error) = expire_overdue_queued_operations(
                        &store,
                        execution_config,
                        config.claim_limit,
                    )
                    .await
                    {
                        tracing::warn!(
                            error = %error,
                            "public-operation-dispatcher could not expire overdue queued operations"
                        );
                    }
                }
                let free = concurrency.saturating_sub(in_flight.len());
                if free == 0 {
                    continue;
                }
                #[cfg(test)]
                tests::before_claim().await;
                let started = Instant::now();
                match claim_up_to(&config, &store, &executor, free).await {
                    Ok(claim) => {
                        // An execution that committed while the claim ran
                        // must not make its successor look busy.
                        while let Some(joined) = in_flight.try_join_next_with_id() {
                            finish_execution(joined, &mut busy_keys, &mut task_keys);
                        }
                        let claimed = claim.operations.len();
                        for operation in claim.operations {
                            let key = config
                                .keyed_claim_enabled
                                .then(|| operation.work_key().map(str::to_owned))
                                .flatten();
                            if let Some(key) = &key
                                && let Some(&running) = busy_keys.get(key)
                            {
                                let retry_after = if running == operation.id {
                                    stale_reclaim_retry_after
                                } else {
                                    BUSY_KEY_RETRY_AFTER
                                };
                                hand_backs.spawn(hand_back_busy_key(
                                    store.clone(),
                                    claim.claim_token.clone(),
                                    operation,
                                    retry_after,
                                ));
                                continue;
                            }
                            if let Some(key) = &key {
                                busy_keys.insert(key.clone(), operation.id);
                            }
                            let span = execution_span(&operation);
                            let execution = execute_claimed_operation(
                                store.clone(),
                                executor.clone(),
                                claim.claim_token.clone(),
                                execution_config,
                                config.retry_deferral_enabled,
                                operation,
                            )
                            .instrument(span);
                            let task = in_flight.spawn(execution);
                            if let Some(key) = key {
                                task_keys.insert(task.id(), key);
                            }
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
            Some(joined) = in_flight.join_next_with_id(), if !in_flight.is_empty() => {
                finish_execution(joined, &mut busy_keys, &mut task_keys);
            }
            Some(_) = hand_backs.join_next(), if !hand_backs.is_empty() => {}
            _ = shutdown_token.cancelled() => break,
        }
    }

    // Stop claiming, but let claimed executions finish and record their
    // outcome, matching the previous behavior of completing the in-progress
    // batch before exiting.
    while let Some(joined) = in_flight.join_next_with_id().await {
        finish_execution(joined, &mut busy_keys, &mut task_keys);
    }
    while hand_backs.join_next().await.is_some() {}
}

type ExecutionOutcome =
    Result<TickReport, pplx_space_api::public_operations::PublicOperationStoreError>;

/// Frees the finished execution's key, whether it returned or panicked: a
/// panicked execution is gone, and the claim it stopped renewing goes stale
/// so the claim query recovers the operation in its key's order.
fn finish_execution(
    joined: Result<(tokio::task::Id, ExecutionOutcome), tokio::task::JoinError>,
    busy_keys: &mut HashMap<String, OperationId>,
    task_keys: &mut HashMap<tokio::task::Id, String>,
) {
    let task_id = match &joined {
        Ok((task_id, _)) => *task_id,
        Err(error) => error.id(),
    };
    if let Some(key) = task_keys.remove(&task_id) {
        busy_keys.remove(&key);
    }
    log_execution_outcome(joined.map(|(_, outcome)| outcome));
}

/// Requeue an operation this process cannot start because another of its
/// key's operations is still executing here, without charging an attempt.
async fn hand_back_busy_key(
    store: PublicOperationStore,
    claim_token: String,
    operation: ClaimedPublicOperation,
    retry_after: Duration,
) {
    tracing::warn!(
        operation_id = %operation.id,
        kind = operation.kind.as_str(),
        sandbox_id = operation.sandbox_id(),
        "public-operation-dispatcher claimed an operation whose sandbox is still executing \
         locally; handing it back"
    );
    if let Err(error) = store
        .release_claimed_operation(ReleaseClaimedPublicOperation {
            claim_token,
            operation_id: operation.id,
            progress_message: "waiting for an earlier operation on the same sandbox".to_owned(),
            retry_after,
            count_attempt: false,
            undo_first_start: operation.is_first_start(),
        })
        .await
    {
        tracing::warn!(
            operation_id = %operation.id,
            error = %error,
            "public-operation-dispatcher could not hand back a busy-key claim; \
             it becomes claimable once the claim goes stale"
        );
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
    let input = ClaimPublicOperations {
        limit,
        claim_stale_after: config.claim_stale_after,
        claimable_kinds: executor.supported_kinds(),
    };
    if config.keyed_claim_enabled {
        store.claim_operations(input).await
    } else {
        store.claim_operations_unkeyed(input).await
    }
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

    let claim = claim_up_to(config, store, executor, usize::MAX).await?;

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
            config.retry_deferral_enabled,
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
    defer_retry_waits: bool,
    operation: ClaimedPublicOperation,
) -> Result<TickReport, pplx_space_api::public_operations::PublicOperationStoreError>
where
    E: PublicOperationExecutor,
{
    let execution_claim_token = claim_token.clone();
    let execution_operation = operation.clone();
    let deferral = defer_retry_waits.then(RetryDeferral::new);
    let recorded = deferral.clone();
    let execution = pplx_space_api::retry_deferral::scope(deferral, async move {
        executor
            .execute(&execution_claim_token, execution_operation)
            .await
    });
    drive_claimed_operation(
        store,
        claim_token,
        config,
        operation,
        OperationExecutor::Dispatcher,
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

#[cfg(test)]
mod tests;

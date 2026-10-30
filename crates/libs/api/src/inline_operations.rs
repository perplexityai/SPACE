//! Executing claimed public operations inline, in the API process that
//! admitted them.
//!
//! A route records its operation durably and, when admission claims it for
//! this process, runs it here instead of leaving it to the cluster-controller
//! dispatcher. The execution goes through the dispatcher's driver
//! ([`drive_deferrable_operation`]) under the same [`OperationExecutionConfig`],
//! so it renews its claim, defers retry waits and records its outcome exactly
//! as a dispatcher execution does. If this process dies, the claim goes stale
//! and the dispatcher re-drives the same operation.
//!
//! [`InlineOperations`] bounds that work per process: a fixed number of
//! permits, the API's task tracker (shutdown waits for executions within its
//! grace period), and no new work once the process drains or when it has no
//! route-signing authority to execute with. A route that cannot get a permit
//! records its operation queued for the dispatcher, exactly as it would with
//! inline execution off. Across processes, [`InlineOrgLimit`] bounds what one
//! organization runs inline; admission checks it as it claims.
//!
//! [`InlineLifecycle`] offers the lifecycle operations callers wait on
//! (wakes, resumes and starts) to the runner, under the env switches and the
//! per-organization `space-native-inline-operations` flag.

use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pplx_space_common_core::types::OrgId;
use pplx_space_common_svc::DrainSignal;
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_NATIVE_INLINE_OPERATIONS_FLAG};
use pplx_space_metrics::operations::{InlineAdmissionPath, InlineAdmissionReason};
use pplx_space_public_api::OperationKind;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument as _;

use crate::public_operation_driver::{
    OperationExecutionConfig, OperationExecutionResult, OperationExecutor,
    drive_deferrable_operation, execution_span,
};
use crate::public_operations::{
    ClaimedPublicOperation, CreateInlinePublicOperationResult, CreatePublicOperationResult,
    InlineOrgLimit, InlineQueueReason, PublicOperationStore,
};
use crate::public_sandbox_operations::{
    CreateSandboxOperationExecutor, SandboxLifecycleOperationExecutor,
};

/// Env master for executing lifecycle operations inline.
pub const INLINE_OPERATIONS_ENABLED_ENV: &str = "SPACE_NATIVE_INLINE_OPERATIONS_ENABLED";
/// Comma-separated operation kinds the master enables inline.
pub const INLINE_KINDS_ENV: &str = "SPACE_NATIVE_INLINE_KINDS";
/// Inline executions one API process runs at once; `0` turns inline
/// execution off entirely, including warm-pool creates.
pub const INLINE_MAX_CONCURRENT_ENV: &str = "SPACE_NATIVE_INLINE_MAX_CONCURRENT";
const DEFAULT_INLINE_MAX_CONCURRENT: usize = 128;
const MAX_INLINE_MAX_CONCURRENT: usize = 1024;
/// How often [`InlineOperations::report_slots`] samples the in-flight count.
/// The metrics agent keeps a gauge's last value per flush, so a sample is at
/// most this old when it is sent.
const SLOT_REPORT_INTERVAL: Duration = Duration::from_secs(1);
/// Inline executions one organization may hold at once across every API
/// process; unset or `0` sets no limit.
pub const INLINE_ORG_LIMIT_ENV: &str = "SPACE_NATIVE_INLINE_ORG_LIMIT";

/// The kinds `SPACE_NATIVE_INLINE_KINDS` may name: wakes, which callers wait
/// on, and creates. Everything else stays on the dispatcher.
pub const INLINE_CAPABLE_KINDS: &[OperationKind] = &[
    OperationKind::CreateSandbox,
    OperationKind::WakeSandbox,
    OperationKind::ResumeSandbox,
    OperationKind::StartSandbox,
];

/// The environment's inline switches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineOperationsConfig {
    /// `SPACE_NATIVE_INLINE_OPERATIONS_ENABLED`, off by default.
    pub enabled: bool,
    /// `SPACE_NATIVE_INLINE_KINDS`, empty by default.
    pub kinds: Vec<OperationKind>,
    /// `SPACE_NATIVE_INLINE_MAX_CONCURRENT`.
    pub max_concurrent: usize,
    /// `SPACE_NATIVE_INLINE_ORG_LIMIT`, `None` (no limit) by default.
    pub org_limit: Option<NonZeroU32>,
}

impl Default for InlineOperationsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            kinds: Vec::new(),
            max_concurrent: DEFAULT_INLINE_MAX_CONCURRENT,
            org_limit: None,
        }
    }
}

impl InlineOperationsConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    /// A kind outside [`INLINE_CAPABLE_KINDS`], or not a kind at all, is
    /// left out with a warning. A malformed limit keeps the default, and one
    /// above 1024 is capped. A malformed organization limit sets none, with
    /// a warning.
    pub fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let default = Self::default();
        let mut kinds = Vec::new();
        for name in env
            .var(INLINE_KINDS_ENV)
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            match name.parse::<OperationKind>() {
                Ok(kind) if INLINE_CAPABLE_KINDS.contains(&kind) => {
                    if !kinds.contains(&kind) {
                        kinds.push(kind);
                    }
                }
                _ => tracing::warn!(
                    kind = name,
                    "{INLINE_KINDS_ENV} names a kind that cannot run inline; it stays on the dispatcher"
                ),
            }
        }
        Self {
            enabled: pplx_space_common_core::env::env_flag(
                env.var(INLINE_OPERATIONS_ENABLED_ENV).ok().as_deref(),
                default.enabled,
            ),
            kinds,
            max_concurrent: env
                .var(INLINE_MAX_CONCURRENT_ENV)
                .ok()
                .and_then(|raw| raw.trim().parse::<usize>().ok())
                .map_or(default.max_concurrent, |limit| {
                    limit.min(MAX_INLINE_MAX_CONCURRENT)
                }),
            org_limit: env.var(INLINE_ORG_LIMIT_ENV).ok().and_then(|raw| {
                let raw = raw.trim();
                if raw.is_empty() {
                    return None;
                }
                match raw.parse::<u32>() {
                    Ok(limit) => NonZeroU32::new(limit),
                    Err(_) => {
                        tracing::warn!(
                            value = raw,
                            "{INLINE_ORG_LIMIT_ENV} is not a number; no organization limit is set"
                        );
                        None
                    }
                }
            }),
        }
    }
}

/// Why a route left an operation queued instead of running it inline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineRefusal {
    /// `SPACE_NATIVE_INLINE_MAX_CONCURRENT` is `0`.
    Disabled,
    /// The process has no route-signing authority, which the executors
    /// need to reach sandboxes.
    RouteSigningMissing,
    /// The process is shutting down.
    Draining,
    /// Every permit is held.
    Saturated,
    /// Other work for the operation's sandbox is queued or running.
    Busy,
    /// The organization is at its [`InlineOrgLimit`].
    OrgLimit,
    /// An idempotent replay, which records nothing new.
    Replay,
    /// The request joined an operation already queued or running, which
    /// records nothing new.
    Joined,
}

impl From<InlineQueueReason> for InlineRefusal {
    fn from(reason: InlineQueueReason) -> Self {
        match reason {
            InlineQueueReason::Busy => Self::Busy,
            InlineQueueReason::OrgLimit => Self::OrgLimit,
        }
    }
}

impl InlineRefusal {
    const fn reason(self) -> InlineAdmissionReason {
        match self {
            Self::Disabled => InlineAdmissionReason::Disabled,
            Self::RouteSigningMissing => InlineAdmissionReason::RouteSigningMissing,
            Self::Draining => InlineAdmissionReason::Draining,
            Self::Saturated => InlineAdmissionReason::Saturated,
            Self::Busy => InlineAdmissionReason::Busy,
            Self::OrgLimit => InlineAdmissionReason::OrgLimit,
            Self::Replay => InlineAdmissionReason::Replay,
            Self::Joined => InlineAdmissionReason::Joined,
        }
    }
}

/// One of the process's inline permits, taken before a route claims its
/// operation, and held by the execution until it ends. Dropping it unused
/// (the operation was queued after all) frees it.
pub struct InlinePermit {
    path: InlineAdmissionPath,
    _permit: OwnedSemaphorePermit,
}

/// This process's inline execution: its switches, permits and task tracker.
#[derive(Clone)]
pub struct InlineOperations {
    inner: Arc<Inner>,
}

struct Inner {
    config: InlineOperationsConfig,
    route_signing: bool,
    permits: Arc<Semaphore>,
    in_flight: AtomicUsize,
    tasks: TaskTracker,
    drain_signal: DrainSignal,
    execution: OperationExecutionConfig,
}

impl InlineOperations {
    /// Inline execution under `config`, spawned on `tasks`. Without
    /// `route_signing` (an authority to sign sandbox routes with), nothing
    /// runs inline: every route records its operation queued.
    pub fn new(
        config: InlineOperationsConfig,
        tasks: TaskTracker,
        drain_signal: DrainSignal,
        execution: OperationExecutionConfig,
        route_signing: bool,
    ) -> Self {
        // Expected on the admin surface, which has no authority by design and
        // serves no inline work; the public and shared surfaces fail startup
        // without one. So this is not a misconfiguration to warn about.
        if !route_signing && config.max_concurrent > 0 {
            tracing::info!(
                "inline operation execution is off: this process has no route-signing \
                 authority; operations it records stay queued for the dispatcher"
            );
        }
        Self {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(config.max_concurrent)),
                config,
                route_signing,
                in_flight: AtomicUsize::new(0),
                tasks,
                drain_signal,
                execution,
            }),
        }
    }

    /// Whether the env switches let operations of `kind` run inline: the
    /// master on, and `kind` in the allowlist. A route still needs a permit.
    ///
    /// The master goes on only once every API process, on every surface,
    /// takes the per-sandbox queue lock when it inserts an operation, and
    /// off before the dispatcher's keyed claim: the sole-active claim that
    /// every route reading this uses is sound only under both.
    pub fn kind_enabled(&self, kind: OperationKind) -> bool {
        self.inner.config.enabled && self.inner.config.kinds.contains(&kind)
    }

    /// The organization limit admission holds an inline claim to, with this
    /// process's claim staleness; `None` sets none.
    pub fn org_limit(&self) -> Option<InlineOrgLimit> {
        self.inner.config.org_limit.map(|limit| InlineOrgLimit {
            limit,
            claim_stale_after: self.inner.execution.claim_stale_after,
        })
    }

    /// A permit to run one operation offered on `path` inline, taken before
    /// the route claims it. A refusal is counted here; the route records the
    /// operation queued.
    pub fn try_acquire(&self, path: InlineAdmissionPath) -> Result<InlinePermit, InlineRefusal> {
        let inner = &self.inner;
        let permit = if !inner.route_signing {
            Err(InlineRefusal::RouteSigningMissing)
        } else if inner.config.max_concurrent == 0 {
            Err(InlineRefusal::Disabled)
        } else if inner.drain_signal.is_draining() {
            Err(InlineRefusal::Draining)
        } else {
            Arc::clone(&inner.permits)
                .try_acquire_owned()
                .map_err(|_| InlineRefusal::Saturated)
        };
        match permit {
            Ok(permit) => Ok(InlinePermit {
                path,
                _permit: permit,
            }),
            Err(refusal) => {
                self.refuse(path, refusal);
                Err(refusal)
            }
        }
    }

    /// Report `operations.inline.in_flight` against this process's capacity
    /// now and every [`SLOT_REPORT_INTERVAL`] until `shutdown_token` fires,
    /// so a process that runs nothing inline, or has gone quiet, still
    /// reports both. Capacity is `0` where nothing can run inline (no
    /// route-signing authority, or no permits). Sampling the count, rather
    /// than reporting each change as it happens, keeps two executions that
    /// end together from reporting their counts out of order.
    pub fn report_slots(
        &self,
        shutdown_token: CancellationToken,
    ) -> impl Future<Output = ()> + Send + 'static {
        let inner = Arc::clone(&self.inner);
        let capacity = if inner.route_signing {
            inner.config.max_concurrent
        } else {
            0
        };
        crate::limiter_liveness::run_heartbeat(SLOT_REPORT_INTERVAL, shutdown_token, move || {
            pplx_space_metrics::operations::inline_slots(
                inner.in_flight.load(Ordering::Acquire),
                capacity,
            );
        })
    }

    /// Count an operation offered on `path` that the route left queued for a
    /// reason only it knows.
    pub fn refuse(&self, path: InlineAdmissionPath, refusal: InlineRefusal) {
        pplx_space_metrics::operations::inline_admission(path, refusal.reason());
    }

    /// Drive `operation`, claimed under `claim_token`, to its recorded
    /// outcome in a task on the API's tracker, holding `permit` until it
    /// ends. Dropping the handle detaches the task; it keeps running. If it
    /// dies, the claim goes stale and the dispatcher re-drives the operation.
    pub fn spawn<F>(
        &self,
        permit: InlinePermit,
        store: PublicOperationStore,
        claim_token: String,
        operation: ClaimedPublicOperation,
        execution: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Future<Output = OperationExecutionResult> + Send + 'static,
    {
        pplx_space_metrics::operations::inline_admission(
            permit.path,
            InlineAdmissionReason::Admitted,
        );
        let in_flight = InFlight::enter(Arc::clone(&self.inner));
        let execution_config = self.inner.execution;
        let span = execution_span(&operation);
        self.inner.tasks.spawn(
            async move {
                let _permit = permit;
                let _in_flight = in_flight;
                let operation_id = operation.id;
                let kind = operation.kind;
                match drive_deferrable_operation(
                    store,
                    claim_token,
                    execution_config,
                    operation,
                    OperationExecutor::Inline,
                    execution,
                )
                .await
                {
                    Ok(report) => tracing::debug!(
                        operation_id = %operation_id,
                        kind = kind.as_str(),
                        succeeded = report.succeeded,
                        failed = report.failed,
                        retried = report.retried,
                        stale_claims = report.stale_claims,
                        "inline operation completed"
                    ),
                    // The claim is left to go stale, and the dispatcher
                    // re-drives the operation.
                    Err(error) => tracing::warn!(
                        operation_id = %operation_id,
                        kind = kind.as_str(),
                        %error,
                        "inline operation could not record its outcome"
                    ),
                }
            }
            .instrument(span),
        )
    }
}

/// One claimed operation's execution, as the runner drives it.
pub(crate) type InlineExecution = Pin<Box<dyn Future<Output = OperationExecutionResult> + Send>>;

/// Runs one claimed operation. Production runs the dispatcher's executor for
/// its kind; tests substitute the execution.
pub(crate) trait InlineExecutor: Send + Sync + 'static {
    fn execute(&self, operation: ClaimedPublicOperation) -> InlineExecution;
}

impl InlineExecutor for CreateSandboxOperationExecutor {
    fn execute(&self, operation: ClaimedPublicOperation) -> InlineExecution {
        let executor = self.clone();
        Box::pin(async move { executor.execute(&operation).await.into() })
    }
}

impl InlineExecutor for SandboxLifecycleOperationExecutor {
    fn execute(&self, operation: ClaimedPublicOperation) -> InlineExecution {
        let executor = self.clone();
        Box::pin(async move { executor.execute(&operation).await.into() })
    }
}

/// Inline execution of the lifecycle operations callers wait on: a caller's
/// `:wake`, `:resume` or `:start`, and the wake the system queues for a
/// command, process session or attach. Each runs inline only when the env
/// master is on, its kind is in `SPACE_NATIVE_INLINE_KINDS`, the
/// `space-native-inline-operations` flag allows its organization, and the
/// process has a permit free; admission then claims it only on an otherwise
/// idle sandbox with the organization under its limit. Anything else is
/// recorded queued for the dispatcher, exactly as with inline execution off.
#[derive(Clone)]
pub struct InlineLifecycle {
    runner: InlineOperations,
    executor: Arc<dyn InlineExecutor>,
    feature_flags: FeatureFlagClient,
}

impl InlineLifecycle {
    pub(crate) fn new(
        runner: InlineOperations,
        executor: Arc<dyn InlineExecutor>,
        feature_flags: FeatureFlagClient,
    ) -> Self {
        Self {
            runner,
            executor,
            feature_flags,
        }
    }

    /// The dispatcher's lifecycle executor, run by `runner`.
    pub(crate) fn with_lifecycle_executor(
        runner: InlineOperations,
        executor: SandboxLifecycleOperationExecutor,
        feature_flags: FeatureFlagClient,
    ) -> Self {
        Self::new(runner, Arc::new(executor), feature_flags)
    }

    /// Whether operations of `kind` for `org_id` are offered for inline
    /// execution: the env switches allow the kind, and the flag, evaluated
    /// on the organization under the env master, allows the organization.
    pub fn enabled_for(&self, org_id: OrgId, kind: OperationKind) -> bool {
        let org_enabled = self.feature_flags.gate_enabled(
            self.runner.inner.config.enabled,
            SPACE_NATIVE_INLINE_OPERATIONS_FLAG,
            &org_id.to_string(),
        );
        org_enabled && self.runner.kind_enabled(kind)
    }

    /// A permit to offer an operation of `kind` for `org_id` on `path` for
    /// inline execution, or `None` to record it queued. A refused permit is
    /// counted; an operation not offered at all is not.
    pub(crate) fn offer(
        &self,
        org_id: OrgId,
        kind: OperationKind,
        path: InlineAdmissionPath,
    ) -> Option<InlinePermit> {
        if !self.enabled_for(org_id, kind) {
            return None;
        }
        self.runner.try_acquire(path).ok()
    }

    /// The organization limit admission holds the claim to.
    pub(crate) fn org_limit(&self) -> Option<InlineOrgLimit> {
        self.runner.org_limit()
    }

    /// Count a replay of an operation of `kind` that would have been offered.
    pub(crate) fn refuse_replay(
        &self,
        org_id: OrgId,
        kind: OperationKind,
        path: InlineAdmissionPath,
    ) {
        if self.enabled_for(org_id, kind) {
            self.runner.refuse(path, InlineRefusal::Replay);
        }
    }

    /// Act on what admission recorded under `permit`: run a claimed
    /// operation inline, holding the permit, or count why it was left
    /// queued or not created. Answers what was recorded, as a plain create
    /// answers it, so the route responds exactly as without inline
    /// execution. The execution runs detached; the route does not wait.
    pub(crate) fn settle(
        &self,
        permit: InlinePermit,
        store: &PublicOperationStore,
        recorded: CreateInlinePublicOperationResult,
    ) -> CreatePublicOperationResult {
        let path = permit.path;
        match recorded {
            CreateInlinePublicOperationResult::Claimed {
                claim_token,
                operation,
            } => {
                let operation_id = operation.id;
                let execution = self.executor.execute(operation.clone());
                drop(
                    self.runner
                        .spawn(permit, store.clone(), claim_token, operation, execution),
                );
                CreatePublicOperationResult::Created(operation_id)
            }
            CreateInlinePublicOperationResult::Queued(operation_id, reason) => {
                self.runner.refuse(path, reason.into());
                CreatePublicOperationResult::Created(operation_id)
            }
            CreateInlinePublicOperationResult::NotCreated(result) => {
                if matches!(result, CreatePublicOperationResult::Existing(_)) {
                    self.runner.refuse(path, InlineRefusal::Joined);
                }
                result
            }
        }
    }
}

/// Counts one inline execution while it runs, including one that panics.
/// [`InlineOperations::report_slots`] reports the count.
struct InFlight(Arc<Inner>);

impl InFlight {
    fn enter(inner: Arc<Inner>) -> Self {
        inner.in_flight.fetch_add(1, Ordering::AcqRel);
        Self(inner)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests;

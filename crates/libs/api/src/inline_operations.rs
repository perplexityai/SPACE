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
//! inline execution off.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pplx_space_common_svc::DrainSignal;
use pplx_space_metrics::operations::{InlineAdmissionPath, InlineAdmissionReason};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument as _;

use crate::public_operation_driver::{
    OperationExecutionConfig, OperationExecutionResult, OperationExecutor,
    drive_deferrable_operation, execution_span,
};
use crate::public_operations::{ClaimedPublicOperation, PublicOperationStore};

/// Inline executions one API process runs at once; `0` turns inline
/// execution off entirely, including warm-pool creates.
pub const INLINE_MAX_CONCURRENT_ENV: &str = "SPACE_NATIVE_INLINE_MAX_CONCURRENT";
const DEFAULT_INLINE_MAX_CONCURRENT: usize = 128;
const MAX_INLINE_MAX_CONCURRENT: usize = 1024;
/// How often [`InlineOperations::report_slots`] samples the in-flight count.
/// The metrics agent keeps a gauge's last value per flush, so a sample is at
/// most this old when it is sent.
const SLOT_REPORT_INTERVAL: Duration = Duration::from_secs(1);

/// The environment's inline settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineOperationsConfig {
    /// `SPACE_NATIVE_INLINE_MAX_CONCURRENT`.
    pub max_concurrent: usize,
}

impl Default for InlineOperationsConfig {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_INLINE_MAX_CONCURRENT,
        }
    }
}

impl InlineOperationsConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    /// A malformed limit keeps the default, and one above 1024 is capped.
    pub fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let default = Self::default();
        Self {
            max_concurrent: env
                .var(INLINE_MAX_CONCURRENT_ENV)
                .ok()
                .and_then(|raw| raw.trim().parse::<usize>().ok())
                .map_or(default.max_concurrent, |limit| {
                    limit.min(MAX_INLINE_MAX_CONCURRENT)
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
    /// An idempotent replay, which records nothing new.
    Replay,
}

impl InlineRefusal {
    const fn reason(self) -> InlineAdmissionReason {
        match self {
            Self::Disabled => InlineAdmissionReason::Disabled,
            Self::RouteSigningMissing => InlineAdmissionReason::RouteSigningMissing,
            Self::Draining => InlineAdmissionReason::Draining,
            Self::Saturated => InlineAdmissionReason::Saturated,
            Self::Replay => InlineAdmissionReason::Replay,
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

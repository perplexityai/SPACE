use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_svc::{
    DependencyDrainError, DependencyDrainParticipant, DependencyWorkLease,
    SERVICE_MUTEX_POLL_INTERVAL,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::Instrument;

use super::SandboxManagerService;

// Disconnected clients retain admitted mutations, so HTTP connection limits
// alone cannot bound the work set. Finalizers share their parent's slot.
// Cleanup storage/attempts have their own bounded semaphores and must still
// make progress when foreground requests are waiting for their completion.
pub(super) const MAX_ACTIVE_STATE_WRITERS: usize = 256;
const RESERVED_TEARDOWN_WRITERS: usize = 16;

pub(crate) struct CriticalTaskGuard {
    name: &'static str,
    expected_shutdown: CancellationToken,
    failure_shutdown: CancellationToken,
    drain_state: Arc<DrainState>,
}

impl CriticalTaskGuard {
    pub(crate) fn new(
        name: &'static str,
        expected_shutdown: CancellationToken,
        failure_shutdown: CancellationToken,
        drain_state: Arc<DrainState>,
    ) -> Self {
        Self {
            name,
            expected_shutdown,
            failure_shutdown,
            drain_state,
        }
    }
}

impl Drop for CriticalTaskGuard {
    fn drop(&mut self) {
        if !self.expected_shutdown.is_cancelled() {
            self.drain_state
                .critical_worker_failed
                .store(true, Ordering::Release);
            tracing::error!(
                task = self.name,
                "critical worker exited before shutdown; draining manager"
            );
            self.failure_shutdown.cancel();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupRecoveryAdmissionOutcome {
    Acquired,
    Cancelled,
    Timeout,
    Error,
}

impl StartupRecoveryAdmissionOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Acquired => "acquired",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }
}

impl std::fmt::Display for StartupRecoveryAdmissionOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub(crate) struct DrainState {
    draining: AtomicBool,
    critical_worker_failed: AtomicBool,
    state_writers: Mutex<StateWriters>,
    state_writer_changed_tx: watch::Sender<()>,
}

#[derive(Default)]
struct StateWriters {
    active: usize,
    active_by_method: BTreeMap<&'static str, usize>,
    dependency_drain: Option<DependencyDrainParticipant>,
    dependency_work: Option<DependencyWorkLease>,
}

impl Default for DrainState {
    fn default() -> Self {
        let (state_writer_changed_tx, _state_writer_changed_rx) = watch::channel(());
        Self {
            draining: AtomicBool::default(),
            critical_worker_failed: AtomicBool::default(),
            state_writers: Mutex::new(StateWriters::default()),
            state_writer_changed_tx,
        }
    }
}

#[derive(Clone)]
pub(crate) struct StateWriterLease(Arc<StateWriterOwnership>);

struct StateWriterOwnership {
    drain_state: Arc<DrainState>,
    method: &'static str,
}

tokio::task_local! {
    static ADMITTED_WRITER: StateWriterLease;
}

pub(super) async fn with_state_writer<T>(
    writer: StateWriterLease,
    future: impl Future<Output = T>,
) -> T {
    ADMITTED_WRITER.scope(writer, future).await
}

/// Keeps startup recovery inside the same cross-process work set as runtime
/// state transitions. The guard is intentionally opaque to callers; dropping
/// it is the only supported way to finish recovery admission.
pub struct StartupRecoveryLease {
    _state_writer: StateWriterLease,
}

/// A root span for spawned work that can outlive the request that started
/// it, such as a start finalizer or a suspend publication, linked to the
/// current span.
///
/// It is neither a child of the request's span, which `in_current_span` or a
/// contextual child would make it, nor parented to it by trace context. A
/// child would hold the request's span open, so the request would report the
/// work's duration as its own latency. A trace-context parent looks remote to
/// the Datadog span processor, which then miscounts the request's still-open
/// trace and flushes it in fragments. The link names the request instead.
///
/// A link carries no attributes and log lines do not follow it, so the root
/// records the sandbox's canonical `sandbox_id`, the request's `request_id`
/// and the request's trace id as `origin_trace_id` itself; the sandbox's
/// telemetry queries find it by them. It is sampled as the request was.
pub(super) fn detached_trace_span(work: &'static str, sandbox_id: &str) -> tracing::Span {
    let span = tracing::info_span!(
        parent: None,
        "sbxmgr.detached",
        otel.name = work,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        sandbox_id = tracing::field::Empty,
        request_id = tracing::field::Empty,
        origin_trace_id = tracing::field::Empty,
        origin_sampled = tracing::field::Empty,
    );
    pplx_space_common_svc::record_sandbox_id(&span, sandbox_id);
    if let Some(request_id) = pplx_space_observability::current_request_id() {
        span.record("request_id", request_id.as_str());
    }
    if let Some(traceparent) =
        pplx_space_observability::trace_propagation::traceparent_for_span(&tracing::Span::current())
    {
        pplx_space_observability::trace_propagation::link_root_to_origins(
            &span,
            [traceparent.as_str()],
        );
    }
    span
}

/// Mark the current [`detached_trace_span`] failed. The work runs after its
/// request has returned, so its own span is the only one that can carry the
/// failure, and a search for a sandbox's failed traces needs it there.
pub(super) fn record_detached_failure(description: &str) {
    let span = tracing::Span::current();
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", description);
}

impl SandboxManagerService {
    pub(super) fn fail_poisoned_database(&self) {
        if !self
            .drain_state
            .critical_worker_failed
            .swap(true, Ordering::AcqRel)
        {
            tracing::error!("database mutex poisoned; refusing new mutations and draining manager");
        }
        self.drain_state.draining.store(true, Ordering::Release);
        self.shutdown_token.cancel();
    }

    /// Preserve failure status through graceful drain so the process supervisor
    /// can restart a daemon whose critical worker exited unexpectedly.
    pub fn critical_worker_failed(&self) -> bool {
        self.drain_state
            .critical_worker_failed
            .load(Ordering::Acquire)
    }

    pub fn begin_draining_for_restart(&self) {
        let was_draining = self.drain_state.draining.swap(true, Ordering::AcqRel);
        if !was_draining {
            tracing::info!(
                active_state_transitions = self.active_state_transition_count(),
                "sandbox-manager entering restart drain"
            );
        }
    }

    pub fn is_draining_for_restart(&self) -> bool {
        self.drain_state.draining.load(Ordering::Acquire)
    }

    #[allow(
        clippy::expect_used,
        reason = "Writer registry contains only private counters and owned dependency locks, with no caller callbacks"
    )]
    pub fn active_state_transition_count(&self) -> usize {
        self.drain_state
            .state_writers
            .lock()
            .expect("state writer mutex poisoned")
            .active
    }

    /// Returns the low-cardinality operation names currently keeping a
    /// restart drain open, sorted for stable logging.
    #[allow(
        clippy::expect_used,
        reason = "Writer registry contains only private counters and owned dependency locks, with no caller callbacks"
    )]
    pub fn active_state_transition_methods(&self) -> Vec<(&'static str, usize)> {
        self.drain_state
            .state_writers
            .lock()
            .expect("state writer mutex poisoned")
            .active_by_method
            .iter()
            .map(|(&method, &active)| (method, active))
            .collect()
    }

    /// Enables the cross-process dependency replacement protocol before the
    /// production socket begins serving. The active process holds the
    /// capability for its lifetime; state writers share the work gate while
    /// any transition is active.
    pub fn enable_dependency_restart_drain(
        &self,
        capability_path: impl AsRef<Path>,
        admission_gate_path: impl AsRef<Path>,
        work_gate_path: impl AsRef<Path>,
    ) -> Result<(), EnableDependencyRestartDrainError> {
        let participant = DependencyDrainParticipant::register(
            capability_path,
            admission_gate_path,
            work_gate_path,
        )?;
        let mut state_writers = self
            .drain_state
            .state_writers
            .lock()
            .map_err(|_| EnableDependencyRestartDrainError::StateWritersPoisoned)?;
        if state_writers.active != 0 {
            return Err(EnableDependencyRestartDrainError::ActiveWriters);
        }
        if state_writers.dependency_drain.is_some() {
            return Err(EnableDependencyRestartDrainError::AlreadyEnabled);
        }
        state_writers.dependency_drain = Some(participant);
        Ok(())
    }

    /// Attempts to admit startup recovery without waiting.
    pub fn startup_recovery_lease(&self) -> Result<StartupRecoveryLease, Status> {
        self.state_writer_lease("StartupRecovery")
            .map(|state_writer| StartupRecoveryLease {
                _state_writer: state_writer,
            })
    }

    /// Waits for dependency replacement to reopen admission, then keeps the
    /// dependency work gate shared until every recovery mutation is complete.
    pub async fn wait_for_startup_recovery_lease(
        &self,
        shutdown_token: &CancellationToken,
        timeout: Duration,
    ) -> Result<StartupRecoveryLease, StartupRecoveryAdmissionError> {
        let started_at = tokio::time::Instant::now();
        let acquire = async {
            let mut waiting = false;
            loop {
                match try_acquire_state_writer(&self.drain_state, "StartupRecovery")? {
                    Some(state_writer) => {
                        return Ok::<_, StateWriterAdmissionError>(StartupRecoveryLease {
                            _state_writer: state_writer,
                        });
                    }
                    None if !waiting => {
                        waiting = true;
                        tracing::info!(
                            wait_timeout_ms = duration_millis(timeout),
                            "startup recovery waiting for dependency-drain admission"
                        );
                    }
                    None => {}
                }
                tokio::time::sleep(SERVICE_MUTEX_POLL_INTERVAL).await;
            }
        };
        let result = tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => Err(StartupRecoveryAdmissionError::Cancelled),
            result = tokio::time::timeout(timeout, acquire) => match result {
                Ok(result) => result.map_err(Into::into),
                Err(_) => Err(StartupRecoveryAdmissionError::Timeout { waited: timeout }),
            },
        };
        let outcome = match &result {
            Ok(_) => StartupRecoveryAdmissionOutcome::Acquired,
            Err(StartupRecoveryAdmissionError::Cancelled) => {
                StartupRecoveryAdmissionOutcome::Cancelled
            }
            Err(StartupRecoveryAdmissionError::Timeout { .. }) => {
                StartupRecoveryAdmissionOutcome::Timeout
            }
            Err(
                StartupRecoveryAdmissionError::Gate(_)
                | StartupRecoveryAdmissionError::StateWritersPoisoned
                | StartupRecoveryAdmissionError::CapacityExhausted,
            ) => StartupRecoveryAdmissionOutcome::Error,
        };
        log_startup_recovery_admission_outcome(outcome, started_at, timeout);
        result
    }

    /// Releases the active-process capability immediately before handing the
    /// service mutex to a successor. Admission must already be closed and all
    /// state writers drained, so the successor can never observe two active
    /// dependency-drain participants.
    pub fn release_dependency_restart_drain_capability(
        &self,
    ) -> Result<(), ReleaseDependencyRestartDrainError> {
        if !self.is_draining_for_restart() {
            return Err(ReleaseDependencyRestartDrainError::NotDraining);
        }
        let mut state_writers = self
            .drain_state
            .state_writers
            .lock()
            .map_err(|_| ReleaseDependencyRestartDrainError::StateWritersPoisoned)?;
        if state_writers.active != 0 {
            return Err(ReleaseDependencyRestartDrainError::ActiveWriters {
                active: state_writers.active,
            });
        }
        state_writers
            .dependency_drain
            .take()
            .ok_or(ReleaseDependencyRestartDrainError::NotEnabled)?;
        Ok(())
    }

    pub async fn wait_for_state_transitions_drained(&self) {
        self.wait_for_state_transitions_drained_with(|| {}).await;
    }

    #[cfg(test)]
    pub(super) async fn wait_for_state_transitions_drained_after_observation(
        &self,
        observed: tokio::sync::oneshot::Sender<()>,
    ) {
        let mut observed = Some(observed);
        self.wait_for_state_transitions_drained_with(|| {
            if let Some(observed) = observed.take() {
                let _ = observed.send(());
            }
        })
        .await;
    }

    async fn wait_for_state_transitions_drained_with(&self, mut observe_active: impl FnMut()) {
        let mut state_writer_changed_rx = self.drain_state.state_writer_changed_tx.subscribe();
        loop {
            let active_state_transitions = self.active_state_transition_count();
            if active_state_transitions == 0 {
                tracing::info!("sandbox-manager restart drain has no active state transitions");
                return;
            }

            observe_active();
            tracing::info!(
                active_state_transitions,
                "waiting for active sandbox state transitions before restart"
            );
            if state_writer_changed_rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub(super) fn state_writer_lease(
        &self,
        method: &'static str,
    ) -> Result<StateWriterLease, Status> {
        acquire_state_writer(&self.drain_state, method)
    }

    /// Child mutations inherit the admitted request's ownership even if drain
    /// closed admission meanwhile. The lease survives caller cancellation and
    /// is released only when the tracked mutation actually finishes.
    ///
    /// For work the request awaits: the child runs in the caller's span.
    /// Work that outlives the request uses [`Self::spawn_detached_state_writer`].
    pub(super) fn spawn_state_writer<T: Send + 'static>(
        &self,
        method: &'static str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<T>, Status> {
        let writer = self.state_writer_lease(method)?;
        Ok(self
            .task_tracker
            .spawn(with_state_writer(writer, future).in_current_span()))
    }

    /// [`Self::spawn_state_writer`] for work that outlives the request, which
    /// runs in its own trace linked to the request's (see
    /// [`detached_trace_span`]).
    pub(super) fn spawn_detached_state_writer<T: Send + 'static>(
        &self,
        method: &'static str,
        sandbox_id: &str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<T>, Status> {
        let writer = self.state_writer_lease(method)?;
        Ok(self.task_tracker.spawn(
            with_state_writer(writer, future).instrument(detached_trace_span(method, sandbox_id)),
        ))
    }

    pub(super) fn critical_task_guard(
        &self,
        name: &'static str,
        shutdown: &CancellationToken,
    ) -> CriticalTaskGuard {
        CriticalTaskGuard::new(
            name,
            shutdown.clone(),
            self.shutdown_token.clone(),
            Arc::clone(&self.drain_state),
        )
    }

    pub(super) fn state_writer_lease_if_accepting(
        &self,
        method: &'static str,
    ) -> Option<StateWriterLease> {
        match try_acquire_state_writer(&self.drain_state, method) {
            Ok(writer) => writer,
            Err(error) => {
                tracing::error!(%error, "dependency drain admission failed closed");
                None
            }
        }
    }
}

impl Drop for StateWriterOwnership {
    fn drop(&mut self) {
        release_state_writer(&self.drain_state, self.method);
    }
}

pub(crate) fn acquire_state_writer(
    drain_state: &Arc<DrainState>,
    method: &'static str,
) -> Result<StateWriterLease, Status> {
    if let Some(writer) = inherited_state_writer(drain_state) {
        return Ok(writer);
    }
    match try_acquire_state_writer(drain_state, method) {
        Ok(Some(writer)) => Ok(writer),
        Ok(None) => Err(draining_status(method)),
        Err(StateWriterAdmissionError::CapacityExhausted) => Err(Status::resource_exhausted(
            "sandbox-manager state-writer capacity exhausted; retry after active work completes",
        )),
        Err(error) => {
            tracing::error!(method, %error, "dependency drain admission failed closed");
            Err(Status::internal(format!(
                "dependency drain admission failed for {method}: {error}"
            )))
        }
    }
}

pub(super) fn inherited_state_writer(drain_state: &Arc<DrainState>) -> Option<StateWriterLease> {
    ADMITTED_WRITER
        .try_with(Clone::clone)
        .ok()
        .filter(|writer| Arc::ptr_eq(&writer.0.drain_state, drain_state))
}

fn try_acquire_state_writer(
    drain_state: &Arc<DrainState>,
    method: &'static str,
) -> Result<Option<StateWriterLease>, StateWriterAdmissionError> {
    if drain_state.draining.load(Ordering::Acquire) {
        return Ok(None);
    }
    let mut state_writers = drain_state
        .state_writers
        .lock()
        .map_err(|_| StateWriterAdmissionError::StateWritersPoisoned)?;
    if drain_state.draining.load(Ordering::Acquire) {
        return Ok(None);
    }
    let independently_bounded_cleanup = matches!(method, "AsyncCleanup" | "CleanupAdmission");
    let teardown = matches!(
        method,
        "/space.sandbox.manager.v1.SandboxManager/Delete"
            | "/space.sandbox.manager.v1.SandboxManager/Stop"
            | "/space.sandbox.manager.v1.SandboxManager/Kill"
    );
    let capacity = MAX_ACTIVE_STATE_WRITERS
        + if teardown {
            RESERVED_TEARDOWN_WRITERS
        } else {
            0
        };
    let foreground = state_writers.active
        - state_writers
            .active_by_method
            .get("AsyncCleanup")
            .copied()
            .unwrap_or(0)
        - state_writers
            .active_by_method
            .get("CleanupAdmission")
            .copied()
            .unwrap_or(0);
    if !independently_bounded_cleanup && foreground >= capacity {
        return Err(StateWriterAdmissionError::CapacityExhausted);
    }
    if let Some(dependency_drain) = &state_writers.dependency_drain {
        let Some(admission) = dependency_drain.try_acquire_admission()? else {
            return Ok(None);
        };
        if state_writers.active == 0 {
            let Some(work) = dependency_drain.try_acquire_work(&admission)? else {
                return Ok(None);
            };
            state_writers.dependency_work = Some(work);
        }
    }
    state_writers.active = state_writers.active.saturating_add(1);
    let active_for_method = state_writers.active_by_method.entry(method).or_default();
    *active_for_method = active_for_method.saturating_add(1);
    drop(state_writers);
    drain_state.state_writer_changed_tx.send_replace(());

    Ok(Some(StateWriterLease(Arc::new(StateWriterOwnership {
        drain_state: Arc::clone(drain_state),
        method,
    }))))
}

#[allow(
    clippy::expect_used,
    reason = "Arc-owned writer lease releases its private registration exactly once; no caller callbacks run under the lock"
)]
fn release_state_writer(drain_state: &DrainState, method: &'static str) {
    let mut state_writers = drain_state
        .state_writers
        .lock()
        .expect("state writer mutex poisoned");
    debug_assert!(state_writers.active > 0, "state writer count underflow");
    state_writers.active = state_writers.active.saturating_sub(1);
    let remaining = state_writers.active;
    let active_for_method = state_writers
        .active_by_method
        .get_mut(method)
        .expect("active state writer method must be registered");
    debug_assert!(
        *active_for_method > 0,
        "state writer method count underflow"
    );
    *active_for_method = active_for_method.saturating_sub(1);
    if *active_for_method == 0 {
        state_writers.active_by_method.remove(method);
    }
    if remaining == 0 {
        state_writers.dependency_work = None;
    }
    drop(state_writers);
    tracing::debug!(
        active_state_transitions = remaining,
        "sandbox state writer lease released"
    );
    drain_state.state_writer_changed_tx.send_replace(());
}

fn log_startup_recovery_admission_outcome(
    outcome: StartupRecoveryAdmissionOutcome,
    started_at: tokio::time::Instant,
    timeout: Duration,
) {
    tracing::info!(
        outcome = %outcome,
        wait_ms = duration_millis(started_at.elapsed()),
        wait_timeout_ms = duration_millis(timeout),
        "startup recovery dependency-drain admission wait finished"
    );
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn draining_status(method: &str) -> Status {
    pplx_space_sbx_mgr_client::restart_retry_on_successor_status(format!(
        "sandbox-manager state changes are draining; retry on successor ({method})"
    ))
}

#[derive(Debug, thiserror::Error)]
pub enum EnableDependencyRestartDrainError {
    #[error(transparent)]
    Gate(#[from] DependencyDrainError),
    #[error("dependency restart drain must be enabled before state writers are admitted")]
    ActiveWriters,
    #[error("dependency restart drain is already enabled")]
    AlreadyEnabled,
    #[error("state writer registry mutex is poisoned")]
    StateWritersPoisoned,
}

#[derive(Debug, thiserror::Error)]
pub enum ReleaseDependencyRestartDrainError {
    #[error("dependency restart drain capability cannot be released before draining begins")]
    NotDraining,
    #[error("dependency restart drain capability cannot be released with {active} active writers")]
    ActiveWriters { active: usize },
    #[error("dependency restart drain is not enabled")]
    NotEnabled,
    #[error("state writer registry mutex is poisoned")]
    StateWritersPoisoned,
}

#[derive(Debug, thiserror::Error)]
pub enum StartupRecoveryAdmissionError {
    #[error("shutdown requested while waiting for startup recovery admission")]
    Cancelled,
    #[error("timed out after {waited:?} waiting for startup recovery admission")]
    Timeout { waited: Duration },
    #[error(transparent)]
    Gate(#[from] DependencyDrainError),
    #[error("state writer registry mutex is poisoned")]
    StateWritersPoisoned,
    #[error("state-writer capacity exhausted")]
    CapacityExhausted,
}

#[derive(Debug, thiserror::Error)]
enum StateWriterAdmissionError {
    #[error(transparent)]
    Gate(#[from] DependencyDrainError),
    #[error("state writer registry mutex is poisoned")]
    StateWritersPoisoned,
    #[error("state-writer capacity exhausted")]
    CapacityExhausted,
}

impl From<StateWriterAdmissionError> for StartupRecoveryAdmissionError {
    fn from(error: StateWriterAdmissionError) -> Self {
        match error {
            StateWriterAdmissionError::Gate(error) => Self::Gate(error),
            StateWriterAdmissionError::StateWritersPoisoned => Self::StateWritersPoisoned,
            StateWriterAdmissionError::CapacityExhausted => Self::CapacityExhausted,
        }
    }
}

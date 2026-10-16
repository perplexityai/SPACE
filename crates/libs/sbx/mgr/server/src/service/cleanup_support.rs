use pplx_space_common_core::types::JobId;

use super::*;
use pplx_space_events::{
    SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
    emit_sandbox_event,
};

pub(super) async fn cleanup_sandbox_via_node(
    db: &crate::db::SharedConnection,
    node_client: &mut NodeManagerGrpcClient,
    sandbox_id: &str,
    keep_filesystem: bool,
    context: &'static str,
) -> Result<(), Status> {
    cleanup_sandbox_via_node_with_options(
        db,
        node_client,
        sandbox_id,
        keep_filesystem,
        false,
        false,
        context,
    )
    .await
}

pub(super) async fn cleanup_sandbox_via_node_with_options(
    db: &crate::db::SharedConnection,
    node_client: &mut NodeManagerGrpcClient,
    sandbox_id: &str,
    keep_filesystem: bool,
    return_after_slot_release: bool,
    immediate_kill: bool,
    context: &'static str,
) -> Result<(), Status> {
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::CleanupRequested,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .reason(context)
            .bool_field(SandboxEventBoolField::Force, true)
            .bool_field(SandboxEventBoolField::KeepFilesystem, keep_filesystem)
            .bool_field(
                SandboxEventBoolField::ReturnAfterSlotRelease,
                return_after_slot_release,
            )
            .bool_field(SandboxEventBoolField::ImmediateKill, immediate_kill),
    );
    let fence = read_node_execution_fence(db, sandbox_id)?;
    let request = CleanupSandboxRequest {
        execution_fence: fence.map(Into::into),
        sandbox_id: sandbox_id.to_string(),
        force: true,
        keep_filesystem,
        return_after_slot_release,
        immediate_kill,
    };
    let response = node_client
        .cleanup_sandbox(request)
        .await
        .map_err(|error| Status::new(error.code(), format!("{context}: {}", error.message())))?
        .into_inner();
    if let Some(fence) = fence {
        let retained = response
            .retained_execution_id
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|error| {
                Status::internal(format!(
                    "invalid cleanup execution acknowledgement: {error}"
                ))
            })?;
        let db = db
            .lock()
            .map_err(|_| Status::unavailable("sandbox database mutex poisoned"))?;
        if !crate::db::acknowledge_node_cleanup(&db, sandbox_id, fence, retained).map_err(
            |error| {
                Status::unavailable(format!(
                    "persist cleanup execution acknowledgement: {error}"
                ))
            },
        )? {
            return Err(Status::aborted(
                "cleanup execution was superseded before acknowledgement",
            ));
        }
    }
    Ok(())
}

pub(super) fn read_node_execution_fence(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
) -> Result<Option<pplx_space_node_mgr_client::execution_fence::ExecutionFence>, Status> {
    let db = db
        .lock()
        .map_err(|_| Status::unavailable("sandbox database mutex poisoned"))?;
    crate::db::node_execution_fence(&db, sandbox_id)
        .map_err(|error| Status::unavailable(format!("read node execution authority: {error}")))
}

pub(super) fn admit_prepared_clone_disposal_attempt(
    db: &crate::db::SharedConnection,
    id: &str,
    job_id: Option<&JobId>,
) -> Result<(), Status> {
    let result = {
        let db = db
            .lock()
            .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
        let episode = crate::db::cleanup_episode(&db, id)
            .map_err(|e| Status::internal(format!("read clone disposal: {e}")))?
            .filter(|e| e.completion == CleanupCompletion::DeletePoolClone)
            .ok_or_else(|| Status::aborted("clone disposal ownership changed"))?;
        if crate::db::charge_prepared_clone_disposal_attempt(&db, id, episode.id)
            .map_err(|e| Status::internal(format!("charge clone disposal attempt: {e}")))?
        {
            Ok(())
        } else {
            crate::db::quarantine_cleanup_episode_for_job(
                &db,
                id,
                episode.id,
                "prepared clone disposal exhausted its eight-attempt budget",
                job_id,
            )
            .map_err(|e| Status::internal(format!("hold clone disposal: {e}")))?;
            Err(Status::failed_precondition(
                "prepared clone disposal is held for operator repair",
            ))
        }
    };
    if result.is_err() {
        mark_sandbox_dirty(db, id);
    }
    result
}

pub(super) struct NodeCleanupOptions<'a> {
    pub job_id: Option<&'a JobId>,
    pub keep_filesystem: bool,
    pub return_after_slot_release: bool,
    pub immediate_kill: bool,
}

/// Resolve durable resource authority on every path, including retries and
/// cleanup after persistence errors. Losing the lookup is not permission to
/// fall back to ordinary pathname-based cleanup.
pub(super) async fn service_cleanup_via_node_with_options(
    db: &crate::db::SharedConnection,
    node_client: &mut NodeManagerGrpcClient,
    sandbox_id: &str,
    options: NodeCleanupOptions<'_>,
    context: &'static str,
) -> Result<(), Status> {
    let episode = {
        let db = db
            .lock()
            .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
        crate::db::cleanup_episode(&db, sandbox_id)
            .map_err(|e| Status::internal(format!("read disposal authority: {e}")))?
    };
    if let Some(episode) = episode.filter(|e| e.completion == CleanupCompletion::DeletePoolClone) {
        let result = node_client
            .cleanup_prepared_clone(
                pplx_space_node_mgr_client::proto::CleanupPreparedCloneRequest {
                    sandbox_id: sandbox_id.to_owned(),
                    prepared_destination: episode.prepared_clone_root.map(|root| {
                        pplx_space_node_mgr_client::proto::CloneDirectoryIdentity {
                            device: root.device,
                            inode: root.inode,
                        }
                    }),
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| {
                Status::new(
                    error.code(),
                    format!("prepared clone disposal failed (status {:?})", error.code()),
                )
            });
        if let Err(error) = &result
            && (matches!(
                error.code(),
                tonic::Code::FailedPrecondition
                    | tonic::Code::Unimplemented
                    | tonic::Code::PermissionDenied
                    | tonic::Code::InvalidArgument
            ) || episode.attempt_count >= 8)
        {
            {
                let db = db
                    .lock()
                    .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
                crate::db::quarantine_cleanup_episode_for_job(
                    &db,
                    sandbox_id,
                    episode.id,
                    error.message(),
                    options.job_id,
                )
                .map_err(|e| Status::internal(format!("persist clone disposal hold: {e}")))?;
            }
            mark_sandbox_dirty(db, sandbox_id);
        }
        return result;
    }
    cleanup_sandbox_via_node_with_options(
        db,
        node_client,
        sandbox_id,
        options.keep_filesystem,
        options.return_after_slot_release,
        options.immediate_kill,
        context,
    )
    .await
}

pub(super) use crate::db::CleanupCompletion;

/// Map a [`crate::db::update_state_from`] result to a status. A database
/// error is `Internal`. Every refusal is `Aborted`: a from-state refusal
/// reports the state another transition left the row in, and a generic-guard
/// refusal or a missing row renders [`crate::db::SandboxStateWriteNotApplied`].
pub(super) fn require_state_write_from(
    result: crate::db::Result<crate::db::StateWriteOutcome>,
    context: &str,
) -> Result<(), Status> {
    let outcome = result.map_err(|error| Status::internal(format!("{context}: {error}")))?;
    state_write_result(outcome)
        .map(drop)
        .map_err(|rejected| rejected.into_status(context))
}

/// Why an `update_state_from` write did not apply. A caller that answers one
/// case differently matches it and passes the rest to
/// [`StateWriteRejected::into_status`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StateWriteRejected {
    /// Another transition moved the row outside the edge's source states.
    FromState {
        found: SandboxState,
    },
    /// A generic guard refused. The guards answer first, so `found` may also
    /// be outside the edge's source states.
    Guard {
        refusal: crate::db::StateWriteGuard,
        found: SandboxState,
    },
    Missing,
}

pub(super) fn state_write_result(
    outcome: crate::db::StateWriteOutcome,
) -> Result<SandboxState, StateWriteRejected> {
    use crate::db::{StateWriteOutcome, StateWriteRefusal};
    match outcome {
        StateWriteOutcome::Applied { from } => Ok(from),
        StateWriteOutcome::Refused {
            found,
            refusal: StateWriteRefusal::FromState,
        } => Err(StateWriteRejected::FromState { found }),
        StateWriteOutcome::Refused {
            found,
            refusal: StateWriteRefusal::Guard(refusal),
        } => Err(StateWriteRejected::Guard { refusal, found }),
        StateWriteOutcome::Missing => Err(StateWriteRejected::Missing),
    }
}

impl StateWriteRejected {
    pub(super) fn into_status(self, context: &str) -> Status {
        use crate::db::SandboxStateWriteNotApplied;
        let not_applied = match self {
            Self::FromState { found } => {
                return Status::aborted(format!(
                    "{context}: sandbox state write was superseded: sandbox is {found}"
                ));
            }
            Self::Guard { refusal, .. } => SandboxStateWriteNotApplied::Guard(refusal),
            Self::Missing => SandboxStateWriteNotApplied::Missing,
        };
        Status::aborted(format!("{context}: {not_applied}"))
    }
}

/// The best-effort form of [`crate::db::update_state_from`]: a refusal or an
/// error is logged and leaves the row as it is, and only an applied write
/// marks the sandbox dirty. `None` is a poisoned connection or a
/// database error.
pub(super) fn update_state_from_best_effort(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    edge: &'static crate::db::Edge,
) -> Option<crate::db::StateWriteOutcome> {
    use crate::db::StateWriteOutcome;
    let outcome = {
        let db = crate::node_state_sync::connection_or_signal_failure(db).ok()?;
        crate::db::update_state_from(&db, sandbox_id, edge)
    };
    match outcome {
        Ok(outcome @ StateWriteOutcome::Applied { .. }) => {
            mark_sandbox_dirty(db, sandbox_id);
            Some(outcome)
        }
        Ok(outcome @ StateWriteOutcome::Refused { .. }) => {
            tracing::warn!(
                sandbox_id,
                state = edge.to().as_str(),
                ?outcome,
                "sandbox state write was superseded"
            );
            Some(outcome)
        }
        Ok(outcome @ StateWriteOutcome::Missing) => {
            tracing::warn!(
                sandbox_id,
                state = edge.to().as_str(),
                "sandbox state write found no sandbox row"
            );
            Some(outcome)
        }
        Err(error) => {
            tracing::warn!(
                sandbox_id,
                state = edge.to().as_str(),
                %error,
                "failed to update sandbox state"
            );
            None
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ActivationCleanupSpec {
    pub(super) cleanup_id: SandboxCleanupEpisodeId,
    pub(super) completion: CleanupCompletion,
    pub(super) context: &'static str,
    pub(super) interrupted_reason_code: SandboxQuarantineReasonCode,
    pub(super) interrupted_reason: &'static str,
}

pub(super) type CleanupLock = Arc<tokio::sync::Mutex<()>>;
pub(super) type CleanupLockMap = Arc<Mutex<BTreeMap<String, CleanupLock>>>;

/// Backoff while activation rollback waits to persist its cleanup handoff.
pub(super) const CLEANUP_HANDOFF_RETRY: Duration = Duration::from_millis(100);

/// RAII owner of a per-sandbox [`CleanupLock`] that prunes the sandbox's entry
/// from the [`CleanupLockMap`] when the cleanup task ends — on *any* exit path,
/// including the superseded/duplicate early returns. It owns the sole non-map
/// reference to the lock, so `prune_cleanup_lock`'s `strong_count == 2` check
/// fires exactly when no other runner still holds the lock.
pub(super) struct CleanupLockGuard {
    locks: CleanupLockMap,
    sandbox_id: String,
    lock: CleanupLock,
}

pub(super) struct CleanupFenceGuard<'a> {
    sandbox_id: &'a str,
    _guard: tokio::sync::MutexGuard<'a, ()>,
}

pub(super) struct OwnedCleanupFenceGuard {
    sandbox_id: String,
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _lock_owner: CleanupLockGuard,
}

pub(super) enum CleanupLockTryResult {
    Acquired(OwnedCleanupFenceGuard),
    Busy,
}

pub(super) trait CleanupFenceAuthorization: Send + Sync {
    fn authorizes(&self, sandbox_id: &str) -> bool;
}

impl CleanupFenceAuthorization for CleanupFenceGuard<'_> {
    fn authorizes(&self, sandbox_id: &str) -> bool {
        self.sandbox_id == sandbox_id
    }
}

impl CleanupFenceAuthorization for OwnedCleanupFenceGuard {
    fn authorizes(&self, sandbox_id: &str) -> bool {
        self.sandbox_id == sandbox_id
    }
}

impl CleanupLockGuard {
    pub(super) fn new(locks: CleanupLockMap, sandbox_id: String, lock: CleanupLock) -> Self {
        Self {
            locks,
            sandbox_id,
            lock,
        }
    }

    pub(super) async fn lock(&self) -> CleanupFenceGuard<'_> {
        CleanupFenceGuard {
            sandbox_id: &self.sandbox_id,
            _guard: self.lock.lock().await,
        }
    }

    pub(super) async fn lock_owned(self) -> OwnedCleanupFenceGuard {
        let sandbox_id = self.sandbox_id.clone();
        let guard = Arc::clone(&self.lock).lock_owned().await;
        OwnedCleanupFenceGuard {
            sandbox_id,
            _guard: guard,
            _lock_owner: self,
        }
    }

    pub(super) fn try_acquire_owned(self) -> CleanupLockTryResult {
        let sandbox_id = self.sandbox_id.clone();
        match Arc::clone(&self.lock).try_lock_owned() {
            Ok(guard) => CleanupLockTryResult::Acquired(OwnedCleanupFenceGuard {
                sandbox_id,
                _guard: guard,
                _lock_owner: self,
            }),
            Err(_) => CleanupLockTryResult::Busy,
        }
    }
}

impl Drop for CleanupLockGuard {
    fn drop(&mut self) {
        super::SandboxManagerService::prune_cleanup_lock(&self.locks, &self.sandbox_id, &self.lock);
    }
}

/// Default node-wide cleanup concurrency cap for `schedule_async_cleanup`.
/// Picked to match the cascade-amplifier goal: high enough that
/// healthy churn never queues, low enough that a stuck node-mgr can't be
/// flooded by an unbounded fan-out of cleanup retries.
pub(super) const DEFAULT_CLEANUP_CONCURRENCY_CAP: usize = 16;

/// Bound on blocking cleanup I/O before spawning. SQLite serializes its
/// writers; a small separate budget permits filesystem probes without growing
/// the blocking task population with request concurrency.
pub(super) const CLEANUP_IO_CONCURRENCY_CAP: usize = 4;

/// Counters backing the `sandbox.worker.cleanup.{active,queue_depth}`
/// gauges. We don't trust `Semaphore::available_permits()` for "queue
/// depth" because it doesn't distinguish "permit free, no waiter" from
/// "permit just released". Two atomics are cheap and unambiguous:
/// increment `waiting` before `acquire_owned`, decrement on permit ready,
/// increment `in_flight` while the cleanup runs, decrement on exit.
/// Mirrors the in-flight / enqueued split in `node-mgr`'s
/// `lifecycle_limiter`.
#[derive(Debug, Default)]
pub(super) struct CleanupMetrics {
    in_flight: std::sync::atomic::AtomicUsize,
    waiting: std::sync::atomic::AtomicUsize,
}

impl CleanupMetrics {
    pub(super) fn waiting_inc(&self) {
        let v = self
            .waiting
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        pplx_space_metrics::sandbox::worker::cleanup_queue_depth(v);
    }

    pub(super) fn waiting_dec(&self) {
        let prev = self
            .waiting
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        pplx_space_metrics::sandbox::worker::cleanup_queue_depth(prev.saturating_sub(1));
    }

    pub(super) fn in_flight_inc(&self) {
        let v = self
            .in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        pplx_space_metrics::sandbox::worker::cleanup_active(v);
    }

    pub(super) fn in_flight_dec(&self) {
        let prev = self
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        pplx_space_metrics::sandbox::worker::cleanup_active(prev.saturating_sub(1));
    }

    #[cfg(test)]
    pub(super) fn in_flight(&self) -> usize {
        self.in_flight.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn waiting(&self) -> usize {
        self.waiting.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// RAII guard for the `waiting` (queue-depth) gauge: increments on
/// construction, decrements on drop. Wrapping the permit-acquire wait in
/// this guard keeps `sandbox.worker.cleanup.queue_depth` balanced even when
/// the awaiting future is cancelled (e.g. a blocking cleanup RPC times out
/// or the client disconnects while queued for a permit).
pub(super) struct CleanupWaitGuard {
    metrics: Arc<CleanupMetrics>,
}

impl CleanupWaitGuard {
    pub(super) fn new(metrics: Arc<CleanupMetrics>) -> Self {
        metrics.waiting_inc();
        Self { metrics }
    }
}

impl Drop for CleanupWaitGuard {
    fn drop(&mut self) {
        self.metrics.waiting_dec();
    }
}

/// RAII pairing of a cleanup-semaphore permit with the `in_flight` gauge.
/// Constructed for one active node-manager cleanup attempt. Dropping it before
/// retry backoff releases the permit *and* decrements the gauge, so durable
/// failures do not monopolize worker capacity.
pub(super) struct CleanupPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    metrics: Arc<CleanupMetrics>,
}

impl CleanupPermit {
    pub(super) fn new(
        permit: tokio::sync::OwnedSemaphorePermit,
        metrics: Arc<CleanupMetrics>,
    ) -> Self {
        metrics.in_flight_inc();
        Self {
            _permit: permit,
            metrics,
        }
    }
}

impl Drop for CleanupPermit {
    fn drop(&mut self) {
        self.metrics.in_flight_dec();
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NetGatewayStatsSnapshot {
    pub(super) reachable: bool,
    pub(super) tcp_active_connections: u64,
    pub(super) tcp_total_connections: u64,
    pub(super) tcp_denied_connections: u64,
    pub(super) udp_active_mappings: u64,
    pub(super) network_tx_bytes: u64,
    pub(super) network_rx_bytes: u64,
    pub(super) bytes_forwarded: u64,
}

impl NetGatewayStatsSnapshot {
    pub(super) fn persisted(network_tx_bytes: u64, network_rx_bytes: u64) -> Self {
        Self {
            reachable: false,
            tcp_active_connections: 0,
            tcp_total_connections: 0,
            tcp_denied_connections: 0,
            udp_active_mappings: 0,
            network_tx_bytes,
            network_rx_bytes,
            bytes_forwarded: network_tx_bytes.saturating_add(network_rx_bytes),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CleanupRecoveryTarget {
    pub(super) sandbox_id: SandboxId,
    pub(super) uuid8: SandboxRuntimeLabel,
    pub(super) keep_filesystem: bool,
    pub(super) completion: CleanupCompletion,
    pub(super) cleanup_id: Option<SandboxCleanupEpisodeId>,
}

#[cfg(test)]
mod tests;

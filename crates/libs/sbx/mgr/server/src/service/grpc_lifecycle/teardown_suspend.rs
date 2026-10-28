//! `PrepareTeardownSuspend` RPC and the teardown-suspend driver, including the
//! remote backup fence that must hold before runtime teardown.

use super::*;

mod backup_refusal;

/// The target of everything this module logs, its children included, so that
/// moving code into a child keeps the target searches select on.
const LOG_TARGET: &str = module_path!();

pub(super) use self::backup_refusal::teardown_artifact_backup_status;
use self::backup_refusal::*;
#[cfg(test)]
pub(super) use self::backup_refusal::{
    teardown_backup_drive_verdict, teardown_backup_verdict_status,
};

pub(in crate::service) const SUSPEND_TRIGGER_METADATA: &str = "x-space-suspend-trigger";

/// What a teardown suspend that needs an artifact's backup does with the
/// newest upload under the artifact's key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::service) enum KeyedUploadUse {
    /// Wait for it. An active upload owns the backup; a terminal one answers
    /// the wait at once with its outcome, a failure included.
    Join,
    /// Start a new attempt, which the key admits once the newest upload is no
    /// longer active.
    NewAttempt,
}

/// How many of an artifact's uploads may fail an attempt permanently before a
/// teardown stops starting new ones: the first, which may have met an outage
/// such as a transfer vol-mgr gave up on, and one recovery attempt. A failure
/// that recurs on a fresh attempt is treated as the artifact's own. Refusals
/// of the artifact don't count against it: after one, the teardown starts an
/// attempt only once the fence admits the registration, though other callers
/// under the artifact's key may start their own.
pub(in crate::service) const PERMANENTLY_FAILED_UPLOAD_LIMIT: u64 = 2;

/// The teardown's rule for the newest upload under the artifact's key, in
/// `status` with `failure` and the `scope` its worker recorded, given
/// `permanently_failed` uploads under the key so far, by any caller:
///
/// - an active or completed upload is joined;
/// - a retriable failure only ran out of attempts, so a new attempt follows;
/// - a permanent failure of the artifact itself (see
///   [`crate::snapshot_upload::UploadFailureScope`]) is joined, which makes it
///   final unless the cluster has since stopped refusing the registration
///   (see [`SandboxManagerService::ensure_teardown_suspend_backup`]);
/// - a permanent failure of an attempt gets a new attempt while fewer than
///   [`PERMANENTLY_FAILED_UPLOAD_LIMIT`] uploads have failed permanently, and
///   is final after that.
///
/// The limit bounds the attempt-scoped rule, so a failure recorded as the
/// attempt's that is really the artifact's still ends. It counts no failure
/// recorded as the artifact's: a refusal of the artifact, even one that
/// lapses, isn't an attempt's failure. The failed jobs it counts persist across
/// restarts until the jobs retention pruner removes them.
pub(in crate::service) fn teardown_keyed_upload_use(
    status: pplx_space_common_jobs::JobStatus,
    failure: Option<pplx_space_common_jobs::JobFailure>,
    scope: crate::snapshot_upload::UploadFailureScope,
    permanently_failed: u64,
) -> KeyedUploadUse {
    use crate::snapshot_upload::UploadFailureScope;
    use pplx_space_common_jobs::JobFailure;
    if !status.is_terminal() {
        return KeyedUploadUse::Join;
    }
    match (failure, scope) {
        (None, _) => KeyedUploadUse::Join,
        (Some(JobFailure::Retriable), _) => KeyedUploadUse::NewAttempt,
        (Some(JobFailure::Permanent { .. }), UploadFailureScope::Attempt)
            if permanently_failed < PERMANENTLY_FAILED_UPLOAD_LIMIT =>
        {
            KeyedUploadUse::NewAttempt
        }
        (Some(JobFailure::Permanent { .. }), _) => KeyedUploadUse::Join,
    }
}

fn teardown_suspend_event_details(
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    trigger: SandboxEventTrigger,
) -> SandboxEventDetails {
    SandboxEventDetails::default()
        .operation("generation_suspend")
        .trigger(trigger)
        .execution_generation(execution_generation.to_string())
}

pub(in crate::service) fn teardown_suspend_event_trigger(
    request: &Request<PrepareTeardownSuspendRequest>,
) -> Result<SandboxEventTrigger, Status> {
    match request
        .metadata()
        .get(SUSPEND_TRIGGER_METADATA)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| Status::invalid_argument("invalid suspend trigger metadata"))?
    {
        None | Some("system") => Ok(SandboxEventTrigger::System),
        Some("user") => Ok(SandboxEventTrigger::User),
        Some("auto_lifecycle") => Ok(SandboxEventTrigger::AutoLifecycle),
        Some(_) => Err(Status::invalid_argument("unknown suspend trigger metadata")),
    }
}

/// How often the resumer looks for a `suspending` row's teardown no poll
/// drives (see [`SandboxManagerService::start_teardown_suspend_resumer`]).
/// Each drive still keeps its own retry pacing.
const TEARDOWN_SUSPEND_RESUME_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How a terminal local state affects a generation suspend request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::service) enum TerminalTeardownSuspend {
    /// Not terminal: keep preparing the request.
    Open,
    /// Deleting: the job is closed and the delete's receipt will release the
    /// execution.
    AwaitingDelete,
    /// The delete committed since the request's first snapshot: its receipt
    /// covers the execution and releases it.
    Deleted,
}

impl TerminalTeardownSuspend {
    /// The response that answers the request, or `None` to keep preparing it.
    fn response(
        self,
        started_at: std::time::Instant,
    ) -> Option<Response<PrepareTeardownSuspendResponse>> {
        match self {
            Self::Open => None,
            Self::AwaitingDelete => Some(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Pending,
                Some("sandbox is deleting; its delete receipt releases this execution".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                started_at,
            )),
            Self::Deleted => Some(deleted_teardown_suspend_response(started_at)),
        }
    }
}

fn deleted_teardown_suspend_response(
    started_at: std::time::Instant,
) -> Response<PrepareTeardownSuspendResponse> {
    teardown_suspend_prepare_response(
        TeardownSuspendOutcome::Deleted,
        Some("a generation-fenced local delete covers this execution".to_string()),
        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Deleted,
        started_at,
    )
}

fn teardown_suspend_prepare_response(
    outcome: TeardownSuspendOutcome,
    reason: Option<String>,
    metric_outcome: pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome,
    started_at: std::time::Instant,
) -> Response<PrepareTeardownSuspendResponse> {
    pplx_space_metrics::sandbox::worker::teardown_suspend_prepare(metric_outcome, started_at);
    let span = tracing::Span::current();
    span.record("grpc.response.outcome", outcome.as_str_name());
    if let Some(reason) = reason.as_deref() {
        span.record("grpc.response.reason", reason);
    }
    Response::new(PrepareTeardownSuspendResponse {
        outcome: outcome as i32,
        reason,
        published_artifact: None,
    })
}

fn durable_teardown_suspend_prepare_response(
    status: &crate::db::TeardownSuspendStatus,
    started_at: std::time::Instant,
) -> Result<Response<PrepareTeardownSuspendResponse>, Status> {
    let artifact = status.artifact.as_ref().ok_or_else(|| {
        Status::internal("durable teardown suspend is missing its published artifact")
    })?;
    let artifact_kind = match artifact.artifact_kind {
        pplx_space_common_core::types::ArtifactKind::Disk => crate::proto::ArtifactKind::Disk,
        pplx_space_common_core::types::ArtifactKind::Full => crate::proto::ArtifactKind::Full,
    };
    let mut response = teardown_suspend_prepare_response(
        TeardownSuspendOutcome::Durable,
        None,
        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Durable,
        started_at,
    )
    .into_inner();
    response.published_artifact = Some(crate::proto::PublishedArtifact {
        local_artifact_id: artifact.local_artifact_id.to_string(),
        artifact_kind: artifact_kind as i32,
        origin: crate::proto::ArtifactOrigin::Suspend as i32,
        snapshot_name: None,
    });
    Ok(Response::new(response))
}

/// The wake and artifact locks the teardown driver holds for its whole run.
type TeardownSuspendLocks = (
    crate::auto_lifecycle::LifecycleMutexGuard,
    crate::auto_lifecycle::LifecycleMutexGuard,
);

impl SandboxManagerService {
    pub(in crate::service) async fn prepare_teardown_suspend(
        &self,
        request: Request<PrepareTeardownSuspendRequest>,
    ) -> Result<Response<PrepareTeardownSuspendResponse>, Status> {
        let metric_started_at = std::time::Instant::now();
        self.authorize_peer(&request, "PrepareTeardownSuspend", ALLOW_SANDBOX_OPERATOR)?;
        let event_trigger = teardown_suspend_event_trigger(&request)?;
        let req = request.get_ref();
        let sandbox_id: SandboxId = req
            .sandbox_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        // Runtime attachment establishes this node-local lease from the route
        // identity already passed down with the execution.
        let execution_generation =
            pplx_space_common_core::types::ExecutionGeneration::new(req.execution_generation)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let span = tracing::Span::current();
        span.record("sandbox_id", tracing::field::display(sandbox_id));
        span.record("execution_generation", execution_generation.get());
        if let Some(node_id) = self.local_node_id {
            span.record("current_node_id", tracing::field::display(node_id));
        }
        let action = TeardownSuspendAction::try_from(req.action)
            .ok()
            .filter(|action| *action != TeardownSuspendAction::Unspecified)
            .ok_or_else(|| Status::invalid_argument("teardown suspend action is required"))?;

        let sandbox_id_string = sandbox_id.to_string();
        let evidence = {
            let db = self.db_conn()?;
            crate::db::read::sandbox_view(&db, &sandbox_id_string)
                .teardown()
                .release_evidence(execution_generation)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
        };
        let Some(mut status) = evidence.suspend else {
            if evidence.deleted {
                return Ok(deleted_teardown_suspend_response(metric_started_at));
            }
            if let Some(response) = self
                .close_terminal_teardown_suspend(&sandbox_id_string, execution_generation)?
                .response(metric_started_at)
            {
                return Ok(response);
            }
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Pending,
                Some("sandbox record is not present on this node".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                metric_started_at,
            ));
        };

        if evidence.activation_rolled_back {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a generation-fenced activation rollback retired this execution".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }

        let local_generation = match evidence.runtime_generation {
            Some(generation) => generation,
            None if status.state == SandboxState::Suspended => execution_generation,
            None => {
                if let Some(response) = self
                    .close_terminal_teardown_suspend(&sandbox_id_string, execution_generation)?
                    .response(metric_started_at)
                {
                    return Ok(response);
                }
                return Ok(teardown_suspend_prepare_response(
                    TeardownSuspendOutcome::Pending,
                    Some("local runtime execution generation is not available".to_string()),
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                    metric_started_at,
                ));
            }
        };
        if local_generation != execution_generation {
            let (outcome, reason, metric_outcome) = if local_generation > execution_generation {
                (
                    TeardownSuspendOutcome::Superseded,
                    "a newer local execution owns generation suspend",
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                )
            } else {
                (
                    TeardownSuspendOutcome::Pending,
                    "the requested execution does not own the local runtime",
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                )
            };
            return Ok(teardown_suspend_prepare_response(
                outcome,
                Some(reason.to_string()),
                metric_outcome,
                metric_started_at,
            ));
        }
        // A completed STOP has already released this execution's runtime;
        // retain its disk and let containerd discard only the obsolete task.
        // Cleanup preserves the retired generation as the release witness.
        if status.state == SandboxState::Stopped {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a generation-fenced stop retired this execution".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }
        if status
            .operation
            .as_ref()
            .is_some_and(|operation| operation.execution_generation > execution_generation)
        {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a newer local execution owns generation suspend".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }
        if let Some(response) = self
            .close_terminal_teardown_suspend(&sandbox_id_string, execution_generation)?
            .response(metric_started_at)
        {
            return Ok(response);
        }
        if action == TeardownSuspendAction::BeginOrJoin
            && matches!(status.state, SandboxState::Started | SandboxState::Paused)
        {
            let db = self.db_conn()?;
            match crate::db::begin_teardown_suspend_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
            ) {
                Ok(_) => {}
                Err(error) if error.is_invalid_query() => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::read::sandbox_view(&db, &sandbox_id.to_string())
                .teardown()
                .status()
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
        }
        let has_exact_operation = status.operation.as_ref().is_some_and(|operation| {
            operation.execution_generation == execution_generation
                && operation.status.can_support_release_proof()
        });
        let can_bind_execution = status
            .operation
            .as_ref()
            .is_none_or(|operation| operation.execution_generation <= execution_generation);
        let mut bound_existing_suspend = false;
        if action == TeardownSuspendAction::BeginOrJoin
            && status.state == SandboxState::Suspended
            && !has_exact_operation
            && can_bind_execution
        {
            let db = self.db_conn()?;
            match crate::db::bind_suspended_teardown_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
            ) {
                Ok(_) => {}
                Err(error) if error.is_invalid_query() => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::read::sandbox_view(&db, &sandbox_id.to_string())
                .teardown()
                .status()
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
            bound_existing_suspend = true;
        }
        if bound_existing_suspend {
            emit_sandbox_event(
                &sandbox_id.to_string(),
                SandboxEventType::ExecutionSuspendRequested,
                SandboxEventSource::SbxManager,
                teardown_suspend_event_details(execution_generation, event_trigger)
                    .with_from_state(SandboxState::Suspended.to_string()),
            );
        }
        if status.is_durable_for(execution_generation) {
            return durable_teardown_suspend_prepare_response(&status, metric_started_at);
        }
        if status.is_ready_for(execution_generation) {
            self.complete_teardown_suspend_operation(
                &sandbox_id.to_string(),
                execution_generation,
                event_trigger,
            )?;
            status = {
                let db = self.db_conn()?;
                crate::db::read::sandbox_view(&db, &sandbox_id_string)
                    .teardown()
                    .status()
                    .map_err(|error| Status::internal(format!("db error: {error}")))?
                    .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?
            };
            if status.is_durable_for(execution_generation) {
                return durable_teardown_suspend_prepare_response(&status, metric_started_at);
            }
            return Err(Status::internal(
                "generation suspend completion did not bind its durable artifact",
            ));
        }
        let state = status.state;
        let resume_persisted = status.operation.as_ref().is_some_and(|operation| {
            operation.status == crate::db::TeardownSuspendOperationStatus::Running
                && operation.execution_generation == execution_generation
        });
        let begin_new = action == TeardownSuspendAction::BeginOrJoin
            && matches!(state, SandboxState::Started | SandboxState::Paused);
        if begin_new || resume_persisted {
            self.spawn_teardown_suspend_driver(sandbox_id, execution_generation, event_trigger);
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Pending,
                None,
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                metric_started_at,
            ));
        }
        Ok(teardown_suspend_prepare_response(
            TeardownSuspendOutcome::Pending,
            Some(format!(
                "sandbox is {} without a durable teardown artifact",
                state
            )),
            pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
            metric_started_at,
        ))
    }

    /// Closes this generation's suspend job when the sandbox is terminal.
    ///
    /// A quarantined sandbox, a missing row and a failed job that may not retry
    /// are refused, so callers stop polling without release proof. A deleting
    /// sandbox's job is closed too, but its delete commits a receipt that
    /// releases this execution, so callers are told to keep polling. Refusing
    /// it instead raced the shim's single exit-path release, which containerd
    /// does not retry, and left the generation's pod unreleased. That delete
    /// removes the row and commits the receipt together, possibly after the
    /// request's first snapshot, so a missing row with a receipt covering this
    /// execution is answered as deleted rather than refused.
    pub(super) fn close_terminal_teardown_suspend(
        &self,
        sandbox_id: &str,
        generation: pplx_space_common_core::types::ExecutionGeneration,
    ) -> Result<TerminalTeardownSuspend, Status> {
        let db = self.db_conn()?;
        let state = crate::db::read::sandbox(&db, sandbox_id)
            .map_err(|error| Status::internal(format!("db error: {error}")))?
            .map(|row| row.state);
        let operation = crate::db::read::sandbox_view(&db, sandbox_id)
            .teardown()
            .latest()
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
        // A failed older generation must not block a new execution.
        let exact = operation
            .as_ref()
            .filter(|operation| operation.execution_generation == generation);
        let reason = match state {
            Some(SandboxState::Quarantined) => Some(crate::db::TeardownSuspendFailure::Quarantined),
            Some(SandboxState::Deleting) => Some(crate::db::TeardownSuspendFailure::Deleting),
            None if crate::db::now_epoch_i64()
                .and_then(|now| {
                    crate::db::read::sandbox_view(&db, sandbox_id)
                        .delete_receipts()
                        .execution_covers(generation, now)
                })
                .map_err(|error| Status::internal(format!("db error: {error}")))? =>
            {
                return Ok(TerminalTeardownSuspend::Deleted);
            }
            None if exact.is_some() => Some(crate::db::TeardownSuspendFailure::Missing),
            _ if exact.is_some_and(|operation| {
                operation.status == crate::db::TeardownSuspendOperationStatus::Failed
            }) && !crate::db::read::sandbox_view(&db, sandbox_id)
                .teardown()
                .retry_admissible(generation)
                .map_err(|error| Status::internal(format!("db error: {error}")))? =>
            {
                Some(crate::db::TeardownSuspendFailure::Failed)
            }
            _ => None,
        };
        let Some(reason) = reason else {
            return Ok(TerminalTeardownSuspend::Open);
        };
        crate::db::fail_teardown_suspend_operation(&db, sandbox_id, generation, reason).map_err(
            |error| Status::internal(format!("persist terminal teardown suspend: {error}")),
        )?;
        if reason == crate::db::TeardownSuspendFailure::Deleting {
            return Ok(TerminalTeardownSuspend::AwaitingDelete);
        }
        Err(Status::failed_precondition(reason.as_str()))
    }

    /// Resume, every [`TEARDOWN_SUSPEND_RESUME_INTERVAL`], the teardown of a
    /// `suspending` row no poll drives (see
    /// [`Self::resume_teardown_suspend_drivers`]). The first pass runs at once,
    /// so a restart's deferred rows resume as soon as startup recovery ends.
    pub fn start_teardown_suspend_resumer(
        &self,
        shutdown_token: tokio_util::sync::CancellationToken,
        task_tracker: &tokio_util::task::TaskTracker,
    ) {
        let service = self.clone();
        task_tracker.spawn(async move {
            // The resumer is the only owner of a row whose pod no longer
            // polls, so its exit before shutdown drains the manager.
            let _guard = service.critical_task_guard("teardown suspend resumer", &shutdown_token);
            let mut interval = tokio::time::interval(TEARDOWN_SUSPEND_RESUME_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => return,
                    _ = interval.tick() => {
                        service.resume_teardown_suspend_drivers();
                    }
                }
            }
        });
    }

    /// Resume the teardown-suspend driver of every `suspending` row an active
    /// teardown job owns. Startup leaves such
    /// a row, its runtime still live, to that job (see
    /// `crate::db::startup_cleanup_intent`), and nothing else starts a driver
    /// but the shim's `PrepareTeardownSuspend` poll. A pod force-deleted while
    /// sandbox-manager was down never polls again, and without this the row
    /// would keep its runtime resident with no owner. A driver admitted here
    /// runs as one the poll admitted would, under the same retry pacing, so a
    /// later poll joins it and a row whose retry isn't due yet waits for a
    /// later pass.
    ///
    /// Returns the rows whose driver now runs, started here or already.
    pub(crate) fn resume_teardown_suspend_drivers(&self) -> Vec<SandboxId> {
        let rows = match self.db_conn().and_then(|db| {
            crate::db::scan::teardown_owned_suspending(&db)
                .map_err(|error| Status::internal(format!("db error: {error}")))
        }) {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(error = %error.message(),
                    "cannot list suspending rows to resume their teardowns; their next poll will");
                return Vec::new();
            }
        };
        rows.into_iter()
            .filter_map(|row| match row.sandbox_id.parse::<SandboxId>() {
                Ok(sandbox_id) => Some((sandbox_id, row.execution_generation)),
                Err(error) => {
                    tracing::warn!(sandbox_id = %row.sandbox_id, %error,
                        "skipping a suspending row with an invalid id");
                    None
                }
            })
            .filter_map(|(sandbox_id, execution_generation)| {
                let admission = self.spawn_teardown_suspend_driver(
                    sandbox_id,
                    execution_generation,
                    SandboxEventTrigger::System,
                );
                if admission == TeardownSuspendDriverAdmission::Started {
                    tracing::info!(%sandbox_id, %execution_generation,
                        "resumed the teardown of a suspending row no poll had driven");
                } else {
                    tracing::debug!(%sandbox_id, %execution_generation,
                        admission = admission.as_str(), "suspending row's teardown not resumed");
                }
                matches!(
                    admission,
                    TeardownSuspendDriverAdmission::Started
                        | TeardownSuspendDriverAdmission::Existing
                )
                .then_some(sandbox_id)
            })
            .collect()
    }

    pub(in crate::service) fn spawn_teardown_suspend_driver(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> TeardownSuspendDriverAdmission {
        let due = self.db_conn().and_then(|db| {
            crate::db::read::sandbox_view(&db, &sandbox_id.to_string())
                .teardown()
                .retry_due(execution_generation)
                .map_err(|error| Status::internal(format!("read teardown retry deadline: {error}")))
        });
        match due {
            Ok(true) => {}
            Ok(false) => return TeardownSuspendDriverAdmission::Deferred,
            Err(error) => {
                tracing::warn!(%sandbox_id, %error, "cannot read teardown retry deadline");
                return TeardownSuspendDriverAdmission::Deferred;
            }
        }
        let dashmap::mapref::entry::Entry::Vacant(entry) =
            self.teardown_suspend_drivers.entry(sandbox_id)
        else {
            return TeardownSuspendDriverAdmission::Existing;
        };
        let permit = match Arc::clone(&self.teardown_suspend_driver_sem).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                pplx_space_metrics::sandbox::worker::teardown_suspend_driver_saturated();
                tracing::debug!(
                    %sandbox_id,
                        "generation suspend driver capacity is full; caller polling will retry admission"
                );
                return TeardownSuspendDriverAdmission::Saturated;
            }
        };
        entry.insert(permit);

        let driver_slot = TeardownSuspendDriverSlot {
            drivers: Arc::clone(&self.teardown_suspend_drivers),
            sandbox_id,
        };
        let service = self.clone();
        let shutdown_token = self.shutdown_token.clone();
        // The driver outlives the RPC that admits it, so it is a root linked
        // to that RPC, and the backup job it waits for runs under it.
        let span =
            super::super::drain::detached_trace_span("TeardownSuspend", &sandbox_id.to_string());
        self.task_tracker.spawn(
            async move {
                let _driver_slot = driver_slot;
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        tracing::info!(
                            %sandbox_id,
                            "stopped generation suspend driver during service shutdown"
                        );
                    }
                    result = service.drive_teardown_suspend(
                        sandbox_id,
                        execution_generation,
                        event_trigger,
                    ) => {
                        if let Err(error) = result {
                            tracing::warn!(
                                %sandbox_id,
                                code = %error.code(),
                                error = %error.message(),
                                "generation suspend did not become durable"
                            );
                            super::super::drain::record_detached_failure(error.message());
                        }
                    }
                }
            }
            .instrument(span),
        );
        TeardownSuspendDriverAdmission::Started
    }

    pub(super) async fn drive_teardown_suspend(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> Result<(), Status> {
        self.drive_teardown_suspend_in(
            sandbox_id,
            execution_generation,
            event_trigger,
            std::path::Path::new(CACHE_DIR),
        )
        .await
    }

    /// [`Self::drive_teardown_suspend`] with the sandbox cache rooted at
    /// `cache_dir`, which only tests change.
    pub(super) async fn drive_teardown_suspend_in(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
        cache_dir: &std::path::Path,
    ) -> Result<(), Status> {
        let _state_writer = self.state_writer_lease("PrepareTeardownSuspendBackground")?;
        let result = self
            .drive_teardown_suspend_inner(
                sandbox_id,
                execution_generation,
                event_trigger,
                cache_dir,
            )
            .await;
        self.settle_teardown_suspend_drive(sandbox_id, execution_generation, result)
            .await
    }

    /// The cluster's verdict on the backup refusal a teardown-suspend drive
    /// failed with, or `NotSuperseded` when it didn't fail on one.
    ///
    /// A sandbox that still exists can have refused the backup because the
    /// cluster retired this execution. A refusal the drive met directly, or
    /// re-asked about before rejoining it, carries that verdict, unless the
    /// re-ask went unanswered: the settle asks once more rather than wait a
    /// retry for a cluster that may have answered since. A permanent job
    /// failure no longer says so, so ask again with the upload's own
    /// idempotent registration.
    async fn teardown_drive_refusal_verdict(
        &self,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        error: &Status,
    ) -> crate::snapshot_upload_jobs::RegistrationProbe {
        use crate::snapshot_upload_jobs::RegistrationProbe;
        let Some(failed_artifact) = teardown_failed_backup_artifact(error) else {
            return RegistrationProbe::NotSuperseded;
        };
        if let Some(verdict) = teardown_backup_drive_verdict(error)
            && verdict != RegistrationProbe::Unanswered
        {
            return verdict;
        }
        match self.teardown_backup_payload_bound_to(execution_generation, failed_artifact) {
            Some(payload) => {
                crate::snapshot_upload_jobs::prepared_begin_refused_as_superseded(&payload).await
            }
            None => RegistrationProbe::NotSuperseded,
        }
    }

    /// Settle a teardown-suspend drive's outcome: a backup refusal is checked
    /// against the cluster, which may have retired this execution, before
    /// the failure is recorded.
    pub(super) async fn settle_teardown_suspend_drive(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        result: Result<(), Status>,
    ) -> Result<(), Status> {
        let Err(error) = &result else {
            return result;
        };
        let verdict = self
            .teardown_drive_refusal_verdict(execution_generation, error)
            .await;
        // Registration can also return NotFound for its source node. Confirm
        // sandbox absence with the sandbox-only lookup before making this final.
        // Legacy persisted failures have no typed reason, so verify every
        // permanent backup refusal rather than parsing its historical message.
        let failed_artifact = teardown_failed_backup_artifact(error);
        let backup_lookup = if failed_artifact.is_some() {
            Some(
                self.resolve_org_snapshot_upload_config(&sandbox_id.to_string())
                    .await,
            )
        } else {
            None
        };
        // Use the verdict even when the lookup reported the row missing: that
        // lookup only finds a sandbox placed on this node, so a node the
        // sandbox has moved off sees it as missing, while registration looks
        // the sandbox up wherever it is placed.
        use crate::snapshot_upload_jobs::RegistrationProbe;
        let mut release = None;
        let mut backup_lookup = backup_lookup;
        match verdict {
            RegistrationProbe::Superseded(generation)
                if failed_artifact.is_some() && generation == execution_generation =>
            {
                release = failed_artifact;
                // The verdict proves the cluster row exists, so a failed
                // release mustn't be recorded as a backup refused for a
                // missing sandbox.
                backup_lookup = None;
            }
            // Without an answer, a lookup that can't see a sandbox placed on
            // another node doesn't prove the sandbox is gone. Leave the
            // teardown running so the next drive asks again.
            RegistrationProbe::Unanswered => backup_lookup = None,
            // A verdict about another generation, or an admission, still
            // proves the row exists. An admitted refusal of the artifact is
            // the next drive's to register again (see
            // `ensure_teardown_suspend_backup`), in either arm.
            RegistrationProbe::Superseded(_) | RegistrationProbe::Admitted => backup_lookup = None,
            RegistrationProbe::NotSuperseded => {}
        }
        self.settle_teardown_suspend_driver_failure(
            sandbox_id,
            execution_generation,
            result,
            backup_lookup.as_ref(),
            release,
        )
        .await
    }

    /// Settle a failed teardown-suspend drive. `release` names the staged
    /// artifact whose backup the cluster refused because it retired exactly
    /// this generation: the runtime is released and the capture discarded
    /// unpublished (see [`Self::release_retired_teardown_execution`]).
    /// Otherwise the failure is recorded for the caller's next poll and the
    /// drive's error stands.
    pub(super) async fn settle_teardown_suspend_driver_failure(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        result: Result<(), Status>,
        backup_lookup: Option<&OrgSnapshotConfigResolution>,
        release: Option<LocalArtifactId>,
    ) -> Result<(), Status> {
        let Err(error) = &result else {
            return result;
        };
        let released = match release {
            Some(staged_artifact) => Some(
                self.release_retired_teardown_execution(
                    &sandbox_id.to_string(),
                    execution_generation,
                    staged_artifact,
                )
                .await,
            ),
            None => None,
        };
        // A completed release leaves nothing to record. Any other outcome,
        // whether a claim that lost to a row that moved on, a refusal before
        // the claim, or a cleanup that failed after it, still records the
        // drive's failure, which is idempotent and closes the job for a row
        // that is now quarantined, deleting or gone.
        let result = match released {
            Some(Ok(())) => Ok(()),
            released => {
                if let Err(record_error) = self.record_teardown_suspend_driver_failure(
                    sandbox_id,
                    execution_generation,
                    error,
                    backup_lookup,
                ) {
                    tracing::warn!(%sandbox_id, %execution_generation, %record_error,
                        "failed to persist teardown suspend driver failure; caller polling will retry");
                }
                match released {
                    Some(Err(status)) => {
                        // The release's status replaces the drive's, so keep
                        // the refusal that prompted it in the log.
                        tracing::warn!(%sandbox_id, %execution_generation,
                            backup_error = %error.message(), release_error = %status.message(),
                            "retired execution's teardown release failed");
                        Err(status)
                    }
                    _ => result,
                }
            }
        };
        // A completed release leaves no active job, so this paces only a drive
        // that is still failing.
        if let Err(error) = &result
            && let Err(error) = self.db_conn().and_then(|db| {
                crate::db::defer_teardown_suspend_retry(
                    &db,
                    &sandbox_id.to_string(),
                    execution_generation,
                    error.message(),
                )
                .map_err(|error| {
                    Status::internal(format!("persist teardown retry deadline: {error}"))
                })
            })
        {
            tracing::warn!(%sandbox_id, %error, "cannot persist teardown retry deadline");
        }
        result
    }

    pub(super) fn record_teardown_suspend_driver_failure(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        error: &Status,
        backup_lookup: Option<&OrgSnapshotConfigResolution>,
    ) -> Result<(), Status> {
        let db = self.db_conn()?;
        let local = crate::db::read::sandbox(&db, &sandbox_id.to_string())
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
        let reason = match local {
            None => Some(crate::db::TeardownSuspendFailure::Missing),
            Some(row) if row.state == SandboxState::Quarantined => {
                Some(crate::db::TeardownSuspendFailure::Quarantined)
            }
            Some(row) if row.state == SandboxState::Deleting => {
                Some(crate::db::TeardownSuspendFailure::Deleting)
            }
            _ => None,
        };
        if let Some(reason) = reason {
            crate::db::fail_teardown_suspend_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
                reason,
            )
            .map_err(|error| {
                Status::internal(format!("persist terminal teardown suspend: {error}"))
            })?;
        } else if let Some(failed_artifact) = teardown_failed_backup_artifact(error)
            && matches!(
                backup_lookup,
                Some(OrgSnapshotConfigResolution::ClusterRowMissing)
            )
        {
            let failed = crate::db::fail_teardown_suspend_backup_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
                failed_artifact,
            )
            .map_err(|error| {
                Status::internal(format!("persist terminal teardown backup: {error}"))
            })?;
            drop(db);
            if failed {
                mark_sandbox_dirty(&self.db, &sandbox_id.to_string());
            }
        }
        Ok(())
    }

    async fn drive_teardown_suspend_inner(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
        cache_dir: &std::path::Path,
    ) -> Result<(), Status> {
        let sandbox_id_string = sandbox_id.to_string();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox_id_string);
        let wake_guard = wake_lock.lock_owned().await;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox_id_string);
        let artifact_guard = artifact_lock.lock_owned().await;
        let status = {
            let db = self.db_conn()?;
            crate::db::read::sandbox_view(&db, &sandbox_id_string)
                .teardown()
                .status()
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?
        };
        if status.is_durable_for(execution_generation) {
            return Ok(());
        }
        if status.is_ready_for(execution_generation) {
            self.complete_teardown_suspend_operation(
                &sandbox_id_string,
                execution_generation,
                event_trigger,
            )?;
            return Ok(());
        }
        self.ensure_teardown_suspend_owner(&sandbox_id_string, execution_generation)?;
        // A publication this generation began but could not finish (an
        // encrypted capture that did not seal) resumes through the same
        // recovery as any interrupted publication, which registers the staged
        // artifact under this generation. The artifact arm below then backs
        // it up and finalizes the runtime.
        let (wake_guard, artifact_guard, status) =
            if status.state == SandboxState::Suspending && status.artifact.is_none() {
                let (wake_guard, artifact_guard) = self
                    .recover_teardown_suspend_publication(
                        &sandbox_id_string,
                        cache_dir,
                        (wake_guard, artifact_guard),
                    )
                    .await?;
                let status = {
                    let db = self.db_conn()?;
                    crate::db::read::sandbox_view(&db, &sandbox_id_string)
                        .teardown()
                        .status()
                        .map_err(|error| Status::internal(format!("db error: {error}")))?
                        .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?
                };
                if status.state == SandboxState::Suspending && status.artifact.is_none() {
                    return Err(Status::unavailable(
                        "teardown suspend publication is still pending",
                    ));
                }
                (wake_guard, artifact_guard, status)
            } else {
                (wake_guard, artifact_guard, status)
            };
        let active_generation = Some(execution_generation);
        let result: Result<(), Status> = match (status.state, status.artifact) {
            (SandboxState::Started | SandboxState::Paused, _) => {
                let context = SuspendRequestContext::for_teardown(
                    sandbox_id_string.clone(),
                    execution_generation,
                );
                let (_, publication) = self
                    .prepare_suspend_snapshot_publication_after_locks(
                        &context,
                        &wake_guard,
                        &artifact_guard,
                    )
                    .await?;
                self.complete_suspend_snapshot_publication(publication)
                    .await
            }
            (
                SandboxState::Suspending
                | SandboxState::CleanupRequired
                | SandboxState::CleanupFailed,
                Some(artifact),
            ) if active_generation == Some(execution_generation) => {
                let sandbox = self.lookup_sandbox(&sandbox_id_string)?;
                let format = reupload_format_from_sidecar(&artifact.local_path)?;
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &artifact,
                        format,
                        origin: TeardownBackupOrigin::Captured,
                    },
                )
                .await?;
                self.ensure_teardown_suspend_owner(&sandbox_id_string, execution_generation)?;
                self.finalize_suspended_runtime(
                    &sandbox,
                    artifact.local_artifact_id,
                    &artifact.local_path,
                    None,
                    None,
                )
                .await?;
                mark_sandbox_dirty(&self.db, &sandbox_id_string);
                Ok(())
            }
            (SandboxState::Suspended, Some(artifact))
                if active_generation == Some(execution_generation) =>
            {
                let sandbox = self.lookup_sandbox(&sandbox_id_string)?;
                let format = reupload_format_from_sidecar(&artifact.local_path)?;
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &artifact,
                        format,
                        origin: TeardownBackupOrigin::PreviouslySuspended,
                    },
                )
                .await
            }
            (state, _) => Err(Status::failed_precondition(format!(
                "cannot continue teardown suspend from {}",
                state
            ))),
        };
        result?;
        self.complete_teardown_suspend_operation(
            &sandbox_id_string,
            execution_generation,
            event_trigger,
        )?;
        Ok(())
    }

    /// Seal a recorded encrypted capture, then run the interrupted-publication
    /// recovery the periodic pass uses, with retained-source verdicts and off
    /// the async workers as it does. The row's locks, its cleanup fence and a
    /// state-writer lease move into the blocking work, so a shutdown that
    /// drops this future cannot release them under a live publish. The locks
    /// come back once it finishes. A still-pending artifact leaves the row
    /// suspending, and the driver's deferred retry comes back here.
    async fn recover_teardown_suspend_publication(
        &self,
        sandbox_id: &str,
        cache_dir: &std::path::Path,
        locks: TeardownSuspendLocks,
    ) -> Result<TeardownSuspendLocks, Status> {
        let CleanupLockTryResult::Acquired(cleanup) = self.try_cleanup_fence(sandbox_id) else {
            return Err(Status::unavailable("sandbox cleanup is in progress"));
        };
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        self.seal_recorded_encrypted_capture(&sandbox, cache_dir)
            .await
            .map_err(|error| {
                Status::unavailable(format!(
                    "encrypted suspend capture is still unsealed: {error}"
                ))
            })?;
        // Periodic recovery skips teardown-owned rows, so a deferred
        // btrfs-send capture's retained source is inspected only here.
        let verdicts = self
            .inspect_retained_sources_in(vec![cache_dir.join(sandbox_id)], &mut Default::default())
            .await;
        let state_writer = self.state_writer_lease("TeardownSuspendPublicationRecovery")?;
        let (service, cache_dir) = (self.clone(), cache_dir.to_path_buf());
        let (recovered, locks) = tokio::task::spawn_blocking(move || {
            let _fences = (cleanup, state_writer);
            let recovered = service.recover_interrupted_suspend_snapshot_publication(
                &cache_dir,
                sandbox,
                SuspendSnapshotPublicationRecoveryOrigin::TeardownRetry,
                &verdicts,
            );
            (recovered, locks)
        })
        .await
        .map_err(|error| {
            Status::internal(format!("suspend publication recovery task failed: {error}"))
        })?;
        recovered.map_err(|error| {
            Status::unavailable(format!(
                "interrupted suspend publication remains deferred: {error}"
            ))
        })?;
        Ok(locks)
    }

    fn ensure_teardown_suspend_owner(
        &self,
        sandbox_id: &str,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    ) -> Result<(), Status> {
        let status = {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::internal("database mutex poisoned"))?;
            crate::db::read::sandbox_view(&db, sandbox_id)
                .teardown()
                .status()
                .map_err(|error| Status::internal(format!("db error: {error}")))?
        };
        let owns_runtime = status
            .and_then(|status| status.operation)
            .is_some_and(|operation| {
                operation.execution_generation == execution_generation
                    && operation.status == crate::db::TeardownSuspendOperationStatus::Running
            });
        if owns_runtime {
            Ok(())
        } else {
            Err(Status::aborted(
                "execution generation no longer owns sandbox",
            ))
        }
    }

    pub(super) fn ensure_teardown_suspend_owner_for_durability(
        &self,
        sandbox_id: &str,
        durability: SuspendDurability,
    ) -> Result<(), Status> {
        if let Some(execution_generation) = durability.execution_generation() {
            self.ensure_teardown_suspend_owner(sandbox_id, execution_generation)?;
        }
        Ok(())
    }

    fn complete_teardown_suspend_operation(
        &self,
        sandbox_id: &str,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> Result<(), Status> {
        let completed = {
            let db = self.db_conn()?;
            let status = crate::db::read::sandbox_view(&db, sandbox_id)
                .teardown()
                .status()
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
            let is_ready = status.is_ready_for(execution_generation);
            let artifact_id = status
                .artifact
                .filter(|_| is_ready)
                .map(|artifact| artifact.local_artifact_id)
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "generation suspend artifact is not remotely durable",
                    )
                })?;
            let completed = crate::db::complete_teardown_suspend(
                &db,
                sandbox_id,
                execution_generation,
                Some(artifact_id),
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
            if !completed {
                crate::db::bind_completed_teardown_suspend_artifact(
                    &db,
                    sandbox_id,
                    execution_generation,
                    artifact_id,
                )
                .map_err(|error| Status::internal(format!("db error: {error}")))?;
            }
            completed
        };
        if completed {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::ExecutionSuspendCompleted,
                SandboxEventSource::SbxManager,
                teardown_suspend_event_details(execution_generation, event_trigger)
                    .result("durable")
                    .with_to_state(SandboxState::Suspended.to_string()),
            );
        }
        Ok(())
    }

    pub(super) async fn ensure_teardown_suspend_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        mut backup: TeardownSuspendBackup<'_>,
    ) -> Result<(), Status> {
        let persisted_job = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &crate::snapshot_upload_jobs::artifact_backup_dedup_key(
                backup.artifact.local_artifact_id,
            ),
        )?;
        // An active local upload still owns its completion. Only probe before
        // enqueueing or joining a terminal job: a restored artifact can already
        // be durable without a local completion marker. A failed probe leaves
        // the ordinary upload path responsible for durability.
        if persisted_job.as_ref().is_none_or(|job| job.is_terminal())
            && matches!(
                self.existing_ready_artifact_backup_response(backup.artifact)
                    .await,
                Ok(Some(_))
            )
        {
            return self.verify_teardown_suspend_backup_ownership(sandbox, &backup);
        }
        // A previously-suspended backup is this execution's own capture when
        // the capture's record, or the upload that registered it, names this
        // generation: a row startup recovery marked `suspended` behind its
        // running teardown. Every new attempt of such a capture registers under
        // the generation, so the fence that refused it keeps refusing it,
        // whether the attempt follows a retriable failure, an admission, or
        // the refused upload's pruning. A capture an earlier execution took
        // keeps registering unfenced (see `TeardownBackupOrigin`). A record or
        // upload that can't be read can't vouch for its generation: the drive
        // still joins an upload already registered, which registers nothing
        // new, but refuses to start an unfenced attempt, as eviction's
        // re-upload does, rather than risk registering a superseded capture.
        let mut unvouched = None;
        if backup.origin == TeardownBackupOrigin::PreviouslySuspended {
            match self.capture_execution_generation(
                sandbox,
                backup.artifact,
                persisted_job.as_ref(),
            ) {
                Ok(generation) if generation == Some(backup.execution_generation) => {
                    backup.origin = TeardownBackupOrigin::Captured;
                }
                Ok(_) => {}
                Err(error) => unvouched = Some(error),
            }
        }
        // The verdict a rejoined refusal leaves with, so the settle needn't
        // ask the cluster again.
        let mut verdict = None;
        let joined = match persisted_job {
            Some(job) => match self.teardown_use_of_upload(&backup, &job)? {
                KeyedUploadUse::Join => {
                    use crate::snapshot_upload_jobs::RegistrationProbe;
                    match self.teardown_backup_refusal_verdict(&backup, &job).await {
                        // The cluster refused this execution's registration, and
                        // its fence now admits it: a supersession lapsed when
                        // every execution that superseded this one ended
                        // without running. Rejoining would replay the stale
                        // refusal on every drive and hold the runtime, and the
                        // pod the next wake waits on, for good. The refusal
                        // clears only when the new attempt will register
                        // under this generation, so the fence that admitted
                        // the probe decides it too; otherwise the drive
                        // rejoins, and the refusal stands.
                        Some(RegistrationProbe::Admitted)
                            if backup.registration_generation()
                                == Some(backup.execution_generation)
                                && self.clear_teardown_suspend_backup_permanent_failure(
                                    &sandbox.id,
                                    backup.artifact.local_artifact_id,
                                ) =>
                        {
                            tracing::info!(sandbox_id = %sandbox.id, job_id = %job.job_id,
                                "the cluster now admits a teardown backup it refused; \
                                 starting a new upload");
                            None
                        }
                        probe => {
                            if let Some(probe) = probe {
                                record_refusal_replay(&sandbox.id, &job.job_id, probe);
                                verdict = Some(probe);
                            }
                            Some(job.job_id)
                        }
                    }
                }
                KeyedUploadUse::NewAttempt => None,
            },
            None => None,
        };
        if joined.is_none()
            && let Some(error) = unvouched
        {
            return Err(error);
        }
        let submission = async {
            match joined {
                Some(job_id) => Ok(job_id),
                None => match self
                    .enqueue_suspend_artifact_backup(
                        sandbox,
                        backup.artifact.local_artifact_id,
                        &backup.artifact.local_path,
                        backup.format,
                        backup.registration_generation(),
                    )
                    .await
                    .map_err(|error| {
                        teardown_artifact_backup_status(error, backup.artifact.local_artifact_id)
                    })? {
                    SubmitSuspendSnapshotOutcome::Enqueued { job_id, .. } => Ok(job_id),
                    SubmitSuspendSnapshotOutcome::NoUploadConfig => {
                        Err(Status::failed_precondition(
                            "generation suspend requires a configured snapshot upload destination",
                        ))
                    }
                },
            }
        };
        let job = self
            .snapshot_upload_worker
            .submit_and_wait(submission, self.snapshot_upload_worker.worst_case_runtime())
            .await?;
        if !job.is_terminal() || job.failure.is_some() {
            // Only a failure the next drive would rejoin is final. One that
            // gets a new attempt leaves the row unmarked, and so does a failed
            // lookup of that rule: the next drive rejoins and marks it then.
            if matches!(
                job.failure,
                Some(pplx_space_common_jobs::JobFailure::Permanent { .. })
            ) && self
                .teardown_use_of_upload(&backup, &job)
                .is_ok_and(|then| then == KeyedUploadUse::Join)
            {
                self.record_teardown_suspend_backup_permanent_failure(
                    &sandbox.id,
                    backup.artifact.local_artifact_id,
                );
            }
            let mut error = Status::internal(
                job.error
                    .unwrap_or_else(|| "durable suspend backup failed".to_string()),
            );
            if matches!(
                job.failure,
                Some(pplx_space_common_jobs::JobFailure::Permanent { .. })
            ) {
                error = teardown_backup_failure_status(error, backup.artifact.local_artifact_id);
                if let Some(verdict) = verdict {
                    error = teardown_backup_verdict_status(error, verdict);
                }
            }
            return Err(error);
        }
        {
            let db = self.db_conn()?;
            crate::db::mark_local_artifact_backup_completed(
                &db,
                &backup.artifact.local_artifact_id.to_string(),
            )
            .map_err(|error| {
                Status::internal(format!("persist durable suspend completion: {error}"))
            })?;
        }
        self.verify_teardown_suspend_backup_ownership(sandbox, &backup)
    }

    /// [`teardown_keyed_upload_use`] for `job`, the newest upload under the
    /// artifact's key.
    pub(super) fn teardown_use_of_upload(
        &self,
        backup: &TeardownSuspendBackup<'_>,
        job: &pplx_space_common_jobs::Job,
    ) -> Result<KeyedUploadUse, Status> {
        #[derive(serde::Deserialize)]
        struct RecordedScope {
            #[serde(default)]
            failure_scope: crate::snapshot_upload::UploadFailureScope,
        }
        let status = job
            .status
            .parse::<pplx_space_common_jobs::JobStatus>()
            .map_err(|error| {
                Status::internal(format!(
                    "persisted artifact backup status is invalid for {}: {error}",
                    backup.artifact.local_artifact_id
                ))
            })?;
        // Unreadable means unrecorded, and an unrecorded scope is the attempt's.
        let scope = job
            .payload_as::<RecordedScope>()
            .map_or_else(|_| Default::default(), |recorded| recorded.failure_scope);
        let permanently_failed = {
            let db = self.db_conn()?;
            crate::db::snapshot_upload_jobs::count_permanent_upload_failures_outside_scope(
                &db,
                &crate::snapshot_upload_jobs::artifact_backup_dedup_key(
                    backup.artifact.local_artifact_id,
                ),
                crate::snapshot_upload::UploadFailureScope::Artifact.as_str(),
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))?
        };
        Ok(teardown_keyed_upload_use(
            status,
            job.failure,
            scope,
            permanently_failed,
        ))
    }

    fn verify_teardown_suspend_backup_ownership(
        &self,
        sandbox: &crate::db::SandboxRow,
        backup: &TeardownSuspendBackup<'_>,
    ) -> Result<(), Status> {
        let still_owned = {
            let db = self.db_conn()?;
            crate::db::read::sandbox_view(&db, &sandbox.id)
                .teardown()
                .status()
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .is_some_and(|status| {
                    // Once the artifact is stamped, the next PrepareTeardownSuspend
                    // poll can complete this generation before this waiter sees
                    // its upload finish. That completion is this suspend's.
                    let completed_here = status.is_durable_for(backup.execution_generation);
                    status.backup_completed
                        && (completed_here
                            || status.operation.as_ref().is_some_and(|operation| {
                                operation.status.is_active()
                                    && operation.execution_generation == backup.execution_generation
                            }))
                        && status.artifact.is_some_and(|artifact| {
                            artifact.local_artifact_id == backup.artifact.local_artifact_id
                        })
                })
        };
        if !still_owned {
            return Err(Status::aborted(
                "generation suspend no longer owns durable suspend artifact",
            ));
        }
        Ok(())
    }
}

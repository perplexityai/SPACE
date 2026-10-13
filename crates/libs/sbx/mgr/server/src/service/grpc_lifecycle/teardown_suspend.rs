//! `PrepareTeardownSuspend` RPC and the teardown-suspend driver, including the
//! remote backup fence that must hold before runtime teardown.

use super::*;

pub(in crate::service) const SUSPEND_TRIGGER_METADATA: &str = "x-space-suspend-trigger";

pub(in crate::service) fn teardown_backup_failure_requires_new_attempt(
    failure: Option<pplx_space_common_jobs::JobFailure>,
) -> bool {
    matches!(failure, Some(pplx_space_common_jobs::JobFailure::Retriable))
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

const TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA: &str =
    "x-space-teardown-backup-failed-artifact-bin";

pub(super) fn teardown_artifact_backup_status(
    error: ArtifactBackupSubmissionError,
    artifact: LocalArtifactId,
) -> Status {
    let permanent = matches!(&error, ArtifactBackupSubmissionError::Begin(error)
        if error.kind() == pplx_space_common_jobs::JobFailureKind::Permanent);
    let mut status = artifact_backup_fence_status(error.to_string());
    if permanent {
        status = teardown_backup_failure_status(status, artifact);
    }
    status
}

// The shared publication path returns tonic Status. Keep backup provenance on
// its teardown-only errors until the driver has verified control-plane absence.
fn teardown_backup_failure_status(mut status: Status, artifact: LocalArtifactId) -> Status {
    status.metadata_mut().insert_bin(
        TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA,
        tonic::metadata::MetadataValue::from_bytes(artifact.as_uuid().as_bytes()),
    );
    status
}

fn teardown_failed_backup_artifact(status: &Status) -> Option<LocalArtifactId> {
    let value = status
        .metadata()
        .get_bin(TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA)?
        .to_bytes()
        .ok()?;
    Some(LocalArtifactId::from_uuid(
        uuid::Uuid::from_slice(&value).ok()?,
    ))
}

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
            crate::db::teardown_release_evidence(&db, &sandbox_id_string, execution_generation)
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
                Err(rusqlite::Error::InvalidQuery) => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
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
                Err(rusqlite::Error::InvalidQuery) => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
            bound_existing_suspend = true;
        }
        if bound_existing_suspend {
            emit_sandbox_event(
                &sandbox_id.to_string(),
                SandboxEventType::SuspendRequested,
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
                crate::db::teardown_suspend_status(&db, &sandbox_id_string)
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
        let state = crate::db::get_sandbox(&db, sandbox_id)
            .map_err(|error| Status::internal(format!("db error: {error}")))?
            .map(|row| row.state);
        let operation = crate::db::latest_teardown_suspend_operation(&db, sandbox_id)
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
        // A failed older generation must not block a new execution.
        let exact = operation
            .as_ref()
            .filter(|operation| operation.execution_generation == generation);
        let reason = match state {
            Some(SandboxState::Quarantined) => Some(crate::db::TeardownSuspendFailure::Quarantined),
            Some(SandboxState::Deleting) => Some(crate::db::TeardownSuspendFailure::Deleting),
            None if crate::db::execution_delete_receipt_covers(&db, sandbox_id, generation)
                .map_err(|error| Status::internal(format!("db error: {error}")))? =>
            {
                return Ok(TerminalTeardownSuspend::Deleted);
            }
            None if exact.is_some() => Some(crate::db::TeardownSuspendFailure::Missing),
            _ if exact.is_some_and(|operation| {
                operation.status == crate::db::TeardownSuspendOperationStatus::Failed
            }) && !crate::db::teardown_suspend_retry_admissible(
                &db, sandbox_id, generation,
            )
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

    pub(in crate::service) fn spawn_teardown_suspend_driver(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> TeardownSuspendDriverAdmission {
        let due = self.db_conn().and_then(|db| {
            crate::db::teardown_suspend_retry_due(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
            )
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
        let _state_writer = self.state_writer_lease("PrepareTeardownSuspendBackground")?;
        let result = self
            .drive_teardown_suspend_inner(sandbox_id, execution_generation, event_trigger)
            .await;
        if let Err(error) = &result {
            // Registration can also return NotFound for its source node. Confirm
            // sandbox absence with the sandbox-only lookup before making this final.
            // Legacy persisted failures have no typed reason, so verify every
            // permanent backup refusal rather than parsing its historical message.
            let backup_lookup = if teardown_failed_backup_artifact(error).is_some() {
                Some(
                    self.resolve_org_snapshot_upload_config(&sandbox_id.to_string())
                        .await,
                )
            } else {
                None
            };
            if let Err(record_error) = self.record_teardown_suspend_driver_failure(
                sandbox_id,
                execution_generation,
                error,
                backup_lookup.as_ref(),
            ) {
                tracing::warn!(%sandbox_id, %execution_generation, %record_error,
                    "failed to persist teardown suspend driver failure; caller polling will retry");
            }
            if let Err(error) = self.db_conn().and_then(|db| {
                crate::db::defer_teardown_suspend_retry(
                    &db,
                    &sandbox_id.to_string(),
                    execution_generation,
                    error.message(),
                )
                .map_err(|error| {
                    Status::internal(format!("persist teardown retry deadline: {error}"))
                })
            }) {
                tracing::warn!(%sandbox_id, %error, "cannot persist teardown retry deadline");
            }
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
        let local = crate::db::get_sandbox(&db, &sandbox_id.to_string())
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
    ) -> Result<(), Status> {
        let sandbox_id_string = sandbox_id.to_string();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox_id_string);
        let wake_guard = wake_lock.lock_owned().await;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox_id_string);
        let artifact_guard = artifact_lock.lock_owned().await;
        let status = {
            let db = self.db_conn()?;
            crate::db::teardown_suspend_status(&db, &sandbox_id_string)
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
            crate::db::teardown_suspend_status(&db, sandbox_id)
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
            let status = crate::db::teardown_suspend_status(&db, sandbox_id)
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
                SandboxEventType::SuspendCompleted,
                SandboxEventSource::SbxManager,
                teardown_suspend_event_details(execution_generation, event_trigger)
                    .result("durable")
                    .with_to_state(SandboxState::Suspended.to_string()),
            );
        }
        Ok(())
    }

    /// Record that a teardown suspend's durable backup is over, on the row
    /// the shim keeps polling.
    ///
    /// A permanent job failure ends the artifact's upload for good:
    /// [`teardown_backup_failure_requires_new_attempt`] refuses a fresh
    /// attempt for it, so every later poll rejoins the same terminal job.
    /// The suspend therefore can never publish, the row can never leave
    /// `suspending`, and nothing on this node distinguishes it from a suspend
    /// still uploading — a distinction the cluster-deleted eviction needs
    /// before it may reclaim anything. The marker supplies exactly that
    /// terminality proof and nothing else; whether the sandbox is *gone*
    /// stays the cluster's call.
    ///
    /// Best-effort: a failed stamp leaves the pre-existing stuck row, which is
    /// the behavior without this marker at all, and the next poll retries.
    fn record_teardown_suspend_backup_permanent_failure(
        &self,
        sandbox_id: &str,
        staged_artifact: LocalArtifactId,
    ) {
        let marked = self.db_conn().and_then(|db| {
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &db,
                sandbox_id,
                staged_artifact,
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))
        });
        match marked {
            Ok(true) => {
                tracing::warn!(
                    sandbox_id,
                    %staged_artifact,
                    "generation suspend backup was permanently refused; marked the suspending \
                     row terminal so a cluster-deleted report can reclaim it"
                );
                mark_sandbox_dirty(&self.db, sandbox_id);
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    %staged_artifact,
                    %error,
                    "failed to mark a permanently refused generation suspend backup; the \
                     suspending row stays unreclaimable until a later attempt marks it"
                );
            }
        }
    }

    pub(super) async fn ensure_teardown_suspend_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        backup: TeardownSuspendBackup<'_>,
    ) -> Result<(), Status> {
        let artifact_dedup_key = backup.artifact.local_artifact_id.to_string();
        let persisted_job = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &artifact_dedup_key,
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
        let submission = async {
            match persisted_job {
                Some(job)
                    if !job.is_terminal()
                        || !teardown_backup_failure_requires_new_attempt(job.failure) =>
                {
                    Ok(job.job_id)
                }
                _ => match self
                    .enqueue_suspend_artifact_backup(
                        sandbox,
                        backup.artifact.local_artifact_id,
                        &backup.artifact.local_path,
                        backup.format,
                        Some(&artifact_dedup_key),
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
            if matches!(
                job.failure,
                Some(pplx_space_common_jobs::JobFailure::Permanent { .. })
            ) {
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

    fn verify_teardown_suspend_backup_ownership(
        &self,
        sandbox: &crate::db::SandboxRow,
        backup: &TeardownSuspendBackup<'_>,
    ) -> Result<(), Status> {
        let still_owned = {
            let db = self.db_conn()?;
            crate::db::teardown_suspend_status(&db, &sandbox.id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .is_some_and(|status| {
                    status.backup_completed
                        && status.operation.is_some_and(|operation| {
                            operation.status.is_active()
                                && operation.execution_generation == backup.execution_generation
                        })
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

//! `Pause`, `Resume`, and `Wake` RPCs, plus the pause reconciliation helpers
//! that suspend preparation reuses.

use super::*;
use crate::db::{Edge, edges};
use crate::service::cleanup_support::{
    StateWriteRejected, require_state_write_from, state_write_result, update_state_from_best_effort,
};

const PAUSE_RECONCILIATION_ATTEMPTS: usize = 3;

const PAUSE_RECONCILIATION_BACKOFF: Duration = Duration::from_millis(100);

/// Whether `rejected` found the row outside `edge`'s source states: another
/// operation, such as a Stop the cleanup fence admitted while pause held only
/// the artifact lock, moved the node-local SQLite `sandboxes.state` after
/// pause read it, and now owns the row. Decided on the found state, because
/// the generic guards answer a `stopping` row before the from-set does.
fn superseded(rejected: StateWriteRejected, edge: &Edge) -> bool {
    match rejected {
        StateWriteRejected::FromState { .. } => true,
        StateWriteRejected::Guard { found, .. } => !edge.from().contains(found),
        StateWriteRejected::Missing => false,
    }
}

/// Roll a `pausing` row back to `started`, best effort. Returns the rejection
/// when another operation moved the row out of `pausing`.
fn roll_back_pause_best_effort(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
) -> Option<StateWriteRejected> {
    let edge = &edges::PAUSE_ROLLBACK;
    let rejected =
        state_write_result(update_state_from_best_effort(db, sandbox_id, edge)?).err()?;
    superseded(rejected, edge).then_some(rejected)
}

#[cfg(test)]
pub(super) type GuestPreparation = Arc<
    dyn Fn(
            String,
        ) -> futures_util::future::BoxFuture<
            'static,
            Result<(), crate::guest_filesystem_flush::GuestFlushError>,
        > + Send
        + Sync,
>;

#[cfg(test)]
tokio::task_local! {
    /// Replaces the guest snapshot preparation of pause and the suspend
    /// auto-pause, which dials the host's per-sandbox spaced-host socket, and
    /// lets a test act between their state writes.
    pub(super) static GUEST_PREPARATION: GuestPreparation;
}

pub(super) async fn prepare_guest_for_pause(
    sandbox_id: &str,
    budget: GuestSnapshotPreparationBudget,
) -> Result<(), crate::guest_filesystem_flush::GuestFlushError> {
    #[cfg(test)]
    if let Ok(prepare) = GUEST_PREPARATION.try_with(Clone::clone) {
        return prepare(sandbox_id.to_owned()).await;
    }
    prepare_guest_for_vm_snapshot_via_spaced_host(sandbox_id, budget).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PauseReconciliationOutcome {
    Paused,
    Running,
    Unknown(String),
}

pub(super) fn pause_reconciliation_outcome(
    observation: &crate::runtime_reconciliation::VmStateObservation,
) -> PauseReconciliationOutcome {
    match observation.state() {
        Some(VmState::Paused) => PauseReconciliationOutcome::Paused,
        Some(VmState::Running) => PauseReconciliationOutcome::Running,
        Some(state @ (VmState::NotStarted | VmState::Shutdown)) => {
            PauseReconciliationOutcome::Unknown(format!("runtime reported {state}"))
        }
        None => PauseReconciliationOutcome::Unknown(
            observation
                .unobservable_reason()
                .unwrap_or("Firecracker state was unavailable")
                .to_string(),
        ),
    }
}

async fn reconcile_state_after_unconfirmed_pause(
    sandbox_id: &SandboxId,
    reason: &str,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> PauseReconciliationOutcome {
    let observation =
        observe_vm_state(sandbox_id, "pause response reconciliation", node_manager).await;
    let outcome = pause_reconciliation_outcome(&observation);
    tracing::warn!(
        sandbox_id = %sandbox_id,
        reason,
        reconciled_outcome = ?outcome,
        vm_state = %observation
            .state()
            .map_or_else(|| "unknown".to_string(), |state| state.to_string()),
        "reconciled sandbox state after unconfirmed pause"
    );
    outcome
}

/// Pause the VM of a `pausing` row, reconciling unconfirmed responses against
/// the observed VM state. Returns `paused` for the caller to complete, or the
/// state the row was left in: `started` after a rollback, or `quarantined`
/// when the VM state stays unknown. `Err` is the rejection when the rollback
/// or the quarantine finds that another operation moved the row out of
/// `pausing`; that operation owns the row.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent pause input; the node-manager client and connector are injected"
)]
pub(super) async fn pause_with_reconciliation(
    node_client: &mut NodeManagerGrpcClient,
    execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence>,
    db: &crate::db::SharedConnection,
    sandbox_id: &SandboxId,
    reason_context: &str,
    guest_preparation_budget: GuestSnapshotPreparationBudget,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    balloon_hint_timeout_ms: Option<std::num::NonZeroU32>,
) -> Result<SandboxState, StateWriteRejected> {
    let sandbox_id_string = sandbox_id.to_string();
    let mut last_reason = None;
    for attempt in 0..PAUSE_RECONCILIATION_ATTEMPTS {
        match node_client
            .pause_sandbox(PauseSandboxRequest {
                execution_fence: execution_fence.clone(),
                sandbox_id: sandbox_id_string.clone(),
                // A retry follows a failed or unconfirmed pause and must not
                // spend the hinting budget again.
                balloon_hint_timeout_ms: balloon_hint_timeout_ms
                    .filter(|_| attempt == 0)
                    .map(std::num::NonZeroU32::get),
            })
            .await
        {
            Ok(_) => return Ok(SandboxState::Paused),
            Err(error) => {
                let reason = format!("{reason_context}: {error}");
                last_reason = Some(reason.clone());
                let outcome =
                    reconcile_state_after_unconfirmed_pause(sandbox_id, &reason, node_manager)
                        .await;
                let retry_requires_prepare =
                    matches!(&outcome, PauseReconciliationOutcome::Running);
                match outcome {
                    PauseReconciliationOutcome::Paused => return Ok(SandboxState::Paused),
                    PauseReconciliationOutcome::Running
                        if attempt + 1 == PAUSE_RECONCILIATION_ATTEMPTS =>
                    {
                        return match roll_back_pause_best_effort(db, &sandbox_id_string) {
                            Some(rejected) => Err(rejected),
                            None => Ok(SandboxState::Started),
                        };
                    }
                    PauseReconciliationOutcome::Running => {}
                    PauseReconciliationOutcome::Unknown(detail) => {
                        last_reason = Some(format!("{reason}; {detail}"));
                    }
                }
                if attempt + 1 < PAUSE_RECONCILIATION_ATTEMPTS
                    && retry_requires_prepare
                    && let Err(error) =
                        prepare_guest_for_pause(&sandbox_id_string, guest_preparation_budget).await
                {
                    let superseded = roll_back_pause_best_effort(db, &sandbox_id_string);
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        %error,
                        "pause retry guest preparation failed"
                    );
                    return match superseded {
                        Some(rejected) => Err(rejected),
                        None => Ok(SandboxState::Started),
                    };
                }
            }
        }
        if attempt + 1 < PAUSE_RECONCILIATION_ATTEMPTS {
            tokio::time::sleep(PAUSE_RECONCILIATION_BACKOFF).await;
        }
    }

    let reason = last_reason.unwrap_or_else(|| reason_context.to_string());
    if !mark_quarantined_best_effort(
        db,
        &sandbox_id_string,
        SandboxState::Pausing,
        SandboxQuarantineReasonCode::PauseFailed,
        &reason,
    ) {
        // The quarantine compare-and-set refuses a row that left `pausing`,
        // which is how a Stop that killed the VM mid-pause surfaces here. It
        // also answers false on a poisoned connection or a database error, so
        // the row is read again: a row found outside `pausing`, a
        // `quarantined` one included, was moved by another writer. A failed
        // read, a missing row or a row still `pausing` reports pause's
        // quarantine.
        let found = crate::node_state_sync::connection_or_signal_failure(db)
            .ok()
            .and_then(|db| {
                crate::db::read::sandbox(&db, &sandbox_id_string)
                    .ok()
                    .flatten()
            })
            .map(|row| row.state);
        if let Some(found) = found
            && found != SandboxState::Pausing
        {
            return Err(StateWriteRejected::FromState { found });
        }
    }
    Ok(SandboxState::Quarantined)
}

impl SandboxManagerService {
    pub(in crate::service) async fn pause(
        &self,
        request: Request<PauseRequest>,
    ) -> Result<Response<PauseResponse>, Status> {
        let peer = self.authorize_peer(&request, "Pause", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = &request.get_ref().sandbox_id;

        let sandbox = self.lookup_sandbox(sandbox_id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Pause")?;
        let runtime: pplx_space_common_core::types::RuntimeKind = sandbox
            .runtime
            .parse()
            .map_err(|e| Status::failed_precondition(format!("runtime: {e}")))?;
        if !runtime.baseline_capabilities().pause_resume {
            return Err(Status::failed_precondition(format!(
                "runtime {runtime} does not support pause"
            )));
        }
        let mut event_details =
            SandboxEventDetails::default().with_from_state(sandbox.state.to_string());
        if let Some(trigger) = sandbox_event_trigger(&request) {
            event_details = event_details.trigger(trigger);
        }
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::PauseRequested,
            SandboxEventSource::SbxManager,
            event_details,
        );
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let _artifact_guard = artifact_lock.lock().await;
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Pause")?;
        sandbox
            .state
            .transition(SandboxState::Pausing)
            .map_err(Status::from)?;

        {
            // `lookup_sandbox` and this write take the database mutex
            // separately, so another worker (cleanup admission's blocking
            // task) can commit in between. The edge's `started` from-set
            // refuses whatever it left.
            let db = self.db_conn()?;
            require_state_write_from(
                crate::db::update_state_from(&db, &sandbox.id, &edges::PAUSE_BEGIN),
                "begin pause",
            )?;
        }
        self.watchdog.record_lifecycle_edge(&sandbox.id).await;

        if let Err(e) =
            prepare_guest_for_pause(&sandbox.id, GuestSnapshotPreparationBudget::Standard).await
        {
            const CONTEXT: &str = "roll back failed pause preparation";
            let db = self.db_conn()?;
            let edge = &edges::PAUSE_ROLLBACK;
            match crate::db::update_state_from(&db, &sandbox.id, edge).map(state_write_result) {
                Ok(Ok(_)) => {}
                // Guest preparation fails when a concurrent Stop kills the
                // VM; that Stop, not the preparation, decided the outcome.
                Ok(Err(rejected)) if superseded(rejected, edge) => {
                    return Err(rejected.into_status(CONTEXT));
                }
                Ok(Err(rejected)) => {
                    let error = rejected.into_status(CONTEXT);
                    tracing::warn!(sandbox_id = %sandbox.id, %error, "failed to roll back pause state");
                }
                Err(error) => {
                    tracing::warn!(sandbox_id = %sandbox.id, %error, "failed to roll back pause state");
                }
            }
            return Err(Status::internal(format!(
                "guest snapshot preparation before pause failed: {e}"
            )));
        }

        let mut node_client = match connect_node(&self.node_manager).await {
            Ok(node_client) => node_client,
            Err(e) => {
                if let Some(rejected) = roll_back_pause_best_effort(&self.db, &sandbox.id) {
                    return Err(
                        rejected.into_status("roll back pause after node-manager connect failure")
                    );
                }
                return Err(Status::internal(format!(
                    "cannot connect to node-manager: {e}"
                )));
            }
        };

        let sandbox_id = sandbox
            .id
            .parse::<SandboxId>()
            .map_err(|error| Status::internal(format!("invalid sandbox ID: {error}")))?;
        let pause_state = pause_with_reconciliation(
            &mut node_client,
            sandbox.node_execution_fence(),
            &self.db,
            &sandbox_id,
            "pause failed",
            GuestSnapshotPreparationBudget::Standard,
            &self.node_manager,
            None,
        )
        .await;
        if pause_state != Ok(SandboxState::Paused) {
            self.watchdog.record_lifecycle_edge(&sandbox.id).await;
        }
        let pause_state =
            pause_state.map_err(|rejected| rejected.into_status("reconcile unconfirmed pause"))?;
        if pause_state != SandboxState::Paused {
            return Err(Status::internal(format!(
                "pause failed; sandbox reconciled to {pause_state}"
            )));
        }

        {
            let db = self.db_conn()?;
            require_state_write_from(
                crate::db::update_state_from(&db, &sandbox.id, &edges::PAUSE_COMPLETE),
                "complete pause",
            )?;
        }

        // Pause snapshot scheduling
        self.snapshot_scheduler.remove_sandbox(&sandbox.id).await;
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox.id);
        }

        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::PauseCompleted,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default().with_to_state(SandboxState::Paused.to_string()),
        );
        Ok(Response::new(PauseResponse {}))
    }

    pub(in crate::service) async fn resume(
        &self,
        request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        let peer = self.authorize_peer(&request, "Resume", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let req = request.get_ref();
        let policy = req
            .activation_auto_lifecycle
            .as_ref()
            .map(policy_from_proto)
            .transpose()?;
        let generation = req
            .expected_execution_generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let resume_policy = match (policy.as_ref(), generation) {
            (Some(policy), generation) => {
                self.authorize_peer(
                    &request,
                    "Resume.activation_auto_lifecycle",
                    ALLOW_SANDBOX_START_CONTEXT,
                )?;
                Some((policy, generation))
            }
            (None, None) => None,
            _ => {
                return Err(Status::invalid_argument(
                    "resume execution generation requires a policy",
                ));
            }
        };
        let blocking = req.blocking;
        let sandbox_id = &req.sandbox_id;
        let ready_timeout_secs = optional_positive_or_default(
            "ready_timeout_secs",
            req.ready_timeout_secs,
            DEFAULT_RESUME_READY_TIMEOUT_SECS,
        )?;

        let sandbox = self.lookup_sandbox(sandbox_id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Resume")?;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = sandbox.id.clone();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let _guard = artifact_lock.lock().await;

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.authorize_template_manager_sandbox(&peer, &sandbox, "Resume")?;
        self.resume_after_wake_lock(&sandbox.id, resume_policy, blocking, ready_timeout_secs)
            .await?;
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox_id);
        }
        let sandbox = self.lookup_sandbox(&sandbox_id)?;
        Ok(Response::new(ResumeResponse {
            route_identity: space_api_route_identity_to_wire(&sandbox.space_api_route_identity),
        }))
    }

    pub(in crate::service) async fn wake(
        &self,
        request: Request<WakeRequest>,
    ) -> Result<Response<WakeResponse>, Status> {
        let req = request.get_ref();
        let target_state = wake_target_state_from_proto(req.target_state)?;
        let activity =
            match crate::proto::AutoLifecycleWakeSource::try_from(req.implicit_wake_source) {
                Ok(crate::proto::AutoLifecycleWakeSource::Unspecified) => None,
                Ok(crate::proto::AutoLifecycleWakeSource::Access) => {
                    Some(GuestActivityKind::Access)
                }
                Ok(crate::proto::AutoLifecycleWakeSource::Command) => {
                    Some(GuestActivityKind::Command)
                }
                Err(_) => return Err(Status::invalid_argument("invalid implicit wake source")),
            };
        if activity.is_some() && (!req.blocking || target_state != SandboxState::Started) {
            return Err(Status::invalid_argument(
                "implicit wake requires blocking STARTED",
            ));
        }
        emit_sandbox_event(
            &req.sandbox_id,
            SandboxEventType::WakeRequested,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .target_state(target_state.to_string())
                .bool_field(
                    SandboxEventBoolField::HasNetnsPath,
                    req.start_context
                        .as_ref()
                        .and_then(|start_context| start_context.netns_path.as_deref())
                        .is_some_and(|path| !path.is_empty()),
                )
                .bool_field(
                    SandboxEventBoolField::RestoreStorageUriPresent,
                    req.start_context
                        .as_ref()
                        .and_then(|start_context| {
                            start_context.suspend_restore_storage_uri.as_ref()
                        })
                        .is_some(),
                ),
        );
        let mut start = if let Some(start_context) = &req.start_context {
            self.authorize_peer(&request, "Wake.start_context", ALLOW_SANDBOX_START_CONTEXT)?;
            let space_api_route_identity =
                space_api_route_identity_from_wake_start_context(start_context)?;
            let route_identity_provenance =
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::for_activation_identity(
                    &space_api_route_identity,
                );
            if start_context.expected_execution_generation.is_some()
                && start_context.activation_auto_lifecycle.is_none()
            {
                return Err(Status::invalid_argument(
                    "wake execution generation requires a policy",
                ));
            }
            WakeStartConfig {
                expected_execution_generation: start_context
                    .expected_execution_generation
                    .map(ExecutionGeneration::new)
                    .transpose()
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
                activation_auto_lifecycle: start_context
                    .activation_auto_lifecycle
                    .as_ref()
                    .map(policy_from_proto)
                    .transpose()?,
                operation_activation_attempt_id: operation_activation_attempt_id_from_wire(
                    start_context.operation_activation_attempt_id.as_deref(),
                    &space_api_route_identity,
                )?,
                vcpu: start_context.vcpu,
                memory_mib: start_context.memory_mib,
                network_context: WakeNetworkContext::for_wake_start_context(start_context),
                target_state,
                suspend_restore_storage_uri: start_context.suspend_restore_storage_uri.clone(),
                suspend_restore_source_node: start_context.suspend_restore_source_node.clone(),
                expected_local_artifact_id: start_context.expected_local_artifact_id.clone(),
                requested_space_api_route_identity: Some(space_api_route_identity.clone()),
                space_api_route_identity,
                route_identity_provenance,
                authoritative_route_identity: start_context.route_identity.is_some(),
                provisional_activation_attempt: false,
                runtime_upgrade: None,
            }
        } else {
            self.authorize_peer(&request, "Wake", ALLOW_SANDBOX_OPERATOR)?;
            WakeStartConfig {
                target_state,
                ..WakeStartConfig::default()
            }
        };
        if let Some(upgrade) = &req.runtime_upgrade {
            // The upgrade's cold start runs as this caller, so the caller must
            // hold the upgrade's authority as well as Start's.
            self.authorize_peer(
                &request,
                "Wake.runtime_upgrade",
                ALLOW_SANDBOX_RUNTIME_UPGRADE,
            )?;
            // An implicit wake is admitted by the sandbox's wake policy before
            // the upgrade runs, as before a start or restore.
            if req.start_context.is_none() || !req.blocking || target_state != SandboxState::Started
            {
                return Err(Status::invalid_argument(
                    "a runtime upgrade wake requires start_context and blocking STARTED",
                ));
            }
            start.runtime_upgrade = Some(super::super::runtime_upgrade::WakeRuntimeUpgrade::parse(
                upgrade,
                request.metadata().clone(),
                request.extensions().clone(),
            )?);
        }
        let normalized_route_identity =
            space_api_route_identity_for_runtime_activation(start.space_api_route_identity);
        start.space_api_route_identity = normalized_route_identity.identity;
        start.route_identity_provenance = normalized_route_identity.provenance;
        start.provisional_activation_attempt =
            normalized_route_identity.provisional_activation_attempt;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = req.sandbox_id.clone();

        // An implicit wake cannot stage an absent execution before its effective
        // local policy is known. Existing residents are checked under the wake
        // lock by ensure_active_with_start, including after lock contention.
        if activity.is_some() {
            self.lookup_sandbox(&sandbox_id)?;
        }

        // For any other target, an absent local row is staged from a checkpoint
        // so `wake_with_start_config` below sees a `Suspended` row to wake.
        // `blocking: false` here keeps the ready-wait and the user-facing
        // `WakeTransition` reporting on `wake_with_start_config`; a
        // blocking restore here would double-take the wake lock and the
        // outer call would report `Noop`.
        // A stopped target always delegates to `Start`; otherwise only an absent
        // row does.
        //
        // `Start` already answers every shape a stopped target can meet —
        // absent row → materialize, `Stopped` → check it against the requested
        // artifact and return, a retryable `Restoring` → resume the interrupted
        // materialization, a non-retryable `Restoring` → the same
        // filesystem-keeping retirement a started `Start` drives, then refuse,
        // anything else → refuse — and `resolve_start_sandbox_target`
        // returns `Local` for a present row in any state, so nothing between
        // here and that boundary refuses one first. Routing the target through
        // the wake dispatch instead would need `wake_transition_for_state` to
        // know the target, which it structurally cannot: it maps current state
        // to transition, and for a stopped target the whole table inverts
        // (`Stopped` is converged, `Restoring` needs driving, `Started` is a
        // refusal). Delegating keeps one owner for the rule.
        let stage_from_checkpoint = target_state == SandboxState::Stopped
            || matches!(
                self.lookup_sandbox(&sandbox_id),
                Err(ref status) if status.code() == tonic::Code::NotFound
            );
        if stage_from_checkpoint {
            // `target_state` propagates here too: with `blocking: false`,
            // `start_sandbox` still routes through `start_from_checkpoint`
            // and runs the resume RPC unless we tell it to stop at
            // paused. Without this propagation the staged-in restore
            // would transition through `Started` before
            // `wake_with_start_config` observes the row.
            // A stopped target only places disk bytes and never launches, so it
            // has no use for a runtime namespace; requiring one would refuse a
            // Kubernetes-identity materialization that carries none.
            let netns_path = match start.network_context.netns_path_for_launch() {
                Ok(netns_path) => netns_path,
                Err(_) if target_state == SandboxState::Stopped => None,
                Err(error) => {
                    emit_sandbox_event(
                        &sandbox_id,
                        SandboxEventType::WakeFailed,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .target_state(target_state.to_string())
                            .reason("stage_from_checkpoint")
                            .error(error.to_string()),
                    );
                    return Err(error);
                }
            };
            let staging_start = StartRequest {
                activation_auto_lifecycle: start.activation_auto_lifecycle.clone().map(Into::into),
                sandbox_id: sandbox_id.clone(),
                vcpu: start.vcpu,
                memory_mib: start.memory_mib,
                checkpoint: Some(SUSPEND_CHECKPOINT.to_string()),
                blocking: false,
                netns_path,
                target_state: req.target_state,
                suspend_restore_storage_uri: start.suspend_restore_storage_uri.clone(),
                suspend_restore_source_node: start.suspend_restore_source_node.clone(),
                expected_local_artifact_id: start.expected_local_artifact_id.clone(),
                route_identity: space_api_route_identity_to_wire(&start.space_api_route_identity),
                operation_activation_attempt_id: start
                    .operation_activation_attempt_id
                    .map(|id| id.to_string()),
                ..Default::default()
            };
            // An upgrade boots the staged disk, so the checkpoint is staged
            // and left suspended rather than launched.
            let staged = if start.runtime_upgrade.is_some() {
                // Boxed, like the upgrade it precedes: inlined, the staging
                // future would grow every Wake's state machine.
                Box::pin(self.stage_missing_checkpoint(staging_start)).await
            } else {
                self.start_sandbox_with_authority(
                    staging_start,
                    false,
                    IdentityFreeDirectActivationAuthority::Denied,
                )
                .await
                .map(drop)
            };
            if let Err(error) = staged {
                emit_sandbox_event(
                    &sandbox_id,
                    SandboxEventType::WakeFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .target_state(target_state.to_string())
                        .reason("stage_from_checkpoint")
                        .error(error.to_string()),
                );
                return Err(error);
            }
        }

        // The staging `Start` above owns the stopped target end to end — it
        // verified the row against the requested artifact and refused if it
        // could not — so all that is left is to report it. Re-deriving any of
        // that here would put the same rule in two handlers.
        if target_state == SandboxState::Stopped {
            let sandbox = self.lookup_sandbox(&sandbox_id)?;
            emit_sandbox_event(
                &sandbox_id,
                SandboxEventType::WakeCompleted,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default().target_state(target_state.to_string()),
            );
            if publish_node_state {
                mark_sandbox_dirty(&self.db, &sandbox_id);
            }
            // `Restore` is the least wrong of the five. It is exact when this
            // call materialized the sandbox, and an overstatement on an
            // idempotent re-issue, where the delegated `Start` only re-checked
            // a row that was already here and nothing was pulled. The
            // alternatives are worse in kind rather than in degree: `Resume`
            // and `Start` claim a runtime that does not exist, `Wait` says the
            // call watched someone else's transition, and `Noop` denies the
            // materialization on the path that actually performed one.
            // `WakeTransition` has no "already converged" member to say it
            // properly.
            return Ok(Response::new(WakeResponse {
                sandbox_id,
                transition: WakeTransition::Restore as i32,
                route_identity: space_api_route_identity_to_wire(&sandbox.space_api_route_identity),
            }));
        }

        // The cold-path artifact-dir permission repair runs in
        // `resume_after_wake_lock` (the shared resume chokepoint reached by this
        // Wake RPC, implicit activation, and the resume RPC), so it is not issued
        // here.
        let response = match self
            .wake_with_start_config(
                req.sandbox_id.clone(),
                grpc_timeout_budget(&request),
                start,
                req.blocking,
                activity,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                emit_sandbox_event(
                    &sandbox_id,
                    SandboxEventType::WakeFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .target_state(target_state.to_string())
                        .error(error.to_string()),
                );
                return Err(error);
            }
        };
        // The wake-completion record is written inside the wake-lock-holding
        // paths (`ensure_active_for_guest_activity_with_start` /
        // `start_wake_transition_without_ready_wait`), not here: by this
        // point the wake lock has been released, and a suspend parked on it
        // would acquire the lock and run its superseded-by-wake check before
        // a record written here became visible.
        emit_sandbox_event(
            &sandbox_id,
            SandboxEventType::WakeCompleted,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default().target_state(target_state.to_string()),
        );
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox_id);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod supersession_tests;

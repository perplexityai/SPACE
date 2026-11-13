//! The `UpgradeRuntime` and `GetRuntimeUpgrade` RPCs.
//!
//! On Kubernetes a sandbox's microVM launches in its pod's shim at container
//! create, so a runtime upgrade runs there too: the pod's `runtime-upgrade`
//! checkpoint makes the shim call `UpgradeRuntime` with the cold start it
//! would otherwise send as a `Start`, carrying the pod's network namespace
//! and route identity. That caller can't do anything else afterwards, so the
//! RPC does the whole of what the pod needs under one upgrade lock: stage a
//! suspended sandbox placed away from its checkpoint, upgrade, and for a
//! started target hand back the running execution, activating a sandbox that
//! was already on the target. The control plane can't see the response, so it
//! reads the outcome from `GetRuntimeUpgrade`.

use tonic::metadata::MetadataMap;
use tonic::{Extensions, Request, Response, Status};

use super::{
    RuntimeUpgradeOutcome, RuntimeUpgradeRequest, parse_operation_id, parse_request,
    parse_sandbox_id,
};
use crate::db::{
    RuntimeUpgradeActivationClaim, RuntimeUpgradePhase, RuntimeUpgradeStatus,
    RuntimeUpgradeTargetState, SandboxRow,
};
use crate::proto::{
    GetRuntimeUpgradeRequest, GetRuntimeUpgradeResponse, RuntimeUpgradeStageFrom, StartRequest,
    StartResponse, UpgradeRuntimeRequest, UpgradeRuntimeResponse,
};
use crate::state::SandboxState;

#[cfg(test)]
pub(in crate::service::runtime_upgrade) mod tests;

/// The staging `Start` for a suspended sandbox absent from the node: the
/// upgrade's own start, as a restore from `stage_from`'s checkpoint. A blank
/// storage URI is refused by the staging `Start`'s own parsing.
fn staging_start(start: &StartRequest, stage_from: &RuntimeUpgradeStageFrom) -> StartRequest {
    StartRequest {
        sandbox_id: start.sandbox_id.clone(),
        vcpu: start.vcpu,
        memory_mib: start.memory_mib,
        checkpoint: Some(crate::service::create_support::SUSPEND_CHECKPOINT.to_string()),
        blocking: false,
        netns_path: start.netns_path.clone(),
        suspend_restore_storage_uri: Some(stage_from.suspend_restore_storage_uri.clone()),
        suspend_restore_source_node: stage_from.suspend_restore_source_node.clone(),
        route_identity: start.route_identity.clone(),
        activation_auto_lifecycle: start.activation_auto_lifecycle.clone(),
        operation_activation_attempt_id: start.operation_activation_attempt_id.clone(),
        ..Default::default()
    }
}

/// Whether the execution `sandbox` runs is the caller's: for a pod's shim,
/// the one with its pod UID and execution generation. A caller that names no
/// pod, such as an operator upgrading a Direct sandbox, is answered with
/// whatever execution runs.
fn runs_for_caller(sandbox: &SandboxRow, start: &StartRequest) -> bool {
    let names_a_pod = start
        .route_identity
        .as_ref()
        .is_some_and(|identity| identity.pod_uid.is_some());
    !names_a_pod || names_the_running_pod(sandbox, start)
}

/// Whether `start` names the pod whose execution `sandbox` runs: the same pod
/// UID and execution generation. Both must be present; an identity without
/// them names no pod.
fn names_the_running_pod(sandbox: &SandboxRow, start: &StartRequest) -> bool {
    let running = crate::service::activation_support::space_api_route_identity_to_wire(
        &sandbox.space_api_route_identity,
    );
    let (Some(running), Some(requested)) = (running.as_ref(), start.route_identity.as_ref()) else {
        return false;
    };
    running.pod_uid.is_some()
        && running.execution_generation.is_some()
        && running.pod_uid == requested.pod_uid
        && running.execution_generation == requested.execution_generation
}

impl crate::service::SandboxManagerService {
    pub(in crate::service) async fn upgrade_runtime(
        &self,
        request: Request<UpgradeRuntimeRequest>,
    ) -> Result<Response<UpgradeRuntimeResponse>, Status> {
        self.authorize_peer(
            &request,
            "UpgradeRuntime",
            crate::service::ALLOW_SANDBOX_RUNTIME_UPGRADE,
        )?;
        // Staging runs a restore `Start` would run, with the start's
        // namespace and identity, so it takes the authority such a `Start`
        // takes.
        if request.get_ref().stage_from.is_some() {
            self.authorize_peer(
                &request,
                "UpgradeRuntime.stage_from",
                crate::service::ALLOW_SANDBOX_START_CONTEXT,
            )?;
        }
        let (metadata, extensions, mut message) = request.into_parts();
        let stage_from = message.stage_from.take();
        // Only a suspended sandbox is staged, and its upgrade names the
        // checkpoint it was decided from; refuse here rather than after a
        // transfer.
        if stage_from.is_some() && message.checkpoint_local_artifact_id.is_none() {
            return Err(Status::invalid_argument(
                "stage_from requires checkpoint_local_artifact_id",
            ));
        }
        let upgrade = parse_request(message)?;
        let sandbox_id = upgrade.sandbox_id.clone();
        let _guard = self.runtime_upgrade_guard(&sandbox_id).await?;
        // Boxed: staging, the upgrade and an activation together are a large
        // future, which would otherwise sit on the stack of an unoptimized
        // build's RPC task.
        let response = Box::pin(self.upgrade_runtime_locked(
            metadata,
            extensions,
            upgrade,
            stage_from.as_ref(),
        ))
        .await;
        crate::node_state_sync::mark_sandbox_dirty(&self.db, &sandbox_id);
        response.map(Response::new)
    }

    /// The RPC's work under the sandbox's upgrade lock.
    async fn upgrade_runtime_locked(
        &self,
        metadata: MetadataMap,
        extensions: Extensions,
        upgrade: RuntimeUpgradeRequest,
        stage_from: Option<&RuntimeUpgradeStageFrom>,
    ) -> Result<UpgradeRuntimeResponse, Status> {
        let upgrader = self.caller_upgrader(metadata.clone(), extensions.clone());
        if let Some(stage_from) = stage_from {
            // An operation id names one upgrade of one sandbox. A request that
            // reuses one another sandbox's upgrade recorded stages nothing.
            upgrader.refuse_another_request(&upgrade)?;
            // A backed-up suspended copy of another checkpoint makes way for
            // the decided one; an unproven one stays, and the upgrade's
            // checkpoint check refuses it.
            let staging = staging_start(&upgrade.start, stage_from);
            self.evict_stale_checkpoint_copy(
                &upgrade.sandbox_id,
                upgrade.operation_id,
                upgrade.checkpoint_local_artifact_id,
                upgrade.authorization.memory_loss,
                &staging,
            )
            .await?;
            // Leaves a present sandbox alone, in any state; the upgrade's
            // checkpoint check decides whether its copy may be upgraded.
            self.stage_missing_checkpoint(staging).await?;
        }
        let target_state = upgrade.target_state;
        let operation_id = upgrade.operation_id;
        let decided_checkpoint = upgrade.checkpoint_local_artifact_id;
        let start = upgrade.start.clone();
        let outcome = upgrader.upgrade(upgrade).await?;
        let wire_outcome = match outcome {
            RuntimeUpgradeOutcome::Upgraded | RuntimeUpgradeOutcome::Replayed => {
                crate::proto::RuntimeUpgradeOutcome::Upgraded
            }
            RuntimeUpgradeOutcome::AlreadyOnTarget => {
                crate::proto::RuntimeUpgradeOutcome::AlreadyOnTarget
            }
        };
        // The pod's answer always comes from the sandbox as it is now, never
        // from the boot alone: retirement awaits after the boot, and the
        // sandbox may have stopped or started another execution meanwhile.
        let start = match (outcome, target_state) {
            // Asked to end stopped: only a sandbox at rest is. One that runs is
            // refused rather than reported as stopped, since the pod would
            // then stand in for a stopped sandbox while a microVM runs.
            (_, RuntimeUpgradeTargetState::Stopped) => {
                let sandbox = self.lookup_sandbox(&start.sandbox_id)?;
                refuse_moving(&sandbox)?;
                if !matches!(
                    sandbox.state.unowned(),
                    Some(SandboxState::Stopped | SandboxState::Suspended)
                ) {
                    return Err(Status::failed_precondition(format!(
                        "sandbox {} is on the runtime upgrade's target but is {}, not stopped",
                        sandbox.id, sandbox.state
                    )));
                }
                None
            }
            // The upgrade's boot, or a replay of one that already finished
            // (which never moves the sandbox again): the execution running
            // now, and only to the pod that runs it.
            (
                RuntimeUpgradeOutcome::Upgraded | RuntimeUpgradeOutcome::Replayed,
                RuntimeUpgradeTargetState::Started,
            ) => {
                let sandbox = self.lookup_sandbox(&start.sandbox_id)?;
                Some(self.running_execution_for(&sandbox, &start, || {
                    format!(
                        "runtime upgrade {operation_id} finished and sandbox {} is {}, not \
                         running this pod's execution",
                        sandbox.id, sandbox.state
                    )
                })?)
            }
            (RuntimeUpgradeOutcome::AlreadyOnTarget, RuntimeUpgradeTargetState::Started) => Some(
                Box::pin(self.activate_once(
                    metadata,
                    extensions,
                    operation_id,
                    start,
                    decided_checkpoint,
                ))
                .await?,
            ),
        };
        Ok(UpgradeRuntimeResponse {
            outcome: wire_outcome as i32,
            start,
            attempt_id: None,
        })
    }

    /// Activate a sandbox the upgrade found already on its target, starting
    /// it at most once per operation: the start of a stopped or suspended
    /// sandbox is claimed durably before it runs, with the node execution it
    /// has then. Once anything has started the sandbox since (it holds
    /// another execution, or isn't at rest), a retry, even one after a crash
    /// mid-start, is answered as a replay is, with the live execution of the
    /// same pod or a refusal. A sandbox already running this pod's execution
    /// is answered with it, and that answer is recorded too, so no retry of
    /// the operation starts the sandbox after a later stop.
    async fn activate_once(
        &self,
        metadata: MetadataMap,
        extensions: Extensions,
        operation_id: crate::db::RuntimeUpgradeOperationId,
        start: StartRequest,
        decided_checkpoint: Option<pplx_space_common_core::types::LocalArtifactId>,
    ) -> Result<StartResponse, Status> {
        let claim = match crate::db::read::runtime_upgrade_report(&*self.db_conn()?, operation_id)
            .map_err(|error| {
            Status::internal(format!("read runtime upgrade: {error}"))
        })? {
            Some(report) if report.sandbox_id != start.sandbox_id => {
                return Err(Status::invalid_argument(format!(
                    "runtime upgrade {operation_id} names another sandbox"
                )));
            }
            Some(crate::db::RuntimeUpgradeReport {
                status: RuntimeUpgradeStatus::AlreadyOnTarget { activation },
                ..
            }) => activation,
            _ => None,
        };
        let sandbox = self.lookup_sandbox(&start.sandbox_id)?;
        refuse_moving(&sandbox)?;
        // The upgrade checked the checkpoint against the row it read; the
        // sandbox may have suspended since, to one the upgrade didn't decide.
        super::require_decided_checkpoint(&sandbox, decided_checkpoint)?;
        let at_rest = matches!(
            sandbox.state.unowned(),
            Some(SandboxState::Stopped | SandboxState::Suspended)
        );
        // A claimed start that never began leaves the sandbox at rest on the
        // execution it had when claimed, as a start refused before its
        // activation (staging busy, admission) does: that one may run again.
        // An operation that answered with a running execution never starts.
        let from_execution = match claim {
            None => Some(sandbox.node_execution_id),
            Some(RuntimeUpgradeActivationClaim::Start { from_execution })
                if from_execution == sandbox.node_execution_id =>
            {
                Some(from_execution)
            }
            Some(_) => None,
        };
        let Some(from_execution) = from_execution.filter(|_| at_rest) else {
            let running = self.running_execution_for(&sandbox, &start, || {
                if claim.is_some() {
                    format!(
                        "runtime upgrade {operation_id} already activated sandbox {}, which is \
                         {} and not running this pod's execution",
                        sandbox.id, sandbox.state
                    )
                } else {
                    format!(
                        "sandbox {} is already on the runtime upgrade's target and is {}, \
                         not running this pod's execution",
                        sandbox.id, sandbox.state
                    )
                }
            })?;
            if claim.is_none() {
                self.claim_activation(
                    operation_id,
                    &start.sandbox_id,
                    RuntimeUpgradeActivationClaim::Attach,
                )?;
            }
            return Ok(running);
        };
        if claim.is_none()
            && !self.claim_activation(
                operation_id,
                &start.sandbox_id,
                RuntimeUpgradeActivationClaim::Start { from_execution },
            )?
        {
            return Err(Status::aborted(format!(
                "runtime upgrade {operation_id} could not claim the start of sandbox {}; retry",
                sandbox.id
            )));
        }
        self.activate_on_target(
            metadata,
            extensions,
            start.clone(),
            sandbox.state.lifecycle(),
            from_execution,
        )
        .await?;
        self.answer_after_activation(operation_id, &start)
    }

    /// Record how the operation claims its already-on-target activation;
    /// false when its activation is already claimed or no already-on-target
    /// outcome is recorded for the sandbox.
    fn claim_activation(
        &self,
        operation_id: crate::db::RuntimeUpgradeOperationId,
        sandbox_id: &str,
        claim: RuntimeUpgradeActivationClaim,
    ) -> Result<bool, Status> {
        crate::db::claim_runtime_upgrade_activation(
            &*self.db_conn()?,
            operation_id,
            sandbox_id,
            claim,
        )
        .map_err(|error| Status::internal(format!("claim runtime upgrade activation: {error}")))
    }

    /// The pod's answer once its activation returned, read from the sandbox
    /// as it is now rather than from the start's own response: a restore
    /// that finds the sandbox already started reattaches to another
    /// activation's execution, and a stop or suspend that finished since
    /// keeps the pod's identity on the row while the runtime is gone, so the
    /// start's pid would be stale.
    fn answer_after_activation(
        &self,
        operation_id: crate::db::RuntimeUpgradeOperationId,
        start: &StartRequest,
    ) -> Result<StartResponse, Status> {
        let sandbox = self.lookup_sandbox(&start.sandbox_id)?;
        self.running_execution_for(&sandbox, start, || {
            format!(
                "runtime upgrade {operation_id} activated sandbox {}, which is {} and not \
                 running this pod's execution",
                sandbox.id, sandbox.state
            )
        })
    }

    /// Start a stopped sandbox already on the upgrade's target, or restore a
    /// suspended one from its checkpoint (the target's), with the upgrade's
    /// start, so the pod that asked runs it.
    async fn activate_on_target(
        &self,
        metadata: MetadataMap,
        extensions: Extensions,
        mut start: StartRequest,
        state: SandboxState,
        from_execution: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    ) -> Result<StartResponse, Status> {
        if state == SandboxState::Suspended {
            start.checkpoint = Some(crate::service::create_support::SUSPEND_CHECKPOINT.to_string());
        }
        // The upgrade's caller holds Start's authority: the upgrade's
        // principals are a subset of Start's.
        self.start_with_runtime_upgrade(
            Request::from_parts(metadata, extensions, start),
            crate::service::start::StartAdmission {
                runtime_upgrade: None,
                // The claim's execution: another activation since moved the
                // sandbox off it, and this start was decided without it.
                expected_node_execution: Some(from_execution),
            },
        )
        .await
        .map(Response::into_inner)
    }

    /// The live execution of `sandbox` when it's started and runs for the
    /// caller (see [`runs_for_caller`]). Anything else is refused with
    /// `refusal`, rather than moved.
    fn running_execution_for(
        &self,
        sandbox: &SandboxRow,
        start: &StartRequest,
        refusal: impl FnOnce() -> String,
    ) -> Result<StartResponse, Status> {
        if !sandbox.state.is_unowned(SandboxState::Started) || !runs_for_caller(sandbox, start) {
            return Err(Status::failed_precondition(refusal()));
        }
        crate::service::create_support::checkpoint_start_response_for_live_runtime(
            sandbox,
            (self.sandbox_runtime_live)(&sandbox.id),
        )
        .ok_or_else(|| Status::failed_precondition(refusal()))
    }

    pub(in crate::service) fn get_runtime_upgrade(
        &self,
        request: &Request<GetRuntimeUpgradeRequest>,
    ) -> Result<GetRuntimeUpgradeResponse, Status> {
        use crate::proto::RuntimeUpgradePhase as WirePhase;
        use crate::proto::RuntimeUpgradeStatus as Wire;
        self.authorize_peer(
            request,
            "GetRuntimeUpgrade",
            crate::service::ALLOW_SANDBOX_RUNTIME_UPGRADE,
        )?;
        let sandbox_id = parse_sandbox_id(&request.get_ref().sandbox_id)?;
        let operation_id = parse_operation_id(&request.get_ref().operation_id)?;
        let report = crate::db::read::runtime_upgrade_report(&*self.db_conn()?, operation_id)
            .map_err(|error| Status::internal(format!("read runtime upgrade: {error}")))?;
        let Some(report) = report else {
            return Ok(GetRuntimeUpgradeResponse {
                status: Wire::Absent as i32,
                ..Default::default()
            });
        };
        if report.sandbox_id != sandbox_id {
            return Err(Status::invalid_argument(format!(
                "runtime upgrade {operation_id} names another sandbox"
            )));
        }
        let mut response = GetRuntimeUpgradeResponse::default();
        response.status = match report.status {
            RuntimeUpgradeStatus::InProgress(phase) => {
                response.phase = match phase {
                    RuntimeUpgradePhase::Fenced => WirePhase::Fenced,
                    RuntimeUpgradePhase::Preserved => WirePhase::Preserved,
                    RuntimeUpgradePhase::Booting => WirePhase::Booting,
                    RuntimeUpgradePhase::Verified => WirePhase::Verified,
                    // The report names these Upgraded and RolledBack.
                    RuntimeUpgradePhase::RolledBack | RuntimeUpgradePhase::Retired => {
                        WirePhase::Unspecified
                    }
                } as i32;
                Wire::InProgress
            }
            RuntimeUpgradeStatus::Upgraded => Wire::Upgraded,
            RuntimeUpgradeStatus::RolledBack { failure } => {
                response.failure = failure;
                Wire::RolledBack
            }
            RuntimeUpgradeStatus::AlreadyOnTarget { activation } => {
                response.activation_claimed = activation.is_some();
                Wire::AlreadyOnTarget
            }
        } as i32;
        Ok(response)
    }
}

/// Refuse, as retryable, a sandbox another activation or stop is moving, or
/// one whose pending cleanup recovery is still driving back to rest: a retry
/// finds where it lands.
fn refuse_moving(sandbox: &SandboxRow) -> Result<(), Status> {
    if sandbox.state.is_cleanup_owned()
        || matches!(
            sandbox.state.lifecycle(),
            SandboxState::Creating
                | SandboxState::Starting
                | SandboxState::Pausing
                | SandboxState::Resuming
                | SandboxState::Checkpointing
                | SandboxState::Suspending
                | SandboxState::Restoring
                | SandboxState::Stopping
        )
    {
        return Err(Status::unavailable(format!(
            "sandbox {} is {}; retry",
            sandbox.id, sandbox.state
        )));
    }
    Ok(())
}

//! Request-driven cold start and checkpoint restore orchestration.
//!
//! Startup and periodic repair stay in `recovery`; this module owns activation
//! request parsing, placement and memory-policy resolution, and launch sequencing.

use pplx_space_common_core::types::{LocalArtifactId, RateLimiter, TokenBucket};

use super::activation_support::{
    ActivationFailureStage, IdentityFreeDirectActivationAuthority, StartStateTransition,
    operation_activation_attempt_id_from_wire, space_api_route_identity_for_runtime_activation,
    space_api_route_identity_from_start_request, space_api_route_identity_to_wire,
    spawn_wait_ready_state_transition, wait_ready_and_mark_started, wake_target_state_from_proto,
};
use super::cleanup_support::{ActivationCleanupSpec, read_node_execution_fence};
use super::create::StartFromCheckpointLaunch;
use super::create_support::{
    CheckpointRestoreRetryAction, RestoredSnapshotProvenance, RuntimeActivatedEvent,
    SUSPEND_CHECKPOINT, admit_warm_start, checkpoint_restore_retry_action,
    checkpoint_start_response_for_existing_started_sandbox, emit_runtime_activated_event,
    guest_console_logs_enabled, resolve_template_paths_for_start,
    stage_base_snapshot_runtime_metadata, template_runtime_defaults_for_sandbox_start,
    template_source_for_sandbox_prepare, write_warm_start_restaged,
};
use super::network_runtime::{
    launch_net_gateway_via_node, net_gateway_launch_config, register_net_gateway_launch,
};
use super::recovery_cross_node::{
    CrossNodeRestoreRequest, CrossNodeRestoreStagingAdmission, CrossNodeRestoreTarget,
    RestoreArtifactAsNewContext, RestoreSourceSelection, SuspendRestoreDecision,
    SuspendRestorePlacement, codec_from_local_sidecar, decide_suspend_restore,
    suspend_restore_placement,
};
use super::restart_admission::wait_for_activation_admission;
use super::spaced_bridge::{
    ActivationEndpointProofRequirement, activation_endpoint_proof_requirement,
    capture_spaced_endpoint_proof_for_activation, endpoint_runtime_identity_for_activation,
    launch_spaced_host_via_node,
};
use super::start_support::{
    ActivationAdmissionKind, START_NODE_METADATA_TIMEOUT, START_TEMPLATE_RESOLUTION_TIMEOUT,
    SandboxStartExecutionPath, SandboxStartPath, persist_allocated_runtime_label, start_stage,
    start_stage_timeout, start_stage_timeout_or,
};
use super::suspend_snapshot_publication::canonical_suspend_snapshot_dir;
use super::tail_support::connect_node;
use super::validation::{
    TemplateRuntimeDefaults, optional_positive_or_default,
    reject_incompatible_template_spaced_compatibility,
};
use super::{
    CACHE_DIR, DEFAULT_BLOCKING_READY_TIMEOUT_SECS, RUN_DIR, RouteStreamEpochIdentity,
    SandboxManagerService,
};
use crate::{
    db::CleanupCompletion,
    proto::{StartRequest, StartResponse},
    state::SandboxState,
};
use pplx_space_common_core::types::{
    SandboxCleanupEpisodeId, SandboxId, SandboxOwnerKind, SandboxQuarantineReasonCode,
    SpaceApiRouteIdentity, StorageUri,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};
use pplx_space_metrics::sandbox::admission::AdmissionPermitOutcome;
use pplx_space_node_mgr_client::{
    NodeManagerGrpcClient,
    proto::{
        AllocateRequest, LaunchSandboxRequest, NetworkPoolStatusRequest, PrepareSandboxRequest,
        ReleaseRequest,
    },
};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Response, Status};
use tracing::Instrument;

fn resolve_missing_checkpoint_restore_route_identity(
    sandbox_id: SandboxId,
    identity: SpaceApiRouteIdentity,
) -> Result<SpaceApiRouteIdentity, Status> {
    match RouteStreamEpochIdentity::from_runtime_update(sandbox_id, identity.clone())? {
        Some(_) => Ok(identity),
        None => Err(
            pplx_space_sbx_mgr_client::authoritative_route_identity_required_status(
                "missing checkpoint restore requires an authoritative route identity",
            ),
        ),
    }
}

struct StartSandboxContext {
    activation_auto_lifecycle: Option<pplx_space_common_core::types::AutoLifecyclePolicy>,
    operation_activation_attempt_id:
        Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    checkpoint: String,
    suspend_restore_storage_uri: Option<String>,
    restore_placement: SuspendRestorePlacement,
    netns_path: Option<String>,
    ready_timeout_secs: u32,
    from_warm: bool,
    blocking: bool,
    target_state: SandboxState,
    /// The cross-node restore finalize arm this request selects. `from_request`
    /// builds `StoppedDiskArtifact` only for a `Stopped` target and only with
    /// the artifact id the request names, so a stopped boundary keyed on it
    /// always has the id it compares.
    restore_target: CrossNodeRestoreTarget,
    req_vcpu: Option<u32>,
    req_memory_mib: Option<u32>,
    execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    space_api_route_identity: pplx_space_common_core::types::SpaceApiRouteIdentity,
    route_identity_provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    open_activation_claim_eligible: bool,
    restore_artifact_as_new: Option<RestoreArtifactAsNewContext>,
    identity_free_direct_authority: IdentityFreeDirectActivationAuthority,
    /// Set only by the runtime upgrade operation, never from the wire: this
    /// cold boot is that upgrade's, and may pass its activation fence.
    runtime_upgrade: Option<crate::db::RuntimeUpgradeOperationId>,
}

struct StartSandboxLookup {
    sandbox: crate::db::SandboxRow,
    restored_snapshot: RestoredSnapshotProvenance,
}

enum StartSandboxTarget {
    Local(Box<StartSandboxLookup>),
    MissingCheckpointRestore { sandbox_id: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InterruptedRestoreAction {
    Continue,
    ResumeSuspendCheckpoint,
    RematerializeArtifact,
}

/// Emits a `snapshot.restore.started` sample once, at the moment the
/// encryption stamp becomes known, so `started` always carries the same
/// `encrypted` value as the terminal sample it pairs with.
///
/// The cross-node restore path cannot know the stamp up front — it runs
/// because the local row is absent — and tagging the start provisionally
/// would split one flow across both `encrypted` series, corrupting
/// `started - completed` and every encrypted rate/latency panel.
struct DeferredRestoreStart {
    kind: &'static str,
    codec: &'static str,
    emitted: std::cell::Cell<bool>,
}

impl DeferredRestoreStart {
    fn new(kind: &'static str, codec: &'static str) -> Self {
        Self {
            kind,
            codec,
            emitted: std::cell::Cell::new(false),
        }
    }

    /// Emit the start sample if it has not been emitted yet. Call
    /// immediately before a terminal sample, with that sample's own
    /// `encrypted` value.
    fn emit(&self, encrypted: bool) {
        if self.emitted.replace(true) {
            return;
        }
        pplx_space_metrics::snapshot::restore_started(self.kind, self.codec, encrypted);
    }
}

/// Refuse unless the local row is a `Stopped` materialization of exactly the
/// artifact this request named.
///
/// The lifecycle is not evidence. A sandbox that stopped on this node earlier
/// and then had its disk published for backup (`EnqueueArtifactBackup` mints
/// the pin on a stopped row) leaves the identical shape an evacuation does —
/// `Stopped`, pinning a `disk`/`stop` artifact — so answering "your bytes are here" on the state
/// match alone would answer for *older* bytes, and the cluster would then be
/// free to retire the source. That is silent data loss, so the row has to name
/// the artifact that was asked for.
///
/// The comparison is local because the caller already knows the answer: the
/// cluster hands it `local_artifact_id` and `storage_uri` in one
/// `GetArtifactBackupLocation` reply, and it forwards the id as
/// `expected_local_artifact_id`. Reading object storage to re-derive it would
/// put an S3 dependency on the placement path to learn something the request
/// could simply carry — the same reasoning that has the fork transport check
/// `RestoreArtifactAsNewContext.artifact_snapshot_id` against its URI locally.
///
/// Run on every stopped-target boundary, including the one that just staged the
/// bytes: there the check is nearly free and it additionally catches a caller
/// whose expectation disagrees with the artifact its own URI resolved to.
fn verify_stopped_materialization(
    sandbox: &crate::db::SandboxRow,
    expected: pplx_space_common_core::types::LocalArtifactId,
) -> Result<(), Status> {
    if sandbox.state != SandboxState::Stopped {
        return Err(Status::failed_precondition(format!(
            "cannot materialize sandbox {} as stopped while local state is {}",
            sandbox.id, sandbox.state
        )));
    }
    let pinned = sandbox.current_local_artifact_id.ok_or_else(|| {
        Status::failed_precondition(format!(
            "sandbox {} is stopped locally but pins no artifact, so it cannot be the \
             requested materialization",
            sandbox.id
        ))
    })?;
    if pinned != expected {
        return Err(Status::failed_precondition(format!(
            "sandbox {} is stopped locally pinning artifact {pinned}, not the requested \
             {expected}; refusing to report a stale row as materialized",
            sandbox.id
        )));
    }
    // The kind and origin the pin carries are what make it an evacuated disk
    // rather than a suspend checkpoint or a user's named snapshot. Checking
    // them here rather than trusting the id keeps this the single place that
    // decides what "is the requested materialization" means.
    let kind = sandbox.current_local_artifact_kind;
    let origin = sandbox.current_local_artifact_origin;
    if !super::recovery_cross_node::is_stop_origin_disk(kind, origin) {
        return Err(Status::failed_precondition(format!(
            "sandbox {} pins artifact {pinned} as {kind:?}/{origin:?}, not a stop-origin disk \
             artifact",
            sandbox.id
        )));
    }
    Ok(())
}

fn interrupted_restore_action(
    checkpoint: &str,
    state: SandboxState,
    cleanup_completion: Option<crate::db::CleanupCompletion>,
    restore_artifact_as_new: bool,
) -> InterruptedRestoreAction {
    let suspend_restore = checkpoint == SUSPEND_CHECKPOINT
        && (matches!(state, SandboxState::Suspended | SandboxState::Restoring)
            || cleanup_completion == Some(crate::db::CleanupCompletion::MarkSuspended));
    if restore_artifact_as_new && state == SandboxState::Restoring {
        InterruptedRestoreAction::RematerializeArtifact
    } else if suspend_restore {
        InterruptedRestoreAction::ResumeSuspendCheckpoint
    } else {
        InterruptedRestoreAction::Continue
    }
}

impl StartSandboxContext {
    fn from_request(req: &StartRequest) -> Result<Self, Status> {
        let checkpoint = req.checkpoint.clone().unwrap_or_default();
        let suspend_restore_storage_uri = req.suspend_restore_storage_uri.clone();
        let parsed_suspend_restore_storage_uri = suspend_restore_storage_uri
            .as_deref()
            .map(StorageUri::parse)
            .transpose()
            .map_err(|err| {
                Status::invalid_argument(format!("invalid suspend_restore_storage_uri: {err}"))
            })?;

        let suspend_restore_source_node = req
            .suspend_restore_source_node
            .as_deref()
            .filter(|node| !node.trim().is_empty())
            .map(ToOwned::to_owned);
        if req.suspend_restore_source_node.is_some() && suspend_restore_source_node.is_none() {
            return Err(Status::invalid_argument(
                "suspend_restore_source_node cannot be empty",
            ));
        }

        let local_node_id = std::env::var("SPACE_NODE_ID").ok();
        let restore_placement = suspend_restore_placement(
            suspend_restore_source_node.as_deref(),
            local_node_id.as_deref(),
            suspend_restore_storage_uri.is_some(),
        );
        let ready_timeout_secs = optional_positive_or_default(
            "ready_timeout_secs",
            req.ready_timeout_secs,
            DEFAULT_BLOCKING_READY_TIMEOUT_SECS,
        )?;

        let normalized_route_identity = space_api_route_identity_for_runtime_activation(
            space_api_route_identity_from_start_request(req)?,
        );
        let route_identity_provenance = normalized_route_identity.provenance;
        let open_activation_claim_eligible =
            normalized_route_identity.open_activation_claim_eligible;
        let space_api_route_identity = normalized_route_identity.identity;
        let restore_artifact_as_new = req
            .restore_artifact_as_new
            .as_ref()
            .map(|context| {
                let storage_uri = parsed_suspend_restore_storage_uri.as_ref().ok_or_else(|| {
                    Status::invalid_argument(
                        "restore_artifact_as_new requires a cold start and an exact storage URI",
                    )
                })?;
                RestoreArtifactAsNewContext::from_proto(context, storage_uri)
            })
            .transpose()?;
        if let Some(restore_artifact_as_new) = restore_artifact_as_new.as_ref() {
            if !checkpoint.is_empty() || suspend_restore_storage_uri.is_none() {
                return Err(Status::invalid_argument(
                    "restore_artifact_as_new requires a cold start and an exact storage URI",
                ));
            }
            let target_sandbox_id: SandboxId = req.sandbox_id.parse().map_err(|error| {
                Status::invalid_argument(format!("invalid target sandbox_id: {error}"))
            })?;
            if restore_artifact_as_new.source_sandbox_id == target_sandbox_id {
                return Err(Status::invalid_argument(
                    "artifact restore source and target sandbox IDs must differ",
                ));
            }
            if space_api_route_identity.execution_generation.is_none() {
                return Err(Status::invalid_argument(
                    "restore_artifact_as_new requires a target execution generation",
                ));
            }
            if wake_target_state_from_proto(req.target_state)? != SandboxState::Started {
                return Err(Status::invalid_argument(
                    "disk artifact restore-as-new cold-boots into the started lifecycle",
                ));
            }
        }

        let target_state = wake_target_state_from_proto(req.target_state)?;
        let expected_local_artifact_id = req
            .expected_local_artifact_id
            .as_deref()
            .map(str::parse::<pplx_space_common_core::types::LocalArtifactId>)
            .transpose()
            .map_err(|error| {
                Status::invalid_argument(format!("invalid expected_local_artifact_id: {error}"))
            })?;
        if target_state != SandboxState::Stopped && expected_local_artifact_id.is_some() {
            // The id is evidence only a stopped materialization compares;
            // accepting it elsewhere would read like a check that never runs.
            return Err(Status::invalid_argument(
                "expected_local_artifact_id requires the stopped target state",
            ));
        }
        if target_state == SandboxState::Stopped {
            // Materializing an evacuated stopped sandbox is a data placement,
            // and the inputs that describe one are a suspend checkpoint, the
            // exact URI of its disk artifact, and which artifact that URI is.
            // The first two say where to read from; the third is what lets the
            // node tell an already-materialized row from stale residue without
            // reading object storage. All three are required together, and
            // parsing them here is the one place that decides a stopped target
            // is well-formed.
            if checkpoint != SUSPEND_CHECKPOINT || suspend_restore_storage_uri.is_none() {
                return Err(Status::invalid_argument(
                    "stopped target state requires a suspend checkpoint and an exact storage URI",
                ));
            }
            // Nothing more is needed to keep a fork out: the restore-as-new
            // block above already refuses any checkpoint (`requires a cold
            // start`) and any target other than `Started`, so the combination
            // cannot reach here. A second check would read like the one that
            // closes it and would never run.
        }
        let restore_target = match (target_state, expected_local_artifact_id) {
            (SandboxState::Stopped, Some(expected)) => {
                CrossNodeRestoreTarget::StoppedDiskArtifact { expected }
            }
            (SandboxState::Stopped, None) => {
                return Err(Status::invalid_argument(
                    "stopped target state requires expected_local_artifact_id",
                ));
            }
            _ => CrossNodeRestoreTarget::SuspendedCheckpoint,
        };

        Ok(Self {
            activation_auto_lifecycle: req
                .activation_auto_lifecycle
                .as_ref()
                .map(crate::auto_lifecycle::policy_from_proto)
                .transpose()?,
            operation_activation_attempt_id: operation_activation_attempt_id_from_wire(
                req.operation_activation_attempt_id.as_deref(),
                &space_api_route_identity_from_start_request(req)?,
            )?,
            checkpoint,
            suspend_restore_storage_uri,
            restore_placement,
            netns_path: req.netns_path.clone(),
            ready_timeout_secs,
            from_warm: req.from_warm,
            blocking: req.blocking,
            target_state,
            restore_target,
            req_vcpu: req.vcpu,
            req_memory_mib: req.memory_mib,
            execution_generation: space_api_route_identity.execution_generation,
            space_api_route_identity,
            route_identity_provenance,
            open_activation_claim_eligible,
            restore_artifact_as_new,
            identity_free_direct_authority: IdentityFreeDirectActivationAuthority::Denied,
            runtime_upgrade: None,
        })
    }
}

/// Short, low-cardinality string for a gRPC status code, suitable as a
/// metric tag `reason` value.
fn short_grpc_code(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "ok",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Unknown => "unknown",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::Unauthenticated => "unauthenticated",
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NodeSandboxMemoryPolicy {
    pub(super) cgroup_memory_mib: u64,
    pub(super) guest_memory_max_mib: u32,
    pub(super) headroom_mib: u64,
}

impl NodeSandboxMemoryPolicy {
    pub(super) fn cgroup_memory_mib_for_guest(self, guest_memory_mib: u32) -> u64 {
        (u64::from(guest_memory_mib) + self.headroom_mib).min(self.cgroup_memory_mib)
    }
}

const LOCAL_INSTANCE_DIR: &str = "instance";

fn snapshot_artifacts_materialized(
    dir: &std::path::Path,
    artifacts: &[crate::snapshot_upload::Artifact],
) -> bool {
    artifacts
        .iter()
        .all(|artifact| dir.join(artifact.local_name).exists())
}

fn local_suspend_snapshot_materialized(cache_dir: &std::path::Path, sandbox_id: &str) -> bool {
    let sandbox_dir = cache_dir.join(sandbox_id);
    let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
    let instance_dir = sandbox_dir.join(LOCAL_INSTANCE_DIR);

    let plaintext_snapshot =
        snapshot_artifacts_materialized(&snapshot_dir, crate::snapshot_upload::COMMON_VM_ARTIFACTS);
    let encrypted_artifacts = crate::snapshot_upload::encrypted_vm_artifacts();
    let same_node_encrypted_snapshot =
        snapshot_artifacts_materialized(&instance_dir, &encrypted_artifacts);
    let cross_node_encrypted_snapshot =
        snapshot_artifacts_materialized(&snapshot_dir, &encrypted_artifacts);

    plaintext_snapshot || same_node_encrypted_snapshot || cross_node_encrypted_snapshot
}

const NET_GATEWAY_LAUNCH_SETTLE_TIMEOUT: Duration = Duration::from_secs(2);

async fn settle_net_gateway_launch_task(
    mut netgtw_task: tokio_util::task::AbortOnDropHandle<
        Result<pplx_space_node_mgr_client::proto::LaunchResponse, Status>,
    >,
    sandbox_id: &str,
    reason: &'static str,
) {
    tokio::select! {
        result = &mut netgtw_task => match result {
            Ok(Ok(response)) => {
                tracing::warn!(
                    sandbox_id,
                    pid = response.pid,
                    reason,
                    "net-gateway launched during failed sandbox start; cleanup will terminate it"
                );
            }
            Ok(Err(error)) => {
                tracing::debug!(
                    sandbox_id,
                    %error,
                    reason,
                    "net-gateway launch failed before sandbox cleanup"
                );
            }
            Err(error) => {
                tracing::debug!(
                    sandbox_id,
                    %error,
                    reason,
                    "net-gateway launch task ended before sandbox cleanup"
                );
            }
        },
        () = tokio::time::sleep(NET_GATEWAY_LAUNCH_SETTLE_TIMEOUT) => {
            netgtw_task.abort();
            let _ = netgtw_task.await;
            tracing::warn!(
                sandbox_id,
                reason,
                timeout_ms = NET_GATEWAY_LAUNCH_SETTLE_TIMEOUT.as_millis() as u64,
                "aborted slow net-gateway launch task before sandbox cleanup"
            );
        }
    }
}

fn required_node_memory_policy_field(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        Err(Status::internal(format!(
            "node-manager reported zero {field}; sandbox memory policy is unavailable"
        )))
    } else {
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ResolvedSandboxMemoryPolicy {
    pub(super) requested_memory_mib: u32,
    pub(super) effective_memory_mib: u32,
    pub(super) cgroup_memory_mib: u64,
    pub(super) guest_memory_max_mib: u32,
    pub(super) cgroup_memory_max_mib: u64,
    pub(super) memory_headroom_mib: u64,
}

async fn prepare_suspend_restore_dir(
    service: &SandboxManagerService,
    owner: &pplx_space_common_core::types::SandboxOwner,
    sandbox_id: String,
    storage_uri: Option<String>,
    execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
) -> Result<crate::snapshot_restore::RestorePrepared, crate::snapshot_restore::RestoreError> {
    let retire_scope = |
        transfer_scope_id: pplx_space_common_core::types::TransferScopeId,
    | -> Result<(), crate::snapshot_restore::RestoreError> {
        let db = service.db.lock().map_err(|_| {
            crate::snapshot_restore::RestoreError::Other(
                "sandbox database mutex poisoned".to_string(),
            )
        })?;
        let retired = crate::db::retire_restore_transfer_scope_if_matches(
            &db,
            &sandbox_id,
            transfer_scope_id,
        )
        .map_err(|source| {
            crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source }
        })?;
        if !retired {
            return Err(crate::snapshot_restore::RestoreError::Other(format!(
                "restore transfer scope ownership changed for {sandbox_id}; retry"
            )));
        }
        Ok(())
    };

    let existing_scope = {
        let db = service.db.lock().map_err(|_| {
            crate::snapshot_restore::RestoreError::Other(
                "sandbox database mutex poisoned".to_string(),
            )
        })?;
        crate::db::read::sandbox_view(&db, &sandbox_id)
            .restore_scope()
            .scope()
            .map_err(
                |source| crate::snapshot_restore::RestoreError::AdvanceRestorePhase { source },
            )?
    };
    if let Some(existing_scope) = existing_scope {
        service
            .cancel_restore_transfer_scope_and_remove_staging(&sandbox_id, &existing_scope)
            .await
            .map_err(|error| {
                crate::snapshot_restore::RestoreError::Other(format!(
                    "cancel previous restore transfer scope for {sandbox_id}: {error}"
                ))
            })?;
        retire_scope(existing_scope.transfer_scope_id)?;
    }

    let transfer_scope_id = service.create_restore_transfer_scope(
        &sandbox_id,
        execution_generation,
        activation_attempt_id,
    )?;
    let scope = crate::db::RestoreTransferScopeRow {
        transfer_scope_id,
        execution_generation,
        activation_attempt_id,
        staging_dir: crate::snapshot_restore::remote_restore_metadata_staging_dir(
            &sandbox_id,
            transfer_scope_id,
        )
        .to_string_lossy()
        .into_owned(),
    };
    // Capture authority before downloads, never refresh it when a stale request retries.
    let execution_fence = read_node_execution_fence(&service.db, &sandbox_id)
        .map_err(|error| crate::snapshot_restore::RestoreError::Other(error.to_string()))?
        .ok_or_else(|| {
            crate::snapshot_restore::RestoreError::Other(
                "restore execution identity is missing".into(),
            )
        })?;
    let prepared = match storage_uri.as_deref() {
        Some(storage_uri) => {
            crate::snapshot_restore::prepare_restore_dir_from_uri(
                &sandbox_id,
                owner,
                execution_fence,
                storage_uri,
                transfer_scope_id,
                &service.node_manager,
                service.template_resolver.as_ref(),
            )
            .await
        }
        None => {
            crate::snapshot_restore::prepare_restore_dir(
                &sandbox_id,
                owner,
                execution_fence,
                transfer_scope_id,
                &service.node_manager,
                service.template_resolver.as_ref(),
            )
            .await
        }
    };

    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(stage_error) => {
            service
                .cancel_restore_transfer_scope_and_remove_staging(&sandbox_id, &scope)
                .await
                .map_err(|cleanup_error| {
                    crate::snapshot_restore::RestoreError::Other(format!(
                        "{stage_error}; cancel failed in-place restore transfer scope \
                         {transfer_scope_id}: {cleanup_error}"
                    ))
                })?;
            retire_scope(transfer_scope_id)?;
            return Err(stage_error);
        }
    };

    retire_scope(transfer_scope_id)?;
    Ok(prepared)
}

async fn stage_remote_suspend_restore_if_needed<F, Fut>(
    sandbox_id: &str,
    cache_dir: &std::path::Path,
    restore_placement: SuspendRestorePlacement,
    selected_uri: Option<&str>,
    restored_snapshot: &mut RestoredSnapshotProvenance,
    stage: F,
) -> Result<(), Status>
where
    F: FnOnce(String, Option<String>) -> Fut,
    Fut: std::future::Future<
            Output = Result<
                crate::snapshot_restore::RestorePrepared,
                crate::snapshot_restore::RestoreError,
            >,
        >,
{
    let local_snapshot_missing = !local_suspend_snapshot_materialized(cache_dir, sandbox_id);
    if !local_snapshot_missing
        && let Some(staged_uri) = selected_uri.and_then(|selected_uri| {
            (restored_snapshot.storage_uri.as_deref() == Some(selected_uri)).then_some(selected_uri)
        })
    {
        tracing::info!(
            sandbox_id,
            storage_uri = staged_uri,
            "restore: selected suspend snapshot was already staged earlier in this request"
        );
        return Ok(());
    }

    let remote_stage_reason = match decide_suspend_restore(
        restore_placement,
        selected_uri.is_some(),
        local_snapshot_missing,
    ) {
        SuspendRestoreDecision::UseLocal => None,
        SuspendRestoreDecision::StageRemote(reason) => Some(reason),
    };

    if let Some(remote_stage_reason) = remote_stage_reason {
        let local_instance_rootfs = cache_dir
            .join(sandbox_id)
            .join(LOCAL_INSTANCE_DIR)
            .join("rootfs.ext4");
        if remote_stage_reason == RestoreSourceSelection::RemoteLocalFilesMissing
            && local_instance_rootfs.exists()
        {
            return Err(Status::failed_precondition(format!(
                "sandbox {sandbox_id} is Suspended but instance/rootfs.ext4 is present; \
                 refusing to stage from S3 to avoid overwriting potentially-live \
                 guest writes"
            )));
        }

        use crate::snapshot_restore::RestoreError;
        let prepared = stage(sandbox_id.to_string(), selected_uri.map(ToOwned::to_owned)).await;
        match prepared {
            Ok(prepared) => {
                restored_snapshot.storage_uri = Some(prepared.storage_uri);
                restored_snapshot.local_artifact_id =
                    prepared.local_artifact_id.map(|id| id.to_string());
                tracing::info!(
                    sandbox_id,
                    reason = %remote_stage_reason,
                    storage_uri = restored_snapshot.storage_uri.as_deref(),
                    "restore: staged suspend snapshot from S3"
                );
            }
            Err(RestoreError::SnapshotNotRegistered(_))
                if remote_stage_reason == RestoreSourceSelection::RemoteLocalFilesMissing =>
            {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id} is Suspended but local snapshot files are \
                     missing and no S3 snapshot is registered to stage from"
                )));
            }
            Err(RestoreError::ClusterApiUnconfigured(_))
                if remote_stage_reason == RestoreSourceSelection::RemoteLocalFilesMissing =>
            {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id} is Suspended but local snapshot files are \
                     missing and no S3 snapshot is registered to stage from"
                )));
            }
            Err(
                RestoreError::SnapshotNotRegistered(_) | RestoreError::ClusterApiUnconfigured(_),
            ) => {
                return Err(Status::unavailable(format!(
                    "newer ready suspend snapshot for sandbox {sandbox_id} could not be staged"
                )));
            }
            Err(RestoreError::AdmissionRejected(status)) => return Err(status),
            Err(error @ RestoreError::RootfsPolicy(..)) => {
                return Err(Status::failed_precondition(error.to_string()));
            }
            Err(reason) => {
                return Err(Status::unavailable(format!(
                    "suspend snapshot stage-in failed: {reason}"
                )));
            }
        }
    }

    Ok(())
}

impl SandboxManagerService {
    fn resolve_start_context_route_identity(
        &self,
        sandbox: &crate::db::SandboxRow,
        ctx: &mut StartSandboxContext,
    ) -> Result<(), Status> {
        let resolved_route_identity = self.resolve_runtime_activation_identity_with_authority(
            &sandbox.id,
            ctx.space_api_route_identity.clone(),
            sandbox.pool_template.is_some(),
            ctx.identity_free_direct_authority,
        )?;
        ctx.space_api_route_identity = resolved_route_identity.identity;
        ctx.open_activation_claim_eligible = resolved_route_identity.open_activation_claim_eligible;
        ctx.execution_generation = ctx.space_api_route_identity.execution_generation;
        Ok(())
    }

    pub(super) async fn node_sandbox_memory_policy(
        node_client: &mut NodeManagerGrpcClient,
    ) -> Result<NodeSandboxMemoryPolicy, Status> {
        let response = node_client
            .get_network_pool_status(NetworkPoolStatusRequest {})
            .await
            .map_err(memory_policy_lookup_error)?
            .into_inner();
        Self::node_sandbox_memory_policy_from(response)
    }

    fn node_sandbox_memory_policy_from(
        response: pplx_space_node_mgr_client::proto::NetworkPoolStatusResponse,
    ) -> Result<NodeSandboxMemoryPolicy, Status> {
        let cgroup_memory_mib = required_node_memory_policy_field(
            response.sandbox_cgroup_memory_mib,
            "sandbox_cgroup_memory_mib",
        )?;
        let headroom_mib = required_node_memory_policy_field(
            response.sandbox_memory_headroom_mib,
            "sandbox_memory_headroom_mib",
        )?;
        let computed_guest_memory_max_mib =
            cgroup_memory_mib.checked_sub(headroom_mib).ok_or_else(|| {
                Status::internal(format!(
                    "invalid node sandbox memory policy: cgroup={cgroup_memory_mib} MiB headroom={headroom_mib} MiB"
                ))
            })?;
        if computed_guest_memory_max_mib == 0 {
            return Err(Status::internal(format!(
                "invalid node sandbox memory policy: cgroup={cgroup_memory_mib} MiB headroom={headroom_mib} MiB"
            )));
        }

        let reported_guest_memory_max_mib = required_node_memory_policy_field(
            response.sandbox_guest_memory_max_mib,
            "sandbox_guest_memory_max_mib",
        )?;
        let guest_memory_max_mib = reported_guest_memory_max_mib.min(computed_guest_memory_max_mib);
        let guest_memory_max_mib = u32::try_from(guest_memory_max_mib).map_err(|_| {
            Status::internal(format!(
                "node sandbox guest memory max exceeds u32: {guest_memory_max_mib} MiB"
            ))
        })?;

        Ok(NodeSandboxMemoryPolicy {
            cgroup_memory_mib,
            guest_memory_max_mib,
            headroom_mib,
        })
    }

    pub(super) fn resolve_sandbox_memory_policy(
        &self,
        requested_memory_mib: u32,
        node_memory_policy: NodeSandboxMemoryPolicy,
    ) -> ResolvedSandboxMemoryPolicy {
        let sandbox_config_cap = self.config.max_memory_mib.unwrap_or(u32::MAX);
        let effective_memory_mib = requested_memory_mib
            .min(sandbox_config_cap)
            .min(node_memory_policy.guest_memory_max_mib);
        ResolvedSandboxMemoryPolicy {
            requested_memory_mib,
            effective_memory_mib,
            cgroup_memory_mib: node_memory_policy.cgroup_memory_mib_for_guest(effective_memory_mib),
            guest_memory_max_mib: node_memory_policy.guest_memory_max_mib,
            cgroup_memory_max_mib: node_memory_policy.cgroup_memory_mib,
            memory_headroom_mib: node_memory_policy.headroom_mib,
        }
    }

    pub(super) async fn resolve_sandbox_memory_policy_for_request(
        &self,
        requested_memory_mib: u32,
        caller_metadata: &tonic::metadata::MetadataMap,
    ) -> Result<ResolvedSandboxMemoryPolicy, Status> {
        // Template-manager asks for this on every reconcile, so the read shares
        // the service's kept node-manager channel rather than dialing
        // node-manager for each call. It keeps the 10 s budget the dial used
        // to have: template-manager falls back to an unclamped memory size
        // when this fails, so a node-manager restart must be waited out, not
        // failed fast. The budget bounds the whole read, not only its retries:
        // the kept channel dials inside an attempt, and a node-manager that
        // accepts but never answers would otherwise hold an attempt until the
        // server's own timeout. The caller's deadline, when it sent one, ends
        // the read sooner.
        const POLICY: pplx_space_common_grpc::GrpcRetryPolicy =
            pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY;
        let node_manager = self.node_manager.lazy();
        let response = pplx_space_common_grpc::retry_grpc_status_with_context(
            "node-manager",
            "GetNetworkPoolStatus",
            POLICY,
            pplx_space_common_grpc::GrpcRetryContext::with_timeout(POLICY.max_elapsed)
                .with_grpc_timeout_metadata(caller_metadata),
            || {
                let mut client = node_manager.clone();
                async move {
                    client
                        .get_network_pool_status(NetworkPoolStatusRequest {})
                        .await
                }
            },
        )
        .await
        .map_err(memory_policy_lookup_error)?
        .into_inner();
        let node_memory_policy = Self::node_sandbox_memory_policy_from(response)?;
        Ok(self.resolve_sandbox_memory_policy(requested_memory_mib, node_memory_policy))
    }

    fn resolve_start_sandbox_target(
        &self,
        sandbox_id: &str,
        ctx: &StartSandboxContext,
        restrict_to_template_manager_owned: bool,
    ) -> Result<StartSandboxTarget, Status> {
        match self.lookup_sandbox(sandbox_id) {
            Ok(sandbox) => {
                super::activation_support::validate_route_identity_sandbox(
                    &ctx.space_api_route_identity,
                    &sandbox.id,
                )?;
                Ok(StartSandboxTarget::Local(Box::new(StartSandboxLookup {
                    sandbox,
                    restored_snapshot: RestoredSnapshotProvenance::default(),
                })))
            }
            Err(status)
                if status.code() == tonic::Code::NotFound
                    && (ctx.checkpoint == SUSPEND_CHECKPOINT
                        || ctx.restore_artifact_as_new.is_some()) =>
            {
                if restrict_to_template_manager_owned {
                    return Err(status);
                }
                let sandbox_id = uuid::Uuid::parse_str(sandbox_id)
                    .map(|id| id.to_string())
                    .map_err(|_| status)?;
                super::activation_support::validate_route_identity_sandbox(
                    &ctx.space_api_route_identity,
                    &sandbox_id,
                )?;
                Ok(StartSandboxTarget::MissingCheckpointRestore { sandbox_id })
            }
            Err(other) => Err(other),
        }
    }

    pub(super) fn checkpoint_restore_cleanup_episode_id(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<Option<SandboxCleanupEpisodeId>, Status> {
        if !matches!(
            sandbox.state,
            SandboxState::CleanupRequired | SandboxState::CleanupFailed
        ) {
            return Ok(None);
        }
        let db = self
            .db
            .lock()
            .map_err(|_| Status::internal("database mutex poisoned"))?;
        let episode = crate::db::read::sandbox_view(&db, &sandbox.id)
            .cleanup_episode()
            .strict()
            .map_err(|error| Status::internal(format!("load restore cleanup: {error}")))?;
        Ok(episode
            .filter(|episode| {
                episode.requested_at.is_some()
                    && episode.completion == crate::db::CleanupCompletion::MarkSuspended
                    && episode.activation_state == SandboxState::Restoring
                    && episode.entered_from_state == Some(SandboxState::Suspended)
                    && episode.quarantine_recovery.is_none()
            })
            .map(|episode| episode.id))
    }

    /// The stopped-target boundary, shared by both `Start`-side sites.
    ///
    /// One place records the span field, checks the row against the artifact
    /// the request named, and builds the reply — those three steps *are* the
    /// boundary, and keeping them in one method is what stops the two sites
    /// drifting apart on any of them. A stopped sandbox has no runtime, so the
    /// reply carries only the identity it converged.
    fn materialized_stopped_response(
        &self,
        sandbox: &crate::db::SandboxRow,
        expected: pplx_space_common_core::types::LocalArtifactId,
    ) -> Result<Response<StartResponse>, Status> {
        tracing::Span::current().record(
            "start_execution_path",
            SandboxStartExecutionPath::DiskMaterialize.as_str(),
        );
        verify_stopped_materialization(sandbox, expected)?;
        // A `Stopped` row with a live Firecracker process is not a stopped
        // sandbox: the VM can still write the disk the pin names, so the caller
        // must not treat this node as a stopped destination.
        if (self.sandbox_runtime_live)(&sandbox.id) {
            return Err(Status::failed_precondition(format!(
                "sandbox {} is stopped locally pinning artifact {expected}, but its runtime is \
                 still live; refusing to report it as materialized",
                sandbox.id
            )));
        }
        // The row and its pin are only identity. Vouching for a placement also
        // needs the bytes a start would boot from, or the caller could retire
        // the source over a node that cannot run the sandbox.
        match (self.sandbox_rootfs_present)(&sandbox.id) {
            Ok(true) => {}
            Ok(false) => {
                return Err(Status::failed_precondition(format!(
                    "sandbox {} is stopped locally pinning artifact {expected}, but its rootfs \
                     is missing; refusing to report it as materialized",
                    sandbox.id
                )));
            }
            // A stat that failed isn't evidence the bytes are gone, but it
            // isn't evidence they're there either, and nothing on the stopped
            // path repairs it (the permission repair runs only on a cold
            // start). Refuse with the real reason rather than retrying forever.
            Err(error) => {
                return Err(Status::failed_precondition(format!(
                    "sandbox {} is stopped locally pinning artifact {expected}, but its rootfs \
                     could not be checked: {error}",
                    sandbox.id
                )));
            }
        }
        Ok(Response::new(StartResponse {
            sandbox_id: sandbox.id.clone(),
            ..Default::default()
        }))
    }

    async fn materialize_missing_checkpoint_restore(
        &self,
        sandbox_id: &str,
        ctx: &mut StartSandboxContext,
    ) -> Result<StartSandboxLookup, Status> {
        // The local sandbox row is gone on this node (cold-evicted, or the
        // restore landed on a different node). Even without a URI threaded into
        // the request, resolve the sandbox's latest ready backup from
        // cluster-api: run_cross_node_restore with a None URI performs the
        // GetSuspendSnapshotLocation lookup. A sandbox with no registered ready
        // snapshot fails downstream as SnapshotNotRegistered.
        let resolved_route_identity = self.resolve_runtime_activation_identity(
            sandbox_id,
            ctx.space_api_route_identity.clone(),
            false,
        )?;
        let parsed_sandbox_id = sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox id: {error}")))?;
        ctx.space_api_route_identity = resolve_missing_checkpoint_restore_route_identity(
            parsed_sandbox_id,
            resolved_route_identity.identity,
        )?;
        ctx.execution_generation = ctx.space_api_route_identity.execution_generation;

        let original_not_found = Status::not_found(format!("sandbox not found: {sandbox_id}"));
        let cross_node_started_at = std::time::Instant::now();
        let cross_node_codec_at_start = pplx_space_metrics::snapshot::CODEC_FULL;
        // The `started` sample is deferred rather than emitted here. This
        // function runs precisely when the local row is absent, so the
        // encryption stamp is unknown until the restore repopulates it —
        // and emitting a provisional `encrypted:false` start against a
        // terminal `encrypted:true` would unpair the two, making
        // `started - completed` and any encrypted rate or latency panel
        // wrong for exactly the BYOK flows they are meant to measure.
        // Instead each terminal site emits the start immediately before
        // itself with the same value, so the pair always agrees. Failures
        // that happen before the stamp is knowable pair as `false`, which
        // is self-consistent rather than merely provisional.
        let cross_node_start = DeferredRestoreStart::new("cross_node", cross_node_codec_at_start);
        match self
            .run_cross_node_restore(CrossNodeRestoreRequest {
                sandbox_id,
                storage_uri: ctx.suspend_restore_storage_uri.as_deref(),
                execution_generation: ctx.execution_generation,
                activation_attempt_id: ctx.space_api_route_identity.activation_attempt_id,
                recovery: None,
                restore_artifact_as_new: ctx.restore_artifact_as_new.as_ref(),
                target: ctx.restore_target,
                staging_admission: CrossNodeRestoreStagingAdmission::Foreground,
            })
            .await
        {
            Ok(provenance) => {
                let sandbox = self.lookup_sandbox(sandbox_id).map_err(|error| {
                    let observed_encrypted =
                        super::recovery_cross_node::encrypted_from_staged_sidecar(sandbox_id);
                    cross_node_start.emit(observed_encrypted);
                    pplx_space_metrics::snapshot::restore_failed(
                        "cross_node",
                        cross_node_codec_at_start,
                        observed_encrypted,
                        "repopulate_row",
                        "post_insert_lookup_failed",
                        cross_node_started_at,
                    );
                    Status::internal(format!(
                        "cross-node restore repopulated sandbox row but \
                         post-insert lookup failed: {}",
                        error.message()
                    ))
                })?;
                let codec = codec_from_local_sidecar(&sandbox);
                cross_node_start.emit(sandbox.volume_encryption);
                pplx_space_metrics::snapshot::restore_completed(
                    "cross_node",
                    codec,
                    sandbox.volume_encryption,
                    cross_node_started_at,
                );
                let restored_snapshot = match provenance {
                    Some(provenance) => RestoredSnapshotProvenance {
                        storage_uri: Some(provenance.storage_uri),
                        local_artifact_id: provenance.local_artifact_id,
                    },
                    None => RestoredSnapshotProvenance::default(),
                };
                Ok(StartSandboxLookup {
                    sandbox,
                    restored_snapshot,
                })
            }
            Err(crate::snapshot_restore::RestoreError::SnapshotNotRegistered(_)) => {
                tracing::warn!(
                    sandbox_id,
                    "cross-node restore skipped: no suspend snapshot registered"
                );
                let observed_encrypted =
                    super::recovery_cross_node::encrypted_from_staged_sidecar(sandbox_id);
                cross_node_start.emit(observed_encrypted);
                pplx_space_metrics::snapshot::restore_failed(
                    "cross_node",
                    cross_node_codec_at_start,
                    observed_encrypted,
                    "cluster_lookup",
                    "snapshot_not_registered",
                    cross_node_started_at,
                );
                Err(original_not_found)
            }
            Err(crate::snapshot_restore::RestoreError::ClusterApiUnconfigured(reason)) => {
                tracing::debug!(
                    sandbox_id,
                    reason,
                    "cross-node restore skipped: cluster-api not configured"
                );
                let observed_encrypted =
                    super::recovery_cross_node::encrypted_from_staged_sidecar(sandbox_id);
                cross_node_start.emit(observed_encrypted);
                pplx_space_metrics::snapshot::restore_failed(
                    "cross_node",
                    cross_node_codec_at_start,
                    observed_encrypted,
                    "cluster_lookup",
                    "cluster_api_unconfigured",
                    cross_node_started_at,
                );
                Err(original_not_found)
            }
            Err(crate::snapshot_restore::RestoreError::AdmissionRejected(status)) => {
                tracing::warn!(
                    sandbox_id,
                    error = %status,
                    "cross-node restore admission rejected"
                );
                let observed_encrypted =
                    super::recovery_cross_node::encrypted_from_staged_sidecar(sandbox_id);
                cross_node_start.emit(observed_encrypted);
                pplx_space_metrics::snapshot::restore_failed(
                    "cross_node",
                    cross_node_codec_at_start,
                    observed_encrypted,
                    "stage",
                    "admission_rejected",
                    cross_node_started_at,
                );
                Err(status)
            }
            Err(error @ crate::snapshot_restore::RestoreError::RootfsPolicy(..)) => {
                let observed_encrypted =
                    super::recovery_cross_node::encrypted_from_staged_sidecar(sandbox_id);
                cross_node_start.emit(observed_encrypted);
                pplx_space_metrics::snapshot::restore_failed(
                    "cross_node",
                    cross_node_codec_at_start,
                    observed_encrypted,
                    "stage",
                    "rootfs_policy",
                    cross_node_started_at,
                );
                Err(Status::failed_precondition(error.to_string()))
            }
            Err(error @ crate::snapshot_restore::RestoreError::CanonicalRootConflict { .. }) => {
                let observed_encrypted =
                    super::recovery_cross_node::encrypted_from_staged_sidecar(sandbox_id);
                cross_node_start.emit(observed_encrypted);
                pplx_space_metrics::snapshot::restore_failed(
                    "cross_node",
                    cross_node_codec_at_start,
                    observed_encrypted,
                    "stage",
                    "canonical_root_conflict",
                    cross_node_started_at,
                );
                Err(Status::failed_precondition(error.to_string()))
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    %error,
                    "cross-node restore failed; surfacing Unavailable"
                );
                let observed_encrypted =
                    super::recovery_cross_node::encrypted_from_staged_sidecar(sandbox_id);
                cross_node_start.emit(observed_encrypted);
                pplx_space_metrics::snapshot::restore_stage_failed(
                    "cross_node",
                    cross_node_codec_at_start,
                    observed_encrypted,
                    error.stage_error_kind(),
                    cross_node_started_at,
                );
                // A caller that named an artifact the staged sidecar does not
                // match cannot be helped by retrying — `UNAVAILABLE` would have
                // it re-stage the metadata on every attempt, forever. Everything
                // else here is a transfer or control-plane failure, which is
                // retryable.
                Err(if error.is_stopped_materialization_refusal() {
                    Status::failed_precondition(error.to_string())
                } else {
                    Status::unavailable(format!("cross-node restore stage-in failed: {error}"))
                })
            }
        }
    }

    async fn restore_suspended_checkpoint_start(
        &self,
        req: &StartRequest,
        sandbox: crate::db::SandboxRow,
        ctx: &mut StartSandboxContext,
        mut restored_snapshot: RestoredSnapshotProvenance,
        cleanup_id: Option<SandboxCleanupEpisodeId>,
    ) -> Result<Response<StartResponse>, Status> {
        if ctx.target_state != SandboxState::Stopped {
            // A stopped target records `disk_materialize` at the boundary
            // below; recording `checkpoint_restore` here too would leave the
            // field's value dependent on subscriber last-write semantics.
            tracing::Span::current().record(
                "start_execution_path",
                SandboxStartExecutionPath::CheckpointRestore.as_str(),
            );
        }
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let mut artifact_guard = Some(
            start_stage(
                "restore.artifact_lock_wait",
                &sandbox.id,
                wait_for_activation_admission(&sandbox.id, artifact_lock.lock()),
            )
            .await?,
        );
        // Adoption and remote staging below write the checkpoint before the
        // activation claim, and a retiring upgrade's checkpoint may be stale; a
        // runtime upgrade inserts its fence under this lock.
        self.refuse_unretired_runtime_upgrade(&sandbox.id)?;
        let mut sandbox = self.lookup_sandbox(&sandbox.id)?;
        let mut remote_stage_completed = false;

        if sandbox.state == SandboxState::Restoring {
            let staging_lock = self.auto_lifecycle.staging_lock(&sandbox.id);
            if staging_lock.try_lock().is_err() {
                return Err(Status::unavailable(format!(
                    "sandbox {} restore staging is in progress; retry",
                    sandbox.id
                )));
            }
            let target_state = wake_target_state_from_proto(req.target_state)?;
            let runtime_live = (self.sandbox_runtime_live)(&sandbox.id);
            match checkpoint_restore_retry_action(&sandbox, target_state, runtime_live) {
                CheckpointRestoreRetryAction::ResumeRestoring => {
                    self.resolve_start_context_route_identity(&sandbox, ctx)?;
                    let provenance = start_stage(
                        "restore.cross_node.retry_stage",
                        &sandbox.id,
                        self.run_cross_node_restore(CrossNodeRestoreRequest {
                            sandbox_id: &sandbox.id,
                            storage_uri: ctx.suspend_restore_storage_uri.as_deref(),
                            execution_generation: ctx.execution_generation,
                            activation_attempt_id: ctx
                                .space_api_route_identity
                                .activation_attempt_id,
                            recovery: None,
                            restore_artifact_as_new: ctx.restore_artifact_as_new.as_ref(),
                            target: ctx.restore_target,
                            // Zero, not the orphan path's budget: this handler
                            // already peeked the staging lock non-blockingly a few
                            // lines up and answered `unavailable`, so waiting here
                            // would add a stall the caller did not have before.
                            staging_admission: CrossNodeRestoreStagingAdmission::WaitAtMost(
                                std::time::Duration::ZERO,
                            ),
                        }),
                    )
                    .await
                    .map_err(|error| match error {
                        crate::snapshot_restore::RestoreError::AdmissionRejected(status) => status,
                        error @ crate::snapshot_restore::RestoreError::RootfsPolicy(..) => {
                            Status::failed_precondition(error.to_string())
                        }
                        error @ crate::snapshot_restore::RestoreError::RestoreOperationBusy {
                            ..
                        } => Status::unavailable(error.to_string()),
                        error @ crate::snapshot_restore::RestoreError::CanonicalRootConflict {
                            ..
                        } => Status::failed_precondition(error.to_string()),
                        // A caller that named the wrong artifact cannot be
                        // helped by retrying: `UNAVAILABLE` would have it retry
                        // the same refused restore on every attempt, forever.
                        error if error.is_stopped_materialization_refusal() => {
                            Status::failed_precondition(error.to_string())
                        }
                        other => Status::unavailable(format!(
                            "resume interrupted cross-node restore staging failed: {other}"
                        )),
                    })?;
                    let provenance = provenance.ok_or_else(|| {
                        Status::unavailable(format!(
                            "sandbox {} restore staging ownership changed; retry",
                            sandbox.id
                        ))
                    })?;
                    restored_snapshot.storage_uri = Some(provenance.storage_uri);
                    restored_snapshot.local_artifact_id = provenance.local_artifact_id;
                    remote_stage_completed = true;
                    sandbox = self.lookup_sandbox(&sandbox.id)?;
                }
                CheckpointRestoreRetryAction::CleanupRestoring => {
                    // Retryable pre-runtime cross-node restores are resumed
                    // above. Anything reaching this arm has live or partially
                    // committed runtime evidence, an artifact pin, or cleanup
                    // intent and must follow the explicit retirement path.
                    // The retirement's persisted intent must not clobber a
                    // foreign cleanup intent already on the row (a pending
                    // user delete): overwriting `delete_row` with
                    // `mark_suspended` would silently convert the delete into
                    // a retirement. The CAS writes only while no intent is
                    // persisted; on a miss the row is left exactly as found —
                    // the restore fails closed below (`expected suspended`)
                    // and the pending cleanup keeps its intent.
                    let retired = {
                        let db = self.db_conn()?;
                        crate::db::mark_cleanup_required_if_no_intent(
                            &db,
                            &sandbox.id,
                            true,
                            crate::db::CleanupCompletion::MarkSuspended,
                            "checkpoint retry found an ambiguous restoring runtime",
                        )
                        .map_err(|error| {
                            Status::internal(format!(
                                "persist ambiguous restore cleanup before retry: {error}"
                            ))
                        })?
                    };
                    if !retired {
                        pplx_space_metrics::snapshot::restore_retry_preserved_cleanup_intent();
                    }
                    sandbox = self.lookup_sandbox(&sandbox.id)?;
                }
                CheckpointRestoreRetryAction::Reattach(_)
                | CheckpointRestoreRetryAction::Reject => {
                    return Err(Status::failed_precondition(format!(
                        "cannot recover sandbox {} while local state is {}",
                        sandbox.id, sandbox.state
                    )));
                }
            }
        }

        if sandbox.cleanup_completion == Some(crate::db::CleanupCompletion::MarkSuspended) {
            if !matches!(
                sandbox.state,
                SandboxState::CleanupRequired | SandboxState::CleanupFailed
            ) {
                let cleanup_reason = sandbox.failure_reason.clone().unwrap_or_else(|| {
                    "restore requires completion of pending runtime retirement".to_string()
                });
                let db = self.db_conn()?;
                crate::db::mark_cleanup_required(
                    &db,
                    &sandbox.id,
                    true,
                    crate::db::CleanupCompletion::MarkSuspended,
                    &cleanup_reason,
                )
                .map_err(|error| {
                    Status::internal(format!(
                        "persist pending runtime retirement before restore: {error}"
                    ))
                })?;
            }
            // The cleanup executor may need the artifact lock to validate and
            // publish its completion. Never await it while owning that lock.
            // Reacquire and reload before authorizing any restore work.
            drop(artifact_guard.take());
            let cleanup = super::cleanup::SynchronousCleanupRequest {
                sandbox_id: &sandbox.id,
                uuid8: sandbox.uuid8.as_str(),
                keep_filesystem: true,
                context: "retire incomplete runtime before checkpoint restore",
                completion: CleanupCompletion::MarkSuspended,
                published_artifact: None,
                immediate_kill: Self::cleanup_immediate_kill(CleanupCompletion::MarkSuspended),
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            };
            if let Some(cleanup_id) = cleanup_id {
                start_stage(
                    "restore.pending_cleanup",
                    &sandbox.id,
                    self.run_cleanup_episode_to_completion(cleanup, cleanup_id),
                )
                .await?;
            } else {
                start_stage(
                    "restore.pending_cleanup",
                    &sandbox.id,
                    self.run_cleanup_to_completion(cleanup),
                )
                .await?;
            }
            artifact_guard =
                Some(wait_for_activation_admission(&sandbox.id, artifact_lock.lock()).await?);
            sandbox = self.lookup_sandbox(&sandbox.id)?;
        }
        let _artifact_guard = artifact_guard;

        // The second half of the stopped materialization's boundary, after the
        // cleanup above rather than before it.
        //
        // After the resume arm above, a resumed materialization leaves the row
        // `Stopped`, which the `expected suspended` precondition below would
        // refuse; this is where it gets its success reply. A non-retryable
        // `Restoring` row (the `CleanupRestoring` action) — residue a stopped
        // materialization never creates — gets the
        // same filesystem-keeping retirement any checkpoint `Start` drives for
        // it, and the row that cleanup leaves (`Suspended`) is refused here
        // rather than launched. `start_from_checkpoint` refuses a stopped target too, so
        // that refusal is the backstop, not the boundary.
        //
        // Keyed on the target alone: `materialized_stopped_response` decides by
        // row state, answering a `Stopped` row and refusing anything else,
        // which is exactly the split this position needs.
        if let CrossNodeRestoreTarget::StoppedDiskArtifact { expected } = ctx.restore_target {
            // A concurrent fresh materialization can finalize the row between
            // this request's first look and the re-read above, so the resume
            // arm's fence may not have run; apply it here before answering.
            self.resolve_start_context_route_identity(&sandbox, ctx)?;
            return self.materialized_stopped_response(&sandbox, expected);
        }

        if sandbox.state != SandboxState::Suspended {
            return Err(Status::failed_precondition(format!(
                "cannot restore sandbox {} while local state is {}; expected suspended",
                sandbox.id, sandbox.state
            )));
        }

        self.resolve_start_context_route_identity(&sandbox, ctx)?;
        // Ask before staging the snapshot and before `restore_started`, so a
        // retry the control plane already retired neither stages a snapshot
        // nor counts as a failed restore. Kubelet repeats such a retry until
        // the pod is deleted. `start_from_checkpoint` asks again before it
        // claims the row, for a retirement that lands during staging.
        let open_route_activation = super::activation_support::open_route_activation_claim(
            &ctx.space_api_route_identity,
            ctx.route_identity_provenance,
            ctx.open_activation_claim_eligible,
        );
        start_stage(
            "restore.execution_currency",
            &sandbox.id,
            self.refuse_retired_activation_retry(&sandbox.id, open_route_activation.as_ref()),
        )
        .await?;

        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let local_snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        let local_vmstate = local_snapshot_dir.join("vmstate");
        if sandbox.current_local_artifact_id.is_none() && local_vmstate.exists() {
            let local_artifact_id = LocalArtifactId::new();
            let local_snapshot_path = local_snapshot_dir.to_string_lossy().into_owned();
            self.local_artifacts
                .adopt_suspended(&sandbox.id, local_artifact_id, &local_snapshot_path)
                .map_err(|err| Status::internal(format!("db artifact adoption failed: {err}")))?;
            sandbox = self.lookup_sandbox(&sandbox.id)?;
        }

        if !remote_stage_completed {
            let staging_lock = self.auto_lifecycle.staging_lock(&sandbox.id);
            let staging_guard = start_stage(
                "restore.staging_lock_wait",
                &sandbox.id,
                wait_for_activation_admission(&sandbox.id, staging_lock.lock()),
            )
            .await?;
            start_stage(
                "restore.remote_suspend_stage",
                &sandbox.id,
                stage_remote_suspend_restore_if_needed(
                    &sandbox.id,
                    std::path::Path::new(CACHE_DIR),
                    ctx.restore_placement,
                    ctx.suspend_restore_storage_uri.as_deref(),
                    &mut restored_snapshot,
                    |sandbox_id, storage_uri| {
                        prepare_suspend_restore_dir(
                            self,
                            &sandbox.user,
                            sandbox_id,
                            storage_uri,
                            ctx.execution_generation,
                            ctx.space_api_route_identity.activation_attempt_id,
                        )
                    },
                ),
            )
            .await?;
            drop(staging_guard);
        }

        let restore_lease = if let Some(local_artifact_id) = sandbox.current_local_artifact_id {
            let span = tracing::info_span!(
                "sbxmgr.start.stage",
                sandbox_id = %sandbox.id,
                stage = "restore.local_artifact_checkout",
                otel.name = "restore.local_artifact_checkout",
                local_artifact_id = %local_artifact_id,
            );
            let _entered = span.enter();
            match self
                .local_artifacts
                .check_out_for_restore(local_artifact_id)
            {
                Ok(lease) => Some(lease),
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox.id,
                        local_artifact_id = %local_artifact_id,
                        %error,
                        "failed to check out local_artifact for restore; reaper may race",
                    );
                    None
                }
            }
        } else {
            None
        };
        let restore_started_at = std::time::Instant::now();
        let codec = codec_from_local_sidecar(&sandbox);
        pplx_space_metrics::snapshot::restore_started("in_place", codec, sandbox.volume_encryption);
        // Placement alone cannot distinguish a local checkpoint from a remote
        // snapshot staged on the same node. Record provenance after staging;
        // this span measures checkpoint resume, not the preceding transfer.
        let resume_span = tracing::info_span!(
            "restore.checkpoint.resume",
            remote_snapshot_staged = restored_snapshot.storage_uri.is_some(),
            otel.status_code = tracing::field::Empty,
        );
        let result = self
            .start_from_checkpoint(StartFromCheckpointLaunch {
                vcpu: ctx.req_vcpu,
                activation_auto_lifecycle: ctx.activation_auto_lifecycle.as_ref(),
                operation_activation_attempt_id: ctx.operation_activation_attempt_id,
                sandbox: &sandbox,
                blocking: req.blocking,
                netns_path: ctx.netns_path.clone(),
                target_state: wake_target_state_from_proto(req.target_state)?,
                execution_generation: ctx.execution_generation,
                space_api_route_identity: ctx.space_api_route_identity.clone(),
                route_identity_provenance: ctx.route_identity_provenance,
                open_activation_claim_eligible: ctx.open_activation_claim_eligible,
                restored_snapshot,
            })
            .instrument(resume_span.clone())
            .await;
        resume_span.record(
            "otel.status_code",
            if result.is_ok() { "OK" } else { "ERROR" },
        );
        drop(resume_span);

        if let Some(lease) = restore_lease
            && let Err(error) = lease.release()
        {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                %error,
                "failed to release restore lease after start_from_checkpoint; row may be stuck in 'restoring' until manual intervention",
            );
        }
        match &result {
            Ok(_) => pplx_space_metrics::snapshot::restore_completed(
                "in_place",
                codec,
                sandbox.volume_encryption,
                restore_started_at,
            ),
            Err(status) => pplx_space_metrics::snapshot::restore_failed(
                "in_place",
                codec,
                sandbox.volume_encryption,
                "start_from_checkpoint",
                short_grpc_code(status.code()),
                restore_started_at,
            ),
        }
        result
    }

    async fn try_start_from_warm_snapshot(
        &self,
        ctx: &mut StartSandboxContext,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<Option<Response<StartResponse>>, Status> {
        if !ctx.from_warm {
            return Ok(None);
        }
        admit_warm_start(sandbox)?;

        self.ensure_snapshot_restore_supported("start_from_warm")?;
        // Staging writes the checkpoint directory and moves the row before the
        // restore's activation claim, so a runtime upgrade that still owes its
        // retirement is checked first, under the lock its fence is inserted under.
        let artifact_guard = start_stage(
            "warm.artifact_lock_wait",
            &sandbox.id,
            wait_for_activation_admission(
                &sandbox.id,
                self.auto_lifecycle.artifact_lock(&sandbox.id).lock_owned(),
            ),
        )
        .await?;
        self.refuse_unretired_runtime_upgrade(&sandbox.id)?;
        tracing::Span::current().record(
            "start_execution_path",
            SandboxStartExecutionPath::WarmSnapshot.as_str(),
        );
        let sandbox_dir = self.cache_root().join(&sandbox.id);
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        let vmstate_path = snapshot_dir.join("vmstate");

        if !vmstate_path.exists() {
            let resolved_template = resolve_template_paths_for_start(
                self.template_resolver.as_ref(),
                pplx_space_template_mgr_client::proto::TemplateRef {
                    name: sandbox.template.clone(),
                    content_hash: sandbox.template_hash.clone(),
                    tag: None,
                },
                sandbox,
            )
            .await?;
            reject_incompatible_template_spaced_compatibility(
                &sandbox.template,
                resolved_template
                    .content_hash
                    .as_deref()
                    .unwrap_or("<unhashed>"),
                resolved_template.spaced_version.as_deref(),
                resolved_template.spaced_compatibility_family.as_deref(),
            )?;
            let snap_dir = resolved_template.snapshot_dir.ok_or_else(|| {
                Status::failed_precondition(format!(
                    "no base snapshot for template '{}'",
                    sandbox.template
                ))
            })?;
            let base_vmstate = format!("{snap_dir}/vmstate.base");
            let base_snapshot = format!("{snap_dir}/snapshot.base");

            if !std::path::Path::new(&base_vmstate).exists()
                || !std::path::Path::new(&base_snapshot).exists()
            {
                return Err(Status::failed_precondition(format!(
                    "no base snapshot for template '{}' (need vmstate.base + snapshot.base)",
                    sandbox.template
                )));
            }
            // Restore selects its consumer from the snapshot's producer, so a
            // snapshot captured under another binding would move the sandbox.
            // Rows that predate saved bindings ran the initial environment.
            let legacy;
            let execution = match sandbox.execution_metadata.as_ref() {
                Some(execution) => execution,
                None => {
                    legacy = pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
                        super::tail_support::host_architecture()
                            .map_err(|error| Status::failed_precondition(error.to_string()))?,
                        vec![],
                    );
                    &legacy
                }
            };
            if !super::validation::base_snapshot_matches_execution(&snap_dir, execution) {
                return Err(Status::failed_precondition(format!(
                    "base snapshot for template '{}' was captured under another execution environment; start cold instead",
                    sandbox.template
                )));
            }

            std::fs::create_dir_all(&snapshot_dir)
                .map_err(|e| Status::internal(format!("create snapshot dir: {e}")))?;

            // Publish metadata before vmstate: an interrupted copy must not make
            // a retry observe a checkpoint without its allocation evidence.
            stage_base_snapshot_runtime_metadata(
                std::path::Path::new(&snap_dir),
                &snapshot_dir,
                resolved_template.snapshot_vcpu,
            )?;

            pplx_space_node_runtime_core::reflink_copy(
                std::path::Path::new(&base_vmstate),
                &vmstate_path,
            )
            .map_err(|e| Status::internal(format!("reflink vmstate.base: {e}")))?;

            let memory_path = snapshot_dir.join("memory");
            pplx_space_node_runtime_core::reflink_copy(
                std::path::Path::new(&base_snapshot),
                &memory_path,
            )
            .map_err(|e| Status::internal(format!("reflink snapshot.base: {e}")))?;
        }

        if sandbox.state != SandboxState::Suspended {
            let db = self.db_conn()?;
            write_warm_start_restaged(&db, &sandbox.id)?;
        }
        drop(artifact_guard);

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.resolve_start_context_route_identity(&sandbox, ctx)?;
        let space_api_route_identity = ctx.space_api_route_identity.clone();
        let response = self
            .start_from_checkpoint(StartFromCheckpointLaunch {
                vcpu: ctx.req_vcpu,
                activation_auto_lifecycle: ctx.activation_auto_lifecycle.as_ref(),
                operation_activation_attempt_id: ctx.operation_activation_attempt_id,
                sandbox: &sandbox,
                blocking: ctx.blocking,
                netns_path: ctx.netns_path.clone(),
                target_state: ctx.target_state,
                execution_generation: space_api_route_identity.execution_generation,
                space_api_route_identity,
                route_identity_provenance: ctx.route_identity_provenance,
                open_activation_claim_eligible: ctx.open_activation_claim_eligible,
                restored_snapshot: RestoredSnapshotProvenance {
                    storage_uri: ctx.suspend_restore_storage_uri.clone(),
                    local_artifact_id: None,
                },
            })
            .await?;
        Ok(Some(response))
    }

    #[cfg(test)]
    pub(super) async fn start_sandbox(
        &self,
        req: StartRequest,
        restrict_to_template_manager_owned: bool,
    ) -> Result<Response<StartResponse>, Status> {
        let identity_free_direct_authority = self
            .lookup_sandbox(&req.sandbox_id)
            .ok()
            .filter(Self::sandbox_owner_allows_local_route_identity_mint)
            .map(|_| IdentityFreeDirectActivationAuthority::LocalProducer)
            .unwrap_or_default();
        self.start_sandbox_with_authority(
            req,
            restrict_to_template_manager_owned,
            identity_free_direct_authority,
        )
        .await
    }

    pub(super) async fn start_sandbox_with_authority(
        &self,
        req: StartRequest,
        restrict_to_template_manager_owned: bool,
        identity_free_direct_authority: IdentityFreeDirectActivationAuthority,
    ) -> Result<Response<StartResponse>, Status> {
        self.start_sandbox_with_upgrade_authority(
            req,
            restrict_to_template_manager_owned,
            identity_free_direct_authority,
            None,
        )
        .await
    }

    pub(super) async fn start_sandbox_with_upgrade_authority(
        &self,
        req: StartRequest,
        restrict_to_template_manager_owned: bool,
        identity_free_direct_authority: IdentityFreeDirectActivationAuthority,
        runtime_upgrade: Option<crate::db::RuntimeUpgradeOperationId>,
    ) -> Result<Response<StartResponse>, Status> {
        let sandbox_id = &req.sandbox_id;
        let mut start_ctx = StartSandboxContext::from_request(&req)?;
        start_ctx.identity_free_direct_authority = identity_free_direct_authority;
        if runtime_upgrade.is_some()
            && (!start_ctx.checkpoint.is_empty()
                || start_ctx.from_warm
                || start_ctx.restore_artifact_as_new.is_some()
                || start_ctx.target_state != SandboxState::Started)
        {
            return Err(Status::invalid_argument(
                "a runtime upgrade boots only by a cold start into the started lifecycle",
            ));
        }
        start_ctx.runtime_upgrade = runtime_upgrade;
        let req_vcpu = start_ctx.req_vcpu;
        let req_memory_mib = start_ctx.req_memory_mib;
        let checkpoint = start_ctx.checkpoint.clone();
        let netns_path = start_ctx.netns_path.clone();
        let ready_timeout_secs = start_ctx.ready_timeout_secs;
        let start_path = SandboxStartPath::from_request(&req);
        let span = tracing::Span::current();
        let target_state = start_ctx.target_state;
        span.record("checkpoint_present", !checkpoint.is_empty());
        span.record("target_state", tracing::field::display(target_state));

        let start_target = {
            let _entered = tracing::info_span!("sbxmgr.start.lookup_sandbox", sandbox_id).entered();
            self.resolve_start_sandbox_target(
                sandbox_id,
                &start_ctx,
                restrict_to_template_manager_owned,
            )?
        };
        let mut start_lookup = match start_target {
            StartSandboxTarget::Local(start_lookup) => *start_lookup,
            StartSandboxTarget::MissingCheckpointRestore {
                sandbox_id: restore_sandbox_id,
            } => {
                match self
                    .materialize_missing_checkpoint_restore(&restore_sandbox_id, &mut start_ctx)
                    .instrument(tracing::info_span!(
                        "sbxmgr.start.cross_node_restore",
                        sandbox_id = %restore_sandbox_id
                    ))
                    .await
                {
                    Ok(start_lookup) => start_lookup,
                    Err(status) => return Err(status),
                }
            }
        };
        let interrupted_restore_action = interrupted_restore_action(
            &checkpoint,
            start_lookup.sandbox.state,
            start_lookup.sandbox.cleanup_completion,
            start_ctx.restore_artifact_as_new.is_some(),
        );
        if interrupted_restore_action == InterruptedRestoreAction::RematerializeArtifact {
            let restore_sandbox_id = start_lookup.sandbox.id.clone();
            start_lookup = self
                .materialize_missing_checkpoint_restore(&restore_sandbox_id, &mut start_ctx)
                .instrument(tracing::info_span!(
                    "sbxmgr.start.cross_node_restore_retry",
                    sandbox_id = %restore_sandbox_id
                ))
                .await?;
        }
        let sandbox = start_lookup.sandbox;
        if let Some(policy) = &start_ctx.activation_auto_lifecycle {
            let runtime = sandbox
                .runtime
                .parse()
                .map_err(|e| Status::failed_precondition(format!("runtime: {e}")))?;
            super::validation::validate_runtime_lifecycle_policy(runtime, policy)?;
            super::validation::validate_execution_lifecycle_policy(
                sandbox.execution_metadata.as_ref(),
                policy,
            )?;
        }
        let restore_cleanup_id = if checkpoint == SUSPEND_CHECKPOINT {
            self.checkpoint_restore_cleanup_episode_id(&sandbox)?
        } else {
            None
        };
        span.record("start_path", start_path.as_str());
        span.record("template", tracing::field::display(&sandbox.template));
        span.record(
            "content_hash",
            tracing::field::display(sandbox.template_hash.as_deref().unwrap_or("")),
        );
        span.record("runtime", tracing::field::display(&sandbox.runtime));
        span.record("initial_state", tracing::field::display(sandbox.state));
        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let snapshot_vmstate_path = canonical_suspend_snapshot_dir(&sandbox_dir).join("vmstate");
        span.record("has_snapshot", snapshot_vmstate_path.exists());
        if restrict_to_template_manager_owned && !Self::sandbox_owned_by_template_manager(&sandbox)
        {
            return Err(Status::permission_denied(
                "space-tmplmgr may only call Start for template-owned sandboxes",
            ));
        }
        // The hypervisor boundary for an evacuated stopped sandbox. The local
        // row now carries the materialized disk artifact and its pin, which is
        // the whole of what `stopped` means on a node: a rootfs and a row, no
        // VM.
        //
        // This is the first of two boundaries, not the only one. It answers
        // every row state except `Restoring`, which has to keep going so an
        // interrupted materialization can resume; the second boundary, inside
        // `restore_suspended_checkpoint_start` after its cleanup, catches that
        // one. Both are needed — the resume path can reach a launch on its own.
        //
        // `Restoring` is the one state that keeps going: it means a previous
        // materialization was interrupted after adopting the row and before
        // finalizing it. Its resume is the `ResumeSuspendCheckpoint` branch
        // just below, which re-stages under the staging lock and then answers
        // from the second boundary rather than launching — refusing here
        // instead would strand the row, because nothing else drives it.
        if let CrossNodeRestoreTarget::StoppedDiskArtifact { expected } = start_ctx.restore_target
            && sandbox.state != SandboxState::Restoring
        {
            // The same route-identity resolution every restore path applies
            // before it acts — the requested node and activation-claim checks —
            // so a request fenced elsewhere is not told the sandbox converged.
            // The artifact-id comparison below is what separates generations
            // of the bytes themselves.
            self.resolve_start_context_route_identity(&sandbox, &mut start_ctx)?;
            return self.materialized_stopped_response(&sandbox, expected);
        }
        // Suspend-checkpoint retries serialize with artifact consumers. A
        // cancelled first attempt may leave either a live Restoring runtime or
        // persisted MarkSuspended cleanup intent; both must converge before a
        // new restore launch is allowed.
        if interrupted_restore_action == InterruptedRestoreAction::ResumeSuspendCheckpoint {
            return self
                .restore_suspended_checkpoint_start(
                    &req,
                    sandbox,
                    &mut start_ctx,
                    start_lookup.restored_snapshot,
                    restore_cleanup_id,
                )
                .await;
        }

        // A retried restore whose first attempt already launched Firecracker
        // and flipped the local row to `Started` must reattach to the live
        // runtime. This covers both suspend-checkpoint restores and
        // restore-as-new cold boots whose successful response was lost.
        if (checkpoint == SUSPEND_CHECKPOINT || start_ctx.restore_artifact_as_new.is_some())
            && let Some(response) = checkpoint_start_response_for_existing_started_sandbox(
                &sandbox,
                target_state,
                (self.sandbox_runtime_live)(&sandbox.id),
            )
        {
            if start_ctx.activation_auto_lifecycle.is_some()
                && start_ctx.space_api_route_identity.execution_generation
                    != sandbox.space_api_route_identity.execution_generation
            {
                return Err(Status::failed_precondition(
                    "restore retry targets a different execution",
                ));
            }
            tracing::info!(
                sandbox_id = %sandbox.id,
                checkpoint,
                restore_artifact_as_new = start_ctx.restore_artifact_as_new.is_some(),
                target_state = %target_state,
                "restore start found an already-started local runtime; \
                 reattaching to the existing runtime"
            );
            span.record(
                "start_execution_path",
                SandboxStartExecutionPath::CheckpointReattach.as_str(),
            );
            return Ok(Response::new(response));
        }

        if !checkpoint.is_empty() {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                state = %sandbox.state,
                checkpoint,
                "refusing checkpoint start because local sandbox state is not restorable"
            );
            return Err(Status::failed_precondition(format!(
                "cannot start sandbox {} from checkpoint '{checkpoint}' while local state is {}; \
                 expected Suspended or NotFound cross-node restore path",
                sandbox.id, sandbox.state
            )));
        }

        if sandbox.state == SandboxState::Failed {
            return Err(Status::failed_precondition(
                "Failed sandbox requires explicit deletion before cold start",
            ));
        }

        if let Some(response) = self
            .try_start_from_warm_snapshot(&mut start_ctx, &sandbox)
            .await?
        {
            return Ok(response);
        }
        span.record(
            "start_execution_path",
            SandboxStartExecutionPath::FreshCold.as_str(),
        );
        let prepare_source = start_stage_timeout(
            "fresh.template_prepare_source",
            START_TEMPLATE_RESOLUTION_TIMEOUT,
            &sandbox.id,
            template_source_for_sandbox_prepare(
                &self.node_manager,
                self.template_resolver.as_ref(),
                &sandbox,
            ),
        )
        .await?;
        span.record("has_rootfs", prepare_source.existing_artifact);
        let selection_mode = if prepare_source.existing_artifact {
            pplx_space_execution_common::proto::ExecutionSelectionMode::ExistingColdBoot
        } else {
            pplx_space_execution_common::proto::ExecutionSelectionMode::NewColdBoot
        };
        let requested_execution = Some((&prepare_source.execution).into());
        let prepare_template = prepare_source.template;
        let prepare_template_dir = prepare_source.template_dir;
        let template_defaults = start_stage_timeout_or(
            "fresh.template_runtime_defaults",
            START_TEMPLATE_RESOLUTION_TIMEOUT,
            &sandbox.id,
            TemplateRuntimeDefaults::default(),
            template_runtime_defaults_for_sandbox_start(
                self.template_resolver.as_ref(),
                &sandbox,
                prepare_template_dir.as_deref(),
            ),
        )
        .await;

        let id = &sandbox.id;
        let activation_admission_permit = self
            .activation_admission_limiter
            .acquire(ActivationAdmissionKind::Cold, id)
            .await?;
        if let Err(error) = sandbox.state.transition(SandboxState::Starting) {
            activation_admission_permit.finish(AdmissionPermitOutcome::Failed);
            return Err(Status::from(error));
        }
        if let Err(error) = self.resolve_start_context_route_identity(&sandbox, &mut start_ctx) {
            activation_admission_permit.finish(AdmissionPermitOutcome::Failed);
            return Err(error);
        }
        let open_route_activation = super::activation_support::open_route_activation_claim(
            &start_ctx.space_api_route_identity,
            start_ctx.route_identity_provenance,
            start_ctx.open_activation_claim_eligible,
        );
        if let Err(error) = self
            .refuse_retired_activation_retry(id, open_route_activation.as_ref())
            .await
        {
            activation_admission_permit.finish(AdmissionPermitOutcome::Failed);
            return Err(error);
        }

        // Serialize the whole activation boundary with cleanup. A cleanup
        // admitted for the previous execution must either finish before this
        // start claims the row, or observe the attached replacement generation
        // before it can dispatch another node cleanup.
        let activation_lock = self.cleanup_lock(id);
        let activation_lock_owner = crate::service::cleanup_support::CleanupLockGuard::new(
            Arc::clone(&self.cleanup_locks),
            id.clone(),
            activation_lock,
        );
        let activation_fence =
            wait_for_activation_admission(id, activation_lock_owner.lock()).await?;

        let cleanup_episode = {
            let db = self.db_conn()?;
            crate::db::begin_activation_with_route_evidence(
                &db,
                id,
                sandbox.state,
                crate::db::ActivationKind::ColdStart,
                start_ctx.execution_generation,
                start_ctx.space_api_route_identity.activation_attempt_id,
                crate::db::ActivationRouteAdmission {
                    auto_lifecycle: start_ctx.activation_auto_lifecycle.as_ref(),
                    operation_activation_attempt_id: start_ctx.operation_activation_attempt_id,
                    evidence: crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(
                        &start_ctx.space_api_route_identity,
                        start_ctx.route_identity_provenance,
                    ),
                    open_claim: open_route_activation.as_ref(),
                    runtime_upgrade: start_ctx.runtime_upgrade,
                },
            )
            .map_err(|error| {
                super::runtime_upgrade::begin_activation_status(
                    error,
                    "failed to claim starting state before cold activation",
                )
            })
        };
        let cleanup_episode = match cleanup_episode {
            Ok(cleanup_episode) => cleanup_episode,
            Err(error) => {
                activation_admission_permit.finish(AdmissionPermitOutcome::Failed);
                return Err(error);
            }
        };
        let Some(cleanup_episode) = cleanup_episode else {
            activation_admission_permit.finish(AdmissionPermitOutcome::Failed);
            emit_sandbox_event(
                id,
                SandboxEventType::LifecycleTransitionCasLost,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .bool_field(
                        pplx_space_events::SandboxEventBoolField::VolumeEncryption,
                        sandbox.volume_encryption,
                    )
                    .trigger(SandboxEventTrigger::System)
                    .operation("cold_activation")
                    .reason("start_entry_cas_lost")
                    .with_from_state(sandbox.state.to_string())
                    .target_state(SandboxState::Starting.to_string())
                    .rows_affected("0"),
            );
            return Err(Status::unavailable(format!(
                "sandbox activation ownership changed from {}; retry start",
                sandbox.state,
            )));
        };
        let cleanup_id = cleanup_episode.id;
        let cleanup_completion = cleanup_episode.completion;
        let mut uuid8 = sandbox.uuid8.clone();
        let mut activation_cleanup = match self.activation_cleanup_guard(
            id,
            &uuid8,
            ActivationCleanupSpec {
                cleanup_id,
                completion: cleanup_completion,
                context: "cleanup after interrupted cold activation",
                interrupted_reason_code: SandboxQuarantineReasonCode::ColdActivationInterrupted,
                interrupted_reason: "cold activation did not complete",
            },
        ) {
            Ok(guard) => guard,
            Err(error) => {
                activation_admission_permit.finish(AdmissionPermitOutcome::Failed);
                return Err(error);
            }
        };
        activation_cleanup.admit(activation_admission_permit);
        let mut node_client = start_stage_timeout(
            "fresh.connect_node_mgr",
            START_NODE_METADATA_TIMEOUT,
            id,
            async {
                connect_node(&self.node_manager)
                    .await
                    .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))
            },
        )
        .await
        .map_err(|error| activation_cleanup.failure(error))?;
        let node_memory_policy = start_stage_timeout(
            "fresh.node_memory_policy",
            START_NODE_METADATA_TIMEOUT,
            id,
            Self::node_sandbox_memory_policy(&mut node_client),
        )
        .await
        .map_err(|error| activation_cleanup.failure(error))?;
        let memory_default = template_defaults.memory_mib.unwrap_or(1024);
        let requested_memory_mib =
            optional_positive_or_default("memory_mib", req_memory_mib, memory_default)
                .map_err(|error| activation_cleanup.failure(error))?;
        let memory_policy =
            self.resolve_sandbox_memory_policy(requested_memory_mib, node_memory_policy);
        let mem_mib = memory_policy.effective_memory_mib;
        // Like boot args, the balloon device exists only from a cold boot:
        // snapshots carry the device state they were taken with.
        let balloon = template_defaults
            .balloon
            .for_memory_mib(mem_mib)
            .map_err(|error| activation_cleanup.failure(error))?;
        pplx_space_node_runtime_core::boot::validate_resources(
            prepare_source.execution.runtime().kind,
            balloon.is_some(),
            self.config.hugepages,
        )
        .map_err(|error| {
            activation_cleanup.failure(Status::failed_precondition(error.to_string()))
        })?;
        if mem_mib < requested_memory_mib {
            tracing::info!(
                sandbox_id = %id,
                requested_memory_mib,
                effective_memory_mib = mem_mib,
                sandbox_config_max_memory_mib = ?self.config.max_memory_mib,
                node_guest_memory_max_mib = memory_policy.guest_memory_max_mib,
                node_cgroup_memory_mib = memory_policy.cgroup_memory_max_mib,
                "clamped sandbox guest memory"
            );
        }

        let execution_fence = read_node_execution_fence(&self.db, id)?.ok_or_else(|| {
            Status::failed_precondition("activation has no durable node execution identity")
        })?;
        // Allocate network slot. Node-manager always creates a SPACE-managed sandbox
        // namespace; when netns_path is set (CNI), it bridges to the pod namespace.
        let has_netns = netns_path.is_some();
        if !has_netns {
            tracing::warn!(
                sandbox_id = %id,
                has_netns,
                checkpoint,
                from_warm = req.from_warm,
                blocking = req.blocking,
                target_state = %start_ctx.target_state,
                "starting sandbox network allocation without pod netns"
            );
        }
        let alloc = node_client
            .allocate(AllocateRequest {
                execution_fence: Some(execution_fence.into()),
                sandbox_id: id.clone(),
                netns_path,
                sandbox_cgroup_memory_mib: memory_policy.cgroup_memory_mib,
            })
            .instrument(tracing::info_span!(
                "sbxmgr.start.node.allocate_network",
                sandbox_id = %id,
                has_netns
            ))
            .await
            .map_err(|e| {
                activation_cleanup.failure(Status::new(
                    e.code(),
                    format!("network allocate failed: {}", e.message()),
                ))
            })?
            .into_inner();
        let sbx_uid = alloc.sandbox_uid;
        if sbx_uid < 10000 {
            // No VM, sidecar, cgroup, or filesystem work has been launched yet.
            // This rejects a bad fresh allocation, so the raw lease can be
            // returned without running full sandbox cleanup.
            let release_result = node_client
                .release(ReleaseRequest {
                    execution_fence: Some(execution_fence.into()),
                    sandbox_id: id.clone(),
                    force: true,
                })
                .await;
            let db = self.db_conn()?;
            let restored_state =
                match crate::db::rollback_starting_episode(&db, id, sandbox.state, cleanup_id) {
                    Ok(restored_state) => restored_state,
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id = %id,
                            state = ?sandbox.state,
                            %error,
                            "failed to restore sandbox state after invalid sandbox uid"
                        );
                        false
                    }
                };
            if release_result.is_ok() && restored_state {
                activation_cleanup.complete_handled_failure();
            } else {
                activation_cleanup.mark_permit_failed();
            }
            return Err(Status::internal(format!(
                "node-manager returned invalid sandbox uid {sbx_uid}"
            )));
        }
        persist_allocated_runtime_label(
            &self.db,
            id,
            &mut uuid8,
            &mut activation_cleanup,
            alloc.slot_label.clone(),
        )
        .map_err(|error| activation_cleanup.failure(error))?;

        // The runtime adapter owns device names and configuration syntax.
        let vcpu_default = template_defaults.vcpu.unwrap_or(2);
        let vcpu = optional_positive_or_default("vcpu", req_vcpu, vcpu_default)
            .map_err(|error| activation_cleanup.failure(error))?;
        let is_builder = sandbox.user.kind() == SandboxOwnerKind::Builder;
        let guest_cid = alloc.slot.checked_add(3).ok_or_else(|| {
            activation_cleanup.failure(Status::internal("allocated guest CID overflows"))
        })?;
        let boot = pplx_space_node_runtime_core::boot::LinuxBoot {
            guest: prepare_source.execution.guest(),
            cpus: pplx_space_common_core::types::VcpuCount::try_from_u32(vcpu).map_err(
                |error| activation_cleanup.failure(Status::invalid_argument(error.to_string())),
            )?,
            memory_mib: std::num::NonZeroU64::new(u64::from(mem_mib)).ok_or_else(|| {
                activation_cleanup
                    .failure(Status::invalid_argument("guest memory must be positive"))
            })?,
            guest_cid: std::num::NonZeroU32::new(guest_cid).ok_or_else(|| {
                activation_cleanup.failure(Status::internal("allocated guest CID is zero"))
            })?,
            tap: &alloc.tap_device,
            mac: &alloc.guest_mac,
            disk_rate: self.rootfs_rate_limiter(is_builder, sandbox.disk_io_limits),
            network_rate: (!is_builder).then_some(NET_RATE_LIMITER),
            balloon,
            huge_pages: self.config.hugepages,
            console: guest_console_logs_enabled(),
            builder: is_builder,
        };
        let vm_config = pplx_space_node_runtime_core::boot::configuration(
            prepare_source.execution.runtime().kind,
            &boot,
        )
        .map_err(|error| {
            use pplx_space_node_runtime_core::boot::BootConfigError;
            let status = match error {
                BootConfigError::Json(_) | BootConfigError::UnsupportedRuntime(_) => {
                    Status::internal(error.to_string())
                }
                BootConfigError::Guest(_)
                | BootConfigError::UnsupportedFeature { .. }
                | BootConfigError::MemoryOverflow
                | BootConfigError::Invalid(_) => Status::failed_precondition(error.to_string()),
            };
            activation_cleanup.failure(status)
        })?;

        // Host-visible paths
        let run_dir = format!("{RUN_DIR}/{id}");
        let log_path = format!("{run_dir}/fc.log");
        let vsock_uds_path = pplx_space_node_runtime_core::control::guest_socket_path(
            prepare_source.execution.runtime().kind,
            id,
        )
        .map_err(|error| {
            activation_cleanup.failure(Status::failed_precondition(error.to_string()))
        })?
        .display()
        .to_string();

        // Node-manager prepares filesystem after slot allocation so UID/GID
        // ownership is derived from the allocated slot. If Create only recorded
        // metadata, this call also performs the first rootfs clone.
        let mut prepare_client = node_client.clone();
        let prepare_rootfs_size_mb = prepare_template.as_ref().and(sandbox.rootfs_size_mb);
        let prepare_sandbox = async {
            prepare_client
                .prepare_sandbox(PrepareSandboxRequest {
                    execution_fence: Some(execution_fence.into()),
                    sandbox_id: id.clone(),
                    template: prepare_template,
                    uid: sbx_uid,
                    vm_config_json: Some(vm_config),
                    metadata_json: super::tail_support::activation_metadata_json(
                        &sandbox,
                        Some(
                            pplx_space_common_core::types::VcpuCount::try_from_u32(vcpu)
                                .map_err(|error| Status::invalid_argument(error.to_string()))?,
                        ),
                        Some(
                            pplx_space_common_core::types::MemorySize::mib(u64::from(mem_mib))
                                .map_err(|error| Status::invalid_argument(error.to_string()))?,
                        ),
                    )?,
                    runtime: sandbox.runtime.clone(),
                    template_dir: prepare_template_dir,
                    snapshot_dir: None,
                    rootfs_size_mb: prepare_rootfs_size_mb,
                    requested_execution,
                    execution_selection_mode: selection_mode as i32,
                    artifact_template: Some(sandbox.template.clone()),
                    content_hash: sandbox.template_hash.clone(),
                })
                .instrument(tracing::info_span!(
                    "sbxmgr.start.node.prepare_sandbox",
                    sandbox_id = %id
                ))
                .await
        };
        // Net-gateway only needs the allocated network namespace and sandbox
        // UID. Start it while node-manager clones/prepares the rootfs, but do
        // not wait on gateway process readiness before launching Firecracker.
        // The namespace is fail-closed before TPROXY exists. Pool members do
        // not receive policy until adoption assigns tenant identity. The
        // gateway begins pulling policy via host-control the moment its
        // process starts, and host-control authorizes it by registered
        // (pid, uid) — so the PID registration is part of this task, not a
        // post-join step after the jailer and spaced-host launches.
        let mut netgtw_client = node_client.clone();
        let netgtw_no_identity = sandbox.pool_template.is_some();
        let netgtw_sandbox_id = id.to_string();
        let normalized_route_identity = space_api_route_identity_for_runtime_activation(
            start_ctx.space_api_route_identity.clone(),
        );
        let route_identity = normalized_route_identity.identity;
        let netgtw_route_identity = route_identity.clone();
        let netgtw_db = self.db.clone();
        let netgtw_launch_config =
            net_gateway_launch_config(&self.feature_flags, &netgtw_sandbox_id);
        let netgtw_task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
            async move {
                register_net_gateway_launch(
                    &netgtw_db,
                    &netgtw_sandbox_id,
                    sbx_uid,
                    launch_net_gateway_via_node(
                        &mut netgtw_client,
                        execution_fence,
                        &netgtw_sandbox_id,
                        sbx_uid,
                        netgtw_no_identity,
                        Some(&netgtw_route_identity),
                        &netgtw_launch_config,
                    ),
                )
                .await
            }
            .in_current_span(),
        ));

        let prepare_response = match prepare_sandbox.await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                settle_net_gateway_launch_task(netgtw_task, id, "prepare-failed").await;
                let reason = format!("prepare sandbox failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::ColdPrepare.reason_code(),
                    reason.clone(),
                );
                return Err(ActivationFailureStage::ColdPrepare.failure_status(&e, reason));
            }
        };
        let resolved_execution_result = (|| {
            let resolved_execution = prepare_response.resolved_execution.ok_or_else(|| {
                Status::internal(
                    "node-manager prepare response omitted resolved execution metadata",
                )
            })?;
            let resolved_execution_metadata =
                pplx_space_common_core::types::ExecutionMetadata::try_from(&resolved_execution)
                    .map_err(|error| Status::internal(error.to_string()))?;
            super::validation::require_environment_acknowledgement(
                &prepare_source.execution,
                &resolved_execution_metadata,
            )?;
            {
                let db = self.db_conn()?;
                crate::db::upsert_sandbox_execution_metadata(&db, id, &resolved_execution_metadata)
                    .map_err(|error| {
                        Status::internal(format!("persist sandbox execution metadata: {error}"))
                    })?;
            }
            Ok::<_, Status>((resolved_execution, resolved_execution_metadata))
        })();
        let (resolved_execution, resolved_execution_metadata) = match resolved_execution_result {
            Ok(resolved) => resolved,
            Err(error) => {
                settle_net_gateway_launch_task(netgtw_task, id, "prepare-metadata-failed").await;
                let reason = format!("prepare metadata failed: {}", error.message());
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::ColdMetadata.reason_code(),
                    reason.clone(),
                );
                return Err(ActivationFailureStage::ColdMetadata.failure_status(&error, reason));
            }
        };

        // BYOK: bring up the encrypted rootfs device and hand Firecracker the
        // dm-crypt mapper instead of the plaintext file. The sandbox's
        // immutable volume_encryption flag (stamped at create) decides
        // encryption; a BYOK failure is fail-closed: fail the start rather
        // than boot on a plaintext rootfs. Plaintext sandboxes may still take
        // one node-local round-trip for the warm-pool composite *shape* below
        // — a device-layout choice, not an encryption decision.
        let rootfs_block_device_path = if sandbox.volume_encryption {
            match crate::rootfs_encryption::prepare_encrypted_rootfs(
                id,
                execution_fence,
                None,
                &self.node_manager,
            )
            .instrument(tracing::info_span!(
                "sbxmgr.start.encrypted_rootfs_prepare",
                sandbox_id = %id
            ))
            .await
            {
                Ok(path) => Some(path),
                Err(e) => {
                    settle_net_gateway_launch_task(
                        netgtw_task,
                        id,
                        "encrypted-rootfs-prepare-failed",
                    )
                    .await;
                    let reason = format!("encrypted rootfs preparation failed: {e}");
                    let status = e.into_status(reason.clone());
                    activation_cleanup.cleanup_after_failure(
                        ActivationFailureStage::ColdEncryptedRootfsPrepare.reason_code(),
                        reason.clone(),
                    );
                    return Err(status);
                }
            }
        } else if crate::rootfs_encryption::pool_plain_cow_exists(id) {
            // Warm-pool member: boot on the dm-snapshot composite over a
            // plaintext COW so a later claim can re-home that COW onto an
            // org-keyed LUKS one without cold-creating. A surviving cow.plain
            // selects this arm regardless of pool_template — a claimed member
            // (pool_template cleared at claim) must keep riding the COW that
            // holds its writes, and prepare re-opens a persisted store
            // idempotently.
            //
            // Failing closed on every error would strand a member that is
            // still perfectly usable on the plaintext file, so a preparation
            // failure degrades to it — but ONLY when the failure proves no
            // device stack can survive node-side AND the sandbox has no
            // committed COW state to abandon. Booting the file over a live
            // composite mutates the composite's read-only origin underneath
            // it; booting it past an existing COW (cow.plain, or cow.luks
            // from an unstamped re-home) silently rolls back every write the
            // guest made through it.
            match crate::rootfs_encryption::prepare_snapshot_rootfs(
                id,
                execution_fence,
                &self.node_manager,
            )
            .instrument(tracing::info_span!(
                "sbxmgr.start.pool_snapshot_rootfs_prepare",
                sandbox_id = %id
            ))
            .await
            {
                Ok(path) => Some(path),
                Err(e)
                    if e.degrade_to_rootfs_file_is_safe()
                        && !crate::rootfs_encryption::committed_cow_state_exists(id) =>
                {
                    tracing::warn!(
                        sandbox_id = %id,
                        error = %e,
                        "pool snapshot rootfs preparation failed; booting on the template file"
                    );
                    None
                }
                Err(e) => {
                    settle_net_gateway_launch_task(
                        netgtw_task,
                        id,
                        "pool-snapshot-rootfs-prepare-failed",
                    )
                    .await;
                    let reason = format!(
                        "pool snapshot rootfs preparation failed and the rootfs file is not \
                         safe to boot (possible surviving composite or committed COW state): {e}"
                    );
                    activation_cleanup.cleanup_after_failure(
                        ActivationFailureStage::ColdPrepare.reason_code(),
                        reason.clone(),
                    );
                    return Err(Status::internal(reason));
                }
            }
        } else if crate::rootfs_encryption::rehomed_encrypted_cow_exists(id) {
            // A committed claim-time re-home whose volume_encryption stamp
            // has not landed: the guest's writes live in cow.luks, and only
            // the encrypted arm knows how to boot them. Booting the bare
            // rootfs file here would orphan those writes AND mutate the
            // origin the encrypted COW's exception store references. Fail
            // closed until the claim path stamps the row.
            settle_net_gateway_launch_task(netgtw_task, id, "unstamped-rehomed-cow").await;
            let reason = format!(
                "sandbox {id} carries an encrypted rootfs COW but is not stamped \
                 volume_encryption; refusing to boot the plaintext rootfs file over it"
            );
            activation_cleanup.cleanup_after_failure(
                ActivationFailureStage::ColdPrepare.reason_code(),
                reason.clone(),
            );
            return Err(Status::internal(reason));
        } else {
            None
        };

        // Launch Firecracker first. spaced-host also binds a bridge-host socket
        // inside the Firecracker jail root, which launch_sandbox creates.
        let jailer_result = node_client
            .launch_sandbox(LaunchSandboxRequest {
                execution_fence: Some(execution_fence.into()),
                sandbox_id: id.clone(),
                uid: sbx_uid,
                gid: sbx_uid,
                vcpu_count: Some(vcpu),
                // Cgroup gets guest memory plus fixed MiB headroom for FC overhead
                // (page tables, virtio buffers, FC process). This ensures the
                // guest kernel OOM-kills processes before the host kills FC.
                memory_mib: Some(memory_policy.cgroup_memory_mib),
                pids_max: None,
                runtime: sandbox.runtime.clone(),
                snapshot_restore: false,
                requested_execution: Some(resolved_execution),
                execution_selection_mode: selection_mode as i32,
                rootfs_block_device_path,
            })
            .instrument(tracing::info_span!(
                "sbxmgr.start.node.launch_firecracker",
                sandbox_id = %id,
                snapshot_restore = false
            ))
            .await;
        let jailer_resp = match jailer_result {
            Ok(response) => {
                activation_cleanup.finish_admission(AdmissionPermitOutcome::Succeeded);
                response.into_inner()
            }
            Err(e) => {
                settle_net_gateway_launch_task(netgtw_task, id, "jailer-launch-failed").await;
                let reason = format!("jailer launch failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::ColdFirecrackerLaunch.reason_code(),
                    reason.clone(),
                );
                return Err(
                    ActivationFailureStage::ColdFirecrackerLaunch.failure_status(&e, reason)
                );
            }
        };
        let pid = jailer_resp.pid;
        let (spaced_host_result, netgtw_result) = {
            let mut spaced_host_client = node_client.clone();
            let launch_spaced_host = launch_spaced_host_via_node(
                &mut spaced_host_client,
                execution_fence,
                id,
                sbx_uid,
                &vsock_uds_path,
                &self.host_control_socket_path,
                "fresh",
            );
            let wait_netgtw = async {
                netgtw_task.await.map_err(|error| {
                    Status::internal(format!("net-gateway launch task failed: {error}"))
                })?
            };
            tokio::join!(launch_spaced_host, wait_netgtw)
        };
        let spaced_host_resp = match spaced_host_result {
            Ok(response) => response,
            Err(e) => {
                // The spaced-host failure wins the early return, discarding
                // the gateway arm's outcome; log a dropped gateway launch or
                // registration-write error so it stays diagnosable.
                if let Err(netgtw_error) = &netgtw_result {
                    tracing::warn!(
                        sandbox_id = %id,
                        %netgtw_error,
                        "net-gateway launch/registration also failed during failed spaced-host launch"
                    );
                }
                let reason = format!("launch spaced-host failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::ColdSidecarLaunch.reason_code(),
                    reason,
                );
                return Err(e);
            }
        };
        // The gateway task covers both the launch and the PID registration
        // write, so a registration failure is cleaned up here exactly like a
        // launch failure.
        let netgtw_resp = match netgtw_result {
            Ok(response) => response,
            Err(e) => {
                let reason = format!("launch net-gateway failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::ColdSidecarLaunch.reason_code(),
                    reason,
                );
                return Err(e);
            }
        };
        let endpoint_proof = match activation_endpoint_proof_requirement(
            &route_identity,
            sandbox.pool_template.as_deref(),
        ) {
            ActivationEndpointProofRequirement::DeferredUntilPoolAdoption => None,
            ActivationEndpointProofRequirement::Required => {
                let endpoint_runtime = match endpoint_runtime_identity_for_activation(
                    id,
                    &route_identity,
                    spaced_host_resp.pid,
                    pid,
                    sbx_uid,
                    guest_cid,
                    &vsock_uds_path,
                ) {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let reason = format!("construct spaced endpoint identity failed: {error}");
                        activation_cleanup.cleanup_after_failure(
                            ActivationFailureStage::ColdSidecarLaunch.reason_code(),
                            reason,
                        );
                        return Err(error);
                    }
                };
                match capture_spaced_endpoint_proof_for_activation(
                    &mut node_client,
                    endpoint_runtime,
                )
                .await
                {
                    Ok(proof) => Some(proof),
                    Err(error) => {
                        let reason = format!("capture spaced endpoint proof failed: {error}");
                        activation_cleanup.cleanup_after_failure(
                            ActivationFailureStage::ColdSidecarLaunch.reason_code(),
                            reason,
                        );
                        return Err(error);
                    }
                }
            }
        };
        // Update SQLite with runtime fields (state stays Starting until guest Ready)
        let api_socket_path = format!("{RUN_DIR}/{id}/api.sock");
        let response_route_identity = space_api_route_identity_to_wire(&route_identity);
        {
            let _entered = tracing::info_span!(
                "sbxmgr.start.db.update_runtime_fields",
                sandbox_id = %id
            )
            .entered();
            self.update_runtime_fields_for_activation(
                id.parse::<SandboxId>().map_err(|error| {
                    Status::invalid_argument(format!("invalid sandbox id: {error}"))
                })?,
                cleanup_id,
                crate::db::RuntimeFields {
                    uid: sbx_uid,
                    slot: alloc.slot,
                    guest_cid,
                    pid,
                    spaced_host_pid: spaced_host_resp.pid,
                    netgtw_pid: netgtw_resp.pid,
                    vsock_uds_path: &vsock_uds_path,
                    api_socket_path: &api_socket_path,
                    spaced_endpoint_proof: endpoint_proof.as_ref(),
                    space_api_route_identity: route_identity,
                    route_identity_provenance: start_ctx.route_identity_provenance,
                },
            )?;
        }
        drop(activation_fence);
        drop(activation_lock_owner);
        emit_runtime_activated_event(RuntimeActivatedEvent {
            sandbox_id: id,
            boot_type: "fresh",
            pid,
            spaced_host_pid: spaced_host_resp.pid,
            net_gateway_pid: netgtw_resp.pid,
            sandbox_uid: sbx_uid,
            network_slot: alloc.slot,
            guest_cid,
            guest_ip: &alloc.guest_ip,
            guest_gateway: &alloc.guest_gateway,
            namespace: &alloc.namespace,
            vcpu: Some(vcpu),
            memory_mib: u32::try_from(memory_policy.cgroup_memory_mib).unwrap_or(u32::MAX),
            has_netns_path: has_netns,
            execution: &resolved_execution_metadata,
            execution_generation: start_ctx.execution_generation.map(|value| value.get()),
            restored_snapshot_storage_uri: None,
            restored_snapshot_local_artifact_id: None,
            local_artifact_id: None,
            volume_encryption: sandbox.volume_encryption,
        });
        self.watchdog
            .add_sandbox(crate::watchdog::SidecarInfo {
                node_execution_id: Some(execution_fence.current()),
                sandbox_id: id.clone(),
                uid: Some(sbx_uid),
                runtime_pid: Some(pid),
                spaced_host_pid: Some(spaced_host_resp.pid),
                netgtw_pid: Some(netgtw_resp.pid),
                vsock_uds_path: Some(vsock_uds_path.clone()),
                process_env: sandbox.process_env.clone(),
                network_tx_bytes: sandbox.network_tx_bytes,
                network_rx_bytes: sandbox.network_rx_bytes,
            })
            .instrument(tracing::info_span!(
                "sbxmgr.start.watchdog.add_sandbox",
                sandbox_id = %id
            ))
            .await;

        // Start snapshot scheduling if enabled
        if sandbox.snapshots_enabled && sandbox.pool_template.is_none() {
            self.snapshot_scheduler
                .add_sandbox(id.clone(), sandbox.template.clone())
                .instrument(tracing::info_span!(
                    "sbxmgr.start.snapshot_scheduler.add_sandbox",
                    sandbox_id = %id
                ))
                .await;
        }

        // Gate reachability on the network policy (fail-closed): the sandbox is
        // not marked Started until the net-gateway reports Ready with an
        // enforcing policy. The gateway hydrates the policy itself, so we don't
        // resolve a ref here — that keeps a SetNetworkPolicy or template change
        // landing mid-start from failing the start on a stale-ref mismatch, and
        // avoids a fallible policy lookup after the sandbox is already launched.
        // If the desired policy changes mid-start, that path enqueues its own
        // reconcile against the live gateway to converge it. Pool members get
        // their policy via the adoption path, so they skip the gate. The push
        // runs concurrently with the guest-readiness wait, so it adds no latency
        // on the common path where guest boot is the slower half.
        let apply_network_policy = !Self::sandbox_is_pool_member(&sandbox);
        if req.blocking {
            if let Err(error) = wait_ready_and_mark_started(
                self.db.clone(),
                id,
                &sandbox.process_env,
                ready_timeout_secs,
                "start",
                apply_network_policy,
                Some(cleanup_id),
            )
            .await
            {
                return Err(activation_cleanup.failure(error));
            }
            activation_cleanup.complete_success();
        } else {
            spawn_wait_ready_state_transition(
                self,
                self.db.clone(),
                StartStateTransition {
                    sandbox_id: id.clone(),
                    process_env: sandbox.process_env.clone(),
                    ready_timeout_secs,
                    phase: "start",
                    apply_network_policy,
                    cleanup_id,
                },
                activation_cleanup,
            )?;
        }
        Ok(Response::new(StartResponse {
            sandbox_id: id.clone(),
            pid,
            guest_ip: alloc.guest_ip,
            guest_gateway: alloc.guest_gateway,
            namespace: alloc.namespace,
            log_path,
            vsock_uds_path,
            route_identity: response_route_identity,
        }))
    }
}

impl SandboxManagerService {
    /// A sandbox VM's rootfs rate limiter, at cold boot and after a snapshot
    /// load alike. Build-manager VMs keep their exemption; every other sandbox
    /// uses the immutable assignment stored on its row.
    pub(super) fn rootfs_rate_limiter(
        &self,
        is_builder: bool,
        limits: pplx_space_common_core::types::DiskIoLimits,
    ) -> Option<RateLimiter> {
        (!is_builder).then(|| RateLimiter::for_disk(limits, self.config.disk_io_burst_secs()))
    }
}

/// The limiter for node-manager to apply after a snapshot load, which
/// otherwise keeps the limiter of the VM the snapshot was taken from.
pub(super) fn drive_rate_limit_proto(
    limiter: RateLimiter,
) -> pplx_space_node_mgr_client::proto::DriveRateLimit {
    let bucket = |bucket: TokenBucket| pplx_space_node_mgr_client::proto::TokenBucket {
        size: bucket.size,
        refill_time_ms: bucket.refill_time_ms,
    };
    pplx_space_node_mgr_client::proto::DriveRateLimit {
        bandwidth: Some(bucket(limiter.bandwidth)),
        operations: Some(bucket(limiter.operations)),
    }
}

/// Each direction of a sandbox's network interface: 50 MiB/s and 10000
/// packets/s, refilled every second.
pub(super) const NET_RATE_LIMITER: RateLimiter = RateLimiter {
    bandwidth: TokenBucket {
        size: 52_428_800,
        refill_time_ms: 1000,
    },
    operations: TokenBucket {
        size: 10_000,
        refill_time_ms: 1000,
    },
};

fn memory_policy_lookup_error(error: Status) -> Status {
    Status::new(
        error.code(),
        format!(
            "node-manager memory policy lookup failed: {}",
            error.message()
        ),
    )
}

#[cfg(test)]
pub(super) mod tests;

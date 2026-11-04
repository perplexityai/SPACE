use super::activation_support::space_api_route_identity_to_wire;
use super::cleanup_support::{StateWriteRejected, require_state_write_from, state_write_result};
use super::tail_support::connect_node;
use super::template_snapshot_support::compute_partial_sha256;
use super::validation::{
    TemplateRuntimeDefaults, reject_incompatible_template_arch,
    reject_incompatible_template_spaced_compatibility, template_execution_metadata,
    template_metadata_arch, template_metadata_spaced_compatibility, template_runtime_defaults,
};
use super::*;
use pplx_space_common_core::types::{EnvMap, EnvSanitizationIssue};
use pplx_space_common_svc::template_lookup::TemplateLookupScope;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, SandboxRestoreProvenance,
    emit_sandbox_event,
};

pub(super) struct ResolvedTemplatePaths {
    pub(super) guest_image: pplx_space_common_core::types::guest_image::GuestImageContract,
    pub(super) template_dir: String,
    pub(super) snapshot_dir: Option<String>,
    pub(super) snapshot_vcpu: Option<pplx_space_common_core::types::VcpuCount>,
    pub(super) content_hash: Option<String>,
    pub(super) default_auto_lifecycle: AutoLifecyclePolicy,
    pub(super) default_network_policy_authority: crate::db::PolicyAuthority,
    pub(super) default_auto_lifecycle_authority: crate::db::PolicyAuthority,
    pub(super) arch: String,
    pub(super) spaced_version: Option<String>,
    pub(super) spaced_compatibility_family: Option<String>,
    pub(super) runtime_defaults: TemplateRuntimeDefaults,
    pub(super) execution: pplx_space_common_core::types::ExecutionMetadata,
}

#[derive(Debug)]
pub(super) struct SandboxPrepareSource {
    pub(super) template: Option<String>,
    pub(super) template_dir: Option<String>,
    pub(super) execution: pplx_space_common_core::types::ExecutionMetadata,
    pub(super) existing_artifact: bool,
}

pub(super) struct RequestedSandboxCreateSpec<'a> {
    pub(super) template: &'a str,
    pub(super) template_hash: &'a str,
    pub(super) user: &'a SandboxOwner,
    pub(super) runtime: &'a str,
    pub(super) rootfs_size_mb: Option<u32>,
    pub(super) pool_template: Option<&'a str>,
    pub(super) volume_encryption: bool,
    pub(super) auto_lifecycle: &'a AutoLifecyclePolicy,
    /// An explicit selection must match the row's saved binding. An
    /// unselected retry keeps the row's own binding.
    pub(super) environment: Option<pplx_space_common_core::types::EnvironmentReference>,
}

pub(super) struct CreateWithIdArgs<'a> {
    pub(super) guest_image: pplx_space_common_core::types::guest_image::GuestImageContract,
    pub(super) req: &'a CreateRequest,
    pub(super) sandbox_owner: &'a SandboxOwner,
    pub(super) effective_mode: BootMode,
    pub(super) id: String,
    pub(super) template_dir: &'a str,
    pub(super) execution: Option<&'a pplx_space_common_core::types::ExecutionMetadata>,
    pub(super) snapshot_dir_resolved: Option<&'a str>,
    pub(super) snapshot_vcpu: Option<pplx_space_common_core::types::VcpuCount>,
    pub(super) content_hash_resolved: Option<&'a str>,
    pub(super) auto_lifecycle: &'a AutoLifecyclePolicy,
    pub(super) auto_lifecycle_authority: crate::db::PolicyAuthority,
    pub(super) default_network_policy_authority: crate::db::PolicyAuthority,
}

#[derive(Debug)]
pub(super) enum SandboxCreateIntent {
    Create { requested_id: Option<Uuid> },
    AdoptPool,
}

pub(super) fn validate_create_request(
    req: &CreateRequest,
) -> Result<(BootMode, Option<Uuid>), Status> {
    if req.runtime.is_empty() {
        return Err(Status::invalid_argument("runtime is required"));
    }
    let runtime: pplx_space_common_core::types::RuntimeKind = req
        .runtime
        .parse()
        .map_err(|e| Status::invalid_argument(format!("runtime: {e}")))?;
    if !matches!(
        runtime,
        pplx_space_common_core::types::RuntimeKind::Firecracker
            | pplx_space_common_core::types::RuntimeKind::CloudHypervisor
    ) {
        return Err(Status::unimplemented(
            "runtime has no sandbox lifecycle adapter",
        ));
    }
    validate_create_request_fields(req)
}

/// Validate inputs independent of the runtime resolved from an environment.
pub(super) fn validate_create_request_fields(
    req: &CreateRequest,
) -> Result<(BootMode, Option<Uuid>), Status> {
    let boot_mode = BootMode::try_from(req.boot_mode).map_err(|_| {
        Status::invalid_argument(format!("unknown boot_mode value {}", req.boot_mode))
    })?;
    if let Some(size_mib) = req.rootfs_size_mb {
        pplx_space_common_core::sandbox_storage::validate_rootfs_mib(size_mib)
            .map_err(|error| Status::invalid_argument(format!("rootfs_size_mb: {error}")))?;
    }
    let requested_id = req
        .sandbox_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .map(|id| {
            Uuid::parse_str(id).map_err(|_| Status::invalid_argument("sandbox_id must be a UUID"))
        })
        .transpose()?;
    pplx_space_common_core::types::DiskIoLimits::try_from_optional(
        req.disk_io_max_operations_per_sec,
        req.disk_io_max_bandwidth_mib_per_sec,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    validate_pool_member_create_options(
        req.pool_template
            .as_deref()
            .filter(|value| !value.is_empty()),
        req.snapshots_enabled,
        req.volume_encryption,
    )?;
    Ok((boot_mode, requested_id))
}

impl SandboxManagerService {
    pub(super) fn resolve_create_intent(
        &self,
        req: &CreateRequest,
        requested_mode: BootMode,
        requested_id: Option<Uuid>,
        target: CreateTarget<'_>,
    ) -> Result<(BootMode, SandboxCreateIntent), Status> {
        if req.rootfs_size_mb.is_some() && requested_mode == BootMode::BaseSnapshot {
            return Err(Status::invalid_argument(
                "rootfs_size_mb requires boot_mode=cold",
            ));
        }
        // Pool is an optimization: donor identity and immutable resources cannot
        // change at adoption. Skip ineligible donors before testing occupancy so
        // valid caller intent follows the same fallback whether the pool is full
        // or empty. A rootfs resize additionally requires a cold create. An
        // explicit environment selection never falls back, while a warm start
        // restores through whichever loader the node selects for the snapshot,
        // and donors are matched by build rather than binding; so a selected
        // create always boots cold.
        let eligible_mode = if req.rootfs_size_mb.is_some() || target.selected {
            BootMode::Cold
        } else if requested_mode == BootMode::Pool
            && (requested_id.is_some()
                || req.volume_encryption
                || req.snapshots_enabled
                || req.disk_io_max_operations_per_sec.is_some()
                || req.disk_io_max_bandwidth_mib_per_sec.is_some()
                || req
                    .pool_template
                    .as_deref()
                    .is_some_and(|value| !value.is_empty()))
        {
            BootMode::BaseSnapshot
        } else {
            requested_mode
        };
        let effective_mode = self.resolve_boot_mode(
            eligible_mode,
            &req.template,
            target.snapshot_dir,
            target.execution,
        )?;
        let intent = if effective_mode == BootMode::Pool {
            SandboxCreateIntent::AdoptPool
        } else {
            SandboxCreateIntent::Create { requested_id }
        };
        Ok((effective_mode, intent))
    }
}

/// What a create will run, as far as choosing its boot path depends on it.
#[derive(Clone, Copy)]
pub(super) struct CreateTarget<'a> {
    pub(super) snapshot_dir: Option<&'a str>,
    pub(super) execution: &'a pplx_space_common_core::types::ExecutionMetadata,
    /// The request selected its environment explicitly.
    pub(super) selected: bool,
}

pub(super) struct RuntimeActivatedEvent<'a> {
    pub(super) sandbox_id: &'a str,
    pub(super) boot_type: &'static str,
    pub(super) pid: u32,
    pub(super) spaced_host_pid: Option<u32>,
    pub(super) net_gateway_pid: u32,
    pub(super) sandbox_uid: u32,
    pub(super) network_slot: u32,
    pub(super) guest_cid: u32,
    pub(super) guest_ip: &'a str,
    pub(super) guest_gateway: &'a str,
    pub(super) namespace: &'a str,
    pub(super) vcpu: Option<u32>,
    pub(super) memory_mib: u32,
    pub(super) has_netns_path: bool,
    pub(super) execution: &'a pplx_space_common_core::types::ExecutionMetadata,
    pub(super) execution_generation: Option<i64>,
    pub(super) restored_snapshot_storage_uri: Option<String>,
    pub(super) restored_snapshot_local_artifact_id: Option<String>,
    pub(super) local_artifact_id: Option<String>,
    /// The sandbox row's immutable `volume_encryption` stamp, which selected
    /// the encrypted or plaintext rootfs this activation booted from.
    pub(super) volume_encryption: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct RestoredSnapshotProvenance {
    pub(super) storage_uri: Option<String>,
    pub(super) local_artifact_id: Option<String>,
}

pub(super) fn mark_restore_storage_uri_present(
    details: SandboxEventDetails,
) -> SandboxEventDetails {
    details.bool_field(
        pplx_space_events::SandboxEventBoolField::RestoreStorageUriPresent,
        true,
    )
}

pub(super) fn emit_runtime_activated_event(event: RuntimeActivatedEvent<'_>) {
    let restore_provenance = classify_restore_provenance(
        event.boot_type,
        event.restored_snapshot_storage_uri.as_deref(),
        event.restored_snapshot_local_artifact_id.as_deref(),
        event.local_artifact_id.as_deref(),
    );
    let mut details = SandboxEventDetails::default()
        .boot_type(event.boot_type)
        .restore_provenance(restore_provenance)
        .pid(event.pid.to_string())
        .net_gateway_pid(event.net_gateway_pid.to_string())
        .sandbox_uid(event.sandbox_uid.to_string())
        .network_slot(event.network_slot.to_string())
        .guest_cid(event.guest_cid.to_string())
        .guest_ip(event.guest_ip)
        .guest_gateway(event.guest_gateway)
        .netns(event.namespace)
        .memory_mib(event.memory_mib.to_string())
        .execution_metadata(event.execution)
        .bool_field(
            pplx_space_events::SandboxEventBoolField::HasNetnsPath,
            event.has_netns_path,
        )
        .bool_field(
            pplx_space_events::SandboxEventBoolField::VolumeEncryption,
            event.volume_encryption,
        );
    if let Some(pid) = event.spaced_host_pid {
        details = details.spaced_host_pid(pid.to_string());
    }
    if let Some(vcpu) = event.vcpu {
        details = details.vcpu(vcpu.to_string());
    }
    if let Some(execution_generation) = event.execution_generation {
        details = details.execution_generation(execution_generation.to_string());
    }
    if event.restored_snapshot_storage_uri.is_some() {
        details = mark_restore_storage_uri_present(details);
    }
    if let Some(local_artifact_id) = event.restored_snapshot_local_artifact_id {
        details = details.restored_snapshot_local_artifact_id(local_artifact_id);
    }
    if let Some(local_artifact_id) = event.local_artifact_id {
        details = details.local_artifact_id(local_artifact_id);
    }
    emit_sandbox_event(
        event.sandbox_id,
        SandboxEventType::RuntimeActivated,
        SandboxEventSource::SbxManager,
        details,
    );
}

fn classify_restore_provenance(
    boot_type: &str,
    restored_snapshot_storage_uri: Option<&str>,
    restored_snapshot_local_artifact_id: Option<&str>,
    local_artifact_id: Option<&str>,
) -> SandboxRestoreProvenance {
    if boot_type == "fresh" {
        return SandboxRestoreProvenance::Fresh;
    }

    if restored_snapshot_storage_uri.is_none() {
        return if local_artifact_id.is_some() {
            SandboxRestoreProvenance::LocalOnly
        } else {
            SandboxRestoreProvenance::LocalUnknown
        };
    }

    match (restored_snapshot_local_artifact_id, local_artifact_id) {
        (Some(restored), Some(local)) if restored == local => {
            SandboxRestoreProvenance::RemoteMatchesLocal
        }
        (Some(_), Some(_)) => SandboxRestoreProvenance::RemoteLocalMismatch,
        (Some(_), None) => SandboxRestoreProvenance::RemoteOnly,
        (None, _) => SandboxRestoreProvenance::RemoteMetadataUnknown,
    }
}

pub(super) fn template_create_hash(
    template_dir: &str,
    content_hash_resolved: Option<&str>,
) -> Result<String, Status> {
    let template_rootfs = format!("{template_dir}/rootfs.ext4");
    if let Some(content_hash) = content_hash_resolved {
        Ok(content_hash.to_string())
    } else {
        compute_partial_sha256(Path::new(&template_rootfs))
            .map_err(|e| Status::internal(format!("compute template hash: {e}")))
    }
}

/// Create completion's node-local SQLite `sandboxes.state` write,
/// `creating → created`. Create holds no lock between its insert and this
/// write, so a row a Stop moved to `stopping` refuses it, and the Stop keeps
/// the row.
pub(super) fn write_create_completed(db: &crate::db::Connection, id: &str) -> Result<(), Status> {
    let edge = &crate::db::edges::CREATE_COMPLETE;
    require_state_write_from(
        crate::db::update_state_from(db, id, edge),
        "complete sandbox create",
    )
}

/// Base-snapshot create's node-local SQLite `sandboxes.state` write after its
/// own `created` write, `created → suspended`, refused like
/// [`write_create_completed`].
pub(super) fn write_base_snapshot_create_completed(
    db: &crate::db::Connection,
    id: &str,
) -> Result<(), Status> {
    let edge = &crate::db::edges::CREATE_BASE_SNAPSHOT_COMPLETE;
    require_state_write_from(
        crate::db::update_state_from(db, id, edge),
        "complete restored sandbox create",
    )
}

fn warm_start_refused(id: &str, found: SandboxState) -> Status {
    Status::failed_precondition(format!(
        "cannot start sandbox {id} from a warm snapshot while local state is {found}; \
         expected suspended, created or stopped"
    ))
}

/// Admit a warm-snapshot Start only of a row read in its node-local SQLite
/// `sandboxes.state` as a restage source or already `suspended`, before it
/// stages the base snapshot into the sandbox's suspend snapshot directory,
/// which restore treats as local suspend evidence. The restage write checks
/// again: a row can move while the snapshot is staged.
pub(super) fn admit_warm_start(sandbox: &crate::db::SandboxRow) -> Result<(), Status> {
    if sandbox.state == SandboxState::Suspended
        || crate::db::edges::WARM_START_RESTAGE
            .from()
            .contains(sandbox.state)
    {
        Ok(())
    } else {
        Err(warm_start_refused(&sandbox.id, sandbox.state))
    }
}

/// Warm-snapshot Start's node-local SQLite `sandboxes.state` write after it
/// staged the base snapshot, `created` or `stopped` → `suspended`, so the
/// checkpoint restore can claim the row. A row that moved to `suspended` in
/// the meantime needs no write. A row another transition moved anywhere else
/// is `FailedPrecondition`, as [`admit_warm_start`] answers it; a generic-guard
/// refusal or a missing row keeps `require_state_write_from`'s `Aborted`.
/// A guard refusal of a `suspended` row (`Guard { found: Suspended }`) stays
/// `Aborted`, unlike `FromState { Suspended }`: the guard means cleanup or
/// quarantine owns the row, so it is not a staged row the restore may claim.
/// A transient row answers `FailedPrecondition`, where a cold Start answers
/// `Unavailable`, deliberately: warm start does not wait out a transition, it
/// refuses a row it cannot restage.
pub(super) fn write_warm_start_restaged(
    db: &crate::db::Connection,
    id: &str,
) -> Result<(), Status> {
    const CONTEXT: &str = "prepare recovered sandbox restore";
    let outcome = crate::db::restage_for_warm_start(db, id)
        .map_err(|error| Status::internal(format!("{CONTEXT}: {error}")))?;
    match state_write_result(outcome) {
        Ok(_)
        | Err(StateWriteRejected::FromState {
            found: SandboxState::Suspended,
        }) => Ok(()),
        Err(StateWriteRejected::FromState { found }) => Err(warm_start_refused(id, found)),
        Err(rejected) => Err(rejected.into_status(CONTEXT)),
    }
}

#[cfg(test)]
mod state_write_tests;
#[cfg(test)]
mod tests;

pub(super) fn create_process_env(req: &CreateRequest) -> Result<BTreeMap<String, String>, Status> {
    let process_env: BTreeMap<String, String> = req.env.clone().into_iter().collect();
    Ok(sanitize_process_env(process_env, "sandbox_create"))
}

pub(super) fn sanitize_process_env(
    process_env: BTreeMap<String, String>,
    context: &'static str,
) -> BTreeMap<String, String> {
    let (env, issues) = EnvMap::sanitize(process_env);
    warn_env_sanitization_issues(context, &issues);
    env.into()
}

fn warn_env_sanitization_issues(context: &'static str, issues: &[EnvSanitizationIssue]) {
    if issues.is_empty() {
        return;
    }
    tracing::warn!(
        context = context,
        env_issue_count = issues.len(),
        env_issues = ?issues,
        "dropped invalid env entries"
    );
}

pub(super) fn validate_pool_member_create_options(
    pool_template: Option<&str>,
    snapshots_enabled: bool,
    volume_encryption: bool,
) -> Result<(), Status> {
    if pool_template.is_some() && snapshots_enabled {
        return Err(Status::invalid_argument(
            "pre-warm pool sandboxes cannot be created with snapshots_enabled=true",
        ));
    }
    // Pool members are unclaimed plaintext template clones; a running pool VM
    // cannot be encrypted in place at adoption, so encrypted sandboxes must
    // cold-create and never enter a pool.
    if pool_template.is_some() && volume_encryption {
        return Err(Status::invalid_argument(
            "pre-warm pool sandboxes cannot be created with volume_encryption=true",
        ));
    }
    Ok(())
}

pub(super) fn requested_create_reuse_state(state: SandboxState) -> bool {
    matches!(
        state,
        SandboxState::Created
            | SandboxState::Started
            | SandboxState::Paused
            | SandboxState::Suspended
            | SandboxState::Stopped
    )
}

/// Compare the immutable identity of an existing sandbox row against a
/// requested-id create, returning the names of the fields that differ.
///
/// Only fields that are fixed for the lifetime of the sandbox belong here.
/// A requested-id create that finds an existing row must converge: the
/// kubelet retries `FailedCreatePodSandBox` forever, so rejecting on a
/// field whose row value can legitimately drift from the request value
/// turns a routine pod recreate or same-node restore into a permanent
/// `AlreadyExists` loop. Runtime-mutable fields are therefore excluded,
/// and on reuse the row's value wins (the reuse path never applies the
/// request's value for them):
///
/// * `process_env` — warm-pool adoption and the `Update` RPC rewrite the
///   row's env after create, while a restore pod re-derives its
///   `CreateRequest` env from the control-plane spec plus the pod's OCI
///   process env; the representations permanently diverge.
/// * `snapshots_enabled` — the `EnableSnapshots`/`DisableSnapshots` RPCs
///   and the snapshot scheduler's auto-disable rewrite the row after
///   create, while the Kubernetes shim never sets
///   `CreateRequest.snapshots_enabled`, so every pod-recreate create
///   carries the proto default.
pub(super) fn requested_sandbox_create_mismatches(
    existing: &crate::db::SandboxRow,
    requested: &RequestedSandboxCreateSpec<'_>,
) -> Vec<&'static str> {
    let mut mismatches = Vec::new();
    if existing.template != requested.template {
        mismatches.push("template");
    }
    if existing.template_hash.as_deref() != Some(requested.template_hash) {
        mismatches.push("template_hash");
    }
    if existing.user != *requested.user {
        mismatches.push("user");
    }
    if existing.runtime != requested.runtime {
        mismatches.push("runtime");
    }
    if existing.rootfs_size_mb != requested.rootfs_size_mb {
        mismatches.push("rootfs_size_mb");
    }
    if existing.pool_template.as_deref() != requested.pool_template {
        mismatches.push("pool_template");
    }
    if existing.volume_encryption != requested.volume_encryption {
        mismatches.push("volume_encryption");
    }
    // Saved bindings can move to a newly selected loader after a failed
    // restore, so this is the one non-fixed field compared: an explicit
    // selection never falls back, and reuse must not acknowledge another one.
    if let Some(environment) = requested.environment
        && existing
            .execution_metadata
            .as_ref()
            .and_then(|execution| execution.environment())
            != Some(environment)
    {
        mismatches.push("environment");
    }
    mismatches
}

pub(super) fn requested_sandbox_create_auto_lifecycle_changed(
    existing: &crate::db::SandboxRow,
    requested: &RequestedSandboxCreateSpec<'_>,
) -> bool {
    existing.auto_lifecycle != *requested.auto_lifecycle
}

pub(super) fn update_existing_sandbox_create_auto_lifecycle(
    conn: &crate::db::Connection,
    sandbox_id: &str,
    state: SandboxState,
    policy: &AutoLifecyclePolicy,
) -> Result<(), Status> {
    let mut runtime_state = if state == SandboxState::Started {
        initial_runtime_state(policy, now_secs())
    } else {
        AutoLifecycleRuntimeState::default()
    };
    recompute_runtime_state(&mut runtime_state, policy);

    let transaction = crate::db::Transaction::deferred(conn)
        .map_err(|e| Status::internal(format!("db transaction failed: {e}")))?;
    crate::db::update_auto_lifecycle_with_state(
        &transaction,
        sandbox_id,
        policy,
        &runtime_state,
        crate::db::PolicyAuthority::Cluster,
    )
    .map_err(|e| Status::internal(format!("db auto_lifecycle update failed: {e}")))?;
    transaction
        .commit()
        .map_err(|e| Status::internal(format!("db transaction commit failed: {e}")))?;
    Ok(())
}

/// Static per-netns /30 point-to-point addressing baked into every sandbox's
/// kernel cmdline (`ip=169.254.0.21::169.254.0.22:...` in `recovery.rs`). The
/// guest always sees these, so response builders that describe an existing
/// runtime reuse them rather than re-deriving per sandbox.
pub(super) const GUEST_IP: &str = pplx_space_common_core::guest::linux::network::IPV4_ADDR;
pub(super) const GUEST_GATEWAY: &str =
    pplx_space_common_core::guest::linux::network::GATEWAY_IPV4_ADDR;

pub(super) fn create_response_for_existing_sandbox(
    existing: &crate::db::SandboxRow,
    effective_mode: BootMode,
) -> CreateResponse {
    let has_process = existing.pid.is_some();
    CreateResponse {
        environment: None,
        sandbox_id: existing.id.clone(),
        pid: existing.pid,
        guest_ip: has_process.then(|| GUEST_IP.to_string()),
        namespace: has_process.then(|| format!("sns-{}", existing.uuid8)),
        log_path: has_process.then(|| format!("{RUN_DIR}/{}/fc.log", existing.id)),
        vsock_uds_path: has_process
            .then(|| existing.vsock_uds_path.clone())
            .flatten(),
        boot_mode_used: effective_mode.into(),
        reused_existing: true,
        route_identity: has_process
            .then(|| space_api_route_identity_to_wire(&existing.space_api_route_identity))
            .flatten(),
    }
}

/// Checkpoint name used for the suspend → restore snapshot. Auto-suspend
/// writes this checkpoint and the restore path keys its idempotent-reattach
/// behavior on it.
pub(super) const SUSPEND_CHECKPOINT: &str = "suspend";

#[derive(Debug)]
pub(super) enum CheckpointRestoreRetryAction {
    Reattach(Box<StartResponse>),
    ResumeRestoring,
    CleanupRestoring,
    Reject,
}

fn checkpoint_start_response_for_live_runtime(
    existing: &crate::db::SandboxRow,
    runtime_live: bool,
) -> Option<StartResponse> {
    if !runtime_live {
        return None;
    }

    if existing.uid.is_none()
        || existing.slot.is_none()
        || existing.guest_cid.is_none()
        || (existing.guest_image.expects_agent() && existing.spaced_host_pid.is_none())
        || existing.netgtw_pid.is_none()
        || existing.api_socket_path.is_none()
    {
        return None;
    }
    let (Some(pid), Some(vsock_uds_path)) = (existing.pid, existing.vsock_uds_path.clone()) else {
        return None;
    };

    Some(StartResponse {
        sandbox_id: existing.id.clone(),
        pid,
        guest_ip: GUEST_IP.to_string(),
        guest_gateway: GUEST_GATEWAY.to_string(),
        namespace: format!("sns-{}", existing.uuid8),
        log_path: format!("{RUN_DIR}/{}/fc.log", existing.id),
        vsock_uds_path,
        route_identity: space_api_route_identity_to_wire(&existing.space_api_route_identity),
    })
}

pub(super) fn checkpoint_restore_retry_action(
    existing: &crate::db::SandboxRow,
    target_state: SandboxState,
    runtime_live: bool,
) -> CheckpointRestoreRetryAction {
    match existing.state {
        SandboxState::Started if target_state == SandboxState::Started => {
            checkpoint_start_response_for_live_runtime(existing, runtime_live)
                .map(Box::new)
                .map(CheckpointRestoreRetryAction::Reattach)
                .unwrap_or(CheckpointRestoreRetryAction::Reject)
        }
        // A cross-node restore writes the Restoring row before pulling its
        // durable-backed artifacts. If the request is cancelled, a later wake
        // can safely reattach while no runtime, artifact pin, or cleanup intent
        // was committed. The current request supplies the desired target state
        // after staging returns the row to Suspended.
        SandboxState::Restoring if !runtime_live && existing.is_retryable_interrupted_restore() => {
            CheckpointRestoreRetryAction::ResumeRestoring
        }
        SandboxState::Restoring => CheckpointRestoreRetryAction::CleanupRestoring,
        _ => CheckpointRestoreRetryAction::Reject,
    }
}

/// Build the `StartResponse` for an idempotent suspend-checkpoint restore that
/// finds the sandbox already `Started` with a live runtime on this node.
///
/// The ECW access-idle restore path can issue the same `Start(checkpoint =
/// "suspend")` more than once: the first attempt launches Firecracker and flips
/// the local row to `Started`, then the container restore pod create retries
/// the same checkpoint start. Without this reattach, the retry hits the
/// `expected Suspended` precondition and loops until the API readiness probe
/// times out (surfacing as a `guest_not_ready` readiness-timeout 504).
/// Returning the live runtime's attachment makes the retry idempotent instead.
///
/// Fails closed (`None`) unless the row is `Started`, the request targets
/// `Started`, the runtime is live, and every runtime field needed to describe
/// the attachment is present — a stale DB-only `Started` row still rejects.
pub(super) fn checkpoint_start_response_for_existing_started_sandbox(
    existing: &crate::db::SandboxRow,
    target_state: SandboxState,
    runtime_live: bool,
) -> Option<StartResponse> {
    match checkpoint_restore_retry_action(existing, target_state, runtime_live) {
        CheckpointRestoreRetryAction::Reattach(response) => Some(*response),
        CheckpointRestoreRetryAction::ResumeRestoring
        | CheckpointRestoreRetryAction::CleanupRestoring
        | CheckpointRestoreRetryAction::Reject => None,
    }
}

pub(crate) fn template_lookup_scope(
    owner: &SandboxOwner,
    sandbox_id: Option<&str>,
) -> Result<TemplateLookupScope, Status> {
    if matches!(owner.kind(), SandboxOwnerKind::PlatformUser(_)) {
        let Some(sandbox_id) = sandbox_id else {
            return Ok(TemplateLookupScope::Cluster);
        };
        let id = sandbox_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid template sandbox identity"))?;
        Ok(TemplateLookupScope::Sandbox {
            id,
            caller_node: None,
        })
    } else {
        Ok(TemplateLookupScope::LocalExecution)
    }
}

pub(super) async fn resolve_template_paths(
    templates: &dyn crate::template_resolver::TemplateResolver,
    template_ref: pplx_space_template_mgr_client::proto::TemplateRef,
    scope: TemplateLookupScope,
) -> Result<ResolvedTemplatePaths, tonic::Status> {
    use crate::template_resolver::TemplateLookupError;
    let mut request =
        tonic::Request::new(pplx_space_template_mgr_client::proto::GetTemplateRequest {
            caller_org_id: None,
            template: Some(template_ref),
            vcpu: None,
            memory_mib: None,
        });
    scope.attach(&mut request);
    let response = templates
        .get_template(request)
        .await
        .map_err(|error| match error {
            TemplateLookupError::Connect(e) => {
                tonic::Status::internal(format!("cannot connect to template-manager: {e}"))
            }
            TemplateLookupError::Rpc(e) => {
                tonic::Status::new(e.code(), format!("GetTemplate failed: {e}"))
            }
        })?;
    scope.observe_response(&response)?;
    let inner = response.into_inner();
    let tmpl = inner.template.unwrap_or_default();
    let arch = template_metadata_arch(&inner.template_dir).unwrap_or(inner.arch);
    let spaced_compatibility = template_metadata_spaced_compatibility(&inner.template_dir);
    let runtime_defaults = template_runtime_defaults(&inner.template_dir);
    let execution = template_execution_metadata(&inner.template_dir, &arch)?;
    let guest_image = if inner.manifest_json.is_empty() {
        Default::default()
    } else {
        let manifest: pplx_space_common_core::types::TemplateManifest =
            serde_json::from_str(&inner.manifest_json).map_err(|error| {
                Status::failed_precondition(format!("invalid template manifest: {error}"))
            })?;
        manifest
            .execution_metadata()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        manifest.guest_image.unwrap_or_default()
    };
    Ok(ResolvedTemplatePaths {
        guest_image,
        template_dir: inner.template_dir,
        snapshot_dir: inner.snapshot_dir,
        snapshot_vcpu: inner
            .snapshot_vcpu
            .map(pplx_space_common_core::types::VcpuCount::try_from_u32)
            .transpose()
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "invalid template snapshot CPU allocation: {error}"
                ))
            })?,
        content_hash: tmpl.content_hash,
        default_auto_lifecycle: inner
            .default_auto_lifecycle
            .as_ref()
            .map(policy_from_proto)
            .transpose()?
            .unwrap_or_else(AutoLifecyclePolicy::disabled),
        default_network_policy_authority: crate::db::PolicyAuthority::try_from(
            inner.network_policy_authority,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?,
        default_auto_lifecycle_authority: crate::db::PolicyAuthority::try_from(
            inner.auto_lifecycle_authority,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?,
        arch,
        spaced_version: spaced_compatibility.version,
        spaced_compatibility_family: spaced_compatibility.compatibility_family,
        runtime_defaults,
        execution,
    })
}

/// The producer a template base snapshot records. Snapshots predating the
/// record were captured under the initial environment.
pub(super) fn read_base_snapshot_execution_metadata(
    source_dir: &std::path::Path,
) -> Result<pplx_space_common_core::types::SnapshotExecutionMetadata, Status> {
    use pplx_space_common_core::types::{ExecutionMetadata, SnapshotExecutionMetadata};

    let metadata = match std::fs::read(source_dir.join("metadata.json")) {
        Ok(bytes) => {
            serde_json::from_slice::<SnapshotExecutionMetadata>(&bytes).map_err(|error| {
                Status::failed_precondition(format!(
                    "invalid base snapshot runtime metadata: {error}"
                ))
            })?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let architecture = std::env::consts::ARCH.parse().map_err(
                |error: pplx_space_common_core::types::ArchParseError| {
                    Status::failed_precondition(error.to_string())
                },
            )?;
            SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
                architecture,
                vec![],
            ))
            .map_err(|error| Status::failed_precondition(error.to_string()))?
        }
        Err(error) => {
            return Err(Status::internal(format!(
                "read base snapshot runtime metadata: {error}"
            )));
        }
    };
    Ok(metadata)
}

/// Stage the exact base snapshot's runtime and allocation evidence together.
/// Older metadata lacks CPU allocation; the selected snapshot dimensions are
/// authoritative for that artifact, unlike mutable template boot defaults.
pub(super) fn stage_base_snapshot_runtime_metadata(
    source_dir: &std::path::Path,
    destination_dir: &std::path::Path,
    snapshot_vcpu: Option<pplx_space_common_core::types::VcpuCount>,
) -> Result<(), Status> {
    let metadata = read_base_snapshot_execution_metadata(source_dir)?;
    // Captured allocation wins; selected dimensions fill only legacy absence.
    let cpu_budget = metadata.cpu_budget().or(snapshot_vcpu);
    let bytes =
        serde_json::to_vec_pretty(&metadata.with_cpu_budget(cpu_budget)).map_err(|error| {
            Status::internal(format!("serialize base snapshot runtime metadata: {error}"))
        })?;
    crate::snapshot_scheduler::write_file_atomic(destination_dir.join("runtime.json"), &bytes)
        .map_err(|error| {
            Status::internal(format!(
                "write sandbox checkpoint runtime metadata: {error}"
            ))
        })
}

pub(super) async fn resolve_template_paths_for_start(
    templates: &dyn crate::template_resolver::TemplateResolver,
    template_ref: pplx_space_template_mgr_client::proto::TemplateRef,
    sandbox: &crate::db::SandboxRow,
) -> Result<ResolvedTemplatePaths, Status> {
    // Start restores an existing sandbox's identity. A name-only lookup could
    // silently pick a new image after an alias moves; only create may do that.
    if template_ref
        .content_hash
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Err(Status::failed_precondition(
            "cannot recover sandbox artifact without its saved template hash",
        ));
    }
    resolve_template_paths(
        templates,
        template_ref,
        template_lookup_scope(&sandbox.user, Some(&sandbox.id))?,
    )
    .await
    .map_err(start_template_resolution_failed_status)
}

fn start_template_resolution_failed_status(error: Status) -> Status {
    // A missing build cannot become available by retrying Start for the same
    // sandbox. Keep genuine service failures retryable, but surface this
    // permanent condition to the shim and orchestrator as terminal.
    let code = if error.code() == tonic::Code::NotFound {
        tonic::Code::FailedPrecondition
    } else {
        error.code()
    };
    Status::new(code, error.message().to_string())
}

pub(super) async fn template_runtime_defaults_for_sandbox_start(
    templates: &dyn crate::template_resolver::TemplateResolver,
    sandbox: &crate::db::SandboxRow,
    prepared_template_dir: Option<&str>,
) -> TemplateRuntimeDefaults {
    if let Some(template_dir) = prepared_template_dir {
        return template_runtime_defaults(template_dir);
    }

    match resolve_template_paths(
        templates,
        pplx_space_template_mgr_client::proto::TemplateRef {
            name: sandbox.template.clone(),
            content_hash: sandbox.template_hash.clone(),
            tag: None,
        },
        match template_lookup_scope(&sandbox.user, Some(&sandbox.id)) {
            Ok(scope) => scope,
            Err(error) => {
                tracing::warn!(%error, "invalid sandbox template scope");
                return TemplateRuntimeDefaults::default();
            }
        },
    )
    .await
    {
        Ok(resolved) => resolved.runtime_defaults,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                template = %sandbox.template,
                template_hash = ?sandbox.template_hash,
                error = %error,
                "failed to resolve template runtime defaults; using service fallbacks"
            );
            TemplateRuntimeDefaults::default()
        }
    }
}

pub(super) fn sandbox_rootfs_exists_in(
    cache_dir: &std::path::Path,
    sandbox_id: &str,
) -> std::io::Result<bool> {
    cache_dir
        .join(sandbox_id)
        .join("instance")
        .join("rootfs.ext4")
        .try_exists()
}

/// Older node-managers left instance/ private to the VM owner. Ask the
/// privileged publisher to repair traversal before anything inspects the
/// retained rootfs.
pub(super) async fn repair_retained_instance_traversal(
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    cache_dir: &std::path::Path,
    sandbox_id: &str,
) -> Result<(), Status> {
    if sandbox_rootfs_exists_in(cache_dir, sandbox_id)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
    {
        connect_node(node_manager)
            .await
            .map_err(|error| Status::unavailable(format!("repair retained instance: {error}")))?
            .prepare_sandbox_artifact_dir(
                pplx_space_node_mgr_client::proto::PrepareSandboxArtifactDirRequest {
                    sandbox_id: sandbox_id.to_string(),
                },
            )
            .await?;
    }
    Ok(())
}

pub(super) async fn template_source_for_sandbox_prepare(
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    templates: &dyn crate::template_resolver::TemplateResolver,
    sandbox: &crate::db::SandboxRow,
) -> Result<SandboxPrepareSource, Status> {
    template_source_for_sandbox_prepare_in(
        node_manager,
        templates,
        sandbox,
        std::path::Path::new(CACHE_DIR),
    )
    .await
}

pub(super) async fn template_source_for_sandbox_prepare_in(
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
    templates: &dyn crate::template_resolver::TemplateResolver,
    sandbox: &crate::db::SandboxRow,
    cache_dir: &std::path::Path,
) -> Result<SandboxPrepareSource, Status> {
    repair_retained_instance_traversal(node_manager, cache_dir, &sandbox.id).await?;
    if sandbox_rootfs_exists_in(cache_dir, &sandbox.id).map_err(|error| {
        Status::failed_precondition(format!(
            "inspect retained rootfs for {}: {error}",
            sandbox.id
        ))
    })? {
        let execution = super::tail_support::sandbox_execution_metadata_in(cache_dir, &sandbox.id)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        return Ok(SandboxPrepareSource {
            template: None,
            template_dir: None,
            execution: bind_saved_execution(sandbox, execution)?,
            existing_artifact: true,
        });
    }

    if sandbox.template == ORPHANED_SANDBOX_TEMPLATE
        || sandbox.user.kind() == SandboxOwnerKind::Orphaned
    {
        return Err(Status::failed_precondition(format!(
            "cannot start orphaned sandbox {}: no rootfs exists at {}/{}/instance/rootfs.ext4",
            sandbox.id, CACHE_DIR, sandbox.id
        )));
    }

    let resolved_template = resolve_template_paths_for_start(
        templates,
        pplx_space_template_mgr_client::proto::TemplateRef {
            name: sandbox.template.clone(),
            content_hash: sandbox.template_hash.clone(),
            tag: None,
        },
        sandbox,
    )
    .await?;
    reject_incompatible_template_arch(
        &sandbox.template,
        resolved_template
            .content_hash
            .as_deref()
            .unwrap_or("<unhashed>"),
        &resolved_template.arch,
    )?;
    let image = resolved_template.guest_image;
    if image != sandbox.guest_image {
        return Err(Status::failed_precondition(
            "template guest contract differs from persisted sandbox",
        ));
    }
    if sandbox.guest_image.expects_agent() {
        reject_incompatible_template_spaced_compatibility(
            &sandbox.template,
            resolved_template
                .content_hash
                .as_deref()
                .unwrap_or("<unhashed>"),
            resolved_template.spaced_version.as_deref(),
            resolved_template.spaced_compatibility_family.as_deref(),
        )?;
    }

    Ok(SandboxPrepareSource {
        template: Some(sandbox.template.clone()),
        template_dir: Some(resolved_template.template_dir),
        execution: bind_disk_execution(
            sandbox.execution_metadata.as_ref(),
            resolved_template.execution,
        )?,
        existing_artifact: false,
    })
}

/// The saved sandbox binding owns execution selection. Disk metadata records
/// the runtime that last prepared the disk; it is not a second selection pin.
/// Raw disks can cross qualified consumers while their guest and kernel inputs
/// remain fixed. Snapshot producer evidence is read separately from its manifest.
fn bind_saved_execution(
    sandbox: &crate::db::SandboxRow,
    observed: pplx_space_common_core::types::ExecutionMetadata,
) -> Result<pplx_space_common_core::types::ExecutionMetadata, Status> {
    bind_disk_execution(sandbox.execution_metadata.as_ref(), observed)
}

pub(super) fn bind_disk_execution(
    saved: Option<&pplx_space_common_core::types::ExecutionMetadata>,
    observed: pplx_space_common_core::types::ExecutionMetadata,
) -> Result<pplx_space_common_core::types::ExecutionMetadata, Status> {
    let Some(saved) = saved.filter(|saved| saved.environment().is_some()) else {
        return Ok(observed);
    };
    // Only the Firecracker adapter currently declares cross-consumer raw-disk
    // compatibility. Other adapters must supply their own disk contract.
    if saved.runtime().kind != pplx_space_common_core::types::RuntimeKind::Firecracker
        && saved != &observed
    {
        return Err(Status::failed_precondition(
            "runtime adapter requires the exact disk execution contract",
        ));
    }
    if saved.runtime().kind != observed.runtime().kind
        || saved.guest() != observed.guest()
        || !matching_execution_components(saved, &observed)
    {
        return Err(Status::failed_precondition(
            "disk boot inputs differ from the sandbox's saved execution",
        ));
    }
    Ok(saved.clone())
}

fn matching_execution_components(
    saved: &pplx_space_common_core::types::ExecutionMetadata,
    observed: &pplx_space_common_core::types::ExecutionMetadata,
) -> bool {
    observed.components().len() == saved.components().len()
        && saved.components().iter().all(|expected| {
            observed.components().iter().any(|actual| {
                expected.kind == actual.kind
                    && expected.role == actual.role
                    && match expected.digest {
                        Some(digest) => actual.digest == Some(digest),
                        None => {
                            actual.digest.is_none()
                                && actual.version == expected.version
                                && actual.format == expected.format
                        }
                    }
            })
        })
}

pub(super) fn guest_console_logs_enabled() -> bool {
    std::env::var("SPACE_ENABLE_GUEST_CONSOLE_LOGS")
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

/// Reject path traversal in any field that gets interpolated into a filesystem path.
pub(super) fn validate_path_component(field_name: &str, value: &str) -> Result<(), Status> {
    if value.contains("..") || value.contains('/') || value.contains('\\') {
        return Err(Status::invalid_argument(format!(
            "invalid {field_name}: contains path traversal characters"
        )));
    }
    Ok(())
}

pub(super) fn validate_snapshot_name(snapshot_name: &str) -> Result<(), Status> {
    use std::path::{Component, Path};

    if snapshot_name.is_empty() {
        return Err(Status::invalid_argument("snapshot_name must not be empty"));
    }
    if snapshot_name.contains('/') || snapshot_name.contains('\\') {
        return Err(Status::invalid_argument(format!(
            "invalid snapshot_name: {snapshot_name}"
        )));
    }

    let mut components = Path::new(snapshot_name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(Status::invalid_argument(format!(
            "invalid snapshot_name: {snapshot_name}"
        ))),
    }
}

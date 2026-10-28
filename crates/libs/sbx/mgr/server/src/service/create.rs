use super::activation_support::{
    ActivationFailureStage, IdentityFreeDirectActivationAuthority, complete_restore_and_mark_final,
    fresh_runtime_activation_identity, runtime_activation_wire_identity_can_open_claim,
    space_api_route_identity_for_runtime_activation, space_api_route_identity_from_create_request,
    space_api_route_identity_to_wire, spawn_restore_state_transition,
};
use super::cache_cleanup::ensure_sandbox_cache_root_in;
use super::cleanup_support::{ActivationCleanupSpec, CleanupCompletion, read_node_execution_fence};
use super::create_support::*;
use super::network_runtime::*;
use super::spaced_bridge::*;
use super::start_support::{
    ActivationAdmissionKind, START_NODE_METADATA_TIMEOUT, START_TEMPLATE_RESOLUTION_TIMEOUT,
    persist_allocated_runtime_label, start_stage, start_stage_timeout,
};
use super::tail_support::*;
use super::*;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};
use pplx_space_metrics::sandbox::admission::AdmissionPermitOutcome;

const MIB_BYTES: u64 = 1024 * 1024;

pub(super) struct StartFromCheckpointLaunch<'a> {
    pub(super) vcpu: Option<u32>,
    pub(super) activation_auto_lifecycle: Option<&'a AutoLifecyclePolicy>,
    pub(super) operation_activation_attempt_id:
        Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    pub(super) sandbox: &'a crate::db::SandboxRow,
    pub(super) blocking: bool,
    pub(super) netns_path: Option<String>,
    pub(super) target_state: SandboxState,
    pub(super) execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    pub(super) space_api_route_identity: pplx_space_common_core::types::SpaceApiRouteIdentity,
    pub(super) route_identity_provenance:
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    pub(super) open_activation_claim_eligible: bool,
    pub(super) restored_snapshot: RestoredSnapshotProvenance,
}

/// BYOK restore: rebuild the encrypted rootfs from the suspended `cow.luks` and
/// open the encrypted snapshot volume from the suspended `snapshot.luks`.
/// Returns `(rootfs_block_device_path, snapshot_volume_mount_dir)` to thread
/// into the launch + load-snapshot RPCs. The caller gates on the sandbox's
/// immutable `volume_encryption` flag and only calls this for encrypted
/// sandboxes.
///
/// Decide, per sealed artifact, whether node-mgr must stage a downloaded copy
/// to the canonical `instance/` path or open the kept canonical backing in
/// place. The signal is the presence of the artifact in `snapshot_dir`, which
/// holds the current suspend's sealed output in both cases: a cross-node
/// restore's snapshot pull downloads `cow.luks`/`snapshot.luks` there, and a
/// same-node suspend publishes the very same files there. When present,
/// node-mgr stages it (atomically) to the canonical `instance/` path before
/// opening — so a later re-suspend seals and stages that canonical file.
///
/// Keying on the download's presence rather than on `instance/cow.luks`
/// existence is deliberate: an interrupted first restore attempt (cleanup runs
/// with `keep_filesystem`) can leave a *partial or single-artifact* canonical
/// state. Trusting that would make a retry open a corrupt backing, or take a
/// "same-node" path for one artifact while the other's canonical file is
/// absent. Re-staging from the intact `snapshot_dir` download on every attempt
/// avoids both. When the download is absent (a genuine same-node restore that
/// kept `instance/` in place and whose publication dir was already reclaimed),
/// `None` opens the canonical backing directly.
async fn reconstruct_encrypted_restore_devices(
    sandbox_id: &str,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    snapshot_dir: &str,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<(String, String), Status> {
    let restore_source_if_present = |file: &str| -> Option<String> {
        let path = format!("{snapshot_dir}/{file}");
        std::path::Path::new(&path).exists().then_some(path)
    };
    let cow_source = restore_source_if_present("cow.luks");
    let snapshot_restore_source = restore_source_if_present("snapshot.luks");

    let rootfs_block_device_path = crate::rootfs_encryption::prepare_encrypted_rootfs(
        sandbox_id,
        execution_fence,
        cow_source.as_deref(),
        node_manager,
    )
    .await
    .map_err(encrypted_restore_rootfs_error_status)?;
    let mount_dir = crate::snapshot_encryption::prepare_encrypted_snapshot_volume(
        sandbox_id,
        Some(execution_fence.into()),
        false,
        snapshot_restore_source.as_deref(),
        node_manager,
    )
    .await
    .map_err(encrypted_restore_snapshot_error_status)?
    .mount_dir;
    Ok((rootfs_block_device_path, mount_dir))
}

pub(super) fn encrypted_restore_rootfs_error_status(
    error: crate::rootfs_encryption::EncryptedRootfsPrepareError,
) -> Status {
    let reason = format!("encrypted restore reconstruction failed: {error}");
    error.into_status(reason)
}

pub(super) fn encrypted_restore_snapshot_error_status(
    error: crate::snapshot_encryption::EncryptedSnapshotPrepareError,
) -> Status {
    let reason = format!("encrypted restore reconstruction failed: {error}");
    error.into_status(reason)
}

fn memory_file_mib(path: &std::path::Path) -> Result<Option<u64>, Status> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Status::internal(format!(
                "stat snapshot memory file {}: {error}",
                path.display()
            )));
        }
    };
    let bytes = metadata.len();
    if bytes == 0 {
        return Err(Status::failed_precondition(format!(
            "{} is empty",
            path.display()
        )));
    }
    Ok(Some(bytes.div_ceil(MIB_BYTES)))
}

fn snapshot_memory_file_mib(snapshot_dir: &str) -> Result<Option<u64>, Status> {
    memory_file_mib(&std::path::Path::new(snapshot_dir).join("memory"))
}

fn restored_guest_memory_mib(
    captured: Option<pplx_space_common_core::types::MemorySize>,
    file_mib: Option<u64>,
) -> Result<Option<u64>, Status> {
    let captured = captured.map(|size| size.as_mib_ceil());
    if let (Some(captured), Some(file)) = (captured, file_mib)
        && captured != file
    {
        return Err(Status::failed_precondition(
            "snapshot memory differs from captured guest allocation",
        ));
    }
    Ok(captured.or(file_mib))
}

fn base_snapshot_memory_file_mib(snapshot_dir: &str) -> Result<Option<u64>, Status> {
    memory_file_mib(&std::path::Path::new(snapshot_dir).join("snapshot.base"))
}

fn ensure_snapshot_memory_allowed(
    snapshot_memory_mib: Option<u64>,
    guest_memory_max_mib: u32,
) -> Result<Option<u32>, Status> {
    let Some(snapshot_memory_mib) = snapshot_memory_mib else {
        return Ok(None);
    };
    if snapshot_memory_mib > u64::from(guest_memory_max_mib) {
        return Err(Status::failed_precondition(format!(
            "snapshot requires {snapshot_memory_mib} MiB guest memory, but node policy allows {guest_memory_max_mib} MiB"
        )));
    }
    let snapshot_memory_mib = u32::try_from(snapshot_memory_mib).map_err(|_| {
        Status::failed_precondition(format!(
            "snapshot guest memory exceeds u32: {snapshot_memory_mib} MiB"
        ))
    })?;
    Ok(Some(snapshot_memory_mib))
}

/// An activation may carry an allocation explicitly; otherwise retain the
/// captured allocation. Legacy snapshots without either source stay unknown.
pub(super) fn restore_cpu_budget(
    requested: Option<u32>,
    captured: Option<pplx_space_common_core::types::VcpuCount>,
) -> Result<Option<pplx_space_common_core::types::VcpuCount>, Status> {
    requested
        .map(pplx_space_common_core::types::VcpuCount::try_from_u32)
        .transpose()
        .map(|requested| requested.or(captured))
        .map_err(|error| {
            Status::invalid_argument(format!("invalid restore CPU allocation: {error}"))
        })
}

impl SandboxManagerService {
    pub(super) fn ensure_snapshot_restore_supported(
        &self,
        operation: &'static str,
    ) -> Result<(), Status> {
        self.config.validate_snapshot_restore().map_err(|message| {
            tracing::error!(
                operation,
                hugepages = self.config.hugepages,
                "unsupported snapshot restore configuration: {message}"
            );
            Status::failed_precondition(message)
        })
    }

    /// Execution evidence is committed before a VM is launched. Legacy rows
    /// without it predate selected environments; filesystem cache loss cannot
    /// erase a candidate's durable binding or turn it into legacy execution.
    pub(super) fn ensure_memory_state_supported(
        &self,
        sandbox: &crate::db::SandboxRow,
        operation: &'static str,
    ) -> Result<(), Status> {
        if sandbox
            .execution_metadata
            .as_ref()
            .is_some_and(super::validation::requires_cold_creation)
        {
            return Err(Status::failed_precondition(format!(
                "cannot {operation}: execution environment does not support memory-state operations"
            )));
        }
        Ok(())
    }

    pub(super) fn ensure_not_pool_sandbox_for_snapshots(
        &self,
        sandbox: &crate::db::SandboxRow,
        operation: &'static str,
    ) -> Result<(), Status> {
        let Some(pool_template) = sandbox.pool_template.as_deref() else {
            return Ok(());
        };

        tracing::warn!(
            operation,
            sandbox_id = %sandbox.id,
            pool_template,
            "rejecting snapshot operation on pre-warm pool sandbox"
        );
        Err(Status::failed_precondition(format!(
            "cannot {operation} for pre-warm pool sandbox {} (template '{}'); adopt it from the pool first",
            sandbox.id, pool_template
        )))
    }

    /// A plaintext-flagged sandbox with committed COW state cannot be
    /// captured: every plaintext artifact set (full rootfs reflink,
    /// btrfs-send delta, checkpoint export) reads the origin `rootfs.ext4`,
    /// which dm-snapshot never writes — the guest's writes live only in its
    /// COW. That COW is `cow.plain` for an unclaimed or non-BYOK-claimed
    /// member, and `cow.luks` for a member whose claim-time re-home committed
    /// before the row's `volume_encryption` stamp landed. Capturing anyway
    /// would publish a snapshot that silently rolls the guest back to the
    /// template, and a restore would then detach guest memory from its own
    /// disk state (in-guest filesystem corruption on top of the data loss).
    /// Encrypted-flagged sandboxes capture `cow.luks` explicitly, which is
    /// why `volume_encryption` exempts them.
    ///
    /// Fail-closed consequence: a member claimed by a non-BYOK org keeps its
    /// plaintext COW and stays running (never suspendable) until the claim
    /// path re-homes or collapses the COW.
    pub(super) fn ensure_no_plaintext_pool_cow_for_snapshots(
        &self,
        sandbox: &crate::db::SandboxRow,
        operation: &'static str,
    ) -> Result<(), Status> {
        if sandbox.volume_encryption {
            return Ok(());
        }
        if !crate::rootfs_encryption::committed_cow_state_exists(&sandbox.id) {
            return Ok(());
        }
        tracing::warn!(
            operation,
            sandbox_id = %sandbox.id,
            "rejecting snapshot operation on a plaintext-flagged sandbox with committed COW state"
        );
        Err(Status::failed_precondition(format!(
            "cannot {operation} for sandbox {}: its writes live in a COW that no plaintext \
             snapshot format captures; the claim must re-home or collapse the COW (or stamp \
             volume_encryption) first",
            sandbox.id
        )))
    }

    pub(super) fn lookup_requested_sandbox(
        &self,
        requested_id: Uuid,
    ) -> Result<Option<crate::db::SandboxRow>, Status> {
        let requested_id_str = requested_id.to_string();
        let db = self.db_conn()?;
        crate::db::read::sandbox(&db, &requested_id_str)
            .map_err(|e| Status::internal(format!("db lookup failed: {e}")))
    }

    pub(super) async fn create_with_id(
        &self,
        args: CreateWithIdArgs<'_>,
    ) -> Result<Response<CreateResponse>, Status> {
        let CreateWithIdArgs {
            req,
            sandbox_owner,
            effective_mode,
            id,
            template_dir,
            execution,
            snapshot_dir_resolved,
            snapshot_vcpu,
            content_hash_resolved,
            auto_lifecycle,
            auto_lifecycle_authority,
            default_network_policy_authority,
        } = args;
        let template = &req.template;
        let runtime = &req.runtime;
        let sandbox_id = SandboxId::from_str(&id)
            .map_err(|e| Status::invalid_argument(format!("invalid sandbox id: {e}")))?;
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
        if effective_mode == BootMode::BaseSnapshot {
            self.ensure_snapshot_restore_supported("create_with_base_snapshot")?;
        }

        let template_hash = template_create_hash(template_dir, content_hash_resolved)?;

        let pool_template = req.pool_template.as_deref().filter(|s| !s.is_empty());
        validate_pool_member_create_options(
            pool_template,
            req.snapshots_enabled,
            req.volume_encryption,
        )?;
        let process_env = create_process_env(req)?;
        let requested_create_route_identity = space_api_route_identity_from_create_request(req)?;
        super::activation_support::validate_route_identity_sandbox(
            &requested_create_route_identity,
            &id,
        )?;
        let requested_create_route_identity_can_open_claim =
            runtime_activation_wire_identity_can_open_claim(&requested_create_route_identity);
        let normalized_create_route_identity = fresh_runtime_activation_identity(
            requested_create_route_identity,
            pool_template.is_some(),
            self.local_node_id,
        )?;
        let create_route_identity = normalized_create_route_identity.identity;
        let create_route_provenance = normalized_create_route_identity.provenance;
        let open_route_activation =
            (!req.autostart && requested_create_route_identity_can_open_claim).then(|| {
                crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(
                    &create_route_identity,
                    create_route_provenance,
                )
            })
            .flatten();
        let network_policy_override = Self::resolve_create_network_policy_override(req)?;
        let network_policy_authority = create_network_policy_authority(
            network_policy_override.as_ref(),
            req.network_policy_authority,
            default_network_policy_authority,
        )?;
        let network_policy_generation = create_network_policy_generation(
            network_policy_override.as_ref(),
            req.network_policy_generation,
            network_policy_authority,
        );
        let auto_lifecycle_authority = if req.auto_lifecycle.is_some() {
            crate::db::PolicyAuthority::try_from(req.auto_lifecycle_authority)
                .map_err(|error| Status::invalid_argument(error.to_string()))?
        } else {
            auto_lifecycle_authority
        };

        if req.autostart {
            let mut node_client = connect_node(&self.node_manager)
                .await
                .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;
            self.reject_autostart_create_without_ready_slot(&mut node_client)
                .await?;
        }

        let create_cleanup_id = SandboxCleanupEpisodeId::new();
        {
            let db = self.db_conn()?;
            crate::db::insert_sandbox_with_cleanup_episode_and_route_evidence(
                &db,
                crate::db::InsertSandboxParams {
                    id: &id,
                    uuid8: &uuid8,
                    state: SandboxState::Creating,
                    template,
                    template_hash: Some(&template_hash),
                    user: sandbox_owner,
                    runtime,
                    snapshots_enabled: req.snapshots_enabled,
                    rootfs_size_mb: req.rootfs_size_mb,
                    disk_io_limits: pplx_space_common_core::types::DiskIoLimits::try_from_optional(
                        req.disk_io_max_operations_per_sec,
                        req.disk_io_max_bandwidth_mib_per_sec,
                    )
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
                    pool_template,
                    volume_encryption: req.volume_encryption,
                    process_env: &process_env,
                    auto_lifecycle,
                    network_policy_override: network_policy_override.as_ref(),
                    network_policy_generation,
                    network_policy_authority,
                    auto_lifecycle_authority,
                },
                crate::db::CleanupEpisodeSpec {
                    id: create_cleanup_id,
                    completion: crate::db::CleanupCompletion::DeleteRow,
                    keep_filesystem: false,
                    entered_from_state: None,
                    activation_state: SandboxState::Creating,
                    execution_generation: create_route_identity.execution_generation,
                    activation_attempt_id: create_route_identity.activation_attempt_id,
                },
                create_route_evidence_before_start(
                    &create_route_identity,
                    create_route_provenance,
                    req.autostart,
                ),
                open_route_activation.as_ref(),
                execution,
            )
            .map_err(|e| Status::internal(format!("db insert failed: {e}")))?;
        }
        let create_cleanup = self.activation_cleanup_guard(
            &id,
            &uuid8,
            ActivationCleanupSpec {
                cleanup_id: create_cleanup_id,
                completion: CleanupCompletion::DeleteRow,
                context: "cleanup after interrupted create",
                interrupted_reason_code: SandboxQuarantineReasonCode::CreateInterrupted,
                interrupted_reason: "create did not complete",
            },
        )?;

        let cache_dir = self.cache_root();
        if let Err(status) = ensure_sandbox_cache_root_in(cache_dir, &id) {
            create_cleanup.cleanup_after_failure(
                SandboxQuarantineReasonCode::CreateInterrupted,
                status.message(),
            );
            return Err(status);
        }

        let update_result = {
            let db = self.db_conn()?;
            write_create_completed(&db, &id)
        };
        if let Err(status) = update_result {
            create_cleanup.cleanup_after_failure(
                SandboxQuarantineReasonCode::CreateInterrupted,
                status.message(),
            );
            return Err(status);
        }

        if effective_mode == BootMode::BaseSnapshot {
            let Some(snapshot_dir_resolved) = snapshot_dir_resolved else {
                let status = Status::failed_precondition(format!(
                    "no base snapshot for template '{template}'"
                ));
                create_cleanup.cleanup_after_failure(
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    status.message(),
                );
                return Err(status);
            };
            let base_vmstate = format!("{snapshot_dir_resolved}/vmstate.base");
            let base_snapshot = format!("{snapshot_dir_resolved}/snapshot.base");

            if !std::path::Path::new(&base_vmstate).exists()
                || !std::path::Path::new(&base_snapshot).exists()
            {
                let status = Status::failed_precondition(format!(
                    "no base snapshot for template '{template}' (need vmstate.base + snapshot.base)"
                ));
                create_cleanup.cleanup_after_failure(
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    status.message(),
                );
                return Err(status);
            }

            let status = match base_snapshot_memory_file_mib(snapshot_dir_resolved) {
                Ok(snapshot_memory_mib) => match connect_node(&self.node_manager).await {
                    Ok(mut node_client) => {
                        match Self::node_sandbox_memory_policy(&mut node_client).await {
                            Ok(node_memory_policy) => ensure_snapshot_memory_allowed(
                                snapshot_memory_mib,
                                node_memory_policy.guest_memory_max_mib,
                            )
                            .map(|_| ()),
                            Err(status) => Err(status),
                        }
                    }
                    Err(e) => Err(Status::internal(format!(
                        "cannot connect to node-manager: {e}"
                    ))),
                },
                Err(status) => Err(status),
            };
            if let Err(status) = status {
                create_cleanup.cleanup_after_failure(
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    status.message(),
                );
                return Err(status);
            }

            let sandbox_snapshot_dir = cache_dir
                .join(&id)
                .join("snapshot")
                .to_string_lossy()
                .into_owned();
            if let Err(e) = std::fs::create_dir_all(&sandbox_snapshot_dir) {
                let status = Status::internal(format!("create snapshot dir: {e}"));
                create_cleanup.cleanup_after_failure(
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    status.message(),
                );
                return Err(status);
            }

            if let Err(e) = pplx_space_node_runtime_core::reflink_copy(
                std::path::Path::new(&base_vmstate),
                std::path::Path::new(&format!("{sandbox_snapshot_dir}/vmstate")),
            ) {
                let status = Status::internal(format!("reflink vmstate.base: {e}"));
                create_cleanup.cleanup_after_failure(
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    status.message(),
                );
                return Err(status);
            }

            if let Err(e) = pplx_space_node_runtime_core::reflink_copy(
                std::path::Path::new(&base_snapshot),
                std::path::Path::new(&format!("{sandbox_snapshot_dir}/memory")),
            ) {
                let status = Status::internal(format!("reflink snapshot.base: {e}"));
                create_cleanup.cleanup_after_failure(
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    status.message(),
                );
                return Err(status);
            }

            if let Err(status) = stage_base_snapshot_runtime_metadata(
                std::path::Path::new(snapshot_dir_resolved),
                std::path::Path::new(&sandbox_snapshot_dir),
                snapshot_vcpu,
            ) {
                create_cleanup.cleanup_after_failure(
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    status.message(),
                );
                return Err(status);
            }

            {
                let update_result = {
                    let db = self.db_conn()?;
                    write_base_snapshot_create_completed(&db, &id)
                };
                if let Err(status) = update_result {
                    create_cleanup.cleanup_after_failure(
                        SandboxQuarantineReasonCode::CreateInterrupted,
                        status.message(),
                    );
                    return Err(status);
                }
            }
        }

        let completed = {
            let db = self.db_conn()?;
            crate::db::complete_create(&db, &id, create_cleanup_id, !req.autostart)
                .map_err(|error| Status::internal(format!("complete create: {error}")))?
        };
        if !completed {
            return Err(Status::aborted(
                "sandbox create cleanup episode was no longer current",
            ));
        }
        create_cleanup.complete_success();

        if req.autostart {
            let from_warm = effective_mode == BootMode::BaseSnapshot;
            let start_resp = match self
                .start_sandbox_with_authority(
                    autostart_request(id.clone(), from_warm, req.blocking, &create_route_identity),
                    false,
                    IdentityFreeDirectActivationAuthority::Denied,
                )
                .await
            {
                Ok(response) => response,
                Err(status) => return Err(status),
            };
            let r = start_resp.into_inner();
            return Ok(Response::new(CreateResponse {
                environment: None,
                sandbox_id: id,
                pid: Some(r.pid),
                guest_ip: Some(r.guest_ip),
                namespace: Some(r.namespace),
                log_path: Some(r.log_path),
                vsock_uds_path: Some(r.vsock_uds_path),
                boot_mode_used: effective_mode.into(),
                reused_existing: false,
                route_identity: r.route_identity,
            }));
        }

        Ok(Response::new(CreateResponse {
            sandbox_id: id,
            boot_mode_used: effective_mode.into(),
            ..Default::default()
        }))
    }

    /// Start a sandbox from a checkpoint/suspend state (restore path).
    /// Called by `start()` when a checkpoint is specified and sandbox is Suspended.
    /// When `blocking` is true, returns after the restored VM is resumed (or, for
    /// `target_state = Paused`, after the snapshot is loaded and the post-load
    /// resume is intentionally skipped). Assigned Started restores also wait for
    /// guest compatibility/readiness (30s) and gateway network readiness (60s);
    /// `ready_timeout_secs` does not apply to this path.
    ///
    /// `target_state = Paused` is used by the cluster-controller's lifecycle-
    /// preserving drain evacuation: it leaves the firecracker VM in the paused
    /// state that snapshot-load puts it in, so the sandbox is `SandboxState::Paused`
    /// at the end instead of `SandboxState::Started`. Without this, a paused
    /// user sandbox would silently resume after a drain-time round-trip.
    pub(super) async fn start_from_checkpoint(
        &self,
        launch: StartFromCheckpointLaunch<'_>,
    ) -> Result<Response<StartResponse>, Status> {
        let StartFromCheckpointLaunch {
            vcpu,
            activation_auto_lifecycle,
            operation_activation_attempt_id,
            sandbox,
            blocking,
            netns_path,
            target_state,
            execution_generation,
            space_api_route_identity,
            route_identity_provenance,
            open_activation_claim_eligible,
            restored_snapshot,
        } = launch;
        // A stopped target is a data placement that must end before
        // Firecracker. The `Start` boundaries answer it before reaching here;
        // refusing it at the launch itself means a path that misses them fails
        // instead of booting a sandbox that is supposed to stay inert.
        if target_state == SandboxState::Stopped {
            return Err(Status::failed_precondition(format!(
                "sandbox {} has a stopped target; a checkpoint launch never runs for it",
                sandbox.id
            )));
        }
        self.ensure_snapshot_restore_supported("start_from_checkpoint")?;

        let id = &sandbox.id;
        let original_state = sandbox.state;
        let mut uuid8 = sandbox.uuid8.clone();
        let cache_root = self.cache_root();
        let prepare_source = start_stage_timeout(
            "restore.template_prepare_source",
            START_TEMPLATE_RESOLUTION_TIMEOUT,
            id,
            template_source_for_sandbox_prepare_in(
                &self.node_manager,
                self.template_resolver.as_ref(),
                sandbox,
                cache_root,
            ),
        )
        .await?;
        let prepare_template = prepare_source.template;
        let prepare_template_dir = prepare_source.template_dir;
        // Resolve the effective policy ref up front; it is pushed to the
        // net-gateway in the background after launch (below). The gateway
        // namespace denies egress until policy and CA readiness (fail-closed).
        // Hydration overlaps snapshot load; the finalizer confirms traffic
        // readiness before publishing Started. Hydration requires that the
        // gateway can actually pull policy as soon as its
        // process starts: host-control authorizes those pulls by the gateway's
        // registered (pid, uid), so the launch below persists the registration
        // inside the gateway launch arm rather than after the jailer join.
        let effective_policy_ref = if Self::sandbox_is_pool_member(sandbox) {
            None
        } else {
            // Cache resolution owns and measures the foreground deadline.
            let effective_policy = start_stage(
                "restore.effective_network_policy",
                id,
                self.effective_network_policy(sandbox),
            )
            .await?;
            Some(effective_network_policy_ref(sandbox, &effective_policy)?)
        };
        let mut node_client = start_stage_timeout(
            "restore.connect_node_mgr",
            START_NODE_METADATA_TIMEOUT,
            id,
            async {
                connect_node(&self.node_manager)
                    .await
                    .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))
            },
        )
        .await?;
        let node_memory_policy = start_stage_timeout(
            "restore.node_memory_policy",
            START_NODE_METADATA_TIMEOUT,
            id,
            Self::node_sandbox_memory_policy(&mut node_client),
        )
        .await?;
        let snapshot_dir = cache_root
            .join(id)
            .join("snapshot")
            .to_string_lossy()
            .into_owned();
        let snapshot_execution = checkpoint_execution_metadata(&snapshot_dir)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let cpu_budget = restore_cpu_budget(vcpu, snapshot_execution.cpu_budget())?;
        let captured_memory = snapshot_execution.guest_memory();
        let producer_execution = snapshot_execution.into_execution();
        if sandbox.execution_metadata.as_ref().is_some_and(|saved| {
            saved.runtime().kind != producer_execution.runtime().kind
                || saved.guest() != producer_execution.guest()
        }) {
            return Err(Status::failed_precondition(
                "checkpoint runtime or guest differs from the sandbox binding",
            ));
        }
        let requested_execution = start_stage_timeout(
            "restore.select_consumer",
            START_NODE_METADATA_TIMEOUT,
            id,
            super::environment_selection::select_with_client(
                &mut node_client,
                &producer_execution,
                pplx_space_node_mgr_client::proto::ExecutionEnvironmentSelection::Restore,
            ),
        )
        .await?;
        let snapshot_memory_mib = ensure_snapshot_memory_allowed(
            restored_guest_memory_mib(captured_memory, snapshot_memory_file_mib(&snapshot_dir)?)?,
            node_memory_policy.guest_memory_max_mib,
        )?;
        let metadata_json = activation_metadata_json(sandbox, cpu_budget, captured_memory)?;
        let cgroup_memory_mib = snapshot_memory_mib
            .map(|memory_mib| node_memory_policy.cgroup_memory_mib_for_guest(memory_mib))
            .unwrap_or(node_memory_policy.cgroup_memory_mib);
        sandbox
            .state
            .transition(SandboxState::Restoring)
            .map_err(Status::from)?;

        let open_route_activation = super::activation_support::open_route_activation_claim(
            &space_api_route_identity,
            route_identity_provenance,
            open_activation_claim_eligible,
        );
        // An in-place restore already asked before staging, so this ask
        // catches a retirement that landed during staging. A warm start asks
        // only here.
        self.refuse_retired_activation_retry(id, open_route_activation.as_ref())
            .await?;

        // Serialize the whole activation boundary with cleanup. A cleanup
        // admitted for the previous execution must either finish before this
        // restore claims the row, or observe the attached replacement
        // generation before it can dispatch another node cleanup.
        let activation_lock = self.cleanup_lock(id);
        let activation_lock_owner = crate::service::cleanup_support::CleanupLockGuard::new(
            Arc::clone(&self.cleanup_locks),
            id.clone(),
            activation_lock,
        );
        let activation_fence = super::restart_admission::wait_for_activation_admission(
            id,
            activation_lock_owner.lock(),
        )
        .await?;

        let cleanup_episode = {
            let db = self.db_conn()?;
            crate::db::begin_activation_with_route_evidence(
                &db,
                id,
                original_state,
                crate::db::ActivationKind::Restore,
                execution_generation,
                space_api_route_identity.activation_attempt_id,
                crate::db::ActivationRouteAdmission {
                    auto_lifecycle: activation_auto_lifecycle,
                    operation_activation_attempt_id,
                    evidence: crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(
                        &space_api_route_identity,
                        route_identity_provenance,
                    ),
                    open_claim: open_route_activation.as_ref(),
                    runtime_upgrade: None,
                },
            )
            .map_err(|error| {
                super::runtime_upgrade::begin_activation_status(
                    error,
                    "failed to claim restoring state before restore prepare",
                )
            })?
        };
        let Some(cleanup_episode) = cleanup_episode else {
            emit_sandbox_event(
                id,
                SandboxEventType::LifecycleTransitionCasLost,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .trigger(SandboxEventTrigger::System)
                    .operation("restore_activation")
                    .reason("restore_entry_cas_lost")
                    .with_from_state(original_state.to_string())
                    .target_state(SandboxState::Restoring.to_string())
                    .rows_affected("0"),
            );
            return Err(Status::unavailable(format!(
                "sandbox activation ownership changed from {original_state}; retry restore"
            )));
        };
        let cleanup_id = cleanup_episode.id;
        let cleanup_completion = cleanup_episode.completion;

        let mut activation_cleanup = self.activation_cleanup_guard(
            id,
            &uuid8,
            ActivationCleanupSpec {
                cleanup_id,
                completion: cleanup_completion,
                context: "cleanup after cancelled restore activation",
                interrupted_reason_code: SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                interrupted_reason: "restore activation did not complete",
            },
        )?;

        // Commit the consumer before Prepare can rewrite disk metadata. The
        // checkpoint manifest remains producer evidence; a cancelled attempt
        // retains the consumer for a later cold start. Another restore attempt
        // selects again from the unchanged checkpoint producer.
        {
            let db = self.db_conn()?;
            crate::db::upsert_sandbox_execution_metadata(&db, id, &requested_execution)
                .map_err(|error| Status::internal(format!("persist restore consumer: {error}")))?;
        }

        // Allocate network slot
        let execution_fence = read_node_execution_fence(&self.db, id)?.ok_or_else(|| {
            Status::failed_precondition("activation has no durable node execution identity")
        })?;
        let has_netns_path = netns_path.is_some();
        let alloc = node_client
            .allocate(AllocateRequest {
                execution_fence: Some(execution_fence.into()),
                sandbox_id: id.clone(),
                netns_path,
                sandbox_cgroup_memory_mib: cgroup_memory_mib,
            })
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
                crate::db::rollback_restoring_episode(&db, id, sandbox.state, cleanup_id)
                    .inspect_err(|error| {
                        tracing::warn!(
                            sandbox_id = %id,
                            state = ?sandbox.state,
                            %error,
                            "failed to restore sandbox state after invalid sandbox uid"
                        );
                    })
                    .unwrap_or(false);
            drop(db);
            if release_result.is_ok() && restored_state {
                activation_cleanup.complete_handled_failure();
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

        let guest_cid = alloc.slot + 3;
        let runtime = requested_execution.runtime().kind;
        let vsock_uds_path = pplx_space_node_runtime_core::control::guest_socket_path(runtime, id)
            .map_err(|e| activation_cleanup.failure(Status::failed_precondition(e.to_string())))?
            .display()
            .to_string();
        let api_socket_path = pplx_space_node_runtime_core::control::socket_path(
            runtime,
            id.parse().map_err(|e| {
                activation_cleanup.failure(Status::invalid_argument(format!("sandbox id: {e}")))
            })?,
        )
        .map_err(|e| activation_cleanup.failure(Status::failed_precondition(e.to_string())))?
        .display()
        .to_string();

        // Build minimal VM config for snapshot restore (need network + vsock)
        // Keep Firecracker's block-device defaults. The cache/io-engine variants
        // we measured did not improve lifecycle latency, so avoid carrying an
        // override surface into normal sandbox starts.
        let vm_config = format!(
            r#"{{
  "network-interfaces": [{{
    "iface_id": "eth0",
    "guest_mac": "{}",
    "host_dev_name": "{}"
  }}],
  "vsock": {{
    "guest_cid": {},
    "uds_path": "/v.sock"
  }}
}}"#,
            alloc.guest_mac, alloc.tap_device, guest_cid
        );

        let vm_config = if runtime == pplx_space_common_core::types::RuntimeKind::CloudHypervisor {
            let memory_mib = snapshot_memory_mib
                .and_then(|mib| std::num::NonZeroU64::new(u64::from(mib)))
                .ok_or_else(|| {
                    activation_cleanup.failure(Status::failed_precondition(
                        "CH restore requires recorded guest memory or a nonempty plaintext memory artifact",
                    ))
                })?;
            let cpus = cpu_budget.ok_or_else(|| {
                activation_cleanup.failure(Status::failed_precondition(
                    "CH restore requires persisted CPU shape",
                ))
            })?;
            let builder = sandbox.user.kind() == SandboxOwnerKind::Builder;
            pplx_space_node_runtime_core::boot::configuration(
                runtime,
                &pplx_space_node_runtime_core::boot::LinuxBoot {
                    guest: requested_execution.guest(),
                    cpus,
                    memory_mib,
                    guest_cid: std::num::NonZeroU32::new(guest_cid).ok_or_else(|| {
                        activation_cleanup.failure(Status::internal("invalid guest CID"))
                    })?,
                    tap: &alloc.tap_device,
                    mac: &alloc.guest_mac,
                    disk_rate: self.rootfs_rate_limiter(builder, sandbox.disk_io_limits),
                    network_rate: (!builder).then_some(super::start::NET_RATE_LIMITER),
                    balloon: None,
                    huge_pages: self.config.hugepages,
                    console: false,
                    builder,
                },
            )
            .map_err(|e| activation_cleanup.failure(Status::failed_precondition(e.to_string())))?
        } else {
            vm_config
        };

        // Prepare sandbox after slot allocation so node-manager can derive and
        // enforce the slot UID. If this sandbox was only logically created,
        // this call also performs the first rootfs clone.
        let prepare_rootfs_size_mb = prepare_template.as_ref().and(sandbox.rootfs_size_mb);
        let prepare_response = match node_client
            .prepare_sandbox(PrepareSandboxRequest {
                execution_fence: Some(execution_fence.into()),
                sandbox_id: id.clone(),
                template: prepare_template,
                uid: sbx_uid,
                vm_config_json: Some(vm_config),
                metadata_json,
                runtime: sandbox.runtime.clone(),
                template_dir: prepare_template_dir,
                snapshot_dir: None,
                rootfs_size_mb: prepare_rootfs_size_mb,
                requested_execution: Some((&requested_execution).into()),
                execution_selection_mode: pplx_space_execution_common::proto::ExecutionSelectionMode::PersistedStateRestore
                    as i32,
                artifact_template: Some(sandbox.template.clone()),
                content_hash: sandbox.template_hash.clone(),
            })
            .instrument(tracing::info_span!(
                "sbxmgr.restore.node.prepare_sandbox",
                sandbox_id = %id
            ))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(e) => {
                let reason = format!("prepare failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::RestorePrepare.reason_code(),
                    reason.clone(),
                );
                return Err(ActivationFailureStage::RestorePrepare.failure_status(&e, reason));
            }
        };
        let resolved_execution_result = (|| {
            let resolved_execution =
                prepare_response.resolved_execution.clone().ok_or_else(|| {
                    Status::internal(
                        "node-manager prepare response omitted resolved execution metadata",
                    )
                })?;
            let resolved_execution_metadata =
                pplx_space_common_core::types::ExecutionMetadata::try_from(&resolved_execution)
                    .map_err(|error| Status::internal(error.to_string()))?;
            super::validation::require_environment_acknowledgement(
                &requested_execution,
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
                let reason = format!("prepare metadata failed: {}", error.message());
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::RestoreMetadata.reason_code(),
                    reason.clone(),
                );
                return Err(ActivationFailureStage::RestoreMetadata.failure_status(&error, reason));
            }
        };

        // BYOK: reconstruct the encrypted rootfs from the downloaded cow.luks
        // (node-mgr lays it under the template-cloned origin) and open the
        // encrypted snapshot volume from the downloaded snapshot.luks, so the
        // restored Firecracker runs its rootfs and loads vmstate+memory
        // entirely off encrypted devices. The sandbox's immutable
        // volume_encryption flag is the sole decision input (for a cross-node
        // restore the row was recreated from the snapshot metadata's
        // `encrypted` marker, which equals it by invariant); plaintext
        // sandboxes take the plaintext path with no service round-trip.
        let (rootfs_block_device_path, snapshot_volume_mount_dir) = if sandbox.volume_encryption {
            match reconstruct_encrypted_restore_devices(
                id,
                execution_fence,
                &snapshot_dir,
                &self.node_manager,
            )
            .instrument(tracing::info_span!(
                "sbxmgr.restore.encrypted_devices_prepare",
                sandbox_id = %id
            ))
            .await
            {
                Ok((device_path, mount_dir)) => (Some(device_path), Some(mount_dir)),
                Err(status) => {
                    let reason = status.message().to_string();
                    activation_cleanup.cleanup_after_failure(
                        ActivationFailureStage::RestoreEncryptedDevicesPrepare.reason_code(),
                        reason.clone(),
                    );
                    return Err(status);
                }
            }
        } else {
            (None, None)
        };

        // Launch Firecracker idle (no config boot) and net-gateway together.
        // The VM cannot emit guest packets until the snapshot is loaded and
        // resumed; policy hydration still runs independently and fail-closed.
        // The gateway begins pulling policy via host-control the moment its
        // process starts, so its PID registration is part of the gateway arm
        // (it must be visible to host-control auth before the jailer launch
        // completes), not a post-join step.
        match self
            .activation_admission_limiter
            .acquire(ActivationAdmissionKind::Restore, id)
            .await
        {
            Ok(permit) => {
                activation_cleanup.admit(permit);
            }
            Err(status) => {
                activation_cleanup.cleanup();
                return Err(status);
            }
        }
        let normalized_route_identity =
            space_api_route_identity_for_runtime_activation(space_api_route_identity.clone());
        let route_identity = normalized_route_identity.identity;
        let gateway_launch_config = net_gateway_launch_config(&self.feature_flags, id);
        let (jailer_result, netgtw_result) = {
            let mut jailer_client = node_client.clone();
            let mut netgtw_client = node_client.clone();
            let launch_jailer = async {
                let result = jailer_client
                    .launch_sandbox(LaunchSandboxRequest {
                        execution_fence: Some(execution_fence.into()),
                        sandbox_id: id.clone(),
                        uid: sbx_uid,
                        gid: sbx_uid,
                        vcpu_count: cpu_budget.map(|count| count.get().get()),
                        memory_mib: Some(cgroup_memory_mib),
                        pids_max: None,
                        runtime: sandbox.runtime.clone(),
                        snapshot_restore: true,
                        requested_execution: Some(resolved_execution),
                        execution_selection_mode: pplx_space_execution_common::proto::ExecutionSelectionMode::PersistedStateRestore
                            as i32,
                        rootfs_block_device_path: rootfs_block_device_path.clone(),
                    })
                    .instrument(tracing::info_span!(
                        "sbxmgr.restore.node.launch_firecracker",
                        sandbox_id = %id,
                        snapshot_restore = true
                    ))
                    .await;
                match &result {
                    Ok(_) => activation_cleanup.finish_admission(AdmissionPermitOutcome::Succeeded),
                    Err(_) => activation_cleanup.mark_permit_failed(),
                }
                result
            };
            let launch_netgtw = register_net_gateway_launch(
                &self.db,
                id,
                sbx_uid,
                launch_net_gateway_via_node(
                    &mut netgtw_client,
                    execution_fence,
                    id,
                    sbx_uid,
                    sandbox.pool_template.is_some(),
                    Some(&route_identity),
                    &gateway_launch_config,
                ),
            );
            tokio::join!(launch_jailer, launch_netgtw)
        };
        let jailer_resp = match jailer_result {
            Ok(r) => r.into_inner(),
            Err(e) => {
                // The jailer failure wins the early return, discarding the
                // gateway arm's outcome; log a dropped gateway launch or
                // registration-write error so it stays diagnosable.
                if let Err(netgtw_error) = &netgtw_result {
                    tracing::warn!(
                        sandbox_id = %id,
                        %netgtw_error,
                        "net-gateway launch/registration also failed during failed jailer launch"
                    );
                }
                let reason = format!("launch failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::RestoreFirecrackerLaunch.reason_code(),
                    reason.clone(),
                );
                return Err(
                    ActivationFailureStage::RestoreFirecrackerLaunch.failure_status(&e, reason)
                );
            }
        };
        let pid = jailer_resp.pid;

        // The gateway arm covers both the launch and the PID registration
        // write, so a registration failure is cleaned up here exactly like a
        // launch failure.
        let netgtw_resp = match netgtw_result {
            Ok(response) => response,
            Err(e) => {
                let reason = format!("launch net-gateway failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::RestoreNetGatewayLaunch.reason_code(),
                    reason,
                );
                return Err(e);
            }
        };
        if let Some(effective_policy_ref) = effective_policy_ref {
            spawn_network_policy_refresh(
                self.db.clone(),
                id.to_string(),
                effective_policy_ref,
                "snapshot-restore",
            );
        }

        // Load the snapshot and start spaced-host concurrently. spaced-host only
        // binds its local socket at startup; guest bridge connections happen on
        // demand and are no longer part of the default blocking restore gate.
        let (load_snapshot_result, spaced_host_result) = {
            let mut snapshot_client = node_client.clone();
            let mut spaced_host_client = node_client.clone();
            let load_snapshot = async {
                snapshot_client
                    .restore_vm_state(RestoreVmStateRequest {
                        execution_fence: Some(execution_fence.into()),
                        sandbox_id: id.clone(),
                        artifacts: super::snapshot_capture::artifacts(),
                        storage: Some(pplx_space_node_mgr_client::proto::VmStateStorage {
                            directory: Some(snapshot_dir.clone()),
                            encrypted_volume_mount: snapshot_volume_mount_dir.clone(),
                        }),
                        rootfs_rate_limit: self
                            .rootfs_rate_limiter(
                                sandbox.user.kind() == SandboxOwnerKind::Builder,
                                sandbox.disk_io_limits,
                            )
                            .map(super::start::drive_rate_limit_proto),
                    })
                    .instrument(tracing::info_span!(
                        "sbxmgr.restore.node.load_snapshot",
                        sandbox_id = %id
                    ))
                    .await
            };
            let launch_spaced_host = launch_spaced_host_via_node(
                &mut spaced_host_client,
                execution_fence,
                id,
                sbx_uid,
                &vsock_uds_path,
                &self.host_control_socket_path,
                "snapshot",
            );
            tokio::join!(load_snapshot, launch_spaced_host)
        };

        if let Err(e) = load_snapshot_result {
            let reason = format!("load snapshot failed: {e}");
            activation_cleanup.cleanup_after_failure(
                ActivationFailureStage::RestoreSnapshotLoad.reason_code(),
                reason.clone(),
            );
            return Err(ActivationFailureStage::RestoreSnapshotLoad.failure_status(&e, reason));
        }

        let spaced_host_resp = match spaced_host_result {
            Ok(response) => response,
            Err(e) => {
                let reason = format!("launch spaced-host failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::RestoreSpacedHostLaunch.reason_code(),
                    reason,
                );
                return Err(e);
            }
        };
        // Resume VM from snapshot (retry on transient h2/transport errors).
        //
        // Skipped entirely for `target_state = Paused` (drain lifecycle-
        // preserving evacuation): the snapshot-load leaves the firecracker
        // VM in the paused state, and the drain pipeline wants it to stay
        // there so the user-visible lifecycle is preserved across the
        // suspend → restore round-trip. The final state-transition step
        // below routes to `SandboxState::Paused` instead of `Started` for
        // this path. The proto-to-state mapping only emits Started or
        // Paused, so any other variant on `target_state` is a programmer
        // error caught at FSM transition time.
        if target_state != SandboxState::Paused {
            let mut resume_err = None;
            for attempt in 0..3 {
                match node_client
                    .resume_sandbox(ResumeSandboxRequest {
                        execution_fence: Some(execution_fence.into()),
                        sandbox_id: id.clone(),
                    })
                    .instrument(tracing::info_span!(
                        "sbxmgr.restore.node.resume",
                        sandbox_id = %id,
                        attempt
                    ))
                    .await
                {
                    Ok(_) => {
                        resume_err = None;
                        break;
                    }
                    Err(e) => {
                        resume_err = Some(e);
                        if attempt < 2 {
                            // Reconnect to node-manager in case the connection was reset
                            if let Ok(new_client) = connect_node(&self.node_manager).await {
                                node_client = new_client;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        }
                    }
                }
            }
            if let Some(e) = resume_err {
                let reason = format!("resume after restore failed: {e}");
                activation_cleanup.cleanup_after_failure(
                    ActivationFailureStage::RestoreResume.reason_code(),
                    reason.clone(),
                );
                return Err(ActivationFailureStage::RestoreResume.failure_status(&e, reason));
            }
        }
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
                            ActivationFailureStage::RestoreSpacedHostLaunch.reason_code(),
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
                            ActivationFailureStage::RestoreSpacedHostLaunch.reason_code(),
                            reason,
                        );
                        return Err(error);
                    }
                }
            }
        };
        // Update SQLite with runtime fields (state stays Restoring until the
        // snapshot restore contract is accepted).
        let log_path = format!("{RUN_DIR}/{id}/fc.log");
        let response_route_identity = space_api_route_identity_to_wire(&route_identity);
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
                route_identity_provenance,
            },
        )?;
        drop(activation_fence);
        drop(activation_lock_owner);
        emit_runtime_activated_event(RuntimeActivatedEvent {
            sandbox_id: id,
            boot_type: "snapshot",
            pid,
            spaced_host_pid: spaced_host_resp.pid,
            net_gateway_pid: netgtw_resp.pid,
            sandbox_uid: sbx_uid,
            network_slot: alloc.slot,
            guest_cid,
            guest_ip: &alloc.guest_ip,
            guest_gateway: &alloc.guest_gateway,
            namespace: &alloc.namespace,
            vcpu: cpu_budget.map(|count| count.get().get()),
            memory_mib: u32::try_from(cgroup_memory_mib).unwrap_or(u32::MAX),
            has_netns_path,
            execution: &resolved_execution_metadata,
            execution_generation: execution_generation.map(|value| value.get()),
            restored_snapshot_storage_uri: restored_snapshot.storage_uri,
            restored_snapshot_local_artifact_id: restored_snapshot.local_artifact_id,
            local_artifact_id: sandbox.current_local_artifact_id.map(|id| id.to_string()),
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
                "sbxmgr.restore.watchdog.add_sandbox",
                sandbox_id = %id
            ))
            .await;
        // Whatever the restore's final state, paused included: the guest's
        // first diagnostics drain is the snapshot's, not this sandbox's.
        self.watchdog.mark_restored_from_snapshot(id);

        // Resume snapshot scheduling if enabled
        if sandbox.snapshots_enabled && sandbox.pool_template.is_none() {
            self.snapshot_scheduler
                .add_sandbox(id.clone(), sandbox.template.clone())
                .await;
        }

        // `target_state` is already a `SandboxState` (`Started` or
        // `Paused` after the proto boundary); use it as the final state
        // directly. Any other variant is unreachable on this path —
        // the proto-to-state mapping only emits these two — but the
        // FSM transition below will reject anything invalid.
        let final_state = target_state;
        if blocking {
            if let Err(error) = complete_restore_and_mark_final(
                self.db.clone(),
                id,
                &sandbox.process_env,
                final_state,
                Some(cleanup_id),
            )
            .await
            {
                return Err(activation_cleanup.failure(error));
            }
            if sandbox.pool_template.is_none() {
                cleanup_restored_snapshot_dir_async(id, &snapshot_dir, &self.node_manager);
            }
            activation_cleanup.complete_success();
        } else {
            spawn_restore_state_transition(
                self,
                id.clone(),
                sandbox.process_env.clone(),
                final_state,
                sandbox.pool_template.is_none().then_some(snapshot_dir),
                activation_cleanup,
                cleanup_id,
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

pub(super) fn autostart_request(
    sandbox_id: String,
    from_warm: bool,
    blocking: bool,
    create_route_identity: &pplx_space_common_core::types::SpaceApiRouteIdentity,
) -> StartRequest {
    StartRequest {
        sandbox_id,
        from_warm,
        blocking,
        // The normalized identity, not the raw request: the create cleanup
        // episode already recorded it, and re-normalizing the raw wire identity
        // here would derive a second attempt that does not own that episode.
        route_identity: space_api_route_identity_to_wire(create_route_identity),
        ..Default::default()
    }
}

pub(super) fn create_route_evidence_before_start(
    identity: &pplx_space_common_core::types::SpaceApiRouteIdentity,
    provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    autostart: bool,
) -> Option<crate::auto_lifecycle_delete_receipts::RouteEvidence> {
    if autostart {
        return None;
    }
    crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(identity, provenance)
}

#[cfg(test)]
mod tests;

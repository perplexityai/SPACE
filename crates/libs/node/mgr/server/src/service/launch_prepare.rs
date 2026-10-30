use super::cleanup_helpers::*;
use super::validation::*;
use super::*;
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::host::linux::paths::{FIRECRACKER_LOG_FILE, FIRECRACKER_METRICS_FIFO};
use pplx_space_common_core::sandbox_storage::{validate_rootfs_bytes, validate_rootfs_mib};
use pplx_space_common_core::types::RuntimeKind;
use pplx_space_node_runtime_core::proto::{ArtifactIntegrityRequirement, VerifiedRuntimeLaunch};
use pplx_space_node_runtime_core::runtime_inventory::{
    ExecutionSelectionMode, ExecutionSourceIntegrity, RuntimeFileRole, boot_component_role,
};

struct PreparationOwnership {
    lease: SlotInfo,
    _gate: SandboxCgroupLaunchGuards,
    _permit: crate::lifecycle_limiter::LifecyclePermit,
}

impl NodeManagerService {
    async fn launch_admitted_runtime(
        &self,
        sandbox_id: &str,
        request: pplx_space_node_runtime_core::proto::LaunchRuntimeRequest,
    ) -> Result<pplx_space_node_runtime_core::proto::LaunchRuntimeResponse, Status> {
        let expected_environment = request
            .verified_runtime
            .as_ref()
            .map(|verified| verified.environment_reference.clone());
        let launched = self
            .runtime_supervisor
            .launch_runtime(request)
            .await
            .map_err(pplx_space_node_runtime_core::launch_admission::supervisor_launch_status)?
            .into_inner();
        if expected_environment
            .is_some_and(|reference| launched.accepted_environment_reference != reference)
        {
            kill_launched_process_after_pid_persist_failure(self, sandbox_id, launched.pid).await;
            return Err(Status::failed_precondition(
                "supervisor did not acknowledge the verified execution environment",
            ));
        }
        Ok(launched)
    }

    async fn acquire_preparation_ownership(
        &self,
        sandbox_id: &str,
        uid: u32,
        permit: crate::lifecycle_limiter::LifecyclePermit,
    ) -> Result<PreparationOwnership, Status> {
        let expected = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::not_found("sandbox not allocated"))?;
        let gate = tokio::time::timeout(
            Duration::from_secs(30),
            self.lock_sandbox_vm_cgroup_launch(sandbox_id),
        )
        .await
        .map_err(|_| Status::unavailable("sandbox preparation ownership is busy"))?;
        let current = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| {
                Status::failed_precondition("sandbox was released before preparation")
            })?;
        if current.sandbox_id != sandbox_id
            || (current.slot, current.generation, current.sandbox_uid)
                != (expected.slot, expected.generation, expected.sandbox_uid)
        {
            return Err(Status::failed_precondition(
                "sandbox lease changed before preparation",
            ));
        }
        validate_sandbox_uid(sandbox_id, uid, current.sandbox_uid)?;
        if self.pool.shutdown_token.is_cancelled() {
            return Err(Status::unavailable("node-manager is shutting down"));
        }
        Ok(PreparationOwnership {
            lease: current,
            _gate: gate,
            _permit: permit,
        })
    }

    async fn run_owned_preparation(
        &self,
        ownership: PreparationOwnership,
        reservation: Option<crate::disk_admission::DiskAdmissionReservation>,
        work: impl FnOnce() -> Result<(), Status> + Send + 'static,
    ) -> Result<(), Status> {
        // All preflight checks have passed. Keep the gate held while the store
        // atomically fences recovery, then hand ownership directly to tracked work.
        self.adopt_validated_lease(&ownership.lease.sandbox_id, &ownership.lease)?;
        // The enclosing sandbox-work task already outlives the RPC waiter.
        // Move all guards into the blocking child, then retain them through
        // accounting refresh in that same owner; no second detached task.
        let (ownership, reservation, result) = self
            .off_runtime("preparation", move || {
                let result = work();
                Ok((ownership, reservation, result))
            })
            .await?;
        let _ownership = ownership;
        if let Some(reservation) = reservation {
            // Partial failure can consume disk too.
            self.disk_admission.release_after_success(reservation).await;
        }
        result
    }
}

fn parse_runtime_request(
    runtime: &str,
    requested_execution: Option<&pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<
    (
        pplx_space_common_core::types::RuntimeKind,
        Option<pplx_space_common_core::types::ExecutionMetadata>,
    ),
    Status,
> {
    use pplx_space_common_core::types::RuntimeKind;

    let runtime = runtime
        .parse::<RuntimeKind>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let requested = requested_execution
        .map(pplx_space_common_core::types::ExecutionMetadata::try_from)
        .transpose()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok((runtime, requested))
}

fn parse_execution_selection_mode(value: i32) -> Result<ExecutionSelectionMode, Status> {
    use pplx_space_execution_common::proto::ExecutionSelectionMode as ProtoMode;

    match ProtoMode::try_from(value).unwrap_or(ProtoMode::Unspecified) {
        ProtoMode::NewColdBoot => Ok(ExecutionSelectionMode::NewColdBoot),
        ProtoMode::ExistingColdBoot => Ok(ExecutionSelectionMode::ExistingColdBoot),
        ProtoMode::PersistedStateRestore => Ok(ExecutionSelectionMode::PersistedStateRestore),
        ProtoMode::Unspecified => Err(Status::invalid_argument(
            "execution_selection_mode is required",
        )),
    }
}

fn sandbox_metadata(
    req: &PrepareSandboxRequest,
    execution: pplx_space_common_core::types::ExecutionMetadata,
) -> Result<pplx_space_common_core::types::SandboxArtifactMetadata, Status> {
    use std::str::FromStr;

    use pplx_space_common_core::types::{
        ContentHash, ORPHANED_SANDBOX_TEMPLATE, SandboxArtifactMetadata, SandboxId, TemplateName,
    };

    let sandbox_id = SandboxId::from_str(&req.sandbox_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let metadata = if !req.metadata_json.is_empty() {
        let mut metadata: SandboxArtifactMetadata = serde_json::from_str(&req.metadata_json)
            .map_err(|error| {
                Status::invalid_argument(format!("invalid sandbox metadata: {error}"))
            })?;
        if metadata.sandbox_id != sandbox_id {
            return Err(Status::invalid_argument(
                "sandbox metadata id does not match request",
            ));
        }
        metadata.execution = execution;
        metadata
    } else {
        let template = req
            .artifact_template
            .as_deref()
            .or(req.template.as_deref())
            .filter(|value| !value.is_empty())
            .unwrap_or(ORPHANED_SANDBOX_TEMPLATE);
        let template = TemplateName::parse(template)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let content_hash = req
            .content_hash
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(ContentHash::from_str)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        SandboxArtifactMetadata::new(sandbox_id, execution, template, content_hash)
    };
    pplx_space_node_runtime_core::boot::GuestProfile::resolve(
        &metadata.execution,
        metadata.guest_image(),
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(metadata)
}

fn write_sandbox_metadata_atomic(path: &Path, contents: &[u8]) -> Result<(), Status> {
    use std::io::Write;
    super::artifact_fs::write_atomic(path, |mut output| {
        output
            .write_all(contents)
            .map_err(super::artifact_fs::error)?;
        super::trusted_paths::set_permissions(output, Some(gids::SPACE_CACHE_RW), 0o660)
    })
    .map(|_| ())
}

fn prepare_sandbox_owned_paths<'a>(
    cache_dir: &'a str,
    rootfs_path: &'a str,
    config_path: &'a str,
) -> [&'a str; 3] {
    [cache_dir, rootfs_path, config_path]
}

struct PrepareSandboxCachePaths {
    instance_dir: String,
    rootfs: String,
    sandbox_dir: String,
    snapshots_dir: String,
}

fn prepare_sandbox_cache_paths(sandbox_id: &str) -> PrepareSandboxCachePaths {
    PrepareSandboxCachePaths {
        instance_dir: paths::sandbox_instance_dir(sandbox_id),
        rootfs: paths::sandbox_rootfs_path(sandbox_id),
        sandbox_dir: paths::sandbox_cache_dir(sandbox_id),
        snapshots_dir: paths::sandbox_snapshots_dir(sandbox_id),
    }
}

fn log_resolved_execution(
    operation: &'static str,
    sandbox_id: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) {
    use pplx_space_common_core::types::ArtifactComponentKind;

    let state_format = execution.state_format();
    let kernel = execution
        .components()
        .iter()
        .find(|component| component.kind == ArtifactComponentKind::Kernel);
    tracing::info!(
        sandbox_id,
        operation,
        runtime = %execution.runtime().kind,
        runtime_version = %execution.runtime().version,
        guest_os = %execution.guest().os,
        guest_arch = %execution.guest().architecture,
        state_format_name = state_format.map(|identity| identity.format.as_str()).unwrap_or(""),
        state_format_version = state_format.map(|identity| identity.version.as_str()).unwrap_or(""),
        kernel_version = kernel.and_then(|component| component.version.as_ref()).map(|version| version.as_str()).unwrap_or(""),
        kernel_digest = kernel.and_then(|component| component.digest).map(|digest| digest.to_string()).unwrap_or_default(),
        "resolved sandbox execution environment",
    );
}

impl NodeManagerService {
    pub(super) async fn launch_sandbox(
        &self,
        request: Request<LaunchSandboxRequest>,
    ) -> Result<Response<LaunchSandboxResponse>, Status> {
        self.authorize_peer(&request, "LaunchSandbox", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        validate_sandbox_id(&sandbox_id)?;
        let service = self.clone();
        self.run_owned_sandbox_work(&sandbox_id, async move {
            service.launch_sandbox_inner(request).await
        })
        .await
    }

    async fn launch_sandbox_inner(
        &self,
        request: Request<LaunchSandboxRequest>,
    ) -> Result<Response<LaunchSandboxResponse>, Status> {
        let req = request.get_ref();
        let sandbox_id = &req.sandbox_id;
        validate_sandbox_id(sandbox_id)?;
        if let Some(path) = req
            .rootfs_block_device_path
            .as_deref()
            .filter(|p| !p.is_empty())
        {
            super::trusted_paths::validate_rootfs_device(sandbox_id, path)?;
        }
        let (runtime, requested_runtime) =
            parse_runtime_request(&req.runtime, req.requested_execution.as_ref())?;
        let selection_mode = parse_execution_selection_mode(req.execution_selection_mode)?;
        if req.snapshot_restore != (selection_mode == ExecutionSelectionMode::PersistedStateRestore)
        {
            return Err(Status::invalid_argument(
                "snapshot_restore and execution_selection_mode disagree",
            ));
        }
        let resolved_execution = self
            .resolve_bound_execution(requested_runtime.as_ref(), runtime, selection_mode)
            .await?;
        let firecracker_bin = resolved_execution
            .runtime_files
            .path(RuntimeFileRole::RuntimeExecutable)
            .ok_or_else(|| {
                Status::failed_precondition("resolved runtime is missing its executable")
            })?
            .display()
            .to_string();
        let jailer_bin = resolved_execution
            .runtime_files
            .path(RuntimeFileRole::LauncherExecutable)
            .map(|path| path.display().to_string());
        if runtime == pplx_space_common_core::types::RuntimeKind::Firecracker
            && jailer_bin.is_none()
        {
            return Err(Status::failed_precondition(
                "resolved runtime is missing its launcher",
            ));
        }
        let standalone = runtime == pplx_space_common_core::types::RuntimeKind::CloudHypervisor;
        if standalone && self.disk_io_ceiling.configured().is_some() {
            return Err(Status::failed_precondition(
                "runtime does not support a live node disk-I/O ceiling",
            ));
        }
        log_resolved_execution("launch", sandbox_id, &resolved_execution.execution);
        let _lifecycle_permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::LaunchSandbox)
            .await?;
        let _cgroup_guard = self.lock_sandbox_vm_cgroup_launch(sandbox_id).await;
        self.check_execution(sandbox_id, req.execution_fence.as_ref(), false)?;
        // Re-check after waiting for the permit and lock, so a policy applied
        // meanwhile still gates this launch.
        self.execution_policy()
            .check_execution(&resolved_execution.execution, selection_mode)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let lease = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::not_found("sandbox not allocated"))?;
        let sandbox_uid =
            validate_sandbox_uid_gid(sandbox_id, req.uid, req.gid, lease.sandbox_uid)?;
        let ns_path = lease.ns_path();
        let boot_source = pplx_space_node_runtime_core::boot::BootSource::from_execution(
            &resolved_execution.execution,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let boot_kernel = if (req.snapshot_restore && !standalone)
            || boot_source == pplx_space_node_runtime_core::boot::BootSource::Uefi
        {
            None
        } else {
            Some(
                resolved_execution
                    .component(
                        pplx_space_common_core::types::ArtifactComponentKind::Kernel,
                        Some(&boot_component_role()),
                    )
                    .ok_or_else(|| {
                        Status::failed_precondition("resolved runtime is missing its boot kernel")
                    })?,
            )
        };

        let boot_firmware = if boot_source == pplx_space_node_runtime_core::boot::BootSource::Uefi {
            Some(
                resolved_execution
                    .component(
                        pplx_space_common_core::types::ArtifactComponentKind::Firmware,
                        Some(&"firmware".parse().map_err(|error| {
                            Status::internal(format!("firmware role: {error}"))
                        })?),
                    )
                    .ok_or_else(|| {
                        Status::failed_precondition("resolved runtime is missing its firmware")
                    })?,
            )
        } else {
            None
        };

        // Jailer chroot base on btrfs so hard-links to rootfs work (same filesystem).
        // Jailer creates: {chroot_base}/firecracker/{id}/root/
        // Result: /space/cache/firecracker/jails/firecracker/{id}/root/
        let chroot_base = JAIL_DIR.to_string();

        // Place kernel + rootfs + config into the jailer's expected chroot root dir.
        // Clean any leftover from a previous launch (jailer fails with "Text file busy"
        // or "File exists" if artifacts remain from a prior attempt).
        let jail_root = pplx_space_node_runtime_core::control::isolation_root(runtime, sandbox_id)
            .map_err(|e| Status::failed_precondition(e.to_string()))?
            .display()
            .to_string();
        let control_socket = pplx_space_node_runtime_core::control::socket_path(
            runtime,
            sandbox_id
                .parse()
                .map_err(|e| Status::invalid_argument(format!("sandbox id: {e}")))?,
        )
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let observed_id = sandbox_id.to_string();
        let existing_pid = self
            .off_runtime("observe jail ownership", move || {
                super::runtime::firecracker_before_jail_rebuild(&observed_id)
            })
            .await?;
        if let Some(pid) = existing_pid {
            self.adopt_validated_lease(sandbox_id, &lease)?;
            self.pool
                .set_pid(sandbox_id, pid.get().get())
                .map_err(|error| {
                    Status::unavailable(format!("persist existing VM pid: {error}"))
                })?;
            // Reusing a pre-existing runtime bypasses cold-boot configuration.
            // Keep its PID discoverable by the sweep even if verification fails.
            if standalone {
                let client = self.runtime_control(runtime, sandbox_id).await?;
                let state = client
                    .state()
                    .await
                    .map_err(|e| Status::unavailable(e.to_string()))?;
                use pplx_space_node_runtime_core::control::VmState;
                let adoptable = match state {
                    VmState::Running => !req.snapshot_restore,
                    VmState::NotStarted | VmState::Paused => req.snapshot_restore,
                    VmState::Shutdown => false,
                };
                if !adoptable {
                    return Err(Status::failed_precondition(
                        "resident runtime has not completed boot; stop before retrying",
                    ));
                }
            } else {
                let client = crate::fc_api::FcApiClient::new(control_socket.clone());
                client.get_vm_state().await.map_err(|error| {
                    Status::unavailable(format!("resident runtime is not ready: {error}"))
                })?;
                self.disk_io_ceiling
                    .enforce_launch_adoption(&client, req.snapshot_restore)
                    .await?;
            }
            return Ok(Response::new(LaunchSandboxResponse {
                pid: pid.get().get(),
                api_socket_path: control_socket.display().to_string(),
                resolved_execution: Some((&resolved_execution.execution).into()),
            }));
        }
        let verified_environment = protected_launch_environment(
            resolved_execution.execution.runtime().kind,
            resolved_execution.runtime_files.integrity(),
            resolved_execution.environment,
            requested_runtime
                .as_ref()
                .and_then(|execution| execution.environment()),
            req.snapshot_restore,
            resolved_execution
                .components
                .iter()
                .map(|component| component.integrity),
        )?;
        if standalone && verified_environment.is_none() {
            return Err(Status::failed_precondition(
                "standalone runtime requires catalog-authenticated execution sources",
            ));
        }
        self.adopt_validated_lease(sandbox_id, &lease)?;
        let stale_root = jail_root.clone();
        spawn_lifecycle_blocking(&_lifecycle_permit, move || {
            cleanup_jail_root(
                &stale_root,
                |path| unmount_if_present(path),
                || Ok(Vec::new()),
            )
        })
        .await
        .map_err(|error| Status::internal(format!("stale jail cleanup task failed: {error}")))?
        .map_err(Status::from)?;
        let jail_root_for_create = jail_root.clone();
        spawn_lifecycle_blocking(&_lifecycle_permit, move || {
            std::fs::create_dir_all(&jail_root_for_create)
        })
        .await
        .map_err(|e| Status::internal(format!("create jail root task failed: {e}")))?
        .map_err(|e| Status::internal(format!("cannot create jail root: {e}")))?;

        // Bind-mount instance rootfs into jail so FC writes go directly to the
        // instance subvolume (required for btrfs snapshot support). Hard links don't
        // work across btrfs subvolumes, and a copy would create a separate file.
        //
        // BYOK encrypted rootfs: bind-mount the caller-supplied dm-crypt
        // composite device instead of the plaintext instance file. When the
        // field is absent — the only shape a non-BYOK launch ever sends —
        // this is byte-identical to the historical path. Either way the
        // instance file still exists (it's the dm-snapshot origin behind the
        // mapper), and the `chown -Rh sandbox_uid jail_root` below chowns the
        // bind-mount target (the mapper device) so the unprivileged jailed
        // Firecracker can open it.
        //
        // Both ends are opened first and the mount uses those open files, so
        // the bind lands on exactly the files opened here even if a path
        // component changes between the open and the mount.
        let rootfs_device = req
            .rootfs_block_device_path
            .clone()
            .filter(|s| !s.is_empty());
        let instance_dir = format!("{CACHE_DIR}/{sandbox_id}/instance");
        let jail_dir = jail_root.clone();
        spawn_lifecycle_blocking(&_lifecycle_permit, move || {
            let source = match &rootfs_device {
                Some(device) => super::trusted_paths::open_rootfs_device(Path::new(device))?,
                None => open_instance_rootfs(Path::new(&instance_dir))?,
            };
            let jail = super::trusted_paths::open_directory(Path::new(&jail_dir))?;
            let target =
                super::trusted_paths::open_bind_target(&jail, std::ffi::OsStr::new("rootfs.ext4"))?;
            super::trusted_paths::bind_pinned_files(&source, &target)
        })
        .await
        .map_err(|e| Status::internal(format!("bind rootfs task failed: {e}")))??;

        // Copy config into jail (skip for snapshot restore — FC starts idle)
        if !req.snapshot_restore || standalone {
            let config_src = format!("{RUN_DIR}/{sandbox_id}/vm-config.json");
            let config_dst = format!("{jail_root}/vm-config.json");
            let ceiling = self.disk_io_ceiling;
            spawn_lifecycle_blocking(&_lifecycle_permit, move || {
                ceiling
                    .stage_cold_boot_config(Path::new(&config_src), Path::new(&config_dst))
                    .map_err(std::io::Error::other)
            })
            .await
            .map_err(|e| Status::internal(format!("copy config task failed: {e}")))?
            .map_err(|e| Status::internal(format!("cannot copy config into jail: {e}")))?;
        }

        if standalone {
            let root = PathBuf::from(&jail_root);
            spawn_lifecycle_blocking(&_lifecycle_permit, move || {
                let jail = super::trusted_paths::open_directory(&root)?;
                super::trusted_paths::shared_state_mount(&jail)?;
                Ok::<_, Status>(())
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))??;
        }

        // Chown jail root to sandbox UID (--no-dereference to avoid symlink
        // attacks). A partial chown leaves root-owned files in the jail, so
        // refuse the launch rather than boot a sandbox on an inconsistent jail.
        let uid_str = format!("{sandbox_uid}:{sandbox_uid}");
        run_permission_command(
            "chown",
            &["-Rh", &uid_str, &jail_root],
            &jail_root,
            &uid_str,
            &_lifecycle_permit,
        )
        .await?;

        // CH restore also needs authenticated boot inputs. Inputs are linked after
        // chown: their root-owned inodes are shared with the authenticated
        // cache and must never be handed to the sandbox UID.
        for (component, name) in [(boot_kernel, "vmlinux.bin"), (boot_firmware, "firmware.fd")] {
            let Some(component) = component else { continue };
            let source = component.path.clone();
            let destination = PathBuf::from(&jail_root).join(name);
            use pplx_space_node_runtime_core::authenticated_source::{
                AuthenticatedFile, Authentication,
            };
            let authentication = match component.integrity {
                ExecutionSourceIntegrity::FsVerity => Some(Authentication::FsVerity(
                    component
                        .metadata
                        .digest
                        .map(pplx_space_node_runtime_core::catalog::compiled_verity_digest)
                        .transpose()
                        .map_err(|error| Status::failed_precondition(error.to_string()))?
                        .flatten()
                        .ok_or_else(|| {
                            Status::failed_precondition(
                                "sealed boot component has no trusted measurement",
                            )
                        })?,
                )),
                ExecutionSourceIntegrity::CatalogSha256 if standalone => Some(
                    Authentication::CatalogSha256(component.metadata.digest.ok_or_else(|| {
                        Status::failed_precondition(
                            "authenticated boot component has no catalog digest",
                        )
                    })?),
                ),
                // Firecracker retains its existing SHA-verified legacy path.
                ExecutionSourceIntegrity::CatalogSha256
                | ExecutionSourceIntegrity::UnverifiedExternal => None,
            };
            spawn_lifecycle_blocking(&_lifecycle_permit, move || {
                if let Some(authentication) = authentication {
                    let file =
                        AuthenticatedFile::open(&source, authentication).map_err(|error| {
                            if error.is_transient() {
                                Status::internal(error.to_string())
                            } else {
                                Status::failed_precondition(error.to_string())
                            }
                        })?;
                    file.link_to(std::path::Path::new(&destination))
                } else {
                    std::fs::hard_link(&source, &destination)
                }
                .map_err(|error| {
                    Status::internal(format!(
                        "cannot hard-link boot component into jail: {error}"
                    ))
                })
            })
            .await
            .map_err(|e| Status::internal(format!("link boot component task failed: {e}")))??;
        }

        // For snapshot restore: start FC idle (no config, just API socket)
        // For normal boot: start FC with config file
        let fc_config = if req.snapshot_restore {
            ""
        } else {
            "/vm-config.json"
        };
        let fc_log = format!("/{FIRECRACKER_LOG_FILE}");
        // Firecracker writes its metrics flushes to a FIFO that sandbox-manager
        // reads. Created after the chown above, which would hand its group to
        // the sandbox. Best effort: a launch without one has no Firecracker
        // metrics, and Firecracker is told about it only once it exists.
        let metrics_fifo = format!("{jail_root}/{FIRECRACKER_METRICS_FIFO}");
        let fc_metrics = if standalone {
            String::new()
        } else {
            match spawn_lifecycle_blocking(&_lifecycle_permit, move || {
                create_firecracker_metrics_fifo(Path::new(&metrics_fifo), sandbox_uid)
            })
            .await
            {
                Ok(Ok(())) => format!("/{FIRECRACKER_METRICS_FIFO}"),
                Ok(Err(error)) => {
                    tracing::warn!(sandbox_id, %error, "launching without Firecracker metrics");
                    String::new()
                }
                Err(error) => {
                    tracing::warn!(sandbox_id, %error, "launching without Firecracker metrics");
                    String::new()
                }
            }
        };
        let fc_console = if standalone {
            // The contained launcher creates and pins a fresh log inode before
            // entering the jail; node-manager supplies only its derived locator.
            format!("{jail_root}/console.log")
        } else if self.guest_console_logs {
            format!("{RUN_DIR}/{sandbox_id}/guest-console.log")
        } else {
            String::new()
        };
        let verified_runtime = verified_environment.map(|reference| VerifiedRuntimeLaunch {
            sandbox_id: sandbox_id.to_string(),
            environment_reference: reference.to_string(),
            integrity_requirement: ArtifactIntegrityRequirement::AuthenticatedImmutableSources
                .into(),
        });
        let standalone_configuration = if standalone && !req.snapshot_restore {
            Some(
                self.standalone_configuration(
                    runtime,
                    PathBuf::from(format!("{jail_root}/vm-config.json")),
                    boot_source,
                )
                .await?,
            )
        } else {
            None
        };
        let request = pplx_space_node_runtime_core::proto::LaunchRuntimeRequest {
            sandbox_id: sandbox_id.to_string(),
            execution_id: req
                .execution_fence
                .as_ref()
                .map_or_else(String::new, |f| f.execution_id.clone()),
            runtime: pplx_space_execution_common::proto::RuntimeKind::from(runtime) as i32,
            runtime_binary: firecracker_bin,
            launcher_binary: jailer_bin,
            workspace_root: chroot_base,
            network_namespace: ns_path,
            uid: sandbox_uid,
            gid: sandbox_uid,
            configuration_path: fc_config.to_owned(),
            log_path: fc_log,
            metrics_path: fc_metrics,
            console_path: fc_console,
            resource_limits: Some(self.launch_cgroup_limits(req)),
            verified_runtime,
        };
        if self
            .recorded_guest_profile(sandbox_id.to_owned())
            .await?
            .is_some_and(|profile| profile.uses_dhcp())
        {
            self.ensure_guest_dhcp(sandbox_id, req.execution_fence.as_ref())
                .await?;
        }
        let launched = self.launch_admitted_runtime(sandbox_id, request).await?;
        let fc_pid = launched.pid;
        if let Err(error) = self.pool.set_pid(sandbox_id, fc_pid) {
            tracing::error!(
                sandbox_id,
                pid = fc_pid,
                error = %error,
                "failed to persist runtime pid"
            );
            kill_launched_process_after_pid_persist_failure(self, sandbox_id, fc_pid).await;
            return Err(Status::internal(format!(
                "failed to persist runtime pid: {error}"
            )));
        }

        if let Some(configuration) = standalone_configuration
            && let Err(error) = self
                .boot_standalone(runtime, sandbox_id, control_socket.clone(), &configuration)
                .await
        {
            kill_launched_process_after_pid_persist_failure(self, sandbox_id, fc_pid).await;
            return Err(error);
        }

        // Make jail root traversable by sbx-manager so it can
        // reach v.sock for direct vsock health checks after restore.
        // Must run AFTER jailer creates the root/ dir (jailer sets it to 0700).
        if let Err(status) = run_permission_command(
            "chgrp",
            &[group_names::SPACE_SBXMGR, &jail_root],
            &jail_root,
            group_names::SPACE_SBXMGR,
            &_lifecycle_permit,
        )
        .await
        {
            kill_launched_process_after_pid_persist_failure(self, sandbox_id, fc_pid).await;
            return Err(status);
        }
        if let Err(status) = run_permission_command(
            "chmod",
            &["0750", &jail_root],
            &jail_root,
            "mode=0750",
            &_lifecycle_permit,
        )
        .await
        {
            kill_launched_process_after_pid_persist_failure(self, sandbox_id, fc_pid).await;
            return Err(status);
        }
        let api_socket_path = control_socket.display().to_string();
        if !req.snapshot_restore {
            // Make v.sock connectable by sbx-manager (for direct vsock health checks).
            // FC creates it owned by sandbox UID; set group to space-sbxmgr so
            // sbx-manager (with that primary group) can connect. Snapshot restores
            // create v.sock during load_snapshot instead, so waiting here only adds
            // a fixed 1s delay.
            let vsock_path = format!("{jail_root}/v.sock");
            let started = Instant::now();
            loop {
                let vsock_path_b = vsock_path.clone();
                let exists = spawn_lifecycle_blocking(&_lifecycle_permit, move || {
                    std::path::Path::new(&vsock_path_b).exists()
                })
                .await
                .unwrap_or(false);
                if exists {
                    if let Err(status) = run_permission_command(
                        "chgrp",
                        &[group_names::SPACE_SBXMGR, &vsock_path],
                        &vsock_path,
                        group_names::SPACE_SBXMGR,
                        &_lifecycle_permit,
                    )
                    .await
                    {
                        kill_launched_process_after_pid_persist_failure(self, sandbox_id, fc_pid)
                            .await;
                        return Err(status);
                    }
                    if let Err(status) = run_permission_command(
                        "chmod",
                        &["0770", &vsock_path],
                        &vsock_path,
                        "mode=0770",
                        &_lifecycle_permit,
                    )
                    .await
                    {
                        kill_launched_process_after_pid_persist_failure(self, sandbox_id, fc_pid)
                            .await;
                        return Err(status);
                    }
                    break;
                }
                if started.elapsed() >= FIRECRACKER_VSOCK_TIMEOUT {
                    break;
                }
                tokio::time::sleep(FIRECRACKER_VSOCK_POLL).await;
            }
        }

        if verified_environment.is_none()
            && resolved_execution.runtime_files.integrity() == ExecutionSourceIntegrity::FsVerity
        {
            warn_legacy_execution(sandbox_id);
            pplx_space_metrics::node_manager::legacy_execution_source_launch();
        }

        Ok(Response::new(LaunchSandboxResponse {
            pid: fc_pid,
            api_socket_path,
            resolved_execution: Some((&resolved_execution.execution).into()),
        }))
    }

    pub(super) async fn prepare_sandbox(
        &self,
        request: Request<PrepareSandboxRequest>,
    ) -> Result<Response<PrepareSandboxResponse>, Status> {
        self.authorize_peer(&request, "PrepareSandbox", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        validate_sandbox_id(&sandbox_id)?;
        let service = self.clone();
        self.run_owned_sandbox_work(&sandbox_id, async move {
            service.prepare_sandbox_inner(request).await
        })
        .await
    }

    async fn prepare_sandbox_inner(
        &self,
        request: Request<PrepareSandboxRequest>,
    ) -> Result<Response<PrepareSandboxResponse>, Status> {
        let req = request.get_ref();
        let sandbox_id = &req.sandbox_id;
        validate_sandbox_id(sandbox_id)?;
        if let Some(size_mib) = req.rootfs_size_mb {
            validate_rootfs_mib(size_mib)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        }
        let template = req.template.as_deref().filter(|value| !value.is_empty());
        let (runtime, requested_runtime) =
            parse_runtime_request(&req.runtime, req.requested_execution.as_ref())?;
        let selection_mode = parse_execution_selection_mode(req.execution_selection_mode)?;
        let runtime_installation = self
            .resolve_bound_execution(requested_runtime.as_ref(), runtime, selection_mode)
            .await?;
        log_resolved_execution("prepare", sandbox_id, &runtime_installation.execution);
        let metadata = sandbox_metadata(req, runtime_installation.execution.clone())?;
        let disk_layout = metadata.guest_image().disk();
        let sandbox_metadata = serde_json::to_vec_pretty(&metadata)
            .map_err(|error| Status::internal(format!("serialize sandbox metadata: {error}")))?;
        let lifecycle_permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox)
            .await?;
        let ownership = self
            .acquire_preparation_ownership(sandbox_id, req.uid, lifecycle_permit)
            .await?;
        self.check_execution(sandbox_id, req.execution_fence.as_ref(), false)?;
        // Re-check after waiting for the permit and ownership, so a policy
        // applied meanwhile still gates this preparation.
        self.execution_policy()
            .check_execution(&runtime_installation.execution, selection_mode)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let uid = ownership.lease.sandbox_uid;
        let cache_paths = prepare_sandbox_cache_paths(sandbox_id);
        let cache_dir = cache_paths.instance_dir;
        let run_dir = format!("{RUN_DIR}/{sandbox_id}");
        let rootfs_path = cache_paths.rootfs;
        let config_path = format!("{run_dir}/vm-config.json");
        let log_path = format!("{run_dir}/fc.log");
        let sandbox_cache_dir = cache_paths.sandbox_dir;
        let snapshots_dir = cache_paths.snapshots_dir;

        // Resolve template paths inline (validation only, cheap). Template
        // manager is responsible for returning a runtime-ready rootfs.
        let template_owned = template.map(|s| s.to_string());
        let master_rootfs = if template.is_some() {
            let template_dir = req.template_dir.as_deref().unwrap_or("");
            let master = validated_template_rootfs(template_dir)?;
            Some(master.to_string_lossy().into_owned())
        } else {
            None
        };
        let prepared_rootfs = super::prepared_rootfs::validate_ready_handoff(
            std::path::Path::new(&cache_dir),
            std::path::Path::new(&rootfs_path),
            req.rootfs_size_mb,
        )?;
        if req.rootfs_size_mb.is_some() && master_rootfs.is_none() && !prepared_rootfs {
            return Err(Status::invalid_argument(
                "rootfs_size_mb requires a template rootfs",
            ));
        }
        let disk_reservation = if prepared_rootfs {
            None
        } else {
            Some(
                self.disk_admission.reserve(
                    crate::disk_admission::DiskAllocationOperation::PrepareSandbox,
                    req.rootfs_size_mb
                        .map(|mib| u64::from(mib).saturating_mul(1024 * 1024)),
                )?,
            )
        };

        // Move all blocking host work (btrfs, cp --reflink, chown -R, chgrp,
        // chmod, recursive fs ops) onto the blocking pool so the Tokio runtime
        // can keep accepting and dispatching new RPCs while these run.
        let cache_dir_b = cache_dir.clone();
        let run_dir_b = run_dir.clone();
        let rootfs_path_b = rootfs_path.clone();
        let config_path_b = config_path.clone();
        let sandbox_cache_dir_b = sandbox_cache_dir.clone();
        let snapshots_dir_b = snapshots_dir.clone();
        let sandbox_metadata_path_b =
            pplx_space_common_core::host::linux::paths::sandbox_metadata_path(sandbox_id);
        let sandbox_metadata = sandbox_metadata.clone();
        let vm_config_json = req.vm_config_json.clone();
        let rootfs_size_mb = req.rootfs_size_mb;
        self.run_owned_preparation(
            ownership,
            disk_reservation,
            move || -> Result<(), Status> {
                if let Some(master) = master_rootfs.as_ref() {
                    validate_materialized_rootfs(std::path::Path::new(master))?;
                }
                // Always ensure run directory exists (needed for vm-config.json, logs, sockets)
                super::trusted_paths::create_directory(Path::new(&run_dir_b))?;
                if prepared_rootfs {
                    // A privileged local builder committed this exact file while
                    // node-manager held disk admission. The marker is the explicit
                    // handoff that permits skipping the template clone.
                    super::prepared_rootfs::require_ready_handoff(
                        std::path::Path::new(&cache_dir_b),
                        std::path::Path::new(&rootfs_path_b),
                        rootfs_size_mb,
                    )?;
                    super::trusted_paths::create_directory(Path::new(&snapshots_dir_b))?;
                } else if let (Some(_template), Some(master_rootfs)) =
                    (template_owned.as_ref(), master_rootfs)
                {
                    use super::{artifact_fs, trusted_paths};
                    let instance_path = Path::new(&cache_dir_b);
                    let parent_path = instance_path
                        .parent()
                        .ok_or_else(|| Status::internal("no instance parent"))?;
                    let parent = trusted_paths::create_directory(parent_path)?;
                    let name = instance_path
                        .file_name()
                        .ok_or_else(|| Status::internal("no instance name"))?;
                    let master = Path::new(&master_rootfs);
                    let (template, rootfs_name) = artifact_fs::parent(master)?;
                    let source = trusted_paths::open_regular_file(&template, rootfs_name)?;
                    let instance = match artifact_fs::directory(&parent, name) {
                        Ok(instance) => instance,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // Archived directory-form templates still need the
                            // plain-file fallback; both arms pin their inputs.
                            if artifact_fs::btrfs::is_subvolume(&template)
                                .map_err(artifact_fs::error)?
                            {
                                artifact_fs::btrfs::snapshot(&template, &parent, name, false)
                                    .map_err(artifact_fs::error)?;
                            } else {
                                artifact_fs::btrfs::create(&parent, name)
                                    .map_err(artifact_fs::error)?;
                            }
                            artifact_fs::directory(&parent, name).map_err(artifact_fs::error)?
                        }
                        Err(e) => return Err(artifact_fs::error(e)),
                    };
                    match nix::sys::stat::fstatat(
                        &instance,
                        "rootfs.ext4",
                        nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW,
                    ) {
                        Err(nix::errno::Errno::ENOENT) => {
                            crate::contained_copy::stage_open_file(
                                &source,
                                &instance,
                                "rootfs.ext4".as_ref(),
                            )
                            .map_err(artifact_fs::error)?;
                            instance.sync_all().map_err(artifact_fs::error)?;
                        }
                        Ok(_) => {
                            trusted_paths::open_regular_file(&instance, "rootfs.ext4".as_ref())?;
                        }
                        Err(e) => return Err(artifact_fs::error(e)),
                    }
                    trusted_paths::create_directory(Path::new(&snapshots_dir_b))?;

                    if let Some(rootfs_size_mb) = rootfs_size_mb {
                        resize_guest_disk(&rootfs_path_b, rootfs_size_mb, disk_layout)?;
                    }
                }

                if std::path::Path::new(&rootfs_path_b).exists() {
                    validate_materialized_rootfs(std::path::Path::new(&rootfs_path_b))?;
                }

                if let Some(vm_config_json) = vm_config_json {
                    use std::io::Write;
                    super::artifact_fs::write_atomic(Path::new(&config_path_b), |mut output| {
                        output
                            .write_all(vm_config_json.as_bytes())
                            .map_err(super::artifact_fs::error)
                    })?;
                }

                ensure_sandbox_artifact_dir(&sandbox_cache_dir_b)?;
                write_sandbox_metadata_atomic(
                    std::path::Path::new(&sandbox_metadata_path_b),
                    &sandbox_metadata,
                )?;

                // Chown only sandbox-owned artifacts. The sidecar run dir's
                // group and setgid bit control UDS socket reachability; sidecar
                // launch repairs its owner and group immediately before spawn.
                for path in
                    prepare_sandbox_owned_paths(&cache_dir_b, &rootfs_path_b, &config_path_b)
                {
                    if std::path::Path::new(path).exists() {
                        chown_path_blocking(path, uid, uid)?;
                    }
                }

                // Set sandbox cache dir group to space-cache-rw so sbx-manager can
                // create subdirs (e.g., snapshot/) for base-snapshot files.
                {
                    ensure_sandbox_artifact_dir_permissions(&sandbox_cache_dir_b)?;
                    // Discovery of a retained rootfs runs as sandbox-manager,
                    // independently of the VM's per-sandbox owner.
                    ensure_sandbox_instance_traversal(&cache_dir_b)?;
                    ensure_sandbox_artifact_dir_permissions(&snapshots_dir_b)?;
                }

                // Set run_dir group to space-sidecarclients with setgid so sockets
                // created by spaced-host/netgw are only reachable by sandbox-manager.
                if std::path::Path::new(&run_dir_b).exists() {
                    ensure_sidecar_run_dir_permissions(&run_dir_b)?;
                }
                Ok(())
            },
        )
        .await?;

        Ok(Response::new(PrepareSandboxResponse {
            cache_dir,
            run_dir,
            rootfs_path,
            config_path,
            log_path,
            resolved_execution: Some((&runtime_installation.execution).into()),
        }))
    }

    pub(super) async fn prepare_sandbox_artifact_dir(
        &self,
        request: Request<PrepareSandboxArtifactDirRequest>,
    ) -> Result<Response<PrepareSandboxArtifactDirResponse>, Status> {
        self.authorize_peer(
            &request,
            "PrepareSandboxArtifactDir",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        validate_sandbox_id(&sandbox_id)?;
        let sandbox_cache_dir = PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}"));
        let snapshots_dir = sandbox_cache_dir.join("snapshots");
        let _lifecycle_permit = self
            .lifecycle_limiter
            .acquire(crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox)
            .await?;

        spawn_lifecycle_blocking(&_lifecycle_permit, move || -> Result<(), Status> {
            ensure_sandbox_artifact_dir(&sandbox_cache_dir)?;
            // Retained instances can predate traversal permissions. Repair only
            // an existing, pinned directory; never create or replace its files.
            ensure_sandbox_instance_traversal(sandbox_cache_dir.join("instance"))?;
            ensure_sandbox_artifact_dir(&snapshots_dir)
        })
        .await
        .map_err(|error| {
            Status::internal(format!(
                "prepare_sandbox_artifact_dir blocking task failed: {error}"
            ))
        })??;

        Ok(Response::new(PrepareSandboxArtifactDirResponse {}))
    }
}

fn protected_launch_environment(
    kind: RuntimeKind,
    runtime: ExecutionSourceIntegrity,
    environment: Option<pplx_space_common_core::types::EnvironmentReference>,
    requested: Option<pplx_space_common_core::types::EnvironmentReference>,
    snapshot_restore: bool,
    components: impl IntoIterator<Item = ExecutionSourceIntegrity>,
) -> Result<Option<pplx_space_common_core::types::EnvironmentReference>, Status> {
    let authenticated = |integrity| match integrity {
        ExecutionSourceIntegrity::FsVerity => true,
        ExecutionSourceIntegrity::CatalogSha256 => kind == RuntimeKind::CloudHypervisor,
        ExecutionSourceIntegrity::UnverifiedExternal => false,
    };
    if !authenticated(runtime) {
        return Ok(None);
    }
    match environment {
        Some(reference) => {
            if (!snapshot_restore || kind == RuntimeKind::CloudHypervisor)
                && components
                    .into_iter()
                    .any(|integrity| !authenticated(integrity))
            {
                return Err(Status::failed_precondition(
                    "protected boot requires authenticated boot components",
                ));
            }
            Ok(Some(reference))
        }
        None if requested.is_some() => Err(Status::failed_precondition(
            "compiled execution lost its environment binding",
        )),
        None => Ok(None),
    }
}

fn resize_guest_disk(
    rootfs_path: &str,
    rootfs_size_mb: u32,
    layout: pplx_space_common_core::types::guest_image::GuestDiskLayout,
) -> Result<(), Status> {
    let target_bytes = validate_rootfs_mib(rootfs_size_mb)
        .map_err(|error| Status::invalid_argument(error.to_string()))?
        .as_bytes();
    let backing = crate::device_backing::Backing::require(
        crate::device_backing::Backing::open(Path::new(rootfs_path), true)
            .map_err(super::artifact_fs::error)?,
    )
    .map_err(super::artifact_fs::error)?;
    let current_bytes = backing
        .file
        .metadata()
        .map_err(super::artifact_fs::error)?
        .len();
    if target_bytes < current_bytes {
        return Err(Status::invalid_argument(format!(
            "rootfs_size_mb={rootfs_size_mb} is smaller than cloned rootfs size {} MiB",
            current_bytes.div_ceil(1024 * 1024)
        )));
    }
    if target_bytes == current_bytes {
        return Ok(());
    }

    backing
        .resize(target_bytes)
        .map_err(super::artifact_fs::error)?;
    if layout == pplx_space_common_core::types::guest_image::GuestDiskLayout::OpaqueRawDisk {
        return Ok(());
    }
    // These tools must see the pinned inode too, not a cache pathname which
    // could be replaced after set_len. The loop detaches when this scope ends.
    let device = crate::device_backing::LoopDevice::attach(&backing, false)
        .map_err(super::artifact_fs::error)?;

    let mut fsck = crate::host_command::Command::new("e2fsck");
    fsck.arg("-fy").arg(device.path());
    run_rootfs_resize_command("e2fsck", &mut fsck, |code| code == Some(1))?;

    let mut resize = crate::host_command::Command::new("resize2fs");
    resize.arg(device.path());
    run_rootfs_resize_command("resize2fs", &mut resize, |_| false)
}

pub(super) fn validate_materialized_rootfs(rootfs_path: &std::path::Path) -> Result<(), Status> {
    let metadata = std::fs::symlink_metadata(rootfs_path).map_err(|error| {
        Status::internal(format!(
            "cannot stat rootfs {}: {error}",
            rootfs_path.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(Status::failed_precondition("rootfs must be a regular file"));
    }
    validate_rootfs_bytes(metadata.len())
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(())
}

/// Open the plaintext launch source, `<instance_dir>/rootfs.ext4`, without
/// following links at any component; it must be a single-link regular file.
fn open_instance_rootfs(instance_dir: &Path) -> Result<std::fs::File, Status> {
    let instance = super::trusted_paths::open_directory(instance_dir)?;
    super::trusted_paths::open_regular_file(&instance, std::ffi::OsStr::new("rootfs.ext4"))
}

fn run_rootfs_resize_command(
    operation: &'static str,
    command: &mut crate::host_command::Command,
    success_code: impl FnOnce(Option<i32>) -> bool,
) -> Result<(), Status> {
    let status = crate::host_command::output(command, crate::host_command::MUTATION_BUDGET, &[])
        .map_err(|error| Status::internal(format!("{operation} failed: {error}")))?
        .status;
    if status.success() || success_code(status.code()) {
        return Ok(());
    }
    Err(Status::internal(format!(
        "{operation} failed with status {status}"
    )))
}

// Keep a per-process warning sample while counting every legacy launch.
fn warn_legacy_execution(sandbox_id: &str) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    static NEXT_WARNING: AtomicU64 = AtomicU64::new(0);
    let elapsed = START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs();
    if NEXT_WARNING
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
            (elapsed >= next).then_some(elapsed.saturating_add(60))
        })
        .is_ok()
    {
        tracing::warn!(
            sandbox_id,
            "unbound historical execution uses legacy runtime source admission (warning sampled once per minute)"
        );
    }
}

#[cfg(test)]
mod tests;

/// Create the metrics FIFO owned by the sandbox's Firecracker user, which
/// opens it read-write and non-blocking so a full pipe drops a flush rather
/// than stalling the VMM, and readable by sandbox-manager's group.
fn create_firecracker_metrics_fifo(path: &Path, sandbox_uid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let sbxmgr_gid = pplx_space_common_core::host::linux::identities::groups::SPACE_SBXMGR.gid;
    nix::unistd::mkfifo(path, nix::sys::stat::Mode::from_bits_truncate(0o600))?;
    let configured = std::os::unix::fs::lchown(path, Some(sandbox_uid), Some(sbxmgr_gid))
        .and_then(|()| std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o640)));
    if let Err(error) = configured {
        // Firecracker must not start with a FIFO it cannot open.
        if let Err(remove_error) = std::fs::remove_file(path) {
            tracing::warn!(path = %path.display(), error = %remove_error, "cannot remove unusable metrics FIFO");
        }
        return Err(error);
    }
    Ok(())
}

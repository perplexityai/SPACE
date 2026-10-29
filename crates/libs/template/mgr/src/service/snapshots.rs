use super::*;
mod retry_backoff;
use pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER;
pub(super) use retry_backoff::RetryBackoff;

pub(super) struct BuildBaseSnapshotArgs<'a> {
    pub(super) sandbox_id: &'a str,
    pub(super) template_name: &'a str,
    pub(super) content_hash: &'a str,
    pub(super) vcpu: u32,
    pub(super) memory_mib: u32,
    pub(super) ready_timeout: u32,
}

/// Outcome of a completed template base-snapshot build.
pub(super) struct BuiltBaseSnapshot {
    /// Guest RAM in MiB captured in `snapshot.base`; sbx-manager may report a
    /// value different from the requested size.
    pub(super) memory_mib: u32,
}

fn base_snapshot_start_request(
    sandbox_id: String,
    vcpu: u32,
    memory_mib: u32,
    ready_timeout: u32,
) -> pplx_space_sbx_mgr_client::proto::StartRequest {
    pplx_space_sbx_mgr_client::proto::StartRequest {
        activation_auto_lifecycle: None,
        operation_activation_attempt_id: None,
        restore_artifact_as_new: None,
        expected_local_artifact_id: None,
        sandbox_id,
        vcpu: Some(vcpu),
        memory_mib: Some(memory_mib),
        checkpoint: None,
        from_warm: false,
        blocking: true,
        netns_path: None,
        ready_timeout_secs: Some(ready_timeout),
        target_state: 0,
        suspend_restore_storage_uri: None,
        suspend_restore_source_node: None,
        route_identity: None,
    }
}

fn base_snapshot_create_request(
    template_name: &str,
    content_hash: &str,
    snapshot_rootfs_size_mb: Option<u32>,
    runtime: pplx_space_common_core::types::RuntimeKind,
) -> pplx_space_sbx_mgr_client::proto::CreateRequest {
    pplx_space_sbx_mgr_client::proto::CreateRequest {
        environment: None,
        template: template_name.to_string(),
        owner: TEMPLATE_MANAGER_SANDBOX_OWNER.to_string(),
        runtime: runtime.alias().into(),
        snapshots_enabled: false,
        autostart: false,
        pool_template: None,
        volume_encryption: false,
        boot_mode: pplx_space_sbx_mgr_client::proto::BootMode::Cold.into(),
        blocking: false,
        sandbox_id: None,
        content_hash: Some(content_hash.to_string()),
        env: Default::default(),
        auto_lifecycle: None,
        rootfs_size_mb: snapshot_rootfs_size_mb,
        disk_io_max_operations_per_sec: None,
        disk_io_max_bandwidth_mib_per_sec: None,
        network_policy: None,
        network_policy_generation: 0,
        network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
        template_network_policy: None,
        template_network_policy_generation: 0,
        template_network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
        route_identity: None,
    }
}

impl TemplateManagerService {
    /// Resolve a template reference to (name, content_hash).
    /// Accepts name + optional content_hash (tag is accepted but ignored).
    /// Returns NOT_FOUND if unresolvable.
    ///
    /// Every successful resolution advances `last_used_at` on the
    /// matched build, under the same DB lock, so the on-node LRU
    /// eviction loop sees real usage. The touch is also what keeps a
    /// build just handed to a sandbox create from being evicted before
    /// sandbox-manager records that sandbox: the GC's grace period and
    /// `DeleteIdleTemplate`'s idle bound both read it. Touch failures are
    /// logged but do **not** fail the resolution, so a failed touch can
    /// leave the build open to either eviction.
    ///
    /// Builds in `state='deleting'` (mid-eviction; see
    /// `eviction::execute_precheck`) are treated as not-resolvable
    /// — sandbox-create against a doomed build would race the
    /// disk delete and fail with a missing rootfs. Returning
    /// `NotFound` instead lets the caller fail fast with a clean
    /// error.
    pub(super) fn resolve_template(
        conn: &rusqlite::Connection,
        name: &str,
        content_hash: &str,
        _tag: &str,
    ) -> Result<(String, String), Status> {
        // Exact hash takes precedence
        if !content_hash.is_empty() {
            let row = crate::db::get_build(conn, name, content_hash)
                .map_err(|e| Status::internal(format!("db get_build: {e}")))?
                .filter(|row| row.state == "ready")
                .ok_or_else(|| {
                    Status::not_found(format!("build not found: {name}@{content_hash}"))
                })?;
            validate_content_hash(&row.content_hash)?;
            touch_lru(conn, &row.name, &row.content_hash);
            return Ok((row.name, row.content_hash));
        }

        // Find the newest build for this template name.
        // `get_latest_build` already filters `state='ready'`, so a
        // mid-eviction build is invisible to the latest-build path.
        let build = crate::db::get_latest_build(conn, name)
            .map_err(|e| Status::internal(format!("db get_latest_build: {e}")))?
            .ok_or_else(|| Status::not_found(format!("no builds found for '{name}'")))?;
        validate_content_hash(&build.content_hash)?;
        touch_lru(conn, &build.name, &build.content_hash);

        Ok((build.name, build.content_hash))
    }

    /// Inner build logic used by desired base-snapshot reconciliation.
    /// Writes snapshot to the versioned snapshot dir.
    pub(super) async fn do_build_base_snapshot(
        &self,
        sbx_client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
        args: BuildBaseSnapshotArgs<'_>,
    ) -> Result<BuiltBaseSnapshot, Status> {
        let BuildBaseSnapshotArgs {
            sandbox_id,
            template_name,
            content_hash,
            vcpu,
            memory_mib,
            ready_timeout,
        } = args;

        // 2. Start the sandbox and block until the guest reports ready.
        sbx_client
            .start(base_snapshot_start_request(
                sandbox_id.to_string(),
                vcpu,
                memory_mib,
                ready_timeout,
            ))
            .await
            .map_err(|e| Status::internal(format!("sbx start: {e}")))?;

        let app_check = sbx_client
            .wait_template_apps(pplx_space_sbx_mgr_client::proto::WaitTemplateAppsRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await
            .map_err(|e| Status::internal(format!("snapshot app check: {e}")))?
            .into_inner();
        let app_warning = app_check.warnings.join("; ");
        if !app_warning.is_empty() {
            tracing::warn!(template = template_name, content_hash, warning = %app_warning,
                "capturing base snapshot with best-effort app warning");
        }

        // 3. Pause the VM
        sbx_client
            .pause(pplx_space_sbx_mgr_client::proto::PauseRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await
            .map_err(|e| Status::internal(format!("sbx pause: {e}")))?;

        // 4. Ask sbx-manager to authorize and create the template base snapshot.
        let snapshot_resp = sbx_client
            .create_template_base_snapshot(
                pplx_space_sbx_mgr_client::proto::CreateTemplateBaseSnapshotRequest {
                    sandbox_id: sandbox_id.to_string(),
                    vcpu,
                    memory_mib,
                },
            )
            .await
            .map_err(|e| Status::internal(format!("create template base snapshot: {e}")))?;
        let snapshot_resp = snapshot_resp.into_inner();
        let snap_hash = snapshot_resp.snapshot_hash;
        let snapshot_memory_mib = if snapshot_resp.memory_mib == 0 {
            memory_mib
        } else {
            snapshot_resp.memory_mib
        };
        if snapshot_memory_mib != memory_mib {
            tracing::info!(
                template = %template_name,
                content_hash,
                requested_snapshot_memory_mib = memory_mib,
                actual_snapshot_memory_mib = snapshot_memory_mib,
                "template base snapshot memory differed from request"
            );
        }

        // 5. Verify sbx-manager created the canonical memory artifact.
        let snap_dir = snapshot_dir(template_name, content_hash, vcpu, snapshot_memory_mib);
        let snapshot_base = format!("{snap_dir}/snapshot.base");
        std::fs::metadata(&snapshot_base)
            .map_err(|e| Status::internal(format!("stat snapshot.base {snapshot_base}: {e}")))?;

        // 6. Publish only after required apps passed. Best-effort failures are
        // logged above. User lifecycle readiness stays agent-only.
        {
            let db = Arc::clone(&self.db);
            let name = template_name.to_string();
            let hash = content_hash.to_string();
            let sh = snap_hash;
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| crate::db::mutex_poison_error("base snapshot upsert"))?;
                crate::db::upsert_snapshot(&conn, &name, &hash, vcpu, snapshot_memory_mib, &sh)
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("db upsert_snapshot: {e}")))?;
        }
        crate::node_state_sync::mark_template_dirty(&self.db, template_name);

        // 7. Invalidate existing pool sandboxes for this template
        {
            let sbx_connect = pplx_space_sbx_mgr_client::connect()
                .await
                .map_err(|e| e.to_string());
            if let Ok(mut pool_sbx_client) = sbx_connect
                && let Ok(sandboxes) =
                    pplx_space_sbx_mgr_client::list_all_sandboxes(&mut pool_sbx_client).await
            {
                for sbx in sandboxes {
                    if sbx.pool_template.as_deref() == Some(template_name) {
                        let _ = pool_sbx_client
                            .stop(pplx_space_sbx_mgr_client::proto::StopRequest {
                                sandbox_id: sbx.sandbox_id.clone(),
                                template_filter: None,
                                blocking: true,
                                immediate_kill: false,
                            })
                            .await;
                        let _ = pool_sbx_client
                            .delete(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                                sandbox_id: sbx.sandbox_id,
                                template_filter: None,
                                blocking: true,
                            })
                            .await;
                    }
                }
            }
            tracing::debug!(
                template = %template_name,
                "invalidated pool sandboxes after base snapshot rebuild"
            );
        }

        Ok(BuiltBaseSnapshot {
            memory_mib: snapshot_memory_mib,
        })
    }

    pub(super) async fn build_base_snapshot_for_resolved(
        &self,
        template_name: &str,
        content_hash: &str,
        vcpu: u32,
        memory_mib: u32,
        ready_timeout: u32,
    ) -> Result<BuiltBaseSnapshot, Status> {
        let requested_memory_mib = memory_mib;
        let resolved_memory =
            resolve_sandbox_memory(memory_mib, self.sbx_connect_retry_policy).await?;
        let memory_mib = resolved_memory.effective_memory_mib;
        if memory_mib < requested_memory_mib {
            tracing::info!(
                template = %template_name,
                content_hash,
                requested_snapshot_memory_mib = resolved_memory.requested_memory_mib,
                effective_snapshot_memory_mib = memory_mib,
                sandbox_guest_memory_max_mib = resolved_memory.guest_memory_max_mib,
                sandbox_cgroup_memory_mib = resolved_memory.cgroup_memory_mib,
                sandbox_cgroup_memory_max_mib = resolved_memory.cgroup_memory_max_mib,
                sandbox_memory_headroom_mib = resolved_memory.memory_headroom_mib,
                "clamped template base snapshot build memory to sandbox policy"
            );
        }
        let bdir = build_dir(template_name, content_hash);
        if !std::path::Path::new(&format!("{bdir}/rootfs.ext4")).exists() {
            return Err(Status::not_found(format!(
                "template rootfs not found: {bdir}/rootfs.ext4"
            )));
        }

        // Read the template's runtime disk size from metadata. The base
        // snapshot's cold-boot sandbox needs to be CoW-cloned and resized to
        // this size before it boots — otherwise the snapshot bakes in
        // whatever shape the digest_dir rootfs has on disk (compact post-
        // register, expanded post-pull), and pool sandboxes restored from a
        // compact-shaped snapshot are too small for runtime workloads.
        let snapshot_rootfs_size_mb =
            crate::rootfs_cache::rootfs_size_mb_from_metadata(std::path::Path::new(&bdir))
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "read disk_size_mb for {template_name}@{content_hash}: {error}"
                    ))
                })?;

        let runtime = {
            let db = Arc::clone(&self.db);
            let name = template_name.to_owned();
            let hash = content_hash.to_owned();
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| Status::internal("template DB mutex poisoned"))?;
                let build = crate::db::get_build(&conn, &name, &hash)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .ok_or_else(|| Status::not_found("base snapshot template build disappeared"))?;
                build
                    .runtime
                    .parse::<pplx_space_common_core::types::RuntimeKind>()
                    .map_err(|error| Status::failed_precondition(error.to_string()))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))??
        };

        let mut sbx_client = pplx_space_sbx_mgr_client::connect()
            .await
            .map_err(|e| Status::internal(format!("cannot connect to sbx-manager: {e}")))?;

        // Build the base snapshot from a cold boot of the template rootfs.
        // Pool sandboxes can still start from this snapshot; the snapshot
        // builder itself must not snapshot a guest restored from an older
        // snapshot, or stale guest/sidecar state can get baked back in.
        let create_resp = sbx_client
            .create(base_snapshot_create_request(
                template_name,
                content_hash,
                snapshot_rootfs_size_mb,
                runtime,
            ))
            .await
            .map_err(|e| Status::internal(format!("sbx create: {e}")))?;
        let sandbox_id = create_resp.into_inner().sandbox_id;

        let build_timeout = std::time::Duration::from_secs(
            u64::from(ready_timeout).saturating_add(BASE_SNAPSHOT_BUILD_TIMEOUT_GRACE_SECS),
        );
        let result = match tokio::time::timeout(
            build_timeout,
            self.do_build_base_snapshot(
                &mut sbx_client,
                BuildBaseSnapshotArgs {
                    sandbox_id: &sandbox_id,
                    template_name,
                    content_hash,
                    vcpu,
                    memory_mib,
                    ready_timeout,
                },
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(Status::deadline_exceeded(format!(
                "template base snapshot build timed out after {}s",
                build_timeout.as_secs()
            ))),
        };

        // Always stop and delete the temporary sandbox.
        let cleanup_timeout =
            std::time::Duration::from_secs(BASE_SNAPSHOT_BUILD_CLEANUP_TIMEOUT_SECS);
        match tokio::time::timeout(
            cleanup_timeout,
            sbx_client.stop(pplx_space_sbx_mgr_client::proto::StopRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
                immediate_kill: false,
            }),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "failed to stop temporary base snapshot sandbox"
                );
            }
            Err(_) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    timeout_secs = BASE_SNAPSHOT_BUILD_CLEANUP_TIMEOUT_SECS,
                    "timed out stopping temporary base snapshot sandbox"
                );
            }
        }
        match tokio::time::timeout(
            cleanup_timeout,
            sbx_client.delete(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
            }),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "failed to delete temporary base snapshot sandbox"
                );
            }
            Err(_) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    timeout_secs = BASE_SNAPSHOT_BUILD_CLEANUP_TIMEOUT_SECS,
                    "timed out deleting temporary base snapshot sandbox"
                );
            }
        }

        result
    }

    pub(super) fn snapshot_files_exist_in_dir(dir: &str) -> bool {
        std::path::Path::new(&format!("{dir}/snapshot.base")).exists()
            && std::path::Path::new(&format!("{dir}/vmstate.base")).exists()
    }

    pub(super) fn snapshot_files_exist(
        template_name: &str,
        content_hash: &str,
        vcpu: u32,
        memory_mib: u32,
    ) -> bool {
        let dir = snapshot_dir(template_name, content_hash, vcpu, memory_mib);
        Self::snapshot_files_exist_in_dir(&dir)
    }

    pub(super) async fn delete_base_snapshots_for_template(
        &self,
        template: &str,
    ) -> Result<bool, Status> {
        validate_template_name(template)?;

        let db = Arc::clone(&self.db);
        let template_for_db = template.to_string();
        let db_had_snapshots = tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| Status::internal("db mutex poisoned while deleting base snapshots"))?;
            let builds = crate::db::list_builds_by_name(&conn, &template_for_db)
                .map_err(|e| Status::internal(format!("db list_builds_by_name: {e}")))?;
            let mut had_snapshots = false;
            for build in builds {
                let snapshots = crate::db::list_snapshots(&conn, &build.name, &build.content_hash)
                    .map_err(|e| Status::internal(format!("db list_snapshots: {e}")))?;
                had_snapshots |= !snapshots.is_empty();
                crate::db::delete_snapshots_for_build(&conn, &build.name, &build.content_hash)
                    .map_err(|e| Status::internal(format!("db delete_snapshots_for_build: {e}")))?;
            }
            Ok::<_, Status>(had_snapshots)
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;

        let snapshots_base = format!("{TEMPLATE_DIR}/{template}/snapshots");
        let disk_had_snapshots = std::path::Path::new(&snapshots_base).exists();
        if disk_had_snapshots {
            std::fs::remove_dir_all(&snapshots_base)
                .map_err(|e| Status::internal(format!("remove snapshot dir: {e}")))?;
        }

        let changed = db_had_snapshots || disk_had_snapshots;
        if changed {
            crate::node_state_sync::mark_template_dirty(&self.db, template);
        }
        Ok(changed)
    }

    pub(super) async fn prune_base_snapshots_except(
        &self,
        template: &str,
        keep_name: &str,
        keep_content_hash: &str,
        keep_vcpu: u32,
        keep_memory_mib: u32,
    ) -> Result<bool, Status> {
        validate_template_name(template)?;

        let db = Arc::clone(&self.db);
        let keep_name = keep_name.to_string();
        let keep_content_hash = keep_content_hash.to_string();
        let stale_snapshots = tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| Status::internal("db mutex poisoned while listing stale snapshots"))?;
            let mut stale_snapshots = Vec::new();
            let snapshots = crate::db::list_snapshots(&conn, &keep_name, &keep_content_hash)
                .map_err(|e| Status::internal(format!("db list_snapshots: {e}")))?;
            for snapshot in snapshots {
                if snapshot.vcpu == keep_vcpu && snapshot.memory_mib == keep_memory_mib {
                    continue;
                }
                stale_snapshots.push((
                    snapshot.name,
                    snapshot.content_hash,
                    snapshot.vcpu,
                    snapshot.memory_mib,
                ));
            }
            Ok::<_, Status>(stale_snapshots)
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;

        let mut pruned = false;
        for (name, content_hash, vcpu, memory_mib) in stale_snapshots {
            let dir = snapshot_dir(&name, &content_hash, vcpu, memory_mib);
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(Status::internal(format!(
                        "remove stale snapshot dir {dir}: {err}"
                    )));
                }
            }

            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || {
                let conn = db.lock().map_err(|_| {
                    Status::internal("db mutex poisoned while deleting stale snapshot")
                })?;
                crate::db::delete_snapshot(&conn, &name, &content_hash, vcpu, memory_mib)
                    .map_err(|e| Status::internal(format!("db delete_snapshot: {e}")))
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;
            pruned = true;
        }

        if pruned {
            crate::node_state_sync::mark_template_dirty(&self.db, template);
        }
        Ok(pruned)
    }

    pub(super) async fn reconcile_desired_base_snapshot(
        &self,
        template: &str,
    ) -> SnapshotReconcileOutcome {
        let template = template.to_string();
        let reconcile_lock = {
            let mut locks = match self.snapshot_reconcile_locks.lock() {
                Ok(locks) => locks,
                Err(_error) => {
                    return SnapshotReconcileOutcome {
                        error: Some(
                            "snapshot reconcile lock map mutex poisoned while starting reconcile"
                                .to_string(),
                        ),
                        ..Default::default()
                    };
                }
            };
            // Map-only Arcs are idle; active reconciles hold their own clone.
            locks.retain(|_, lock| Arc::strong_count(lock) > 1);
            Arc::clone(
                locks
                    .entry(template.clone())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        let reconcile_guard = match reconcile_lock.try_lock_owned() {
            Ok(guard) => guard,
            Err(_) => {
                tracing::debug!(
                    template = %template,
                    "desired base snapshot reconcile already in progress"
                );
                return SnapshotReconcileOutcome {
                    in_progress: true,
                    ..Default::default()
                };
            }
        };

        let settings = {
            let db = Arc::clone(&self.db);
            let template = template.clone();
            match tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| crate::db::mutex_poison_error("load snapshot settings"))?;
                crate::db::get_template_settings(&conn, &template)
            })
            .await
            {
                Ok(Ok(settings)) => settings,
                Ok(Err(err)) => {
                    return SnapshotReconcileOutcome {
                        error: Some(format!("db get_template_settings: {err}")),
                        ..Default::default()
                    };
                }
                Err(err) => {
                    return SnapshotReconcileOutcome {
                        error: Some(format!("spawn_blocking: {err}")),
                        ..Default::default()
                    };
                }
            }
        };

        let Some(settings) = settings else {
            return SnapshotReconcileOutcome::default();
        };

        if settings.base_snapshot_policy_set_at.is_none() {
            return SnapshotReconcileOutcome::default();
        }

        if !settings.ensure_base_snapshot {
            match self.delete_base_snapshots_for_template(&template).await {
                Ok(snapshot_deleted) => {
                    self.record_snapshot_reconcile_result(&template, None).await;
                    return SnapshotReconcileOutcome {
                        reconciled: true,
                        snapshot_deleted,
                        ..Default::default()
                    };
                }
                Err(err) => {
                    let message = err.message().to_string();
                    self.record_snapshot_reconcile_result(&template, Some(&message))
                        .await;
                    return SnapshotReconcileOutcome {
                        reconciled: true,
                        error: Some(message),
                        ..Default::default()
                    };
                }
            }
        }

        let resolved_memory = match resolve_sandbox_memory(
            settings.snapshot_memory_mib,
            self.sbx_connect_retry_policy,
        )
        .await
        {
            Ok(memory) => memory,
            Err(err) => {
                let message = err.message().to_string();
                self.record_snapshot_reconcile_result(&template, Some(&message))
                    .await;
                return SnapshotReconcileOutcome {
                    reconciled: true,
                    error: Some(message),
                    ..Default::default()
                };
            }
        };
        let snapshot_vcpu = settings.snapshot_vcpu;
        let snapshot_memory_mib = resolved_memory.effective_memory_mib;
        if snapshot_memory_mib < settings.snapshot_memory_mib {
            tracing::info!(
                template = %template,
                requested_snapshot_memory_mib = resolved_memory.requested_memory_mib,
                effective_snapshot_memory_mib = snapshot_memory_mib,
                sandbox_guest_memory_max_mib = resolved_memory.guest_memory_max_mib,
                sandbox_cgroup_memory_mib = resolved_memory.cgroup_memory_mib,
                sandbox_cgroup_memory_max_mib = resolved_memory.cgroup_memory_max_mib,
                sandbox_memory_headroom_mib = resolved_memory.memory_headroom_mib,
                "clamped template base snapshot memory to sandbox policy"
            );
        }

        let resolved = {
            let db = Arc::clone(&self.db);
            let template = template.clone();
            tokio::task::spawn_blocking(move || {
                let conn = db.lock().map_err(|_| {
                    Status::internal("db mutex poisoned while resolving desired snapshot")
                })?;
                // Transitional M3a behavior: desired snapshot policy follows
                // this node's local latest build until cluster-pinned template
                // hashes/aliases land and the caller can pass an exact hash.
                let (name, content_hash) = Self::resolve_template(&conn, &template, "", "latest")?;
                let snapshot = crate::db::get_snapshot(
                    &conn,
                    &name,
                    &content_hash,
                    snapshot_vcpu,
                    snapshot_memory_mib,
                )
                .map_err(|e| Status::internal(format!("db get_snapshot: {e}")))?;
                Ok::<_, Status>((name, content_hash, snapshot))
            })
            .await
        };

        let (name, content_hash, snapshot) = match resolved {
            Ok(Ok(resolved)) => resolved,
            Ok(Err(err)) => {
                let message = err.message().to_string();
                self.record_snapshot_reconcile_result(&template, Some(&message))
                    .await;
                return SnapshotReconcileOutcome {
                    reconciled: true,
                    error: Some(message),
                    ..Default::default()
                };
            }
            Err(err) => {
                let message = format!("spawn_blocking: {err}");
                self.record_snapshot_reconcile_result(&template, Some(&message))
                    .await;
                return SnapshotReconcileOutcome {
                    reconciled: true,
                    error: Some(message),
                    ..Default::default()
                };
            }
        };

        let snapshot_ok = snapshot.is_some()
            && Self::snapshot_files_exist(&name, &content_hash, snapshot_vcpu, snapshot_memory_mib);
        if snapshot_ok {
            let snapshot_deleted = match self
                .prune_base_snapshots_except(
                    &template,
                    &name,
                    &content_hash,
                    snapshot_vcpu,
                    snapshot_memory_mib,
                )
                .await
            {
                Ok(pruned) => pruned,
                Err(err) => {
                    let message = err.message().to_string();
                    self.record_snapshot_reconcile_result(&template, Some(&message))
                        .await;
                    return SnapshotReconcileOutcome {
                        reconciled: true,
                        content_hash: Some(content_hash),
                        error: Some(message),
                        ..Default::default()
                    };
                }
            };
            self.record_snapshot_reconcile_result(&template, None).await;
            return SnapshotReconcileOutcome {
                reconciled: true,
                snapshot_deleted,
                content_hash: Some(content_hash),
                ..Default::default()
            };
        }

        let retry_allowed = match self.snapshot_retry_backoff.lock() {
            Ok(mut backoff) => backoff.allows(&name, &content_hash, std::time::Instant::now()),
            Err(_) => {
                return SnapshotReconcileOutcome {
                    error: Some("snapshot backoff mutex poisoned".into()),
                    ..Default::default()
                };
            }
        };
        if !retry_allowed {
            return SnapshotReconcileOutcome {
                content_hash: Some(content_hash),
                error: Some("snapshot preparation retry deferred after failure (five-minute per-digest backoff)".into()),
                ..Default::default()
            };
        }
        {
            let db = Arc::clone(&self.db);
            let template_to_record = template.clone();
            let recorded = tokio::task::spawn_blocking(move || {
                let conn = db.lock().map_err(|_| {
                    crate::db::mutex_poison_error("record snapshot reconcile started")
                })?;
                crate::db::record_snapshot_reconcile_started(&conn, &template_to_record)
            })
            .await;
            if !matches!(recorded, Ok(Ok(()))) {
                tracing::warn!(template, "could not record snapshot reconcile start");
            }
        }

        let service = self.clone_for_snapshot_reconcile();
        let task_template = template.clone();
        let task_name = name.clone();
        let task_content_hash = content_hash.clone();
        let build_task = tokio::spawn(async move {
            let _reconcile_guard = reconcile_guard;
            service
                .run_base_snapshot_reconcile_build(
                    task_template,
                    task_name,
                    task_content_hash,
                    snapshot_vcpu,
                    snapshot_memory_mib,
                    BASE_SNAPSHOT_BUILD_READY_TIMEOUT_SECS,
                )
                .await
        });

        match build_task.await {
            Ok(outcome) => outcome,
            Err(err) => {
                self.defer_failed_snapshot(&template, &content_hash);
                let message = format!("base snapshot reconcile task failed: {err}");
                self.record_snapshot_reconcile_result(&template, Some(&message))
                    .await;
                SnapshotReconcileOutcome {
                    reconciled: true,
                    content_hash: Some(content_hash),
                    error: Some(message),
                    ..Default::default()
                }
            }
        }
    }

    async fn run_base_snapshot_reconcile_build(
        &self,
        template: String,
        name: String,
        content_hash: String,
        snapshot_vcpu: u32,
        snapshot_memory_mib: u32,
        ready_timeout: u32,
    ) -> SnapshotReconcileOutcome {
        let result = self
            .build_base_snapshot_for_resolved(
                &name,
                &content_hash,
                snapshot_vcpu,
                snapshot_memory_mib,
                ready_timeout,
            )
            .await;
        match result {
            Ok(built) => {
                let built_memory_mib = built.memory_mib;
                let snapshot_deleted = match self
                    .prune_base_snapshots_except(
                        &template,
                        &name,
                        &content_hash,
                        snapshot_vcpu,
                        built_memory_mib,
                    )
                    .await
                {
                    Ok(pruned) => pruned,
                    Err(err) => {
                        let message = err.message().to_string();
                        self.record_snapshot_reconcile_result(&template, Some(&message))
                            .await;
                        return SnapshotReconcileOutcome {
                            reconciled: true,
                            snapshot_built: true,
                            content_hash: Some(content_hash),
                            error: Some(message),
                            ..Default::default()
                        };
                    }
                };
                self.record_snapshot_reconcile_result(&template, None).await;
                SnapshotReconcileOutcome {
                    reconciled: true,
                    snapshot_built: true,
                    snapshot_deleted,
                    content_hash: Some(content_hash),
                    ..Default::default()
                }
            }
            Err(err) => {
                let message = err.message().to_string();
                self.defer_failed_snapshot(&name, &content_hash);
                tracing::warn!(template, content_hash, error = %message,
                    "base snapshot preparation failed; cold boot remains available");
                self.record_snapshot_reconcile_result(&template, Some(&message))
                    .await;
                SnapshotReconcileOutcome {
                    reconciled: true,
                    content_hash: Some(content_hash),
                    error: Some(message),
                    ..Default::default()
                }
            }
        }
    }

    fn defer_failed_snapshot(&self, name: &str, hash: &str) {
        match self.snapshot_retry_backoff.lock() {
            Ok(mut backoff) => backoff.failed(name, hash, std::time::Instant::now()),
            Err(_) => tracing::error!(
                "snapshot backoff mutex poisoned; further preparations will be refused"
            ),
        }
    }

    pub(super) async fn record_snapshot_reconcile_result(
        &self,
        template: &str,
        error: Option<&str>,
    ) {
        let db = Arc::clone(&self.db);
        let template = template.to_string();
        let error = error.map(str::to_string);
        let _ = tokio::task::spawn_blocking(move || {
            let conn = db.lock().map_err(|_| {
                crate::db::mutex_poison_error("record snapshot reconcile completed")
            })?;
            crate::db::record_snapshot_reconcile_completed(&conn, &template, error.as_deref())
        })
        .await;
    }
}

/// Advance the LRU clock on a resolved build. Logged on failure but
/// never surfaced to the caller: the resolution succeeded. Eviction's
/// recency guards read this column (see `resolve_template`), so a failed
/// touch can let them evict the build.
fn touch_lru(conn: &rusqlite::Connection, name: &str, content_hash: &str) {
    if let Err(err) = crate::db::touch_build(conn, name, content_hash) {
        tracing::warn!(
            template = %name,
            content_hash = %content_hash,
            %err,
            "failed to advance template LRU clock; eviction may pick this build sooner than expected"
        );
    }
}

#[cfg(test)]
mod route_identity_tests;

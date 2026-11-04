use super::*;

impl TemplateManagerService {
    pub(super) async fn delete_template(
        &self,
        request: Request<DeleteTemplateRequest>,
    ) -> Result<Response<DeleteTemplateResponse>, Status> {
        self.authorize_peer(
            &request,
            "DeleteTemplate",
            ALLOW_TEMPLATE_DESIRED_STATE_OPERATOR,
        )?;
        let template = &request.get_ref().template;
        validate_template_name(template)?;
        let publish_node_state = !crate::node_state_sync::should_skip_node_publish(&request);

        if !self.artifact_store.template_exists(template) {
            if !self.template_local_state_exists(template).await? {
                return Err(Status::not_found(format!("template not found: {template}")));
            }
            tracing::info!(
                template,
                "template artifacts already missing during delete; completing local DB cleanup"
            );
            self.delete_template_local_state(template).await?;
            if publish_node_state {
                crate::node_state_sync::mark_template_dirty(&self.db, template);
            }
            return Ok(Response::new(DeleteTemplateResponse {}));
        }

        ensure_no_dependent_sandboxes_for_template_builds(
            TemplateBuildDeleteOperation::Template,
            DependentSandboxScope::ExcludePoolMembers,
            TemplateBuildDeleteTarget::Template { name: template },
        )
        .await?;

        // 1. Delete pool and pool sandboxes
        let delete_pool_req = internal_request(DeletePoolRequest {
            template: template.clone(),
        });
        let _ = self.delete_pool(delete_pool_req).await;

        ensure_no_dependent_sandboxes_for_template_builds(
            TemplateBuildDeleteOperation::Template,
            DependentSandboxScope::All,
            TemplateBuildDeleteTarget::Template { name: template },
        )
        .await?;

        self.delete_template_after_dependency_checks(template)
            .await?;

        if publish_node_state {
            crate::node_state_sync::mark_template_dirty(&self.db, template);
        }

        Ok(Response::new(DeleteTemplateResponse {}))
    }

    /// Delete a template the cluster no longer wants on this node, without
    /// racing a sandbox create that is about to use it: builds go through the
    /// GC's eviction with an idle bound instead of `DeleteTemplate`'s
    /// whole-tree removal (see [`crate::eviction::evict_idle_template`]).
    pub(super) async fn delete_idle_template(
        &self,
        request: Request<crate::proto::DeleteIdleTemplateRequest>,
    ) -> Result<Response<crate::proto::DeleteIdleTemplateResponse>, Status> {
        self.authorize_peer(
            &request,
            "DeleteIdleTemplate",
            ALLOW_TEMPLATE_DESIRED_STATE_OPERATOR,
        )?;
        let publish_node_state = !crate::node_state_sync::should_skip_node_publish(&request);
        let crate::proto::DeleteIdleTemplateRequest {
            template,
            idle_secs,
        } = request.into_inner();
        validate_template_name(&template)?;
        if idle_secs == 0 {
            return Err(Status::invalid_argument("idle_secs must be positive"));
        }
        let touched_before_secs = jiff::Timestamp::now()
            .as_second()
            .max(0)
            .cast_unsigned()
            .saturating_sub(idle_secs);

        // Pool members keep the builds referenced, and each refill touches
        // its build, so the pool goes first, every time: `DeletePool` also
        // stops members a failed earlier teardown left behind. The eviction
        // below then refuses until the pool's last touch is older than the
        // idle period.
        self.delete_pool(internal_request(DeletePoolRequest {
            template: template.clone(),
        }))
        .await?;

        let dependency_error = |error: String| {
            Status::unavailable(format!(
                "cannot check what protects template {template} before deleting it: {error}"
            ))
        };
        let active = fetch_build_referencing_sandbox_keys()
            .await
            .map_err(dependency_error)?;
        let warm_pool = fetch_warm_pool_protected_keys()
            .await
            .map_err(dependency_error)?;
        // Spawned, so a caller that stops waiting cannot interrupt the
        // eviction between a build's `deleting` swap and its final row delete.
        let eviction = tokio::spawn({
            let db = Arc::clone(&self.db);
            let gate = self.eviction_gate.clone();
            let name = template.clone();
            let disk = crate::eviction::EvictionDisk {
                delete: gc_delete_disk_fn(self.artifact_store.clone()),
                probe: gc_reaper_probe_fn(self.artifact_store.clone()),
            };
            async move {
                crate::eviction::evict_idle_template(
                    db,
                    &gate,
                    &name,
                    touched_before_secs,
                    active,
                    warm_pool,
                    disk,
                )
                .await
            }
        });
        let outcome = eviction
            .await
            .map_err(|e| blocking_join_status("evict idle template", &template, e))?
            .map_err(|e| Status::internal(format!("evict idle template {template}: {e}")))?;

        // `DeletePool` publishes its own change; this publishes evicted builds.
        let (changed, result) = match outcome {
            crate::eviction::IdleTemplateEviction::Evicted => (true, Ok(())),
            crate::eviction::IdleTemplateEviction::NotFound => (
                false,
                Err(Status::not_found(format!("template not found: {template}"))),
            ),
            crate::eviction::IdleTemplateEviction::Busy => (
                false,
                Err(Status::aborted(format!(
                    "template cache eviction is running; retry deleting {template} later"
                ))),
            ),
            crate::eviction::IdleTemplateEviction::Kept { evicted, reason } => (
                evicted > 0,
                Err(Status::failed_precondition(format!(
                    "kept template {template}: {reason}"
                ))),
            ),
        };
        if changed && publish_node_state {
            crate::node_state_sync::mark_template_dirty(&self.db, &template);
        }
        result.map(|()| Response::new(crate::proto::DeleteIdleTemplateResponse {}))
    }

    async fn template_local_state_exists(&self, template: &str) -> Result<bool, Status> {
        let db = Arc::clone(&self.db);
        let name = template.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| Status::internal("db mutex poisoned while checking template"))?;
            crate::db::template_local_state_exists(&conn, &name)
                .map_err(|e| Status::internal(format!("db template_local_state_exists: {e}")))
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
    }

    pub(super) async fn delete_template_after_dependency_checks(
        &self,
        template: &str,
    ) -> Result<(), Status> {
        self.remove_template_artifacts_subvolume_aware(template)
            .await?;

        // Remove all DB entries for this template name
        self.delete_template_local_state(template).await?;

        Ok(())
    }

    /// Remove a template's whole on-disk tree ahead of its DB cleanup.
    ///
    /// `<root>/<name>` holds one digest dir per build, and installed
    /// builds are read-only btrfs subvolumes — recursing into them with
    /// plain `remove_dir_all` fails with EROFS exactly like the
    /// build-level delete. Route every on-disk subvolume through the
    /// build-level subvolume-aware delete first, then remove the
    /// remaining plain tree (snapshot dirs, legacy plain-dir digests,
    /// stray files).
    ///
    /// Disk, not the DB, is the source of truth for which digest dirs
    /// need the subvolume path: a build whose row was already cleaned
    /// up (or whose earlier delete crashed between disk and DB work)
    /// still leaves a subvolume that the plain tree removal below
    /// cannot delete.
    async fn remove_template_artifacts_subvolume_aware(
        &self,
        template: &str,
    ) -> Result<(), Status> {
        let template_dir = template_artifacts_dir(&self.artifact_store.root, template);
        let subvolumes: Vec<std::path::PathBuf> = tokio::task::spawn_blocking({
            let template_dir = template_dir.clone();
            move || {
                let Ok(entries) = std::fs::read_dir(&template_dir) else {
                    // Missing template dir: nothing on disk to remove.
                    return Vec::new();
                };
                entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| pplx_space_common_core::host::btrfs::is_subvolume(path))
                    .collect()
            }
        })
        .await
        .map_err(|e| blocking_join_status("enumerate digest subvolumes", template, e))?;

        // One node-mgr budget for the whole template delete: the loop
        // is serial, and a per-subvolume budget would multiply to
        // N × NODEMGR_SUBVOLUME_DELETE_TIMEOUT with node-manager down,
        // blowing the caller's ~10 s deadline after a handful of
        // builds.
        let nodemgr_deadline =
            tokio::time::Instant::now() + lookup::NODEMGR_SUBVOLUME_DELETE_TIMEOUT;
        for path in &subvolumes {
            // The digest dir name is the build's content hash (or a
            // transient sibling such as `….importing` /
            // `….replaced.<uuid>`, which the build-level delete
            // detects and cleans locally without the node-mgr RPC).
            match path.file_name().and_then(|n| n.to_str()) {
                Some(content_hash) => {
                    self.remove_build_artifacts_subvolume_aware(
                        template,
                        content_hash,
                        nodemgr_deadline,
                    )
                    .await?;
                }
                None => {
                    // Non-UTF-8 dir name: it has no RPC identity and no
                    // snapshot dirs can reference it, but it still must
                    // not reach the plain tree removal below — a
                    // read-only subvolume would EROFS it. Clean it
                    // locally.
                    tracing::warn!(
                        template,
                        path = %path.display(),
                        "subvolume with non-UTF-8 name under template dir; cleaning locally"
                    );
                    let cleanup_result = tokio::task::spawn_blocking({
                        let path = path.clone();
                        move || crate::volume::cleanup_digest_dir_sync(&path)
                    })
                    .await
                    .map_err(|e| blocking_join_status("clean stray subvolume", template, e))?;
                    if let Err(error) = cleanup_result {
                        tracing::error!(
                            template,
                            path = %path.display(),
                            %error,
                            "failed to remove stray subvolume before DB cleanup"
                        );
                        return Err(Status::internal(format!(
                            "remove stray subvolume {}: {error}",
                            path.display()
                        )));
                    }
                }
            }
        }

        let store = self.artifact_store.clone();
        let t = template.to_string();
        let removal = tokio::task::spawn_blocking(move || store.remove_template_artifacts(&t))
            .await
            .map_err(|e| blocking_join_status("remove template artifacts", template, e))?;
        if let Err(error) = removal {
            tracing::error!(
                template,
                %error,
                "failed to remove template artifacts before DB cleanup"
            );
            return Err(Status::internal(error.to_string()));
        }
        Ok(())
    }

    async fn delete_template_local_state(&self, template: &str) -> Result<(), Status> {
        let db = Arc::clone(&self.db);
        let name = template.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().map_err(|_| {
                Status::internal("db mutex poisoned while deleting template local state")
            })?;
            crate::db::delete_template_local_state(&conn, &name)
                .map_err(|e| Status::internal(format!("db delete_template_local_state: {e}")))?;
            Ok::<_, Status>(())
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e: Status| e)
    }

    pub(super) async fn list_templates(
        &self,
        request: Request<ListTemplatesRequest>,
    ) -> Result<Response<ListTemplatesResponse>, Status> {
        self.authorize_peer(&request, "ListTemplates", ALLOW_TEMPLATE_OBSERVER)?;
        // Fetch all builds from DB
        let db = Arc::clone(&self.db);
        let (builds, all_snapshots, pools, template_settings) =
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| Status::internal("db mutex poisoned while listing templates"))?;
                let builds = crate::db::list_builds(&conn).unwrap_or_default();
                let pools = crate::db::get_all_pools(&conn).unwrap_or_default();
                let settings = crate::db::list_template_settings(&conn).unwrap_or_default();
                // Collect all snapshots for each build
                let mut all_snaps = Vec::new();
                for build in &builds {
                    if let Ok(snaps) =
                        crate::db::list_snapshots(&conn, &build.name, &build.content_hash)
                    {
                        all_snaps.extend(snaps);
                    }
                }
                Ok::<_, Status>((builds, all_snaps, pools, settings))
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;

        // Fetch live sandbox list to compute pool_current
        let sbx_sandboxes = self
            .pool_count_sandboxes(request.get_ref().omit_sandbox_counts)
            .await;
        let mut effective_snapshot_memory_by_requested = HashMap::new();
        for build in &builds {
            let settings = template_settings.get(&build.name);
            let runtime_defaults = template_runtime_defaults(&build.name, &build.content_hash);
            let requested_memory_mib = settings
                .map(|settings| settings.snapshot_memory_mib)
                .or(runtime_defaults.memory_mib)
                .unwrap_or(1024);
            if effective_snapshot_memory_by_requested.contains_key(&requested_memory_mib) {
                continue;
            }
            let effective_memory_mib =
                match resolve_sandbox_memory(requested_memory_mib, self.sbx_connect_retry_policy)
                    .await
                {
                    Ok(resolved) => {
                        if resolved.effective_memory_mib < resolved.requested_memory_mib {
                            tracing::info!(
                                requested_snapshot_memory_mib = resolved.requested_memory_mib,
                                effective_snapshot_memory_mib = resolved.effective_memory_mib,
                                sandbox_guest_memory_max_mib = resolved.guest_memory_max_mib,
                                sandbox_cgroup_memory_mib = resolved.cgroup_memory_mib,
                                sandbox_cgroup_memory_max_mib = resolved.cgroup_memory_max_mib,
                                sandbox_memory_headroom_mib = resolved.memory_headroom_mib,
                                "clamped template list snapshot memory to sandbox policy"
                            );
                        }
                        resolved.effective_memory_mib
                    }
                    Err(status) => {
                        tracing::warn!(
                            code = ?status.code(),
                            error = %status.message(),
                            requested_snapshot_memory_mib = requested_memory_mib,
                            "could not resolve sandbox memory policy for template listing"
                        );
                        requested_memory_mib
                    }
                };
            effective_snapshot_memory_by_requested
                .insert(requested_memory_mib, effective_memory_mib);
        }

        let effective_snapshot_memory = |requested_memory_mib: u32| {
            effective_snapshot_memory_by_requested
                .get(&requested_memory_mib)
                .copied()
                .unwrap_or(requested_memory_mib)
        };

        for sandbox in &sbx_sandboxes {
            sandbox
                .observed_lifecycle()
                .map_err(|error| Status::internal(error.to_string()))?;
        }

        let infos = builds
            .into_iter()
            .map(|build| {
                // Snapshots for this build
                let snapshots: Vec<crate::proto::SnapshotInfo> = all_snapshots
                    .iter()
                    .filter(|s| s.name == build.name && s.content_hash == build.content_hash)
                    .map(|s| crate::proto::SnapshotInfo {
                        vcpu: s.vcpu,
                        memory_mib: s.memory_mib,
                        snapshot_hash: s.snapshot_hash.clone(),
                    })
                    .collect();

                let has_base_snapshot = !snapshots.is_empty();
                let template_name = build.name.clone();
                let settings = template_settings.get(&template_name);
                let runtime_defaults = template_runtime_defaults(&build.name, &build.content_hash);
                let snapshot_vcpu = settings
                    .map(|settings| settings.snapshot_vcpu)
                    .or(runtime_defaults.vcpu)
                    .unwrap_or(2);
                let snapshot_memory_mib = settings
                    .map(|settings| settings.snapshot_memory_mib)
                    .or(runtime_defaults.memory_mib)
                    .map(effective_snapshot_memory)
                    .unwrap_or(1024);

                let pool = pools.iter().find(|p| p.template == build.name);
                let pool_current = u32::try_from(
                    sbx_sandboxes
                        .iter()
                        .filter(|s| {
                            s.pool_template.as_deref() == Some(build.name.as_str())
                                && s.observed_lifecycle().is_ok_and(|state| state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Paused)
                        })
                        .count(),
                )
                .map_err(|_| {
                    Status::internal(format!(
                        "template '{}' has too many pooled sandboxes to report",
                        build.name
                    ))
                })?;

                // Summarize the snapshot that matches the desired config when
                // available. Older nodes can have stale snapshots for a prior
                // config; keep them in `snapshots`, but don't let them drive
                // the single base-snapshot summary.
                let summary_snapshot = snapshots
                    .iter()
                    .find(|snap| {
                        snap.vcpu == snapshot_vcpu && snap.memory_mib == snapshot_memory_mib
                    })
                    .or_else(|| snapshots.first());
                let base_snapshot_size = summary_snapshot.and_then(|snap| {
                    snapshot_cache_size_bytes(
                        &build.name,
                        &build.content_hash,
                        snap.vcpu,
                        snap.memory_mib,
                    )
                });

                let cache_size_bytes = template_cache_size_bytes(&build.name, &build.content_hash);
                let execution = template_execution_proto(&build);

                Ok::<_, Status>(crate::proto::TemplateFullInfo {
                    name: template_name.clone(),
                    has_base_snapshot,
                    pool_target: pool.map(|p| p.target_count),
                    pool_current,
                    artifact_size_bytes: Some(build.artifact_size_bytes),
                    base_snapshot_size_bytes: base_snapshot_size,
                    built_at: Some(build.build_date),
                    content_hash: Some(build.content_hash),
                    runtime: Some(build.runtime),
                    arch: Some(build.arch),
                    snapshots,
                    default_auto_lifecycle: settings
                        .map(|settings| common_policy_to_proto(&settings.default_auto_lifecycle)),
                    network_policy: settings.map(|settings| {
                        common_network_policy_to_proto(&settings.default_network_policy)
                    }),
                    network_policy_generation: settings
                        .map(|settings| settings.default_network_policy_generation)
                        .unwrap_or(0),
                    network_policy_authority: settings
                        .map(|settings| settings.default_network_policy_authority.as_proto_i32())
                        .unwrap_or_else(|| crate::db::PolicyAuthority::Cluster.as_proto_i32()),
                    auto_lifecycle_authority: settings
                        .map(|settings| settings.default_auto_lifecycle_authority.as_proto_i32())
                        .unwrap_or_else(|| crate::db::PolicyAuthority::Cluster.as_proto_i32()),
                    execution,
                    ensure_base_snapshot: settings.map(|settings| settings.ensure_base_snapshot),
                    snapshot_vcpu: settings.map(|settings| settings.snapshot_vcpu),
                    snapshot_memory_mib: settings
                        .map(|settings| effective_snapshot_memory(settings.snapshot_memory_mib)),
                    vcpu: runtime_defaults.vcpu,
                    memory_mib: runtime_defaults.memory_mib,
                    disk_size_mb: runtime_defaults.disk_size_mb,
                    cache_size_bytes,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(ListTemplatesResponse { templates: infos }))
    }

    // ── Template transfer / versioning RPCs ─────────────────────────────────

    pub(super) async fn register_template(
        &self,
        request: Request<crate::proto::RegisterTemplateRequest>,
    ) -> Result<Response<crate::proto::RegisterTemplateResponse>, Status> {
        self.authorize_peer(&request, "RegisterTemplate", ALLOW_TEMPLATE_OPERATOR)?;
        let publish_node_state = !crate::node_state_sync::should_skip_node_publish(&request);
        let req = request.get_ref();
        let tmpl_ref = req
            .template
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("template is required"))?;
        let template_id = &tmpl_ref.name;
        validate_template_name(template_id)?;
        let requested_network_policy = req
            .network_policy
            .as_ref()
            .map(proto_network_policy_to_common)
            .transpose()?;

        let name_dir = self
            .artifact_store
            .root
            .join(template_id)
            .display()
            .to_string();
        let register_source = match req.staging_dir.as_deref() {
            Some(staging_dir_name) => {
                // Build-scoped source: a single validated path component under
                // the name dir, written by build-manager.
                let component = validate_build_staging_dir_name(staging_dir_name)?;
                let dir = format!("{name_dir}/{component}");
                if !Path::new(&dir).join(TEMPLATE_ROOTFS_FILE).exists() {
                    return Err(Status::not_found(format!(
                        "build staging dir for template '{template_id}' not found on disk"
                    )));
                }
                RegisterSource::BuildStaging { dir }
            }
            None => {
                // Legacy flat layout (builder CLI, seed bootstrap scripts).
                let flat_rootfs = format!("{name_dir}/rootfs.ext4");
                if !std::path::Path::new(&flat_rootfs).exists() {
                    // Maybe already organized — check for any digest directory with rootfs
                    let has_build = std::fs::read_dir(&name_dir)
                        .ok()
                        .map(|entries| {
                            entries.flatten().any(|e| {
                                e.file_name().to_string_lossy().parse::<Digest>().is_ok()
                                    && e.path().join("rootfs.ext4").exists()
                            })
                        })
                        .unwrap_or(false);

                    if !has_build {
                        return Err(Status::not_found(format!(
                            "template '{template_id}' not found on disk"
                        )));
                    }

                    // Already organized — nothing to do
                    return Err(Status::already_exists(format!(
                        "template '{template_id}' already in digest layout"
                    )));
                }
                RegisterSource::Flat {
                    rootfs: flat_rootfs,
                    metadata: format!("{name_dir}/metadata.json"),
                }
            }
        };

        let metadata_bytes = match std::fs::read(register_source.metadata_path()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(Status::failed_precondition(format!(
                    "read template metadata: {error}"
                )));
            }
        };
        let source_manifest = register_source
            .metadata_path()
            .with_file_name("manifest.json");
        let manifest_bytes = match std::fs::read(&source_manifest) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(Status::failed_precondition(format!(
                    "read template manifest: {error}"
                )));
            }
        };
        let metadata = validate_guest_documents(&metadata_bytes, &manifest_bytes)?;
        if metadata.guest_image.is_some() {
            self.authorize_peer(
                &request,
                "RegisterFirmwareTemplate",
                &[
                    UdsPeerPrincipal::Root,
                    UdsPeerPrincipal::Role(LocalRole::Operator),
                ],
            )?;
        }

        // Network policy validation is independent of the content hash, so run
        // it before any disk work.
        if let Some(requested_policy) = requested_network_policy.as_ref() {
            let db = Arc::clone(&self.db);
            let name = template_id.clone();
            let requested_policy = requested_policy.clone();
            let existing_policy = tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| crate::db::mutex_poison_error("check template settings"))?;
                crate::db::get_template_settings(&conn, &name)
                    .map(|settings| settings.map(|settings| settings.default_network_policy))
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("db get_template_settings: {e}")))?;
            if let Some(existing_policy) = existing_policy
                && existing_policy != requested_policy
            {
                return Err(Status::failed_precondition(
                    "template already has a network policy; use UpdateTemplateSettings to change it",
                ));
            }
        }

        let staging_dir = format!("{name_dir}/sha256:register.importing.{}", Uuid::now_v7());
        let staging_path = Path::new(&staging_dir);
        std::fs::create_dir(staging_path)
            .map_err(|e| Status::internal(format!("create register staging dir: {e}")))?;
        let mut staging_guard = ImportStagingGuard::new(&staging_dir);
        set_template_cache_permissions(staging_path, TEMPLATE_CACHE_DIR_MODE)?;

        let rootfs_path = staging_path.join(TEMPLATE_ROOTFS_FILE);
        copy_template_file(&register_source.rootfs_path(), &rootfs_path)?;
        let source_meta = register_source.metadata_path();
        if source_meta.exists() {
            copy_template_file(&source_meta, &staging_path.join("metadata.json"))?;
        }

        // Read metadata for build_date / arch.
        let meta_path = format!("{staging_dir}/metadata.json");
        let metadata_json = std::fs::read_to_string(&meta_path).unwrap_or_default();
        let metadata = validate_guest_documents(metadata_json.as_bytes(), &[])?;
        let build_date = metadata.built_at.clone();
        // No manifest on disk yet during register — the empty default is
        // the equivalent of the previous `serde_json::Value::Null`.
        let empty_manifest = pplx_space_common_core::types::TemplateManifest::default();
        let arch = template_arch(&metadata, &empty_manifest);

        expand_template_rootfs(template_id, "pending", &staging_dir, "register").await?;
        let content_hash = compute_template_file_hash(
            rootfs_path.clone(),
            self.production_hash_algorithm,
            "hash expanded rootfs",
            self.template_read_mode(template_id),
        )
        .await?;
        let expanded_size = std::fs::metadata(&rootfs_path)
            .map(|m| m.len())
            .map_err(|e| Status::internal(format!("stat rootfs {}: {e}", rootfs_path.display())))?;

        let digest_dir = build_artifacts_dir(&self.artifact_store.root, template_id, &content_hash)
            .display()
            .to_string();

        // If a build with this same expanded hash already exists,
        // drop the staging copy and short-circuit to a metadata refresh.
        // The register staging is no longer needed; the guard reaps it on drop.
        if std::path::Path::new(&digest_dir).exists() {
            let metadata_bytes = std::fs::read(format!("{staging_dir}/metadata.json")).ok();
            return self
                .finish_register_with_existing_digest(ExistingDigestRegistration {
                    template_id,
                    content_hash: &content_hash,
                    digest_dir: &digest_dir,
                    metadata_bytes: metadata_bytes.as_deref(),
                    requested_network_policy: requested_network_policy.as_ref(),
                    publish_node_state,
                    register_source: &register_source,
                })
                .await;
        }

        let seed_manifest = pplx_space_common_core::types::TemplateManifest {
            name: template_id.clone(),
            content_hash: content_hash.clone(),
            build_date: build_date.clone(),
            arch: arch.clone(),
            ..pplx_space_common_core::types::TemplateManifest::default()
        };
        let manifest = normalize_template_manifest(
            seed_manifest,
            template_id,
            &content_hash,
            &metadata,
            expanded_size,
            None,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let manifest_str = serde_json::to_string_pretty(&manifest)
            .map_err(|e| Status::internal(format!("serialize manifest: {e}")))?;
        write_template_manifest_atomic(
            &Path::new(&staging_dir).join("manifest.json"),
            &manifest_str,
        )?;
        normalize_template_cache_tree(staging_path)?;
        match std::fs::rename(&staging_dir, &digest_dir) {
            Ok(()) => {}
            // A concurrent registration of identical content can create the
            // digest dir between the existence check above and this rename;
            // renaming onto a non-empty directory then fails deterministically.
            // Treat that exactly like the up-front short-circuit — the content
            // is already registered. The guard reaps our staging copy on drop.
            Err(_) if std::path::Path::new(&digest_dir).exists() => {
                let metadata_bytes = std::fs::read(format!("{staging_dir}/metadata.json")).ok();
                return self
                    .finish_register_with_existing_digest(ExistingDigestRegistration {
                        template_id,
                        content_hash: &content_hash,
                        digest_dir: &digest_dir,
                        metadata_bytes: metadata_bytes.as_deref(),
                        requested_network_policy: requested_network_policy.as_ref(),
                        publish_node_state,
                        register_source: &register_source,
                    })
                    .await;
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "publish register staging dir: {e}"
                )));
            }
        }
        staging_guard.disarm();
        register_source.cleanup();

        // Persist to DB
        let db = Arc::clone(&self.db);
        let name = template_id.clone();
        let hash = content_hash.clone();
        let date = build_date.clone();
        let runtime = manifest.runtime.clone();
        let requested_network_policy_for_db = requested_network_policy.clone();
        tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| crate::db::mutex_poison_error("register template upsert"))?;
            crate::db::upsert_build(
                &conn,
                &crate::db::BuildUpsert {
                    name: &name,
                    content_hash: &hash,
                    build_date: &date,
                    manifest_json: &manifest_str,
                    runtime: &runtime,
                    arch: &arch,
                    artifact_size_bytes: expanded_size,
                    state: "ready",
                },
            )?;
            let _ = crate::db::ensure_template_network_policy_with_seed(
                &conn,
                &name,
                requested_network_policy_for_db.as_ref(),
            )?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e| Status::internal(format!("db: {e}")))?;

        self.spawn_template_compressed_rootfs_generation(
            template_id.clone(),
            content_hash.clone(),
            digest_dir.clone(),
            "register",
        );

        let outcome = self.reconcile_desired_base_snapshot(template_id).await;
        if let Some(error) = outcome.error {
            tracing::warn!(
                template = %template_id,
                error = %error,
                "desired base snapshot reconcile after register failed"
            );
        }
        if publish_node_state {
            crate::node_state_sync::mark_template_dirty(&self.db, template_id);
        }

        Ok(Response::new(crate::proto::RegisterTemplateResponse {
            template: Some(crate::proto::TemplateRef {
                name: template_id.clone(),
                content_hash: Some(content_hash),
                tag: None,
            }),
            build_date,
        }))
    }

    /// Completes a registration whose content already exists in a digest dir:
    /// refreshes the digest metadata, removes the registration source,
    /// converges the DB row, and reports the existing build. Reached both via
    /// the up-front digest-existence check and via the staging→digest rename
    /// race, where a concurrent same-content registration won the rename.
    pub(super) async fn finish_register_with_existing_digest(
        &self,
        registration: ExistingDigestRegistration<'_>,
    ) -> Result<Response<crate::proto::RegisterTemplateResponse>, Status> {
        let ExistingDigestRegistration {
            template_id,
            content_hash,
            digest_dir,
            metadata_bytes,
            requested_network_policy,
            publish_node_state,
            register_source,
        } = registration;
        self.refresh_register_existing_expanded_build(
            template_id,
            content_hash,
            digest_dir,
            metadata_bytes,
        )
        .await?;
        register_source.cleanup();
        let build_date = {
            let db = Arc::clone(&self.db);
            let name = template_id.to_string();
            let hash = content_hash.to_string();
            let requested_network_policy = requested_network_policy.cloned();
            tokio::task::spawn_blocking(move || {
                let conn = db.lock().map_err(|_| {
                    crate::db::mutex_poison_error("finish existing template registration")
                })?;
                let _ = crate::db::ensure_template_network_policy_with_seed(
                    &conn,
                    &name,
                    requested_network_policy.as_ref(),
                )?;
                crate::db::get_build(&conn, &name, &hash)
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("db get_build: {e}")))?
            .map(|build| build.build_date)
            .ok_or_else(|| {
                Status::already_exists(format!(
                    "template '{template_id}' already exists on disk but is not registered"
                ))
            })?
        };
        let outcome = self.reconcile_desired_base_snapshot(template_id).await;
        if let Some(error) = outcome.error {
            tracing::warn!(
                template = %template_id,
                error = %error,
                "desired base snapshot reconcile after register failed"
            );
        }
        if publish_node_state {
            crate::node_state_sync::mark_template_dirty(&self.db, template_id);
        }
        Ok(Response::new(crate::proto::RegisterTemplateResponse {
            template: Some(crate::proto::TemplateRef {
                name: template_id.to_string(),
                content_hash: Some(content_hash.to_string()),
                tag: None,
            }),
            build_date,
        }))
    }
}

fn template_execution_proto(
    build: &crate::db::BuildRow,
) -> Option<pplx_space_execution_common::proto::ExecutionMetadata> {
    let raw = build.execution_metadata_json.as_deref()?;
    match serde_json::from_str::<pplx_space_common_core::types::ExecutionMetadata>(raw) {
        Ok(execution) => Some((&execution).into()),
        Err(error) => {
            tracing::warn!(
                template = %build.name,
                content_hash = %build.content_hash,
                %error,
                "stored template execution metadata is invalid"
            );
            None
        }
    }
}

/// Inputs for completing a registration against an already-existing digest
/// dir (same-content short-circuit or staging→digest rename race).
pub(super) struct ExistingDigestRegistration<'a> {
    pub(super) template_id: &'a str,
    pub(super) content_hash: &'a str,
    pub(super) digest_dir: &'a str,
    pub(super) metadata_bytes: Option<&'a [u8]>,
    pub(super) requested_network_policy: Option<&'a NetworkPolicyV1>,
    pub(super) publish_node_state: bool,
    pub(super) register_source: &'a RegisterSource,
}

/// Validates the wire `staging_dir` value: a single safe path component that
/// carries the build-manager staging prefix. Keeps digest dirs (`sha256:*`)
/// and template-manager's own transient dirs unreachable through this field.
pub(super) fn validate_build_staging_dir_name(name: &str) -> Result<String, Status> {
    let component = pplx_space_common_core::types::SafePathComponent::parse(name)
        .map_err(|e| Status::invalid_argument(format!("invalid staging_dir: {e}")))?;
    if !is_build_staging_dir(component.as_str()) {
        return Err(Status::invalid_argument(format!(
            "staging_dir must begin with '{BUILD_IMPORTING_PREFIX}'"
        )));
    }
    Ok(component.as_str().to_string())
}

/// Where a `RegisterTemplate` call reads its rootfs/metadata from, and what
/// it consumes on success.
pub(super) enum RegisterSource {
    /// Legacy flat layout: `{name_dir}/rootfs.ext4` + `metadata.json`,
    /// written by the seed bootstrap scripts and pre-staging builders.
    Flat { rootfs: String, metadata: String },
    /// A build-manager per-job staging directory
    /// (`{name_dir}/build.importing.{job_id}`).
    BuildStaging { dir: String },
}

impl RegisterSource {
    pub(super) fn rootfs_path(&self) -> PathBuf {
        match self {
            Self::Flat { rootfs, .. } => PathBuf::from(rootfs),
            Self::BuildStaging { dir } => Path::new(dir).join(TEMPLATE_ROOTFS_FILE),
        }
    }

    pub(super) fn metadata_path(&self) -> PathBuf {
        match self {
            Self::Flat { metadata, .. } => PathBuf::from(metadata),
            Self::BuildStaging { dir } => Path::new(dir).join("metadata.json"),
        }
    }

    /// Consume the source after its content reached a digest dir. Flat files
    /// are removed best-effort, matching the historical behavior. Build
    /// staging dirs are deliberately left alone: their lifetime belongs to
    /// build-manager, which removes them only after observing a durable
    /// response. If registration consumed them, a retry after a lost
    /// response (template-manager restart, transport blip) would hit
    /// NotFound for a build that was in fact published; with the dir
    /// intact, the retry recomputes the hash and converges through the
    /// existing-digest path — including re-upserting the DB row when the
    /// first attempt died between the rename and the DB write.
    pub(super) fn cleanup(&self) {
        match self {
            Self::Flat { rootfs, metadata } => {
                let _ = std::fs::remove_file(rootfs);
                let _ = std::fs::remove_file(metadata);
            }
            Self::BuildStaging { .. } => {}
        }
    }
}

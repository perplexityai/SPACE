use pplx_space_common_core::types::OrgId;
use pplx_space_common_svc::template_lookup::TemplateLookupScope;

use super::*;

pub(super) const fn pull_failure_code(failure: crate::volume::PullFailure) -> tonic::Code {
    match failure.class() {
        crate::volume::PullFailureClass::Missing => tonic::Code::NotFound,
        crate::volume::PullFailureClass::Retryable => tonic::Code::Unavailable,
        crate::volume::PullFailureClass::InvalidInput => tonic::Code::InvalidArgument,
        crate::volume::PullFailureClass::FailedPrecondition => tonic::Code::FailedPrecondition,
        crate::volume::PullFailureClass::DataLoss => tonic::Code::DataLoss,
    }
}

impl TemplateManagerService {
    pub(super) async fn get_template(
        &self,
        request: Request<crate::proto::GetTemplateRequest>,
    ) -> Result<Response<crate::proto::GetTemplateResponse>, Status> {
        let scope = TemplateLookupScope::from_request(&request)?;
        if scope.is_none() {
            pplx_space_metrics::auth::template_lookup_compatibility("missing_scope");
        }
        let mut response = self
            .get_template_resolving(
                request,
                async |name, hash, tag, org| {
                    crate::volume::find_release_in_scope(name, hash, tag, org, scope).await
                },
                async |spec, resolve_elapsed, release| {
                    self.volume_puller
                        .materialize_release(Arc::clone(&self.db), spec, resolve_elapsed, release)
                        .await
                },
            )
            .await?;
        if let Some(scope) = scope {
            scope.receipt(&mut response);
        }
        Ok(response)
    }

    pub(super) async fn get_template_resolving(
        &self,
        request: Request<crate::proto::GetTemplateRequest>,
        resolve: impl AsyncFnOnce(
            &str,
            &str,
            &str,
            Option<OrgId>,
        ) -> Result<
            crate::volume::ResolvedRelease,
            crate::volume::FindReleaseError,
        >,
        materialize: impl AsyncFnOnce(
            crate::volume::PullSpec<'_>,
            std::time::Duration,
            crate::volume::ResolvedRelease,
        ) -> crate::volume::PullOutcome,
    ) -> Result<Response<crate::proto::GetTemplateResponse>, Status> {
        self.authorize_peer(&request, "GetTemplate", ALLOW_TEMPLATE_OBSERVER)?;
        let scope = TemplateLookupScope::from_request(&request)?;
        let req = request.get_ref();
        let tmpl_ref = req
            .template
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("template is required"))?;
        let template_id = &tmpl_ref.name;
        if template_id.is_empty() {
            return Err(Status::invalid_argument("template name required"));
        }

        let req_hash = tmpl_ref.content_hash.clone().unwrap_or_default();
        if !req_hash.is_empty() {
            validate_content_hash(&req_hash)?;
        }
        let req_tag = tmpl_ref.tag.clone().unwrap_or_default();
        let caller_org = req
            .caller_org_id
            .as_deref()
            .map(str::parse::<OrgId>)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        // Request metrics (cache, fetch-error, and pull) are tagged with the
        // caller's selector, not the resolved hash that replaces `req_hash`
        // below.
        let requested_hash = req_hash.clone();
        let resolve_started = std::time::Instant::now();
        // Explicit tags need authoritative resolution even on cache hits. Trusted
        // name-only policy refreshes and exact-digest reads can still work offline.
        let release = if caller_org.is_some() || (req_hash.is_empty() && !req_tag.is_empty()) {
            Some(
                resolve(template_id, &req_hash, &req_tag, caller_org)
                    .await
                    .map_err(|error| match error {
                        crate::volume::FindReleaseError::NotFound => {
                            Status::not_found("template release not found")
                        }
                        crate::volume::FindReleaseError::Unavailable => {
                            Status::unavailable("cluster metadata unavailable")
                        }
                        crate::volume::FindReleaseError::InvalidResponse => {
                            Status::failed_precondition(
                                "cluster did not return an authorized exact release",
                            )
                        }
                        crate::volume::FindReleaseError::Status(status) => status,
                    })?,
            )
        } else {
            None
        };
        let resolve_elapsed = resolve_started.elapsed();
        let req_hash = release
            .as_ref()
            .map(|release| release.hash.clone())
            .unwrap_or(req_hash);
        let snapshot_selector = optional_snapshot_dimensions(req.vcpu, req.memory_mib)?;
        let policy_snapshot_memory_mib = if snapshot_selector.is_none() {
            let desired_memory_mib = {
                let db = Arc::clone(&self.db);
                let template_id = template_id.clone();
                let req_hash = req_hash.clone();
                let req_tag = req_tag.clone();
                tokio::task::spawn_blocking(move || {
                    let conn = db
                        .lock()
                        .map_err(|_| Status::internal("template DB mutex poisoned"))?;
                    let (name, _) = TemplateManagerService::resolve_template(
                        &conn,
                        &template_id,
                        &req_hash,
                        &req_tag,
                    )?;
                    let settings = crate::db::get_template_settings(&conn, &name)
                        .map_err(|e| Status::internal(format!("db get_template_settings: {e}")))?;
                    Ok::<_, Status>(
                        settings
                            .filter(|settings| settings.base_snapshot_policy_set_at.is_some())
                            .map(|settings| settings.snapshot_memory_mib),
                    )
                })
                .await
            };
            match desired_memory_mib {
                Ok(Ok(Some(memory_mib))) => {
                    match resolve_sandbox_memory(memory_mib, self.sbx_connect_retry_policy).await {
                        Ok(resolved) => {
                            if resolved.effective_memory_mib < resolved.requested_memory_mib {
                                tracing::info!(
                                    template = %template_id,
                                    requested_snapshot_memory_mib = resolved.requested_memory_mib,
                                    effective_snapshot_memory_mib = resolved.effective_memory_mib,
                                    sandbox_guest_memory_max_mib = resolved.guest_memory_max_mib,
                                    sandbox_cgroup_memory_mib = resolved.cgroup_memory_mib,
                                    sandbox_cgroup_memory_max_mib = resolved.cgroup_memory_max_mib,
                                    sandbox_memory_headroom_mib = resolved.memory_headroom_mib,
                                    "clamped template snapshot lookup memory to sandbox policy"
                                );
                            }
                            Some(resolved.effective_memory_mib)
                        }
                        Err(status) => {
                            tracing::warn!(
                                template = %template_id,
                                code = ?status.code(),
                                error = %status.message(),
                                "could not resolve sandbox memory policy for template snapshot lookup"
                            );
                            None
                        }
                    }
                }
                Ok(Ok(None)) | Ok(Err(_)) => None,
                Err(error) => {
                    tracing::warn!(
                        template = %template_id,
                        %error,
                        "could not read desired template snapshot policy for snapshot lookup"
                    );
                    None
                }
            }
        } else {
            None
        };
        let cache_tags = get_template_metric_tags(template_id, &requested_hash, &req_tag);

        // First attempt: local-only lookup.
        match build_get_template_response(
            self.artifact_store.root.clone(),
            Arc::clone(&self.db),
            template_id.clone(),
            req_hash.clone(),
            req_tag.clone(),
            snapshot_selector,
            policy_snapshot_memory_mib,
        )
        .await
        {
            Ok(mut resp) => {
                resp.authorized_org_id = caller_org.map(|org| org.to_string());
                pplx_space_metrics::template::get_cache_hit(&cache_tags);
                if snapshot_selector.is_none() && resp.snapshot_dir.is_none() {
                    self.spawn_desired_base_snapshot_reconcile(
                        template_id.clone(),
                        "get_template_snapshot_miss",
                    );
                }
                return Ok(Response::new(resp));
            }
            Err(status) if status.code() != tonic::Code::NotFound => {
                let reason = if status.code() == tonic::Code::FailedPrecondition {
                    "template_rootfs_unavailable"
                } else {
                    "local_db_error"
                };
                record_fetch_error(template_id, &requested_hash, &req_tag, reason);
                return Err(status);
            }
            Err(_) => {
                pplx_space_metrics::template::get_cache_miss(&cache_tags);
            }
        }

        let outcome = match release {
            Some(release) => {
                let spec = crate::volume::PullSpec {
                    name: template_id,
                    req_hash: &requested_hash,
                    req_tag: &req_tag,
                    trigger: crate::volume::PullTrigger::OnDemand,
                };
                materialize(spec, resolve_elapsed, release).await
            }
            None => {
                self.volume_puller
                    .pull_in_scope(
                        Arc::clone(&self.db),
                        template_id,
                        &req_hash,
                        &req_tag,
                        crate::volume::PullTrigger::OnDemand,
                        scope,
                    )
                    .await
            }
        };
        match outcome {
            crate::volume::PullOutcome::Materialized => {
                let template_id_owned = template_id.clone();
                let req_hash_for_err = requested_hash.clone();
                let req_tag_for_err = req_tag.clone();
                build_get_template_response(
                    self.artifact_store.root.clone(),
                    Arc::clone(&self.db),
                    template_id.clone(),
                    req_hash,
                    req_tag,
                    snapshot_selector,
                    policy_snapshot_memory_mib,
                )
                .await
                .inspect(|resp| {
                    if snapshot_selector.is_none() && resp.snapshot_dir.is_none() {
                        self.spawn_desired_base_snapshot_reconcile(
                            template_id_owned.clone(),
                            "get_template_snapshot_miss",
                        );
                    }
                })
                .map(|mut response| {
                    response.authorized_org_id = caller_org.map(|org| org.to_string());
                    Response::new(response)
                })
                .inspect_err(|_| {
                    record_fetch_error(
                        &template_id_owned,
                        &req_hash_for_err,
                        &req_tag_for_err,
                        "post_pull_local_lookup_failed",
                    );
                })
            }
            crate::volume::PullOutcome::ClassifiedFailure(failure) => {
                record_fetch_error(
                    template_id,
                    &requested_hash,
                    &req_tag,
                    failure.metric_reason(),
                );
                let code = pull_failure_code(failure);
                let lookup = describe_lookup(&req_hash, &req_tag);
                let message = if code == tonic::Code::NotFound {
                    format!("build not found: {template_id} ({lookup}): {failure}")
                } else {
                    format!("template pull failed for {template_id} ({lookup}): {failure}")
                };
                Err(Status::new(code, message))
            }
            crate::volume::PullOutcome::Retryable(status) => {
                record_fetch_error(template_id, &requested_hash, &req_tag, "resource_exhausted");
                Err(status)
            }
            crate::volume::PullOutcome::DownstreamFailure { failure, status } => {
                record_fetch_error(
                    template_id,
                    &requested_hash,
                    &req_tag,
                    failure.metric_reason(),
                );
                Err(status)
            }
            crate::volume::PullOutcome::HashMismatch(status) => {
                record_fetch_error(template_id, &requested_hash, &req_tag, "hash_mismatch");
                Err(status)
            }
        }
    }

    pub(super) async fn update_template_settings(
        &self,
        request: Request<UpdateTemplateSettingsRequest>,
    ) -> Result<Response<UpdateTemplateSettingsResponse>, Status> {
        let req = request.get_ref();
        let peer = self.authorize_peer(
            &request,
            "UpdateTemplateSettings",
            update_template_settings_principals(req),
        )?;
        if req.template.trim().is_empty() {
            return Err(Status::invalid_argument("template is required"));
        }
        let auto_lifecycle = req
            .default_auto_lifecycle
            .as_ref()
            .map(proto_policy_to_common)
            .transpose()?;
        if let Some(policy) = auto_lifecycle.as_ref()
            && policy.requires_memory_lifecycle()
            && self
                .template_guest_image(&req.template)
                .await?
                .is_some_and(|image| !image.supports_auto_memory_lifecycle())
        {
            return Err(Status::failed_precondition(
                "agentless firmware guests do not support automatic pause or suspend",
            ));
        }
        let network_policy = req
            .network_policy
            .as_ref()
            .map(proto_network_policy_to_common)
            .transpose()?;
        if auto_lifecycle.is_none() && network_policy.is_none() {
            return Err(Status::invalid_argument(
                "at least one template setting is required",
            ));
        }

        let publish_node_state = !crate::node_state_sync::should_skip_node_publish(&request);
        let auto_lifecycle_requested = auto_lifecycle.is_some();
        let auto_lifecycle_authority =
            crate::db::PolicyAuthority::try_from(req.auto_lifecycle_authority)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let network_policy_authority =
            crate::db::PolicyAuthority::try_from(req.network_policy_authority)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if network_policy.is_some() {
            pplx_space_common_svc::require_local_authority_peer(
                peer.principal(),
                network_policy_authority == crate::db::PolicyAuthority::Local,
            )?;
        }
        if auto_lifecycle_requested {
            pplx_space_common_svc::require_local_authority_peer(
                peer.principal(),
                auto_lifecycle_authority == crate::db::PolicyAuthority::Local,
            )?;
        }

        let db = Arc::clone(&self.db);
        let template = req.template.clone();
        let network_policy_generation = req.network_policy_generation;
        let update_outcome = tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| Status::internal("template DB mutex poisoned"))?;
            crate::db::update_template_settings_with_outbox(
                &conn,
                crate::db::TemplateSettingsUpdate {
                    template: &template,
                    default_auto_lifecycle: auto_lifecycle.as_ref(),
                    default_auto_lifecycle_authority: auto_lifecycle_authority,
                    default_network_policy: network_policy.as_ref(),
                    default_network_policy_generation: network_policy_generation,
                    default_network_policy_authority: network_policy_authority,
                    mark_dirty: publish_node_state,
                },
            )
            .map_err(|e| match e {
                crate::db::TemplateNetworkPolicyUpsertError::GenerationConflict => {
                    Status::failed_precondition("template network policy generation conflict")
                }
                crate::db::TemplateNetworkPolicyUpsertError::Db(e) => {
                    Status::internal(format!("db upsert_template_settings: {e}"))
                }
            })
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;
        if publish_node_state && auto_lifecycle_requested && update_outcome.auto_lifecycle_applied {
            crate::node_state_sync::mark_template_dirty(&self.db, &req.template);
        }
        Ok(Response::new(UpdateTemplateSettingsResponse {
            network_policy_apply_decision: if req.network_policy.is_some() {
                match update_outcome.network_policy {
                    crate::db::TemplateNetworkPolicyUpdate::Apply => {
                        NetworkPolicyApplyDecision::Apply as i32
                    }
                    crate::db::TemplateNetworkPolicyUpdate::Skip => {
                        NetworkPolicyApplyDecision::Skip as i32
                    }
                    crate::db::TemplateNetworkPolicyUpdate::Repair => {
                        NetworkPolicyApplyDecision::Repair as i32
                    }
                    crate::db::TemplateNetworkPolicyUpdate::Conflict => {
                        NetworkPolicyApplyDecision::Conflict as i32
                    }
                    crate::db::TemplateNetworkPolicyUpdate::Unspecified => {
                        NetworkPolicyApplyDecision::Unspecified as i32
                    }
                }
            } else {
                NetworkPolicyApplyDecision::Unspecified as i32
            },
        }))
    }

    pub(super) async fn update_template_snapshot_policy(
        &self,
        request: Request<UpdateTemplateSnapshotPolicyRequest>,
    ) -> Result<Response<UpdateTemplateSnapshotPolicyResponse>, Status> {
        self.authorize_peer(
            &request,
            "UpdateTemplateSnapshotPolicy",
            ALLOW_TEMPLATE_SNAPSHOT_POLICY_OPERATOR,
        )?;
        let req = request.get_ref();
        let template = req.template.trim().to_string();
        validate_template_name(&template)?;
        let ensure_base_snapshot = req.ensure_base_snapshot;
        if ensure_base_snapshot && self.template_guest_image(&template).await?.is_some() {
            return Err(Status::failed_precondition(
                "firmware guests do not support snapshots",
            ));
        }
        let snapshot_vcpu = optional_positive_or_default("snapshot_vcpu", req.snapshot_vcpu, 2)?;
        let snapshot_memory_mib =
            optional_positive_or_default("snapshot_memory_mib", req.snapshot_memory_mib, 1024)?;

        {
            let db = Arc::clone(&self.db);
            let template = template.clone();
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| Status::internal("template DB mutex poisoned"))?;
                crate::db::update_template_snapshot_policy(
                    &conn,
                    &template,
                    ensure_base_snapshot,
                    snapshot_vcpu,
                    snapshot_memory_mib,
                )
                .map_err(|e| Status::internal(format!("db update_template_snapshot_policy: {e}")))
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;
        }

        let outcome = self.reconcile_desired_base_snapshot(&template).await;
        if outcome.reconciled {
            crate::node_state_sync::mark_template_dirty(&self.db, &template);
        }
        Ok(Response::new(UpdateTemplateSnapshotPolicyResponse {
            reconciled: outcome.reconciled,
            snapshot_built: outcome.snapshot_built,
            content_hash: outcome.content_hash,
            error: outcome.error,
            snapshot_deleted: outcome.snapshot_deleted,
        }))
    }

    pub(super) async fn delete_template_build(
        &self,
        request: Request<crate::proto::DeleteTemplateBuildRequest>,
    ) -> Result<Response<crate::proto::DeleteTemplateBuildResponse>, Status> {
        self.authorize_peer(&request, "DeleteTemplateBuild", ALLOW_TEMPLATE_OPERATOR)?;
        let skip_node_publish = crate::node_state_sync::should_skip_node_publish(&request);
        let req = request.into_inner();
        let tmpl_ref = req
            .template
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("template is required"))?;
        let name = tmpl_ref.name.clone();
        let content_hash = tmpl_ref.content_hash.clone().unwrap_or_default();

        if name.is_empty() || content_hash.is_empty() {
            return Err(Status::invalid_argument(
                "name and content_hash are required",
            ));
        }
        validate_template_name(&name)?;
        validate_content_hash(&content_hash)?;

        // Check that no pools or local sandboxes reference this build before
        // deleting its master rootfs.
        let db = Arc::clone(&self.db);
        let n = name.clone();
        let h = content_hash.clone();
        let build = tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| Status::internal("template DB mutex poisoned"))?;

            // Verify build exists
            let build = crate::db::get_build(&conn, &n, &h)
                .map_err(|e| Status::internal(format!("db: {e}")))?
                .ok_or_else(|| Status::not_found(format!("build not found: {n}@{h}")))?;

            // Check pools by template name
            let pools = crate::db::get_all_pools(&conn)
                .map_err(|e| Status::internal(format!("db: {e}")))?;

            if pools.iter().any(|p| p.template == n) {
                return Err(Status::failed_precondition(format!(
                    "cannot delete build {n}@{h}: template is referenced by a pool"
                )));
            }

            Ok::<_, Status>(build)
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e: Status| e)?;

        ensure_no_dependent_sandboxes_for_template_builds(
            TemplateBuildDeleteOperation::Build,
            DependentSandboxScope::All,
            TemplateBuildDeleteTarget::Builds(std::slice::from_ref(&build)),
        )
        .await?;

        self.delete_template_build_after_dependency_checks(&name, &content_hash)
            .await?;
        if !skip_node_publish {
            crate::node_state_sync::mark_template_dirty(&self.db, &name);
        }
        Ok(Response::new(crate::proto::DeleteTemplateBuildResponse {}))
    }

    pub(super) async fn delete_template_build_after_dependency_checks(
        &self,
        name: &str,
        content_hash: &str,
    ) -> Result<(), Status> {
        self.remove_build_artifacts_subvolume_aware(
            name,
            content_hash,
            tokio::time::Instant::now() + NODEMGR_SUBVOLUME_DELETE_TIMEOUT,
        )
        .await?;

        let db = Arc::clone(&self.db);
        let n = name.to_string();
        let h = content_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| Status::internal("template DB mutex poisoned"))?;
            // Delete build and its snapshot rows from DB.
            crate::db::delete_build(&conn, &n, &h)
                .map_err(|e| Status::internal(format!("db delete_build: {e}")))?;

            Ok::<_, Status>(())
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e: Status| e)?;

        Ok(())
    }

    /// Remove a build's on-disk artifacts ahead of its DB cleanup,
    /// handling the digest dir's two on-disk forms.
    ///
    /// Installed builds are read-only btrfs subvolumes
    /// (`received_uuid`-stamped `btrfs send` parents), and tmplmgr runs
    /// without `CAP_SYS_ADMIN` in production — plain
    /// `std::fs::remove_dir_all` EROFSes on the first unlink inside the
    /// read-only subvolume, so every RPC delete of an installed build
    /// failed and template-cache disk was never reclaimed. Subvolume
    /// digest dirs are deleted via node-mgr's privileged
    /// `BtrfsDeleteTemplateSubvolume` like the eviction loop; the
    /// per-`(vcpu, memory)` snapshot dirs are plain dirs and are removed
    /// locally either way.
    ///
    /// The local fallback works without privilege because node-mgr
    /// chowns digest subvolumes to the tmplmgr user at create time:
    /// clearing the RO flag is an owner-permitted operation, subvolume
    /// deletion needs only rmdir-equivalent permissions on current
    /// kernels, and even where that ioctl is refused, `remove_dir_all`
    /// completes — rmdir of an emptied subvolume root succeeds like a
    /// plain directory. It does not depend on the
    /// `user_subvol_rm_allowed` mount option.
    ///
    /// Unlike the eviction fallback's soft-success, a genuine
    /// local-cleanup failure aborts before DB cleanup and the delete
    /// stays retryable. What survives such a failure is not a usable
    /// build: by the time the local cleanup can fail, the snapshot
    /// dirs are gone, the subvolume's RO flag is cleared, and its
    /// contents are wiped — at most an emptied subvolume root remains,
    /// with the DB row still tracking it for the retry (and startup
    /// recovery prunes rows whose artifacts are missing). Plain-dir
    /// digest dirs (legacy installs, non-btrfs hosts) keep the
    /// existing `remove_dir_all` path.
    ///
    /// Latency: the node-mgr RPC leg is bounded by `nodemgr_deadline`.
    /// A single-build delete passes `now + NODEMGR_SUBVOLUME_DELETE_TIMEOUT`;
    /// the template-level delete computes ONE deadline for its whole
    /// serial loop, so N subvolumes cannot multiply the RPC budget to
    /// N × 2 s — the callers of both delete RPCs budget ~10 s
    /// end-to-end (the admin node-maintenance route and build-manager's
    /// serial prune), and the node-mgr client's own connect retry
    /// budget alone would consume all of it. Once the deadline is
    /// exhausted, remaining legs skip straight to the local cleanup.
    /// Abandoning the RPC at the deadline is safe: the delete is
    /// idempotent, so a server-side completion racing the local
    /// fallback is benign.
    pub(super) async fn remove_build_artifacts_subvolume_aware(
        &self,
        name: &str,
        content_hash: &str,
        nodemgr_deadline: tokio::time::Instant,
    ) -> Result<(), Status> {
        let build_dir = build_artifacts_dir(&self.artifact_store.root, name, content_hash);
        let is_subvol = tokio::task::spawn_blocking({
            let build_dir = build_dir.clone();
            move || pplx_space_common_core::host::btrfs::is_subvolume(&build_dir)
        })
        .await
        .map_err(|e| {
            blocking_join_status("probe digest dir", &format!("{name}@{content_hash}"), e)
        })?;

        if !is_subvol {
            let store = self.artifact_store.clone();
            let n = name.to_string();
            let h = content_hash.to_string();
            return tokio::task::spawn_blocking(move || store.remove_build_artifacts(&n, &h))
                .await
                .map_err(|e| {
                    blocking_join_status(
                        "remove build artifacts",
                        &format!("{name}@{content_hash}"),
                        e,
                    )
                })?
                .map_err(|error| {
                    tracing::error!(
                        template = name,
                        content_hash,
                        %error,
                        "failed to remove template build artifacts before DB cleanup"
                    );
                    Status::internal(error.to_string())
                });
        }

        // Snapshot dirs are plain dirs under
        // `<root>/<name>/snapshots/<hash>/`, separate from the digest
        // subvolume; remove them locally before touching the subvolume.
        let snapshot_removal = tokio::task::spawn_blocking({
            let store = self.artifact_store.clone();
            let n = name.to_string();
            let h = content_hash.to_string();
            move || store.remove_snapshot_artifacts(&n, &h)
        })
        .await
        .map_err(|e| {
            blocking_join_status(
                "remove snapshot artifacts",
                &format!("{name}@{content_hash}"),
                e,
            )
        })?;
        if let Err(error) = snapshot_removal {
            tracing::error!(
                template = name,
                content_hash,
                %error,
                "failed to remove template snapshot artifacts before DB cleanup"
            );
            return Err(Status::internal(error.to_string()));
        }

        // Node-mgr derives the digest path from `(name, content_hash)`
        // against the canonical TEMPLATE_DIR on its own side, so the
        // RPC only means "delete *this* build" when the artifact store
        // is rooted there. On a non-canonical root (dev/test
        // environments) the RPC would target a different tree — skip
        // straight to the local cleanup, which operates on the real
        // `build_dir`.
        //
        // The RPC leg is also skipped when `content_hash` is not a
        // valid content hash: the template-level delete routes
        // transient digest-sibling subvolumes (`….importing`,
        // `….replaced.<uuid>`, `build.importing.<job>`) through here,
        // and node-mgr rejects those names at its validation boundary,
        // so firing the RPC would add one guaranteed error per
        // transient dir to the gRPC error stream this path exists to
        // quiet. The local cleanup deletes them fine.
        let take_rpc_leg = self.artifact_store.root == Path::new(TEMPLATE_DIR)
            && validate_content_hash(content_hash).is_ok();
        if take_rpc_leg && nodemgr_deadline > tokio::time::Instant::now() {
            match tokio::time::timeout_at(
                nodemgr_deadline,
                crate::btrfs_via_nodemgr::delete_template_subvolume(name, content_hash),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(rpc_err)) => {
                    tracing::warn!(
                        template = name,
                        content_hash,
                        error = %rpc_err,
                        "btrfs_via_nodemgr::delete_template_subvolume failed; \
                         falling back to subvolume-aware local cleanup"
                    );
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        template = name,
                        content_hash,
                        "btrfs_via_nodemgr::delete_template_subvolume hit the delete's \
                         node-mgr deadline; falling back to subvolume-aware local cleanup"
                    );
                }
            }
        } else if take_rpc_leg {
            tracing::warn!(
                template = name,
                content_hash,
                "skipping node-mgr subvolume delete: the delete's node-mgr budget is \
                 already exhausted; using subvolume-aware local cleanup"
            );
        }
        // Always run the local subvolume-aware cleanup: it is a no-op
        // when the node-mgr delete already removed the digest dir, and
        // it is the deletion path when the RPC failed, timed out, or
        // was skipped for a non-canonical root.
        let cleanup_result = tokio::task::spawn_blocking({
            let build_dir = build_dir.clone();
            move || crate::volume::cleanup_digest_dir_sync(&build_dir)
        })
        .await
        .map_err(|e| {
            blocking_join_status("clean digest dir", &format!("{name}@{content_hash}"), e)
        })?;
        if let Err(error) = cleanup_result {
            tracing::error!(
                template = name,
                content_hash,
                %error,
                dir = %build_dir.display(),
                "failed to remove template build subvolume before DB cleanup"
            );
            return Err(Status::internal(format!(
                "remove template build dir {}: {error}",
                build_dir.display()
            )));
        }
        Ok(())
    }
}

/// Cap on the node-mgr `BtrfsDeleteTemplateSubvolume` legs of a delete
/// RPC — one shared budget per delete, not per subvolume, so a
/// template with many builds cannot multiply it. Deliberately shorter
/// than the node-mgr client's ~10 s connect retry budget: the callers
/// of the delete RPCs budget ~10 s end-to-end, so letting the RPC legs
/// retry that long would starve the local fallback entirely whenever
/// node-manager is down or mid-rollout.
pub(super) const NODEMGR_SUBVOLUME_DELETE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2);

use super::*;

impl TemplateManagerService {
    pub(super) async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<Response<CreatePoolResponse>, Status> {
        self.authorize_peer(&request, "CreatePool", ALLOW_TEMPLATE_POOL_OPERATOR)?;
        let publish_node_state = !crate::node_state_sync::should_skip_node_publish(&request);
        let req = request.get_ref();
        validate_template_name(&req.template)?;

        if self.template_guest_image(&req.template).await?.is_some() {
            return Err(Status::failed_precondition(
                "firmware guests do not support warm pools",
            ));
        }
        let requested_shape = optional_pool_shape(req.vcpu, req.memory_mib)?;
        let (existing_pool, settings, runtime_defaults) = {
            let db = Arc::clone(&self.db);
            let template = req.template.clone();
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| crate::db::mutex_poison_error("create pool template lookup"))?;
                let existing_pool = crate::db::get_pool(&conn, &template)?;
                let settings = crate::db::get_template_settings(&conn, &template)?;
                let runtime_defaults = crate::pool_manager::pool_base_build(&conn, &template)?
                    .map(|build| template_runtime_defaults(&template, &build.content_hash))
                    .unwrap_or_default();
                Ok::<_, rusqlite::Error>((existing_pool, settings, runtime_defaults))
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("db: {e}")))?
        };

        let boot_mode = if req.boot_mode.trim().is_empty() {
            existing_pool
                .as_ref()
                .map(|pool| pool.boot_mode.clone())
                .unwrap_or_else(|| "cold".to_string())
        } else {
            req.boot_mode.trim().to_string()
        };

        if boot_mode != "snapshot" && boot_mode != "cold" {
            return Err(Status::invalid_argument(
                "boot_mode must be 'snapshot' or 'cold'",
            ));
        }
        let target_count = optional_positive_or_default(
            "target_count",
            req.target_count,
            existing_pool.as_ref().map_or(3, |pool| pool.target_count),
        )?;
        let (vcpu, requested_memory_mib) = resolve_pool_shape(
            requested_shape,
            settings.as_ref(),
            existing_pool.as_ref(),
            runtime_defaults,
        );
        let resolved_memory =
            resolve_sandbox_memory(requested_memory_mib, self.sbx_connect_retry_policy).await?;
        let memory_mib = resolved_memory.effective_memory_mib;
        if memory_mib != requested_memory_mib {
            tracing::info!(
                template = %req.template,
                requested_pool_memory_mib = resolved_memory.requested_memory_mib,
                effective_pool_memory_mib = memory_mib,
                sandbox_guest_memory_max_mib = resolved_memory.guest_memory_max_mib,
                sandbox_cgroup_memory_mib = resolved_memory.cgroup_memory_mib,
                sandbox_cgroup_memory_max_mib = resolved_memory.cgroup_memory_max_mib,
                sandbox_memory_headroom_mib = resolved_memory.memory_headroom_mib,
                "clamped template pool memory to sandbox policy"
            );
        }

        // Snapshot boot requires a snapshot to exist for this template's latest build
        if boot_mode == "snapshot" {
            let db = Arc::clone(&self.db);
            let t = req.template.clone();
            let has_snapshot = tokio::task::spawn_blocking(move || {
                let conn = db.lock().map_err(|_| {
                    Status::internal("db mutex poisoned while checking pool snapshot")
                })?;
                let Some(build) = crate::pool_manager::pool_base_build(&conn, &t)
                    .map_err(|e| Status::internal(format!("db get_latest_build: {e}")))?
                else {
                    return Ok::<_, Status>(None);
                };
                crate::db::get_snapshot(&conn, &t, &build.content_hash, vcpu, memory_mib)
                    .map_err(|e| Status::internal(format!("db get_snapshot: {e}")))
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;

            if has_snapshot.is_none() {
                return Err(Status::failed_precondition(format!(
                    "cannot use boot_mode 'snapshot': template '{}' has no snapshot for {} vCPU / {} MiB (set or rebuild base snapshot policy first)",
                    req.template, vcpu, memory_mib
                )));
            }
        }

        let unchanged = existing_pool.as_ref().is_some_and(|pool| {
            pool.target_count == target_count
                && pool.boot_mode == boot_mode
                && pool.vcpu == vcpu
                && pool.memory_mib == memory_mib
        });
        if !unchanged {
            let db = Arc::clone(&self.db);
            let template = req.template.clone();
            let boot_mode = boot_mode.clone();
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| crate::db::mutex_poison_error("create pool insert"))?;
                crate::db::insert_pool(&conn, &template, target_count, &boot_mode, vcpu, memory_mib)
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("db: {e}")))?;
            if publish_node_state {
                crate::node_state_sync::mark_template_dirty(&self.db, &req.template);
            }
        }

        Ok(Response::new(CreatePoolResponse {}))
    }

    pub(super) async fn delete_pool(
        &self,
        request: Request<DeletePoolRequest>,
    ) -> Result<Response<DeletePoolResponse>, Status> {
        self.authorize_peer(&request, "DeletePool", ALLOW_TEMPLATE_POOL_OPERATOR)?;
        let publish_node_state = !crate::node_state_sync::should_skip_node_publish(&request);
        let req = request.get_ref();
        validate_template_name(&req.template)?;

        {
            let db = Arc::clone(&self.db);
            let template = req.template.clone();
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| crate::db::mutex_poison_error("delete pool"))?;
                crate::db::delete_pool(&conn, &template)
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("db: {e}")))?;
        }

        // Delete existing pool sandboxes via sbx-manager
        let mut deleted_sandbox_ids = vec![];
        let sbx_connect = pplx_space_sbx_mgr_client::connect()
            .await
            .map_err(|e| e.to_string());
        if let Ok(mut sbx_client) = sbx_connect
            && let Ok(sandboxes) =
                pplx_space_sbx_mgr_client::list_all_sandboxes(&mut sbx_client).await
        {
            for sbx in sandboxes {
                if sbx.pool_template.as_deref() == Some(req.template.as_str()) {
                    let _ = sbx_client
                        .stop(pplx_space_sbx_mgr_client::proto::StopRequest {
                            sandbox_id: sbx.sandbox_id.clone(),
                            template_filter: None,
                            blocking: true,
                            immediate_kill: false,
                        })
                        .await;
                    let _ = sbx_client
                        .delete(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                            sandbox_id: sbx.sandbox_id.clone(),
                            template_filter: None,
                            blocking: true,
                        })
                        .await;
                    deleted_sandbox_ids.push(sbx.sandbox_id);
                }
            }
        }

        if publish_node_state {
            crate::node_state_sync::mark_template_dirty(&self.db, &req.template);
        }

        Ok(Response::new(DeletePoolResponse {
            deleted_sandbox_ids,
        }))
    }

    pub(super) async fn pool_status(
        &self,
        request: Request<PoolStatusRequest>,
    ) -> Result<Response<PoolStatusResponse>, Status> {
        self.authorize_peer(&request, "PoolStatus", ALLOW_TEMPLATE_OBSERVER)?;
        let req = request.get_ref();
        let omit_sandbox_counts = req.omit_sandbox_counts;
        let template = req
            .template
            .as_deref()
            .filter(|template| !template.is_empty());
        if let Some(template) = template {
            validate_template_name(template)?;
        }

        let pools = {
            let db = Arc::clone(&self.db);
            let template = template.map(str::to_string);
            tokio::task::spawn_blocking(move || {
                let conn = db
                    .lock()
                    .map_err(|_| crate::db::mutex_poison_error("pool status lookup"))?;
                if let Some(template) = template {
                    crate::db::get_pool(&conn, &template).map(|opt| match opt {
                        Some(p) => vec![p],
                        None => vec![],
                    })
                } else {
                    crate::db::get_all_pools(&conn)
                }
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("db: {e}")))?
        };

        // Fetch sandbox list from sbx-manager to compute pool counts
        let sbx_sandboxes = self.pool_count_sandboxes(omit_sandbox_counts).await;

        for sandbox in &sbx_sandboxes {
            sandbox
                .observed_lifecycle()
                .map_err(|error| Status::internal(error.to_string()))?;
        }

        let pool_infos = pools
            .into_iter()
            .map(|p| {
                let pool_sbxs: Vec<_> = sbx_sandboxes
                    .iter()
                    .filter(|s| s.pool_template.as_deref() == Some(p.template.as_str()))
                    .collect();
                let current_count =
                    u32::try_from(pool_sbxs.iter().filter(|s| s.observed_lifecycle().is_ok_and(|state| state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Paused)).count())
                        .map_err(|_| {
                            Status::internal(format!(
                                "pool '{}' has too many paused sandboxes to report",
                                p.template
                            ))
                        })?;
                let pending_count = u32::try_from(
                    pool_sbxs
                        .iter()
                        .filter(|s| {
                            s.observed_lifecycle().is_ok_and(|state| state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Creating)
                                || s.observed_lifecycle().is_ok_and(|state| state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Created)
                                || s.observed_lifecycle().is_ok_and(|state| state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Starting)
                                || s.observed_lifecycle().is_ok_and(|state| state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Pausing)
                        })
                        .count(),
                )
                .map_err(|_| {
                    Status::internal(format!(
                        "pool '{}' has too many pending sandboxes to report",
                        p.template
                    ))
                })?;
                let sandbox_ids = pool_sbxs.iter().map(|s| s.sandbox_id.clone()).collect();
                Ok::<_, Status>(crate::proto::PoolInfo {
                    template: p.template,
                    target_count: p.target_count,
                    current_count,
                    pending_count,
                    sandbox_ids,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(PoolStatusResponse { pools: pool_infos }))
    }
}

fn optional_pool_shape(
    vcpu: Option<u32>,
    memory_mib: Option<u32>,
) -> Result<Option<(u32, u32)>, Status> {
    match (vcpu, memory_mib) {
        (None, None) => Ok(None),
        (Some(0), _) => Err(Status::invalid_argument(
            "vcpu must be greater than 0 when set",
        )),
        (_, Some(0)) => Err(Status::invalid_argument(
            "memory_mib must be greater than 0 when set",
        )),
        (Some(vcpu), Some(memory_mib)) => Ok(Some((vcpu, memory_mib))),
        _ => Err(Status::invalid_argument(
            "vcpu and memory_mib must be set together",
        )),
    }
}

pub(super) fn resolve_pool_shape(
    requested_shape: Option<(u32, u32)>,
    settings: Option<&crate::db::TemplateSettingsRow>,
    existing_pool: Option<&crate::db::PoolRow>,
    runtime_defaults: TemplateRuntimeDefaults,
) -> (u32, u32) {
    let vcpu = requested_shape
        .map(|(vcpu, _)| vcpu)
        .or_else(|| settings.map(|settings| settings.snapshot_vcpu))
        .or_else(|| existing_pool.map(|pool| pool.vcpu))
        .or(runtime_defaults
            .vcpu
            .filter(|value| *value >= MIN_BASE_SNAPSHOT_VCPU))
        .unwrap_or(DEFAULT_BASE_SNAPSHOT_VCPU);
    let memory_mib = requested_shape
        .map(|(_, memory_mib)| memory_mib)
        .or_else(|| settings.map(|settings| settings.snapshot_memory_mib))
        .or_else(|| existing_pool.map(|pool| pool.memory_mib))
        .or(runtime_defaults
            .memory_mib
            .filter(|value| *value >= MIN_BASE_SNAPSHOT_MEMORY_MIB))
        .unwrap_or(DEFAULT_BASE_SNAPSHOT_MEMORY_MIB);
    (vcpu, memory_mib)
}

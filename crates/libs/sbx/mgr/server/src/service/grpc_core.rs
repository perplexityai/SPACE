use super::activation_support::{
    IdentityFreeDirectActivationAuthority, internal_request, sandbox_event_trigger,
    space_api_route_identity_to_wire, wake_target_state_from_proto,
};
use super::cache_cleanup::{ReconcileSource, cleanup_orphaned_dirs};
use super::cleanup::{AsyncCleanupRequest, SynchronousCleanupRequest};
use super::cleanup_support::{CleanupCompletion, CleanupLockTryResult};
use super::create_support::*;
use super::network_policy::template_network_policy_from_proto;
use super::network_runtime::require_local_authority_peer;
use super::start_support::SandboxStartPath;
use super::tail_support::*;
use super::validation::{
    canonical_batched_sandbox_ids, reject_incompatible_template_arch,
    reject_incompatible_template_spaced_compatibility,
};
use super::*;

use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_node_mgr_client::proto::{
    GetDiskUsageRequest as NodeGetDiskUsageRequest,
    MissingDiskUsageReason as NodeMissingDiskUsageReason,
};

/// Project an on-node sandbox row onto the cheap wire inventory shape shared by
/// `list` and `list_build_referencing_sandboxes`. Keeping one projection means
/// a new `SandboxListElement` field is wired in both RPCs at once.
fn sandbox_list_element(row: &crate::db::SandboxListRow) -> SandboxListElement {
    SandboxListElement {
        sandbox_id: row.id.clone(),
        template: Some(row.template.clone()),
        lifecycle: Some(crate::proto::SandboxLifecycleState::from(row.state) as i32),
        image_hash: row.template_hash.clone(),
        pool_template: row.pool_template.clone(),
        runtime: Some(row.runtime.clone()),
        created_at: Some(row.created_at.clone()),
        owner: Some(row.user.to_string()),
    }
}

/// Bounds the opaque List cursor, which carries one sandbox ID.
const MAX_LIST_PAGE_TOKEN_BYTES: usize = 256;

/// List page size when the request leaves `page_size` unset (zero).
const DEFAULT_LIST_PAGE_SIZE: usize = pplx_space_common_core::inventory::READ_BATCH_ITEMS;

/// Assemble one List page within the encoded budget. The cursor advances past
/// every row the page accounts for, including rows the owner filter hides, so
/// a filtered page may be empty yet still continue the scan.
fn list_page_response(
    page: crate::db::inventory_read::ListPage,
    template_manager_peer: bool,
) -> Result<Response<ListResponse>, Status> {
    use crate::db::inventory_read::ListPageRow;
    use pplx_space_common_core::inventory::{InventoryLimitExceeded, MAX_ENCODED_BYTES};

    // Leave room for the cursor field beside the elements.
    let element_budget = MAX_ENCODED_BYTES - (MAX_LIST_PAGE_TOKEN_BYTES + 8);
    let mut sandboxes = Vec::new();
    let mut encoded = 0usize;
    let mut cursor: Option<&str> = None;
    let mut more = page.more;
    for row in &page.rows {
        let element = match row {
            ListPageRow::Decoded(row) => {
                if template_manager_peer && row.user.kind() != SandboxOwnerKind::TemplateManager {
                    cursor = Some(&row.id);
                    continue;
                }
                sandbox_list_element(row)
            }
            // Owner filtering must not treat an undecodable row as another
            // owner's; other observers see its identity instead.
            ListPageRow::Undecodable { error, .. } if template_manager_peer => {
                return Err(Status::internal(format!(
                    "db list failed: {}",
                    crate::db::describe_error(error)
                )));
            }
            ListPageRow::Undecodable { id, state, error } => {
                tracing::warn!(
                    sandbox_id = %id,
                    error = %crate::db::describe_error(error),
                    "listing sandbox identity after a row decode failure"
                );
                SandboxListElement {
                    sandbox_id: id.clone(),
                    lifecycle: Some(
                        state
                            .as_deref()
                            .and_then(|state| {
                                crate::proto::SandboxLifecycleState::from_label(state).ok()
                            })
                            .unwrap_or(crate::proto::SandboxLifecycleState::Unspecified)
                            as i32,
                    ),
                    ..SandboxListElement::default()
                }
            }
        };
        let len = prost::Message::encoded_len(&element);
        let framed = 1 + prost::length_delimiter_len(len) + len;
        if encoded + framed > element_budget {
            if cursor.is_none() {
                return Err(crate::inventory_budget::exceeded(InventoryLimitExceeded));
            }
            more = true;
            break;
        }
        encoded += framed;
        sandboxes.push(element);
        cursor = Some(row.id());
    }
    let next_page_token = match (more, cursor) {
        (false, _) => String::new(),
        (true, Some(cursor)) => cursor.to_owned(),
        (true, None) => return Err(Status::internal("List page has no cursor")),
    };
    crate::inventory_budget::response(ListResponse {
        sandboxes,
        next_page_token,
    })
}

pub(super) fn pool_adoption_request(
    req: &CreateRequest,
    template: String,
    owner: String,
    auto_lifecycle: crate::proto::AutoLifecyclePolicy,
    auto_lifecycle_authority: i32,
    content_hash: Option<String>,
) -> AdoptPoolSandboxRequest {
    AdoptPoolSandboxRequest {
        template,
        owner,
        env: req.env.clone(),
        auto_lifecycle: Some(auto_lifecycle),
        network_policy: req.network_policy.clone(),
        network_policy_generation: req.network_policy_generation,
        network_policy_authority: req.network_policy_authority,
        auto_lifecycle_authority,
        content_hash,
        template_network_policy: req.template_network_policy.clone(),
        template_network_policy_generation: req.template_network_policy_generation,
        template_network_policy_authority: req.template_network_policy_authority,
        route_identity: req.route_identity.clone(),
    }
}

impl SandboxManagerService {
    pub(super) fn finish_unpersisted_delete_cleanup(
        &self,
        sandbox_id: &str,
        persistence_error: Option<String>,
    ) -> Result<(), Status> {
        self.auto_lifecycle.remove_sandbox(sandbox_id);
        if let Some(error) = persistence_error {
            return Err(Status::internal(format!(
                "runtime cleanup completed but deleting transition could not be persisted for sandbox {sandbox_id}: {error}"
            )));
        }
        Ok(())
    }

    /// Resolve and verify the exact environment within 30 seconds, including connect.
    async fn resolve_create_environment(
        &self,
        reference: pplx_space_common_core::types::EnvironmentReference,
    ) -> Result<pplx_space_node_mgr_client::environment_selection::SelectedEnvironment, Status>
    {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut node = connect_node(&self.node_manager)
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            super::environment_selection::resolve_exact(&mut node, reference).await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("environment admission timed out"))?
    }

    pub(super) async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let peer = self.authorize_peer(&request, "Create", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let mut request = request;
        let environment = super::validation::normalize_environment_input(request.get_mut())?;
        let span = tracing::info_span!("sbxmgr.create", template = %request.get_ref().template);
        let publish_node_state = !should_skip_node_publish(&request);
        let mut result = async move {
            let req = request.get_ref();
            let sandbox_owner = Self::authorize_create_sandbox_owner(&peer, req)?;
            let (requested_boot_mode, requested_id) = if environment.is_some() {
                super::validation::validate_selected_create_fields(req)?;
                validate_create_request_fields(req)?
            } else {
                validate_create_request(req)?
            };
            if req.network_policy.is_some() {
                let authority = crate::db::PolicyAuthority::try_from(
                    req.network_policy_authority,
                )
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
                require_local_authority_peer(peer.principal(), authority)?;
            }
            if req.auto_lifecycle.is_some() {
                let authority =
                    crate::db::PolicyAuthority::try_from(req.auto_lifecycle_authority)
                        .map_err(|error| Status::invalid_argument(error.to_string()))?;
                require_local_authority_peer(peer.principal(), authority)?;
            }
            let verified = if let Some(reference) = environment {
                let selected = self.resolve_create_environment(reference).await?;
                let pplx_space_common_core::types::RuntimeBinding::ManagedRelease(release) = &selected.definition.descriptor().spec().runtime;
                request.get_mut().runtime = release.runtime.kind.alias().to_owned();
                Some(selected)
            } else { None };
            let req = request.get_ref();
            let template = &req.template;
            let runtime = &req.runtime;

            use pplx_space_common_svc::template_lookup::TemplateLookupScope;
            let mut scope = template_lookup_scope(&sandbox_owner, req.sandbox_id.as_deref())?;
            if peer.principal() == UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI) {
                scope = match (scope, TemplateLookupScope::from_request(&request)?) {
                    (TemplateLookupScope::Sandbox { id, .. }, Some(TemplateLookupScope::Node(address))) =>
                        TemplateLookupScope::Sandbox { id, caller_node: Some(address) },
                    (scope @ TemplateLookupScope::Sandbox { .. }, Some(TemplateLookupScope::Cluster)) => scope,
                    (_, Some(scope @ (TemplateLookupScope::Node(_) | TemplateLookupScope::Cluster))) => scope,
                    (scope, None) => {
                        pplx_space_metrics::auth::template_lookup_compatibility("missing_scope");
                        scope
                    }
                    _ => return Err(Status::permission_denied("invalid remote template caller scope")),
                };
            }

            // Resolve template to content-addressed paths via template-manager.
            // If caller pinned a content_hash, use it; otherwise resolve newest.
            let mut resolved_template = resolve_template_paths(
                self.template_resolver.as_ref(),
                pplx_space_template_mgr_client::proto::TemplateRef {
                    name: template.to_string(),
                    content_hash: req.content_hash.clone(),
                    tag: None,
                },
                scope,
            )
            .await?;
            if let Some(selected) = &verified {
                selected.definition.check_cold_boot_source(&resolved_template.execution)
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                resolved_template.execution = selected.execution.clone();
            }
            super::validation::validate_environment_create(
                req, environment, &resolved_template.execution,
            )?;
            if let Some(policy) = template_network_policy_from_proto(
                req.template_network_policy.as_ref(),
                req.template_network_policy_generation,
                req.template_network_policy_authority,
            )? {
                self.template_network_policies
                    .require_hint(template, &policy, req.network_policy.is_none())
                    .await?;
            }
            reject_incompatible_template_arch(
                template,
                resolved_template
                    .content_hash
                    .as_deref()
                    .unwrap_or("<unhashed>"),
                &resolved_template.arch,
            )?;
            let image = resolved_template.guest_image;
            if image.expects_agent() {
                reject_incompatible_template_spaced_compatibility(
                template,
                resolved_template
                    .content_hash
                    .as_deref()
                    .unwrap_or("<unhashed>"),
                resolved_template.spaced_version.as_deref(),
                    resolved_template.spaced_compatibility_family.as_deref(),
                )?;
            }

            let target = super::create_support::CreateTarget {
                snapshot_dir: resolved_template.snapshot_dir.as_deref(),
                execution: &resolved_template.execution,
                selected: environment.is_some(),
            };
            let (mut effective_mode, mut intent) =
                self.resolve_create_intent(req, requested_boot_mode, requested_id, target)?;
            if effective_mode == BootMode::BaseSnapshot {
                self.ensure_snapshot_restore_supported("create_base_snapshot")?;
            }
            let pool_template = req
                .pool_template
                .as_deref()
                .filter(|value| !value.is_empty());
            let auto_lifecycle =
                self.resolve_create_auto_lifecycle_policy(req, &resolved_template, pool_template)?;
            super::validation::validate_runtime_lifecycle_policy(resolved_template.execution.runtime().kind, &auto_lifecycle)?;
            let auto_lifecycle_authority = if req.auto_lifecycle.is_some() {
                crate::db::PolicyAuthority::try_from(req.auto_lifecycle_authority)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
            } else {
                resolved_template.default_auto_lifecycle_authority
            };

            // Pool path: adopt a paused pool sandbox
            let mut resp_from_pool = None;
            if matches!(intent, SandboxCreateIntent::AdoptPool) {
                super::validation::validate_execution_lifecycle_policy(Some(&resolved_template.execution), &auto_lifecycle)?;
                let adopt_req = internal_request(pool_adoption_request(
                    req,
                    template.clone(),
                    sandbox_owner.to_string(),
                    policy_to_proto(&auto_lifecycle),
                    auto_lifecycle_authority.as_proto_i32(),
                    resolved_template.content_hash.clone(),
                ));
                resp_from_pool = match self.adopt_pool_sandbox_outcome(adopt_req).await? {
                    super::grpc_pool_wait_clone::PoolAdoption::Adopted(resp) => Some(resp),
                    // Pool is an optimization: a donor the node won't run is
                    // skipped like any other ineligible donor.
                    super::grpc_pool_wait_clone::PoolAdoption::NoUsableDonor(status) => {
                        tracing::info!(template = %template, %status, "pool donor refused; creating without the pool");
                        (effective_mode, intent) = self.resolve_create_intent(
                            req,
                            BootMode::BaseSnapshot,
                            requested_id,
                            target,
                        )?;
                        if effective_mode == BootMode::BaseSnapshot {
                            self.ensure_snapshot_restore_supported("create_base_snapshot")?;
                        }
                        None
                    }
                };
            }
            if let Some(resp) = resp_from_pool {
                let sbx_id = resp.into_inner().sandbox_id;

                if req.autostart {
                    // Pool members were fully started and paused after readiness before
                    // adoption. Do not spend create --blocking latency on a fresh guest
                    // probe here; resume_sandbox is the handoff point.
                    self.resume(internal_request(ResumeRequest {
                        activation_auto_lifecycle: None,
                        expected_execution_generation: None,
                        sandbox_id: sbx_id.clone(),
                        blocking: false,
                        ready_timeout_secs: None,
                    }))
                    .await?;
                    let sandbox = self.lookup_sandbox(&sbx_id)?;
                    let log_path = format!("{RUN_DIR}/{}/fc.log", sbx_id);
                    let namespace = format!("sns-{}", sandbox.uuid8);
                    return Ok(Response::new(CreateResponse {
                        environment: None,
                        sandbox_id: sbx_id,
                        pid: sandbox.pid,
                        guest_ip: Some("169.254.0.21".to_string()),
                        namespace: Some(namespace),
                        log_path: Some(log_path),
                        vsock_uds_path: sandbox.vsock_uds_path,
                        boot_mode_used: effective_mode.into(),
                        reused_existing: false,
                        route_identity: space_api_route_identity_to_wire(
                            &sandbox.space_api_route_identity,
                        ),
                    }));
                }
                return Ok(Response::new(CreateResponse {
                    sandbox_id: sbx_id,
                    boot_mode_used: effective_mode.into(),
                    ..Default::default()
                }));
            }

            let SandboxCreateIntent::Create { requested_id } = intent else {
                return Err(Status::internal("pool adoption did not complete"));
            };
            if let Some(requested_id) = requested_id {
                let template_hash = template_create_hash(
                    &resolved_template.template_dir,
                    resolved_template.content_hash.as_deref(),
                )?;
                if let Some(mut existing) = self.lookup_requested_sandbox(requested_id)? {
                    let identity = super::activation_support::space_api_route_identity_from_create_request(req)?;
                    super::activation_support::validate_route_identity_sandbox(&identity, &existing.id)?;
                    let requested = RequestedSandboxCreateSpec {
                        template,
                        template_hash: &template_hash,
                        user: &sandbox_owner,
                        runtime,
                        rootfs_size_mb: req.rootfs_size_mb,
                        pool_template,
                        volume_encryption: req.volume_encryption,
                        auto_lifecycle: &auto_lifecycle,
                        environment,
                    };
                    let auto_lifecycle_changed =
                        requested_sandbox_create_auto_lifecycle_changed(&existing, &requested);
                    let mismatches = requested_sandbox_create_mismatches(&existing, &requested);
                    if !mismatches.is_empty() {
                        return Err(Status::already_exists(format!(
                            "requested sandbox {} exists with incompatible create parameters: {}",
                            existing.id,
                            mismatches.join(", ")
                        )));
                    }

                    if matches!(
                        existing.state,
                        SandboxState::CleanupRequired | SandboxState::CleanupFailed
                    ) {
                        return Err(Status::unavailable(format!(
                            "requested sandbox {} is still completing activation cleanup; retry",
                            existing.id
                        )));
                    }
                    if !requested_create_reuse_state(existing.state) {
                        return Err(Status::already_exists(format!(
                            "requested sandbox {} already exists in state {}",
                            existing.id, existing.state
                        )));
                    }

                    if auto_lifecycle_changed {
                        let sandbox_id = existing.id.clone();
                        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox_id);
                        let _guard = wake_lock.lock().await;
                        {
                            let db = self.db_conn()?;
                            existing = crate::db::read::sandbox(&db, &sandbox_id)
                                .map_err(|e| Status::internal(format!("db lookup failed: {e}")))?
                                .ok_or_else(|| {
                                    Status::not_found(format!(
                                        "requested sandbox {sandbox_id} disappeared"
                                    ))
                                })?;
                            let mismatches =
                                requested_sandbox_create_mismatches(&existing, &requested);
                            if !mismatches.is_empty() {
                                return Err(Status::already_exists(format!(
                                    "requested sandbox {} exists with incompatible create parameters: {}",
                                    existing.id,
                                    mismatches.join(", ")
                                )));
                            }
                            if matches!(
                                existing.state,
                                SandboxState::CleanupRequired | SandboxState::CleanupFailed
                            ) {
                                return Err(Status::unavailable(format!(
                                    "requested sandbox {} is still completing activation cleanup; retry",
                                    existing.id
                                )));
                            }
                            if !requested_create_reuse_state(existing.state) {
                                return Err(Status::already_exists(format!(
                                    "requested sandbox {} already exists in state {}",
                                    existing.id, existing.state
                                )));
                            }
                            super::validation::validate_execution_lifecycle_policy(existing.execution_metadata.as_ref(), &auto_lifecycle)?;
                            if existing.auto_lifecycle != auto_lifecycle {
                                update_existing_sandbox_create_auto_lifecycle(
                                    &db,
                                    &sandbox_id,
                                    existing.state,
                                    &auto_lifecycle,
                                )?;
                                existing = crate::db::read::sandbox(&db, &sandbox_id)
                                    .map_err(|e| {
                                        Status::internal(format!("db lookup failed: {e}"))
                                    })?
                                    .ok_or_else(|| {
                                        Status::not_found(format!(
                                            "requested sandbox {sandbox_id} disappeared"
                                        ))
                                    })?;
                            }
                        }
                    }

                    // req.env is neither validated nor applied on reuse: the
                    // row's env is authoritative (warm-pool adoption and the
                    // Update RPC rewrite it after create) and a restore pod's
                    // request env is a lossy reconstruction. See the doc
                    // comment on requested_sandbox_create_mismatches. Counts
                    // only — env values are sensitive.
                    let request_env_differs = req.env.len() != existing.process_env.len()
                        || req
                            .env
                            .iter()
                            .any(|(k, v)| existing.process_env.get(k) != Some(v));
                    if request_env_differs {
                        tracing::info!(
                            sandbox_id = %existing.id,
                            row_env_count = existing.process_env.len(),
                            request_env_count = req.env.len(),
                            "request env differs from sandbox row env on requested-id reuse; keeping row env"
                        );
                    }

                    return Ok(Response::new(create_response_for_existing_sandbox(
                        &existing,
                        effective_mode,
                    )));
                }
            }

            // Reused IDs return above before selection. Cluster placement and
            // pool identity keep their exact environment, which the node must
            // still admit before the row is persisted.
            if environment.is_none() && resolved_template.execution.environment().is_some() {
                if super::validation::allows_node_local_environment_selection(req) {
                    resolved_template.execution = self.select_compatible_execution(
                        &resolved_template.execution,
                        pplx_space_node_mgr_client::proto::ExecutionEnvironmentSelection::ColdBoot,
                    ).await?;
                } else {
                    self.admit_exact_execution(&resolved_template.execution).await?;
                }
            }

            super::validation::validate_execution_lifecycle_policy(Some(&resolved_template.execution), &auto_lifecycle)?;

            self.create_with_id(CreateWithIdArgs {
                guest_image: resolved_template.guest_image,
                req,
                sandbox_owner: &sandbox_owner,
                effective_mode,
                id: requested_id.unwrap_or_else(Uuid::now_v7).to_string(),
                template_dir: &resolved_template.template_dir,
                execution: resolved_template
                    .execution
                    .environment()
                    .map(|_| &resolved_template.execution),
                snapshot_dir_resolved: resolved_template.snapshot_dir.as_deref(),
                snapshot_vcpu: resolved_template.snapshot_vcpu,
                content_hash_resolved: resolved_template.content_hash.as_deref(),
                auto_lifecycle: &auto_lifecycle,
                auto_lifecycle_authority,
                default_network_policy_authority: resolved_template.default_network_policy_authority,
            })
            .await
        }
        .instrument(span)
        .await;
        if let Ok(response) = result.as_mut() {
            response.get_mut().environment = environment.map(|reference| reference.to_string());
        }
        if publish_node_state && let Ok(response) = result.as_ref() {
            mark_sandbox_dirty(&self.db, &response.get_ref().sandbox_id);
        }
        result
    }

    pub(super) async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        self.start_with_runtime_upgrade(request, None).await
    }

    /// [`Self::start`] as the cold boot of `runtime_upgrade`, which may pass
    /// that upgrade's activation fence. The caller's authorization is checked
    /// exactly as for any Start.
    pub(super) async fn start_with_runtime_upgrade(
        &self,
        request: Request<StartRequest>,
        runtime_upgrade: Option<crate::db::RuntimeUpgradeOperationId>,
    ) -> Result<Response<StartResponse>, Status> {
        let peer = self.authorize_peer(&request, "Start", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let req = request.get_ref().clone();
        if req.activation_auto_lifecycle.is_some() {
            self.authorize_peer(
                &request,
                "Start.activation_auto_lifecycle",
                ALLOW_SANDBOX_START_CONTEXT,
            )?;
        }
        let target_state = wake_target_state_from_proto(req.target_state)?;
        self.authorize_netns_start_context(&request, "Start.netns_path", &req.netns_path)?;
        if let Some(context) = &req.restore_artifact_as_new {
            self.authorize_peer(
                &request,
                "Start.restore_artifact_as_new",
                ALLOW_SANDBOX_START_CONTEXT,
            )?;
            Self::authorize_sandbox_owner_label(&peer, &context.target_owner)?;
        }
        if req.operation_activation_attempt_id.is_some() {
            self.authorize_peer(
                &request,
                "Start.operation_activation_attempt_id",
                ALLOW_SANDBOX_START_CONTEXT,
            )?;
        }
        // Restores may target a sandbox that does not have a local row yet;
        // start_sandbox stages the durable artifact and inserts it.
        let checkpoint = req.checkpoint.as_deref().unwrap_or("");
        let identity_free_direct_authority = match self.lookup_sandbox(&req.sandbox_id) {
            Ok(sandbox) => {
                self.authorize_template_manager_sandbox(&peer, &sandbox, "Start")?;
                let authority = Self::identity_free_start_authority(&peer, &sandbox);
                if req.route_identity.is_none()
                    && true
                    && authority == IdentityFreeDirectActivationAuthority::Denied
                {
                    return Err(Status::failed_precondition(
                        "identity-free operator Start/Restore cannot advance a control-plane-owned sandbox under strict route identity enforcement; use the SPACE API",
                    ));
                }
                authority
            }
            Err(status)
                if status.code() == tonic::Code::NotFound
                    && Self::start_can_materialize_missing_sandbox(&req) =>
            {
                IdentityFreeDirectActivationAuthority::Denied
            }
            Err(status) => return Err(status),
        };
        let restrict_to_template_manager_owned = Self::is_template_manager_peer(&peer);
        let span = tracing::info_span!(
            "sbxmgr.start",
            sandbox_id = %req.sandbox_id,
            blocking = req.blocking,
            from_warm = req.from_warm,
            start_path = SandboxStartPath::from_request(&req).as_str(),
            start_execution_path = tracing::field::Empty,
            checkpoint_present = !checkpoint.is_empty(),
            target_state = %target_state,
            template = tracing::field::Empty,
            content_hash = tracing::field::Empty,
            runtime = tracing::field::Empty,
            initial_state = tracing::field::Empty,
            has_rootfs = tracing::field::Empty,
            has_snapshot = tracing::field::Empty,
        );
        let sandbox_id = req.sandbox_id.clone();
        let publish_node_state = !should_skip_node_publish(&request);
        let result = async move {
            self.start_sandbox_with_upgrade_authority(
                req,
                restrict_to_template_manager_owned,
                identity_free_direct_authority,
                runtime_upgrade,
            )
            .await
        }
        .instrument(span)
        .await;
        if publish_node_state && result.is_ok() {
            mark_sandbox_dirty(&self.db, &sandbox_id);
        }
        result
    }

    pub(super) fn start_can_materialize_missing_sandbox(req: &StartRequest) -> bool {
        req.checkpoint
            .as_deref()
            .is_some_and(|checkpoint| !checkpoint.is_empty())
            || req.restore_artifact_as_new.is_some()
    }

    pub(super) async fn stop(
        &self,
        request: Request<StopRequest>,
    ) -> Result<Response<StopResponse>, Status> {
        let peer = self.authorize_peer(&request, "Stop", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let _request_permit = self.cleanup_request_permit().await?;
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        let requested_id = req.sandbox_id.parse::<SandboxId>().map_err(|error| {
            Status::invalid_argument(format!("Stop requires one exact sandbox UUID: {error}"))
        })?;
        let template_filter = req.template_filter.as_deref().filter(|s| !s.is_empty());
        let blocking = req.blocking;
        let immediate_kill = req.immediate_kill;

        let to_stop = self
            .resolve_lifecycle_target(
                requested_id,
                template_filter,
                Self::is_template_manager_peer(&peer),
                true,
            )
            .await?;

        let mut stopped_ids: Vec<String> = Vec::new();
        if let Some((id, runtime_label)) = &to_stop {
            let mut details = SandboxEventDetails::default();
            if let Some(trigger) = sandbox_event_trigger(&request) {
                details = details.trigger(trigger);
            }
            emit_sandbox_event(
                id,
                SandboxEventType::StopRequested,
                SandboxEventSource::SbxManager,
                details,
            );
            let (sandbox_id, uuid8) = Self::cleanup_identity(id, runtime_label.as_ref())?;
            let completion = CleanupCompletion::MarkStopped;
            // Admission decides from the state it supersedes, under its fence.
            let guest_flush = if immediate_kill {
                crate::db::cleanup_jobs::GuestFlush::Forbidden
            } else {
                crate::db::cleanup_jobs::GuestFlush::BeforeTermination
            };
            let admission = self
                .admit_selected_cleanup(
                    sandbox_id,
                    completion,
                    immediate_kill,
                    !blocking,
                    template_filter,
                    Self::is_template_manager_peer(&peer),
                )
                .await
                .map_err(|error| error.into_status())?;

            // Stop sidecar monitoring and snapshot scheduling
            self.watchdog.remove_sandbox(id).await;
            self.snapshot_scheduler.remove_sandbox(id).await;

            if admission.is_unpersisted_for_sandbox(sandbox_id)? {
                let persistence_error = self
                    .run_cleanup_after_persistence_failure(
                        SynchronousCleanupRequest {
                            sandbox_id: id,
                            uuid8: uuid8.as_str(),
                            keep_filesystem: true,
                            context: "cleanup after unpersisted stop",
                            completion,
                            published_artifact: None,
                            immediate_kill,
                            guest_flush,
                        },
                        admission,
                    )
                    .await?;
                if let Some(error) = persistence_error {
                    return Err(Status::internal(format!(
                        "runtime cleanup completed but stopping transition could not be persisted for sandbox {id}: {error}"
                    )));
                }
            } else if blocking {
                self.run_admitted_stop_to_completion(
                    SynchronousCleanupRequest {
                        sandbox_id: id,
                        uuid8: uuid8.as_str(),
                        keep_filesystem: true,
                        context: "cleanup after stop",
                        completion,
                        published_artifact: None,
                        immediate_kill,
                        // Admission persisted the stop's decision; this asks nothing.
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    admission,
                )
                .await?;
            } else {
                self.schedule_admitted_async_cleanup(
                    AsyncCleanupRequest {
                        sandbox_id,
                        uuid8,
                        keep_filesystem: true,
                        context: "cleanup after stop",
                        completion,
                        immediate_kill,
                        // Admission persisted the stop's decision; this asks nothing.
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                        persist_intent: false,
                        cleanup_id: None,
                    },
                    admission,
                )
                .await;
            }

            stopped_ids.push(id.to_string());
        }

        if publish_node_state {
            mark_sandboxes_dirty(&self.db, stopped_ids.clone());
        }
        Ok(Response::new(StopResponse { stopped_ids }))
    }

    pub(super) async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.delete_with_auto_lifecycle_expectation(request, None)
            .await
    }

    pub(super) async fn delete_with_auto_lifecycle_expectation(
        &self,
        request: Request<DeleteRequest>,
        expectation: Option<crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let peer = self.authorize_peer(&request, "Delete", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)?;
        let _request_permit = self.cleanup_request_permit().await?;
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        let requested_id = req.sandbox_id.parse::<SandboxId>().map_err(|error| {
            Status::invalid_argument(format!("Delete requires one exact sandbox UUID: {error}"))
        })?;
        let template_filter = req.template_filter.as_deref().filter(|s| !s.is_empty());
        let blocking = req.blocking;
        if expectation.is_some() && template_filter.is_some() {
            return Err(Status::internal(
                "auto-lifecycle delete requires one exact sandbox id",
            ));
        }
        if expectation.is_some() && requested_id.to_string() != req.sandbox_id {
            return Err(Status::internal(
                "auto-lifecycle delete requires a canonical sandbox id",
            ));
        }

        let to_delete = self
            .resolve_lifecycle_target(
                requested_id,
                template_filter,
                Self::is_template_manager_peer(&peer),
                false,
            )
            .await?;

        let mut deleted_ids: Vec<String> = Vec::new();

        for (id, runtime_label) in to_delete.iter() {
            let (sandbox_id, uuid8) = Self::cleanup_identity(id, runtime_label.as_ref())?;
            let (clone_disposal, quarantined) = {
                let db = self
                    .db
                    .lock()
                    .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
                let episode = crate::db::read::sandbox_view(&db, id)
                    .cleanup_episode()
                    .strict()
                    .map_err(|e| Status::internal(format!("read clone disposal: {e}")))?
                    .filter(|episode| episode.completion == CleanupCompletion::DeletePoolClone);
                let quarantined = episode.is_some()
                    && crate::db::read::sandbox_view(&db, id)
                        .current_state_label()
                        .map_err(|e| Status::internal(format!("read clone disposal state: {e}")))?
                        .as_deref()
                        == Some(SandboxState::Quarantined.as_str());
                (episode, quarantined)
            };
            if let Some(episode) = clone_disposal {
                if episode.requested_at.is_none() && !quarantined {
                    return Err(Status::failed_precondition(
                        "pool clone publication still owns this sandbox",
                    ));
                }
                if quarantined {
                    if Self::is_template_manager_peer(&peer) || expectation.is_some() {
                        return Err(Status::failed_precondition(
                            "clone disposal hold requires an explicit operator delete after repair",
                        ));
                    }
                    let CleanupLockTryResult::Acquired(_guard) = self.try_cleanup_fence(id) else {
                        return Err(Status::unavailable("clone disposal is busy"));
                    };
                    let db = self
                        .db
                        .lock()
                        .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
                    if let Some(template) = template_filter {
                        Self::check_cleanup_selection(&db, id, Some(template), false)?;
                    }
                    if !crate::db::rearm_prepared_clone_disposal(&db, id, episode.id)
                        .map_err(|e| Status::internal(format!("rearm clone disposal: {e}")))?
                    {
                        return Err(Status::aborted("clone disposal hold changed"));
                    }
                }
                if blocking {
                    self.run_cleanup_episode_to_completion(
                        SynchronousCleanupRequest {
                            sandbox_id: id,
                            uuid8: uuid8.as_str(),
                            keep_filesystem: false,
                            context: "explicit prepared clone disposal",
                            completion: CleanupCompletion::DeletePoolClone,
                            published_artifact: None,
                            immediate_kill: false,
                            guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                        },
                        episode.id,
                    )
                    .await?;
                } else {
                    self.schedule_async_cleanup(AsyncCleanupRequest {
                        sandbox_id,
                        uuid8,
                        keep_filesystem: false,
                        context: "explicit prepared clone disposal",
                        completion: CleanupCompletion::DeletePoolClone,
                        immediate_kill: false,
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                        persist_intent: false,
                        cleanup_id: Some(episode.id),
                    });
                }
                mark_sandbox_dirty(&self.db, id);
                deleted_ids.push(id.clone());
                continue;
            }
            let completion = CleanupCompletion::DeleteRow;
            let admission = match expectation {
                Some(expectation) => {
                    self.admit_auto_lifecycle_delete(sandbox_id, expectation)
                        .await
                }
                None => {
                    self.admit_selected_cleanup(
                        sandbox_id,
                        completion,
                        false,
                        !blocking,
                        template_filter,
                        Self::is_template_manager_peer(&peer),
                    )
                    .await
                }
            }
            .map_err(super::cleanup::CleanupAdmissionError::into_status)?;

            // Admission owns the authoritative lifecycle row under the
            // per-sandbox cleanup lock. Monitoring is removed only after that
            // claim succeeds, so a deferred delete leaves the sandbox intact.
            self.watchdog.remove_sandbox(id).await;
            self.snapshot_scheduler.remove_sandbox(id).await;

            if admission.is_unpersisted_for_sandbox(sandbox_id)? {
                let persistence_error = self
                    .run_cleanup_after_persistence_failure(
                        SynchronousCleanupRequest {
                            sandbox_id: id,
                            uuid8: uuid8.as_str(),
                            keep_filesystem: false,
                            context: "cleanup after unpersisted delete",
                            completion,
                            published_artifact: None,
                            immediate_kill: Self::cleanup_immediate_kill(completion),
                            guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                        },
                        admission,
                    )
                    .await?;
                self.finish_unpersisted_delete_cleanup(id, persistence_error)?;
            } else if blocking {
                self.run_admitted_cleanup_to_completion(
                    SynchronousCleanupRequest {
                        sandbox_id: id,
                        uuid8: uuid8.as_str(),
                        keep_filesystem: false,
                        context: "cleanup after delete",
                        completion,
                        published_artifact: None,
                        immediate_kill: Self::cleanup_immediate_kill(completion),
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    admission,
                )
                .await?;
            } else {
                self.schedule_admitted_async_cleanup(
                    AsyncCleanupRequest {
                        sandbox_id,
                        uuid8,
                        keep_filesystem: false,
                        context: "cleanup after delete",
                        completion,
                        immediate_kill: Self::cleanup_immediate_kill(completion),
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                        persist_intent: false,
                        cleanup_id: None,
                    },
                    admission,
                )
                .await;
            }
            self.auto_lifecycle.remove_sandbox(id);

            deleted_ids.push(id.to_string());
            if publish_node_state {
                mark_sandbox_dirty(&self.db, id);
            }
        }
        Ok(Response::new(DeleteResponse { deleted_ids }))
    }

    pub(super) async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<ListResponse>, Status> {
        let peer = self.authorize_peer(&request, "List", ALLOW_SANDBOX_OBSERVER_OR_TMPLMGR)?;
        let template_manager_peer = Self::is_template_manager_peer(&peer);
        let ListRequest {
            page_size,
            page_token,
        } = request.into_inner();
        if page_token.len() > MAX_LIST_PAGE_TOKEN_BYTES {
            return Err(Status::invalid_argument("page_token is invalid"));
        }
        let limit = match page_size {
            0 => DEFAULT_LIST_PAGE_SIZE,
            size => usize::try_from(size)
                .unwrap_or(usize::MAX)
                .min(pplx_space_common_core::inventory::MAX_ITEMS),
        };
        let page = self
            .storage(move |db| {
                crate::db::scan::list_page(db, &page_token, limit)
                    .map_err(|error| error.into_status())
            })
            .await?;
        list_page_response(page, template_manager_peer)
    }

    pub(super) async fn list_build_referencing_sandboxes(
        &self,
        request: Request<ListBuildReferencingSandboxesRequest>,
    ) -> Result<Response<ListBuildReferencingSandboxesResponse>, Status> {
        self.authorize_peer(
            &request,
            "ListBuildReferencingSandboxes",
            ALLOW_SANDBOX_OBSERVER_OR_TMPLMGR,
        )?;
        let db_rows = self
            .storage(|db| {
                crate::db::scan::all_sandboxes(
                    db,
                    crate::db::scan::RpcListRows {
                        rpc: "ListBuildReferencingSandboxes",
                    },
                )
                .map_err(|error| error.into_status())
            })
            .await?;

        // Deliberately NOT filtered by owner — an adopted pool sandbox is
        // user-owned yet still references its parent build. The only filter is
        // the state scope: rows whose `instance` subvolume is being torn down
        // (`Deleting`) no longer pin the parent.
        let sandboxes = db_rows
            .iter()
            .filter(|row| row.state.holds_parent_referencing_instance_subvolume())
            .map(sandbox_list_element)
            .collect();

        Ok(crate::inventory_budget::unary_list_response(
            "ListBuildReferencingSandboxes",
            ListBuildReferencingSandboxesResponse { sandboxes },
        ))
    }

    pub(super) async fn get_disk_usage(
        &self,
        request: Request<GetDiskUsageRequest>,
    ) -> Result<Response<GetDiskUsageResponse>, Status> {
        self.authorize_peer(&request, "GetDiskUsage", ALLOW_SANDBOX_OBSERVER)?;
        let sandbox_ids = canonical_batched_sandbox_ids(&request.get_ref().sandbox_ids)?;
        if sandbox_ids.is_empty() {
            return Err(Status::invalid_argument("sandbox_ids is required"));
        }

        let mut response = GetDiskUsageResponse::default();
        for batch in sandbox_ids.chunks(pplx_space_common_core::inventory::READ_BATCH_ITEMS) {
            let batch = self.disk_usage_batch(batch.to_vec()).await?;
            response.usage_bytes.extend(batch.usage_bytes);
            response.missing_ids.extend(batch.missing_ids);
        }
        crate::inventory_budget::response(response)
    }

    async fn disk_usage_batch(
        &self,
        sandbox_ids: Vec<String>,
    ) -> Result<GetDiskUsageResponse, Status> {
        let (resolved_ids, mut missing_ids) = self
            .storage(move |db| {
                let mut resolved_ids = Vec::with_capacity(sandbox_ids.len());
                let mut missing_ids = Vec::new();
                for id in sandbox_ids {
                    if crate::db::read::sandbox_view(db, &id)
                        .exists()
                        .map_err(|error| {
                            Status::internal(format!("read sandbox identity: {error}"))
                        })?
                    {
                        resolved_ids.push(id);
                    } else {
                        missing_ids.push(SbxMissingDiskUsageId {
                            id,
                            reason: SbxMissingDiskUsageReason::SandboxNotFound as i32,
                        });
                    }
                }
                Ok((resolved_ids, missing_ids))
            })
            .await?;

        let mut usage_bytes = std::collections::HashMap::new();
        if !resolved_ids.is_empty() {
            let mut client = connect_node(&self.node_manager)
                .await
                .map_err(|e| Status::unavailable(format!("cannot connect to node-manager: {e}")))?;
            let response = client
                .get_disk_usage(NodeGetDiskUsageRequest {
                    sandbox_ids: resolved_ids,
                })
                .await
                .map_err(|error| {
                    Status::new(
                        error.code(),
                        format!("node-manager GetDiskUsage failed: {error}"),
                    )
                })?
                .into_inner();
            usage_bytes = response.usage_bytes;
            missing_ids.extend(response.missing_ids.into_iter().map(|missing| {
                let reason = match NodeMissingDiskUsageReason::try_from(missing.reason)
                    .unwrap_or(NodeMissingDiskUsageReason::Unspecified)
                {
                    NodeMissingDiskUsageReason::InstancePathMissing
                    | NodeMissingDiskUsageReason::Unspecified => {
                        SbxMissingDiskUsageReason::InstancePathMissing
                    }
                };
                SbxMissingDiskUsageId {
                    id: missing.id,
                    reason: reason as i32,
                }
            }));
        }

        Ok(GetDiskUsageResponse {
            usage_bytes,
            missing_ids,
        })
    }

    pub(super) async fn cleanup(
        &self,
        request: Request<CleanupRequest>,
    ) -> Result<Response<CleanupResponse>, Status> {
        self.authorize_peer(&request, "Cleanup", ALLOW_SANDBOX_OPERATOR)?;
        self.retire_rowless_restore_transfer_scopes().await;
        let service = self.clone();
        let cleaned_ids = tokio::task::spawn_blocking(move || {
            let inventory_report = service
                .reconcile_sandbox_inventory(ReconcileSource::Cleanup)
                .map_err(|error| Status::internal(format!("cleanup inventory: {error}")))?;
            let mut cleaned_ids = inventory_report
                .changed_ids()
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>();
            cleaned_ids.extend(cleanup_orphaned_dirs(ReconcileSource::Cleanup));
            Ok::<_, Status>(cleaned_ids)
        })
        .await
        .map_err(|error| Status::internal(format!("cleanup blocking task failed: {error}")))??;

        self.ensure_cleanup_executor();

        Ok(Response::new(CleanupResponse { cleaned_ids }))
    }

    pub(super) async fn list_templates(
        &self,
        request: Request<ListTemplatesRequest>,
    ) -> Result<Response<ListTemplatesResponse>, Status> {
        self.authorize_peer(&request, "ListTemplates", ALLOW_SANDBOX_OBSERVER)?;
        let mut client = pplx_space_template_mgr_client::lazy();
        let catalog = tokio::time::timeout(
            // Catalog reads do not materialize artifacts and must not inherit
            // GetTemplate's cold-download budget. Includes lazy connection.
            std::time::Duration::from_secs(5),
            client.list_templates(
                pplx_space_template_mgr_client::proto::ListTemplatesRequest::default(),
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("template catalog lookup timed out"))??
        .into_inner();
        let templates = tokio::task::spawn_blocking(move || {
            project_template_catalog(catalog.templates, std::path::Path::new(TEMPLATE_DIR))
        })
        .await
        .map_err(|error| {
            Status::internal(format!("template catalog projection failed: {error}"))
        })??;
        Ok(Response::new(ListTemplatesResponse { templates }))
    }
}

/// The template owner selects eligible builds. Cache directories only supplement
/// display metadata; leftovers and partially downloaded builds are not a catalog.
fn project_template_catalog(
    builds: Vec<pplx_space_template_mgr_client::proto::TemplateFullInfo>,
    template_dir: &std::path::Path,
) -> Result<Vec<TemplateInfo>, Status> {
    let mut latest = std::collections::BTreeMap::<
        String,
        pplx_space_template_mgr_client::proto::TemplateFullInfo,
    >::new();
    for build in builds {
        pplx_space_common_core::types::TemplateName::parse(&build.name).map_err(|error| {
            Status::data_loss(format!("invalid template catalog name: {error}"))
        })?;
        let hash = build.content_hash.as_deref().ok_or_else(|| {
            Status::data_loss(format!(
                "template catalog build {} has no digest",
                build.name
            ))
        })?;
        hash.parse::<pplx_space_common_core::types::ContentHash>()
            .map_err(|error| {
                Status::data_loss(format!("invalid template catalog digest: {error}"))
            })?;
        // Match the owner's latest-build ordering. A digest breaks equal-date
        // ties deterministically instead of depending on filesystem iteration.
        let replace = latest.get(&build.name).is_none_or(|current| {
            (&build.built_at, &build.content_hash) > (&current.built_at, &current.content_hash)
        });
        if replace {
            latest.insert(build.name.clone(), build);
        }
    }
    latest
        .into_values()
        .map(|build| {
            let hash = build.content_hash.unwrap_or_default();
            let path = template_dir
                .join(&build.name)
                .join(&hash)
                .join("metadata.json");
            let metadata: serde_json::Value = match std::fs::read(&path) {
                Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                    tracing::warn!(path = %path.display(), %error, "ignoring malformed supplemental template metadata");
                    serde_json::Value::Null
                }),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    serde_json::Value::Null
                }
                Err(error) => {
                    tracing::warn!(path = %path.display(), %error, "supplemental template metadata unavailable");
                    serde_json::Value::Null
                }
            };
            let text = |key| {
                metadata
                    .get(key)
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string()
            };
            Ok(TemplateInfo {
                id: build.name,
                description: text("description"),
                base: text("base"),
                built_at: build.built_at.unwrap_or_default(),
                size_bytes: build.artifact_size_bytes.unwrap_or_default(),
                sha256: hash,
                spaced_version: text("spaced_version"),
                spaced_compatibility_family: text("spaced_compatibility_family"),
            })
        })
        .collect()
}

#[cfg(test)]
mod create_after_lookup_tests;
#[cfg(test)]
mod create_environment_tests;
#[cfg(test)]
mod create_template_lookup_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod unpersisted_cleanup_tests;

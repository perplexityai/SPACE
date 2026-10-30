//! Reads of the node's local sandbox and template state that the facets reconcile and publish.

use super::*;

pub(super) async fn local_sandbox_allocation_ids(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
) -> Result<BTreeSet<SandboxId>, String> {
    let response = client
        .list_network_leases(Request::new(ListNetworkLeasesRequest {}))
        .await
        .map_err(|error| format!("list local network leases: {error}"))?
        .into_inner();
    let mut sandbox_ids = BTreeSet::new();
    for lease in response.leases {
        if !network_slot_state_has_live_allocation(lease.slot_state)
            || lease.sandbox_id.trim().is_empty()
        {
            continue;
        }
        let sandbox_id = lease
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| format!("invalid local network lease sandbox id: {error}"))?;
        sandbox_ids.insert(sandbox_id);
    }
    Ok(sandbox_ids)
}

pub(super) async fn fetch_local_sandbox_states(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_ids: Vec<String>,
) -> Result<BTreeMap<String, SandboxInfo>, String> {
    if sandbox_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let response = fetch_sandbox_states(client, &sandbox_ids).await?;
    Ok(response
        .sandboxes
        .into_iter()
        .filter(|sandbox| !sandbox.sandbox_id.trim().is_empty())
        .map(|sandbox| (sandbox.sandbox_id.clone(), sandbox))
        .collect())
}

pub(super) fn network_slot_state_has_live_allocation(slot_state: i32) -> bool {
    matches!(
        NetworkSlotState::try_from(slot_state).unwrap_or(NetworkSlotState::Unspecified),
        NetworkSlotState::Leasing | NetworkSlotState::Leased
    )
}

pub(super) async fn resolve_template_scope(scope: FacetScope) -> Result<Vec<String>, String> {
    match scope {
        FacetScope::Items(items) => Ok(util::dedupe_non_empty(items)),
        FacetScope::Full => list_local_template_names().await,
    }
}

pub(super) async fn resolve_sandbox_scope(
    scope: FacetScope,
    sandbox_listing: &InboundSandboxListing,
) -> Result<Vec<String>, String> {
    match scope {
        FacetScope::Items(items) => Ok(util::dedupe_non_empty(items)),
        FacetScope::Full => sandbox_listing.sandbox_ids().await,
    }
}

pub(super) async fn list_local_template_names() -> Result<Vec<String>, String> {
    let mut tmpl = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let response = tmpl
        .list_templates(Request::new(ListTemplatesRequest {
            omit_sandbox_counts: true,
        }))
        .await
        .map_err(|error| format!("list templates: {error}"))?
        .into_inner();
    Ok(util::dedupe_non_empty(
        response
            .templates
            .into_iter()
            .map(|template| template.name)
            .collect(),
    ))
}

pub(super) async fn list_local_sandbox_ids() -> Result<Vec<String>, String> {
    let mut sbx = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| format!("connect sbx-manager: {error}"))?;
    let sandboxes = pplx_space_sbx_mgr_client::list_all_sandboxes(&mut sbx)
        .await
        .map_err(|error| format!("list sandboxes: {error}"))?;
    Ok(util::dedupe_non_empty(
        sandboxes
            .into_iter()
            .map(|sandbox| sandbox.sandbox_id)
            .collect(),
    ))
}

pub(super) async fn collect_sandbox_states(
    scope: FacetScope,
    payload: SandboxPublishPayload,
) -> Result<(Vec<PublishedSandboxState>, u64), String> {
    let mut client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| format!("connect sbx-manager: {error}"))?;
    let mut out = Vec::new();
    match scope {
        FacetScope::Items(items) => {
            let ids = util::dedupe_non_empty(items);
            if ids.is_empty() {
                return Ok((out, 0));
            }
            out.reserve(ids.len());
            let response = fetch_sandbox_states(&mut client, &ids).await?;
            let by_id = index_sandbox_state_response(response.sandboxes, &mut out);
            let disk_usage_by_id = fetch_sandbox_disk_usage(&mut client, &ids).await;
            let policy_by_id = if payload.includes_network_policy() {
                let enrichment_ids = by_id
                    .iter()
                    .filter_map(|(sandbox_id, sandbox)| {
                        (!sandbox.state_degraded).then_some(*sandbox_id)
                    })
                    .collect::<Vec<_>>();
                fetch_sandbox_network_policies(&mut client, &enrichment_ids)
                    .await
                    .map_err(|error| error.to_string())?
            } else {
                BTreeMap::new()
            };
            let snapshot_ids = by_id
                .iter()
                .filter(|(_, sandbox)| sandbox.snapshots_enabled)
                .map(|(sandbox_id, _)| *sandbox_id)
                .collect::<Vec<_>>();
            let snapshots_by_id = fetch_sandbox_snapshots(&mut client, &snapshot_ids)
                .await
                .map_err(|error| error.to_string())?;
            for (sandbox_id, sandbox) in by_id {
                let policy = if payload.includes_network_policy() && !sandbox.state_degraded {
                    Some(
                        policy_by_id
                            .get(&sandbox_id)
                            .ok_or(SandboxInventoryEnrichmentError::MissingNetworkPolicy(
                                sandbox_id,
                            ))
                            .map_err(|error| error.to_string())?,
                    )
                } else {
                    None
                };
                let snapshots = snapshots_by_id
                    .get(&sandbox_id)
                    .cloned()
                    .unwrap_or_default();
                out.push(published_sandbox_state(
                    &sandbox,
                    disk_usage_by_id.get(&sandbox.sandbox_id).copied(),
                    policy,
                    snapshots,
                ));
            }
            for sandbox_id in response.missing_ids {
                out.push(absent_sandbox_state(sandbox_id));
            }
        }
        FacetScope::Full => {
            let ids = pplx_space_sbx_mgr_client::list_all_sandboxes(&mut client)
                .await
                .map_err(|error| format!("list sandboxes: {error}"))?
                .into_iter()
                .map(|sandbox| sandbox.sandbox_id)
                .collect::<Vec<_>>();
            if ids.is_empty() {
                return Ok((out, 0));
            }
            let response = fetch_sandbox_states(&mut client, &ids).await?;
            let by_id = index_sandbox_state_response(response.sandboxes, &mut out);
            let disk_ids = by_id.keys().map(ToString::to_string).collect::<Vec<_>>();
            let disk_usage_by_id = fetch_sandbox_disk_usage(&mut client, &disk_ids).await;
            let policy_by_id = if payload.includes_network_policy() {
                let enrichment_ids = by_id
                    .iter()
                    .filter_map(|(sandbox_id, sandbox)| {
                        (!sandbox.state_degraded).then_some(*sandbox_id)
                    })
                    .collect::<Vec<_>>();
                fetch_sandbox_network_policies(&mut client, &enrichment_ids)
                    .await
                    .map_err(|error| error.to_string())?
            } else {
                BTreeMap::new()
            };
            let snapshot_ids = by_id
                .iter()
                .filter(|(_, sandbox)| sandbox.snapshots_enabled)
                .map(|(sandbox_id, _)| *sandbox_id)
                .collect::<Vec<_>>();
            let snapshots_by_id = fetch_sandbox_snapshots(&mut client, &snapshot_ids)
                .await
                .map_err(|error| error.to_string())?;
            out.reserve(by_id.len());
            for (sandbox_id, sandbox) in by_id {
                let policy = if payload.includes_network_policy() && !sandbox.state_degraded {
                    Some(
                        policy_by_id
                            .get(&sandbox_id)
                            .ok_or(SandboxInventoryEnrichmentError::MissingNetworkPolicy(
                                sandbox_id,
                            ))
                            .map_err(|error| error.to_string())?,
                    )
                } else {
                    None
                };
                let snapshots = snapshots_by_id
                    .get(&sandbox_id)
                    .cloned()
                    .unwrap_or_default();
                out.push(published_sandbox_state(
                    &sandbox,
                    disk_usage_by_id.get(&sandbox.sandbox_id).copied(),
                    policy,
                    snapshots,
                ));
            }
            for sandbox_id in response.missing_ids {
                out.push(absent_sandbox_state(sandbox_id));
            }
        }
    }
    let seen = out.len() as u64;
    Ok((out, seen))
}

pub(super) async fn fetch_sandbox_states(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    ids: &[String],
) -> Result<SbxStateResponse, String> {
    fetch_sandbox_state_batches(ids, |sandbox_ids| {
        let mut client = client.clone();
        async move {
            client
                .state(Request::new(SbxStateRequest { sandbox_ids }))
                .await
                .map(tonic::Response::into_inner)
        }
    })
    .await
}

/// No partial result escapes on a failed batch: the caller may mark the final
/// publication as a complete census only after every batch has succeeded.
pub(super) async fn fetch_sandbox_state_batches<Fut>(
    ids: &[String],
    mut fetch: impl FnMut(Vec<String>) -> Fut,
) -> Result<SbxStateResponse, String>
where
    Fut: Future<Output = Result<SbxStateResponse, tonic::Status>>,
{
    let mut response = SbxStateResponse::default();
    for ids in ids.chunks(pplx_space_common_core::inventory::READ_BATCH_ITEMS) {
        let batch = fetch(ids.to_vec())
            .await
            .map_err(|error| format!("get sandbox state: {error}"))?;
        response.sandboxes.extend(batch.sandboxes);
        response.missing_ids.extend(batch.missing_ids);
    }
    Ok(response)
}

pub(super) fn index_sandbox_state_response(
    sandboxes: Vec<SandboxInfo>,
    out: &mut Vec<PublishedSandboxState>,
) -> BTreeMap<SandboxId, SandboxInfo> {
    let mut by_id = BTreeMap::new();
    for sandbox in sandboxes {
        match parse_sandbox_id(&sandbox.sandbox_id) {
            Ok(sandbox_id) => {
                by_id.insert(sandbox_id, sandbox);
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox.sandbox_id,
                    %error,
                    "forwarding malformed sandbox identity so the control plane disables absence pruning"
                );
                out.push(published_sandbox_state(&sandbox, None, None, Vec::new()));
            }
        }
    }
    by_id
}

pub(super) async fn fetch_sandbox_network_policies(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_ids: &[SandboxId],
) -> Result<BTreeMap<SandboxId, NetworkPolicyDetail>, SandboxInventoryEnrichmentError> {
    if sandbox_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let response = client
        .get_network_policy(Request::new(SbxGetNetworkPolicyRequest {
            sandbox_ids: sandbox_ids.iter().map(ToString::to_string).collect(),
        }))
        .await
        .map_err(SandboxInventoryEnrichmentError::GetNetworkPolicies)?
        .into_inner();
    let mut policies = BTreeMap::new();
    for policy in response.policies {
        policies.insert(parse_sandbox_id(&policy.sandbox_id)?, policy);
    }
    Ok(policies)
}

pub(super) async fn fetch_sandbox_snapshots(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_ids: &[SandboxId],
) -> Result<BTreeMap<SandboxId, Vec<SnapshotDetail>>, SandboxInventoryEnrichmentError> {
    if sandbox_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let response = client
        .list_snapshots(Request::new(ListSnapshotsRequest {
            sandbox_ids: sandbox_ids.iter().map(ToString::to_string).collect(),
        }))
        .await
        .map_err(SandboxInventoryEnrichmentError::ListSnapshots)?
        .into_inner();
    let mut snapshots_by_id = BTreeMap::<SandboxId, Vec<SnapshotDetail>>::new();
    for snapshot in response.snapshots {
        snapshots_by_id
            .entry(parse_sandbox_id(&snapshot.sandbox_id)?)
            .or_default()
            .push(snapshot);
    }
    Ok(snapshots_by_id)
}

pub(super) fn parse_sandbox_id(
    sandbox_id: &str,
) -> Result<SandboxId, SandboxInventoryEnrichmentError> {
    sandbox_id
        .parse()
        .map_err(|source| SandboxInventoryEnrichmentError::InvalidSandboxId {
            value: sandbox_id.to_string(),
            source,
        })
}

pub(super) async fn fetch_sandbox_disk_usage(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_ids: &[String],
) -> BTreeMap<String, u64> {
    if sandbox_ids.is_empty() {
        return BTreeMap::new();
    }
    let mut usage = BTreeMap::new();
    for sandbox_ids in sandbox_ids.chunks(pplx_space_common_core::inventory::READ_BATCH_ITEMS) {
        match client
            .get_disk_usage(Request::new(SbxGetDiskUsageRequest {
                sandbox_ids: sandbox_ids.to_vec(),
            }))
            .await
        {
            Ok(response) => usage.extend(response.into_inner().usage_bytes),
            Err(error) => {
                tracing::warn!(%error, "sandbox disk usage unavailable during inventory publish");
                return BTreeMap::new();
            }
        }
    }
    usage
}

pub(super) fn published_sandbox_state(
    sandbox: &SandboxInfo,
    disk_usage_bytes: Option<u64>,
    policy: Option<&NetworkPolicyDetail>,
    snapshots: Vec<SnapshotDetail>,
) -> PublishedSandboxState {
    let lifecycle = sandbox.observed_lifecycle();
    if let Err(error) = &lifecycle {
        tracing::warn!(sandbox_id = %sandbox.sandbox_id, %error, "preserving cluster lifecycle after unknown node observation");
    }
    PublishedSandboxState {
        sandbox_id: sandbox.sandbox_id.clone(),
        present: true,
        template: sandbox.template.clone(),
        status: lifecycle
            .as_ref()
            .ok()
            .map(|state| state.label().to_string()),
        content_hash: sandbox.image_hash.clone(),
        runtime: sandbox.runtime.clone(),
        created_at: sandbox.created_at.clone(),
        owner: sandbox.owner.clone(),
        observed_network_policy: policy.and_then(|policy| policy.effective_policy.clone()),
        observed_network_policy_generation: policy.map_or(0, |policy| policy.applied_generation),
        observed_network_policy_source: policy.map_or(0, |policy| policy.applied_source),
        observed_network_policy_authority: policy.map_or(0, |policy| policy.authority),
        auto_lifecycle_authority: sandbox.auto_lifecycle_authority,
        network_tx_bytes: sandbox.network_tx_bytes,
        network_rx_bytes: sandbox.network_rx_bytes,
        disk_usage_bytes,
        snapshots_enabled: sandbox.snapshots_enabled,
        pool_template: sandbox.pool_template.clone(),
        failure_reason: sandbox.failure_reason.clone(),
        quarantine_reason_code: sandbox.quarantine_reason_code.clone(),
        transition_receipt: sandbox.transition_receipt.clone(),
        state_degraded: sandbox.state_degraded || lifecycle.is_err(),
        current_local_artifact_id: sandbox.current_local_artifact_id.clone(),
        current_local_artifact_kind: sandbox.current_local_artifact_kind,
        current_local_artifact_origin: sandbox.current_local_artifact_origin,
        state_changed_at: lifecycle.as_ref().ok().and(sandbox.state_changed_at),
        quarantine_recovery: sandbox.quarantine_recovery.clone(),
        execution_generation: sandbox.execution_generation,
        pod_uid: sandbox.pod_uid.clone(),
        activation_attempt_id: sandbox.activation_attempt_id.clone(),
        snapshots,
        pid: sandbox.pid,
        namespace: sandbox.namespace.clone(),
        uptime: sandbox.uptime.clone(),
        execution: sandbox.execution.clone(),
        last_used_at: sandbox_last_used_at(sandbox),
        last_access_at: sandbox_last_access_at(sandbox),
        last_command_at: sandbox_last_command_at(sandbox),
        spaced_version: sandbox_spaced_version(sandbox),
    }
}

pub(super) fn sandbox_spaced_version(sandbox: &SandboxInfo) -> Option<String> {
    let raw = sandbox.spaced_version.as_deref()?;
    match SemVer::parse(raw) {
        Ok(version) => Some(version.to_string()),
        Err(error) => {
            tracing::warn!(
                sandbox_id = %sandbox.sandbox_id,
                error = %error,
                "sandbox inventory contained invalid spaced version"
            );
            None
        }
    }
}

pub(super) fn sandbox_last_used_at(sandbox: &SandboxInfo) -> Option<prost_types::Timestamp> {
    max_proto_timestamp(
        sandbox_last_access_at(sandbox),
        sandbox_last_command_at(sandbox),
    )
}

pub(super) fn sandbox_last_access_at(sandbox: &SandboxInfo) -> Option<prost_types::Timestamp> {
    sandbox
        .auto_lifecycle_status
        .as_ref()
        .and_then(|status| status.last_access_at)
}

pub(super) fn sandbox_last_command_at(sandbox: &SandboxInfo) -> Option<prost_types::Timestamp> {
    sandbox
        .auto_lifecycle_status
        .as_ref()
        .and_then(|status| status.last_command_at)
}

pub(super) fn max_proto_timestamp(
    left: Option<prost_types::Timestamp>,
    right: Option<prost_types::Timestamp>,
) -> Option<prost_types::Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if (right.seconds, right.nanos) >= (left.seconds, left.nanos) {
                Some(right)
            } else {
                Some(left)
            }
        }
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(super) fn absent_sandbox_state(sandbox_id: String) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id,
        present: false,
        ..PublishedSandboxState::default()
    }
}

pub(super) async fn collect_template_states(
    scope: FacetScope,
) -> Result<(Vec<PublishedTemplateState>, u64), String> {
    let mut client = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let templates = client
        .list_templates(Request::new(ListTemplatesRequest::default()))
        .await
        .map_err(|error| format!("list templates: {error}"))?
        .into_inner()
        .templates;
    let pools = client
        .pool_status(Request::new(PoolStatusRequest::default()))
        .await
        .map(|response| response.into_inner().pools)
        .unwrap_or_default();
    let pools_by_name = pools
        .into_iter()
        .map(|pool| (pool.template.clone(), pool))
        .collect::<BTreeMap<_, _>>();
    let mut by_name = BTreeMap::new();
    for template in templates {
        by_name.insert(template.name.clone(), template);
    }

    let names: BTreeSet<String> = match scope {
        FacetScope::Items(items) => util::dedupe_non_empty(items).into_iter().collect(),
        FacetScope::Full => by_name.keys().cloned().collect(),
    };
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let Some(template) = by_name.get(&name) else {
            out.push(PublishedTemplateState {
                template: name,
                present: false,
                ..PublishedTemplateState::default()
            });
            continue;
        };
        let pool = pools_by_name.get(&template.name);
        out.push(PublishedTemplateState {
            template: template.name.clone(),
            present: true,
            content_hash: template.content_hash.clone(),
            runtime: template.runtime.clone(),
            arch: template.arch.clone(),
            has_base_snapshot: template.has_base_snapshot,
            network_policy: template.network_policy.clone(),
            ensure_base_snapshot: template.ensure_base_snapshot,
            snapshot_vcpu: template.snapshot_vcpu,
            snapshot_memory_mib: template.snapshot_memory_mib,
            built_at: template.built_at.clone(),
            artifact_size_bytes: template.artifact_size_bytes,
            cache_size_bytes: template.cache_size_bytes,
            base_snapshot_size_bytes: template.base_snapshot_size_bytes,
            pool_target: pool.map(|pool| pool.target_count).or(template.pool_target),
            pool_current: pool.map_or(template.pool_current, |pool| pool.current_count),
            pool_pending: pool.map_or(0, |pool| pool.pending_count),
            pool_sandbox_ids: pool
                .map(|pool| pool.sandbox_ids.clone())
                .unwrap_or_default(),
            snapshots: template.snapshots.clone(),
            default_auto_lifecycle: template.default_auto_lifecycle.clone(),
            network_policy_generation: template.network_policy_generation,
            network_policy_authority: template.network_policy_authority,
            auto_lifecycle_authority: template.auto_lifecycle_authority,
            vcpu: template.vcpu,
            memory_mib: template.memory_mib,
            disk_size_mb: template.disk_size_mb,
            execution: template.execution.clone(),
        });
    }
    let seen = out.len() as u64;
    Ok((out, seen))
}

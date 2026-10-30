//! The network-policy facet: inbound refresh and apply, and outbound publish.

use super::*;

#[derive(Debug, Default)]
pub(super) struct LocalSandboxNetworkPolicies {
    pub(super) policies: BTreeMap<SandboxId, NetworkPolicyDetail>,
    pub(super) missing: BTreeSet<SandboxId>,
}

impl LocalSandboxNetworkPolicies {
    pub(super) fn from_policies(policies: BTreeMap<SandboxId, NetworkPolicyDetail>) -> Self {
        Self {
            policies,
            missing: BTreeSet::new(),
        }
    }

    pub(super) fn get(&self, sandbox_id: &SandboxId) -> Option<&NetworkPolicyDetail> {
        self.policies.get(sandbox_id)
    }

    pub(super) fn insert_missing(&mut self, sandbox_id: SandboxId) {
        self.missing.insert(sandbox_id);
    }

    pub(super) fn is_missing(&self, sandbox_id: &SandboxId) -> bool {
        self.missing.contains(sandbox_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SandboxPolicyReconcileAction {
    Apply,
    Skip,
}

pub(super) fn split_network_policy_items(
    items: Vec<String>,
) -> Result<(Vec<String>, Vec<String>), String> {
    let mut templates = Vec::new();
    let mut sandbox_ids = Vec::new();
    for item in util::dedupe_non_empty(items) {
        match item
            .parse::<NetworkPolicyTarget>()
            .map_err(|error| error.to_string())?
        {
            NetworkPolicyTarget::Template(template) => templates.push(template.to_string()),
            NetworkPolicyTarget::Sandbox(sandbox_id) => sandbox_ids.push(sandbox_id.to_string()),
        }
    }
    Ok((
        util::dedupe_non_empty(templates),
        util::dedupe_non_empty(sandbox_ids),
    ))
}

/// Splits one node's network-policy fetch into bounded serial RPCs: templates
/// (few) ride the first request, sandbox ids are batched at `max_batch_size`
/// per request. Serial batches keep at most one in-flight query per node on
/// cluster-api, and each batch is applied as soon as it arrives, so a timeout
/// or error loses only the remaining batches instead of the whole node.
pub(super) fn network_policy_state_requests(
    node_id: &str,
    node_role: Option<NodeRole>,
    templates: Vec<String>,
    sandbox_ids: Vec<String>,
    max_batch_size: usize,
) -> Vec<GetNodeNetworkPolicyStateRequest> {
    let max_batch_size = max_batch_size.max(1);
    let mut batches: Vec<Vec<String>> = sandbox_ids
        .chunks(max_batch_size)
        .map(<[String]>::to_vec)
        .collect();
    if batches.is_empty() {
        batches.push(Vec::new());
    }
    let mut templates = Some(templates);
    batches
        .into_iter()
        .map(|sandbox_ids| GetNodeNetworkPolicyStateRequest {
            node_id: node_id.to_string(),
            templates: templates.take().unwrap_or_default(),
            sandbox_ids,
            role: proto_node_role(node_role),
        })
        .collect()
}

pub(super) async fn refresh_network_policy(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    progress: crate::FacetProgress,
    sandbox_listing: &InboundSandboxListing,
) -> Result<ReconcileStats, String> {
    let (templates, sandbox_ids) = resolve_network_policy_scope(scope, sandbox_listing).await?;
    let mut stats = ReconcileStats {
        templates_seen: templates.len() as u64,
        sandboxes_seen: sandbox_ids.len() as u64,
        ..ReconcileStats::default()
    };
    if templates.is_empty() && sandbox_ids.is_empty() {
        return Ok(stats);
    }

    let requests = network_policy_state_requests(
        node_id,
        node_role,
        templates,
        sandbox_ids,
        crate::CLUSTER_RPC_MAX_BATCH_SIZE,
    );
    let mut client = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    for request in requests {
        let response = client
            .get_node_network_policy_state(request)
            .await
            .map_err(|error| format!("cluster network policy refresh failed: {error}"))?
            .into_inner();
        apply_network_policy_state(response, &mut stats).await?;
        // Each fetched-and-applied batch is forward progress; the facet
        // watchdog budgets time-per-batch, not time-per-run, so large nodes
        // don't hit a fixed whole-run deadline.
        progress.touch();
    }
    Ok(stats)
}

pub(super) async fn apply_network_policy_state(
    response: GetNodeNetworkPolicyStateResponse,
    stats: &mut ReconcileStats,
) -> Result<(), String> {
    for template in response.templates {
        let Some(policy) = template.policy else {
            continue;
        };
        match apply_template_policy(&template.template, policy, template.generation, false).await {
            Ok(decision) => {
                if apply_decision_counts_as_applied(decision) {
                    stats.templates_applied = stats.templates_applied.saturating_add(1);
                }
            }
            Err(error) => {
                return Err(format!(
                    "failed to apply template network policy {}: {error}",
                    template.template
                ));
            }
        }
    }
    let sandboxes = response.sandboxes;
    let local_sandbox_policies = fetch_local_sandbox_network_policies(&sandboxes).await?;
    for sandbox in sandboxes {
        let sandbox_id =
            parse_sandbox_id(&sandbox.sandbox_id).map_err(|error| error.to_string())?;
        if sandbox_policy_reconcile_action(&sandbox, &sandbox_id, &local_sandbox_policies)
            == SandboxPolicyReconcileAction::Skip
        {
            continue;
        }
        let source = NetworkPolicySource::try_from(sandbox.source)
            .unwrap_or(NetworkPolicySource::Unspecified);
        let inherit_template = source == NetworkPolicySource::Template;
        let policy = if inherit_template {
            None
        } else {
            sandbox.policy
        };
        match apply_sandbox_policy(
            &sandbox.sandbox_id,
            policy,
            inherit_template,
            sandbox.generation,
            false,
        )
        .await
        {
            Ok(decision) => {
                if apply_decision_counts_as_applied(decision) {
                    stats.sandboxes_applied = stats.sandboxes_applied.saturating_add(1);
                }
            }
            Err(error) => {
                return Err(format!(
                    "failed to apply sandbox network policy {}: {error}",
                    sandbox.sandbox_id
                ));
            }
        }
    }
    Ok(())
}

pub(super) async fn fetch_local_sandbox_network_policies(
    sandboxes: &[SandboxNetworkPolicyState],
) -> Result<LocalSandboxNetworkPolicies, String> {
    if sandboxes.is_empty() {
        return Ok(LocalSandboxNetworkPolicies::default());
    }

    let sandbox_ids = sandboxes
        .iter()
        .map(|sandbox| parse_sandbox_id(&sandbox.sandbox_id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| format!("connect sbx-manager: {error}"))?;
    match fetch_sandbox_network_policies(&mut client, &sandbox_ids).await {
        Ok(policies) => Ok(LocalSandboxNetworkPolicies::from_policies(policies)),
        Err(error) if should_fallback_to_individual_policy_fetch(&error) => {
            tracing::debug!(
                %error,
                "batched sandbox network policy fetch hit a missing sandbox; retrying individually"
            );
            fetch_local_sandbox_network_policies_individually(&mut client, &sandbox_ids).await
        }
        Err(error) => Err(error.to_string()),
    }
}

pub(super) async fn fetch_local_sandbox_network_policies_individually(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_ids: &[SandboxId],
) -> Result<LocalSandboxNetworkPolicies, String> {
    let mut local = LocalSandboxNetworkPolicies::default();
    for sandbox_id in sandbox_ids {
        match fetch_sandbox_network_policies(client, std::slice::from_ref(sandbox_id)).await {
            Ok(mut policies) => {
                if let Some(policy) = policies.remove(sandbox_id) {
                    local.policies.insert(*sandbox_id, policy);
                } else {
                    tracing::debug!(
                        %sandbox_id,
                        "sandbox-manager returned no network policy for local sandbox"
                    );
                    local.insert_missing(*sandbox_id);
                }
            }
            Err(error) if should_fallback_to_individual_policy_fetch(&error) => {
                tracing::debug!(
                    %sandbox_id,
                    "skipping network policy reconcile for sandbox missing locally"
                );
                local.insert_missing(*sandbox_id);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(local)
}

pub(super) fn should_fallback_to_individual_policy_fetch(
    error: &SandboxInventoryEnrichmentError,
) -> bool {
    matches!(
        error,
        SandboxInventoryEnrichmentError::GetNetworkPolicies(status)
            if status.code() == tonic::Code::NotFound
    )
}

pub(super) fn sandbox_policy_reconcile_action(
    desired: &SandboxNetworkPolicyState,
    sandbox_id: &SandboxId,
    local: &LocalSandboxNetworkPolicies,
) -> SandboxPolicyReconcileAction {
    if local.is_missing(sandbox_id) {
        return SandboxPolicyReconcileAction::Skip;
    }
    if sandbox_policy_needs_apply(desired, local.get(sandbox_id)) {
        SandboxPolicyReconcileAction::Apply
    } else {
        SandboxPolicyReconcileAction::Skip
    }
}

pub(super) fn sandbox_policy_needs_apply(
    desired: &SandboxNetworkPolicyState,
    local: Option<&NetworkPolicyDetail>,
) -> bool {
    let Some(local) = local else {
        return true;
    };

    let source =
        NetworkPolicySource::try_from(desired.source).unwrap_or(NetworkPolicySource::Unspecified);
    let inherit_template = source == NetworkPolicySource::Template;
    let expected_applied_source = if inherit_template {
        NetworkPolicySource::Template
    } else {
        NetworkPolicySource::Override
    };

    // The desired generation is the sandbox's own, but sandbox-manager reports
    // an inherited policy's generations from the template's counter. Comparing
    // the two would re-apply, and refresh the gateway, on every pass whenever
    // the counters differ, so an inherited policy is matched by content and by
    // the gateway having applied the template generation it inherits.
    let generation_drifted = if inherit_template {
        local.applied_generation != local.effective_generation
    } else {
        local.effective_generation != desired.generation
            || local.applied_generation != desired.generation
    };
    local.authority != PolicyAuthority::Cluster.as_proto_i32()
        || local.inherited != inherit_template
        || generation_drifted
        || desired.policy != local.effective_policy
        || local.applied_source != expected_applied_source as i32
}

pub(super) fn apply_decision_from_proto(value: i32) -> NetworkPolicyApplyDecision {
    NetworkPolicyApplyDecision::try_from(value).unwrap_or(NetworkPolicyApplyDecision::Unspecified)
}

pub(super) fn apply_decision_counts_as_applied(decision: NetworkPolicyApplyDecision) -> bool {
    matches!(
        decision,
        NetworkPolicyApplyDecision::Apply | NetworkPolicyApplyDecision::Repair
    )
}

pub(super) async fn publish_network_policy(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    node_status: Option<PublishedNodeStatus>,
) -> Result<ReconcileStats, String> {
    let (templates, sandbox_ids) = resolve_network_policy_publish_scope(scope)?;
    let (sandboxes, sandboxes_seen) =
        collect_sandbox_states(sandbox_ids, SandboxPublishPayload::NetworkPolicy).await?;
    let (templates, templates_seen) = collect_template_states(templates).await?;
    publish_node_state(
        node_id,
        node_role,
        NodeStatePublish {
            sandbox_inventory_complete: false,
            sandboxes,
            template_inventory_complete: false,
            templates,
            sandboxes_seen,
            templates_seen,
            node_status,
            execution_observation: None,
            auto_lifecycle_delete_completions: Vec::new(),
            pending_local_backups: None,
        },
    )
    .await
}

pub(super) async fn resolve_network_policy_scope(
    scope: FacetScope,
    sandbox_listing: &InboundSandboxListing,
) -> Result<(Vec<String>, Vec<String>), String> {
    match scope {
        FacetScope::Full => {
            let templates = list_local_template_names().await?;
            let sandbox_ids = sandbox_listing.sandbox_ids().await?;
            Ok((templates, sandbox_ids))
        }
        FacetScope::Items(items) => split_network_policy_items(items),
    }
}

pub(super) fn resolve_network_policy_publish_scope(
    scope: FacetScope,
) -> Result<(FacetScope, FacetScope), String> {
    match scope {
        FacetScope::Full => Ok((FacetScope::Full, FacetScope::Full)),
        FacetScope::Items(items) => {
            let (templates, sandbox_ids) = split_network_policy_items(items)?;
            Ok((FacetScope::Items(templates), FacetScope::Items(sandbox_ids)))
        }
    }
}

pub async fn apply_template_policy(
    template: &str,
    policy: ProtoNetworkPolicyV1,
    generation: u64,
    sync_cluster: bool,
) -> Result<NetworkPolicyApplyDecision, String> {
    let mut client = match pplx_space_template_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(template, %error, "cannot connect to template-manager");
            return Err(format!("connect template-manager: {error}"));
        }
    };
    match client
        .update_template_settings(local_manager_request(
            UpdateTemplateSettingsRequest {
                template: template.to_string(),
                default_auto_lifecycle: None,
                network_policy: Some(policy),
                network_policy_generation: generation,
                network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
                auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
            },
            sync_cluster,
        ))
        .await
    {
        Ok(response) => Ok(apply_decision_from_proto(
            response.into_inner().network_policy_apply_decision,
        )),
        Err(error) => {
            tracing::warn!(
                template,
                generation,
                %error,
                "failed to apply template network policy"
            );
            Err(error.to_string())
        }
    }
}

pub async fn apply_sandbox_policy(
    sandbox_id: &str,
    policy: Option<ProtoNetworkPolicyV1>,
    inherit_template: bool,
    generation: u64,
    sync_cluster: bool,
) -> Result<NetworkPolicyApplyDecision, String> {
    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(sandbox_id, %error, "cannot connect to sbx-manager");
            return Err(format!("connect sbx-manager: {error}"));
        }
    };
    match client
        .set_network_policy(local_manager_request(
            SetNetworkPolicyRequest {
                sandbox_id: sandbox_id.to_string(),
                policy,
                inherit_template,
                policy_generation: generation,
                authority: PolicyAuthority::Cluster.as_proto_i32(),
                sync_to_gateway: false,
            },
            sync_cluster,
        ))
        .await
    {
        Ok(response) => Ok(apply_decision_from_proto(
            response.into_inner().apply_decision,
        )),
        Err(error) => {
            tracing::warn!(
                sandbox_id,
                generation,
                %error,
                "failed to apply sandbox network policy"
            );
            Err(error.to_string())
        }
    }
}

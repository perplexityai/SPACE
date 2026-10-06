use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pplx_space_common_core::port_exposure_signals::{
    EXPOSE_TERMINAL_REASON_METADATA_KEY, EXPOSE_TERMINAL_REASON_STALE_POD_NETNS,
};
use pplx_space_common_core::types::{PolicyAuthority, SandboxId, SandboxOwner, SemVer};
use pplx_space_common_svc::types::NetworkPolicyTarget;
use pplx_space_net_common::proto::{
    NetworkPolicyApplyDecision, NetworkPolicySource, NetworkPolicyV1 as ProtoNetworkPolicyV1,
};
use pplx_space_node_api_client::proto::{
    AutoLifecycleDeleteCompletion, GetNodeNetworkPolicyStateRequest,
    GetNodeNetworkPolicyStateResponse, GetNodeSandboxDesiredStateRequest,
    GetNodeTemplateBaseSnapshotPolicyStateRequest, GetNodeTemplateDesiredStateRequest, NodeRole,
    PendingLocalBackupReport, PublishNodeStateRequest, PublishSandboxPortExposureStatesRequest,
    PublishedNodeStatus, PublishedSandboxState, PublishedTemplateState, SandboxNetworkPolicyState,
    SandboxPortApplyStateKind, SandboxPortDesiredStateKind, SandboxPortExposureApplyState,
    SandboxPortExposureDesiredState, SandboxPortProtocol,
};
use pplx_space_node_mgr_client::proto::{
    AllowPortRequest, DenyPortRequest, ExposePortRequest, GetExecutionInventoryRequest,
    ListNetworkLeasesRequest, NetworkProtocol, NetworkSlotState, UnexposePortRequest,
};
use pplx_space_sbx_mgr_client::proto::{
    AcknowledgeAutoLifecycleDeleteReceiptsRequest, DisableSnapshotsRequest, EnableSnapshotsRequest,
    EvictClusterDeletedSandboxRequest, GetDiskUsageRequest as SbxGetDiskUsageRequest,
    GetNetworkPolicyRequest as SbxGetNetworkPolicyRequest, ListAutoLifecycleDeleteReceiptsRequest,
    ListPendingLocalBackupsRequest, ListSnapshotsRequest, NetworkPolicyDetail, SandboxInfo,
    SetAutoLifecyclePolicyRequest, SetNetworkPolicyRequest, SnapshotDetail,
    StateRequest as SbxStateRequest, StateResponse as SbxStateResponse,
};
use pplx_space_template_mgr_client::proto::{
    CreatePoolRequest, DeletePoolRequest, DeleteTemplateRequest, ListTemplatesRequest,
    PoolStatusRequest, UpdateTemplateSettingsRequest, UpdateTemplateSnapshotPolicyRequest,
};
use tonic::{Code, Request};

use crate::dirty_state::{
    FACET_NETWORK_POLICY, FACET_NODE_STATUS, FACET_SANDBOX_DESIRED_STATE, FACET_SANDBOX_INVENTORY,
    FACET_TEMPLATE_BASE_SNAPSHOT_POLICY, FACET_TEMPLATE_DESIRED_STATE, FACET_TEMPLATE_INVENTORY,
    FacetRef,
};
use crate::{InboundFacetResult, ReconcileStats, connect_cluster_api, local_manager_request, util};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FacetKind {
    NetworkPolicy,
    SandboxDesiredState,
    SandboxInventory,
    TemplateInventory,
    TemplateBaseSnapshotPolicy,
    TemplateDesiredState,
    NodeStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FacetTickMode {
    Disabled,
    DirtyOnly,
    EveryTickFull,
}

#[derive(Debug, thiserror::Error)]
enum SandboxInventoryEnrichmentError {
    #[error("invalid sandbox id {value:?}: {source}")]
    InvalidSandboxId {
        value: String,
        #[source]
        source: pplx_space_common_core::types::SandboxIdParseError,
    },

    #[error("get sandbox network policies: {0}")]
    GetNetworkPolicies(#[source] tonic::Status),

    #[error("list sandbox snapshots: {0}")]
    ListSnapshots(#[source] tonic::Status),

    #[error("missing sandbox network policy {0}")]
    MissingNetworkPolicy(SandboxId),
}

#[derive(Clone, Copy, Debug)]
pub struct FacetPlugin {
    kind: FacetKind,
    inbound_tick: FacetTickMode,
    outbound_tick: FacetTickMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FacetScope {
    Full,
    Items(Vec<String>),
}

struct NodeStatePublish {
    sandbox_inventory_complete: bool,
    sandboxes: Vec<PublishedSandboxState>,
    template_inventory_complete: bool,
    templates: Vec<PublishedTemplateState>,
    sandboxes_seen: u64,
    templates_seen: u64,
    node_status: Option<PublishedNodeStatus>,
    execution_inventory: Option<pplx_space_execution_common::proto::NodeExecutionInventory>,
    auto_lifecycle_delete_completions: Vec<AutoLifecycleDeleteCompletion>,
    /// `None` is "this publish carries no report", which is what a delta
    /// publish and a failed enumeration both produce. It is not "nothing is
    /// pending" — `Some` with an empty list is.
    pending_local_backups: Option<PendingLocalBackupReport>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SandboxPublishPayload {
    Inventory,
    NetworkPolicy,
}

impl SandboxPublishPayload {
    fn includes_network_policy(self) -> bool {
        self == Self::NetworkPolicy
    }
}

#[derive(Debug, Default)]
struct LocalSandboxNetworkPolicies {
    policies: BTreeMap<SandboxId, NetworkPolicyDetail>,
    missing: BTreeSet<SandboxId>,
}

impl LocalSandboxNetworkPolicies {
    fn from_policies(policies: BTreeMap<SandboxId, NetworkPolicyDetail>) -> Self {
        Self {
            policies,
            missing: BTreeSet::new(),
        }
    }

    fn get(&self, sandbox_id: &SandboxId) -> Option<&NetworkPolicyDetail> {
        self.policies.get(sandbox_id)
    }

    fn insert_missing(&mut self, sandbox_id: SandboxId) {
        self.missing.insert(sandbox_id);
    }

    fn is_missing(&self, sandbox_id: &SandboxId) -> bool {
        self.missing.contains(sandbox_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SandboxPolicyReconcileAction {
    Apply,
    Skip,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SandboxDesiredReconcileAction {
    ApplyAll,
    ApplyPortExposuresAndRetry,
    SkipNonLocal,
}

fn sandbox_desired_reconcile_action(
    has_local_state: bool,
    has_port_exposures: bool,
    has_local_allocation: bool,
) -> SandboxDesiredReconcileAction {
    if has_port_exposures && !has_local_allocation {
        SandboxDesiredReconcileAction::SkipNonLocal
    } else if has_local_state {
        SandboxDesiredReconcileAction::ApplyAll
    } else if has_port_exposures {
        SandboxDesiredReconcileAction::ApplyPortExposuresAndRetry
    } else {
        SandboxDesiredReconcileAction::SkipNonLocal
    }
}

impl FacetScope {
    pub fn from_optional_items(items: Option<Vec<String>>) -> Self {
        match items {
            Some(items) => Self::Items(items),
            None => Self::Full,
        }
    }
}

pub const FACETS: &[FacetKind] = &[
    FacetKind::NetworkPolicy,
    FacetKind::SandboxDesiredState,
    FacetKind::SandboxInventory,
    FacetKind::TemplateInventory,
    FacetKind::TemplateBaseSnapshotPolicy,
    FacetKind::TemplateDesiredState,
    FacetKind::NodeStatus,
];

pub const FACET_PLUGINS: &[FacetPlugin] = &[
    FacetPlugin {
        kind: FacetKind::NetworkPolicy,
        inbound_tick: FacetTickMode::EveryTickFull,
        outbound_tick: FacetTickMode::DirtyOnly,
    },
    FacetPlugin {
        kind: FacetKind::SandboxDesiredState,
        inbound_tick: FacetTickMode::EveryTickFull,
        outbound_tick: FacetTickMode::Disabled,
    },
    FacetPlugin {
        kind: FacetKind::SandboxInventory,
        inbound_tick: FacetTickMode::Disabled,
        outbound_tick: FacetTickMode::EveryTickFull,
    },
    FacetPlugin {
        kind: FacetKind::TemplateInventory,
        inbound_tick: FacetTickMode::Disabled,
        outbound_tick: FacetTickMode::EveryTickFull,
    },
    FacetPlugin {
        kind: FacetKind::TemplateBaseSnapshotPolicy,
        inbound_tick: FacetTickMode::EveryTickFull,
        outbound_tick: FacetTickMode::Disabled,
    },
    FacetPlugin {
        kind: FacetKind::TemplateDesiredState,
        inbound_tick: FacetTickMode::EveryTickFull,
        outbound_tick: FacetTickMode::Disabled,
    },
    FacetPlugin {
        kind: FacetKind::NodeStatus,
        inbound_tick: FacetTickMode::Disabled,
        outbound_tick: FacetTickMode::DirtyOnly,
    },
];

impl FacetPlugin {
    pub fn kind(self) -> FacetKind {
        self.kind
    }

    pub fn name(self) -> &'static str {
        self.kind.name()
    }

    pub fn supports_inbound(self) -> bool {
        self.inbound_tick != FacetTickMode::Disabled
    }

    pub fn supports_outbound(self) -> bool {
        self.outbound_tick != FacetTickMode::Disabled
    }

    pub fn inbound_tick(self) -> FacetTickMode {
        self.inbound_tick
    }

    pub fn outbound_tick(self) -> FacetTickMode {
        self.outbound_tick
    }
}

impl FacetKind {
    pub fn parse(name: &str) -> Option<Self> {
        FACET_PLUGINS
            .iter()
            .copied()
            .find(|plugin| plugin.name() == name)
            .map(FacetPlugin::kind)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::NetworkPolicy => FACET_NETWORK_POLICY,
            Self::SandboxDesiredState => FACET_SANDBOX_DESIRED_STATE,
            Self::SandboxInventory => FACET_SANDBOX_INVENTORY,
            Self::TemplateInventory => FACET_TEMPLATE_INVENTORY,
            Self::TemplateBaseSnapshotPolicy => FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
            Self::TemplateDesiredState => FACET_TEMPLATE_DESIRED_STATE,
            Self::NodeStatus => FACET_NODE_STATUS,
        }
    }

    pub fn plugin(self) -> &'static FacetPlugin {
        FACET_PLUGINS
            .iter()
            .find(|plugin| plugin.kind == self)
            .expect("every facet has a plugin")
    }

    pub fn supports_inbound(self) -> bool {
        self.plugin().supports_inbound()
    }

    pub fn supports_outbound(self) -> bool {
        self.plugin().supports_outbound()
    }

    pub fn validate_ref(self, element: Option<&str>) -> Result<(), String> {
        match self {
            Self::NetworkPolicy => {
                if let Some(element) = element {
                    element
                        .parse::<NetworkPolicyTarget>()
                        .map_err(|error| error.to_string())?;
                }
            }
            Self::SandboxDesiredState => {
                if let Some(element) = element {
                    element.parse::<SandboxId>().map_err(|error| {
                        format!("invalid sandbox desired-state element: {error}")
                    })?;
                }
            }
            Self::SandboxInventory
            | Self::TemplateInventory
            | Self::TemplateBaseSnapshotPolicy
            | Self::TemplateDesiredState => {
                if let Some(element) = element
                    && element.trim().is_empty()
                {
                    return Err(format!("{} element must be non-empty", self.name()));
                }
            }
            Self::NodeStatus => {
                if element.is_some() {
                    return Err("node-status facet does not accept an element".to_string());
                }
            }
        }
        Ok(())
    }

    pub async fn run_inbound(
        self,
        node_id: &str,
        node_role: Option<NodeRole>,
        scope: FacetScope,
        progress: crate::FacetProgress,
    ) -> Result<InboundFacetResult, String> {
        match self {
            Self::NetworkPolicy => refresh_network_policy(node_id, node_role, scope, progress)
                .await
                .map(InboundFacetResult::from_stats),
            Self::SandboxDesiredState => {
                refresh_sandbox_desired_state(node_id, node_role, scope).await
            }
            Self::TemplateBaseSnapshotPolicy => {
                refresh_template_base_snapshot_policy(node_id, node_role, scope)
                    .await
                    .map(InboundFacetResult::from_stats)
            }
            Self::TemplateDesiredState => refresh_template_desired_state(node_id, node_role, scope)
                .await
                .map(InboundFacetResult::from_stats),
            Self::SandboxInventory | Self::TemplateInventory | Self::NodeStatus => {
                Ok(InboundFacetResult::default())
            }
        }
    }

    pub async fn run_outbound(
        self,
        node_id: &str,
        node_role: Option<NodeRole>,
        scope: FacetScope,
        node_status: Option<PublishedNodeStatus>,
    ) -> Result<ReconcileStats, String> {
        match self {
            Self::NetworkPolicy => {
                publish_network_policy(node_id, node_role, scope, node_status).await
            }
            Self::SandboxDesiredState => Ok(ReconcileStats::default()),
            Self::SandboxInventory => {
                publish_sandbox_inventory(node_id, node_role, scope, node_status).await
            }
            Self::TemplateInventory => {
                publish_template_inventory(node_id, node_role, scope, node_status).await
            }
            Self::TemplateBaseSnapshotPolicy | Self::TemplateDesiredState => {
                Ok(ReconcileStats::default())
            }
            Self::NodeStatus => publish_node_status(node_id, node_role, node_status).await,
        }
    }
}

pub fn validate_refs(refs: &[FacetRef]) -> Result<(), String> {
    for reference in refs {
        let Some(facet) = FacetKind::parse(&reference.facet) else {
            return Err(format!("unknown facet {:?}", reference.facet));
        };
        facet.validate_ref(reference.element.as_deref())?;
    }
    Ok(())
}

pub fn default_inbound_refs() -> Vec<FacetRef> {
    periodic_inbound_refs()
}

pub fn periodic_inbound_refs() -> Vec<FacetRef> {
    FACET_PLUGINS
        .iter()
        .copied()
        .filter(|plugin| plugin.inbound_tick() == FacetTickMode::EveryTickFull)
        .map(|plugin| FacetRef::new(plugin.name(), None::<String>))
        .collect()
}

pub fn periodic_outbound_refs() -> Vec<FacetRef> {
    FACET_PLUGINS
        .iter()
        .copied()
        .filter(|plugin| plugin.outbound_tick() == FacetTickMode::EveryTickFull)
        .map(|plugin| FacetRef::new(plugin.name(), None::<String>))
        .collect()
}

fn split_network_policy_items(items: Vec<String>) -> Result<(Vec<String>, Vec<String>), String> {
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

fn proto_node_role(role: Option<NodeRole>) -> Option<i32> {
    role.map(|role| role as i32)
}

/// Splits one node's network-policy fetch into bounded serial RPCs: templates
/// (few) ride the first request, sandbox ids are batched at `max_batch_size`
/// per request. Serial batches keep at most one in-flight query per node on
/// cluster-api, and each batch is applied as soon as it arrives, so a timeout
/// or error loses only the remaining batches instead of the whole node.
fn network_policy_state_requests(
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

async fn refresh_network_policy(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    progress: crate::FacetProgress,
) -> Result<ReconcileStats, String> {
    let (templates, sandbox_ids) = resolve_network_policy_scope(scope).await?;
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

async fn apply_network_policy_state(
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

async fn fetch_local_sandbox_network_policies(
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

async fn fetch_local_sandbox_network_policies_individually(
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

fn should_fallback_to_individual_policy_fetch(error: &SandboxInventoryEnrichmentError) -> bool {
    matches!(
        error,
        SandboxInventoryEnrichmentError::GetNetworkPolicies(status)
            if status.code() == tonic::Code::NotFound
    )
}

fn sandbox_policy_reconcile_action(
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

fn sandbox_policy_needs_apply(
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

    local.authority != PolicyAuthority::Cluster.as_proto_i32()
        || local.inherited != inherit_template
        || local.effective_generation != desired.generation
        || desired.policy != local.effective_policy
        || local.applied_generation != desired.generation
        || local.applied_source != expected_applied_source as i32
}

fn apply_decision_from_proto(value: i32) -> NetworkPolicyApplyDecision {
    NetworkPolicyApplyDecision::try_from(value).unwrap_or(NetworkPolicyApplyDecision::Unspecified)
}

fn apply_decision_counts_as_applied(decision: NetworkPolicyApplyDecision) -> bool {
    matches!(
        decision,
        NetworkPolicyApplyDecision::Apply | NetworkPolicyApplyDecision::Repair
    )
}

async fn refresh_template_base_snapshot_policy(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
) -> Result<ReconcileStats, String> {
    let templates = resolve_template_scope(scope).await?;
    let mut stats = ReconcileStats {
        templates_seen: templates.len() as u64,
        ..ReconcileStats::default()
    };
    if templates.is_empty() {
        return Ok(stats);
    }

    let mut client = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    let response = client
        .get_node_template_base_snapshot_policy_state(
            GetNodeTemplateBaseSnapshotPolicyStateRequest {
                node_id: node_id.to_string(),
                templates,
                role: proto_node_role(node_role),
            },
        )
        .await
        .map_err(|error| format!("cluster base-snapshot policy refresh failed: {error}"))?
        .into_inner();

    let mut tmpl = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let mut failures = 0_u64;
    for template in response.templates {
        if let Err(error) = tmpl
            .update_template_snapshot_policy(local_manager_request(
                UpdateTemplateSnapshotPolicyRequest {
                    template: template.template.clone(),
                    ensure_base_snapshot: template.ensure_base_snapshot,
                    snapshot_vcpu: Some(template.snapshot_vcpu),
                    snapshot_memory_mib: Some(template.snapshot_memory_mib),
                },
                false,
            ))
            .await
        {
            tracing::warn!(
                template = %template.template,
                %error,
                "failed to apply template base-snapshot policy"
            );
            failures = failures.saturating_add(1);
            continue;
        }
        stats.templates_applied = stats.templates_applied.saturating_add(1);
    }
    if failures > 0 {
        return Err(format!(
            "failed to apply {failures} template base-snapshot policy item(s)"
        ));
    }
    Ok(stats)
}

async fn refresh_template_desired_state(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
) -> Result<ReconcileStats, String> {
    let templates = resolve_template_scope(scope).await?;
    let mut stats = ReconcileStats {
        templates_seen: templates.len() as u64,
        ..ReconcileStats::default()
    };
    if templates.is_empty() {
        return Ok(stats);
    }

    let mut cluster = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    let response = cluster
        .get_node_template_desired_state(GetNodeTemplateDesiredStateRequest {
            node_id: node_id.to_string(),
            templates,
            role: proto_node_role(node_role),
        })
        .await
        .map_err(|error| format!("cluster template desired-state refresh failed: {error}"))?
        .into_inner();

    let mut tmpl = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let local_templates = tmpl
        .list_templates(Request::new(ListTemplatesRequest {}))
        .await
        .map_err(|error| format!("list local templates: {error}"))?
        .into_inner()
        .templates
        .into_iter()
        .map(|template| (template.name.clone(), template.default_auto_lifecycle))
        .collect::<BTreeMap<_, _>>();
    let local_pools = tmpl
        .pool_status(Request::new(PoolStatusRequest { template: None }))
        .await
        .map(|response| {
            response
                .into_inner()
                .pools
                .into_iter()
                .map(|pool| (pool.template.clone(), pool))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let mut failures = 0_u64;
    for template in response.templates {
        let mut applied = false;
        if template.present == Some(false) {
            if local_pools.contains_key(&template.template) {
                match tmpl
                    .delete_pool(local_manager_request(
                        DeletePoolRequest {
                            template: template.template.clone(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        applied = true;
                    }
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to delete undesired template pool"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
            if local_templates.contains_key(&template.template) {
                match tmpl
                    .delete_template(local_manager_request(
                        DeleteTemplateRequest {
                            template: template.template.clone(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        applied = true;
                    }
                    Err(error) if error.code() == Code::NotFound => {}
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to delete undesired template"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
            if applied {
                stats.templates_applied = stats.templates_applied.saturating_add(1);
            }
            continue;
        }

        if !local_templates.contains_key(&template.template) {
            if local_pools.contains_key(&template.template) {
                match tmpl
                    .delete_pool(local_manager_request(
                        DeletePoolRequest {
                            template: template.template.clone(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        stats.templates_applied = stats.templates_applied.saturating_add(1);
                    }
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to delete pool for absent local template"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
            continue;
        }

        if let Some(policy) = template.default_auto_lifecycle.clone() {
            let current = local_templates
                .get(&template.template)
                .and_then(|policy| policy.as_ref());
            if current != Some(&policy) {
                match tmpl
                    .update_template_settings(local_manager_request(
                        UpdateTemplateSettingsRequest {
                            template: template.template.clone(),
                            default_auto_lifecycle: Some(policy),
                            network_policy: None,
                            network_policy_generation: 0,
                            network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
                            auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        applied = true;
                    }
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to apply template default auto-lifecycle policy"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
        }

        match template.pool {
            Some(pool) => match tmpl
                .create_pool(local_manager_request(
                    CreatePoolRequest {
                        template: template.template.clone(),
                        target_count: Some(pool.target_count),
                        boot_mode: pool.boot_mode,
                        vcpu: pool.vcpu,
                        memory_mib: pool.memory_mib,
                    },
                    true,
                ))
                .await
            {
                Ok(_) => {
                    applied = true;
                }
                Err(error) => {
                    tracing::warn!(
                        template = %template.template,
                        %error,
                        "failed to apply template pool policy"
                    );
                    failures = failures.saturating_add(1);
                }
            },
            None => {
                if local_pools.contains_key(&template.template) {
                    match tmpl
                        .delete_pool(local_manager_request(
                            DeletePoolRequest {
                                template: template.template.clone(),
                            },
                            true,
                        ))
                        .await
                    {
                        Ok(_) => {
                            applied = true;
                        }
                        Err(error) => {
                            tracing::warn!(
                                template = %template.template,
                                %error,
                                "failed to delete undesired template pool"
                            );
                            failures = failures.saturating_add(1);
                        }
                    }
                }
            }
        }

        if applied {
            stats.templates_applied = stats.templates_applied.saturating_add(1);
        }
    }

    if failures > 0 {
        return Err(format!(
            "failed to apply {failures} template desired-state item(s)"
        ));
    }
    Ok(stats)
}

async fn refresh_sandbox_desired_state(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
) -> Result<InboundFacetResult, String> {
    let sandbox_ids = resolve_sandbox_scope(scope).await?;
    let mut stats = ReconcileStats {
        sandboxes_seen: sandbox_ids.len() as u64,
        ..ReconcileStats::default()
    };
    if sandbox_ids.is_empty() {
        return Ok(InboundFacetResult::from_stats(stats));
    }

    let mut cluster = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    let response = cluster
        .get_node_sandbox_desired_state(GetNodeSandboxDesiredStateRequest {
            node_id: node_id.to_string(),
            sandbox_ids,
            role: proto_node_role(node_role),
        })
        .await
        .map_err(|error| format!("cluster sandbox desired-state refresh failed: {error}"))?
        .into_inner();

    let has_port_exposures = response
        .sandboxes
        .iter()
        .any(|sandbox| !sandbox.port_exposures.is_empty());
    let mut sbx = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| format!("connect sbx-manager: {error}"))?;
    // Deleted ids are included so the eviction step below can gate on the
    // sandbox's current local state without a second State round-trip.
    let local_state_lookup_ids = response
        .sandboxes
        .iter()
        .map(|sandbox| sandbox.sandbox_id.clone())
        .chain(response.deleted_sandbox_ids.iter().cloned())
        .collect::<Vec<_>>();
    let local_sandbox_state_by_id = fetch_local_sandbox_states(&mut sbx, local_state_lookup_ids)
        .await
        .map_err(|error| format!("sandbox desired-state shared state lookup failed: {error}"))?;
    let mut node = has_port_exposures.then(pplx_space_node_mgr_client::lazy);
    let local_sandbox_allocations = if let Some(node) = node.as_mut() {
        local_sandbox_allocation_ids(node).await.map_err(|error| {
            format!("sandbox desired-state shared allocation lookup failed: {error}")
        })?
    } else {
        BTreeSet::new()
    };
    let mut element_errors = BTreeMap::new();
    let mut exposure_apply_states = Vec::new();
    for sandbox in response.sandboxes {
        let sandbox_id = sandbox
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| format!("invalid sandbox desired-state id: {error}"))?;
        let has_sandbox_port_exposures = !sandbox.port_exposures.is_empty();
        let local_sandbox_state = local_sandbox_state_by_id.get(&sandbox.sandbox_id);
        let action = sandbox_desired_reconcile_action(
            local_sandbox_state.is_some(),
            has_sandbox_port_exposures,
            local_sandbox_allocations.contains(&sandbox_id),
        );
        if action == SandboxDesiredReconcileAction::SkipNonLocal {
            tracing::debug!(
                sandbox_id = %sandbox.sandbox_id,
                exposures = sandbox.port_exposures.len(),
                "skipping desired state for sandbox that is no longer local"
            );
            continue;
        }
        if let Some(local_sandbox_state) = local_sandbox_state {
            handle_local_desired_state_apply(
                apply_sandbox_snapshot_desired(
                    &mut sbx,
                    &sandbox.sandbox_id,
                    local_sandbox_state,
                    sandbox.snapshots_enabled_desired,
                )
                .await,
                &sandbox.sandbox_id,
                &mut stats,
                &mut element_errors,
            );
            if let Some(policy) = sandbox.auto_lifecycle {
                handle_local_desired_state_apply(
                    apply_sandbox_auto_lifecycle_desired(
                        &mut sbx,
                        &sandbox.sandbox_id,
                        local_sandbox_state,
                        policy,
                    )
                    .await,
                    &sandbox.sandbox_id,
                    &mut stats,
                    &mut element_errors,
                );
            }
        } else {
            tracing::warn!(
                sandbox_id = %sandbox.sandbox_id,
                exposures = sandbox.port_exposures.len(),
                "local sandbox state is unavailable; applying port exposures and retrying the ref"
            );
        }
        if let Some(node) = node.as_mut() {
            reconcile_sandbox_port_exposures(
                PortExposureReconcileMode { action },
                node,
                &sandbox.sandbox_id,
                sandbox.port_exposures,
                &mut stats,
                &mut element_errors,
                &mut exposure_apply_states,
            )
            .await;
        }
    }
    if !exposure_apply_states.is_empty() {
        match cluster
            .publish_sandbox_port_exposure_states(PublishSandboxPortExposureStatesRequest {
                node_id: node_id.to_string(),
                states: exposure_apply_states,
                role: proto_node_role(node_role),
            })
            .await
        {
            Ok(response) => {
                for error in response.into_inner().errors {
                    tracing::warn!(
                        sandbox_id = %error.sandbox_id,
                        protocol = port_protocol_label(error.protocol),
                        guest_port = error.guest_port,
                        error = %error.error,
                        "failed to publish sandbox port exposure apply state"
                    );
                    record_element_error(
                        &mut element_errors,
                        &error.sandbox_id,
                        &format!("publish port exposure apply state: {}", error.error),
                    );
                }
            }
            Err(error) => {
                return Err(format!(
                    "publish sandbox port exposure apply states: {error}"
                ));
            }
        }
    }
    if !response.deleted_sandbox_ids.is_empty() {
        let evict_cap = CLUSTER_DELETED_EVICTS_PER_SYNC;
        let now = std::time::Instant::now();
        CLUSTER_DELETED_EVICT_BACKOFF_UNTIL.retain(|_, until| *until > now);
        let candidates = select_cluster_deleted_evict_candidates(
            &response.deleted_sandbox_ids,
            &local_sandbox_state_by_id,
            CLUSTER_DELETED_CANDIDATE_WINDOW,
            |sandbox_id| CLUSTER_DELETED_EVICT_BACKOFF_UNTIL.contains_key(sandbox_id),
        );
        let started = std::time::Instant::now();
        let mut evicted = 0_u64;
        let mut deferred = 0_u64;
        let mut budget_exhausted = false;
        for sandbox_id in candidates {
            if evicted >= evict_cap as u64 {
                break;
            }
            if started.elapsed() > CLUSTER_DELETED_EVICT_SYNC_BUDGET {
                budget_exhausted = true;
                break;
            }
            let call = sbx.evict_cluster_deleted_sandbox(local_manager_request(
                EvictClusterDeletedSandboxRequest {
                    sandbox_id: sandbox_id.to_string(),
                },
                true,
            ));
            let result = match tokio::time::timeout(CLUSTER_DELETED_EVICT_RPC_TIMEOUT, call).await {
                Ok(result) => result,
                // The teardown runs detached in sbx-manager, so an
                // elapsed deadline means "still in progress", not
                // failure; the next sync observes the outcome.
                Err(_elapsed) => Err(tonic::Status::deadline_exceeded(
                    "eviction still in progress at the per-RPC deadline",
                )),
            };
            let status = match &result {
                Ok(_) => {
                    evicted = evicted.saturating_add(1);
                    stats.sandboxes_applied = stats.sandboxes_applied.saturating_add(1);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Evicted,
                    );
                    continue;
                }
                Err(status) => status,
            };
            match cluster_deleted_evict_refusal_disposition(status) {
                ClusterDeletedEvictDisposition::RetryNextSync => {
                    deferred = deferred.saturating_add(1);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Deferred,
                    );
                    tracing::debug!(
                        sandbox_id,
                        status = %status,
                        "cluster-deleted eviction deferred to the next sync: restore staging in flight"
                    );
                }
                ClusterDeletedEvictDisposition::Deferred(backoff) => {
                    deferred = deferred.saturating_add(1);
                    CLUSTER_DELETED_EVICT_BACKOFF_UNTIL
                        .insert(sandbox_id.to_string(), std::time::Instant::now() + backoff);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Deferred,
                    );
                    tracing::debug!(
                        sandbox_id,
                        status = %status,
                        backoff_secs = backoff.as_secs(),
                        "cluster-deleted eviction deferred"
                    );
                }
                ClusterDeletedEvictDisposition::Failed(backoff) => {
                    CLUSTER_DELETED_EVICT_BACKOFF_UNTIL
                        .insert(sandbox_id.to_string(), std::time::Instant::now() + backoff);
                    pplx_space_metrics::reconcile::cluster_deleted_evict(
                        pplx_space_metrics::reconcile::ClusterDeletedEvictResult::Failed,
                    );
                    tracing::warn!(
                        sandbox_id,
                        status = %status,
                        "cluster-deleted eviction failed"
                    );
                    record_element_error(
                        &mut element_errors,
                        sandbox_id,
                        &format!("cluster-deleted eviction failed: {status}"),
                    );
                }
            }
        }
        if evicted > 0 || deferred > 0 || budget_exhausted {
            tracing::info!(
                deleted_reported = response.deleted_sandbox_ids.len(),
                evicted,
                deferred,
                budget_exhausted,
                per_sync_cap = evict_cap,
                "evicted local presence of cluster-deleted sandboxes"
            );
        }
    }

    Ok(InboundFacetResult {
        stats,
        element_errors,
    })
}

fn record_element_error(errors: &mut BTreeMap<String, String>, element: &str, error: &str) {
    errors
        .entry(element.to_string())
        .and_modify(|existing| {
            existing.push_str("; ");
            existing.push_str(error);
        })
        .or_insert_with(|| error.to_string());
}

/// Cap on successful cluster-deleted evictions per sync tick.
/// Bounds both the teardown work added to one facet pass and the blast
/// radius of a bad cluster answer; the backlog drains across syncs.
const CLUSTER_DELETED_EVICTS_PER_SYNC: usize = 25;

/// How many candidates one sync may examine while filling the eviction
/// cap. Wider than the cap so rows that defer within one sync cannot
/// monopolize the window; the per-candidate backoff below handles rows
/// that keep deferring across syncs.
const CLUSTER_DELETED_CANDIDATE_WINDOW: usize = 100;

/// Per-candidate retry backoff: a candidate that deferred or failed is
/// skipped by selection for this long, mirroring the disk-pressure
/// eviction loop's candidate backoff, so a permanently-deferring oldest
/// prefix (e.g. malformed `updated_at` rows) rotates out of the window
/// instead of starving the drainable backlog sorted behind it. Process
/// lifetime, in-memory: a node-ctl restart merely retries earlier.
const CLUSTER_DELETED_EVICT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Slack added to a server-supplied retry horizon so the retry lands on a
/// sync *after* the horizon passes rather than racing it. Sized to the
/// deployed default poll interval as a fixed safety margin: the interval is
/// runtime-configurable, so a shorter one just skips a few more syncs and a
/// longer one still adds less than one interval of margin; neither breaks
/// the schedule, it only shifts the retry by at most one extra sync.
const CLUSTER_DELETED_EVICT_RETRY_SLACK: std::time::Duration =
    std::time::Duration::from_secs(crate::DEFAULT_POLL_INTERVAL_SECS);

/// What one refused `EvictClusterDeletedSandbox` means for the per-id retry
/// schedule (a successful claim needs no schedule: the teardown runs
/// detached). Pure so the routing is testable without a live sbx-manager.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterDeletedEvictDisposition {
    /// Retry on the very next sync, no back-off: sbx-manager found a
    /// restore staging pull live on the row at claim time (`Aborted`),
    /// which ends on the pull's own timescale.
    RetryNextSync,
    /// Skip the id for this long, then retry. Not an element failure.
    Deferred(std::time::Duration),
    /// The eviction failed outright; back off and report an element error.
    Failed(std::time::Duration),
}

/// Route a refused eviction RPC. `NotFound` (the local row vanished),
/// `FailedPrecondition` (row transitioned / inside the quiescence window /
/// refused as local-provenance or pool), `Unimplemented` (rollout skew),
/// `Unavailable` (sbx-manager draining) and `DeadlineExceeded` (teardown
/// still running detached) all defer. A refusal that carries a retry
/// horizon — sbx-manager's quiescence refusal says how long the window has
/// left — defers exactly that long plus one sync of slack, so a fresh
/// cluster-deleted row is reclaimed at quiescence end (~11 min after the
/// delete) instead of after the flat 30 min back-off that stays in place
/// for refusals with no horizon. Everything else is a failure.
fn cluster_deleted_evict_refusal_disposition(
    status: &tonic::Status,
) -> ClusterDeletedEvictDisposition {
    match status.code() {
        Code::Aborted => ClusterDeletedEvictDisposition::RetryNextSync,
        Code::NotFound
        | Code::FailedPrecondition
        | Code::Unimplemented
        | Code::Unavailable
        | Code::DeadlineExceeded => ClusterDeletedEvictDisposition::Deferred(
            pplx_space_common_grpc::retry_after(status)
                .map(|horizon| horizon.saturating_add(CLUSTER_DELETED_EVICT_RETRY_SLACK))
                .map_or(CLUSTER_DELETED_EVICT_BACKOFF, |until| {
                    until.min(CLUSTER_DELETED_EVICT_BACKOFF)
                }),
        ),
        _ => ClusterDeletedEvictDisposition::Failed(CLUSTER_DELETED_EVICT_BACKOFF),
    }
}

static CLUSTER_DELETED_EVICT_BACKOFF_UNTIL: std::sync::LazyLock<
    dashmap::DashMap<String, std::time::Instant>,
> = std::sync::LazyLock::new(dashmap::DashMap::new);

/// Wall-clock budget for the eviction pass inside one facet sync. The
/// whole inbound facet races a 30s cancel-by-drop timeout in the work
/// loop; stopping well inside it leaves room for the desired-state applies
/// that share the tick and carries the remainder to the next sync.
const CLUSTER_DELETED_EVICT_SYNC_BUDGET: std::time::Duration = std::time::Duration::from_secs(15);

/// Per-eviction RPC deadline. The teardown is detached server-side, so
/// this bounds slot occupancy, not the teardown itself.
const CLUSTER_DELETED_EVICT_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Pick which cluster-deleted sandbox ids to evict locally this tick:
/// only ids whose local sbx-manager row is currently `suspended` (the
/// leaked wake-cache shape), `restoring` with a persisted staging
/// failure (the row a failed cross-node restore retains on the target
/// "for retry" — a retry can never come once the cluster row is gone, and
/// no other path reclaims it), or `suspending` with a permanently refused
/// teardown backup (the row a pod teardown leaves when the sandbox was
/// deleted mid-activation, whose suspend can never publish), owned by a
/// platform user (`user:<uuid>` —
/// positive evidence the cluster minted the sandbox; local-provenance
/// rows like `root@local` or `tmplmgr@local` never had a cluster row, so
/// its absence proves nothing about them), and not a pool slot. A
/// `restoring` row without the failure marker is a first-attempt restore
/// and is left alone, as is a `suspending` row without the terminal
/// teardown-backup marker, and anything else mid-transition, failed, or
/// already absent locally. Neither marker is a liveness proof — a retry of
/// the failed restore reuses the row without clearing it. sbx-manager re-checks the owner, pool and quiescence guards, takes
/// the staging lock non-blocking to prove no retry pull is live, and claims
/// the row at the exact version it read before tearing anything down — but
/// it makes no cluster call of its own: the `deleted_sandbox_ids` report
/// this selection is drawn from IS the authority that the cluster row is
/// gone, so getting that report right is this caller's responsibility, not
/// something sbx-manager can recover from.
/// Oldest first — UUIDv7 ids sort by creation time — so the largest
/// accumulated dead weight drains ahead of fresh arrivals.
fn select_cluster_deleted_evict_candidates<'a>(
    deleted_sandbox_ids: &'a [String],
    local_states: &BTreeMap<String, SandboxInfo>,
    window: usize,
    is_backed_off: impl Fn(&str) -> bool,
) -> Vec<&'a str> {
    let mut candidates = deleted_sandbox_ids
        .iter()
        .filter(|sandbox_id| !is_backed_off(sandbox_id))
        .filter(|sandbox_id| {
            local_states.get(*sandbox_id).is_some_and(|state| {
                local_row_is_cluster_deleted_reclaimable(state)
                    && state.pool_template.is_none()
                    && state
                        .owner
                        .as_deref()
                        .and_then(|owner| SandboxOwner::parse(owner).ok())
                        .is_some_and(|owner| owner.is_platform_user())
            })
        })
        .map(String::as_str)
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    candidates.truncate(window);
    candidates
}

/// The local row shapes a cluster-deleted report may reclaim: a
/// stopped rootfs, a suspended wake-cache copy, a failed cross-node restore's retained
/// `restoring` row (identified by any persisted `failure_reason`; a
/// whitespace-only reason is treated as no marker, matching sbx-manager),
/// or a `suspending` row whose teardown suspend had its durable backup
/// permanently refused, or a quarantined row retained for a deleted sandbox.
///
/// The `suspending` shape matches one exact reason rather than "any
/// failure", because that is the only `suspending` failure that ends the
/// row's own publication path for good. Other reasons sit on suspends
/// recovery can still finish, and offering them would spend eviction
/// budget and back-off slots on ids sbx-manager is bound to refuse.
fn local_row_is_cluster_deleted_reclaimable(state: &SandboxInfo) -> bool {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    match state.observed_lifecycle().ok() {
        Some(
            SandboxLifecycleState::Stopped
            | SandboxLifecycleState::Suspended
            | SandboxLifecycleState::Quarantined,
        ) => true,
        Some(SandboxLifecycleState::Restoring) => state
            .failure_reason
            .as_deref()
            .is_some_and(|reason| !reason.trim().is_empty()),
        Some(SandboxLifecycleState::Suspending) => {
            state.failure_reason.as_deref()
                == Some(
                    pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON,
                )
        }
        _ => false,
    }
}

async fn local_sandbox_allocation_ids(
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

async fn fetch_local_sandbox_states(
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

fn network_slot_state_has_live_allocation(slot_state: i32) -> bool {
    matches!(
        NetworkSlotState::try_from(slot_state).unwrap_or(NetworkSlotState::Unspecified),
        NetworkSlotState::Leasing | NetworkSlotState::Leased
    )
}

async fn publish_network_policy(
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
            execution_inventory: None,
            auto_lifecycle_delete_completions: Vec::new(),
            pending_local_backups: None,
        },
    )
    .await
}

async fn publish_sandbox_inventory(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    node_status: Option<PublishedNodeStatus>,
) -> Result<ReconcileStats, String> {
    let complete = matches!(scope, FacetScope::Full);
    let (sandboxes, sandboxes_seen) =
        collect_sandbox_states(scope, SandboxPublishPayload::Inventory).await?;
    let auto_lifecycle_delete_completions =
        collect_auto_lifecycle_delete_completions(node_role).await;
    // The report rides the Full census only. Not because a delta's list
    // would be short — `ListPendingLocalBackups` answers for the whole
    // node either way — but because the contract this field is published
    // under (see `PublishNodeStateRequest` in
    // `proto/node/api/v1/cluster.proto`) lets a consumer cross-check the
    // report against the sandbox rows in the same publication, and a delta
    // carries only a slice of those.
    //
    // The report is also strictly the newest thing in the publication:
    // `collect_sandbox_states` and the delete-receipt collector each made
    // their own round trip before this one. A sandbox that reaches
    // suspended-with-pin in between is still *present* in this
    // publication's `sandboxes` — the Full scope enumerates from sbx-mgr
    // `List`, whose query is `FROM sandboxes ORDER BY created_at` with no
    // state filter, so the row was picked up while it was still running.
    // Present, not missing — but its lifecycle is whatever the `State`
    // call saw, and `State` is only the second of several round trips
    // before the report: disk usage, snapshots and the delete receipts
    // all follow it. A sandbox that suspends after `State` therefore
    // ships a Running row beside a pending entry naming it. Genuine
    // absence is narrower still, needing a sandbox created *and* pinned
    // entirely inside the gap: a create plus a stop or suspend between
    // two consecutive RPCs.
    //
    // The order is still the better one. Reversed, a sandbox that suspends
    // after the report but before `collect_sandbox_states` ships a
    // freshly-suspended row whose pending entry is missing — a publication
    // that contradicts itself in the direction of under-reporting. As
    // written, the contradiction runs the other way: an entry whose row is
    // a moment stale, which is the better shape to hand a reader than an
    // absence. It does put an obligation on that reader, though, and this
    // is the only place the obligation is visible: **a pending entry can
    // name an artifact newer than anything its own sandbox row reflects**,
    // so the row's backup marker must never be read as evidence about the
    // reported artifact. The next census reconciles either way.
    let pending_local_backups = if complete {
        collect_pending_local_backups().await
    } else {
        None
    };
    publish_node_state(
        node_id,
        node_role,
        NodeStatePublish {
            sandbox_inventory_complete: complete,
            sandboxes,
            template_inventory_complete: false,
            templates: Vec::new(),
            sandboxes_seen,
            templates_seen: 0,
            node_status,
            execution_inventory: None,
            auto_lifecycle_delete_completions,
            pending_local_backups,
        },
    )
    .await
}

async fn publish_template_inventory(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
    node_status: Option<PublishedNodeStatus>,
) -> Result<ReconcileStats, String> {
    let complete = matches!(scope, FacetScope::Full);
    let (templates, templates_seen) = collect_template_states(scope).await?;
    publish_node_state(
        node_id,
        node_role,
        NodeStatePublish {
            sandbox_inventory_complete: false,
            sandboxes: Vec::new(),
            template_inventory_complete: complete,
            templates,
            sandboxes_seen: 0,
            templates_seen,
            node_status,
            execution_inventory: None,
            auto_lifecycle_delete_completions: Vec::new(),
            pending_local_backups: None,
        },
    )
    .await
}

async fn publish_node_status(
    node_id: &str,
    node_role: Option<NodeRole>,
    node_status: Option<PublishedNodeStatus>,
) -> Result<ReconcileStats, String> {
    let execution_inventory = match pplx_space_node_mgr_client::connect().await {
        Ok(mut node_manager) => match node_manager
            .get_execution_inventory(local_manager_request(
                GetExecutionInventoryRequest {},
                false,
            ))
            .await
        {
            Ok(response) => Some(response.into_inner()),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "skipping execution inventory publish because node-manager inventory is unavailable"
                );
                None
            }
        },
        Err(error) => {
            tracing::warn!(
                %error,
                "skipping execution inventory publish because node-manager is unavailable"
            );
            None
        }
    };
    publish_node_state(
        node_id,
        node_role,
        NodeStatePublish {
            sandbox_inventory_complete: false,
            sandboxes: Vec::new(),
            template_inventory_complete: false,
            templates: Vec::new(),
            sandboxes_seen: 0,
            templates_seen: 0,
            node_status,
            execution_inventory,
            auto_lifecycle_delete_completions: Vec::new(),
            pending_local_backups: None,
        },
    )
    .await
}

async fn publish_node_state(
    node_id: &str,
    node_role: Option<NodeRole>,
    state: NodeStatePublish,
) -> Result<ReconcileStats, String> {
    if state.sandboxes.is_empty()
        && state.templates.is_empty()
        && !state.sandbox_inventory_complete
        && !state.template_inventory_complete
        && state.node_status.is_none()
        && state.execution_inventory.is_none()
        && state.auto_lifecycle_delete_completions.is_empty()
    {
        return Ok(ReconcileStats {
            sandboxes_seen: state.sandboxes_seen,
            templates_seen: state.templates_seen,
            ..ReconcileStats::default()
        });
    }

    let templates_seen = state.templates_seen;
    let sandboxes_seen = state.sandboxes_seen;
    let mut client = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    let response = client
        .publish_node_state(PublishNodeStateRequest {
            node_id: node_id.to_string(),
            sandbox_inventory_complete: state.sandbox_inventory_complete,
            sandboxes: state.sandboxes,
            template_inventory_complete: state.template_inventory_complete,
            templates: state.templates,
            role: proto_node_role(node_role),
            node_status: state.node_status,
            execution_inventory: state.execution_inventory,
            auto_lifecycle_delete_completions: state.auto_lifecycle_delete_completions,
            pending_local_backups: state.pending_local_backups,
        })
        .await
        .map_err(|error| format!("cluster node state publish failed: {error}"))?
        .into_inner();

    acknowledge_auto_lifecycle_delete_completions(
        node_role,
        &response.acknowledged_auto_lifecycle_delete_receipt_ids,
    )
    .await;

    Ok(ReconcileStats {
        templates_seen,
        sandboxes_seen,
        templates_applied: response.templates_accepted,
        sandboxes_applied: response.sandboxes_accepted,
    })
}

/// How long one census will spend obtaining its pending-backup report.
///
/// Both legs below are otherwise generous: `connect()` carries the shared
/// 10-second connect-retry budget, and the sandbox-manager channel's own
/// request timeout is 120 seconds. Those are the right numbers for a
/// payload that has to arrive. This one does not — every failure yields
/// `None`, the census publishes anyway, and the next tick regenerates the
/// report (60 s by default, `poll_interval_secs`). Spending ten of the
/// facet watchdog's ninety — its default, `CLUSTER_RPC_TIMEOUT` × 3 —
/// let alone all of them, to produce the same `None` an immediate failure
/// produces would starve the publish the report rides on.
///
/// Two seconds still covers several of `connect()`'s retry attempts, so a
/// momentary socket blip costs nothing; only a genuinely unavailable
/// manager costs the node a cycle of gate freshness.
const PENDING_LOCAL_BACKUP_BUDGET: Duration = Duration::from_secs(2);

/// Ask sandbox-manager which of this node's current lifecycle artifacts
/// still have no durable remote copy.
///
/// Every failure — sandbox-manager unreachable, the RPC erroring, the whole
/// exchange outrunning [`PENDING_LOCAL_BACKUP_BUDGET`], an older
/// sandbox-manager answering `Unimplemented` — yields `None`, which is
/// byte-identical on the wire to what a node built before the field existed
/// sends. Deliberately not an empty report: an empty *report* asserts this
/// disk holds nothing that needs rescuing, and a failed enumeration has not
/// established that.
async fn collect_pending_local_backups() -> Option<PendingLocalBackupReport> {
    bounded_pending_local_backups(fetch_pending_local_backups()).await
}

/// The budget, split from the fetch so the expiry arm can be exercised
/// without a sandbox-manager: a test supplies its own slow future.
async fn bounded_pending_local_backups(
    fetch: impl Future<Output = Option<PendingLocalBackupReport>>,
) -> Option<PendingLocalBackupReport> {
    match tokio::time::timeout(PENDING_LOCAL_BACKUP_BUDGET, fetch).await {
        Ok(report) => report,
        Err(_elapsed) => {
            tracing::warn!(
                budget = ?PENDING_LOCAL_BACKUP_BUDGET,
                "gave up listing pending local backups within the census budget"
            );
            None
        }
    }
}

async fn fetch_pending_local_backups() -> Option<PendingLocalBackupReport> {
    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(%error, "cannot connect to sbx-manager to list pending local backups");
            return None;
        }
    };
    match client
        .list_pending_local_backups(Request::new(ListPendingLocalBackupsRequest {}))
        .await
    {
        Ok(response) => {
            // `cluster.proto` carries sandbox-manager's own
            // `PendingLocalBackup`, so the artifact identities move up
            // without being re-encoded — there is no field-by-field copy
            // here to fall out of step with the message.
            let pending = response.into_inner().pending;
            // The counterpart to the warn arms above: they say the node
            // could not answer, this says it did and with how many. Without
            // it a node-local log cannot tell an empty report apart from an
            // enumeration that never ran.
            tracing::debug!(
                pending_local_backups = pending.len(),
                "reporting pending local backups in the full census"
            );
            Some(PendingLocalBackupReport { pending })
        }
        Err(error) if error.code() == Code::Unimplemented => {
            // Not a warning — an older sandbox-manager during a rollout is
            // expected. Logged all the same so a census that quietly stops
            // carrying the report has a node-local explanation.
            tracing::debug!(%error, "sbx-manager does not serve the pending-backup inventory");
            None
        }
        Err(error) => {
            tracing::warn!(%error, "cannot list pending local backups");
            None
        }
    }
}

async fn collect_auto_lifecycle_delete_completions(
    node_role: Option<NodeRole>,
) -> Vec<AutoLifecycleDeleteCompletion> {
    if node_role != Some(NodeRole::Worker) {
        return Vec::new();
    }
    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot connect to sbx-manager to list delete receipts");
            return Vec::new();
        }
    };
    match client
        .list_auto_lifecycle_delete_receipts(Request::new(
            ListAutoLifecycleDeleteReceiptsRequest {},
        ))
        .await
    {
        Ok(response) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Succeeded,
            );
            response
                .into_inner()
                .receipts
                .into_iter()
                .map(|receipt| AutoLifecycleDeleteCompletion {
                    receipt_id: receipt.receipt_id,
                    sandbox_id: receipt.sandbox_id,
                    execution_generation: receipt.execution_generation,
                    route_activation_id: receipt.route_activation_id,
                    auto_lifecycle_policy_fingerprint: receipt.auto_lifecycle_policy_fingerprint,
                })
                .collect()
        }
        Err(error) if error.code() == Code::Unimplemented => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Unimplemented,
            );
            Vec::new()
        }
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::List,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot list auto-lifecycle delete receipts");
            Vec::new()
        }
    }
}

async fn acknowledge_auto_lifecycle_delete_completions(
    node_role: Option<NodeRole>,
    receipt_ids: &[String],
) {
    if node_role != Some(NodeRole::Worker) || receipt_ids.is_empty() {
        return;
    }
    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::Acknowledge,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot connect to sbx-manager to acknowledge delete receipts");
            return;
        }
    };
    match client
        .acknowledge_auto_lifecycle_delete_receipts(Request::new(
            AcknowledgeAutoLifecycleDeleteReceiptsRequest {
                receipt_ids: receipt_ids.to_vec(),
            },
        ))
        .await
    {
        Ok(_) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::Acknowledge,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Succeeded,
            );
        }
        Err(error) => {
            pplx_space_metrics::reconcile::auto_lifecycle_delete_delivery(
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOperation::Acknowledge,
                pplx_space_metrics::reconcile::AutoLifecycleDeleteDeliveryOutcome::Failed,
            );
            tracing::warn!(%error, "cannot acknowledge auto-lifecycle delete receipts");
        }
    }
}

async fn resolve_network_policy_scope(
    scope: FacetScope,
) -> Result<(Vec<String>, Vec<String>), String> {
    match scope {
        FacetScope::Full => {
            let templates = list_local_template_names().await?;
            let sandbox_ids = list_local_sandbox_ids().await?;
            Ok((templates, sandbox_ids))
        }
        FacetScope::Items(items) => split_network_policy_items(items),
    }
}

fn resolve_network_policy_publish_scope(
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

async fn resolve_template_scope(scope: FacetScope) -> Result<Vec<String>, String> {
    match scope {
        FacetScope::Items(items) => Ok(util::dedupe_non_empty(items)),
        FacetScope::Full => list_local_template_names().await,
    }
}

async fn resolve_sandbox_scope(scope: FacetScope) -> Result<Vec<String>, String> {
    match scope {
        FacetScope::Items(items) => Ok(util::dedupe_non_empty(items)),
        FacetScope::Full => list_local_sandbox_ids().await,
    }
}

async fn list_local_template_names() -> Result<Vec<String>, String> {
    let mut tmpl = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let response = tmpl
        .list_templates(Request::new(ListTemplatesRequest {}))
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

async fn list_local_sandbox_ids() -> Result<Vec<String>, String> {
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

async fn collect_sandbox_states(
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

async fn fetch_sandbox_states(
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
async fn fetch_sandbox_state_batches<Fut>(
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

fn index_sandbox_state_response(
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

async fn fetch_sandbox_network_policies(
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

async fn fetch_sandbox_snapshots(
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

fn parse_sandbox_id(sandbox_id: &str) -> Result<SandboxId, SandboxInventoryEnrichmentError> {
    sandbox_id
        .parse()
        .map_err(|source| SandboxInventoryEnrichmentError::InvalidSandboxId {
            value: sandbox_id.to_string(),
            source,
        })
}

async fn fetch_sandbox_disk_usage(
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

fn published_sandbox_state(
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

fn sandbox_spaced_version(sandbox: &SandboxInfo) -> Option<String> {
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

fn sandbox_last_used_at(sandbox: &SandboxInfo) -> Option<prost_types::Timestamp> {
    max_proto_timestamp(
        sandbox_last_access_at(sandbox),
        sandbox_last_command_at(sandbox),
    )
}

fn sandbox_last_access_at(sandbox: &SandboxInfo) -> Option<prost_types::Timestamp> {
    sandbox
        .auto_lifecycle_status
        .as_ref()
        .and_then(|status| status.last_access_at)
}

fn sandbox_last_command_at(sandbox: &SandboxInfo) -> Option<prost_types::Timestamp> {
    sandbox
        .auto_lifecycle_status
        .as_ref()
        .and_then(|status| status.last_command_at)
}

fn max_proto_timestamp(
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

fn absent_sandbox_state(sandbox_id: String) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id,
        present: false,
        ..PublishedSandboxState::default()
    }
}

async fn collect_template_states(
    scope: FacetScope,
) -> Result<(Vec<PublishedTemplateState>, u64), String> {
    let mut client = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let templates = client
        .list_templates(Request::new(ListTemplatesRequest {}))
        .await
        .map_err(|error| format!("list templates: {error}"))?
        .into_inner()
        .templates;
    let pools = client
        .pool_status(Request::new(PoolStatusRequest { template: None }))
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

/// Node-manager port RPCs issued while reconciling one exposure. Kept as an
/// enum so terminal-code classification keys off the exact operation instead
/// of substring-matching a formatted message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PortExposureRpc {
    Allow,
    Expose,
    Unexpose,
    Deny,
}

impl PortExposureRpc {
    fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow port",
            Self::Expose => "expose port",
            Self::Unexpose => "unexpose port",
            Self::Deny => "deny port",
        }
    }
}

impl std::fmt::Display for PortExposureRpc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Terminal-vs-retryable disposition for a tonic `Status` returned by an
/// sbx-manager desired-state apply sub-op (loopback-bridge open, snapshot,
/// auto-lifecycle). Terminal means retrying on this node cannot converge while
/// state is unchanged, so the queue ref must complete; a lifecycle or cluster
/// repair changes state and periodic desired-state sync re-applies afterward.
///
/// Terminal **only** when sbx-manager marks the status. sbx-manager answers
/// `FailedPrecondition` for both terminal cases (guest unreachable in a
/// settled non-reachable state; auto-lifecycle target stuck in
/// `Failed`/`CleanupRequired`/`CleanupFailed`/`Quarantined`) and transient ones (a guest still
/// initializing — `guest_not_ready`; a mid-transition lifecycle state that
/// settles), and `NotFound` for both a gone row and the startup race where the
/// local row has not appeared yet (`ApplyPortExposuresAndRetry`). Neither bare
/// code can distinguish these, so classification defers entirely to the marker
/// the server sets at its own precondition gate; everything unmarked —
/// transport, `Unavailable`, `Internal`, timeouts, rollout-skew
/// `Unimplemented`, unmarked `FailedPrecondition`/`NotFound` — stays retryable.
///
/// The node-manager `ExposePort` path does **not** use this: its
/// `FailedPrecondition`/`NotFound` are unambiguously the terminal stale-netns /
/// gone-allocation cases, so it classifies by code directly (see
/// `SandboxPortExposureApplyError`).
fn sbx_mgr_desired_state_apply_is_terminal(status: &tonic::Status) -> bool {
    pplx_space_common_grpc::is_desired_state_apply_terminal(status)
}

/// Failure applying one sandbox port exposure. RPC failures preserve the
/// tonic `Status` (code + message) instead of flattening it to a `String` so
/// the reconcile loop can distinguish terminal dispositions from retryable
/// ones.
#[derive(Debug, thiserror::Error)]
enum SandboxPortExposureApplyError {
    /// Local pre-validation failed before any node-manager RPC.
    #[error("{0}")]
    Invalid(String),
    /// A node-manager port RPC failed.
    #[error("{operation}: {status}")]
    Rpc {
        operation: PortExposureRpc,
        #[source]
        status: tonic::Status,
    },
    /// The in-VM loopback bridge re-arm failed after the exposure RPCs
    /// succeeded.
    #[error(transparent)]
    Bridge(OpenLoopbackBridgeError),
}

impl SandboxPortExposureApplyError {
    /// Terminal: this node cannot make progress by retrying while cluster
    /// state is unchanged. Completion policy is handled separately because
    /// stale-netns and unreachable-guest failures have different repair paths.
    ///
    /// The two terminal sub-ops classify differently because their servers do.
    ///
    /// `ExposePort` (node-manager): terminal on `NotFound` (no allocation for
    /// this sandbox — authoritatively absent from this node) or a
    /// `FailedPrecondition` **tagged with the stale-pod-netns marker** (the CNI
    /// namespace behind the allocation is gone). `FailedPrecondition` is *not*
    /// terminal on its own — the RPC also returns it for the root-namespace
    /// ingress-safety rejection, which has a live allocation the reconciler
    /// won't repair — so keying on the exact marker keeps the two apart.
    ///
    /// The loopback-bridge open (sbx-manager) answers `FailedPrecondition` for
    /// both the terminal guest-unreachable gate and the transient
    /// `guest_not_ready` case, so it defers to
    /// [`sbx_mgr_desired_state_apply_is_terminal`], which trusts the server-set
    /// terminal marker rather than the bare code.
    ///
    /// Transport errors, `Internal` (e.g. netlink timeouts), the bridge
    /// not-yet-ready status, and every other RPC stay retryable.
    fn is_terminal(&self) -> bool {
        match self {
            Self::Rpc {
                operation: PortExposureRpc::Expose,
                status,
            } => status.code() == Code::NotFound || Self::has_stale_pod_netns_marker(status),
            Self::Bridge(OpenLoopbackBridgeError::Rpc(status)) => {
                sbx_mgr_desired_state_apply_is_terminal(status)
            }
            _ => false,
        }
    }

    /// Whether this classified failure may complete the enclosing sandbox ref.
    ///
    /// A stale-netns `ExposePort` failure never completes the ref: the ref stays
    /// retryable so a stale-netns sandbox cannot be stranded with no repair
    /// owner. A marked
    /// sbx-manager bridge failure is different: local state exists but the
    /// guest cannot become reachable without a separate lifecycle transition,
    /// so retrying this ref cannot help. Complete it immediately and let the
    /// periodic full reconcile reapply after the lifecycle changes.
    fn completes_ref(&self) -> bool {
        match self {
            Self::Bridge(OpenLoopbackBridgeError::Rpc(status)) => {
                sbx_mgr_desired_state_apply_is_terminal(status)
            }
            _ => false,
        }
    }

    fn has_stale_pod_netns_marker(status: &tonic::Status) -> bool {
        status
            .metadata()
            .get(EXPOSE_TERMINAL_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(EXPOSE_TERMINAL_REASON_STALE_POD_NETNS)
    }
}

/// Outcome of applying one sandbox port exposure. The `result` decides
/// `apply_state` (Ok → applied, Err → failed); `bridge_status` and
/// `service_generation` are observation metadata persisted alongside so the
/// reconciler can skip steady-state rows and a future UI can render
/// reachability without a live node probe.
struct SandboxPortExposureApplyOutcome {
    result: Result<(), SandboxPortExposureApplyError>,
    bridge_status: Option<i32>,
    service_generation: Option<String>,
}

trait SandboxPortExposureApplier {
    fn apply<'a>(
        &'a mut self,
        sandbox_id: &'a str,
        exposure: &'a SandboxPortExposureDesiredState,
    ) -> Pin<Box<dyn Future<Output = SandboxPortExposureApplyOutcome> + Send + 'a>>;
}

impl SandboxPortExposureApplier for pplx_space_node_mgr_client::NodeManagerGrpcClient {
    fn apply<'a>(
        &'a mut self,
        sandbox_id: &'a str,
        exposure: &'a SandboxPortExposureDesiredState,
    ) -> Pin<Box<dyn Future<Output = SandboxPortExposureApplyOutcome> + Send + 'a>> {
        Box::pin(apply_sandbox_port_exposure_desired(
            self, sandbox_id, exposure,
        ))
    }
}

/// How one sandbox's port-exposure batch is reconciled.
#[derive(Clone, Copy)]
struct PortExposureReconcileMode {
    action: SandboxDesiredReconcileAction,
}

async fn reconcile_sandbox_port_exposures<A: SandboxPortExposureApplier>(
    mode: PortExposureReconcileMode,
    applier: &mut A,
    sandbox_id: &str,
    exposures: Vec<SandboxPortExposureDesiredState>,
    stats: &mut ReconcileStats,
    element_errors: &mut BTreeMap<String, String>,
    exposure_apply_states: &mut Vec<SandboxPortExposureApplyState>,
) {
    let action = mode.action;
    if action == SandboxDesiredReconcileAction::SkipNonLocal {
        return;
    }

    let mut terminal_failure = false;
    for exposure in exposures {
        let outcome = applier.apply(sandbox_id, &exposure).await;
        let (apply_state, error) = match outcome.result {
            Ok(()) => {
                stats.sandboxes_applied = stats.sandboxes_applied.saturating_add(1);
                (
                    SandboxPortApplyStateKind::SandboxPortApplyStateApplied,
                    None,
                )
            }
            Err(error) => {
                // Completion policy depends on the failing boundary. A stale
                // node-manager netns needs the rollout-gated orphan repair
                // owner, while a marked sbx-manager bridge failure has local
                // state and completes until a lifecycle change makes the guest
                // reachable again. Keep classification and completion separate
                // so the stale-netns safety gate cannot suppress the actual
                // unreachable-guest poison-ref fix.
                let classified_terminal = error.is_terminal();
                let complete_ref = error.completes_ref();
                tracing::warn!(
                    sandbox_id,
                    protocol = port_protocol_label(exposure.protocol),
                    guest_port = exposure.guest_port,
                    desired_state = port_desired_state_label(exposure.desired_state),
                    classified_terminal,
                    complete_ref,
                    %error,
                    "failed to apply sandbox port exposure desired state"
                );
                let error = error.to_string();
                let result_kind = if complete_ref {
                    // Publish the failed observation but do not fail the
                    // sandbox ref: retrying on this node can never succeed
                    // (the pod netns / allocation is gone, or the guest is
                    // unreachable). The owning reconciler/lifecycle transition
                    // repairs durable state; periodic desired-state sync then
                    // re-applies anything that becomes actionable.
                    terminal_failure = true;
                    pplx_space_metrics::reconcile::DesiredStateApplyResult::Terminal
                } else {
                    record_element_error(element_errors, sandbox_id, &error);
                    pplx_space_metrics::reconcile::DesiredStateApplyResult::Retryable
                };
                pplx_space_metrics::reconcile::desired_state_apply(result_kind);
                (
                    SandboxPortApplyStateKind::SandboxPortApplyStateFailed,
                    Some(error),
                )
            }
        };
        exposure_apply_states.push(SandboxPortExposureApplyState {
            sandbox_id: sandbox_id.to_string(),
            protocol: exposure.protocol,
            guest_port: exposure.guest_port,
            apply_state: apply_state as i32,
            error,
            bridge_status: outcome.bridge_status,
            service_generation: outcome.service_generation,
        });
    }

    // Missing local sandbox state normally keeps the ref failing so the node
    // retries until sbx-manager state shows up. A terminal exposure failure
    // proves that state never will (the netns behind the allocation is gone),
    // so recording the retry error would poison the queue with a ref that can
    // never converge.
    if action == SandboxDesiredReconcileAction::ApplyPortExposuresAndRetry && !terminal_failure {
        record_element_error(
            element_errors,
            sandbox_id,
            "local sandbox state unavailable",
        );
    }
}

async fn apply_sandbox_port_exposure_desired(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox_id: &str,
    exposure: &SandboxPortExposureDesiredState,
) -> SandboxPortExposureApplyOutcome {
    let mut outcome = SandboxPortExposureApplyOutcome {
        result: Ok(()),
        bridge_status: None,
        service_generation: None,
    };
    if SandboxPortProtocol::try_from(exposure.protocol).unwrap_or(SandboxPortProtocol::Unspecified)
        != SandboxPortProtocol::Tcp
    {
        outcome.result = Err(SandboxPortExposureApplyError::Invalid(format!(
            "unsupported sandbox port exposure protocol {:?}",
            port_protocol_label(exposure.protocol)
        )));
        return outcome;
    }
    let port = match u16::try_from(exposure.guest_port)
        .ok()
        .filter(|port| *port > 0)
    {
        Some(port) => u32::from(port),
        None => {
            outcome.result = Err(SandboxPortExposureApplyError::Invalid(format!(
                "invalid sandbox port {}",
                exposure.guest_port
            )));
            return outcome;
        }
    };
    match SandboxPortDesiredStateKind::try_from(exposure.desired_state)
        .unwrap_or(SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified)
    {
        SandboxPortDesiredStateKind::SandboxPortDesiredStateOpen => {
            if let Err(status) = client
                .allow_port(Request::new(AllowPortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port,
                    protocol: NetworkProtocol::Tcp as i32,
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Allow,
                    status,
                });
                return outcome;
            }
            if let Err(status) = client
                .expose_port(Request::new(ExposePortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port,
                    protocol: NetworkProtocol::Tcp as i32,
                    guest_port: Some(port),
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Expose,
                    status,
                });
                return outcome;
            }
            // Re-arm the in-VM loopback bridge. This matters on (a) node
            // restart / state recovery — `LoopbackBridgeManager` starts
            // empty so authorized ports must be re-installed — and (b)
            // sandbox guest restart, where spaced-svc forgets the
            // authorized set. The observed bridge status and service
            // generation are persisted regardless of result so the
            // reconciler's steady-state skip and the future UI both have
            // current data.
            let bridge = open_loopback_bridge_for_exposure(sandbox_id, port).await;
            // `bridge.bridge_status` and `bridge.service_generation` are
            // already `Option`s — `None` for transport failures (no
            // observation possible) so the publish path's `COALESCE`
            // preserves any prior observation rather than overwriting
            // with `UNSPECIFIED` (which the cluster parser rejects).
            outcome.bridge_status = bridge.bridge_status;
            outcome.service_generation = bridge.service_generation;
            outcome.result = bridge.result.map_err(SandboxPortExposureApplyError::Bridge);
        }
        SandboxPortDesiredStateKind::SandboxPortDesiredStateClosed => {
            if let Err(status) = client
                .unexpose_port(Request::new(UnexposePortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port: Some(port),
                    protocol: NetworkProtocol::Tcp as i32,
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Unexpose,
                    status,
                });
                return outcome;
            }
            if let Err(status) = client
                .deny_port(Request::new(DenyPortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port: Some(port),
                    protocol: NetworkProtocol::Tcp as i32,
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Deny,
                    status,
                });
                return outcome;
            }
            // Tear down the in-VM bridge symmetric to the open arm. This
            // is purely housekeeping: the bridge dies when the sandbox
            // does, but if the sandbox keeps running across many open/
            // close cycles, leaving stale forwarders authorized burns one
            // tokio task per stale port. Best-effort.
            if let Err(error) = close_loopback_bridge_for_exposure(sandbox_id, port).await {
                tracing::warn!(
                    sandbox_id,
                    port,
                    %error,
                    "loopback bridge close failed during reconcile (forwarder dies with sandbox)"
                );
            }
        }
        SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified => {
            outcome.result = Err(SandboxPortExposureApplyError::Invalid(
                "sandbox port desired state is required".to_string(),
            ));
        }
    }
    outcome
}

/// Failure opening the in-VM loopback bridge for one exposure. The RPC arm
/// preserves the tonic `Status` so a guest-unreachable `FailedPrecondition`
/// (sandbox failed/suspending/suspended/stopped) is distinguishable from
/// transient failures; `Connect` and `NotReady` are always retryable.
#[derive(Debug, thiserror::Error)]
enum OpenLoopbackBridgeError {
    /// Could not connect to the local sbx-manager socket — transport, so
    /// retryable.
    #[error("connect sbx-mgr: {0}")]
    Connect(String),
    /// The `SpacedOpenLoopbackBridge` RPC returned an error status. The tonic
    /// `Status` is preserved so the guest-unreachable `FailedPrecondition`
    /// case can be classified terminal by the shared classifier.
    #[error("spaced_open_loopback_bridge: {0}")]
    Rpc(#[source] tonic::Status),
    /// The RPC succeeded but the bridge is not in a serving status yet
    /// (`WaitingForListener`/`BindConflict`/`Failed`/`Unspecified`). The
    /// reconciler re-ticks to promote it, so this stays retryable.
    #[error("loopback bridge not ready: {0}")]
    NotReady(String),
}

/// Outcome of one `SpacedOpenLoopbackBridge` call. `bridge_status` is
/// `Some(numeric proto enum value)` only when we received a response —
/// transport failures (sbx-mgr connect, RPC error) return `None` so the
/// publish path leaves the column unchanged via `COALESCE` instead of
/// overwriting with `UNSPECIFIED` (which the cluster parser rejects and
/// would fail the entire batch). `service_generation` is the
/// spaced-guest-svc per-process random id — likewise `None` on transport
/// failure.
struct OpenLoopbackBridgeOutcome {
    result: Result<(), OpenLoopbackBridgeError>,
    bridge_status: Option<i32>,
    service_generation: Option<String>,
}

async fn open_loopback_bridge_for_exposure(
    sandbox_id: &str,
    port: u32,
) -> OpenLoopbackBridgeOutcome {
    use pplx_space_sbx_mgr_client::proto::SpacedLoopbackBridgeStatus as S;

    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            return OpenLoopbackBridgeOutcome {
                result: Err(OpenLoopbackBridgeError::Connect(error.to_string())),
                bridge_status: None,
                service_generation: None,
            };
        }
    };
    let response = match client
        .spaced_open_loopback_bridge(
            pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeRequest {
                sandbox_id: sandbox_id.to_string(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                        port,
                        wait_timeout: Some(prost_types::Duration {
                            seconds: 2,
                            nanos: 0,
                        }),
                    },
                ),
            },
        )
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) => {
            return OpenLoopbackBridgeOutcome {
                result: Err(OpenLoopbackBridgeError::Rpc(status)),
                bridge_status: None,
                service_generation: None,
            };
        }
    };
    let status = S::try_from(response.status).unwrap_or(S::Unspecified);
    let service_generation = response.service_generation.clone();
    // Only `Direct` and `LoopbackForwarded` count as `applied` — the
    // exposure represents the whole externally visible port contract, not
    // just the host DNAT half. `WaitingForListener` means the bridge is
    // armed but no loopback listener has appeared yet, so the port still
    // returns 502 to clients; that's not `applied`. The reconciler ticks
    // every poll interval and will promote the row once the watcher sees
    // the listener and transitions to `LoopbackForwarded`. `BindConflict`
    // / `Failed` / `Unspecified` are propagated for the same reason.
    // Symmetric with the api-side caller in `controllers/sandbox/ports.rs`.
    let result = match status {
        S::Direct | S::LoopbackForwarded => {
            tracing::debug!(
                sandbox_id,
                port,
                ?status,
                detail = %response.detail,
                service_generation = %service_generation,
                "reconcile installed loopback bridge"
            );
            Ok(())
        }
        S::WaitingForListener | S::BindConflict | S::Failed | S::Unspecified => {
            Err(OpenLoopbackBridgeError::NotReady(format!(
                "status={status:?} detail={} service_generation={service_generation}",
                response.detail
            )))
        }
    };
    OpenLoopbackBridgeOutcome {
        result,
        bridge_status: Some(response.status),
        service_generation: Some(service_generation),
    }
}

async fn close_loopback_bridge_for_exposure(sandbox_id: &str, port: u32) -> Result<(), String> {
    let mut client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| format!("connect sbx-mgr: {error}"))?;
    let _ = client
        .spaced_close_loopback_bridge(
            pplx_space_sbx_mgr_client::proto::SpacedCloseLoopbackBridgeRequest {
                sandbox_id: sandbox_id.to_string(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::CloseLoopbackBridgeRequest { port },
                ),
            },
        )
        .await
        .map_err(|error| format!("spaced_close_loopback_bridge: {error}"))?;
    Ok(())
}

fn port_protocol_label(value: i32) -> &'static str {
    match SandboxPortProtocol::try_from(value).unwrap_or(SandboxPortProtocol::Unspecified) {
        SandboxPortProtocol::Tcp => "tcp",
        SandboxPortProtocol::Unspecified => "unspecified",
    }
}

fn port_desired_state_label(value: i32) -> &'static str {
    match SandboxPortDesiredStateKind::try_from(value)
        .unwrap_or(SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified)
    {
        SandboxPortDesiredStateKind::SandboxPortDesiredStateOpen => "open",
        SandboxPortDesiredStateKind::SandboxPortDesiredStateClosed => "closed",
        SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified => "unspecified",
    }
}

/// sbx-manager RPCs the snapshot / auto-lifecycle desired-state apply sub-ops
/// issue. Kept as an enum so terminal-code classification keys off the exact
/// operation instead of substring-matching a formatted message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalDesiredStateRpc {
    EnableSnapshots,
    DisableSnapshots,
    SetAutoLifecyclePolicy,
}

impl LocalDesiredStateRpc {
    fn as_str(self) -> &'static str {
        match self {
            Self::EnableSnapshots => "enable snapshots",
            Self::DisableSnapshots => "disable snapshots",
            Self::SetAutoLifecyclePolicy => "set auto-lifecycle policy",
        }
    }
}

impl std::fmt::Display for LocalDesiredStateRpc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Failure applying a snapshot / auto-lifecycle desired-state sub-op against
/// the local sbx-manager. RPC failures preserve the tonic `Status` so a
/// guest-unreachable `FailedPrecondition` (sandbox failed/suspending/
/// suspended/stopped) is distinguishable from transient failures.
#[derive(Debug, thiserror::Error)]
enum LocalDesiredStateApplyError {
    /// A sbx-manager RPC failed.
    #[error("{operation}: {status}")]
    Rpc {
        operation: LocalDesiredStateRpc,
        #[source]
        status: tonic::Status,
    },
}

impl LocalDesiredStateApplyError {
    /// See [`sbx_mgr_desired_state_apply_is_terminal`].
    fn is_terminal(&self) -> bool {
        match self {
            Self::Rpc { status, .. } => sbx_mgr_desired_state_apply_is_terminal(status),
        }
    }
}

async fn apply_sandbox_snapshot_desired(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_id: &str,
    state: &SandboxInfo,
    snapshots_enabled_desired: bool,
) -> Result<(), LocalDesiredStateApplyError> {
    if state.snapshots_enabled == snapshots_enabled_desired {
        return Ok(());
    }

    let (operation, result) = if snapshots_enabled_desired {
        (
            LocalDesiredStateRpc::EnableSnapshots,
            client
                .enable_snapshots(local_manager_request(
                    EnableSnapshotsRequest {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    true,
                ))
                .await
                .map(|_| ()),
        )
    } else {
        (
            LocalDesiredStateRpc::DisableSnapshots,
            client
                .disable_snapshots(local_manager_request(
                    DisableSnapshotsRequest {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    true,
                ))
                .await
                .map(|_| ()),
        )
    };

    result.map_err(|status| LocalDesiredStateApplyError::Rpc { operation, status })
}

async fn apply_sandbox_auto_lifecycle_desired(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_id: &str,
    state: &SandboxInfo,
    policy: pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy,
) -> Result<(), LocalDesiredStateApplyError> {
    if state.auto_lifecycle.as_ref() == Some(&policy) {
        return Ok(());
    }

    client
        .set_auto_lifecycle_policy(local_manager_request(
            SetAutoLifecyclePolicyRequest {
                sandbox_id: sandbox_id.to_string(),
                policy: Some(policy),
                authority: PolicyAuthority::Cluster.as_proto_i32(),
            },
            true,
        ))
        .await
        .map(|_| ())
        .map_err(|status| LocalDesiredStateApplyError::Rpc {
            operation: LocalDesiredStateRpc::SetAutoLifecyclePolicy,
            status,
        })
}

/// Fold a snapshot / auto-lifecycle apply outcome into the reconcile stats and
/// element errors. `Ok` (applied or already in desired state) counts as
/// applied. A server-marked terminal state-gate failure publishes
/// the diagnostic and metric but records **no** element error, so the sandbox
/// desired-state ref completes. A separate lifecycle/cluster repair changes
/// the state, and periodic desired-state sync re-applies afterward; retrying
/// this unchanged ref could never converge and would pin the queue-lag gauge.
/// A transient failure records an element error so the queue re-drives it.
fn handle_local_desired_state_apply(
    result: Result<(), LocalDesiredStateApplyError>,
    sandbox_id: &str,
    stats: &mut ReconcileStats,
    element_errors: &mut BTreeMap<String, String>,
) {
    let Err(error) = result else {
        stats.sandboxes_applied = stats.sandboxes_applied.saturating_add(1);
        return;
    };
    let result_kind = if error.is_terminal() {
        tracing::info!(
            sandbox_id,
            %error,
            "terminal sandbox desired-state apply; completing ref until state changes"
        );
        pplx_space_metrics::reconcile::DesiredStateApplyResult::Terminal
    } else {
        tracing::warn!(
            sandbox_id,
            %error,
            "retryable sandbox desired-state apply failure"
        );
        record_element_error(element_errors, sandbox_id, &error.to_string());
        pplx_space_metrics::reconcile::DesiredStateApplyResult::Retryable
    };
    pplx_space_metrics::reconcile::desired_state_apply(result_kind);
}

#[cfg(test)]
mod tests;

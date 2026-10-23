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
    AllowPortRequest, DenyPortRequest, ExposePortRequest, ListNetworkLeasesRequest,
    NetworkProtocol, NetworkSlotState, UnexposePortRequest,
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
    FACET_NETWORK_POLICY, FACET_NODE_STATUS, FACET_RUNTIME_PROVISIONING,
    FACET_SANDBOX_DESIRED_STATE, FACET_SANDBOX_INVENTORY, FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
    FACET_TEMPLATE_DESIRED_STATE, FACET_TEMPLATE_INVENTORY, FacetRef,
};
use crate::{InboundFacetResult, ReconcileStats, connect_cluster_api, local_manager_request, util};

mod cluster_deleted_eviction;
mod execution_inventory;
mod inventory_publish;
mod local_state;
mod network_policy;
mod port_exposure;
mod runtime_provisioning;
pub(crate) use runtime_provisioning::Provisioning;
mod sandbox_desired_state;
mod template_desired_state;

use cluster_deleted_eviction::*;
use inventory_publish::*;
use local_state::*;
pub use network_policy::*;
use port_exposure::*;
use sandbox_desired_state::*;
use template_desired_state::*;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FacetKind {
    NetworkPolicy,
    SandboxDesiredState,
    SandboxInventory,
    TemplateInventory,
    TemplateBaseSnapshotPolicy,
    TemplateDesiredState,
    NodeStatus,
    RuntimeProvisioning,
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
    execution_observation: Option<execution_inventory::CollectedExecutionInventory>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SandboxDesiredReconcileAction {
    ApplyAll,
    ApplyPortExposuresAndRetry,
    SkipNonLocal,
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
    FacetKind::RuntimeProvisioning,
];

pub const FACET_PLUGINS: &[FacetPlugin] = &[
    FacetPlugin {
        kind: FacetKind::RuntimeProvisioning,
        inbound_tick: FacetTickMode::EveryTickFull,
        outbound_tick: FacetTickMode::Disabled,
    },
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
        outbound_tick: FacetTickMode::EveryTickFull,
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
            Self::RuntimeProvisioning => FACET_RUNTIME_PROVISIONING,
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
            Self::NodeStatus | Self::RuntimeProvisioning => {
                if element.is_some() {
                    return Err("whole-node facet does not accept an element".to_string());
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn run_inbound(
        self,
        node_id: &str,
        node_role: Option<NodeRole>,
        scope: FacetScope,
        progress: crate::FacetProgress,
        sync_interval: std::time::Duration,
        provisioning: &Provisioning,
    ) -> Result<InboundFacetResult, String> {
        match self {
            Self::RuntimeProvisioning => {
                runtime_provisioning::refresh(node_id, node_role, provisioning)
                    .await
                    .map(|()| InboundFacetResult::default())
                    .map_err(|error| error.to_string())
            }
            Self::NetworkPolicy => refresh_network_policy(node_id, node_role, scope, progress)
                .await
                .map(InboundFacetResult::from_stats),
            Self::SandboxDesiredState => {
                refresh_sandbox_desired_state(node_id, node_role, scope, sync_interval).await
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
            Self::TemplateBaseSnapshotPolicy
            | Self::TemplateDesiredState
            | Self::RuntimeProvisioning => Ok(ReconcileStats::default()),
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

fn proto_node_role(role: Option<NodeRole>) -> Option<i32> {
    role.map(|role| role as i32)
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
        && state.execution_observation.is_none()
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
    let (execution_inventory, execution_observation) = match state.execution_observation {
        Some(observation) => {
            let (legacy, report) = observation.into_publication();
            (legacy, Some(report))
        }
        None => (None, None),
    };
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
            execution_inventory,
            execution_observation,
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

#[cfg(test)]
mod tests;

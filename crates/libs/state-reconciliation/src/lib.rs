//! Node state-reconciliation vocabulary shared by the control plane and node
//! services.
//!
//! Holds the reconcile facets and their wire names, the facet observation
//! keys, the `state_reconciliation_reason` Postgres enum (`ReconcileReason`),
//! the facet and element references a reconcile trigger names, the facet and
//! claim generation counters, and network-policy targets. It depends only on
//! common-core's sandbox and template identifiers, sqlx and thiserror, so a
//! crate that needs only this vocabulary can depend on it without the service
//! runtime in `pplx-space-common-svc`, which re-exports it as
//! `types::reconcile`.

use std::{fmt, str::FromStr};

use pplx_space_common_core::types::{
    SandboxId, SandboxIdParseError, TemplateName, TemplateParseError,
};
use thiserror::Error;

pub const FACET_NETWORK_POLICY: &str = "network-policy";
pub const FACET_SANDBOX_INVENTORY: &str = "sandbox-inventory";
pub const FACET_TEMPLATE_INVENTORY: &str = "template-inventory";
pub const FACET_TEMPLATE_BASE_SNAPSHOT_POLICY: &str = "template-base-snapshot-policy";
pub const FACET_SANDBOX_DESIRED_STATE: &str = "sandbox-desired-state";
pub const FACET_TEMPLATE_DESIRED_STATE: &str = "template-desired-state";
pub const FACET_NODE_STATUS: &str = "node-status";
pub const FACET_RUNTIME_PROVISIONING: &str = "runtime-provisioning";
pub const SANDBOX_FAILURE_EPISODE_KEY: &str = "failure_episode";
pub const SANDBOX_FAILURE_EXECUTION_GENERATION_KEY: &str = "execution_generation";
pub const SANDBOX_FAILURE_POD_UID_KEY: &str = "pod_uid";
pub const SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY: &str = "activation_attempt_id";
/// Exact node-local suspend identity, before durable backup finalization.
/// This facet observation permits waiting only, never restoring artifact bytes.
pub const SANDBOX_LOCAL_SUSPEND_KEY: &str = "local_suspend";

pub const NETWORK_POLICY_TARGET_TEMPLATE_PREFIX: &str = "template:";
pub const NETWORK_POLICY_TARGET_SANDBOX_PREFIX: &str = "sandbox:";
pub const RECONCILE_REASON_MAX_CHARS: usize = 128;
pub const NODE_RECONCILE_CLAIM_STALE_SECS: i64 = 120;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FacetGeneration(u64);

impl FacetGeneration {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for FacetGeneration {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<FacetGeneration> for u64 {
    fn from(value: FacetGeneration) -> Self {
        value.get()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClaimGeneration(u64);

impl ClaimGeneration {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for ClaimGeneration {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<ClaimGeneration> for u64 {
    fn from(value: ClaimGeneration) -> Self {
        value.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ApplyDecisionKind {
    Skip,
    Apply,
    Repair,
    Conflict,
}

impl ApplyDecisionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::Apply => "apply",
            Self::Repair => "repair",
            Self::Conflict => "conflict",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReconcileFacet {
    NetworkPolicy,
    SandboxInventory,
    TemplateInventory,
    TemplateBaseSnapshotPolicy,
    SandboxDesiredState,
    TemplateDesiredState,
    NodeStatus,
    RuntimeProvisioning,
}

pub const RECONCILE_FACETS: &[ReconcileFacet] = &[
    ReconcileFacet::NetworkPolicy,
    ReconcileFacet::SandboxInventory,
    ReconcileFacet::TemplateInventory,
    ReconcileFacet::TemplateBaseSnapshotPolicy,
    ReconcileFacet::SandboxDesiredState,
    ReconcileFacet::TemplateDesiredState,
    ReconcileFacet::NodeStatus,
    ReconcileFacet::RuntimeProvisioning,
];

impl ReconcileFacet {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NetworkPolicy => FACET_NETWORK_POLICY,
            Self::SandboxInventory => FACET_SANDBOX_INVENTORY,
            Self::TemplateInventory => FACET_TEMPLATE_INVENTORY,
            Self::TemplateBaseSnapshotPolicy => FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
            Self::SandboxDesiredState => FACET_SANDBOX_DESIRED_STATE,
            Self::TemplateDesiredState => FACET_TEMPLATE_DESIRED_STATE,
            Self::NodeStatus => FACET_NODE_STATUS,
            Self::RuntimeProvisioning => FACET_RUNTIME_PROVISIONING,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            FACET_NETWORK_POLICY => Some(Self::NetworkPolicy),
            FACET_SANDBOX_INVENTORY => Some(Self::SandboxInventory),
            FACET_TEMPLATE_INVENTORY => Some(Self::TemplateInventory),
            FACET_TEMPLATE_BASE_SNAPSHOT_POLICY => Some(Self::TemplateBaseSnapshotPolicy),
            FACET_SANDBOX_DESIRED_STATE => Some(Self::SandboxDesiredState),
            FACET_TEMPLATE_DESIRED_STATE => Some(Self::TemplateDesiredState),
            FACET_NODE_STATUS => Some(Self::NodeStatus),
            FACET_RUNTIME_PROVISIONING => Some(Self::RuntimeProvisioning),
            _ => None,
        }
    }
}

impl fmt::Display for ReconcileFacet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, sqlx::Type)]
#[sqlx(type_name = "state_reconciliation_reason", rename_all = "snake_case")]
pub enum ReconcileReason {
    NodeRegistered,
    NodeUpdated,
    ManualNodeStatusRefresh,
    ManualNodeReconcile,
    ManualNodeNetworkPolicyReconcile,
    NodeTemplatePulled,
    SandboxRegistered,
    SandboxPortExposureUpdated,
    SandboxNetworkPolicyUpdated,
    SandboxDesiredStateUpdated,
    TemplateNetworkPolicyUpdated,
    TemplateDesiredStateUpdated,
    TemplateBaseSnapshotPolicyUpdated,
    TemplatePoolPolicyUpdated,
    TemplatePoolPolicyDeleted,
    NodeSandboxInventoryPublished,
    NodeTemplateInventoryPublished,
}

impl ReconcileReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeRegistered => "node_registered",
            Self::NodeUpdated => "node_updated",
            Self::ManualNodeStatusRefresh => "manual_node_status_refresh",
            Self::ManualNodeReconcile => "manual_node_reconcile",
            Self::ManualNodeNetworkPolicyReconcile => "manual_node_network_policy_reconcile",
            Self::NodeTemplatePulled => "node_template_pulled",
            Self::SandboxRegistered => "sandbox_registered",
            Self::SandboxPortExposureUpdated => "sandbox_port_exposure_updated",
            Self::SandboxNetworkPolicyUpdated => "sandbox_network_policy_updated",
            Self::SandboxDesiredStateUpdated => "sandbox_desired_state_updated",
            Self::TemplateNetworkPolicyUpdated => "template_network_policy_updated",
            Self::TemplateDesiredStateUpdated => "template_desired_state_updated",
            Self::TemplateBaseSnapshotPolicyUpdated => "template_base_snapshot_policy_updated",
            Self::TemplatePoolPolicyUpdated => "template_pool_policy_updated",
            Self::TemplatePoolPolicyDeleted => "template_pool_policy_deleted",
            Self::NodeSandboxInventoryPublished => "node_sandbox_inventory_published",
            Self::NodeTemplateInventoryPublished => "node_template_inventory_published",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "node_registered" => Some(Self::NodeRegistered),
            "node_updated" => Some(Self::NodeUpdated),
            "manual_node_status_refresh" => Some(Self::ManualNodeStatusRefresh),
            "manual_node_reconcile" => Some(Self::ManualNodeReconcile),
            "manual_node_network_policy_reconcile" => Some(Self::ManualNodeNetworkPolicyReconcile),
            "node_template_pulled" => Some(Self::NodeTemplatePulled),
            "sandbox_registered" => Some(Self::SandboxRegistered),
            "sandbox_port_exposure_updated" => Some(Self::SandboxPortExposureUpdated),
            "sandbox_network_policy_updated" => Some(Self::SandboxNetworkPolicyUpdated),
            "sandbox_desired_state_updated" => Some(Self::SandboxDesiredStateUpdated),
            "template_network_policy_updated" => Some(Self::TemplateNetworkPolicyUpdated),
            "template_desired_state_updated" => Some(Self::TemplateDesiredStateUpdated),
            "template_base_snapshot_policy_updated" => {
                Some(Self::TemplateBaseSnapshotPolicyUpdated)
            }
            "template_pool_policy_updated" => Some(Self::TemplatePoolPolicyUpdated),
            "template_pool_policy_deleted" => Some(Self::TemplatePoolPolicyDeleted),
            "node_sandbox_inventory_published" => Some(Self::NodeSandboxInventoryPublished),
            "node_template_inventory_published" => Some(Self::NodeTemplateInventoryPublished),
            _ => None,
        }
    }

    pub fn normalized(self) -> String {
        normalize_reconcile_reason(self.as_str())
    }
}

impl fmt::Display for ReconcileReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn normalize_reconcile_reason(reason: &str) -> String {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "unspecified"
    } else {
        reason
    };
    reason.chars().take(RECONCILE_REASON_MAX_CHARS).collect()
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReconcileElement {
    Sandbox(SandboxId),
    Template(TemplateName),
    NetworkPolicyTarget(NetworkPolicyTarget),
}

impl ReconcileElement {
    pub fn as_element_string(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for ReconcileElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sandbox(sandbox_id) => sandbox_id.fmt(f),
            Self::Template(template) => template.fmt(f),
            Self::NetworkPolicyTarget(target) => target.fmt(f),
        }
    }
}

#[derive(Debug, Error)]
pub enum ReconcileElementParseError {
    #[error("facet {0} does not accept an element")]
    UnexpectedElement(ReconcileFacet),
    #[error("facet {0} requires an element")]
    MissingElement(ReconcileFacet),
    #[error("network policy target parse failed: {0}")]
    NetworkPolicy(#[source] NetworkPolicyTargetParseError),
    #[error("sandbox id parse failed: {0}")]
    Sandbox(#[source] SandboxIdParseError),
    #[error("template parse failed: {0}")]
    Template(#[source] TemplateParseError),
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReconcileRef {
    facet: ReconcileFacet,
    element: Option<ReconcileElement>,
}

impl ReconcileRef {
    pub fn new(facet: ReconcileFacet, element: Option<ReconcileElement>) -> Self {
        Self { facet, element }
    }

    pub fn full(facet: ReconcileFacet) -> Self {
        Self {
            facet,
            element: None,
        }
    }

    pub fn from_storage(
        facet: ReconcileFacet,
        element: Option<&str>,
    ) -> Result<Self, ReconcileElementParseError> {
        let element = match facet {
            ReconcileFacet::NodeStatus | ReconcileFacet::RuntimeProvisioning => {
                if element.is_some_and(|value| !value.trim().is_empty()) {
                    return Err(ReconcileElementParseError::UnexpectedElement(facet));
                }
                None
            }
            ReconcileFacet::NetworkPolicy => {
                let element = element.ok_or(ReconcileElementParseError::MissingElement(facet))?;
                Some(ReconcileElement::NetworkPolicyTarget(
                    element
                        .parse()
                        .map_err(ReconcileElementParseError::NetworkPolicy)?,
                ))
            }
            ReconcileFacet::SandboxInventory | ReconcileFacet::SandboxDesiredState => {
                let element = element.ok_or(ReconcileElementParseError::MissingElement(facet))?;
                Some(ReconcileElement::Sandbox(
                    element
                        .parse()
                        .map_err(ReconcileElementParseError::Sandbox)?,
                ))
            }
            ReconcileFacet::TemplateInventory
            | ReconcileFacet::TemplateBaseSnapshotPolicy
            | ReconcileFacet::TemplateDesiredState => {
                let element = element.ok_or(ReconcileElementParseError::MissingElement(facet))?;
                Some(ReconcileElement::Template(
                    TemplateName::parse(element).map_err(ReconcileElementParseError::Template)?,
                ))
            }
        };
        Ok(Self { facet, element })
    }

    pub fn network_policy(target: NetworkPolicyTarget) -> Self {
        Self {
            facet: ReconcileFacet::NetworkPolicy,
            element: Some(ReconcileElement::NetworkPolicyTarget(target)),
        }
    }

    pub fn sandbox_inventory(sandbox_id: SandboxId) -> Self {
        Self {
            facet: ReconcileFacet::SandboxInventory,
            element: Some(ReconcileElement::Sandbox(sandbox_id)),
        }
    }

    pub fn sandbox_desired_state(sandbox_id: SandboxId) -> Self {
        Self {
            facet: ReconcileFacet::SandboxDesiredState,
            element: Some(ReconcileElement::Sandbox(sandbox_id)),
        }
    }

    pub fn template_inventory(template: TemplateName) -> Self {
        Self {
            facet: ReconcileFacet::TemplateInventory,
            element: Some(ReconcileElement::Template(template)),
        }
    }

    pub fn template_base_snapshot_policy(template: TemplateName) -> Self {
        Self {
            facet: ReconcileFacet::TemplateBaseSnapshotPolicy,
            element: Some(ReconcileElement::Template(template)),
        }
    }

    pub fn template_desired_state(template: TemplateName) -> Self {
        Self {
            facet: ReconcileFacet::TemplateDesiredState,
            element: Some(ReconcileElement::Template(template)),
        }
    }

    pub fn facet(&self) -> ReconcileFacet {
        self.facet
    }

    pub fn facet_str(&self) -> &'static str {
        self.facet.as_str()
    }

    pub fn element(&self) -> Option<&ReconcileElement> {
        self.element.as_ref()
    }

    pub fn element_string(&self) -> Option<String> {
        self.element
            .as_ref()
            .map(ReconcileElement::as_element_string)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReconcileElementRef {
    facet: ReconcileFacet,
    element: ReconcileElement,
}

impl ReconcileElementRef {
    pub fn new(facet: ReconcileFacet, element: ReconcileElement) -> Self {
        Self { facet, element }
    }

    pub fn network_policy(target: NetworkPolicyTarget) -> Self {
        Self {
            facet: ReconcileFacet::NetworkPolicy,
            element: ReconcileElement::NetworkPolicyTarget(target),
        }
    }

    pub fn sandbox_inventory(sandbox_id: SandboxId) -> Self {
        Self {
            facet: ReconcileFacet::SandboxInventory,
            element: ReconcileElement::Sandbox(sandbox_id),
        }
    }

    pub fn sandbox_desired_state(sandbox_id: SandboxId) -> Self {
        Self {
            facet: ReconcileFacet::SandboxDesiredState,
            element: ReconcileElement::Sandbox(sandbox_id),
        }
    }

    pub fn template_inventory(template: TemplateName) -> Self {
        Self {
            facet: ReconcileFacet::TemplateInventory,
            element: ReconcileElement::Template(template),
        }
    }

    pub fn template_base_snapshot_policy(template: TemplateName) -> Self {
        Self {
            facet: ReconcileFacet::TemplateBaseSnapshotPolicy,
            element: ReconcileElement::Template(template),
        }
    }

    pub fn template_desired_state(template: TemplateName) -> Self {
        Self {
            facet: ReconcileFacet::TemplateDesiredState,
            element: ReconcileElement::Template(template),
        }
    }

    pub fn facet(&self) -> ReconcileFacet {
        self.facet
    }

    pub fn facet_str(&self) -> &'static str {
        self.facet.as_str()
    }

    pub fn element(&self) -> &ReconcileElement {
        &self.element
    }

    pub fn element_string(&self) -> String {
        self.element.as_element_string()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NetworkPolicyTarget {
    Template(TemplateName),
    Sandbox(SandboxId),
}

impl NetworkPolicyTarget {
    pub fn template(template: TemplateName) -> Self {
        Self::Template(template)
    }

    pub fn sandbox(sandbox_id: SandboxId) -> Self {
        Self::Sandbox(sandbox_id)
    }

    pub fn as_element_string(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for NetworkPolicyTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Template(template) => {
                f.write_str(NETWORK_POLICY_TARGET_TEMPLATE_PREFIX)?;
                template.fmt(f)
            }
            Self::Sandbox(sandbox_id) => {
                f.write_str(NETWORK_POLICY_TARGET_SANDBOX_PREFIX)?;
                sandbox_id.fmt(f)
            }
        }
    }
}

impl FromStr for NetworkPolicyTarget {
    type Err = NetworkPolicyTargetParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(template) = value.strip_prefix(NETWORK_POLICY_TARGET_TEMPLATE_PREFIX) {
            return Ok(Self::Template(
                TemplateName::parse(template)
                    .map_err(NetworkPolicyTargetParseError::InvalidTemplate)?,
            ));
        }
        if let Some(sandbox_id) = value.strip_prefix(NETWORK_POLICY_TARGET_SANDBOX_PREFIX) {
            return Ok(Self::Sandbox(
                SandboxId::from_str(sandbox_id)
                    .map_err(NetworkPolicyTargetParseError::InvalidSandbox)?,
            ));
        }
        Err(NetworkPolicyTargetParseError::MissingPrefix)
    }
}

#[derive(Debug, Error)]
pub enum NetworkPolicyTargetParseError {
    #[error("network policy target must start with template: or sandbox:")]
    MissingPrefix,
    #[error("invalid template network policy target: {0}")]
    InvalidTemplate(#[source] TemplateParseError),
    #[error("invalid sandbox network policy target: {0}")]
    InvalidSandbox(#[source] SandboxIdParseError),
}

#[cfg(test)]
mod tests;

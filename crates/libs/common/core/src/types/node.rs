//! Node registry vocabulary.

use std::{fmt, net::IpAddr, str::FromStr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub const NODE_ROLE_WORKER: &str = "worker";
pub const NODE_ROLE_BUILDER: &str = "builder";
pub const NODE_ROLE_CLUSTER: &str = "cluster";
pub const NODE_MANAGED_BY_SPACE: &str = "space";
pub const NODE_MANAGED_BY_K8S: &str = "k8s";

/// Registered node IP address.
///
/// This is intentionally IP-only. Full gRPC endpoints and DNS names should use
/// endpoint/socket types instead.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeAddress(IpAddr);

impl NodeAddress {
    pub const fn from_ip_addr(value: IpAddr) -> Self {
        Self(value)
    }

    pub const fn as_ip_addr(self) -> IpAddr {
        self.0
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for NodeAddress {
    type Err = std::net::AddrParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse::<IpAddr>().map(Self)
    }
}

impl From<NodeAddress> for String {
    fn from(value: NodeAddress) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for NodeAddress {
    type Error = std::net::AddrParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

#[cfg(feature = "sqlx")]
impl pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres> for NodeAddress {
    fn type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::type_info()
    }

    fn compatible(ty: &pplx_space_sql_codec::postgres::PgTypeInfo) -> bool {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres> for NodeAddress {
    fn encode_by_ref(
        &self,
        buf: &mut pplx_space_sql_codec::postgres::PgArgumentBuffer,
    ) -> Result<pplx_space_sql_codec::encode::IsNull, pplx_space_sql_codec::error::BoxDynError>
    {
        <&str as pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres>>::encode(
            self.to_string().as_str(),
            buf,
        )
    }
}

#[cfg(feature = "sqlx")]
impl<'r> pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres> for NodeAddress {
    fn decode(
        value: pplx_space_sql_codec::postgres::PgValueRef<'r>,
    ) -> Result<Self, pplx_space_sql_codec::error::BoxDynError> {
        let raw =
            <&str as pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres>>::decode(
                value,
            )?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// Error returned when a node pool name fails validation.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum NodePoolNameParseError {
    /// The value was empty.
    #[error("node pool name must not be empty")]
    Empty,
    /// The value was not a valid Kubernetes label value.
    #[error(transparent)]
    InvalidLabelValue(#[from] pplx_space_k8s_ids::LabelValueParseError),
}

/// Validated node pool name, used to pin a sandbox to an isolation node pool.
///
/// A non-empty Kubernetes label value (see [`pplx_space_k8s_ids::LabelValue`]) — the
/// grammar is not re-implemented here. It is rendered as the value of the pool
/// discriminator label (see `SANDBOX_PLACEMENT_POOL_LABEL_KEY`), so it must be
/// a legal label value. The matching isolation-taint toleration uses
/// `operator = Exists`; empty is rejected because it could not pin anything.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodePoolName(pplx_space_k8s_ids::LabelValue);

impl NodePoolName {
    /// Parses and validates a node pool name (a non-empty label value).
    pub fn parse(value: impl Into<String>) -> Result<Self, NodePoolNameParseError> {
        let value = value.into();
        if value.is_empty() {
            return Err(NodePoolNameParseError::Empty);
        }
        Ok(Self(pplx_space_k8s_ids::LabelValue::parse(value)?))
    }

    /// Returns the validated node pool name.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for NodePoolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for NodePoolName {
    type Err = NodePoolNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for NodePoolName {
    type Error = NodePoolNameParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for NodePoolName {
    type Error = NodePoolNameParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<NodePoolName> for String {
    fn from(value: NodePoolName) -> Self {
        value.0.as_str().to_string()
    }
}

/// SPACE node role.
///
/// This is the storage/domain vocabulary for registered nodes. Wire-specific
/// enums convert into this type at service boundaries.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeRole {
    Worker,
    Builder,
    Cluster,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported node role '{0}'")]
pub struct NodeRoleParseError(String);

impl NodeRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Worker => NODE_ROLE_WORKER,
            Self::Builder => NODE_ROLE_BUILDER,
            Self::Cluster => NODE_ROLE_CLUSTER,
        }
    }

    pub fn parse(value: &str) -> Result<Self, NodeRoleParseError> {
        match value {
            NODE_ROLE_WORKER => Ok(Self::Worker),
            NODE_ROLE_BUILDER => Ok(Self::Builder),
            NODE_ROLE_CLUSTER => Ok(Self::Cluster),
            other => Err(NodeRoleParseError(other.to_string())),
        }
    }
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NodeRole {
    type Err = NodeRoleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl From<NodeRole> for String {
    fn from(value: NodeRole) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for NodeRole {
    type Error = NodeRoleParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

#[cfg(feature = "sqlx")]
impl pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres> for NodeRole {
    fn type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::type_info()
    }

    fn compatible(ty: &pplx_space_sql_codec::postgres::PgTypeInfo) -> bool {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres> for NodeRole {
    fn encode_by_ref(
        &self,
        buf: &mut pplx_space_sql_codec::postgres::PgArgumentBuffer,
    ) -> Result<pplx_space_sql_codec::encode::IsNull, pplx_space_sql_codec::error::BoxDynError>
    {
        <&str as pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres>>::encode(
            self.as_str(),
            buf,
        )
    }
}

#[cfg(feature = "sqlx")]
impl<'r> pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres> for NodeRole {
    fn decode(
        value: pplx_space_sql_codec::postgres::PgValueRef<'r>,
    ) -> Result<Self, pplx_space_sql_codec::error::BoxDynError> {
        let raw =
            <&str as pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres>>::decode(
                value,
            )?;
        Self::parse(raw).map_err(Into::into)
    }
}

/// Control plane that owns a registered node's backing runtime placement.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeManagedBy {
    Space,
    Kubernetes,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported node managed_by value '{0}'")]
pub struct NodeManagedByParseError(String);

impl NodeManagedBy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Space => NODE_MANAGED_BY_SPACE,
            Self::Kubernetes => NODE_MANAGED_BY_K8S,
        }
    }

    pub fn parse(value: &str) -> Result<Self, NodeManagedByParseError> {
        match value {
            NODE_MANAGED_BY_SPACE => Ok(Self::Space),
            NODE_MANAGED_BY_K8S => Ok(Self::Kubernetes),
            other => Err(NodeManagedByParseError(other.to_string())),
        }
    }
}

impl fmt::Display for NodeManagedBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NodeManagedBy {
    type Err = NodeManagedByParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[cfg(feature = "sqlx")]
impl pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres> for NodeManagedBy {
    fn type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::type_info()
    }

    fn compatible(ty: &pplx_space_sql_codec::postgres::PgTypeInfo) -> bool {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres> for NodeManagedBy {
    fn encode_by_ref(
        &self,
        buf: &mut pplx_space_sql_codec::postgres::PgArgumentBuffer,
    ) -> Result<pplx_space_sql_codec::encode::IsNull, pplx_space_sql_codec::error::BoxDynError>
    {
        <&str as pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres>>::encode(
            self.as_str(),
            buf,
        )
    }
}

#[cfg(feature = "sqlx")]
impl<'r> pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres> for NodeManagedBy {
    fn decode(
        value: pplx_space_sql_codec::postgres::PgValueRef<'r>,
    ) -> Result<Self, pplx_space_sql_codec::error::BoxDynError> {
        let raw =
            <&str as pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres>>::decode(
                value,
            )?;
        Self::parse(raw).map_err(Into::into)
    }
}

impl From<NodeManagedBy> for String {
    fn from(value: NodeManagedBy) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for NodeManagedBy {
    type Error = NodeManagedByParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

/// Substrate that owns a sandbox or node: the value of control-plane Postgres
/// `sandboxes.managed_by` and `nodes.managed_by`, and of a route identity's
/// `substrate_kind`.
///
/// `NodeManagedBy` predates its use for sandbox rows; new and converted code
/// names the value through this alias. It encodes as the column's text, so SQL
/// binds it as a parameter (`managed_by = $1` with
/// `.bind(SubstrateKind::Kubernetes)`) rather than spelling `'k8s'`.
pub type SubstrateKind = NodeManagedBy;

/// Facts about one row's substrate, its sandbox or node `managed_by`, which
/// may differ from the substrate the running service uses. Shared code that
/// decides something from a row's substrate calls one of these instead of
/// matching a variant or comparing the label. Two exceptions: Rust that
/// repeats a statement's fixed substrate scope compares against the named
/// const the statement binds (such as the admin node registry's
/// `REGISTRY_SUBSTRATE`), and code that maps a variant to a value (a wire
/// label, a route identity, a constructed target) may match it. Facts about
/// the running substrate are the api crate's `SubstrateCapabilities`. Each
/// caller says what its decision does with the answer and with a label that
/// does not parse.
impl SubstrateKind {
    /// Does the substrate durably publish a sandbox's activation ownership
    /// before it launches a runtime, so that a row carrying none of that
    /// ownership has no runtime?
    ///
    /// - Kubernetes: yes.
    /// - Space: no. A direct launch records nothing durable first.
    pub const fn proves_execution_absence(self) -> bool {
        match self {
            Self::Kubernetes => true,
            Self::Space => false,
        }
    }

    /// When the control path cannot connect to a sandbox's runtime, can a
    /// wake give the sandbox a runtime to connect to?
    ///
    /// - Kubernetes: yes. A dormant sandbox (suspended, or created and never
    ///   started) has no pod until a wake creates one.
    /// - Space: no. Direct wakes a suspended or restoring sandbox on the
    ///   access path before connecting (`DirectSubstrate::access_runtime_client`
    ///   in the api crate), not after a failed control connect.
    pub const fn wakes_after_failed_control_connect(self) -> bool {
        match self {
            Self::Kubernetes => true,
            Self::Space => false,
        }
    }

    /// Is a node listed in the live node inventory, the Kubernetes node list?
    ///
    /// - Kubernetes: yes.
    /// - Space: no. Its nodes are registered through the admin API and never
    ///   appear in that inventory.
    pub const fn listed_in_live_node_inventory(self) -> bool {
        match self {
            Self::Kubernetes => true,
            Self::Space => false,
        }
    }

    /// Does a sandbox restore through a Kubernetes restore pod?
    ///
    /// - Kubernetes: yes.
    /// - Space: no.
    pub const fn restores_through_pods(self) -> bool {
        match self {
            Self::Kubernetes => true,
            Self::Space => false,
        }
    }

    /// Does the cluster controller's node-drain reconciler manage a node?
    /// Its draining and terminating listings bind the one substrate that
    /// answers yes (`DRAIN_RECONCILER_SUBSTRATE` in its store). The running
    /// substrate's counterpart is `SubstrateCapabilities::supports_node_drain`
    /// in the api crate.
    ///
    /// - Kubernetes: yes. The reconciler itself is opt-in, which this cannot
    ///   see.
    /// - Space: no.
    pub const fn has_node_drain_reconciler(self) -> bool {
        match self {
            Self::Kubernetes => true,
            Self::Space => false,
        }
    }
}

pub use pplx_space_node_drain::{
    NODE_DRAIN_MODE_FORCED, NODE_DRAIN_MODE_GENTLE, NODE_DRAIN_PHASE_DRAINED,
    NODE_DRAIN_PHASE_DRAINING, NODE_DRAIN_PHASE_TERMINATING, NodeDrainMode,
    NodeDrainModeParseError, NodeDrainPhase, NodeDrainPhaseParseError, NodeDrainState,
};

#[cfg(test)]
mod tests;

//! Node registry vocabulary.

use std::{fmt, net::IpAddr, str::FromStr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub const NODE_ROLE_WORKER: &str = "worker";
pub const NODE_ROLE_BUILDER: &str = "builder";
pub const NODE_ROLE_CLUSTER: &str = "cluster";
pub const NODE_MANAGED_BY_SPACE: &str = "space";
pub const NODE_MANAGED_BY_K8S: &str = "k8s";
pub const NODE_DRAIN_MODE_FORCED: &str = "forced";
pub const NODE_DRAIN_MODE_GENTLE: &str = "gentle";
pub const NODE_DRAIN_PHASE_DRAINING: &str = "draining";
pub const NODE_DRAIN_PHASE_DRAINED: &str = "drained";
pub const NODE_DRAIN_PHASE_TERMINATING: &str = "terminating";

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
impl sqlx::Type<sqlx::Postgres> for NodeAddress {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for NodeAddress {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.to_string().as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NodeAddress {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
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
    InvalidLabelValue(#[from] crate::types::LabelValueParseError),
}

/// Validated node pool name, used to pin a sandbox to an isolation node pool.
///
/// A non-empty Kubernetes label value (see [`crate::types::LabelValue`]) — the
/// grammar is not re-implemented here. It is rendered as the value of the pool
/// discriminator label (see `SANDBOX_PLACEMENT_POOL_LABEL_KEY`), so it must be
/// a legal label value. The matching isolation-taint toleration uses
/// `operator = Exists`; empty is rejected because it could not pin anything.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodePoolName(crate::types::LabelValue);

impl NodePoolName {
    /// Parses and validates a node pool name (a non-empty label value).
    pub fn parse(value: impl Into<String>) -> Result<Self, NodePoolNameParseError> {
        let value = value.into();
        if value.is_empty() {
            return Err(NodePoolNameParseError::Empty);
        }
        Ok(Self(crate::types::LabelValue::parse(value)?))
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

/// Kubernetes pod UID.
///
/// Kubernetes treats UIDs as opaque strings. SPACE keeps them typed because
/// they are execution identity material, not user content.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PodUid(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("pod uid must not be empty")]
pub struct PodUidParseError;

impl PodUid {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PodUidParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PodUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PodUid {
    type Err = PodUidParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(PodUidParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<PodUid> for String {
    fn from(value: PodUid) -> Self {
        value.0
    }
}

impl TryFrom<String> for PodUid {
    type Error = PodUidParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for PodUid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PodUid {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PodUid {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// Kubernetes pod name (`metadata.name`).
///
/// Validation is deliberately minimal — non-empty and RFC 1123 DNS
/// subdomain-shaped (lowercase alphanumerics, `-`, `.`, at most 253 chars,
/// alphanumeric at both ends) — enough to reject an obviously wrong value
/// without re-implementing the apiserver's validation.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PodName(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("pod name must be a non-empty lowercase DNS subdomain")]
pub struct PodNameParseError;

impl PodName {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PodNameParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PodName {
    type Err = PodNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if !is_dns_subdomain(value) {
            return Err(PodNameParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<PodName> for String {
    fn from(value: PodName) -> Self {
        value.0
    }
}

impl TryFrom<String> for PodName {
    type Error = PodNameParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for PodName {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PodName {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PodName {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// Kubernetes pod `metadata.resourceVersion`.
///
/// The apiserver defines this as an opaque, non-comparable token: it is only
/// ever tested for equality, and a client that parses or orders it is wrong.
/// Typing it keeps it from being mixed with a UID or a name — the three are
/// all strings and all pod identity material, and the one place they are used
/// together is a delete precondition, where confusing two of them silently
/// weakens the fence instead of failing.
///
/// Deliberately not `Ord`. Kubernetes defines resourceVersion as opaque with
/// no ordering — only equality is meaningful — so deriving it would make
/// `a < b` compile and read as "older than" while actually comparing strings
/// lexicographically.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PodResourceVersion(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("pod resource version must not be empty")]
pub struct PodResourceVersionParseError;

impl PodResourceVersion {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PodResourceVersionParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PodResourceVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PodResourceVersion {
    type Err = PodResourceVersionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(PodResourceVersionParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<PodResourceVersion> for String {
    fn from(value: PodResourceVersion) -> Self {
        value.0
    }
}

impl TryFrom<String> for PodResourceVersion {
    type Error = PodResourceVersionParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// Kubernetes node name (`node_k8s.k8s_node_name`, the kube Node's
/// `metadata.name`).
///
/// Same deliberately minimal DNS-subdomain validation as [`PodName`]. This
/// is the kube-side name, not a SPACE node id — resolving one to the other
/// goes through the `node_k8s` mapping.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct K8sNodeName(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("kubernetes node name must be a non-empty lowercase DNS subdomain")]
pub struct K8sNodeNameParseError;

impl K8sNodeName {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, K8sNodeNameParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for K8sNodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for K8sNodeName {
    type Err = K8sNodeNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if !is_dns_subdomain(value) {
            return Err(K8sNodeNameParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<K8sNodeName> for String {
    fn from(value: K8sNodeName) -> Self {
        value.0
    }
}

impl TryFrom<String> for K8sNodeName {
    type Error = K8sNodeNameParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for K8sNodeName {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for K8sNodeName {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for K8sNodeName {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// Kubernetes node UID (`node_k8s.k8s_node_uid`, the kube Node's
/// `metadata.uid`).
///
/// Like [`PodUid`], kube treats it as an opaque string and so does SPACE:
/// it is node-identity material — a same-name replacement node gets a fresh
/// UID — so only non-emptiness is validated.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct K8sNodeUid(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("kubernetes node uid must not be empty")]
pub struct K8sNodeUidParseError;

impl K8sNodeUid {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, K8sNodeUidParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for K8sNodeUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for K8sNodeUid {
    type Err = K8sNodeUidParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(K8sNodeUidParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<K8sNodeUid> for String {
    fn from(value: K8sNodeUid) -> Self {
        value.0
    }
}

impl TryFrom<String> for K8sNodeUid {
    type Error = K8sNodeUidParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for K8sNodeUid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for K8sNodeUid {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for K8sNodeUid {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// RFC 1123 DNS subdomain shape, the loose common denominator for kube pod
/// and node names.
fn is_dns_subdomain(value: &str) -> bool {
    let alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let bytes = value.as_bytes();
    bytes.len() <= 253
        && bytes.iter().all(|&b| alnum(b) || b == b'-' || b == b'.')
        && bytes.first().copied().is_some_and(alnum)
        && bytes.last().copied().is_some_and(alnum)
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
impl sqlx::Type<sqlx::Postgres> for NodeRole {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for NodeRole {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NodeRole {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
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
impl sqlx::Type<sqlx::Postgres> for NodeManagedBy {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for NodeManagedBy {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NodeManagedBy {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
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
    /// - Space: no. Direct wakes a dormant sandbox on the access path before
    ///   connecting (`DirectSubstrate::access_runtime_client` in the api
    ///   crate), not after a failed control connect.
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

/// Stage of a node's drain, as stored in `nodes.drain_phase`. NULL in that
/// column means no drain is in progress and has no variant here — model it as
/// `Option<NodeDrainPhase>`.
///
/// The lifecycle is `NULL -> draining -> drained -> terminating -> row
/// deleted`. There is no `deleted` variant: a successful terminate DELETEs the
/// `nodes` row.
///
/// Several modules predate this enum and still carry private `&'static str`
/// phase constants (the cluster controller's `node_drain` reconciler, the
/// admin `:drain` route). New code reads and writes the phase through this
/// type; migrating those constants onto it is a mechanical follow-up kept out
/// of the change that introduced drain modes, so a semantic change to draining
/// and a rename sweep across the drain state machine stay separately
/// reviewable.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeDrainPhase {
    Draining,
    Drained,
    Terminating,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported node drain phase '{0}'; expected 'draining', 'drained' or 'terminating'")]
pub struct NodeDrainPhaseParseError(String);

impl NodeDrainPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draining => NODE_DRAIN_PHASE_DRAINING,
            Self::Drained => NODE_DRAIN_PHASE_DRAINED,
            Self::Terminating => NODE_DRAIN_PHASE_TERMINATING,
        }
    }

    pub fn parse(value: &str) -> Result<Self, NodeDrainPhaseParseError> {
        match value {
            NODE_DRAIN_PHASE_DRAINING => Ok(Self::Draining),
            NODE_DRAIN_PHASE_DRAINED => Ok(Self::Drained),
            NODE_DRAIN_PHASE_TERMINATING => Ok(Self::Terminating),
            other => Err(NodeDrainPhaseParseError(other.to_string())),
        }
    }
}

impl fmt::Display for NodeDrainPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NodeDrainPhase {
    type Err = NodeDrainPhaseParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl From<NodeDrainPhase> for String {
    fn from(value: NodeDrainPhase) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for NodeDrainPhase {
    type Error = NodeDrainPhaseParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for NodeDrainPhase {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for NodeDrainPhase {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NodeDrainPhase {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::parse(raw).map_err(Into::into)
    }
}

/// A node's drain state as the placement guards see it: the phase, plus the mode
/// chosen for that drain.
///
/// Carried as one value because every question worth asking depends on both — a
/// `draining` node blocks user calls only in forced mode, while `drained` and
/// `terminating` block in either.
///
/// Lives here rather than in either consumer because there are two, and they must
/// agree: the api-server's user-API drain gate, and the admin
/// `:force-upload-local-snapshots` endpoint, whose byte-stability story is
/// precisely "the drain gate is refusing wakes right now".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeDrainState {
    pub phase: NodeDrainPhase,
    pub mode: NodeDrainMode,
}

impl NodeDrainState {
    /// Resolve a `(drain_phase, drain_mode)` column pair. `None` when the node
    /// has no drain in progress.
    pub fn from_columns(phase: Option<NodeDrainPhase>, mode: Option<&str>) -> Option<Self> {
        phase.map(|phase| Self {
            phase,
            mode: NodeDrainMode::from_column_str(mode),
        })
    }

    /// Should a placement-affecting or access call against a sandbox resolved to
    /// this node be rejected?
    ///
    /// A node in `drained` or `terminating` blocks regardless of mode: it is
    /// about to be destroyed, so placing or re-placing a sandbox on it is never
    /// right.
    ///
    /// `draining` is where the mode decides:
    ///
    /// - **Forced** blocks. The reconciler is actively suspending and migrating
    ///   residents, and an unguarded user call would race that pipeline — a wake
    ///   could re-pin a pod to the cordoned source node between the suspend and
    ///   the restore, or resume a VM the reconciler just paused to snapshot.
    /// - **Gentle** does not block. There is no pipeline to race: the reconciler
    ///   only cordons and waits, and the point of the mode is that residents keep
    ///   serving traffic normally until they leave on their own. Blocking here
    ///   would manufacture exactly the disruption the mode exists to avoid, for a
    ///   drain that may legitimately last days.
    ///
    /// Two consequences of not blocking in gentle mode are intended. A wake of a
    /// suspended sandbox whose only snapshot is still local-only lands back on
    /// the draining node, adding a resident that has to leave again — accepted,
    /// because the alternative is 409ing a user's sandbox for the whole drain,
    /// and promotion keeps making those snapshots S3-backed, after which restore
    /// affinity sends them elsewhere. And a gentle drain an operator escalates
    /// to forced starts blocking from that moment, because the guards read the
    /// live mode rather than the mode at drain start.
    ///
    /// The inverse reading matters too, and is why this is shared: anything whose
    /// safety depends on wakes being refused (the legacy sandbox-id-only snapshot
    /// reupload, which reads the live on-disk snapshot directory) may only run
    /// when this returns `true`.
    pub const fn blocks_placement(self) -> bool {
        match self.phase {
            NodeDrainPhase::Draining => self.mode.blocks_user_calls_while_draining(),
            NodeDrainPhase::Drained | NodeDrainPhase::Terminating => true,
        }
    }
}

/// How aggressively the node-drain reconciler empties a `draining` node.
///
/// Stored in `nodes.drain_mode`, chosen per drain by the operator through the
/// admin `:drain` request body. Orthogonal to `nodes.drain_phase`: a node in
/// either mode is cordoned, is excluded from placement, advances
/// `draining → drained` on the same zero-blockers gate, and terminates through
/// the same path. The mode governs only what the reconciler does to sandboxes
/// that are already resident, and whether the user-API drain gates reject
/// calls against them.
///
/// [`Self::Gentle`] is the answer everywhere a mode is missing or unreadable
/// rather than chosen — a `:drain` body with no `mode` (see
/// `pplx_space_common_core::types::admin::DrainRequest`), a NULL
/// `nodes.drain_mode` column, and a column value this build does not recognize
/// (the latter two via [`Self::from_column_str`]). That is one rule, and it is a safety
/// property rather than a preference: [`Self::Forced`] is the destructive mode,
/// so it is reachable only through an explicit, recognized `forced`. Every
/// other path degrades into "leave the residents alone", which stalls a drain
/// instead of suspending, evicting and restoring every sandbox on the node.
///
/// Because that rule is now uniform, [`Default`] states it once
/// ([`Self::Gentle`]) and both fallbacks defer to it. The leniency stops at the
/// column: [`Self::parse`] / [`FromStr`] / [`TryFrom<String>`] stay strict, so
/// a typo in a `:drain` body is a 400 rather than a silent gentle drain — a
/// rollout script that believes it asked for `forced` and got gentle would
/// cordon its nodes and wait forever while reporting success.
///
/// - [`Self::Forced`] — the original behavior. Every resident `started` /
///   `paused` sandbox is evacuated immediately (suspend → evict → restore
///   elsewhere), and the user-API drain gates return 409 on every
///   placement-affecting and access path to a resident sandbox for the whole
///   drain window, so users cannot race the evacuation pipeline. Per-node
///   disruption is proportional to resident live-sandbox count.
/// - [`Self::Gentle`] — no proactive evacuation. Residents keep serving
///   traffic and leave on their own (auto-suspend on inactivity, a user
///   suspend, or a delete). The local-snapshot promotion machinery still runs,
///   so local-only suspended artifacts become S3-backed and their restores
///   redirect off the node. Trades drain latency for zero user-visible
///   disruption; a node hosting a sandbox that never suspends never converges
///   on its own, which is what the admin `:drain` endpoint's in-place
///   escalation is for — a second `:drain` with `mode: forced` against a node
///   already `draining` rewrites this column without disturbing `drain_phase`
///   or `drain_started_at`, so the drain becomes destructive without losing
///   its age.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeDrainMode {
    Forced,
    /// The default: see the type-level docs. A missing, NULL or unrecognized
    /// mode is gentle so the destructive branch needs an explicit token.
    #[default]
    Gentle,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported node drain mode '{0}'; expected 'forced' or 'gentle'")]
pub struct NodeDrainModeParseError(String);

impl NodeDrainMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Forced => NODE_DRAIN_MODE_FORCED,
            Self::Gentle => NODE_DRAIN_MODE_GENTLE,
        }
    }

    pub fn parse(value: &str) -> Result<Self, NodeDrainModeParseError> {
        match value {
            NODE_DRAIN_MODE_FORCED => Ok(Self::Forced),
            NODE_DRAIN_MODE_GENTLE => Ok(Self::Gentle),
            other => Err(NodeDrainModeParseError(other.to_string())),
        }
    }

    /// Does the reconciler proactively evacuate residents in this mode?
    pub const fn evacuates_residents(self) -> bool {
        matches!(self, Self::Forced)
    }

    /// Do the user-API drain gates reject placement-affecting and access
    /// calls against sandboxes resident on a node draining in this mode?
    ///
    /// Only meaningful for `drain_phase = 'draining'`: `drained` and
    /// `terminating` block regardless of mode, because the node is about to
    /// be destroyed.
    pub const fn blocks_user_calls_while_draining(self) -> bool {
        matches!(self, Self::Forced)
    }

    /// Interpret a raw `nodes.drain_mode` column value. **This is the only way
    /// drain-decision code should read that column.**
    ///
    /// NULL means no mode was recorded — the value on every node nobody has
    /// drained, and the value a future writer leaves if it forgets to set one.
    /// An unrecognized string means this build does not know the value someone
    /// wrote. Both read as [`Self::Gentle`], because neither is an operator
    /// asking to evacuate a node's sandboxes, and resolving either to `forced`
    /// would make the evacuating mode reachable by omission.
    ///
    /// An unrecognized value is deliberately *not* an error. Failing the read
    /// would propagate as a 500 from the user-API drain gate for every sandbox
    /// resolved to that node, turning one bad column value into an outage on
    /// healthy sandboxes. The column carries **no** `CHECK` constraint — the
    /// closed set is enforced here, in application code, so that adding a third
    /// mode costs one variant instead of a migration to widen the constraint
    /// plus an ordered follow-up code PR. That makes this branch load-bearing
    /// rather than defensive: any writer can put an unknown string in the
    /// column, including a newer service writing a mode an older reader has
    /// never heard of during a rollout.
    ///
    /// This resolver is **silent** — no logging. It sits on the per-request
    /// user-API drain gate, so warning here would emit one line per request for
    /// as long as one node carries a bad value, and the rollout that produces
    /// such a value is exactly when request volume is highest. Discoverability
    /// is the caller's job: [`Self::from_column_str_checked`] hands back the
    /// parse error, and the cluster controller's per-tick listing logs it once
    /// per node. The admin node listing resolves silently — it renders the
    /// effective mode, and a reader chasing an anomaly has the reconciler's warn.
    ///
    /// Takes `&str` rather than an already-decoded `Option<Self>` precisely so
    /// the unrecognized case is reachable: a typed `Option<NodeDrainMode>`
    /// column read would have failed in sqlx's `Decode` before this function
    /// ever saw it.
    pub fn from_column_str(value: Option<&str>) -> Self {
        Self::from_column_str_checked(value).unwrap_or_default()
    }

    /// [`Self::from_column_str`], but reporting an unrecognized value instead of
    /// absorbing it.
    ///
    /// Two entry points exist because the two readers want different things: the
    /// per-request drain gate wants the resolved mode and must stay silent, while
    /// the reconciler's per-tick listing wants to log what it could not parse. A
    /// caller that needs both gets them from one parse here rather than asking
    /// twice. NULL is not an error — it is the documented absence of a choice, so
    /// it resolves to [`Self::Gentle`] on the `Ok` path.
    pub fn from_column_str_checked(value: Option<&str>) -> Result<Self, NodeDrainModeParseError> {
        value.map_or(Ok(Self::Gentle), Self::parse)
    }
}

impl fmt::Display for NodeDrainMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NodeDrainMode {
    type Err = NodeDrainModeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl From<NodeDrainMode> for String {
    fn from(value: NodeDrainMode) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for NodeDrainMode {
    type Error = NodeDrainModeParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for NodeDrainMode {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

/// Writes only. There is deliberately **no** `Decode` impl: a typed column read
/// would fail the whole row on a value this build does not recognize, which is
/// exactly what [`NodeDrainMode::from_column_str`] exists to avoid. Reading the
/// column as `Option<String>` and resolving through that function is therefore
/// the only way, enforced here by the absence of the impl rather than by
/// convention.
#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for NodeDrainMode {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(test)]
mod tests;

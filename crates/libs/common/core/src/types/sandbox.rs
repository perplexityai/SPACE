//! Sandbox identity, selection, metadata, and lifecycle types.
//!
//! These types keep stable sandbox identity separate from human lookup input,
//! display labels, PII-valued user names, and lifecycle state.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::{SandboxId, SandboxSelector};
//! let id = SandboxId::new();
//! let selector = SandboxSelector::parse(id.label().to_string())?;
//! assert!(selector.matches_id(id));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::auth::NodeId;
use super::host_linux::{LINUX_NETWORK_SLOT_LABEL_WIDTH, MAX_LINUX_NETWORK_SLOT};
use super::node::{NodeManagedBy, PodUid};

const SANDBOX_LABEL_HEX_LEN: usize = 8;
const SANDBOX_RUNTIME_LABEL_MAX_LEN: usize =
    max_usize(SANDBOX_LABEL_HEX_LEN, LINUX_NETWORK_SLOT_LABEL_WIDTH);
const MAX_SANDBOX_METADATA_TOTAL_BYTES: usize = 64 * 1024;

/// Maximum number of desired-open TCP port exposures admitted for one sandbox.
/// Already-open rows are grandfathered so rollout cannot disable a legacy
/// sandbox whose durable desired state predates this limit.
pub const MAX_SANDBOX_PORT_EXPOSURES: usize = 128;

/// Error returned when a sandbox execution generation is not positive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("sandbox execution generation must be positive")]
pub struct ExecutionGenerationParseError;

/// Monotonically increasing identity of one sandbox execution incarnation.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "i64", into = "i64"))]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExecutionGeneration(i64);

impl ExecutionGeneration {
    pub fn new(value: i64) -> Result<Self, ExecutionGenerationParseError> {
        (value > 0)
            .then_some(Self(value))
            .ok_or(ExecutionGenerationParseError)
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for ExecutionGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ExecutionGeneration {
    type Err = ExecutionGenerationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<i64>()
            .ok()
            .and_then(|value| Self::new(value).ok())
            .ok_or(ExecutionGenerationParseError)
    }
}

impl TryFrom<i64> for ExecutionGeneration {
    type Error = ExecutionGenerationParseError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ExecutionGeneration {
    type Error = ExecutionGenerationParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl From<ExecutionGeneration> for i64 {
    fn from(value: ExecutionGeneration) -> Self {
        value.get()
    }
}

/// Runtime adapter intent for atomically releasing one sandbox execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReleaseIntent {
    Stop,
    Delete,
}

impl ExecutionReleaseIntent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Delete => "delete",
        }
    }
}

impl fmt::Display for ExecutionReleaseIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why sandbox-manager retained lifecycle ownership during execution release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReleaseDetachReason {
    UnknownLifecycle,
    LifecycleNotReleasable,
    ManagerCleanupInProgress,
    ExecutionGenerationMismatch,
    SandboxRowMissing,
}

impl ExecutionReleaseDetachReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownLifecycle => "unknown_lifecycle",
            Self::LifecycleNotReleasable => "lifecycle_not_releasable",
            Self::ManagerCleanupInProgress => "manager_cleanup_in_progress",
            Self::ExecutionGenerationMismatch => "execution_generation_mismatch",
            Self::SandboxRowMissing => "sandbox_row_missing",
        }
    }
}

impl fmt::Display for ExecutionReleaseDetachReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for ExecutionGeneration {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <i64 as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for ExecutionGeneration {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i64 as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ExecutionGeneration {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <i64 as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Ok(Self::try_from(value)?)
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u64", into = "u64"))]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EcwCredentialEpoch(u64);

impl EcwCredentialEpoch {
    pub fn new(value: u64) -> Result<Self, EcwCredentialEpochParseError> {
        (value > 0 && value <= i64::MAX as u64)
            .then_some(Self(value))
            .ok_or(EcwCredentialEpochParseError)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    #[cfg(feature = "sqlx")]
    const fn as_database_value(self) -> i64 {
        self.0 as i64
    }
}

impl TryFrom<u64> for EcwCredentialEpoch {
    type Error = EcwCredentialEpochParseError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<i64> for EcwCredentialEpoch {
    type Error = EcwCredentialEpochParseError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        u64::try_from(value)
            .ok()
            .and_then(|value| Self::new(value).ok())
            .ok_or(EcwCredentialEpochParseError)
    }
}

impl From<EcwCredentialEpoch> for u64 {
    fn from(value: EcwCredentialEpoch) -> Self {
        value.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("ECW credential epoch must be in the positive i64 range")]
pub struct EcwCredentialEpochParseError;

/// Error returned when a sandbox route generation cannot fit the storage model.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("sandbox route generation must be in the positive i64 range")]
pub struct RouteGenerationParseError;

/// Monotonically increasing identity of the currently routable sandbox slot.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u64", into = "u64"))]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RouteGeneration(u64);

impl RouteGeneration {
    pub const INITIAL: Self = Self(1);

    pub fn new(value: u64) -> Result<Self, RouteGenerationParseError> {
        (value > 0 && value <= i64::MAX as u64)
            .then_some(Self(value))
            .ok_or(RouteGenerationParseError)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    #[cfg(feature = "sqlx")]
    const fn as_database_value(self) -> i64 {
        self.0 as i64
    }
}

impl fmt::Display for RouteGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for RouteGeneration {
    type Err = RouteGenerationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<u64>()
            .ok()
            .and_then(|value| Self::new(value).ok())
            .ok_or(RouteGenerationParseError)
    }
}

impl TryFrom<u64> for RouteGeneration {
    type Error = RouteGenerationParseError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for RouteGeneration {
    type Error = RouteGenerationParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl From<RouteGeneration> for u64 {
    fn from(value: RouteGeneration) -> Self {
        value.get()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for RouteGeneration {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <i64 as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for RouteGeneration {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i64 as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.as_database_value(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for RouteGeneration {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <i64 as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        let value = u64::try_from(value)?;
        Ok(Self::try_from(value)?)
    }
}

/// Route/execution identity that must match before SPACE API bridge traffic
/// reaches a sandbox runtime.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpaceApiRouteIdentity {
    pub sandbox_id: Option<SandboxId>,
    pub route_generation: Option<RouteGeneration>,
    pub current_node_id: Option<NodeId>,
    pub substrate_kind: Option<NodeManagedBy>,
    pub execution_generation: Option<ExecutionGeneration>,
    pub pod_uid: Option<PodUid>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
}

impl SpaceApiRouteIdentity {
    pub fn is_absent(&self) -> bool {
        self.sandbox_id.is_none()
            && self.route_generation.is_none()
            && self.current_node_id.is_none()
            && self.substrate_kind.is_none()
            && self.execution_generation.is_none()
            && self.pod_uid.is_none()
            && self.activation_attempt_id.is_none()
    }

    /// Resolve the substrate for migration-era identities that predate the
    /// explicit substrate field. A pod UID unambiguously identifies the
    /// Kubernetes substrate; identities without either field remain unknown.
    pub fn effective_substrate_kind(&self) -> Option<NodeManagedBy> {
        Self::effective_substrate_kind_for(self.substrate_kind, self.pod_uid.as_ref())
    }

    pub fn effective_substrate_kind_for(
        substrate_kind: Option<NodeManagedBy>,
        pod_uid: Option<&PodUid>,
    ) -> Option<NodeManagedBy> {
        substrate_kind.or_else(|| pod_uid.map(|_| NodeManagedBy::Kubernetes))
    }

    pub fn try_into_complete(
        self,
    ) -> Result<SandboxRouteIdentity, IncompleteSandboxRouteIdentityError> {
        SandboxRouteIdentity::try_new(
            self.sandbox_id
                .ok_or(IncompleteSandboxRouteIdentityError::MissingSandboxId)?,
            self.route_generation
                .ok_or(IncompleteSandboxRouteIdentityError::MissingRouteGeneration)?,
            self.current_node_id
                .ok_or(IncompleteSandboxRouteIdentityError::MissingCurrentNodeId)?,
            self.substrate_kind
                .ok_or(IncompleteSandboxRouteIdentityError::MissingSubstrateKind)?,
            self.execution_generation
                .ok_or(IncompleteSandboxRouteIdentityError::MissingExecutionGeneration)?,
            self.activation_attempt_id
                .ok_or(IncompleteSandboxRouteIdentityError::MissingActivationAttemptId)?,
            self.pod_uid,
        )
        .map_err(IncompleteSandboxRouteIdentityError::InvalidProfile)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IncompleteSandboxRouteIdentityError {
    #[error("sandbox route identity is missing sandbox_id")]
    MissingSandboxId,
    #[error("sandbox route identity is missing route_generation")]
    MissingRouteGeneration,
    #[error("sandbox route identity is missing current_node_id")]
    MissingCurrentNodeId,
    #[error("sandbox route identity is missing substrate_kind")]
    MissingSubstrateKind,
    #[error("sandbox route identity is missing execution_generation")]
    MissingExecutionGeneration,
    #[error("sandbox route identity is missing activation_attempt_id")]
    MissingActivationAttemptId,
    #[error("sandbox route identity has an invalid substrate profile: {0}")]
    InvalidProfile(#[source] SandboxRouteIdentityInvariantError),
}

/// Complete identity of one routable sandbox execution.
///
/// Unlike [`SpaceApiRouteIdentity`], this type is not a migration envelope:
/// every identity dimension needed for authorization is present, and the
/// substrate/Pod relationship is validated at construction time.
#[cfg_attr(feature = "serde", derive(Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxRouteIdentity {
    sandbox_id: SandboxId,
    route_generation: RouteGeneration,
    current_node_id: NodeId,
    substrate_kind: NodeManagedBy,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
    pod_uid: Option<PodUid>,
}

#[cfg(feature = "serde")]
#[derive(Deserialize)]
struct SandboxRouteIdentityWire {
    sandbox_id: SandboxId,
    route_generation: RouteGeneration,
    current_node_id: NodeId,
    substrate_kind: NodeManagedBy,
    execution_generation: ExecutionGeneration,
    activation_attempt_id: SandboxActivationAttemptId,
    pod_uid: Option<PodUid>,
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for SandboxRouteIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = SandboxRouteIdentityWire::deserialize(deserializer)?;
        Self::try_new(
            wire.sandbox_id,
            wire.route_generation,
            wire.current_node_id,
            wire.substrate_kind,
            wire.execution_generation,
            wire.activation_attempt_id,
            wire.pod_uid,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl SandboxRouteIdentity {
    pub fn try_new(
        sandbox_id: SandboxId,
        route_generation: RouteGeneration,
        current_node_id: NodeId,
        substrate_kind: NodeManagedBy,
        execution_generation: ExecutionGeneration,
        activation_attempt_id: SandboxActivationAttemptId,
        pod_uid: Option<PodUid>,
    ) -> Result<Self, SandboxRouteIdentityInvariantError> {
        match (substrate_kind, pod_uid.as_ref()) {
            (NodeManagedBy::Kubernetes, None) => {
                return Err(SandboxRouteIdentityInvariantError::MissingPodUid);
            }
            (NodeManagedBy::Space, Some(_)) => {
                return Err(SandboxRouteIdentityInvariantError::UnexpectedPodUid);
            }
            _ => {}
        }
        Ok(Self {
            sandbox_id,
            route_generation,
            current_node_id,
            substrate_kind,
            execution_generation,
            activation_attempt_id,
            pod_uid,
        })
    }

    pub const fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }

    pub const fn route_generation(&self) -> RouteGeneration {
        self.route_generation
    }

    pub const fn current_node_id(&self) -> NodeId {
        self.current_node_id
    }

    pub const fn substrate_kind(&self) -> NodeManagedBy {
        self.substrate_kind
    }

    pub const fn execution_generation(&self) -> ExecutionGeneration {
        self.execution_generation
    }

    pub const fn activation_attempt_id(&self) -> SandboxActivationAttemptId {
        self.activation_attempt_id
    }

    pub const fn pod_uid(&self) -> Option<&PodUid> {
        self.pod_uid.as_ref()
    }
}

impl From<SandboxRouteIdentity> for SpaceApiRouteIdentity {
    fn from(identity: SandboxRouteIdentity) -> Self {
        Self {
            sandbox_id: Some(identity.sandbox_id),
            route_generation: Some(identity.route_generation),
            current_node_id: Some(identity.current_node_id),
            substrate_kind: Some(identity.substrate_kind),
            execution_generation: Some(identity.execution_generation),
            pod_uid: identity.pod_uid,
            activation_attempt_id: Some(identity.activation_attempt_id),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SandboxRouteIdentityInvariantError {
    #[error("Kubernetes sandbox route identity requires pod_uid")]
    MissingPodUid,
    #[error("direct sandbox route identity must not contain pod_uid")]
    UnexpectedPodUid,
}

const _: () =
    assert!(MAX_LINUX_NETWORK_SLOT <= max_decimal_value_for_width(LINUX_NETWORK_SLOT_LABEL_WIDTH));

const fn max_usize(left: usize, right: usize) -> usize {
    if left > right { left } else { right }
}

const fn max_decimal_value_for_width(width: usize) -> u32 {
    if width >= 10 {
        return u32::MAX;
    }

    let mut value = 1u32;
    let mut index = 0;
    while index < width {
        value *= 10;
        index += 1;
    }
    value - 1
}

/// Error returned when a sandbox UUID cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::SandboxId;
/// assert!(SandboxId::from_str("not-a-uuid").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum SandboxIdParseError {
    #[error("invalid sandbox UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

/// Stable SPACE sandbox identity.
///
/// `SandboxId` is the ID that survives pause, resume, stop, restart, and node
/// movement until the sandbox is deleted.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxId;
/// let id = SandboxId::new();
/// assert_eq!(id.to_string().len(), 36);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxId(Uuid);

impl SandboxId {
    /// Creates a fresh v7 UUID-backed sandbox ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxId;
    /// let id = SandboxId::new();
    /// assert_eq!(id.as_uuid().get_version_num(), 7);
    /// ```
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an already parsed UUID as a sandbox ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxId;
    /// # use uuid::Uuid;
    /// let uuid = Uuid::nil();
    /// let id = SandboxId::from_uuid(uuid);
    /// assert_eq!(id.as_uuid(), uuid);
    /// ```
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the underlying UUID for storage or wire encoding.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxId;
    /// let id = SandboxId::new();
    /// let uuid = id.as_uuid();
    /// assert_eq!(uuid, id.as_uuid());
    /// ```
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }

    /// Returns the short display label derived from the UUID suffix.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::str::FromStr;
    /// # use pplx_space_common_core::types::SandboxId;
    /// let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2")?;
    /// assert_eq!(id.label().to_string(), "77f3a3a2");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn label(self) -> SandboxLabel {
        SandboxLabel::from_uuid(self.0)
    }
}

impl Default for SandboxId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SandboxId").field(&self.to_string()).finish()
    }
}

impl fmt::Display for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SandboxId {
    type Err = SandboxIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self).map_err(Into::into)
    }
}

impl TryFrom<String> for SandboxId {
    type Error = SandboxIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SandboxId> for String {
    fn from(value: SandboxId) -> Self {
        value.to_string()
    }
}

/// Identity of one durable node-local sandbox cleanup episode.
///
/// Cleanup work can outlive the request and process that created it. The
/// episode identity fences a stale worker from completing a newer lifecycle
/// attempt that happens to have the same rollback target. When a cleanup
/// recovers quarantine, this same identity is its at-most-once control-plane
/// receipt; completion does not mint a second identity for the same episode.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxCleanupEpisodeId(Uuid);

impl SandboxCleanupEpisodeId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for SandboxCleanupEpisodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SandboxCleanupEpisodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SandboxCleanupEpisodeId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

impl TryFrom<String> for SandboxCleanupEpisodeId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SandboxCleanupEpisodeId> for String {
    fn from(value: SandboxCleanupEpisodeId) -> Self {
        value.to_string()
    }
}

/// Identity of one durable auto-lifecycle delete completion receipt.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AutoLifecycleDeleteReceiptId(Uuid);

impl AutoLifecycleDeleteReceiptId {
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for AutoLifecycleDeleteReceiptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for AutoLifecycleDeleteReceiptId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

impl TryFrom<String> for AutoLifecycleDeleteReceiptId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<AutoLifecycleDeleteReceiptId> for String {
    fn from(value: AutoLifecycleDeleteReceiptId) -> Self {
        value.to_string()
    }
}

/// Identity of one control-plane activation dispatch.
///
/// Execution generations can be reused by retries. This identity changes for
/// every dispatch so delayed rollback evidence cannot retire a later attempt.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxActivationAttemptId(Uuid);

impl SandboxActivationAttemptId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Mint a dispatch identity strictly after a durable predecessor.
    ///
    /// `Uuid::now_v7` is monotonic only within one process. Activation
    /// assignment is serialized in Postgres across API replicas, so the
    /// allocator uses this fallback when another replica's durable value is
    /// greater than the local candidate. Completed owners can be forgotten,
    /// leaving only a resident route as the floor: retain fresh UUID entropy
    /// rather than deriving the same `previous + 1` on every allocation.
    pub fn new_after(previous: Self) -> Option<Self> {
        let candidate = Self::new();
        if candidate > previous {
            return Some(candidate);
        }
        // UUIDv7's leading 48 bits are milliseconds. Advancing that prefix
        // orders the new dispatch after the floor while preserving the fresh
        // candidate's version, variant, and uniqueness bits. Fail closed when
        // no larger timestamp exists; never wrap or fall back to a reused ID.
        let next_millis = (previous.0.as_u128() >> 80) + 1;
        if next_millis >= (1_u128 << 48) {
            return None;
        }
        let suffix = candidate.0.as_u128() & ((1_u128 << 80) - 1);
        Some(Self(Uuid::from_u128((next_millis << 80) | suffix)))
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for SandboxActivationAttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SandboxActivationAttemptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SandboxActivationAttemptId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

impl TryFrom<String> for SandboxActivationAttemptId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SandboxActivationAttemptId> for String {
    fn from(value: SandboxActivationAttemptId) -> Self {
        value.to_string()
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxTransitionReceiptKind {
    ActivationCompleted,
    ActivationRollback,
    QuarantineRecovery,
}

impl SandboxTransitionReceiptKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ActivationCompleted => "activation_completed",
            Self::ActivationRollback => "activation_rollback",
            Self::QuarantineRecovery => "quarantine_recovery",
        }
    }
}

impl fmt::Display for SandboxTransitionReceiptKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SandboxTransitionReceiptKind {
    type Err = SandboxTransitionReceiptKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "activation_completed" => Ok(Self::ActivationCompleted),
            "activation_rollback" => Ok(Self::ActivationRollback),
            "quarantine_recovery" => Ok(Self::QuarantineRecovery),
            _ => Err(SandboxTransitionReceiptKindParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown sandbox transition receipt kind")]
pub struct SandboxTransitionReceiptKindParseError;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxTransitionTargetState {
    Created,
    Started,
    Paused,
    Stopped,
    Suspended,
}

impl SandboxTransitionTargetState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Started => "started",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Suspended => "suspended",
        }
    }
}

impl fmt::Display for SandboxTransitionTargetState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SandboxTransitionTargetState {
    type Err = SandboxTransitionTargetStateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "created" => Ok(Self::Created),
            "started" => Ok(Self::Started),
            "paused" => Ok(Self::Paused),
            "stopped" => Ok(Self::Stopped),
            "suspended" => Ok(Self::Suspended),
            _ => Err(SandboxTransitionTargetStateParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown sandbox transition receipt target state")]
pub struct SandboxTransitionTargetStateParseError;

/// Atomic evidence that a node completed a fenced sandbox transition.
///
/// `R` is the reason representation chosen by the boundary: producers may
/// preserve a forward-compatible raw reason while consumers can require a
/// known [`SandboxQuarantineReasonCode`]. The receipt's structural invariants
/// are shared regardless of that boundary policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxTransitionReceipt<R> {
    id: SandboxCleanupEpisodeId,
    kind: SandboxTransitionReceiptKind,
    target_state: SandboxTransitionTargetState,
    execution_generation: ExecutionGeneration,
    reason_code: Option<R>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
}

impl<R> SandboxTransitionReceipt<R> {
    pub fn activation_completed(
        id: SandboxCleanupEpisodeId,
        target_state: SandboxTransitionTargetState,
        execution_generation: ExecutionGeneration,
        activation_attempt_id: SandboxActivationAttemptId,
    ) -> Self {
        Self {
            id,
            kind: SandboxTransitionReceiptKind::ActivationCompleted,
            target_state,
            execution_generation,
            reason_code: None,
            activation_attempt_id: Some(activation_attempt_id),
        }
    }

    pub fn quarantine_recovery(
        id: SandboxCleanupEpisodeId,
        execution_generation: ExecutionGeneration,
        reason_code: R,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
    ) -> Self {
        Self {
            id,
            kind: SandboxTransitionReceiptKind::QuarantineRecovery,
            target_state: SandboxTransitionTargetState::Suspended,
            execution_generation,
            reason_code: Some(reason_code),
            activation_attempt_id,
        }
    }

    pub fn try_new(
        id: SandboxCleanupEpisodeId,
        kind: SandboxTransitionReceiptKind,
        target_state: SandboxTransitionTargetState,
        execution_generation: ExecutionGeneration,
        reason_code: Option<R>,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
    ) -> Result<Self, SandboxTransitionReceiptInvariantError> {
        match kind {
            SandboxTransitionReceiptKind::ActivationCompleted
            | SandboxTransitionReceiptKind::ActivationRollback
                if activation_attempt_id.is_none() =>
            {
                return Err(SandboxTransitionReceiptInvariantError::MissingActivationAttempt);
            }
            SandboxTransitionReceiptKind::ActivationCompleted if reason_code.is_some() => {
                return Err(SandboxTransitionReceiptInvariantError::UnexpectedReasonCode);
            }
            SandboxTransitionReceiptKind::ActivationCompleted
                if !matches!(
                    target_state,
                    SandboxTransitionTargetState::Created
                        | SandboxTransitionTargetState::Started
                        | SandboxTransitionTargetState::Paused
                ) =>
            {
                return Err(
                    SandboxTransitionReceiptInvariantError::InvalidActivationCompletionTarget {
                        target_state,
                    },
                );
            }
            SandboxTransitionReceiptKind::ActivationRollback
                if !matches!(
                    target_state,
                    SandboxTransitionTargetState::Created
                        | SandboxTransitionTargetState::Stopped
                        | SandboxTransitionTargetState::Suspended
                ) =>
            {
                return Err(
                    SandboxTransitionReceiptInvariantError::InvalidActivationRollbackTarget {
                        target_state,
                    },
                );
            }
            SandboxTransitionReceiptKind::ActivationRollback if reason_code.is_none() => {
                return Err(SandboxTransitionReceiptInvariantError::MissingReasonCode);
            }
            SandboxTransitionReceiptKind::QuarantineRecovery
                if target_state != SandboxTransitionTargetState::Suspended =>
            {
                return Err(
                    SandboxTransitionReceiptInvariantError::InvalidQuarantineRecoveryTarget {
                        target_state,
                    },
                );
            }
            SandboxTransitionReceiptKind::QuarantineRecovery if reason_code.is_none() => {
                return Err(SandboxTransitionReceiptInvariantError::MissingReasonCode);
            }
            _ => {}
        }
        Ok(Self {
            id,
            kind,
            target_state,
            execution_generation,
            reason_code,
            activation_attempt_id,
        })
    }

    pub const fn id(&self) -> SandboxCleanupEpisodeId {
        self.id
    }

    pub const fn kind(&self) -> SandboxTransitionReceiptKind {
        self.kind
    }

    pub const fn target_state(&self) -> SandboxTransitionTargetState {
        self.target_state
    }

    pub const fn execution_generation(&self) -> ExecutionGeneration {
        self.execution_generation
    }

    pub const fn reason_code(&self) -> Option<&R> {
        self.reason_code.as_ref()
    }

    pub const fn activation_attempt_id(&self) -> Option<SandboxActivationAttemptId> {
        self.activation_attempt_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SandboxTransitionReceiptInvariantError {
    #[error("activation receipt requires an activation attempt")]
    MissingActivationAttempt,
    #[error("transition receipt requires a reason code")]
    MissingReasonCode,
    #[error("successful activation receipt must not carry a reason code")]
    UnexpectedReasonCode,
    #[error("activation completion receipt cannot target {target_state}")]
    InvalidActivationCompletionTarget {
        target_state: SandboxTransitionTargetState,
    },
    #[error("activation rollback receipt cannot target {target_state}")]
    InvalidActivationRollbackTarget {
        target_state: SandboxTransitionTargetState,
    },
    #[error("quarantine recovery receipt cannot target {target_state}")]
    InvalidQuarantineRecoveryTarget {
        target_state: SandboxTransitionTargetState,
    },
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SandboxCleanupEpisodeId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SandboxCleanupEpisodeId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <Uuid as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SandboxCleanupEpisodeId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(<Uuid as sqlx::Decode<'r, sqlx::Postgres>>::decode(
            value,
        )?))
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SandboxActivationAttemptId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SandboxActivationAttemptId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <Uuid as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SandboxActivationAttemptId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(<Uuid as sqlx::Decode<'r, sqlx::Postgres>>::decode(
            value,
        )?))
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for EcwCredentialEpoch {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <i64 as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for EcwCredentialEpoch {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i64 as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.as_database_value(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for EcwCredentialEpoch {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <i64 as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Ok(Self::try_from(value)?)
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SandboxId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SandboxId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <Uuid as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SandboxId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(<Uuid as sqlx::Decode<'r, sqlx::Postgres>>::decode(
            value,
        )?))
    }
}

/// Error returned when a human sandbox selector cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxSelector;
/// assert!(SandboxSelector::parse("abc").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum SandboxSelectorParseError {
    #[error("sandbox selector must not be empty")]
    Empty,
    #[error("sandbox selector must contain only ASCII hex digits and dashes")]
    InvalidCharacter,
    #[error("sandbox selector must contain at least 4 compact hex characters")]
    TooShort,
    #[error("sandbox selector must contain at most 32 compact hex characters")]
    TooLong,
}

/// Human-oriented sandbox lookup selector.
///
/// Short selectors match sandbox labels; longer selectors match compact UUID
/// prefixes. A selector is never a durable identity.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::{SandboxId, SandboxSelector};
/// let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2")?;
/// let selector = SandboxSelector::parse("77f3")?;
/// assert!(selector.matches_id(id));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxSelector(String);

/// Result of selecting from a candidate set where a selector can be ambiguous.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::OneOrMany;
/// let result = OneOrMany::Many(vec!["a", "b"]);
/// assert!(matches!(result, OneOrMany::Many(items) if items.len() == 2));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl SandboxSelector {
    /// Parses a selector by stripping dashes and lowercasing ASCII hex.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxSelector;
    /// let selector = SandboxSelector::parse("AB-cd")?;
    /// assert_eq!(selector.as_str(), "abcd");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn parse(value: impl AsRef<str>) -> Result<Self, SandboxSelectorParseError> {
        let compact = compact_hex(value.as_ref())?;
        match compact.len() {
            0 => Err(SandboxSelectorParseError::Empty),
            1..=3 => Err(SandboxSelectorParseError::TooShort),
            4..=32 => Ok(Self(compact)),
            _ => Err(SandboxSelectorParseError::TooLong),
        }
    }

    /// Returns the normalized compact selector.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxSelector;
    /// let selector = SandboxSelector::parse("AB-cd")?;
    /// assert_eq!(selector.as_str(), "abcd");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Checks whether this selector matches a sandbox ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::str::FromStr;
    /// # use pplx_space_common_core::types::{SandboxId, SandboxSelector};
    /// let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2")?;
    /// assert!(SandboxSelector::parse("77f3")?.matches_id(id));
    /// assert!(SandboxSelector::parse("019d84bd-280d")?.matches_id(id));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn matches_id(&self, id: SandboxId) -> bool {
        if self.0.len() <= SANDBOX_LABEL_HEX_LEN {
            id.label().to_string().starts_with(&self.0)
        } else {
            id.as_uuid().simple().to_string().starts_with(&self.0)
        }
    }

    /// Finds matching items from a candidate set without hiding ambiguity.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::str::FromStr;
    /// # use pplx_space_common_core::types::{OneOrMany, SandboxId, SandboxSelector};
    /// let ids = [
    ///     SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2")?,
    ///     SandboxId::from_str("019d84bd-280d-7331-99c6-b63300112233")?,
    /// ];
    /// let selector = SandboxSelector::parse("0011")?;
    /// assert!(matches!(selector.find(ids, |id| *id), Some(OneOrMany::One(_))));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn find<I, F>(&self, candidates: I, id_of: F) -> Option<OneOrMany<I::Item>>
    where
        I: IntoIterator,
        F: Fn(&I::Item) -> SandboxId,
    {
        let mut matches = candidates
            .into_iter()
            .filter(|candidate| self.matches_id(id_of(candidate)));
        let first = matches.next()?;
        let Some(second) = matches.next() else {
            return Some(OneOrMany::One(first));
        };

        let mut all = vec![first, second];
        all.extend(matches);
        Some(OneOrMany::Many(all))
    }
}

impl fmt::Debug for SandboxSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SandboxSelector").field(&self.0).finish()
    }
}

impl fmt::Display for SandboxSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SandboxSelector {
    type Err = SandboxSelectorParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SandboxSelector {
    type Error = SandboxSelectorParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SandboxSelector> for String {
    fn from(value: SandboxSelector) -> Self {
        value.0
    }
}

/// Collision-prone display label derived from the last four UUID bytes.
///
/// `SandboxLabel` is for operator display and short CLI selection only. It is
/// not unique enough for identity, storage, or Kubernetes labels.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::SandboxId;
/// let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2")?;
/// assert_eq!(id.label().to_string(), "77f3a3a2");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxLabel([u8; 4]);

impl SandboxLabel {
    /// Builds the display label from a UUID suffix.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxLabel;
    /// # use uuid::Uuid;
    /// let uuid = Uuid::parse_str("019d84bd-280d-7331-99c6-b63377f3a3a2")?;
    /// assert_eq!(SandboxLabel::from_uuid(uuid).to_string(), "77f3a3a2");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_uuid(value: Uuid) -> Self {
        let bytes = value.as_bytes();
        let suffix = bytes
            .last_chunk::<4>()
            .expect("UUID bytes always contain a four-byte suffix");
        Self(*suffix)
    }
}

impl fmt::Display for SandboxLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Runtime resource label used in node-local kernel object names.
///
/// This is not the durable sandbox identity. Fresh rows start with the short
/// UUID-derived sandbox label and running rows may switch to a node-manager
/// slot label such as `000017` after allocation.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxRuntimeLabel(String);

impl SandboxRuntimeLabel {
    /// Builds the initial runtime label from a sandbox ID.
    pub fn from_sandbox_id(id: SandboxId) -> Self {
        Self(id.label().to_string())
    }

    /// Parses a runtime label safe for SPACE-managed Linux object names.
    pub fn parse(value: impl Into<String>) -> Result<Self, SandboxTextParseError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SandboxTextParseError("sandbox runtime label is empty"));
        }
        if value.len() > SANDBOX_RUNTIME_LABEL_MAX_LEN {
            return Err(SandboxTextParseError("sandbox runtime label is too long"));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(SandboxTextParseError(
                "sandbox runtime label contains invalid characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated runtime label string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxRuntimeLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SandboxRuntimeLabel {
    type Err = SandboxTextParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SandboxRuntimeLabel {
    type Error = SandboxTextParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SandboxRuntimeLabel> for String {
    fn from(value: SandboxRuntimeLabel) -> Self {
        value.0
    }
}

impl AsRef<str> for SandboxRuntimeLabel {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Error returned by sandbox text value parsers.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxName;
/// assert!(SandboxName::parse("").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct SandboxTextParseError(&'static str);

/// User-authored sandbox display name.
///
/// The value is PII-valued and therefore has redacted `Debug` and no raw
/// `Display`.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxName;
/// let name = SandboxName::parse("analysis-run")?;
/// assert_eq!(name.expose(), "analysis-run");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxName(String);

impl SandboxName {
    /// Parses a non-empty, bounded sandbox name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxName;
    /// assert!(SandboxName::parse("customer run").is_ok());
    /// assert!(SandboxName::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, SandboxTextParseError> {
        let value = value.into();
        non_empty_bounded(&value, 256, "sandbox name is too long")?;
        Ok(Self(value))
    }

    /// Explicitly exposes the raw PII-valued name to an approved boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxName;
    /// let name = SandboxName::parse("customer run")?;
    /// assert_eq!(name.expose(), "customer run");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SandboxName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxName(<redacted>)")
    }
}

impl FromStr for SandboxName {
    type Err = SandboxTextParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SandboxName {
    type Error = SandboxTextParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SandboxName> for String {
    fn from(value: SandboxName) -> Self {
        value.0
    }
}

/// User-authored metadata key attached to a sandbox.
///
/// Metadata keys are PII-valued because customers may encode tenant names or
/// emails in key names.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxMetadataKey;
/// let key = SandboxMetadataKey::parse("ticket")?;
/// assert_eq!(key.expose(), "ticket");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxMetadataKey(String);

impl SandboxMetadataKey {
    /// Parses a non-empty, bounded metadata key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxMetadataKey;
    /// assert!(SandboxMetadataKey::parse("customer").is_ok());
    /// assert!(SandboxMetadataKey::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, SandboxTextParseError> {
        let value = value.into();
        non_empty_bounded(&value, 128, "sandbox metadata key is too long")?;
        Ok(Self(value))
    }

    /// Exposes the raw key for approved storage or owner-facing boundaries.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxMetadataKey;
    /// let key = SandboxMetadataKey::parse("ticket")?;
    /// assert_eq!(key.expose(), "ticket");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SandboxMetadataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxMetadataKey(<redacted>)")
    }
}

impl FromStr for SandboxMetadataKey {
    type Err = SandboxTextParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SandboxMetadataKey {
    type Error = SandboxTextParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SandboxMetadataKey> for String {
    fn from(value: SandboxMetadataKey) -> Self {
        value.0
    }
}

/// Bounded string used as a sandbox metadata value.
///
/// This is a separate type so public enum construction and serde cannot bypass
/// the string bounds enforced by [`SandboxMetadataValue::string`].
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxMetadataString;
/// let value = SandboxMetadataString::parse("abc")?;
/// assert_eq!(value.expose(), "abc");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxMetadataString(String);

impl SandboxMetadataString {
    /// Parses a bounded, non-empty metadata string value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxMetadataString;
    /// assert!(SandboxMetadataString::parse("abc").is_ok());
    /// assert!(SandboxMetadataString::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, SandboxTextParseError> {
        let value = value.into();
        non_empty_bounded(&value, 4096, "sandbox metadata value is too long")?;
        Ok(Self(value))
    }

    /// Exposes the validated metadata string at an approved boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxMetadataString;
    /// let value = SandboxMetadataString::parse("abc")?;
    /// assert_eq!(value.expose(), "abc");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SandboxMetadataString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxMetadataString(<redacted>)")
    }
}

impl FromStr for SandboxMetadataString {
    type Err = SandboxTextParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SandboxMetadataString {
    type Error = SandboxTextParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SandboxMetadataString> for String {
    fn from(value: SandboxMetadataString) -> Self {
        value.0
    }
}

/// Scalar metadata value attached to a sandbox.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxMetadataValue;
/// let value = SandboxMetadataValue::string("abc")?;
/// let SandboxMetadataValue::String(value) = value else { unreachable!() };
/// assert_eq!(value.expose(), "abc");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SandboxMetadataValue {
    String(SandboxMetadataString),
    Integer(i64),
    Bool(bool),
}

impl SandboxMetadataValue {
    /// Builds a bounded string metadata value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxMetadataValue;
    /// assert!(SandboxMetadataValue::string("abc").is_ok());
    /// assert!(SandboxMetadataValue::string("").is_err());
    /// ```
    pub fn string(value: impl Into<String>) -> Result<Self, SandboxTextParseError> {
        SandboxMetadataString::parse(value).map(Self::String)
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::String(value) => value.expose().len(),
            Self::Integer(value) => value.to_string().len(),
            Self::Bool(value) => {
                if *value {
                    "true".len()
                } else {
                    "false".len()
                }
            }
        }
    }
}

impl fmt::Debug for SandboxMetadataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxMetadataValue(<redacted>)")
    }
}

/// Bounded customer metadata map attached to a sandbox.
///
/// The map is PII-valued as a whole because keys and values are customer
/// authored.
///
/// # Examples
///
/// ```rust
/// # use std::collections::BTreeMap;
/// # use pplx_space_common_core::types::{SandboxMetadata, SandboxMetadataKey, SandboxMetadataValue};
/// let mut values = BTreeMap::new();
/// values.insert(SandboxMetadataKey::parse("ticket")?, SandboxMetadataValue::string("S-123")?);
/// let metadata = SandboxMetadata::new(values)?;
/// assert_eq!(metadata.expose().len(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Eq, PartialEq)]
pub struct SandboxMetadata(BTreeMap<SandboxMetadataKey, SandboxMetadataValue>);

impl SandboxMetadata {
    /// Maximum number of metadata entries accepted by [`SandboxMetadata::new`].
    pub const MAX_ENTRIES: usize = 128;
    /// Maximum total encoded key/value bytes accepted by [`SandboxMetadata::new`].
    pub const MAX_TOTAL_BYTES: usize = MAX_SANDBOX_METADATA_TOTAL_BYTES;

    /// Builds a bounded sandbox metadata map.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use pplx_space_common_core::types::{SandboxMetadata, SandboxMetadataKey, SandboxMetadataValue};
    /// let mut values = BTreeMap::new();
    /// values.insert(SandboxMetadataKey::parse("ticket")?, SandboxMetadataValue::Integer(7));
    /// assert!(SandboxMetadata::new(values).is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(
        values: BTreeMap<SandboxMetadataKey, SandboxMetadataValue>,
    ) -> Result<Self, SandboxTextParseError> {
        if values.len() > Self::MAX_ENTRIES {
            return Err(SandboxTextParseError(
                "sandbox metadata has too many entries",
            ));
        }
        if metadata_total_bytes(&values) > Self::MAX_TOTAL_BYTES {
            return Err(SandboxTextParseError("sandbox metadata is too large"));
        }
        Ok(Self(values))
    }

    /// Exposes the validated metadata map for approved boundaries.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use pplx_space_common_core::types::SandboxMetadata;
    /// let metadata = SandboxMetadata::new(BTreeMap::new())?;
    /// assert!(metadata.expose().is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn expose(&self) -> &BTreeMap<SandboxMetadataKey, SandboxMetadataValue> {
        &self.0
    }
}

impl fmt::Debug for SandboxMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SandboxMetadata(<redacted>)")
    }
}

impl TryFrom<BTreeMap<SandboxMetadataKey, SandboxMetadataValue>> for SandboxMetadata {
    type Error = SandboxTextParseError;

    fn try_from(
        value: BTreeMap<SandboxMetadataKey, SandboxMetadataValue>,
    ) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SandboxMetadata> for BTreeMap<SandboxMetadataKey, SandboxMetadataValue> {
    fn from(value: SandboxMetadata) -> Self {
        value.0
    }
}

#[cfg(feature = "serde")]
impl Serialize for SandboxMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for SandboxMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        BTreeMap::<SandboxMetadataKey, SandboxMetadataValue>::deserialize(deserializer)
            .and_then(|values| Self::new(values).map_err(serde::de::Error::custom))
    }
}

/// Canonical sandbox lifecycle state shared across services.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::SandboxLifecycle;
/// assert!(SandboxLifecycle::Starting.is_transient());
/// assert!(SandboxLifecycle::Started.is_active_runtime());
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SandboxLifecycle {
    Created,
    Creating,
    Starting,
    Started,
    Restoring,
    Pausing,
    Paused,
    Resuming,
    Suspending,
    Suspended,
    Stopping,
    Stopped,
    Deleting,
    Checkpointing,
    Quarantined,
    Failed,
}

impl SandboxLifecycle {
    /// All variants in declaration order, for exhaustive iteration in tests.
    ///
    /// **This list cannot enforce its own completeness, and nothing here does.**
    /// Rust has no way to enumerate an enum's variants without a derive macro,
    /// and this workspace has no `strum`, so a test can force an author to *look*
    /// at this list but not to update it.
    ///
    /// What exists: two exhaustive `match`es in other files fail to compile when a
    /// variant is added, each naming it — `tag` in this module's `tests.rs`, and
    /// (more usefully) `counted_by` in the cluster-controller's
    /// `node_drain::store::termination_gate_tests`, which forces the author to
    /// classify the new lifecycle against *both* node-release gates rather than
    /// merely acknowledge it. Those are prompts, not guarantees: adding the match
    /// arms and forgetting this list still compiles and still passes, and anything
    /// driven off `all()` — notably the cluster-controller's drain-gate lifecycle
    /// matrix — then silently stops covering the new value.
    ///
    /// So: **when you add a variant, add it here.** Mirrors
    /// [`crate::types::SnapshotState::all`], which has the same shape and the
    /// same limitation.
    ///
    /// Not every value `sandboxes.lifecycle` can hold: `deleted` is a tombstone
    /// written by raw SQL and `cleanup_required` is a node-local
    /// sandbox-manager state, neither of which is a variant here. A test that
    /// needs those has to name them itself.
    pub const fn all() -> &'static [Self] {
        &[
            Self::Created,
            Self::Creating,
            Self::Starting,
            Self::Started,
            Self::Restoring,
            Self::Pausing,
            Self::Paused,
            Self::Resuming,
            Self::Suspending,
            Self::Suspended,
            Self::Stopping,
            Self::Stopped,
            Self::Deleting,
            Self::Checkpointing,
            Self::Quarantined,
            Self::Failed,
        ]
    }

    /// Returns true for in-progress lifecycle states.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxLifecycle;
    /// assert!(SandboxLifecycle::Creating.is_transient());
    /// assert!(!SandboxLifecycle::Started.is_transient());
    /// ```
    pub const fn is_transient(self) -> bool {
        matches!(
            self,
            Self::Creating
                | Self::Starting
                | Self::Restoring
                | Self::Pausing
                | Self::Resuming
                | Self::Suspending
                | Self::Stopping
                | Self::Deleting
                | Self::Checkpointing
        )
    }

    /// Returns true when a runtime should still exist for the sandbox.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxLifecycle;
    /// assert!(SandboxLifecycle::Started.is_active_runtime());
    /// assert!(SandboxLifecycle::Paused.is_active_runtime());
    /// assert!(!SandboxLifecycle::Stopped.is_active_runtime());
    /// ```
    pub const fn is_active_runtime(self) -> bool {
        matches!(self, Self::Started | Self::Paused)
    }

    /// Returns true for states that can be recovered after node crash.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SandboxLifecycle;
    /// assert!(SandboxLifecycle::Suspended.is_crash_recovery_target());
    /// assert!(!SandboxLifecycle::Failed.is_crash_recovery_target());
    /// ```
    pub const fn is_crash_recovery_target(self) -> bool {
        matches!(self, Self::Started | Self::Paused | Self::Suspended)
    }
}

impl SandboxLifecycle {
    /// The stable string form written to `sandboxes.lifecycle` in
    /// Postgres and accepted back by [`FromStr`]. `as_str` and
    /// `from_str` round-trip losslessly.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Creating => "creating",
            Self::Starting => "starting",
            Self::Started => "started",
            Self::Restoring => "restoring",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Resuming => "resuming",
            Self::Suspending => "suspending",
            Self::Suspended => "suspended",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Deleting => "deleting",
            Self::Checkpointing => "checkpointing",
            Self::Quarantined => "quarantined",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for SandboxLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned by [`SandboxLifecycle`]'s `FromStr` when the input
/// doesn't match any known lifecycle. Carries the offending value so
/// callers can log it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown sandbox lifecycle '{value}'")]
pub struct SandboxLifecycleParseError {
    pub value: String,
}

impl FromStr for SandboxLifecycle {
    type Err = SandboxLifecycleParseError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "created" => Ok(Self::Created),
            "creating" => Ok(Self::Creating),
            "starting" => Ok(Self::Starting),
            "started" => Ok(Self::Started),
            "restoring" => Ok(Self::Restoring),
            "pausing" => Ok(Self::Pausing),
            "paused" => Ok(Self::Paused),
            "resuming" => Ok(Self::Resuming),
            "suspending" => Ok(Self::Suspending),
            "suspended" => Ok(Self::Suspended),
            "stopping" => Ok(Self::Stopping),
            "stopped" => Ok(Self::Stopped),
            "deleting" => Ok(Self::Deleting),
            "checkpointing" => Ok(Self::Checkpointing),
            "quarantined" => Ok(Self::Quarantined),
            "failed" => Ok(Self::Failed),
            other => Err(SandboxLifecycleParseError {
                value: other.to_string(),
            }),
        }
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SandboxLifecycle {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SandboxLifecycle {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SandboxLifecycle {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryNoProgressReason {
    PlanNotFound,
    PlanIneligible,
    PlanRpcUnsupported,
    PlanMalformed,
    UnsupportedTargetState,
    PlanReasonIneligible,
    PlanLookupFailed,
    PlanLookupTimedOut,
    LocalReasonMismatch,
    LocalArtifactMismatch,
    /// The central plan names a different local artifact than the node's episode
    /// does. Distinct from `LocalArtifactMismatch`, which is the node's own
    /// registry disagreeing with what is on its disk: this one is the control
    /// plane and the node disagreeing about which artifact is current, and it
    /// points an operator at the plan rather than at the cache.
    PlanArtifactMismatch,
    /// The operation gave up because it could not renew its claim, having spent
    /// the renewal budget. That is a node-local database failure, not a transfer
    /// that needs retrying, and it points an operator at this node's SQLite
    /// rather than at the snapshot's storage.
    ClaimRenewalFailed,
    /// The node's own database refused a read or write the restore needed. Points
    /// an operator at this node's SQLite, not at the snapshot or the plan.
    NodeDatabaseFailure,
    /// The node's own filesystem refused an operation the restore needed. Points at
    /// this node's disk.
    NodeFilesystemFailure,
    /// The restore's own evidence disagreed with itself — snapshot identity, restore
    /// source, artifact URIs or recovered row metadata. Points at the artifact.
    RestoreEvidenceMismatch,
    /// The control plane declined to serve the restore: unconfigured, unregistered,
    /// admission-rejected, or a plan RPC status. Points away from this node.
    ControlPlaneRefused,
    CleanupFenceBusy,
    LocalArtifactInspectionFailed,
    LocalAdoptionFenceChanged,
    LocalAdoptionFailed,
    /// This sandbox's restore staging lock was already owned, so the claimed episode
    /// is backed off without letting one busy sandbox monopolize the node-wide
    /// orphan-restore permit.
    ///
    /// Not restore-specific, despite the name: cleanup takes the same per-sandbox
    /// staging lock, so a concurrent delete of this sandbox surfaces here too. The
    /// orphan branch never acquires the cleanup fence, so `cleanup_fence_busy` —
    /// which only the adoption branch produces — is not the reason a deleting
    /// sandbox reports. An operator seeing this should check for a delete as well as
    /// a restore.
    OrphanRestoreLocalContention,
    /// The detached restore task panicked or was cancelled before it could
    /// classify and persist its outcome. Metric-only: the live claim expires and
    /// the episode is then eligible for a fresh attempt.
    OrphanRestoreTaskFailed,
    OrphanRestoreDeferred,
    /// The orphan restore hit the operation deadline that bounds its two
    /// sequential volume-manager transfer budgets.
    OrphanRestoreTimedOut,
    /// The claim stopped being the current one mid-operation, so no mutation
    /// could commit under it.
    ClaimSuperseded,
    /// The acquire CAS matched nothing: some conjunct it pins changed
    /// between the scan and the claim.
    ClaimPreconditionsChanged,
    /// The claim write itself failed against SQLite.
    ClaimAcquisitionFailed,
    /// The process was shutting down, so the operation abandoned itself rather
    /// than run to its multi-hour deadline through a SIGTERM.
    ///
    /// Durable retry state *is* written for this, and that is the point: the write
    /// releases the lease and schedules the retry at `now + backoff` rather than
    /// the claim's `expiry + backoff`, so the next process can pick the episode up
    /// without waiting out the remainder of a 15-minute lease. How soon depends on
    /// the attempt count — 48–60s at attempt 1, up to 720–900s once the backoff
    /// caps — and the episode is picked up on the next 60s reconciler tick after
    /// that, so an attempt-1 resume is observed at 48–120s. "Sooner", not "next
    /// tick". The attempt itself was already charged
    /// when the claim was taken, so nothing here adds one.
    ///
    /// The write is fenced like any other, so it is not guaranteed: if a cleanup
    /// owner arrived or the lease already lapsed it reports a stale claim and
    /// leaves the lease alone, and the episode then does wait the lease out.
    AbandonedAtShutdown,
}

impl QuarantineRecoveryNoProgressReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlanNotFound => "plan_not_found",
            Self::PlanIneligible => "plan_ineligible",
            Self::PlanRpcUnsupported => "plan_rpc_unsupported",
            Self::PlanMalformed => "plan_malformed",
            Self::UnsupportedTargetState => "unsupported_target_state",
            Self::PlanReasonIneligible => "plan_reason_ineligible",
            Self::PlanLookupFailed => "plan_lookup_failed",
            Self::PlanLookupTimedOut => "plan_lookup_timed_out",
            Self::LocalReasonMismatch => "local_reason_mismatch",
            Self::LocalArtifactMismatch => "local_artifact_mismatch",
            Self::PlanArtifactMismatch => "plan_artifact_mismatch",
            Self::ClaimRenewalFailed => "claim_renewal_failed",
            Self::NodeDatabaseFailure => "node_database_failure",
            Self::NodeFilesystemFailure => "node_filesystem_failure",
            Self::RestoreEvidenceMismatch => "restore_evidence_mismatch",
            Self::ControlPlaneRefused => "control_plane_refused",
            Self::CleanupFenceBusy => "cleanup_fence_busy",
            Self::LocalArtifactInspectionFailed => "local_artifact_inspection_failed",
            Self::LocalAdoptionFenceChanged => "local_adoption_fence_changed",
            Self::LocalAdoptionFailed => "local_adoption_failed",
            Self::OrphanRestoreLocalContention => "orphan_restore_local_contention",
            Self::OrphanRestoreTaskFailed => "orphan_restore_task_failed",
            Self::OrphanRestoreDeferred => "orphan_restore_deferred",
            Self::OrphanRestoreTimedOut => "orphan_restore_timed_out",
            Self::ClaimSuperseded => "claim_superseded",
            Self::ClaimPreconditionsChanged => "claim_preconditions_changed",
            Self::ClaimAcquisitionFailed => "claim_acquisition_failed",
            Self::AbandonedAtShutdown => "abandoned_at_shutdown",
        }
    }
}

impl fmt::Display for QuarantineRecoveryNoProgressReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxQuarantineSource {
    SbxManager,
    NodeStatePublish,
    ClusterController,
}

impl SandboxQuarantineSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SbxManager => "sbx_manager",
            Self::NodeStatePublish => "node_state_publish",
            Self::ClusterController => "cluster_controller",
        }
    }
}

impl fmt::Display for SandboxQuarantineSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SandboxQuarantineSource {
    type Err = SandboxQuarantineVocabularyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sbx_manager" => Ok(Self::SbxManager),
            "node_state_publish" => Ok(Self::NodeStatePublish),
            "cluster_controller" => Ok(Self::ClusterController),
            other => Err(SandboxQuarantineVocabularyParseError {
                kind: "quarantine source",
                value: other.to_string(),
            }),
        }
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SandboxQuarantineSource {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SandboxQuarantineSource {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SandboxQuarantineSource {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxQuarantineReasonCode {
    Unspecified,
    RuntimeProbeInconsistent,
    SuspendSnapshotPublicationFailed,
    OrphanedSandboxRowMissing,
    CreateInterrupted,
    ColdActivationInterrupted,
    RestoreActivationInterrupted,
    ColdActivationPrepareFailed,
    ColdActivationMetadataFailed,
    ColdActivationEncryptedRootfsPrepareFailed,
    ColdActivationFirecrackerLaunchFailed,
    ColdActivationSidecarLaunchFailed,
    RestorePrepareFailed,
    RestoreMetadataFailed,
    RestoreEncryptedDevicesPrepareFailed,
    RestoreFirecrackerLaunchFailed,
    RestoreNetGatewayLaunchFailed,
    RestoreSnapshotLoadFailed,
    RestoreSpacedHostLaunchFailed,
    RestoreResumeFailed,
    PauseFailed,
    CheckpointResumeFailed,
    ResidentResumeFailed,
    SnapshotRollbackCleanupFailed,
    NodePublishedQuarantined,
    OrphanedExecutionMissingArtifact,
    OrphanedExecutionLegacyDeletingState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxQuarantineRecoveryPolicy {
    ExactSuspendArtifact,
    ExactOrPreQuarantineSuspendArtifact,
}

/// Bounded reason why the legacy quarantine-recovery plan endpoint rejected a
/// request. These values are shared by policy evaluation and telemetry so the
/// metric cannot collapse distinctions the server already knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineRecoveryPlanRejectionCause {
    NoActiveQuarantine,
    NoActiveNode,
    WrongNode,
    NoActiveExecution,
    UnsupportedReason,
    NotAutomaticallyRecoverable,
    NoExactSuspendArtifact,
    NoCompatibleSnapshot,
}

impl QuarantineRecoveryPlanRejectionCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoActiveQuarantine => "no_active_quarantine",
            Self::NoActiveNode => "no_active_node",
            Self::WrongNode => "wrong_node",
            Self::NoActiveExecution => "no_active_execution",
            Self::UnsupportedReason => "unsupported_reason",
            Self::NotAutomaticallyRecoverable => "not_automatically_recoverable",
            Self::NoExactSuspendArtifact => "no_exact_suspend_artifact",
            Self::NoCompatibleSnapshot => "no_compatible_snapshot",
        }
    }
}

impl fmt::Display for QuarantineRecoveryPlanRejectionCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl SandboxQuarantineRecoveryPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactSuspendArtifact => "exact_suspend_artifact",
            Self::ExactOrPreQuarantineSuspendArtifact => "exact_or_pre_quarantine_suspend_artifact",
        }
    }
}

impl fmt::Display for SandboxQuarantineRecoveryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SandboxQuarantineReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::RuntimeProbeInconsistent => "runtime_probe_inconsistent",
            Self::SuspendSnapshotPublicationFailed => "suspend_snapshot_publication_failed",
            Self::OrphanedSandboxRowMissing => "orphaned_sandbox_row_missing",
            Self::CreateInterrupted => "create_interrupted",
            Self::ColdActivationInterrupted => "cold_activation_interrupted",
            Self::RestoreActivationInterrupted => "restore_activation_interrupted",
            Self::ColdActivationPrepareFailed => "cold_activation_prepare_failed",
            Self::ColdActivationMetadataFailed => "cold_activation_metadata_failed",
            Self::ColdActivationEncryptedRootfsPrepareFailed => {
                "cold_activation_encrypted_rootfs_prepare_failed"
            }
            Self::ColdActivationFirecrackerLaunchFailed => {
                "cold_activation_firecracker_launch_failed"
            }
            Self::ColdActivationSidecarLaunchFailed => "cold_activation_sidecar_launch_failed",
            Self::RestorePrepareFailed => "restore_prepare_failed",
            Self::RestoreMetadataFailed => "restore_metadata_failed",
            Self::RestoreEncryptedDevicesPrepareFailed => {
                "restore_encrypted_devices_prepare_failed"
            }
            Self::RestoreFirecrackerLaunchFailed => "restore_firecracker_launch_failed",
            Self::RestoreNetGatewayLaunchFailed => "restore_net_gateway_launch_failed",
            Self::RestoreSnapshotLoadFailed => "restore_snapshot_load_failed",
            Self::RestoreSpacedHostLaunchFailed => "restore_spaced_host_launch_failed",
            Self::RestoreResumeFailed => "restore_resume_failed",
            Self::PauseFailed => "pause_failed",
            Self::CheckpointResumeFailed => "checkpoint_resume_failed",
            Self::ResidentResumeFailed => "resident_resume_failed",
            Self::SnapshotRollbackCleanupFailed => "snapshot_rollback_cleanup_failed",
            Self::NodePublishedQuarantined => "node_published_quarantined",
            Self::OrphanedExecutionMissingArtifact => "orphaned_execution_missing_artifact",
            Self::OrphanedExecutionLegacyDeletingState => {
                "orphaned_execution_legacy_deleting_state"
            }
        }
    }

    /// Automatic recovery is opt-in and exhaustive. Adding a new quarantine
    /// reason therefore requires an explicit safety decision here, without
    /// changing persistence, wire shapes, or observability configuration.
    pub const fn automatic_recovery_policy(self) -> Option<SandboxQuarantineRecoveryPolicy> {
        match self {
            Self::RestorePrepareFailed
            | Self::RestoreFirecrackerLaunchFailed
            | Self::RestoreSpacedHostLaunchFailed => {
                Some(SandboxQuarantineRecoveryPolicy::ExactSuspendArtifact)
            }
            Self::OrphanedSandboxRowMissing => {
                Some(SandboxQuarantineRecoveryPolicy::ExactOrPreQuarantineSuspendArtifact)
            }
            Self::Unspecified
            | Self::RuntimeProbeInconsistent
            | Self::SuspendSnapshotPublicationFailed
            | Self::CreateInterrupted
            | Self::ColdActivationInterrupted
            | Self::RestoreActivationInterrupted
            | Self::ColdActivationPrepareFailed
            | Self::ColdActivationMetadataFailed
            | Self::ColdActivationEncryptedRootfsPrepareFailed
            | Self::ColdActivationFirecrackerLaunchFailed
            | Self::ColdActivationSidecarLaunchFailed
            | Self::RestoreMetadataFailed
            | Self::RestoreEncryptedDevicesPrepareFailed
            | Self::RestoreNetGatewayLaunchFailed
            | Self::RestoreSnapshotLoadFailed
            | Self::RestoreResumeFailed
            | Self::PauseFailed
            | Self::CheckpointResumeFailed
            | Self::ResidentResumeFailed
            | Self::SnapshotRollbackCleanupFailed
            | Self::NodePublishedQuarantined
            | Self::OrphanedExecutionMissingArtifact
            | Self::OrphanedExecutionLegacyDeletingState => None,
        }
    }
}

impl fmt::Display for SandboxQuarantineReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SandboxQuarantineReasonCode {
    type Err = SandboxQuarantineVocabularyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unspecified" => Ok(Self::Unspecified),
            "runtime_probe_inconsistent" => Ok(Self::RuntimeProbeInconsistent),
            "suspend_snapshot_publication_failed" => Ok(Self::SuspendSnapshotPublicationFailed),
            "orphaned_sandbox_row_missing" => Ok(Self::OrphanedSandboxRowMissing),
            "create_interrupted" => Ok(Self::CreateInterrupted),
            "cold_activation_interrupted" => Ok(Self::ColdActivationInterrupted),
            "restore_activation_interrupted" => Ok(Self::RestoreActivationInterrupted),
            "cold_activation_prepare_failed" => Ok(Self::ColdActivationPrepareFailed),
            "cold_activation_metadata_failed" => Ok(Self::ColdActivationMetadataFailed),
            "cold_activation_encrypted_rootfs_prepare_failed" => {
                Ok(Self::ColdActivationEncryptedRootfsPrepareFailed)
            }
            "cold_activation_firecracker_launch_failed" => {
                Ok(Self::ColdActivationFirecrackerLaunchFailed)
            }
            "cold_activation_sidecar_launch_failed" => Ok(Self::ColdActivationSidecarLaunchFailed),
            "restore_prepare_failed" => Ok(Self::RestorePrepareFailed),
            "restore_metadata_failed" => Ok(Self::RestoreMetadataFailed),
            "restore_encrypted_devices_prepare_failed" => {
                Ok(Self::RestoreEncryptedDevicesPrepareFailed)
            }
            "restore_firecracker_launch_failed" => Ok(Self::RestoreFirecrackerLaunchFailed),
            "restore_net_gateway_launch_failed" => Ok(Self::RestoreNetGatewayLaunchFailed),
            "restore_snapshot_load_failed" => Ok(Self::RestoreSnapshotLoadFailed),
            "restore_spaced_host_launch_failed" => Ok(Self::RestoreSpacedHostLaunchFailed),
            "restore_resume_failed" => Ok(Self::RestoreResumeFailed),
            "pause_failed" => Ok(Self::PauseFailed),
            "checkpoint_resume_failed" => Ok(Self::CheckpointResumeFailed),
            "resident_resume_failed" => Ok(Self::ResidentResumeFailed),
            "snapshot_rollback_cleanup_failed" => Ok(Self::SnapshotRollbackCleanupFailed),
            "node_published_quarantined" => Ok(Self::NodePublishedQuarantined),
            "orphaned_execution_missing_artifact" => Ok(Self::OrphanedExecutionMissingArtifact),
            "orphaned_execution_legacy_deleting_state" => {
                Ok(Self::OrphanedExecutionLegacyDeletingState)
            }
            other => Err(SandboxQuarantineVocabularyParseError {
                kind: "quarantine reason code",
                value: other.to_string(),
            }),
        }
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SandboxQuarantineReasonCode {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SandboxQuarantineReasonCode {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SandboxQuarantineReasonCode {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::postgres::PgHasArrayType for SandboxQuarantineReasonCode {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxQuarantineNextAction {
    RequiresOperatorAttention,
    SweeperCleanup,
}

impl SandboxQuarantineNextAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequiresOperatorAttention => "requires_operator_attention",
            Self::SweeperCleanup => "sweeper_cleanup",
        }
    }
}

impl fmt::Display for SandboxQuarantineNextAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SandboxQuarantineNextAction {
    type Err = SandboxQuarantineVocabularyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "requires_operator_attention" => Ok(Self::RequiresOperatorAttention),
            "sweeper_cleanup" => Ok(Self::SweeperCleanup),
            other => Err(SandboxQuarantineVocabularyParseError {
                kind: "quarantine next action",
                value: other.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxQuarantineHistoryAction {
    EnteredQuarantine,
    ReconfirmedQuarantine,
    RecoveredToStarted,
    RecoveredToPaused,
    RecoveredToSuspended,
}

impl SandboxQuarantineHistoryAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EnteredQuarantine => "entered_quarantine",
            Self::ReconfirmedQuarantine => "reconfirmed_quarantine",
            Self::RecoveredToStarted => "recovered_to_started",
            Self::RecoveredToPaused => "recovered_to_paused",
            Self::RecoveredToSuspended => "recovered_to_suspended",
        }
    }
}

impl fmt::Display for SandboxQuarantineHistoryAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SandboxQuarantineHistoryAction {
    type Err = SandboxQuarantineVocabularyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "entered_quarantine" => Ok(Self::EnteredQuarantine),
            "reconfirmed_quarantine" => Ok(Self::ReconfirmedQuarantine),
            "recovered_to_started" => Ok(Self::RecoveredToStarted),
            "recovered_to_paused" => Ok(Self::RecoveredToPaused),
            "recovered_to_suspended" => Ok(Self::RecoveredToSuspended),
            other => Err(SandboxQuarantineVocabularyParseError {
                kind: "quarantine history action",
                value: other.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown {kind} '{value}'")]
pub struct SandboxQuarantineVocabularyParseError {
    pub kind: &'static str,
    pub value: String,
}

fn compact_hex(value: &str) -> Result<String, SandboxSelectorParseError> {
    let mut compact = String::new();
    for ch in value.chars() {
        if ch == '-' {
            continue;
        }
        if !ch.is_ascii_hexdigit() {
            return Err(SandboxSelectorParseError::InvalidCharacter);
        }
        compact.push(ch.to_ascii_lowercase());
    }
    Ok(compact)
}

fn metadata_total_bytes(values: &BTreeMap<SandboxMetadataKey, SandboxMetadataValue>) -> usize {
    values
        .iter()
        .map(|(key, value)| key.expose().len() + value.encoded_len())
        .sum()
}

fn non_empty_bounded(
    value: &str,
    max_len: usize,
    too_long: &'static str,
) -> Result<(), SandboxTextParseError> {
    if value.is_empty() {
        return Err(SandboxTextParseError("value must not be empty"));
    }
    if value.len() > max_len {
        return Err(SandboxTextParseError(too_long));
    }
    if value.contains('\0') {
        return Err(SandboxTextParseError("value must not contain NUL"));
    }
    Ok(())
}

/// Identity of one authorized pod materialization, reused across create retries.
/// A new same-generation pod receives a new identity after its predecessor is gone.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PodMaterializationId(Uuid);

impl PodMaterializationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for PodMaterializationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PodMaterializationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for PodMaterializationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

impl TryFrom<String> for PodMaterializationId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<PodMaterializationId> for String {
    fn from(value: PodMaterializationId) -> Self {
        value.to_string()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for PodMaterializationId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PodMaterializationId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <Uuid as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PodMaterializationId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(<Uuid as sqlx::Decode<'r, sqlx::Postgres>>::decode(
            value,
        )?))
    }
}

#[cfg(test)]
mod tests;

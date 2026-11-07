//! Snapshot identifiers, names, kinds, and resource shape.
//!
//! Snapshots can be addressed by stable UUIDs for persisted records and by
//! path-safe names for node-local artifacts. `SnapshotConfig` records the
//! compute shape and optional disk shape needed to create or resume a snapshot.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! let id = SnapshotId::new();
//! let name = SnapshotName::parse("base")?;
//!
//! assert_eq!(SnapshotId::from_uuid(id.as_uuid()), id);
//! assert_eq!(name.as_str(), "base");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use uuid::Uuid;

pub const SNAPSHOT_RETENTION_SWEEPER_DEFAULT_MIN_AGE_SECONDS: u64 = 3600;
pub const SNAPSHOT_STORAGE_SWEEPER_DEFAULT_MIN_AGE_SECONDS: u64 = 3600;
pub const SANDBOX_FORK_SNAPSHOT_RETENTION_DEFAULT_GRACE_SECONDS: u64 = 3600;
pub const SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_ENV: &str =
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS";

/// Parse an optional snapshot grace from its seconds value (test hook input).
///
/// An absent or invalid value preserves the configured default, while zero
/// explicitly disables the grace.
pub fn snapshot_sweeper_grace_seconds(
    value: Option<&str>,
    default: Option<Duration>,
) -> Option<Duration> {
    match value.map(str::trim) {
        Some(raw) => match raw.parse::<u64>() {
            Ok(0) => None,
            Ok(seconds) => Some(Duration::from_secs(seconds)),
            Err(_) => default,
        },
        None => default,
    }
}

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::template::TemplateArtifactDigest;
use pplx_space_byte_size::{DiskSize, MemorySize};
use pplx_space_execution_spec::compute::{ComputeShape, VcpuCount};

const MAX_SNAPSHOT_NAME_BYTES: usize = 255;

pub const DEFAULT_BASE_SNAPSHOT_VCPU: u32 = 2;
pub const DEFAULT_BASE_SNAPSHOT_MEMORY_MIB: u32 = 1024;
pub const MIN_BASE_SNAPSHOT_VCPU: u32 = 1;
pub const MIN_BASE_SNAPSHOT_MEMORY_MIB: u32 = 512;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BaseSnapshotShape {
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
}

impl BaseSnapshotShape {
    pub const fn new(vcpu: VcpuCount, memory: MemorySize) -> Self {
        Self { vcpu, memory }
    }

    pub fn from_optional_i32(vcpu: Option<i32>, memory_mib: Option<i32>) -> Self {
        Self {
            vcpu: VcpuCount::try_from_u32(base_snapshot_vcpu_or_default(vcpu))
                .expect("base snapshot CPU policy supplies a positive count"),
            memory: MemorySize::mib(u64::from(base_snapshot_memory_mib_or_default(memory_mib)))
                .expect("base snapshot memory policy supplies positive u32 MiB"),
        }
    }
}

pub fn i32_at_least_or_default(value: Option<i32>, minimum: u32, fallback: u32) -> u32 {
    value
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value >= minimum)
        .unwrap_or(fallback)
}

pub fn base_snapshot_vcpu_or_default(value: Option<i32>) -> u32 {
    i32_at_least_or_default(value, MIN_BASE_SNAPSHOT_VCPU, DEFAULT_BASE_SNAPSHOT_VCPU)
}

pub fn base_snapshot_memory_mib_or_default(value: Option<i32>) -> u32 {
    i32_at_least_or_default(
        value,
        MIN_BASE_SNAPSHOT_MEMORY_MIB,
        DEFAULT_BASE_SNAPSHOT_MEMORY_MIB,
    )
}

/// Error returned when a snapshot name fails validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// assert!(SnapshotName::parse("base").is_ok());
/// assert!(SnapshotName::parse("../base").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct SnapshotParseError(&'static str);

/// Error returned when a snapshot UUID cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::*;
/// assert!(SnapshotId::from_str("not-a-uuid").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum SnapshotIdParseError {
    #[error("invalid snapshot UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

/// UUID identity for a persisted snapshot record.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let id = SnapshotId::new();
/// assert_eq!(SnapshotId::from_uuid(id.as_uuid()), id);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SnapshotId(Uuid);

impl SnapshotId {
    /// Creates a new time-ordered snapshot UUID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let id = SnapshotId::new();
    /// assert_eq!(SnapshotId::from_uuid(id.as_uuid()), id);
    /// ```
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing UUID as a snapshot ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let fresh = SnapshotId::new();
    /// let id = SnapshotId::from_uuid(fresh.as_uuid());
    /// assert_eq!(id, fresh);
    /// ```
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the underlying UUID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let id = SnapshotId::new();
    /// assert_eq!(SnapshotId::from_uuid(id.as_uuid()), id);
    /// ```
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for SnapshotId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SnapshotId")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SnapshotId {
    type Err = SnapshotIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self).map_err(Into::into)
    }
}

impl TryFrom<String> for SnapshotId {
    type Error = SnapshotIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SnapshotId> for String {
    fn from(value: SnapshotId) -> Self {
        value.to_string()
    }
}

/// UUID identity for a node-local artifact published by a lifecycle operation.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LocalArtifactId(Uuid);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum LocalArtifactIdParseError {
    #[error("invalid local artifact UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

impl LocalArtifactId {
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

impl Default for LocalArtifactId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for LocalArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LocalArtifactId")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for LocalArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for LocalArtifactId {
    type Err = LocalArtifactIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self).map_err(Into::into)
    }
}

impl TryFrom<String> for LocalArtifactId {
    type Error = LocalArtifactIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl TryFrom<&str> for LocalArtifactId {
    type Error = LocalArtifactIdParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl From<LocalArtifactId> for String {
    fn from(value: LocalArtifactId) -> Self {
        value.to_string()
    }
}

/// UUID identity shared by all transfer jobs belonging to one restore attempt.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TransferScopeId(Uuid);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid transfer scope UUID: {0}")]
pub struct TransferScopeIdParseError(#[from] uuid::Error);

impl TransferScopeId {
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

impl Default for TransferScopeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TransferScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TransferScopeId {
    type Err = TransferScopeIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self).map_err(Into::into)
    }
}

impl TryFrom<String> for TransferScopeId {
    type Error = TransferScopeIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TransferScopeId> for String {
    fn from(value: TransferScopeId) -> Self {
        value.to_string()
    }
}

/// UUID identity for one attempt to copy an artifact to durable storage.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BackupAttemptId(Uuid);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum BackupAttemptIdParseError {
    #[error("invalid backup attempt UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

impl BackupAttemptId {
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

impl Default for BackupAttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for BackupAttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BackupAttemptId")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for BackupAttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for BackupAttemptId {
    type Err = BackupAttemptIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self).map_err(Into::into)
    }
}

impl TryFrom<String> for BackupAttemptId {
    type Error = BackupAttemptIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl TryFrom<&str> for BackupAttemptId {
    type Error = BackupAttemptIdParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl From<BackupAttemptId> for String {
    fn from(value: BackupAttemptId) -> Self {
        value.to_string()
    }
}

/// Path-safe node-local snapshot name.
///
/// Snapshot names are suitable as one path component; they contain only ASCII
/// alphanumeric characters plus `.`, `_`, and `-`, and are no longer than 255
/// bytes.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let name = SnapshotName::parse("before-upgrade")?;
/// assert_eq!(name.as_str(), "before-upgrade");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SnapshotName(String);

impl SnapshotName {
    /// Parses a path-safe snapshot name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// assert!(SnapshotName::parse("base").is_ok());
    /// assert!(SnapshotName::parse("..").is_err());
    /// assert!(SnapshotName::parse("nested/base").is_err());
    /// assert!(SnapshotName::parse("a".repeat(256)).is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, SnapshotParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_SNAPSHOT_NAME_BYTES
            || value == "."
            || value == ".."
            || !value.bytes().all(is_snapshot_name_byte)
        {
            return Err(SnapshotParseError(
                "snapshot name must contain only ASCII alphanumeric characters, '.', '_', or '-' and be <= 255 bytes",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the snapshot name as a string slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let name = SnapshotName::parse("base")?;
    /// assert_eq!(name.as_str(), "base");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_snapshot_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

impl fmt::Display for SnapshotName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SnapshotName {
    type Err = SnapshotParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SnapshotName {
    type Error = SnapshotParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for SnapshotName {
    type Error = SnapshotParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SnapshotName> for String {
    fn from(value: SnapshotName) -> Self {
        value.0
    }
}

/// Template content identity captured on a snapshot.
///
/// New template releases use a canonical [`TemplateArtifactDigest`], but the
/// snapshot schema also contains legacy sandbox hashes (including bare
/// SHA-256 values and Git commit hashes). This type preserves the stored value
/// while exposing a typed artifact digest when one is available.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SnapshotContentHash {
    stored: String,
    artifact_digest: Option<TemplateArtifactDigest>,
}

impl SnapshotContentHash {
    /// Preserve a hash read from an existing sandbox or snapshot row.
    pub fn from_stored(value: impl Into<String>) -> Self {
        let stored = value.into();
        let artifact_digest = stored
            .parse::<TemplateArtifactDigest>()
            .ok()
            .or_else(|| format!("sha256:{stored}").parse().ok());
        Self {
            stored,
            artifact_digest,
        }
    }

    /// Return the exact value represented by the database column.
    pub fn as_stored(&self) -> &str {
        &self.stored
    }

    /// Return the canonical artifact digest when this is not a legacy hash.
    pub const fn artifact_digest(&self) -> Option<TemplateArtifactDigest> {
        self.artifact_digest
    }
}

impl From<TemplateArtifactDigest> for SnapshotContentHash {
    fn from(value: TemplateArtifactDigest) -> Self {
        Self {
            stored: value.to_string(),
            artifact_digest: Some(value),
        }
    }
}

impl From<String> for SnapshotContentHash {
    fn from(value: String) -> Self {
        Self::from_stored(value)
    }
}

impl fmt::Display for SnapshotContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_stored())
    }
}

/// Snapshot capture mode.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let kind = SnapshotKind::Disk;
/// assert_eq!(kind, SnapshotKind::Disk);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SnapshotKind {
    Disk,
    Full,
}

impl SnapshotKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disk => "disk",
            Self::Full => "full",
        }
    }

    pub const fn all() -> &'static [Self] {
        &[Self::Disk, Self::Full]
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown snapshot kind: {0:?}; expected disk or full")]
pub struct SnapshotKindParseError(String);

impl fmt::Display for SnapshotKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SnapshotKind {
    type Err = SnapshotKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disk" => Ok(Self::Disk),
            "full" => Ok(Self::Full),
            other => Err(SnapshotKindParseError(other.to_owned())),
        }
    }
}

impl TryFrom<String> for SnapshotKind {
    type Error = SnapshotKindParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SnapshotKind> for String {
    fn from(value: SnapshotKind) -> Self {
        value.as_str().to_owned()
    }
}

/// Closed set of published local artifact shapes.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactKind {
    Disk,
    Full,
}

impl ArtifactKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disk => "disk",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown artifact kind: {0:?}; expected disk or full")]
pub struct ArtifactKindParseError(String);

impl fmt::Display for ArtifactKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ArtifactKind {
    type Err = ArtifactKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disk" => Ok(Self::Disk),
            "full" => Ok(Self::Full),
            other => Err(ArtifactKindParseError(other.to_owned())),
        }
    }
}

impl TryFrom<String> for ArtifactKind {
    type Error = ArtifactKindParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<ArtifactKind> for String {
    fn from(value: ArtifactKind) -> Self {
        value.as_str().to_owned()
    }
}

/// Operation that published an immutable local artifact.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactOrigin {
    Stop,
    Suspend,
    Snapshot,
    Fork,
}

impl ArtifactOrigin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Suspend => "suspend",
            Self::Snapshot => "snapshot",
            Self::Fork => "fork",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown artifact origin: {0:?}; expected stop, suspend, snapshot, or fork")]
pub struct ArtifactOriginParseError(String);

impl fmt::Display for ArtifactOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ArtifactOrigin {
    type Err = ArtifactOriginParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "stop" => Ok(Self::Stop),
            "suspend" => Ok(Self::Suspend),
            "snapshot" => Ok(Self::Snapshot),
            "fork" => Ok(Self::Fork),
            other => Err(ArtifactOriginParseError(other.to_owned())),
        }
    }
}

impl TryFrom<String> for ArtifactOrigin {
    type Error = ArtifactOriginParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<ArtifactOrigin> for String {
    fn from(value: ArtifactOrigin) -> Self {
        value.as_str().to_owned()
    }
}

/// Selection of the local artifact an internal backup request should pin.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackupSource {
    CurrentInstance,
    Snapshot(SnapshotName),
    LocalArtifact(LocalArtifactId),
}

/// Lifecycle state of a persisted snapshot row.
///
/// A snapshot row is inserted in [`Creating`](Self::Creating) when the suspend
/// upload begins. The upload pipeline transitions it exactly once:
/// [`Ready`](Self::Ready) when all artifacts have been uploaded to object
/// storage, or [`Failed`](Self::Failed) when the upload pipeline gives up.
/// Retention machinery may later claim a `Ready` row as
/// [`Deleting`](Self::Deleting) — the durable de-advertisement that must
/// commit *before* the snapshot's bytes are deleted (bounded-state RFC
/// invariant 1). A `Deleting` row never returns to `Ready`; its catalog row
/// remains only while the irreversible byte cleanup is tracked, and is
/// removed when that finishes. Restore-side readers filter for `Ready` rows
/// only — non-terminal, failed, and deleting rows are invisible to restore by
/// construction, so a partial upload is never observable as a restorable
/// snapshot and a claimed row stops being advertised at claim time.
///
/// Wire form is the lowercase variant name; this is the canonical text stored
/// in `snapshots.state` and in the corresponding proto field.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// # use std::str::FromStr;
/// assert_eq!(SnapshotState::Ready.as_str(), "ready");
/// assert_eq!(SnapshotState::from_str("creating").unwrap(), SnapshotState::Creating);
/// assert!(SnapshotState::Failed.is_terminal());
/// assert!(!SnapshotState::Creating.is_terminal());
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SnapshotState {
    Creating,
    Ready,
    Failed,
    Deleting,
}

impl SnapshotState {
    /// Canonical wire/DB string for the variant. Stable: this is the value
    /// written to `snapshots.state` and read back at the wire boundary.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
        }
    }

    /// True for every state except [`Creating`](Self::Creating). Once a
    /// row leaves `Creating`, the suspend upload pipeline never touches it
    /// again ([`Deleting`](Self::Deleting) is reachable only from `Ready`,
    /// which is already terminal for the upload pipeline).
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Ready | Self::Failed | Self::Deleting)
    }

    /// True only for [`Ready`](Self::Ready). Restore-side queries should use
    /// this predicate (or equivalent SQL) to filter rows visible for restore.
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// True only for [`Failed`](Self::Failed). Useful for retention loops
    /// that prune failed attempts on a shorter cadence than ready rows.
    pub const fn is_failed(self) -> bool {
        matches!(self, Self::Failed)
    }

    /// True only for [`Deleting`](Self::Deleting): the row is claimed for
    /// deletion and de-advertised, but its byte cleanup has not finished.
    pub const fn is_deleting(self) -> bool {
        matches!(self, Self::Deleting)
    }

    /// All variants in declaration order. Useful for exhaustive iteration
    /// in tests so a new variant is forced to update the test as well as
    /// the production match arms.
    pub const fn all() -> &'static [Self] {
        &[Self::Creating, Self::Ready, Self::Failed, Self::Deleting]
    }
}

/// Why a snapshot state string failed to parse.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown snapshot state: {0:?}; expected one of creating/ready/failed/deleting")]
pub struct SnapshotStateParseError(String);

impl SnapshotStateParseError {
    /// The offending input.
    pub fn input(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SnapshotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SnapshotState {
    type Err = SnapshotStateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "creating" => Ok(Self::Creating),
            "ready" => Ok(Self::Ready),
            "failed" => Ok(Self::Failed),
            "deleting" => Ok(Self::Deleting),
            other => Err(SnapshotStateParseError(other.to_owned())),
        }
    }
}

impl TryFrom<String> for SnapshotState {
    type Error = SnapshotStateParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SnapshotState> for String {
    fn from(value: SnapshotState) -> Self {
        value.as_str().to_owned()
    }
}

/// Resource shape associated with a snapshot.
///
/// Disk is optional because some snapshot forms only need compute shape or get
/// disk information from provider-managed state.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// # use pplx_space_execution_spec::{ComputeShape, CpuLimit};
/// # use pplx_space_byte_size::{DiskSize, MemorySize};
/// let compute = ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?);
/// let disk = DiskSize::gib(16)?;
/// let config = SnapshotConfig::new(compute, Some(disk));
///
/// assert_eq!(config.compute(), compute);
/// assert_eq!(config.disk(), Some(disk));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotConfig {
    compute: ComputeShape,
    disk: Option<DiskSize>,
}

impl SnapshotConfig {
    /// Builds a snapshot config from compute shape and optional disk size.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// # use pplx_space_execution_spec::{ComputeShape, CpuLimit};
    /// # use pplx_space_byte_size::MemorySize;
    /// let compute = ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?);
    /// let config = SnapshotConfig::new(compute, None);
    /// assert_eq!(config.compute(), compute);
    /// assert_eq!(config.disk(), None);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn new(compute: ComputeShape, disk: Option<DiskSize>) -> Self {
        Self { compute, disk }
    }

    /// Returns the compute shape captured by the snapshot config.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// # use pplx_space_execution_spec::{ComputeShape, CpuLimit};
    /// # use pplx_space_byte_size::MemorySize;
    /// let compute = ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::gib(1)?);
    /// let config = SnapshotConfig::new(compute, None);
    /// assert_eq!(config.compute(), compute);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn compute(&self) -> ComputeShape {
        self.compute
    }

    /// Returns the optional disk size captured by the snapshot config.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// # use pplx_space_execution_spec::{ComputeShape, CpuLimit};
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let compute = ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::gib(1)?);
    /// let disk = DiskSize::gib(8)?;
    /// let config = SnapshotConfig::new(compute, Some(disk));
    /// assert_eq!(config.disk(), Some(disk));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn disk(&self) -> Option<DiskSize> {
        self.disk
    }
}

#[cfg(test)]
mod tests;

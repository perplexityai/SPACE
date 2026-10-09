//! Key Encryption Key (KEK) provider vocabulary.
//!
//! Per-organization KEKs may be delegated to the external intermediate-key
//! broker, with an in-process stub available for development. These types also
//! recognize legacy provider names so persisted rows fail closed in a typed
//! way instead of becoming unknown strings.
//!
//! The wire format is a string (matches the `org_master_keys.provider`
//! TEXT column, which is intentionally free-text so adding a new
//! backend doesn't require a schema migration). Boundary code parses
//! the string into [`KekProvider`] once and passes the typed value
//! through the rest of the call graph; unknown values fail closed.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! let provider = KekProvider::parse("intermediate-key-broker").unwrap();
//! assert_eq!(provider, KekProvider::IntermediateKeyBroker);
//! assert_eq!(provider.as_str(), "external-branching");
//!
//! let key = MasterKeyIdentifier::parse("broker-key-version-42").unwrap();
//! assert_eq!(key.as_str(), "broker-key-version-42");
//! ```

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Canonical identifier for the legacy AWS KMS backend.
pub const KEK_PROVIDER_AWS_KMS: &str = "aws-kms";
/// Canonical identifier for the in-process stub backend.
///
/// Not for production — wrapped DEKs do not survive a credmgr restart.
pub const KEK_PROVIDER_IN_MEMORY: &str = "in-memory";
/// Configuration identifier for the intermediate-key broker backend.
///
/// The KMS root lives in an external service; the credmgr authority
/// caches the unwrapped per-org intermediate key in memory for a bounded
/// TTL and wraps DEKs under it locally.
pub const KEK_PROVIDER_INTERMEDIATE_KEY_BROKER: &str = "intermediate-key-broker";

// Persisted before the provider-neutral broker terminology was adopted. For
// one rollout release, serialization keeps this spelling so a rollback to the
// previous authority can still read keys provisioned by the new image. Parse
// accepts both spellings so configuration can use the new terminology now.
const LEGACY_INTERMEDIATE_KEY_BROKER_PROVIDER: &str = "external-branching";

/// Which backend wraps and unwraps a given org's DEKs.
///
/// The persisted form is returned by [`Self::as_str`]; callers parse it into
/// this enum at the boundary. During a rollout, configuration may use a newer
/// spelling while persistence remains rollback-compatible. The set is
/// intentionally small: a new backend means a new variant, which makes
/// "is this provider supported?" a compile-time question for every
/// backend-aware site.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// assert_eq!(KekProvider::IntermediateKeyBroker.as_str(), "external-branching");
/// assert_eq!(KekProvider::parse("in-memory").unwrap(), KekProvider::InMemory);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum KekProvider {
    /// Legacy AWS Key Management Service provider. The authority recognizes
    /// persisted rows but does not install this backend.
    AwsKms,
    /// In-process stub backend. KEKs are random keys held in process
    /// memory; intended for tests and local development.
    InMemory,
    /// External intermediate-key broker. An external KMS-holding service
    /// vends per-org intermediate KEKs ("intermediate keys") that the
    /// credmgr authority caches unwrapped, in memory, for a bounded TTL.
    IntermediateKeyBroker,
}

/// Error returned when a provider identifier cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::KekProvider;
/// assert!(KekProvider::from_str("unknown").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported KEK provider '{0}'")]
pub struct KekProviderParseError(String);

impl KekProvider {
    /// Returns the persisted provider identifier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// assert_eq!(KekProvider::IntermediateKeyBroker.as_str(), "external-branching");
    /// ```
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AwsKms => KEK_PROVIDER_AWS_KMS,
            Self::InMemory => KEK_PROVIDER_IN_MEMORY,
            Self::IntermediateKeyBroker => LEGACY_INTERMEDIATE_KEY_BROKER_PROVIDER,
        }
    }

    /// Parses a provider identifier from its canonical wire spelling.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// assert_eq!(
    ///     KekProvider::parse("intermediate-key-broker").unwrap(),
    ///     KekProvider::IntermediateKeyBroker
    /// );
    /// ```
    pub fn parse(value: &str) -> Result<Self, KekProviderParseError> {
        match value {
            KEK_PROVIDER_AWS_KMS => Ok(Self::AwsKms),
            KEK_PROVIDER_IN_MEMORY => Ok(Self::InMemory),
            KEK_PROVIDER_INTERMEDIATE_KEY_BROKER | LEGACY_INTERMEDIATE_KEY_BROKER_PROVIDER => {
                Ok(Self::IntermediateKeyBroker)
            }
            other => Err(KekProviderParseError(other.to_string())),
        }
    }

    /// Whether a keyless org on this platform default gets its KEK provisioned
    /// lazily on first use (first credential Encrypt / sandbox volume DEK) by
    /// the credmgr authority, rather than eagerly at enroll time. In-memory is
    /// included for local development so API services never provision keys.
    pub const fn provisions_lazily(self) -> bool {
        matches!(self, Self::InMemory | Self::IntermediateKeyBroker)
    }
}

/// Environment variable naming the platform-default KEK provider for BYOK orgs
/// that have no key of their own.
pub const PLATFORM_KEK_PROVIDER_ENV: &str = "SPACE_PLATFORM_KEK_PROVIDER";

/// The platform-default KEK provider from [`PLATFORM_KEK_PROVIDER_ENV`], or
/// `None` when unset. This is the single source of truth so the API's
/// BYOK-enable gate and the credmgr authority's provisioner install cannot
/// disagree on whether a keyless org is supported.
///
/// A value that is *set but unrecognized* (e.g. an `intermediate_key_broker` typo)
/// also returns `None`, but logs a `warn` first: the schema deliberately keeps
/// `org_master_keys.provider` free-text so the credmgr fails closed on unknown
/// providers, so an unrecognized platform default must be loud rather than
/// silently selecting a backend with no signal.
pub fn platform_default_kek_provider() -> Option<KekProvider> {
    let value = std::env::var(PLATFORM_KEK_PROVIDER_ENV).ok()?;
    match KekProvider::parse(&value) {
        Ok(provider) => Some(provider),
        Err(_) => {
            tracing::warn!(
                env = PLATFORM_KEK_PROVIDER_ENV,
                value = %value,
                "unrecognized platform KEK provider; ignoring it (nothing selected)"
            );
            None
        }
    }
}

/// Whether the platform default provisions a keyless org's KEK lazily (see
/// [`KekProvider::provisions_lazily`]). Unset or unrecognized => `false`
/// (no lazy platform default).
pub fn platform_provisions_kek_lazily() -> bool {
    platform_default_kek_provider().is_some_and(KekProvider::provisions_lazily)
}

impl fmt::Display for KekProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for KekProvider {
    type Err = KekProviderParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for KekProvider {
    type Error = KekProviderParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<KekProvider> for String {
    fn from(value: KekProvider) -> Self {
        value.to_string()
    }
}

/// Error returned when a master-key identifier is empty or malformed.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("master key identifier must be non-empty, at most 2048 bytes, and contain no NUL byte")]
pub struct MasterKeyIdentifierParseError;

/// Backend-specific reference to a single KEK.
///
/// Opaque to the credmgr surface: for AWS KMS this is a key ID, key
/// ARN, alias name, or alias ARN (anything KMS' `KeyId` parameter
/// accepts); for the in-memory backend it is an arbitrary label that
/// keys a per-org map. Stored verbatim in `org_master_keys.key_identifier`.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let id = MasterKeyIdentifier::parse("alias/example-org-default").unwrap();
/// assert_eq!(id.as_str(), "alias/example-org-default");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MasterKeyIdentifier(String);

impl MasterKeyIdentifier {
    /// Parses a master-key identifier.
    ///
    /// The accepted value is non-empty, at most 2048 bytes (covers AWS
    /// KMS key ARNs and any reasonable alias path), and must not
    /// contain a NUL byte. Backends apply their own additional
    /// validation when they resolve the identifier.
    pub fn parse(value: impl Into<String>) -> Result<Self, MasterKeyIdentifierParseError> {
        let value = value.into();
        if value.is_empty() || value.len() > 2048 || value.contains('\0') {
            return Err(MasterKeyIdentifierParseError);
        }
        Ok(Self(value))
    }

    /// Returns the identifier as a borrowed string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MasterKeyIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for MasterKeyIdentifier {
    type Err = MasterKeyIdentifierParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for MasterKeyIdentifier {
    type Error = MasterKeyIdentifierParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<MasterKeyIdentifier> for String {
    fn from(value: MasterKeyIdentifier) -> Self {
        value.0
    }
}

/// Domain prefix bound into the credmgr wrap/unwrap AAD for a per-sandbox
/// **node-local volume** DEK (the BYOK encrypted rootfs COW and the encrypted
/// snapshot-capture volume). Distinct from the snapshot-artifact AEAD prefix
/// (`space-artifact-wrap/v1`) so a volume DEK can never be unwrapped as an
/// artifact key or vice versa — credmgr's AAD check fails across domains.
const VOLUME_WRAP_AAD_PREFIX: &[u8] = b"space-rootfs-vol/v1";

/// Domain prefix bound into the credmgr wrap/unwrap AAD for a sandbox
/// snapshot-artifact DEK (vol-mgr's envelope-encrypted `PushFilesAsync` /
/// `PullFilesAsync` artifacts).
const ARTIFACT_WRAP_AAD_PREFIX: &[u8] = b"space-artifact-wrap/v1";

/// AAD bound into a sandbox snapshot-artifact DEK. Layout:
/// `ARTIFACT_WRAP_AAD_PREFIX || 0x00 || org_id(16) || sandbox_id(16)`.
///
/// credmgr derives it from the sandbox's durable owner when it issues or
/// unwraps an artifact DEK, so a caller can never choose the AAD; vol-mgr
/// derives the same bytes as the prefix of each entry's per-chunk AEAD AAD.
/// `artifact_id` is intentionally absent: each artifact has its own DEK, so
/// per-DEK isolation already prevents cross-artifact swaps.
pub fn artifact_wrap_aad(org_id: super::OrgId, sandbox_id: &super::SandboxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(ARTIFACT_WRAP_AAD_PREFIX.len() + 1 + 16 + 16);
    out.extend_from_slice(ARTIFACT_WRAP_AAD_PREFIX);
    out.push(0);
    out.extend_from_slice(org_id.as_uuid().as_bytes());
    out.extend_from_slice(sandbox_id.as_uuid().as_bytes());
    out
}

/// AAD bound into a sandbox's node-local volume DEK at credmgr
/// `GetOrIssueSandboxVolumeDek` time. Layout:
/// `VOLUME_WRAP_AAD_PREFIX || 0x00 || org_id(16) || sandbox_id(16)`, the NUL
/// separator preventing prefix collisions even though both ids are
/// fixed-length UUIDs. Deterministic from `(org_id, sandbox_id)` — the issuer
/// (vol-mgr, today) and the consumer (node-mgr, which opens the dm-crypt
/// volumes) derive the identical AAD without persisting it. This is the single
/// source of truth for that AAD across services; do not re-derive it locally.
pub fn volume_wrap_aad(org_id: super::OrgId, sandbox_id: &super::SandboxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(VOLUME_WRAP_AAD_PREFIX.len() + 1 + 16 + 16);
    out.extend_from_slice(VOLUME_WRAP_AAD_PREFIX);
    out.push(0);
    out.extend_from_slice(org_id.as_uuid().as_bytes());
    out.extend_from_slice(sandbox_id.as_uuid().as_bytes());
    out
}

#[cfg(test)]
mod tests;

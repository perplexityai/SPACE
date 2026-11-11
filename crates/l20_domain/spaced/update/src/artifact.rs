use std::{fmt, str::FromStr};

use aws_lc_rs::digest::{SHA256, digest};
use pplx_space_content_digest::{Digest, HashAlg};
use pplx_space_cpu_arch::Arch;
use serde::{Deserialize, Serialize};

use crate::{MAX_BINARY_BYTES, MAX_METADATA_BYTES};

/// SHA-256 identity, distinct from the ordered workspace release version.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Digest", into = "Digest")]
pub struct ArtifactDigest(Digest);

impl ArtifactDigest {
    pub fn of_bytes(bytes: &[u8]) -> Self {
        let mut value = [0; 32];
        value.copy_from_slice(digest(&SHA256, bytes).as_ref());
        Self(Digest::sha256(value))
    }
}

impl TryFrom<Digest> for ArtifactDigest {
    type Error = DescriptorError;

    fn try_from(value: Digest) -> Result<Self, Self::Error> {
        if value.algorithm() != HashAlg::Sha256 {
            return Err(DescriptorError::Invalid("digest algorithm"));
        }
        Ok(Self(value))
    }
}

impl From<ArtifactDigest> for Digest {
    fn from(value: ArtifactDigest) -> Self {
        value.0
    }
}

impl fmt::Display for ArtifactDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ArtifactDigest {
    type Err = DescriptorError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<Digest>()
            .map_err(DescriptorError::Digest)?
            .try_into()
    }
}

/// The complete payload allowlist. Wire input never becomes a filesystem path.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Binary {
    Service,
    Cli,
}

impl Binary {
    pub const ALL: [Self; 2] = [Self::Service, Self::Cli];
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::Service => "spaced-svc",
            Self::Cli => "spaced",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinaryIdentity {
    pub length: u64,
    pub digest: ArtifactDigest,
}

/// Schema 1 canonical encoding is compact serde JSON in this declared field
/// order, no whitespace, UTF-8, with a sorted unique seed-contract list. There
/// are no maps, floats, promotion fields or self-referential digest fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleDescriptor {
    pub schema: u32,
    pub source_revision: String,
    pub built_at: String,
    pub version: semver::Version,
    pub guest_update_required_since: semver::Version,
    pub compatibility_family: String,
    pub architecture: Arch,
    pub seed_contracts: Vec<String>,
    pub service: BinaryIdentity,
    pub cli: BinaryIdentity,
}

#[derive(Debug, thiserror::Error)]
pub enum DescriptorError {
    #[error("invalid agent bundle {0}")]
    Invalid(&'static str),
    #[error("invalid agent bundle JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid agent digest: {0}")]
    Digest(#[source] pplx_space_content_digest::DigestParseError),
}

impl BundleDescriptor {
    pub fn parse(bytes: &[u8]) -> Result<Self, DescriptorError> {
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(DescriptorError::Invalid("metadata size"));
        }
        let value: Self = serde_json::from_slice(bytes)?;
        value.validate()?;
        if value.canonical_bytes()? != bytes {
            return Err(DescriptorError::Invalid("noncanonical encoding"));
        }
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), DescriptorError> {
        if self.schema != 1 {
            return Err(DescriptorError::Invalid("schema"));
        }
        if !(40..=64).contains(&self.source_revision.len())
            || !self
                .source_revision
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(DescriptorError::Invalid("source revision"));
        }
        let timestamp = self
            .built_at
            .parse::<jiff::Timestamp>()
            .map_err(|_| DescriptorError::Invalid("build timestamp"))?;
        if timestamp.to_string() != self.built_at {
            return Err(DescriptorError::Invalid("canonical build timestamp"));
        }
        if !self.version.pre.is_empty()
            || !self.version.build.is_empty()
            || !self.guest_update_required_since.pre.is_empty()
            || !self.guest_update_required_since.build.is_empty()
            || self.guest_update_required_since > self.version
        {
            return Err(DescriptorError::Invalid("guest-update release marker"));
        }
        if !valid_label(&self.compatibility_family) {
            return Err(DescriptorError::Invalid("compatibility family"));
        }
        if self.seed_contracts.is_empty()
            || self.seed_contracts.len() > 16
            || self.seed_contracts.iter().any(|v| !valid_label(v))
            || self
                .seed_contracts
                .windows(2)
                .any(|pair| matches!(pair, [a, b] if a >= b))
        {
            return Err(DescriptorError::Invalid("seed contracts"));
        }
        for binary in Binary::ALL {
            if !(1..=MAX_BINARY_BYTES).contains(&self.binary(binary).length) {
                return Err(DescriptorError::Invalid("payload size"));
            }
        }
        Ok(())
    }

    pub fn binary(&self, binary: Binary) -> &BinaryIdentity {
        match binary {
            Binary::Service => &self.service,
            Binary::Cli => &self.cli,
        }
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DescriptorError> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub fn digest(&self) -> Result<ArtifactDigest, DescriptorError> {
        Ok(ArtifactDigest::of_bytes(&self.canonical_bytes()?))
    }
}

pub(crate) fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_'))
}

/// Platform selected by an installed recovery contract, not a host OS default.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum RecoveryPlatform {
    Linux,
    Windows,
}
impl RecoveryPlatform {
    pub fn from_contract(contract: &str) -> Option<Self> {
        match contract {
            crate::SEED_CONTRACT => Some(Self::Linux),
            crate::WINDOWS_SEED_CONTRACT => Some(Self::Windows),
            _ => None,
        }
    }
    pub const fn contract(self) -> &'static str {
        match self {
            Self::Linux => crate::SEED_CONTRACT,
            Self::Windows => crate::WINDOWS_SEED_CONTRACT,
        }
    }
    pub const fn binary_name(self, binary: Binary) -> &'static str {
        match (self, binary) {
            (Self::Linux, binary) => binary.file_name(),
            (Self::Windows, Binary::Service) => "spaced-svc.exe",
            (Self::Windows, Binary::Cli) => "spaced.exe",
        }
    }
}
impl BundleDescriptor {
    pub fn recovery_platform(&self) -> Result<RecoveryPlatform, DescriptorError> {
        let mut supported = self
            .seed_contracts
            .iter()
            .filter_map(|contract| RecoveryPlatform::from_contract(contract));
        match (supported.next(), supported.next()) {
            (Some(platform), None) => Ok(platform),
            _ => Err(DescriptorError::Invalid(
                "ambiguous or unsupported recovery platform",
            )),
        }
    }
}

impl TryFrom<String> for RecoveryPlatform {
    type Error = DescriptorError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_contract(&value).ok_or(DescriptorError::Invalid("recovery contract"))
    }
}
impl From<RecoveryPlatform> for String {
    fn from(value: RecoveryPlatform) -> Self {
        value.contract().to_owned()
    }
}

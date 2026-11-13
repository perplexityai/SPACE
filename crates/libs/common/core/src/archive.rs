//! Stable archive envelope and verification boundary, independent of storage I/O.
//!
//! The reconstruction schema is owned by the caller's versioned codec. This
//! module does not define a production reconstruction model. See
//! `archive/INTEGRATION.md` for the wire layout and staging/reader obligations.

use std::{fmt, num::NonZeroU16};

use aws_lc_rs::digest::{SHA256, digest};
use uuid::Uuid;

use crate::types::SandboxId;
use pplx_space_content_digest::Digest;

const MAGIC: &[u8; 8] = b"SPARCH\0\0";
const ARCHIVE_FORMAT_VERSION: u16 = 1;
/// Fixed v1 envelope size, before the reconstruction payload.
pub const HEADER_LEN: usize = 52;
/// Hard wire-format limit. Downloaders must enforce this before buffering.
pub const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
/// Maximum complete manifest size, including its envelope.
pub const MAX_MANIFEST_LEN: usize = HEADER_LEN + MAX_PAYLOAD_LEN;

/// Identity of one immutable manifest revision, separate from archive attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ManifestId(Uuid);

impl ManifestId {
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

impl Default for ManifestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A codec's explicitly supported reconstruction schema; zero is never valid.
///
/// The envelope library assigns no production schema numbers. A consumer must
/// choose this from its decoder implementation, never from the received header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReconstructionSchemaVersion(NonZeroU16);

impl ReconstructionSchemaVersion {
    pub const fn new(value: NonZeroU16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Catalog checksum of the **entire encoded manifest**, including the header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ManifestSha256(Digest);

impl ManifestSha256 {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Digest::sha256(bytes))
    }

    pub const fn digest(self) -> Digest {
        self.0
    }
}

/// Trusted identity and checksum read from the catalog (or a staged encoding).
/// It carries no current-owner or existence authority after the catalog read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestReference {
    sandbox_id: SandboxId,
    manifest_id: ManifestId,
    sha256: ManifestSha256,
}

impl ManifestReference {
    pub const fn new(
        sandbox_id: SandboxId,
        manifest_id: ManifestId,
        sha256: ManifestSha256,
    ) -> Self {
        Self {
            sandbox_id,
            manifest_id,
            sha256,
        }
    }

    pub const fn sandbox_id(self) -> SandboxId {
        self.sandbox_id
    }

    pub const fn manifest_id(self) -> ManifestId {
        self.manifest_id
    }

    pub const fn sha256(self) -> ManifestSha256 {
        self.sha256
    }
}

/// Bytes prepared for upload. Encoding proves no remote write or durability.
/// Publish the manifest last, then verify bytes downloaded from its exact key.
pub struct EncodedManifest {
    reference: ManifestReference,
    bytes: Vec<u8>,
}

impl EncodedManifest {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn reference(&self) -> ManifestReference {
        self.reference
    }
}

impl fmt::Debug for EncodedManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncodedManifest")
            .field("reference", &self.reference)
            .field("encoded_len", &self.bytes.len())
            .finish()
    }
}

/// Exact bytes passed all envelope, identity and checksum checks.
///
/// Only [`verify`] constructs this type; it cannot be deserialized. The borrowed
/// bytes cannot change while this proof is held. It does not prove storage
/// persistence, artifact availability, current authorization, or catalog ownership.
/// Business decoders should accept this type, never unverified downloaded bytes.
pub struct VerifiedManifest<'a> {
    reference: ManifestReference,
    schema: ReconstructionSchemaVersion,
    payload: &'a [u8],
}

impl<'a> VerifiedManifest<'a> {
    pub const fn reference(&self) -> ManifestReference {
        self.reference
    }

    pub const fn schema(&self) -> ReconstructionSchemaVersion {
        self.schema
    }

    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

impl fmt::Debug for VerifiedManifest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedManifest")
            .field("reference", &self.reference)
            .field("schema", &self.schema)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    #[error("archive payload has {actual} bytes; maximum is {MAX_PAYLOAD_LEN}")]
    PayloadTooLarge { actual: usize },
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    #[error("archive manifest exceeds {MAX_MANIFEST_LEN} bytes")]
    ManifestTooLarge,
    #[error("archive envelope is truncated; requires {HEADER_LEN} bytes")]
    TruncatedHeader,
    #[error("invalid archive envelope magic")]
    InvalidMagic,
    #[error("unsupported archive format version {0}")]
    ArchiveVersion(u16),
    #[error("unsupported reconstruction schema {actual}; decoder supports {expected}")]
    ReconstructionVersion { expected: u16, actual: u16 },
    #[error("manifest sandbox identity differs from the requested sandbox")]
    SandboxIdentity,
    #[error("manifest revision identity differs from the expected pointer")]
    ManifestIdentity,
    #[error("declared payload length {0} exceeds {MAX_PAYLOAD_LEN} bytes")]
    PayloadTooLarge(u64),
    #[error("payload length mismatch: declared {declared}, received {actual}")]
    PayloadLength { declared: u64, actual: usize },
    #[error("whole-manifest SHA-256 differs from the expected catalog checksum")]
    ManifestChecksum,
}

/// Encode already-serialized reconstruction bytes. The caller's codec owns
/// schema correctness; this function bounds and seals bytes without parsing them.
/// Synchronous, O(payload length), at most [`MAX_PAYLOAD_LEN`] bytes.
pub fn encode(
    sandbox_id: SandboxId,
    manifest_id: ManifestId,
    schema: ReconstructionSchemaVersion,
    payload: &[u8],
) -> Result<EncodedManifest, EncodeError> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(EncodeError::PayloadTooLarge {
            actual: payload.len(),
        });
    }
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&ARCHIVE_FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&schema.get().to_be_bytes());
    bytes.extend_from_slice(sandbox_id.as_uuid().as_bytes());
    bytes.extend_from_slice(manifest_id.as_uuid().as_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(EncodedManifest {
        reference: ManifestReference::new(sandbox_id, manifest_id, ManifestSha256(sha256(&bytes))),
        bytes,
    })
}

/// Verify the complete downloaded object before handing any bytes to a business
/// decoder. `supported_schema` must come from the caller's codec, not the object.
/// Unknown archive versions and any reconstruction-version mismatch fail closed.
///
/// Synchronous, O(downloaded length), bounded by [`MAX_MANIFEST_LEN`]. An async
/// staging/reader caller should run this with its codec in `spawn_blocking`.
/// The I/O caller must cap the download itself; this check cannot undo allocation.
pub fn verify<'a>(
    expected: ManifestReference,
    supported_schema: ReconstructionSchemaVersion,
    downloaded: &'a [u8],
) -> Result<VerifiedManifest<'a>, VerifyError> {
    if downloaded.len() > MAX_MANIFEST_LEN {
        return Err(VerifyError::ManifestTooLarge);
    }
    let mut remaining = downloaded;
    if &take::<8>(&mut remaining)? != MAGIC {
        return Err(VerifyError::InvalidMagic);
    }
    let format = u16::from_be_bytes(take(&mut remaining)?);
    if format != ARCHIVE_FORMAT_VERSION {
        return Err(VerifyError::ArchiveVersion(format));
    }
    let schema = u16::from_be_bytes(take(&mut remaining)?);
    if schema != supported_schema.get() {
        return Err(VerifyError::ReconstructionVersion {
            expected: supported_schema.get(),
            actual: schema,
        });
    }
    let sandbox_id = SandboxId::from_uuid(Uuid::from_bytes(take(&mut remaining)?));
    let manifest_id = ManifestId::from_uuid(Uuid::from_bytes(take(&mut remaining)?));
    if sandbox_id != expected.sandbox_id {
        return Err(VerifyError::SandboxIdentity);
    }
    if manifest_id != expected.manifest_id {
        return Err(VerifyError::ManifestIdentity);
    }
    let declared = u64::from_be_bytes(take(&mut remaining)?);
    if declared > MAX_PAYLOAD_LEN as u64 {
        return Err(VerifyError::PayloadTooLarge(declared));
    }
    if declared != remaining.len() as u64 {
        return Err(VerifyError::PayloadLength {
            declared,
            actual: remaining.len(),
        });
    }
    if ManifestSha256(sha256(downloaded)) != expected.sha256 {
        return Err(VerifyError::ManifestChecksum);
    }
    Ok(VerifiedManifest {
        reference: expected,
        schema: supported_schema,
        payload: remaining,
    })
}

fn take<const N: usize>(remaining: &mut &[u8]) -> Result<[u8; N], VerifyError> {
    let (value, rest) = remaining
        .split_first_chunk::<N>()
        .ok_or(VerifyError::TruncatedHeader)?;
    *remaining = rest;
    Ok(*value)
}

fn sha256(bytes: &[u8]) -> Digest {
    let mut hash = [0; 32];
    hash.copy_from_slice(digest(&SHA256, bytes).as_ref());
    Digest::sha256(hash)
}

#[cfg(test)]
mod tests;

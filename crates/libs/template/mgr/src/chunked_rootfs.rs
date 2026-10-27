//! Opt-in rootfs chunks. Index bytes must be authenticated independently of
//! their transport before any leaf can be trusted or reused. Cache admission
//! additionally requires immutable backing; a chunk digest alone is not a lease.
//!
//! # Trust and lifetime
//!
//! Authorized release metadata supplies the expected logical-index and delivery
//! manifest digests. The logical index commits to ordered raw leaves, zero
//! regions, and exact EOF; the delivery map locates independently compressed
//! frames in immutable objects. Repacking may change the delivery commitment
//! without changing the logical commitment or any image bytes.
//!
//! Newly fetched chunks are checked against their full BLAKE3 digest before
//! assembly. Reused chunks come from admitted read-only btrfs subvolumes, with
//! pinned file/subvolume identities and durable SQLite leases. Readiness and
//! reusable extents are committed only after the filesystem persistence and
//! read-only boundary. All registered-source deletion paths must retire sources
//! and drain leases before removing their backing. Startup recovery assumes
//! exclusive ownership after the old service process has exited.
//!
//! This trusts the service, registry, kernel, and continued immutable backing.
//! It is not fs-verity: cached bytes are not cryptographically checked on each
//! read, and chunk assembly does not perform a final whole-image rehash.
//!
//! # Explicit experiment configuration
//!
//! The builder and template-manager application features are both named
//! `experimental-chunked-templates` and are disabled by default. Enabling a
//! feature alone does not select experimental delivery. Both sides require
//! `SPACE_TEMPLATE_EXPERIMENTAL_TEMPLATE_PREFIX`, a valid template-name prefix
//! ending in `-`; templates outside that namespace keep their ordinary path.
//!
//! The builder additionally takes `SPACE_TEMPLATE_CHUNK_SIZES` as unique,
//! comma-separated byte counts and `SPACE_TEMPLATE_CHUNK_PUBLISHER_DB` as an
//! absolute catalog path. Supported sizes are 512 KiB, 1 MiB, 4 MiB, 16 MiB,
//! and 64 MiB. Normal publication targets 64 MiB compressed packs independently
//! of chunk size. Only acknowledged immutable uploads enter the publisher
//! catalog, and release registration follows successful manifest publication.
//! Referenced packs must remain available; this module does not collect remote
//! objects merely because one release or local cache entry is removed.
//!
//! The reader selects `SPACE_TEMPLATE_EXPERIMENTAL_DELIVERY` as
//! `flat:<chunk bytes>` or `chunks:<chunk bytes>`. Flat delivery verifies the
//! registered whole-image BLAKE3 identity; both selections use durable,
//! read-only output backing. Missing requested representations fail rather
//! than silently selecting another delivery mode. Reader tuning variables are:
//!
//! - `SPACE_TEMPLATE_EXPERIMENTAL_RANGE_CONCURRENCY`: 1..16, default 4.
//! - `SPACE_TEMPLATE_EXPERIMENTAL_REQUEST_TARGET_BYTES`: 1..32 MiB, default 8 MiB.
//! - `SPACE_TEMPLATE_EXPERIMENTAL_SCHEDULING`: `coupled` (default), `pipeline`,
//!   `parallel-frames`, `overlap-reuse`, or `combined`.
//!
//! Request targets govern range coalescing, not chunk or pack identity. A single
//! frame may exceed that target. Explicit frame buffers and temporary range
//! files have separate per-assembly budgets; these are not process-wide RSS or
//! page-cache limits. Multiple concurrent assemblies multiply those budgets.
//!
//! Deploy the additive release-metadata schema before readers/writers that use
//! it. Rollouts and rollback are application operations; this feature neither
//! requires nor enables host kernel or filesystem-wide feature changes. Use the
//! owning template API for cache retirement, including before returning to code
//! that does not participate in verified-source leases. Removing files directly
//! or changing subvolume protection bypasses the cache's correctness contract.

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use pplx_space_common_core::types::{Digest, HashAlg};
use serde::{Deserialize, Serialize};

use crate::content_hash::ContentHasher;

pub mod assemble;
pub mod cache;
pub mod delivery;
pub mod publisher;

const INDEX_VERSION: u32 = 1;
const MAX_INDEX_BYTES: usize = 64 << 20;
const MAX_ROOTFS_BYTES: u64 = 1 << 40;
const MAX_CHUNK_BYTES: u64 = 64 << 20;

pub use pplx_space_common_core::types::template_chunks::{ChunkSize, ChunkSizeError};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Leaf {
    Zero { length: u64 },
    Zstd { length: u64, digest: Digest },
}

impl Leaf {
    pub const fn length(&self) -> u64 {
        match self {
            Self::Zero { length } | Self::Zstd { length, .. } => *length,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Zero { .. } => "zero",
            Self::Zstd { .. } => "zstd",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    version: u32,
    rootfs_digest: Digest,
    logical_bytes: u64,
    chunk_size: ChunkSize,
    leaves: Vec<Leaf>,
}

/// The private constructor ensures that consumers cannot skip index
/// authentication or accept an index for a different registered rootfs.
#[derive(Debug)]
pub struct AuthenticatedIndex(Index);

#[derive(Debug, thiserror::Error)]
pub enum ReadIndexError {
    #[error("chunk index exceeds its bounded wire size")]
    TooLarge,
    #[error("chunk index bytes do not match the trusted BLAKE3 index digest")]
    Authentication,
    #[error("decode chunk index: {0}")]
    Json(#[from] serde_json::Error),
    #[error("chunk index belongs to a different rootfs")]
    Rootfs,
    #[error("invalid chunk index layout: {0}")]
    Layout(&'static str),
}

impl AuthenticatedIndex {
    pub fn from_bytes(
        bytes: &[u8],
        expected_index_digest: Digest,
        expected_rootfs_digest: Digest,
    ) -> Result<Self, ReadIndexError> {
        if bytes.len() > MAX_INDEX_BYTES {
            return Err(ReadIndexError::TooLarge);
        }
        if digest_bytes(bytes) != expected_index_digest {
            return Err(ReadIndexError::Authentication);
        }
        let index: Index = serde_json::from_slice(bytes)?;
        if index.rootfs_digest != expected_rootfs_digest {
            return Err(ReadIndexError::Rootfs);
        }
        if index.version != INDEX_VERSION {
            return Err(ReadIndexError::Layout("unsupported version"));
        }
        if index.logical_bytes == 0 || index.logical_bytes > MAX_ROOTFS_BYTES {
            return Err(ReadIndexError::Layout("rootfs length out of bounds"));
        }
        let count = index.logical_bytes.div_ceil(index.chunk_size.bytes());
        if index.leaves.len() as u64 != count {
            return Err(ReadIndexError::Layout(
                "leaf count disagrees with rootfs length",
            ));
        }
        let mut remaining = index.logical_bytes;
        let mut lengths = std::collections::HashMap::new();
        for leaf in &index.leaves {
            let expected_length = remaining.min(index.chunk_size.bytes());
            if leaf.length() != expected_length {
                return Err(ReadIndexError::Layout(
                    "noncontiguous or incorrectly sized leaf",
                ));
            }
            if let Leaf::Zstd { length, digest } = leaf {
                if digest.algorithm() != HashAlg::Blake3 {
                    return Err(ReadIndexError::Layout("leaf digest must be BLAKE3"));
                }
                if lengths
                    .insert(*digest, *length)
                    .is_some_and(|old| old != *length)
                {
                    return Err(ReadIndexError::Layout("one digest has conflicting lengths"));
                }
            }
            remaining -= expected_length;
        }
        Ok(Self(index))
    }

    pub fn leaves(&self) -> &[Leaf] {
        &self.0.leaves
    }

    pub const fn logical_bytes(&self) -> u64 {
        self.0.logical_bytes
    }

    pub const fn rootfs_digest(&self) -> Digest {
        self.0.rootfs_digest
    }

    pub const fn chunk_size(&self) -> ChunkSize {
        self.0.chunk_size
    }
}

fn digest_bytes(bytes: &[u8]) -> Digest {
    Digest::blake3(*blake3::hash(bytes).as_bytes())
}

pub fn object_name(digest: Digest) -> String {
    format!("{}.zst", digest.hex_string())
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeChunkError {
    #[error("read or decompress chunk: {0}")]
    Io(#[from] io::Error),
    #[error("chunk length or algorithm is outside the supported format")]
    Bounds,
    #[error("chunk must contain exactly one complete zstd frame")]
    Frame,
    #[error("decoded chunk does not match its authenticated length or BLAKE3 digest")]
    Integrity,
}

/// Decode before cache admission. The compressed input and decoded output are
/// bounded even when the sender supplies a truncated frame or a decompression bomb.
pub fn decode_chunk(
    input: impl Read,
    digest: Digest,
    logical_bytes: u64,
) -> Result<Vec<u8>, DecodeChunkError> {
    if logical_bytes == 0
        || logical_bytes > MAX_CHUNK_BYTES
        || digest.algorithm() != HashAlg::Blake3
    {
        return Err(DecodeChunkError::Bounds);
    }
    let compressed_limit = logical_bytes + (1 << 20);
    let mut compressed = Vec::new();
    input
        .take(compressed_limit + 1)
        .read_to_end(&mut compressed)?;
    if compressed.len() as u64 > compressed_limit {
        return Err(DecodeChunkError::Bounds);
    }
    decode_chunk_bytes(&compressed, digest, logical_bytes)
}

pub(crate) fn decode_chunk_bytes(
    compressed: &[u8],
    digest: Digest,
    logical_bytes: u64,
) -> Result<Vec<u8>, DecodeChunkError> {
    if logical_bytes == 0
        || logical_bytes > MAX_CHUNK_BYTES
        || digest.algorithm() != HashAlg::Blake3
        || compressed.len() as u64 > logical_bytes + (1 << 20)
    {
        return Err(DecodeChunkError::Bounds);
    }
    let frame_bytes = zstd::zstd_safe::find_frame_compressed_size(compressed)
        .map_err(|_| DecodeChunkError::Frame)?;
    if frame_bytes != compressed.len() {
        return Err(DecodeChunkError::Frame);
    }
    let capacity = usize::try_from(logical_bytes).map_err(|_| DecodeChunkError::Bounds)?;
    let decoded = zstd::bulk::decompress(compressed, capacity)?;
    if decoded.len() as u64 != logical_bytes || digest_bytes(&decoded) != digest {
        return Err(DecodeChunkError::Integrity);
    }
    Ok(decoded)
}

#[derive(Debug)]
pub struct PublishedIndex {
    pub bytes: Vec<u8>,
    pub digest: Digest,
    pub rootfs_digest: Digest,
    pub image_blake3: Digest,
    pub new_objects: u64,
    pub new_compressed_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishChunksError {
    #[error("read rootfs or publish chunk object: {0}")]
    Io(#[from] io::Error),
    #[error("source rootfs length is out of bounds")]
    Bounds,
    #[error("source rootfs does not match the expected registered digest")]
    Rootfs,
    #[error("existing chunk object failed validation: {0}")]
    ExistingObject(#[from] DecodeChunkError),
    #[error("serialize chunk index: {0}")]
    Json(#[from] serde_json::Error),
}

/// Produce actual zstd payloads from the original artifact, preserving its
/// release digest. Publication of the returned index is the caller's commit
/// point; a failed source check may leave only unreferenced CAS objects.
pub fn publish_chunks(
    source: &Path,
    objects: &Path,
    chunk_size: ChunkSize,
    expected_rootfs: Digest,
) -> Result<PublishedIndex, PublishChunksError> {
    let mut input = File::open(source)?;
    let length = input.metadata()?.len();
    if length == 0 || length > MAX_ROOTFS_BYTES {
        return Err(PublishChunksError::Bounds);
    }
    std::fs::create_dir_all(objects)?;
    let mut rootfs_hasher = ContentHasher::new(expected_rootfs.algorithm());
    let mut image_hasher =
        (expected_rootfs.algorithm() != HashAlg::Blake3).then(blake3::Hasher::new);
    let mut leaves = Vec::new();
    let mut unique = HashSet::new();
    let mut remaining = length;
    let mut new_objects = 0;
    let mut new_compressed_bytes = 0;
    let mut buffer =
        vec![0; usize::try_from(chunk_size.bytes()).map_err(|_| PublishChunksError::Bounds)?];
    while remaining > 0 {
        let bytes = remaining.min(chunk_size.bytes());
        let count = usize::try_from(bytes).map_err(|_| PublishChunksError::Bounds)?;
        let chunk = buffer.get_mut(..count).ok_or(PublishChunksError::Bounds)?;
        input.read_exact(chunk)?;
        rootfs_hasher.update(chunk);
        if let Some(hasher) = &mut image_hasher {
            hasher.update(chunk);
        }
        if chunk.iter().all(|byte| *byte == 0) {
            leaves.push(Leaf::Zero { length: bytes });
        } else {
            let digest = digest_bytes(chunk);
            if unique.insert(digest) {
                let destination = objects.join(object_name(digest));
                if destination.try_exists()? {
                    decode_chunk(File::open(&destination)?, digest, bytes)?;
                } else {
                    let compressed = zstd::bulk::compress(chunk, 3)?;
                    let mut temporary = tempfile::NamedTempFile::new_in(objects)?;
                    temporary.write_all(&compressed)?;
                    temporary.as_file().sync_all()?;
                    match temporary.persist_noclobber(&destination) {
                        Ok(_) => {
                            new_objects += 1;
                            new_compressed_bytes += compressed.len() as u64;
                        }
                        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                            decode_chunk(File::open(&destination)?, digest, bytes)?;
                        }
                        Err(error) => return Err(error.error.into()),
                    }
                }
            }
            leaves.push(Leaf::Zstd {
                length: bytes,
                digest,
            });
        }
        remaining -= bytes;
    }
    let mut extra = [0_u8; 1];
    let actual_rootfs = rootfs_hasher.finish();
    if input.read(&mut extra)? != 0 || actual_rootfs != expected_rootfs {
        return Err(PublishChunksError::Rootfs);
    }
    File::open(objects)?.sync_all()?;
    let bytes = serde_json::to_vec(&Index {
        version: INDEX_VERSION,
        rootfs_digest: expected_rootfs,
        logical_bytes: length,
        chunk_size,
        leaves,
    })?;
    if bytes.len() > MAX_INDEX_BYTES {
        return Err(PublishChunksError::Bounds);
    }
    Ok(PublishedIndex {
        digest: digest_bytes(&bytes),
        rootfs_digest: actual_rootfs,
        image_blake3: image_hasher.map_or(actual_rootfs, |hasher| {
            Digest::blake3(*hasher.finalize().as_bytes())
        }),
        bytes,
        new_objects,
        new_compressed_bytes,
    })
}

#[cfg(test)]
mod tests;

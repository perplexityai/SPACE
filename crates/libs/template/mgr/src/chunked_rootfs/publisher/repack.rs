//! Change physical pack boundaries without changing any compressed frame.
//! The caller supplies authenticated metadata and locally pinned source packs;
//! the same upload-before-catalog-before-registration ordering as publish holds.

use super::*;
use crate::chunked_rootfs::{decode_chunk_bytes, delivery::AuthenticatedManifest};
use std::os::unix::fs::FileExt;

#[derive(Clone, Copy, Debug)]
pub struct PackTarget(u64);

#[derive(Debug, thiserror::Error)]
#[error("compressed pack target must be an integer byte count from 16 MiB through 256 MiB")]
pub struct PackTargetError;

impl Default for PackTarget {
    fn default() -> Self {
        Self(PACK_TARGET_BYTES)
    }
}

impl std::str::FromStr for PackTarget {
    type Err = PackTargetError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bytes = raw.parse().map_err(|_| PackTargetError)?;
        if !((16 << 20)..=(256 << 20)).contains(&bytes) {
            return Err(PackTargetError);
        }
        Ok(Self(bytes))
    }
}

impl PackTarget {
    pub fn bytes(self) -> u64 {
        self.0
    }
}

pub trait PackSource {
    /// Return an open regular file containing the complete immutable pack.
    fn open(&self, object: Digest) -> io::Result<File>;
}

#[derive(Debug, thiserror::Error)]
pub enum RepackError {
    #[error("read source pack: {0}")]
    Io(#[from] io::Error),
    #[error("source pack must be a regular file matching its authenticated length")]
    Pack,
    #[error("verify unchanged source frame: {0}")]
    Frame(#[from] crate::chunked_rootfs::DecodeChunkError),
    #[error("publish repacked representation: {0}")]
    Publish(#[from] PublishError),
    #[error("serialize unchanged logical index: {0}")]
    Index(#[from] serde_json::Error),
}

/// Blocking operator operation: reads unique chunks in first logical occurrence
/// order, verifies each frame, and copies its compressed bytes unchanged. Existing
/// catalog locations are deliberately not reused, since they have old boundaries.
/// Failed attempts can leave unreferenced immutable objects, never a registered
/// partial representation. Callers register only the returned uploaded manifest.
pub(super) fn repack(
    manifest: &AuthenticatedManifest,
    connection: &mut Connection,
    source: &impl PackSource,
    sink: &mut impl ObjectSink,
    staging: &Path,
    target: PackTarget,
) -> Result<UploadedPublication, RepackError> {
    let mut publisher = Publisher {
        namespace: sink.namespace(),
        connection,
        sink,
        objects: BTreeMap::new(),
        locations: BTreeMap::new(),
        statistics: Statistics::default(),
    };
    let mut seen = HashSet::new();
    let mut pack = Pack::new(staging)?;
    for leaf in manifest.index().leaves() {
        let Leaf::Zstd { length, digest } = leaf else {
            continue;
        };
        if !seen.insert(*digest) {
            continue;
        }
        let location = &manifest.locations()[digest];
        let object = &manifest.objects()[&location.object];
        let input = source.open(location.object)?;
        let metadata = input.metadata()?;
        if !metadata.is_file() || metadata.len() != object.compressed_bytes {
            return Err(RepackError::Pack);
        }
        let mut frame = vec![0; location.compressed_length as usize];
        input.read_exact_at(&mut frame, location.compressed_offset)?;
        // Verification allocates at most one bounded raw chunk, then drops it.
        // Packing consumes the original compressed bytes, never a recompression.
        drop(decode_chunk_bytes(&frame, *digest, *length)?);
        if pack.length > 0 && pack.length + frame.len() as u64 > target.bytes() {
            publisher.flush(pack)?;
            pack = Pack::new(staging)?;
        }
        pack.add(*digest, *length, &frame)?;
        publisher.statistics.repacked_chunks += 1;
        publisher.statistics.repacked_compressed_bytes += frame.len() as u64;
        if pack.length >= target.bytes() {
            publisher.flush(pack)?;
            pack = Pack::new(staging)?;
        }
    }
    publisher.flush(pack)?;
    let bytes = serde_json::to_vec(&manifest.index().0)?;
    let index = PublishedIndex {
        digest: digest_bytes(&bytes),
        bytes,
        rootfs_digest: manifest.index().rootfs_digest(),
        image_blake3: manifest.image_blake3(),
        new_objects: publisher.statistics.uploaded_packs,
        new_compressed_bytes: publisher.statistics.uploaded_pack_bytes,
    };
    Ok(finish_publication(publisher, index, staging)?)
}

//! Bounded pack publication. Run this blocking file/codec pipeline on a
//! blocking worker; the sink bridges to the owning transfer service.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use pplx_space_common_core::types::{Digest, HashAlg, StorageUri};
use rusqlite::Connection;

use super::delivery::{self, Binding, Location, ManifestError, Object};
use super::{
    ChunkSize, INDEX_VERSION, Index, Leaf, MAX_INDEX_BYTES, MAX_ROOTFS_BYTES, PublishedIndex,
    digest_bytes,
};
use crate::content_hash::ContentHasher;

pub mod repack;
pub mod store;

/// Fixed across every logical chunk size, including the benchmark matrix.
pub const PACK_TARGET_BYTES: u64 = 64 << 20;

#[derive(Clone, Debug)]
pub struct Namespace(String);

impl Namespace {
    /// Endpoint, bucket and prefix are all part of the reuse boundary. JSON
    /// encoding avoids aliases caused by separators appearing inside a field.
    pub fn new(storage: &StorageUri) -> Self {
        Self(
            serde_json::json!([
                storage.endpoint,
                storage.bucket,
                storage.region,
                storage.path_style,
                storage.key_prefix.trim_end_matches('/')
            ])
            .to_string(),
        )
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("object transport I/O: {0}")]
    Io(#[from] io::Error),
    #[error("object transport RPC: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("object transport did not acknowledge the exact immutable payload")]
    Receipt,
}

pub struct UploadReceipt {
    pub bytes: u64,
    pub version_id: Option<String>,
}

/// The implementation must store these exact bytes at the derived key under
/// this namespace, atomically. It may only acknowledge a completed upload or
/// an already-present identical object. No transparent compression is allowed.
pub trait ObjectSink {
    fn namespace(&self) -> Namespace;
    fn put(
        &mut self,
        key: &str,
        source: &Path,
        digest: Digest,
    ) -> Result<UploadReceipt, UploadError>;
}

#[derive(Default, Debug, serde::Serialize)]
pub struct Statistics {
    pub uploaded_packs: u64,
    pub uploaded_pack_bytes: u64,
    pub compressed_chunks: u64,
    pub reused_chunks: u64,
    pub repacked_chunks: u64,
    pub repacked_compressed_bytes: u64,
    pub manifest_uploaded: bool,
}

#[derive(Debug)]
pub struct UploadedPublication {
    pub binding: Binding,
    pub image_blake3: Digest,
    pub manifest_bytes: Vec<u8>,
    pub statistics: Statistics,
}

pub fn manifest_name(digest: Digest) -> String {
    format!("m-{}.json", digest.hex_string())
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("read source or stage pack: {0}")]
    Io(#[from] io::Error),
    #[error("publisher catalog: {0}")]
    Store(#[from] rusqlite::Error),
    #[error("publish immutable object: {0}")]
    Upload(#[from] UploadError),
    #[error("encode logical index: {0}")]
    Json(#[from] serde_json::Error),
    #[error("validate delivery manifest: {0}")]
    Manifest(#[from] ManifestError),
    #[error("source must be a regular image within the format size bounds")]
    Bounds,
    #[error("source bytes do not match the independently known release digest")]
    Source,
    #[error("publication is not recorded as uploaded")]
    NotUploaded,
}

struct Pack {
    file: tempfile::NamedTempFile,
    hasher: blake3::Hasher,
    length: u64,
    chunks: Vec<store::PackedChunk>,
}

impl Pack {
    fn new(staging: &Path) -> io::Result<Self> {
        Ok(Self {
            file: tempfile::NamedTempFile::new_in(staging)?,
            hasher: blake3::Hasher::new(),
            length: 0,
            chunks: Vec::new(),
        })
    }

    fn add(&mut self, digest: Digest, raw_length: u64, compressed: &[u8]) -> io::Result<()> {
        self.file.write_all(compressed)?;
        self.hasher.update(compressed);
        self.chunks.push(store::PackedChunk {
            digest,
            raw_length,
            offset: self.length,
            compressed_length: compressed.len() as u64,
        });
        self.length += compressed.len() as u64;
        Ok(())
    }
}

struct Publisher<'a, S> {
    connection: &'a mut Connection,
    sink: &'a mut S,
    namespace: Namespace,
    objects: BTreeMap<Digest, Object>,
    locations: BTreeMap<Digest, Location>,
    statistics: Statistics,
}

impl<S: ObjectSink> Publisher<'_, S> {
    fn flush(&mut self, pack: Pack) -> Result<(), PublishError> {
        if pack.chunks.is_empty() {
            return Ok(());
        }
        pack.file.as_file().sync_all()?;
        let digest = Digest::blake3(*pack.hasher.finalize().as_bytes());
        let receipt = self
            .sink
            .put(&delivery::object_name(digest), pack.file.path(), digest)?;
        if receipt.bytes != pack.length
            || receipt.version_id.as_ref().is_some_and(|value| {
                value.is_empty()
                    || value == "null"
                    || value.len() > 1024
                    || value.chars().any(char::is_control)
            })
        {
            return Err(UploadError::Receipt.into());
        }
        let object = Object {
            compressed_bytes: pack.length,
            version_id: receipt.version_id,
        };
        // A crash before this transaction leaves only an unreferenced object.
        // No chunk becomes reusable in SQLite before its whole pack exists.
        store::record_object(
            self.connection,
            self.namespace.as_str(),
            digest,
            &object,
            &pack.chunks,
        )?;
        self.objects.insert(digest, object);
        for chunk in pack.chunks {
            self.locations.insert(
                chunk.digest,
                Location {
                    object: digest,
                    compressed_offset: chunk.offset,
                    compressed_length: chunk.compressed_length,
                },
            );
        }
        self.statistics.uploaded_packs += 1;
        self.statistics.uploaded_pack_bytes += pack.length;
        Ok(())
    }
}

/// The open source handle pins the file identity. Its full legacy digest and
/// BLAKE3 identity come from the same read pass as the chunk hashes. A caller
/// must retain its source lease; a concurrent rewrite fails the final digest.
///
/// Only the returned, uploaded manifest may be registered with the control
/// plane. This routine never marks a release ready or grants trust to itself.
pub fn publish(
    mut source: File,
    connection: &mut Connection,
    sink: &mut impl ObjectSink,
    staging: &Path,
    chunk_size: ChunkSize,
    expected_rootfs: Digest,
) -> Result<UploadedPublication, PublishError> {
    let metadata = source.metadata()?;
    let length = metadata.len();
    if !metadata.is_file() || length == 0 || length > MAX_ROOTFS_BYTES {
        return Err(PublishError::Bounds);
    }
    let mut publisher = Publisher {
        namespace: sink.namespace(),
        connection,
        sink,
        objects: BTreeMap::new(),
        locations: BTreeMap::new(),
        statistics: Statistics::default(),
    };
    let mut rootfs = ContentHasher::new(expected_rootfs.algorithm());
    let mut image = (expected_rootfs.algorithm() != HashAlg::Blake3).then(blake3::Hasher::new);
    let mut buffer = vec![0; chunk_size.bytes() as usize];
    let mut leaves = Vec::new();
    let mut seen = HashSet::new();
    let mut remaining = length;
    let mut pack = Pack::new(staging)?;
    while remaining > 0 {
        let count = remaining.min(chunk_size.bytes());
        let raw = buffer
            .get_mut(..count as usize)
            .ok_or(PublishError::Bounds)?;
        source.read_exact(raw)?;
        rootfs.update(raw);
        if let Some(image) = &mut image {
            image.update(raw);
        }
        if raw.iter().all(|byte| *byte == 0) {
            leaves.push(Leaf::Zero { length: count });
        } else {
            let digest = digest_bytes(raw);
            leaves.push(Leaf::Zstd {
                length: count,
                digest,
            });
            if seen.insert(digest) {
                if let Some(existing) = store::find_chunk(
                    publisher.connection,
                    publisher.namespace.as_str(),
                    digest,
                    count,
                )? {
                    publisher
                        .objects
                        .insert(existing.location.object, existing.object);
                    publisher.locations.insert(digest, existing.location);
                    publisher.statistics.reused_chunks += 1;
                } else {
                    let compressed = zstd::bulk::compress(raw, 3)?;
                    if pack.length > 0 && pack.length + compressed.len() as u64 > PACK_TARGET_BYTES
                    {
                        publisher.flush(pack)?;
                        pack = Pack::new(staging)?;
                    }
                    pack.add(digest, count, &compressed)?;
                    publisher.statistics.compressed_chunks += 1;
                    // An oversized independent frame is its own object.
                    if pack.length >= PACK_TARGET_BYTES {
                        publisher.flush(pack)?;
                        pack = Pack::new(staging)?;
                    }
                }
            }
        }
        remaining -= count;
    }
    if source.read(&mut [0_u8; 1])? != 0 || rootfs.finish() != expected_rootfs {
        return Err(PublishError::Source);
    }
    let image_blake3 = image.map_or(expected_rootfs, |hash| {
        Digest::blake3(*hash.finalize().as_bytes())
    });
    publisher.flush(pack)?;
    let index_bytes = serde_json::to_vec(&Index {
        version: INDEX_VERSION,
        rootfs_digest: expected_rootfs,
        logical_bytes: length,
        chunk_size,
        leaves,
    })?;
    if index_bytes.len() > MAX_INDEX_BYTES {
        return Err(PublishError::Bounds);
    }
    let index = PublishedIndex {
        digest: digest_bytes(&index_bytes),
        bytes: index_bytes,
        rootfs_digest: expected_rootfs,
        image_blake3,
        new_objects: publisher.statistics.uploaded_packs,
        new_compressed_bytes: publisher.statistics.uploaded_pack_bytes,
    };
    finish_publication(publisher, index, staging)
}

fn finish_publication(
    mut publisher: Publisher<'_, impl ObjectSink>,
    index: PublishedIndex,
    staging: &Path,
) -> Result<UploadedPublication, PublishError> {
    let image_blake3 = index.image_blake3;
    let (manifest_bytes, binding) =
        delivery::encode(&index, publisher.objects, publisher.locations)?;
    store::prepare(
        publisher.connection,
        publisher.namespace.as_str(),
        binding,
        image_blake3,
        &manifest_bytes,
    )?;
    if store::phase(
        publisher.connection,
        publisher.namespace.as_str(),
        binding.manifest_digest,
    )? == Some(store::Phase::Prepared)
    {
        let mut manifest = tempfile::NamedTempFile::new_in(staging)?;
        manifest.write_all(&manifest_bytes)?;
        manifest.as_file().sync_all()?;
        let receipt = publisher.sink.put(
            &manifest_name(binding.manifest_digest),
            manifest.path(),
            binding.manifest_digest,
        )?;
        if receipt.bytes != manifest_bytes.len() as u64 {
            return Err(UploadError::Receipt.into());
        }
        store::advance(
            publisher.connection,
            publisher.namespace.as_str(),
            binding.manifest_digest,
            store::Phase::Prepared,
            store::Phase::Uploaded,
        )?;
        publisher.statistics.manifest_uploaded = true;
    }
    if !matches!(
        store::phase(
            publisher.connection,
            publisher.namespace.as_str(),
            binding.manifest_digest
        )?,
        Some(store::Phase::Uploaded | store::Phase::Registered)
    ) {
        return Err(PublishError::NotUploaded);
    }
    Ok(UploadedPublication {
        binding,
        image_blake3,
        manifest_bytes,
        statistics: publisher.statistics,
    })
}

#[cfg(test)]
mod tests;

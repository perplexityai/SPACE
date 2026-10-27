//! A release's authenticated delivery commitments. The object namespace still
//! needs the server's storage-policy check; parsing it is not authorization.

use pplx_space_common_core::types::{Digest, ExactStorageUri, HashAlg, template_chunks::ChunkSize};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "Wire", into = "Wire")]
pub struct ChunkRepresentation {
    chunk_size: ChunkSize,
    index_digest: Digest,
    manifest_digest: Digest,
    manifest_bytes: u64,
    image_blake3: Digest,
    storage_prefix: ExactStorageUri,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    format_version: u32,
    chunk_size_bytes: u64,
    index_digest: Digest,
    manifest_digest: Digest,
    manifest_bytes: u64,
    image_blake3: Digest,
    storage_prefix: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RepresentationError {
    #[error("unsupported chunk representation version")]
    Version,
    #[error("unsupported logical chunk size")]
    ChunkSize,
    #[error("chunk representation commitments must be full BLAKE3 digests")]
    Digest,
    #[error("invalid chunk object storage prefix")]
    Storage,
    #[error("manifest length is outside its 64 MiB bound")]
    ManifestSize,
}

impl TryFrom<Wire> for ChunkRepresentation {
    type Error = RepresentationError;

    fn try_from(wire: Wire) -> Result<Self, Self::Error> {
        if wire.format_version != 1 {
            return Err(RepresentationError::Version);
        }
        if [wire.index_digest, wire.manifest_digest, wire.image_blake3]
            .iter()
            .any(|digest| digest.algorithm() != HashAlg::Blake3)
        {
            return Err(RepresentationError::Digest);
        }
        if wire.manifest_bytes == 0 || wire.manifest_bytes > 64 << 20 {
            return Err(RepresentationError::ManifestSize);
        }
        let storage_prefix = ExactStorageUri::parse(&wire.storage_prefix)
            .map_err(|_| RepresentationError::Storage)?;
        if wire.storage_prefix.len() > 4096
            || wire.storage_prefix.chars().any(char::is_control)
            || storage_prefix.parsed().region.is_empty()
        {
            return Err(RepresentationError::Storage);
        }
        Ok(Self {
            chunk_size: wire
                .chunk_size_bytes
                .try_into()
                .map_err(|_| RepresentationError::ChunkSize)?,
            index_digest: wire.index_digest,
            manifest_digest: wire.manifest_digest,
            manifest_bytes: wire.manifest_bytes,
            image_blake3: wire.image_blake3,
            storage_prefix,
        })
    }
}

impl From<ChunkRepresentation> for Wire {
    fn from(value: ChunkRepresentation) -> Self {
        Self {
            format_version: 1,
            chunk_size_bytes: value.chunk_size.bytes(),
            index_digest: value.index_digest,
            manifest_digest: value.manifest_digest,
            manifest_bytes: value.manifest_bytes,
            image_blake3: value.image_blake3,
            storage_prefix: value.storage_prefix.as_wire().into(),
        }
    }
}

impl TryFrom<crate::proto::TemplateChunkRepresentation> for ChunkRepresentation {
    type Error = RepresentationError;

    fn try_from(value: crate::proto::TemplateChunkRepresentation) -> Result<Self, Self::Error> {
        Wire {
            format_version: value.format_version,
            manifest_bytes: value.manifest_bytes,
            chunk_size_bytes: value.chunk_size_bytes,
            index_digest: value
                .index_digest
                .parse()
                .map_err(|_| RepresentationError::Digest)?,
            manifest_digest: value
                .manifest_digest
                .parse()
                .map_err(|_| RepresentationError::Digest)?,
            image_blake3: value
                .image_blake3
                .parse()
                .map_err(|_| RepresentationError::Digest)?,
            storage_prefix: value.storage_prefix,
        }
        .try_into()
    }
}

impl From<ChunkRepresentation> for crate::proto::TemplateChunkRepresentation {
    fn from(value: ChunkRepresentation) -> Self {
        Self {
            format_version: 1,
            chunk_size_bytes: value.chunk_size.bytes(),
            index_digest: value.index_digest.to_string(),
            manifest_digest: value.manifest_digest.to_string(),
            manifest_bytes: value.manifest_bytes,
            image_blake3: value.image_blake3.to_string(),
            storage_prefix: value.storage_prefix.as_wire().into(),
        }
    }
}

impl ChunkRepresentation {
    pub const fn chunk_size(&self) -> ChunkSize {
        self.chunk_size
    }
    pub const fn index_digest(&self) -> Digest {
        self.index_digest
    }
    pub const fn manifest_digest(&self) -> Digest {
        self.manifest_digest
    }
    pub const fn manifest_bytes(&self) -> u64 {
        self.manifest_bytes
    }
    pub const fn image_blake3(&self) -> Digest {
        self.image_blake3
    }
    pub fn storage_prefix(&self) -> &ExactStorageUri {
        &self.storage_prefix
    }
    pub fn manifest_name(&self) -> String {
        format!("m-{}.json", self.manifest_digest.hex_string())
    }
}

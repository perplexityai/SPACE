//! Physical placement is authenticated separately from the logical index, so
//! immutable packs can be shared or repacked without changing image identity.

use std::collections::{BTreeMap, BTreeSet};

use pplx_space_common_core::types::{Digest, HashAlg};
use serde::{Deserialize, Serialize};

use super::{
    AuthenticatedIndex, Index, Leaf, MAX_INDEX_BYTES, PublishedIndex, ReadIndexError, digest_bytes,
};

const VERSION: u32 = 1;
// Object size is not a decoder allocation: readers fetch bounded ranges and
// verify independent frames. Keep a finite limit while allowing larger packs.
const MAX_OBJECT_BYTES: u64 = 256 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub compressed_bytes: u64,
    pub version_id: Option<String>,
}

/// Each range contains exactly one independent zstd frame.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Location {
    pub object: Digest,
    pub compressed_offset: u64,
    pub compressed_length: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    index: Index,
    image_blake3: Digest,
    // The format fixes independent zstd frames at version 1. A new codec needs
    // a new version; there is no unchecked decoder name from remote metadata.
    #[serde(deserialize_with = "unique_map")]
    objects: BTreeMap<Digest, Object>,
    #[serde(deserialize_with = "unique_map")]
    locations: BTreeMap<Digest, Location>,
}

fn unique_map<'de, D, V>(deserializer: D) -> Result<BTreeMap<Digest, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    V: Deserialize<'de>,
{
    struct Unique<V>(std::marker::PhantomData<V>);
    impl<'de, V: Deserialize<'de>> serde::de::Visitor<'de> for Unique<V> {
        type Value = BTreeMap<Digest, V>;

        fn expecting(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            out.write_str("a map with unique digest keys")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut input: A,
        ) -> Result<Self::Value, A::Error> {
            let mut map = BTreeMap::new();
            while let Some((key, value)) = input.next_entry::<Digest, V>()? {
                if map.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate digest key"));
                }
            }
            Ok(map)
        }
    }
    deserializer.deserialize_map(Unique(std::marker::PhantomData))
}

#[derive(Clone, Copy, Debug)]
pub struct Binding {
    pub rootfs_digest: Digest,
    pub index_digest: Digest,
    pub manifest_digest: Digest,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest exceeds its bounded wire size")]
    TooLarge,
    #[error("delivery manifest does not match the trusted BLAKE3 digest")]
    Authentication,
    #[error("decode delivery manifest: {0}")]
    Json(#[from] serde_json::Error),
    #[error("authenticate logical index: {0}")]
    Index(#[from] ReadIndexError),
    #[error("invalid delivery layout: {0}")]
    Layout(&'static str),
}

/// Callers cannot obtain a trusted location without authenticating both
/// commitments and validating complete coverage and bounded object ranges.
#[derive(Debug)]
pub struct AuthenticatedManifest {
    index: AuthenticatedIndex,
    image_blake3: Digest,
    objects: BTreeMap<Digest, Object>,
    locations: BTreeMap<Digest, Location>,
}

impl AuthenticatedManifest {
    pub fn from_bytes(bytes: &[u8], binding: Binding) -> Result<Self, ManifestError> {
        if bytes.len() > MAX_INDEX_BYTES {
            return Err(ManifestError::TooLarge);
        }
        if digest_bytes(bytes) != binding.manifest_digest {
            return Err(ManifestError::Authentication);
        }
        let manifest: Manifest = serde_json::from_slice(bytes)?;
        if manifest.version != VERSION {
            return Err(ManifestError::Layout("unsupported version"));
        }
        if manifest.image_blake3.algorithm() != HashAlg::Blake3
            || (binding.rootfs_digest.algorithm() == HashAlg::Blake3
                && binding.rootfs_digest != manifest.image_blake3)
        {
            return Err(ManifestError::Layout(
                "inconsistent full-image BLAKE3 binding",
            ));
        }
        let index = AuthenticatedIndex::from_bytes(
            &serde_json::to_vec(&manifest.index)?,
            binding.index_digest,
            binding.rootfs_digest,
        )?;
        let mut required = BTreeMap::new();
        for leaf in index.leaves() {
            if let Leaf::Zstd { length, digest } = leaf {
                required.insert(*digest, *length);
            }
        }
        if required.keys().ne(manifest.locations.keys()) {
            return Err(ManifestError::Layout(
                "locations must exactly cover nonzero chunks",
            ));
        }
        let mut referenced = BTreeSet::new();
        let mut ranges: BTreeMap<Digest, Vec<(u64, u64)>> = BTreeMap::new();
        for (chunk, location) in &manifest.locations {
            let raw_length = required
                .get(chunk)
                .ok_or(ManifestError::Layout("location has no logical chunk"))?;
            let Some(object) = manifest.objects.get(&location.object) else {
                return Err(ManifestError::Layout(
                    "location references an absent object",
                ));
            };
            if location.object.algorithm() != HashAlg::Blake3
                || object.compressed_bytes == 0
                || object.compressed_bytes > MAX_OBJECT_BYTES
                || location.compressed_length == 0
                || location.compressed_length > raw_length + (1 << 20)
            {
                return Err(ManifestError::Layout("object or frame bounds exceeded"));
            }
            if object.version_id.as_ref().is_some_and(|value| {
                value.is_empty()
                    || value == "null"
                    || value.len() > 1024
                    || value.chars().any(char::is_control)
            }) {
                return Err(ManifestError::Layout("invalid pinned object version"));
            }
            let end = location
                .compressed_offset
                .checked_add(location.compressed_length)
                .filter(|end| *end <= object.compressed_bytes)
                .ok_or(ManifestError::Layout("compressed range exceeds object"))?;
            referenced.insert(location.object);
            ranges
                .entry(location.object)
                .or_default()
                .push((location.compressed_offset, end));
        }
        if referenced.iter().ne(manifest.objects.keys()) {
            return Err(ManifestError::Layout(
                "manifest contains unreferenced objects",
            ));
        }
        for ranges in ranges.values_mut() {
            ranges.sort_unstable();
            if ranges
                .iter()
                .zip(ranges.iter().skip(1))
                .any(|(left, right)| left.1 > right.0)
            {
                return Err(ManifestError::Layout("compressed ranges overlap"));
            }
        }
        Ok(Self {
            index,
            image_blake3: manifest.image_blake3,
            objects: manifest.objects,
            locations: manifest.locations,
        })
    }

    pub fn index(&self) -> &AuthenticatedIndex {
        &self.index
    }
    pub const fn image_blake3(&self) -> Digest {
        self.image_blake3
    }
    pub fn objects(&self) -> &BTreeMap<Digest, Object> {
        &self.objects
    }
    pub fn locations(&self) -> &BTreeMap<Digest, Location> {
        &self.locations
    }
}

pub fn object_name(object: Digest) -> String {
    format!("p-{}.zstpack", object.hex_string())
}

/// Build metadata only after the publisher has established that every supplied
/// location is available. The reader's validator also guards publisher output.
pub fn encode(
    index: &PublishedIndex,
    objects: BTreeMap<Digest, Object>,
    locations: BTreeMap<Digest, Location>,
) -> Result<(Vec<u8>, Binding), ManifestError> {
    let verified = AuthenticatedIndex::from_bytes(&index.bytes, index.digest, index.rootfs_digest)?;
    let bytes = serde_json::to_vec(&Manifest {
        version: VERSION,
        index: verified.0,
        image_blake3: index.image_blake3,
        objects,
        locations,
    })?;
    let binding = Binding {
        rootfs_digest: index.rootfs_digest,
        index_digest: index.digest,
        manifest_digest: digest_bytes(&bytes),
    };
    AuthenticatedManifest::from_bytes(&bytes, binding)?;
    Ok((bytes, binding))
}

#[cfg(test)]
mod tests;

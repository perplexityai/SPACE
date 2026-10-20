use pplx_space_common_core::types::{Digest, HashAlg};
use sha2::Digest as _;

/// Streaming verification of the algorithm named by a template's identity.
/// Compressed transport checksums and base-snapshot identities remain SHA-256.
pub(crate) enum ContentHasher {
    Sha256(sha2::Sha256),
    Blake3(Box<blake3::Hasher>),
}

impl ContentHasher {
    pub(crate) fn new(algorithm: HashAlg) -> Self {
        match algorithm {
            HashAlg::Sha256 => Self::Sha256(sha2::Sha256::new()),
            HashAlg::Blake3 => Self::Blake3(Box::new(blake3::Hasher::new())),
        }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha256(hasher) => hasher.update(bytes),
            Self::Blake3(hasher) => {
                hasher.update(bytes);
            }
        }
    }

    pub(crate) fn finish(self) -> Digest {
        match self {
            Self::Sha256(hasher) => Digest::sha256(hasher.finalize().into()),
            Self::Blake3(hasher) => Digest::blake3(*hasher.finalize().as_bytes()),
        }
    }
}

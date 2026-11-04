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

/// Default for new rootfs registrations, independent of verification algorithms.
pub(crate) const DEFAULT_PRODUCTION_ALGORITHM: HashAlg = HashAlg::Blake3;
const PRODUCTION_ALGORITHM_ENV: &str = "SPACE_TEMPLATE_HASH_ALGORITHM";

#[derive(Debug, thiserror::Error)]
#[error("SPACE_TEMPLATE_HASH_ALGORITHM must be sha256 or blake3")]
pub struct TemplateHashConfigError;

pub(crate) fn production_algorithm_from_environment(
    environment: &dyn pplx_space_common_core::env::Environment,
) -> Result<HashAlg, TemplateHashConfigError> {
    match environment.var(PRODUCTION_ALGORITHM_ENV) {
        Ok(value) => value.parse().map_err(|_| TemplateHashConfigError),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_PRODUCTION_ALGORITHM),
        Err(std::env::VarError::NotUnicode(_)) => Err(TemplateHashConfigError),
    }
}

#[cfg(test)]
mod tests;

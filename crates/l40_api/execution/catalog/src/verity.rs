use pplx_space_common_core::types::{Digest, DigestParseError};

/// SHA-256 fs-verity descriptor digest: 4096-byte blocks, no salt, version 1.
/// This is distinct from SHA-256 over the file's contents.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct VerityDigest(Digest);

impl VerityDigest {
    pub const fn new(digest: Digest) -> Self {
        Self(digest)
    }

    pub const fn digest(self) -> Digest {
        self.0
    }
}

impl std::str::FromStr for VerityDigest {
    type Err = DigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl std::fmt::Display for VerityDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

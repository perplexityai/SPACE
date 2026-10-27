use pplx_space_common_core::types::Digest;

/// Identity of plaintext bytes, independent of transport compression.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Integrity {
    pub sha256: [u8; 32],
    pub size_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum IntegrityParseError {
    #[error("content integrity requires a canonical SHA-256 digest")]
    Digest,
}

impl Integrity {
    pub fn parse(digest: &str, size_bytes: u64) -> Result<Self, IntegrityParseError> {
        match digest.parse::<Digest>() {
            Ok(Digest::Sha256(sha256)) if Digest::sha256(sha256).to_string() == digest => {
                Ok(Self { sha256, size_bytes })
            }
            _ => Err(IntegrityParseError::Digest),
        }
    }

    pub fn digest(self) -> Digest {
        Digest::sha256(self.sha256)
    }
}

impl TryFrom<crate::proto::ContentIntegrity> for Integrity {
    type Error = IntegrityParseError;

    fn try_from(value: crate::proto::ContentIntegrity) -> Result<Self, Self::Error> {
        Self::parse(&value.digest, value.size_bytes)
    }
}

impl From<Integrity> for crate::proto::ContentIntegrity {
    fn from(value: Integrity) -> Self {
        Self {
            digest: value.digest().to_string(),
            size_bytes: value.size_bytes,
        }
    }
}

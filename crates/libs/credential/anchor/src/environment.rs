use crate::AnchorError;
use std::str::FromStr;

/// Deployment domain of a hardware-derived authority seed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnchorEnvironment(String);

impl FromStr for AnchorEnvironment {
    type Err = AnchorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(AnchorError::InvalidEnvironment);
        }
        Ok(Self(value.into()))
    }
}

impl AnchorEnvironment {
    /// KMS selects the immutable master; the message binds the deployment.
    /// Neither a clock nor a sandbox UUID participates in seed acquisition.
    pub fn seed_input(&self) -> Vec<u8> {
        kdf_input(b"space-authority-seed-v4", self.0.as_bytes())
    }
}

/// SP800-108-style counter || label || 0 || context length || context || L.
/// Framing and labels are frozen, not a claim of a validated KDF.
pub(crate) fn kdf_input(label: &[u8], context: &[u8]) -> Vec<u8> {
    let mut input = Vec::with_capacity(13 + label.len() + context.len());
    input.extend_from_slice(&1u32.to_be_bytes());
    input.extend_from_slice(label);
    input.push(0);
    input.extend_from_slice(&(context.len() as u32).to_be_bytes());
    input.extend_from_slice(context);
    input.extend_from_slice(&384u32.to_be_bytes());
    input
}

use crate::{AnchorError, sandbox_timestamp_ms};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Public key references only. No master secret is serialized or exported.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyEpoch {
    pub version: u64,
    pub since_ms: u64,
    pub key_id: String,
}

/// Immutable epoch selection preserves a sandbox's key across host changes,
/// authority restarts and master rotation. Retain every restorable epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpochCatalog {
    scheme: u32,
    environment: String,
    epochs: Vec<KeyEpoch>,
}

impl EpochCatalog {
    pub fn new(environment: String, epochs: Vec<KeyEpoch>) -> Result<Self, AnchorError> {
        let catalog = Self {
            scheme: 2,
            environment,
            epochs,
        };
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AnchorError> {
        let catalog: Self = serde_json::from_slice(bytes)?;
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn encode(&self) -> Result<Vec<u8>, AnchorError> {
        Ok(serde_json::to_vec(self)?)
    }

    fn validate(&self) -> Result<(), AnchorError> {
        if self.scheme != 2
            || self.environment.is_empty()
            || self.environment.len() > 128
            || !self
                .environment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || self
                .epochs
                .first()
                .is_none_or(|e| e.version != 1 || e.since_ms != 0)
        {
            return Err(AnchorError::InvalidCatalog);
        }
        for (index, epoch) in self.epochs.iter().enumerate() {
            if epoch.version != index as u64 + 1
                || epoch.key_id.is_empty()
                || epoch.key_id.len() > 2048
                || self
                    .epochs
                    .iter()
                    .take(index)
                    .any(|e| e.since_ms >= epoch.since_ms || e.key_id == epoch.key_id)
            {
                return Err(AnchorError::InvalidCatalog);
            }
        }
        Ok(())
    }

    /// Appending an epoch is permitted; replacing, removing or reordering one
    /// would silently change the trust of an existing sandbox and is rejected.
    pub fn validate_successor(&self, next: &Self) -> Result<(), AnchorError> {
        next.validate()?;
        if self.environment != next.environment
            || self.scheme != next.scheme
            || !next.epochs.starts_with(&self.epochs)
        {
            return Err(AnchorError::ChangedEpoch);
        }
        Ok(())
    }

    pub fn epochs(&self) -> &[KeyEpoch] {
        &self.epochs
    }

    pub fn epoch(&self, id: Uuid) -> Result<&KeyEpoch, AnchorError> {
        let born = sandbox_timestamp_ms(id)?;
        self.epochs
            .iter()
            .rev()
            .find(|epoch| epoch.since_ms <= born)
            .ok_or(AnchorError::InvalidCatalog)
    }

    /// Scheme 2 uses one HMAC-SHA384 invocation inside the hardware provider.
    /// SP800-108 counter-mode framing: counter || label || 0 || context || L.
    /// Context has a length-prefixed environment, u64 epoch and UUID bytes.
    pub fn derivation_input(&self, id: Uuid) -> Result<Vec<u8>, AnchorError> {
        let epoch = self.epoch(id)?;
        let mut input = Vec::with_capacity(64 + self.environment.len());
        input.extend_from_slice(&1u32.to_be_bytes());
        input.extend_from_slice(b"space-sandbox-anchor-v2");
        input.push(0);
        input.extend_from_slice(&(self.environment.len() as u32).to_be_bytes());
        input.extend_from_slice(self.environment.as_bytes());
        input.extend_from_slice(&epoch.version.to_be_bytes());
        input.extend_from_slice(id.as_bytes());
        input.extend_from_slice(&384u32.to_be_bytes());
        Ok(input)
    }
}

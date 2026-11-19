//! Node-installed selection authority, independently keyed by guest recovery ABI.
use crate::{
    ArtifactDigest, MAX_METADATA_BYTES, RecoveryPlatform,
    policy::{Promotion, PromotionError},
};
use pplx_space_cpu_arch::Arch;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledPolicy {
    pub authorization: Uuid,
    pub promotion: Promotion,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub targets: BTreeMap<RecoveryPlatform, BTreeMap<Arch, InstalledPolicy>>,
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("invalid guest-update catalog: {0}")]
    Invalid(&'static str),
    #[error("invalid guest-update catalog JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid installed promotion: {0}")]
    Promotion(#[from] PromotionError),
}

impl Catalog {
    pub fn parse(bytes: &[u8]) -> Result<Self, CatalogError> {
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(CatalogError::Invalid("size"));
        }
        let catalog: Self = serde_json::from_slice(bytes)?;
        let mut digests = HashSet::new();
        for (_, _, policy) in catalog.entries() {
            policy.promotion.accept_after(None)?;
            if policy.authorization.is_nil() {
                return Err(CatalogError::Invalid("authorization"));
            }
            if !digests.insert(policy.promotion.target) {
                return Err(CatalogError::Invalid(
                    "target digest appears in multiple platform entries",
                ));
            }
        }
        Ok(catalog)
    }

    pub fn entries(&self) -> impl Iterator<Item = (RecoveryPlatform, Arch, &InstalledPolicy)> {
        self.targets.iter().flat_map(|(platform, architectures)| {
            architectures
                .iter()
                .map(move |(arch, policy)| (*platform, *arch, policy))
        })
    }

    pub fn select(
        &self,
        platform: RecoveryPlatform,
        architecture: Arch,
    ) -> Option<&InstalledPolicy> {
        self.targets.get(&platform)?.get(&architecture)
    }

    pub fn target(
        &self,
        digest: ArtifactDigest,
    ) -> Option<(RecoveryPlatform, Arch, &InstalledPolicy)> {
        self.entries()
            .find(|(_, _, policy)| policy.promotion.target == digest)
    }
}

#[cfg(test)]
mod tests;

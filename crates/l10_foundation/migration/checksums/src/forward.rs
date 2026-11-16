//! Migrations a release may find recorded without bundling them.
//!
//! An image rollback returns a node service to a release whose catalog lacks
//! the migrations the newer release applied. A release lists those it can run
//! against, each with the exact checksum its bundling release records and why
//! the listing release keeps working on that schema. A node-store runner
//! accepts such a history row as it is and applies nothing for it; any other
//! unbundled row, or a listed version with another checksum, is still refused.

use std::collections::BTreeSet;

use serde::Deserialize;
use sqlx::migrate::MigrateError;

use super::{Checksum, ManifestError, valid_identity};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    store: String,
    version: i64,
    checksum: Checksum,
    /// Why the listing release reads and writes correctly once it is applied.
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForwardCompatible {
    migrations: Vec<Entry>,
}

impl ForwardCompatible {
    pub fn from_toml(source: &str) -> Result<Self, ManifestError> {
        let registry: Self = toml::from_str(source)?;
        let mut identities = BTreeSet::new();
        for entry in &registry.migrations {
            if !valid_identity(&entry.store, entry.version) {
                return Err(ManifestError::Identity {
                    store: entry.store.clone(),
                    version: entry.version,
                });
            }
            if !identities.insert((&entry.store, entry.version)) {
                return Err(ManifestError::DuplicateMigration {
                    store: entry.store.clone(),
                    version: entry.version,
                });
            }
            if entry.reason.trim().is_empty() {
                return Err(ManifestError::Reason {
                    store: entry.store.clone(),
                    version: entry.version,
                });
            }
        }
        Ok(registry)
    }

    pub fn embedded() -> Result<Self, MigrateError> {
        Self::from_toml(include_str!(
            "../../../../../db/forward-compatible-migrations.toml"
        ))
        .map_err(|error| MigrateError::Source(Box::new(error)))
    }

    /// Whether `store` may keep `version` recorded with `applied` as its
    /// checksum while its catalog doesn't bundle that version.
    pub fn accepts(&self, store: &str, version: i64, applied: &[u8]) -> bool {
        self.migrations.iter().any(|entry| {
            entry.store == store && entry.version == version && entry.checksum.0 == applied
        })
    }

    /// The listed `(store, version, checksum)` triples, for checks that each
    /// matches the catalog that bundles it.
    pub fn entries(&self) -> impl Iterator<Item = (&str, i64, &[u8])> {
        self.migrations.iter().map(|entry| {
            (
                entry.store.as_str(),
                entry.version,
                entry.checksum.0.as_slice(),
            )
        })
    }
}

#[cfg(test)]
mod tests;

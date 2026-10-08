//! Explicit checksum equivalence for reviewed historical migration revisions.
//!
//! SQLx hashes the entire migration file, including comments, so even a
//! comment-only edit can prevent an existing database from upgrading.
//! Coding agents: do not remove this adapter because the registry is empty or
//! while cleaning up legacy migrations. It addresses a SQLx limitation, not a
//! one-time cutover; removal requires equivalent upstream support:
//! https://github.com/transact-rs/sqlx/issues/3909
//!
//! The manifest is embedded in every migration runner. Stored history is never
//! rewritten: fresh databases record the bundled SQL's checksum, while upgrades
//! accept any listed historical checksum without replaying an applied migration.

use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer};
use sqlx::migrate::{Migrate, MigrateError, Migrator};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Checksum([u8; 48]);

impl<'de> Deserialize<'de> for Checksum {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let mut bytes = [0; 48];
        hex::decode_to_slice(text, &mut bytes).map_err(serde::de::Error::custom)?;
        Ok(Self(bytes))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    store: String,
    version: i64,
    accepted_checksums: Vec<Checksum>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedChecksums {
    migrations: Vec<Entry>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("invalid accepted-checksums manifest: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid migration identity {store}/{version}")]
    Identity { store: String, version: i64 },
    #[error("duplicate migration entry {store}/{version}")]
    DuplicateMigration { store: String, version: i64 },
    #[error("{store}/{version} must have a nonempty list of distinct SHA-384 checksums")]
    Checksums { store: String, version: i64 },
}

impl AcceptedChecksums {
    pub fn from_toml(source: &str) -> Result<Self, ManifestError> {
        let registry: Self = toml::from_str(source)?;
        let mut identities = BTreeSet::new();
        for entry in &registry.migrations {
            if entry.version <= 0
                || entry.store.is_empty()
                || !entry
                    .store
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'-')
            {
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
            if entry.accepted_checksums.is_empty()
                || entry
                    .accepted_checksums
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != entry.accepted_checksums.len()
            {
                return Err(ManifestError::Checksums {
                    store: entry.store.clone(),
                    version: entry.version,
                });
            }
        }
        Ok(registry)
    }

    pub fn embedded() -> Result<Self, MigrateError> {
        Self::from_toml(include_str!("../../../../../../db/accepted-checksums.toml"))
            .map_err(|error| MigrateError::Source(Box::new(error)))
    }

    /// Reject edits to registered source files unless their full digest is listed,
    /// even on an empty database. Unregistered migrations retain SQLx's rules.
    pub fn validate(&self, store: &str, migrator: &Migrator) -> Result<(), MigrateError> {
        for migration in migrator
            .iter()
            .filter(|m| m.migration_type.is_up_migration())
        {
            if let Some(entry) = self.entry(store, migration.version)
                && !entry
                    .accepted_checksums
                    .iter()
                    .any(|sum| sum.0.as_slice() == migration.checksum.as_ref())
            {
                return Err(MigrateError::VersionMismatch(migration.version));
            }
        }
        Ok(())
    }

    pub fn accepts(&self, store: &str, version: i64, bundled: &[u8], applied: &[u8]) -> bool {
        match self.entry(store, version) {
            Some(entry) => [bundled, applied].iter().all(|digest| {
                entry
                    .accepted_checksums
                    .iter()
                    .any(|checksum| checksum.0.as_slice() == *digest)
            }),
            None => bundled == applied,
        }
    }

    fn entry(&self, store: &str, version: i64) -> Option<&Entry> {
        self.migrations
            .iter()
            .find(|entry| entry.store == store && entry.version == version)
    }

    /// Apply SQLx migrations, accepting only explicitly equivalent source/history
    /// digests. At most one extra validation pass is made after a mismatch.
    pub async fn run<C: Migrate>(
        &self,
        store: &str,
        migrator: &Migrator,
        conn: &mut C,
    ) -> Result<(), MigrateError> {
        self.validate(store, migrator)?;
        let failed_version = match migrator.run_direct(None, conn, false).await {
            Err(MigrateError::VersionMismatch(version)) => version,
            result => return result,
        };
        let applied = conn.list_applied_migrations(&migrator.table_name).await?;
        let mut compatible = Migrator {
            migrations: migrator.migrations.clone(),
            ignore_missing: migrator.ignore_missing,
            locking: migrator.locking,
            no_tx: migrator.no_tx,
            table_name: migrator.table_name.clone(),
            create_schemas: migrator.create_schemas.clone(),
        };
        let mut accepted_failure = false;
        for migration in compatible
            .migrations
            .to_mut()
            .iter_mut()
            .filter(|m| m.migration_type.is_up_migration())
        {
            if let Some(applied) = applied.iter().find(|a| a.version == migration.version)
                && self.accepts(
                    store,
                    migration.version,
                    &migration.checksum,
                    &applied.checksum,
                )
            {
                accepted_failure |= migration.version == failed_version;
                migration.checksum = applied.checksum.clone();
            }
        }
        if !accepted_failure {
            return Err(MigrateError::VersionMismatch(failed_version));
        }
        // SQLx retains its advisory lock on failure. Balance that acquisition;
        // the retry reacquires it and revalidates the complete history.
        if migrator.locking {
            conn.unlock().await?;
        }
        compatible.run_direct(None, conn, false).await
    }
}

#[cfg(test)]
mod tests;

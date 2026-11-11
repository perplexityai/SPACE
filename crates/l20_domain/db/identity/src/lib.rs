//! Control-plane Postgres identities used by application connection pools.
//!
//! It is its own crate so that connection-pool code can name the closed set of
//! application Postgres logins without depending on `pplx-space-common-core`,
//! which re-exports it as `types::database`.

use std::fmt;

/// Login identity assigned to a control-plane Postgres pool.
///
/// These are the only logins the application tier may connect as. The RDS
/// master login (`space_admin`) and the read-only operator login (`space_ro`)
/// are deliberately unrepresentable here: services must not run on the
/// connecting admin, and keeping the set closed makes any attempt to widen a
/// pool's privileges visible in this enum's diff instead of hiding in string
/// manipulation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DatabaseIdentity {
    /// Least-privilege application DML login for ordinary service queries.
    /// Holds no grants on the credential-fenced tables.
    SpaceUser,
    /// Login reserved for narrowly scoped credential-table access
    /// (`user_credentials`, `org_credentials`, `org_master_keys`,
    /// `snapshot_storage_location_encrypted_fields`).
    SpaceCreds,
}

impl DatabaseIdentity {
    /// Canonical Postgres role name used at configuration and telemetry boundaries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SpaceUser => "space_user",
            Self::SpaceCreds => "space_creds",
        }
    }
}

impl fmt::Display for DatabaseIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests;

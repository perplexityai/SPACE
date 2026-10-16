//! sbx-mgr's database as a migration runner sees it. The service migrates it
//! at startup with [`super::migrate`]; the database migrator's
//! `sandbox-manager` subcommand runs the same migrations, and its `down`,
//! `status` and `reset`, from [`target`], so neither names the migrations or
//! the path itself.

use sqlx::migrate::Migrator;

/// What a migration runner needs to migrate sbx-mgr's database.
///
/// The database also carries the shared `db/common` schema (the `jobs`
/// table and its kin). Every runner applies that from its own embedded copy
/// of `db/common`, the one it applies to each database with the shared
/// schema, so the target names only the service's migrations.
#[derive(Debug, Clone, Copy)]
pub struct MigrationTarget {
    /// The name the shared SQLite helpers log and report the database under.
    pub name: &'static str,
    /// Where the service opens the database, and so where a runner migrates
    /// it unless told otherwise.
    pub default_path: &'static str,
    /// The service's own migrations.
    pub service: &'static Migrator,
}

/// sbx-mgr's database and its migrations.
pub const fn target() -> MigrationTarget {
    MigrationTarget {
        name: "sandbox-manager",
        default_path: super::DB_PATH,
        service: &pplx_space_sbxmgr_migrations::SANDBOX_MANAGER_MIGRATOR,
    }
}

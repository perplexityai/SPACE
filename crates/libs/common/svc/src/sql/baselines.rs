//! First supported public SQLite migration epoch.

#[cfg(feature = "sqlite")]
pub const PUBLIC_SQLITE_BASELINE: i64 = 20260930000000;

#[cfg(feature = "sqlite")]
pub fn is_public(migrator: &sqlx::migrate::Migrator) -> bool {
    migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .min()
        .is_some_and(|version| version >= PUBLIC_SQLITE_BASELINE)
}

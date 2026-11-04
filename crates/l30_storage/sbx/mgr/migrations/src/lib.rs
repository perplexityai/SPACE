//! Embedded sandbox-manager and shared SQLite migrations.
use sqlx::migrate::Migrator;

pub static SANDBOX_MANAGER_MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/sandbox-manager");
pub static COMMON_SCHEMA_MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/common");

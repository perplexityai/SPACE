//! An in-memory database with template-manager's schema, for the store's
//! tests.

use rusqlite::Connection;

use crate::database::{COMMON_MIGRATOR, MIGRATOR};

pub(crate) fn conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    for migration in COMMON_MIGRATOR
        .iter()
        .chain(MIGRATOR.iter())
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
    conn
}

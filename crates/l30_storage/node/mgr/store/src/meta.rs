//! `network_pool_meta`: the pool's persisted flags, one row per key.

use pplx_space_sql_locator::sql;
use pplx_space_sqlite::SqliteU64;
use rusqlite::{OptionalExtension as _, params};

use crate::{Connection, Result};

/// Whether flag `key` is stored as `true` or `1`; an absent key reads as false.
pub fn meta_bool(conn: &Connection, key: &str) -> Result<bool> {
    Ok(conn
        .0
        .query_row(
            sql!("SELECT value FROM network_pool_meta WHERE key = ?1"),
            params![key],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map(|value| matches!(value.as_deref(), Some("true") | Some("1")))?)
}

pub fn set_meta_bool(conn: &Connection, key: &str, value: bool, now: u64) -> Result<()> {
    conn.0.execute(
        sql!(
            "INSERT INTO network_pool_meta (key, value, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at"
        ),
        params![key, if value { "true" } else { "false" }, SqliteU64(now)],
    )?;
    Ok(())
}

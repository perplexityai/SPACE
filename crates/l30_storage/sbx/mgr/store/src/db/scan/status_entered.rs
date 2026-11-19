//! When each row entered its status.

use std::collections::BTreeMap;

use pplx_space_sql_locator::sql;
use rusqlite::types::Value;

use crate::db::timestamps::EpochSeconds;
use crate::db::{Connection, Error, Result};

/// Each row's `state_changed_at`, by id: the Unix second its status last
/// changed, its lifecycle (`state`) or the cleanup that owns it
/// (`cleanup_state`). A stamp that doesn't decode, or the column's default
/// `0` that no write leaves, is `None`: the row's time in its status is
/// unknown.
pub fn status_entered_at(conn: &Connection) -> Result<BTreeMap<String, Option<u64>>> {
    let mut statement = conn.prepare(sql!("SELECT id, state_changed_at FROM sandboxes"))?;
    statement
        .query_map([], |row| {
            let entered = EpochSeconds::decode(&row.get::<_, Value>(1)?)
                .ok()
                .map(EpochSeconds::unsigned)
                .filter(|seconds| *seconds > 0);
            Ok((row.get::<_, String>(0)?, entered))
        })?
        .collect::<rusqlite::Result<_>>()
        .map_err(Error::from)
}

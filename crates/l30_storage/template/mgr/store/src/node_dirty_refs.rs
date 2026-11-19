//! The `node_dirty_refs` outbox: the references template-manager's worker
//! lists and sends to node-ctl, and the revision-guarded delete that
//! acknowledges them.

use rusqlite::{Connection, Result, params};

use pplx_space_sql_locator::sql;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDirtyRefRow {
    pub facet: i32,
    pub element_kind: String,
    pub element_value: String,
    /// Opaque send witness; timestamps cannot distinguish same-second writes.
    pub revision: i64,
}

/// The `limit` oldest dirty references, oldest first.
pub fn list_node_dirty_refs(conn: &Connection, limit: usize) -> Result<Vec<NodeDirtyRefRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT facet, element_kind, element_value, revision
         FROM node_dirty_refs
         ORDER BY requested_at ASC
         LIMIT ?1"
    ))?;
    let rows = stmt.query_map(params![limit_to_i64(limit)], |row| {
        Ok(NodeDirtyRefRow {
            facet: row.get(0)?,
            element_kind: row.get(1)?,
            element_value: row.get(2)?,
            revision: row.get(3)?,
        })
    })?;
    rows.collect()
}

/// Delete each listed reference whose revision is still the one listed, and
/// return how many were deleted. A newer enqueue, even for a previously
/// deleted key, has a newer revision and stays pending. Run it in the caller's
/// transaction to delete the batch atomically.
pub fn delete_node_dirty_refs_at_revision(
    conn: &Connection,
    refs: &[NodeDirtyRefRow],
) -> Result<usize> {
    let mut deleted = 0;
    for reference in refs {
        deleted += conn.execute(
            sql!(
                "DELETE FROM node_dirty_refs
             WHERE facet = ?1
               AND element_kind = ?2
               AND element_value = ?3
               AND revision = ?4"
            ),
            params![
                reference.facet,
                reference.element_kind,
                reference.element_value,
                reference.revision
            ],
        )?;
    }
    Ok(deleted)
}

fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

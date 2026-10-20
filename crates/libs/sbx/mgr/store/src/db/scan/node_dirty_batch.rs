//! The node-manager dirty outbox.

use crate::db::{Connection, NodeDirtyRefRow, Result};

/// The `limit` oldest dirty references, oldest first.
pub fn node_dirty_batch(conn: &Connection, limit: usize) -> Result<Vec<NodeDirtyRefRow>> {
    crate::db::list_node_dirty_refs(conn, limit)
}

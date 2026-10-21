//! Rows with no node-manager execution bound yet.

use crate::db::{Connection, Result};

/// Ids of rows without a node execution, in id order, at most the execution
/// preflight limit. Startup adopts a baseline for these legacy rows.
pub fn unbound_executions(conn: &Connection) -> Result<Vec<String>> {
    crate::db::unbound_node_executions(conn)
}

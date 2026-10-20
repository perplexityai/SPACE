//! One page of the List RPC's inventory.

use crate::db::Connection;
use crate::db::inventory_read::{InventoryReadError, ListPage};

/// Up to `limit` rows with ids after `after`, in id order, within the raw
/// inventory budget. The page ends early rather than exceed the budget; only
/// a single row over it is an error. A row that does not decode is kept as
/// `ListPageRow::Undecodable` with its identity.
pub fn list_page(
    conn: &Connection,
    after: &str,
    limit: usize,
) -> Result<ListPage, InventoryReadError> {
    crate::db::inventory_read::list_page(conn, after, limit)
}

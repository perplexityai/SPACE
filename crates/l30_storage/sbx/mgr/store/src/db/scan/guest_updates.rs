use crate::db::{Connection, Result, guest_updates::Record};

/// Bounded recovery page, ordered by operation ID. The next page starts after
/// the last returned ID, including records the caller could not reconcile.
pub fn unfinished_guest_updates(
    conn: &Connection,
    after: Option<uuid::Uuid>,
) -> Result<Vec<Record>> {
    crate::db::guest_updates::unfinished(conn, after)
}

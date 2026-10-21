//! Bounded pages of the `local_artifacts` inventory for the snapshot reaper.

use crate::db::Connection;
use crate::db::inventory_read::InventoryReadError;
use crate::db::snapshot_gc::{ArtifactPage, ArtifactScan};

/// The next page of `scan`: at most 128 rows within the raw inventory
/// budget. An oversized or malformed row is retained and skipped.
pub fn local_artifacts(
    conn: &Connection,
    scan: &mut ArtifactScan,
) -> Result<ArtifactPage, InventoryReadError> {
    scan.next_page(conn)
}

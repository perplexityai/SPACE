//! Discovery of quarantines that legacy automatic recovery owns.

pub use crate::db::quarantine_scan::QuarantineRecoveryBatch;
use crate::db::{Connection, PeriodicQuarantineRecoveryScan, Result};

/// The next batch of `scan`: at most eight due candidates from at most 128
/// raw rows. Malformed rows are counted toward the completed sweep and
/// skipped; a statement failure resets the sweep.
pub fn quarantine_recovery(
    conn: &Connection,
    scan: &mut PeriodicQuarantineRecoveryScan,
) -> Result<QuarantineRecoveryBatch> {
    scan.next_batch(conn)
}

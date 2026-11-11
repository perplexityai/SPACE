//! Bounded pages of the `sandboxes` inventory. Each cursor keeps its own
//! pinning, page size, budget and policy for rows that do not decode; none
//! is a complete inventory or authority to mutate a row it returns.

use crate::db::cleanup_target_scan::{CleanupTargetObservation, CleanupTargetScan};
use crate::db::inventory::SandboxInventoryScan;
use crate::db::inventory_read::InventoryReadError;
use crate::db::{
    CleanupRecoveryPage, Connection, Error, PeriodicLifecycleRecoveryScan, SandboxRow,
};

/// Only the store defines these; callers pick one, they do not add their own.
mod sealed {
    pub trait Sealed {}
    impl Sealed for super::SandboxInventoryScan {}
    impl Sealed for super::CleanupTargetScan {}
    impl Sealed for super::PeriodicLifecycleRecoveryScan {}
    impl Sealed for super::CleanupObligationScan {}
}

/// A cursor over the `sandboxes` inventory that [`sandbox_pages`] advances.
pub trait SandboxPages: sealed::Sealed {
    type Page;
    type Error;

    fn next_page(&mut self, conn: &Connection) -> Result<Self::Page, Self::Error>;
}

/// Read the next page of `pages` and advance it.
pub fn sandbox_pages<P: SandboxPages>(
    conn: &Connection,
    pages: &mut P,
) -> Result<P::Page, P::Error> {
    pages.next_page(conn)
}

/// Inventory: whole rows, 128 rowids per page, pinned to the sweep's upper
/// rowid. Rows that do not decode are skipped.
impl SandboxPages for SandboxInventoryScan {
    type Page = Vec<SandboxRow>;
    type Error = Error;

    fn next_page(&mut self, conn: &Connection) -> Result<Self::Page, Self::Error> {
        self.next_batch(conn)
    }
}

/// Cleanup targets: every row's raw id with its cleanup metadata where that
/// decodes, 128 rows per page within the encoded inventory budget.
impl SandboxPages for CleanupTargetScan {
    type Page = Vec<CleanupTargetObservation>;
    type Error = InventoryReadError;

    fn next_page(&mut self, conn: &Connection) -> Result<Self::Page, Self::Error> {
        self.next_batch(conn)
    }
}

/// Resident recovery: at most eight decoded paused, resuming, checkpointing
/// or unpinned suspending rows from at most 128 rowids, pinned to the sweep's
/// upper rowid.
impl SandboxPages for PeriodicLifecycleRecoveryScan {
    type Page = Vec<SandboxRow>;
    type Error = Error;

    fn next_page(&mut self, conn: &Connection) -> Result<Self::Page, Self::Error> {
        crate::db::periodic_lifecycle_recovery_batch(conn, self)
    }
}

/// Cleanup obligations: ids of rows with durable cleanup evidence and no
/// active cleanup job among the next 128 rowids, read in one deferred
/// transaction. Every qualifying row in the range is returned; there is no
/// candidate cap. The range is not pinned to a sweep: rows inserted behind the
/// cursor are found on its next pass, and a full pass starts over.
///
/// `Copy`, so a caller can advance a copy and keep the original when the read
/// fails.
#[derive(Clone, Copy, Debug, Default)]
pub struct CleanupObligationScan {
    /// The last rowid examined; `None` starts a pass.
    after: Option<i64>,
}

impl CleanupObligationScan {
    const PAGE_ROWS: usize = 128;
}

impl SandboxPages for CleanupObligationScan {
    type Page = CleanupRecoveryPage;
    type Error = Error;

    fn next_page(&mut self, conn: &Connection) -> Result<Self::Page, Self::Error> {
        let page = crate::db::cleanup_recovery_page(conn, self.after, Self::PAGE_ROWS)?;
        self.after = page.next_cursor;
        Ok(page)
    }
}

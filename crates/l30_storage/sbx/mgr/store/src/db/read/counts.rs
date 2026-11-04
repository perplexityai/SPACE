//! Node-wide counts, each computed in SQL so a sampler's memory does not
//! grow with the number of rows.

use crate::auto_lifecycle_delete_receipts::{
    AutoLifecycleDeleteBlockedStats, AutoLifecycleDeleteOutboxStats,
};
use crate::db::{Connection, Result, SandboxLifecycleCount};

/// Only the store defines these; callers pick one, they do not add their own.
mod sealed {
    pub trait Sealed {}
    impl Sealed for super::ByLifecycle {}
    impl Sealed for super::Resident {}
    impl Sealed for super::MalformedSuspendedUpdatedAt {}
    impl Sealed for super::DeleteOutbox {}
    impl Sealed for super::DeleteBlocked {}
}

/// A count [`counts`] can compute, and what it returns.
pub trait Count: sealed::Sealed {
    type Output;

    fn count(self, conn: &Connection) -> Result<Self::Output>;
}

/// Compute `count`.
pub fn counts<C: Count>(conn: &Connection, count: C) -> Result<C::Output> {
    count.count(conn)
}

/// Rows per lifecycle state. A state label this binary does not know fails
/// the count rather than being dropped from it.
#[derive(Clone, Copy, Debug)]
pub struct ByLifecycle;

impl Count for ByLifecycle {
    type Output = Vec<SandboxLifecycleCount>;

    fn count(self, conn: &Connection) -> Result<Self::Output> {
        crate::db::sandbox_lifecycle_counts(conn)
    }
}

/// Every `sandboxes` row, whatever its state: what sizes node-ctl's census.
#[derive(Clone, Copy, Debug)]
pub struct Resident;

impl Count for Resident {
    type Output = usize;

    fn count(self, conn: &Connection) -> Result<Self::Output> {
        crate::db::eviction_scan::count_resident_rows(conn)
    }
}

/// Suspended rows whose `updated_at` is not whole epoch seconds, which the
/// eviction scans therefore cannot consider.
#[derive(Clone, Copy, Debug)]
pub struct MalformedSuspendedUpdatedAt;

impl Count for MalformedSuspendedUpdatedAt {
    type Output = usize;

    fn count(self, conn: &Connection) -> Result<Self::Output> {
        crate::db::eviction_scan::count_malformed_suspended_updated_at(conn)
    }
}

/// The auto-lifecycle delete outbox's pending and completed receipts.
#[derive(Clone, Copy, Debug)]
pub struct DeleteOutbox;

impl Count for DeleteOutbox {
    type Output = AutoLifecycleDeleteOutboxStats;

    fn count(self, conn: &Connection) -> Result<Self::Output> {
        crate::db::auto_lifecycle_delete_receipts::outbox_stats(conn)
    }
}

/// Rows whose auto-lifecycle delete is blocked, per block reason.
#[derive(Clone, Copy, Debug)]
pub struct DeleteBlocked;

impl Count for DeleteBlocked {
    type Output = Vec<AutoLifecycleDeleteBlockedStats>;

    fn count(self, conn: &Connection) -> Result<Self::Output> {
        crate::db::auto_lifecycle_delete_receipts::blocked_stats(conn)
    }
}

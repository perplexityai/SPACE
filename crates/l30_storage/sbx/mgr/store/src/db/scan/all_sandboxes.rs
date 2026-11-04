//! Every `sandboxes` row in one read, in one of four projections.

use crate::db::guest_health_scan::SandboxGuestHealthRow;
use crate::db::inventory_read::InventoryReadError;
use crate::db::{Connection, Error, SandboxListRow, SandboxRow};

/// Only the store defines these; callers pick one, they do not add their own.
mod sealed {
    pub trait Sealed {}
    impl Sealed for super::StrictRows {}
    impl Sealed for super::SkipUndecodableRows {}
    impl Sealed for super::RpcListRows {}
    impl Sealed for super::GuestHealthRows {}
}

/// A projection [`all_sandboxes`] can read, and what it returns.
pub trait AllSandboxes: sealed::Sealed {
    type Output;
    type Error;

    fn read(self, conn: &Connection) -> Result<Self::Output, Self::Error>;
}

/// Read every row through `projection`.
pub fn all_sandboxes<A: AllSandboxes>(
    conn: &Connection,
    projection: A,
) -> Result<A::Output, A::Error> {
    projection.read(conn)
}

/// Whole rows, oldest first; one row that does not decode fails the read.
#[derive(Clone, Copy, Debug)]
pub struct StrictRows;

impl AllSandboxes for StrictRows {
    type Output = Vec<SandboxRow>;
    type Error = Error;

    fn read(self, conn: &Connection) -> Result<Self::Output, Self::Error> {
        crate::db::list_sandboxes(conn)
    }
}

/// Whole rows, oldest first, skipping rows that do not decode. Statement
/// failures still fail the read.
#[derive(Clone, Copy, Debug)]
pub struct SkipUndecodableRows;

impl AllSandboxes for SkipUndecodableRows {
    type Output = Vec<SandboxRow>;
    type Error = Error;

    fn read(self, conn: &Connection) -> Result<Self::Output, Self::Error> {
        crate::db::list_sandboxes_for_recovery(conn)
    }
}

/// List's columns, oldest first, for the unary RPC `rpc`. The inventory
/// budget is measured, not enforced: an overrun is reported against `rpc`
/// and the complete list is still returned.
#[derive(Clone, Copy, Debug)]
pub struct RpcListRows {
    pub rpc: &'static str,
}

impl AllSandboxes for RpcListRows {
    type Output = Vec<SandboxListRow>;
    type Error = InventoryReadError;

    fn read(self, conn: &Connection) -> Result<Self::Output, Self::Error> {
        crate::db::inventory_read::list(conn, self.rpc)
    }
}

/// The id, state and template hash the watchdog's guest-health pass reads.
/// A state label this binary does not know fails the read.
#[derive(Clone, Copy, Debug)]
pub struct GuestHealthRows;

impl AllSandboxes for GuestHealthRows {
    type Output = Vec<SandboxGuestHealthRow>;
    type Error = Error;

    fn read(self, conn: &Connection) -> Result<Self::Output, Self::Error> {
        crate::db::guest_health_scan::list_guest_health_rows(conn)
    }
}

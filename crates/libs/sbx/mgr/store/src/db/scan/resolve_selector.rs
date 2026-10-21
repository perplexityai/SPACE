//! A sandbox id or unambiguous id prefix, resolved against every row.

use crate::db::inventory_read::InventoryReadError;
use crate::db::{Connection, Error, SandboxRow};

/// Only the store defines these; callers pick one, they do not add their own.
mod sealed {
    pub trait Sealed {}
    impl Sealed for super::RpcSelector {}
    impl Sealed for super::InventorySelector {}
}

/// A resolution [`resolve_selector`] can perform, and what it returns.
pub trait ResolveSelector: sealed::Sealed {
    type Output;
    type Error;

    fn resolve(self, conn: &Connection, selector: &str) -> Result<Self::Output, Self::Error>;
}

/// Resolve `selector` through `resolution`.
pub fn resolve_selector<R: ResolveSelector>(
    conn: &Connection,
    resolution: R,
    selector: &str,
) -> Result<R::Output, R::Error> {
    resolution.resolve(conn, selector)
}

/// The ids of at most two matching rows, in id order, decoding nothing else:
/// two prove the selector ambiguous. The caller words its own refusals.
#[derive(Clone, Copy, Debug)]
pub struct RpcSelector;

impl ResolveSelector for RpcSelector {
    type Output = Vec<String>;
    type Error = Error;

    fn resolve(self, conn: &Connection, selector: &str) -> Result<Self::Output, Self::Error> {
        crate::db::find_by_prefix(conn, selector)
    }
}

/// The matching row, an exact id first. The exact-id read and every id the
/// prefix scan examines are charged to one inventory budget of `MAX_ITEMS`
/// (4096) items and `MAX_RAW_BYTES` (16 MiB) of raw bytes; a second match is
/// `InventoryReadError::Ambiguous`. The unique match is then read with a fresh
/// budget of the same size.
#[derive(Clone, Copy, Debug)]
pub struct InventorySelector;

impl ResolveSelector for InventorySelector {
    type Output = Option<SandboxRow>;
    type Error = InventoryReadError;

    fn resolve(self, conn: &Connection, selector: &str) -> Result<Self::Output, Self::Error> {
        crate::db::inventory_read::lookup(conn, selector)
    }
}

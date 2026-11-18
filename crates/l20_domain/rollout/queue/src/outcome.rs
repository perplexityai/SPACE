//! What an item's outcome, or the lapse of its lease, does to it.
//!
//! Each is one conditional update the store writes, so there are no steps to
//! order: an outcome lands only on an item in flight under the token that
//! leased it, whether or not the lease has lapsed on the database clock, and
//! clears the token and expiry. An expired lease that hasn't been re-driven
//! still records: nobody else holds the item, and refusing the outcome of
//! finished work would only make a later lease redo it. A store that must
//! know a lease is live before it acts on an item checks that itself, as
//! [`crate::renew`] does. Re-driving a lapsed lease clears it the same way,
//! with [`LEASE_EXPIRED`] as the item's error.

/// The error of an item whose lease lapsed before an outcome was recorded.
pub const LEASE_EXPIRED: &str = "the lease expired before an outcome was recorded";

/// What re-driving lapsed leases did. An item that has been leased the
/// caller's maximum number of times fails; any other goes back to pending
/// for another lease.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RedriveReport {
    /// Items returned to pending for another lease.
    pub requeued: u64,
    /// Items that had used up their attempts, now failed.
    pub failed: u64,
}

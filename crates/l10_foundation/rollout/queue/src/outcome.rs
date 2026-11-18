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

use crate::ItemState;

/// How a leased item ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Outcome {
    /// Upgraded, or already on the target.
    Done,
    /// Not upgraded, and not a failure.
    Skipped,
    /// Failed; counts against the failure budget.
    Failed,
    /// Not upgraded this time; back to pending for a later lease. The lease
    /// counts as an attempt.
    Retry,
    /// Not attempted; back to pending, and the lease doesn't count as an
    /// attempt.
    Release,
}

impl Outcome {
    /// The state the item goes to.
    pub const fn state(self) -> ItemState {
        match self {
            Self::Done => ItemState::Done,
            Self::Skipped => ItemState::Skipped,
            Self::Failed => ItemState::Failed,
            Self::Retry | Self::Release => ItemState::Pending,
        }
    }

    /// Attempts to give back, never below zero: a release undoes its
    /// lease's.
    pub const fn refunded_attempts(self) -> u64 {
        match self {
            Self::Release => 1,
            Self::Done | Self::Skipped | Self::Failed | Self::Retry => 0,
        }
    }
}

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

//! The mechanics of a rollout queue: a header paces its items out under
//! leases, takes each item's outcome under its lease, and changes phase on
//! its own (at its failure budget, or once drained) or at an operator's
//! request.
//!
//! The store that owns a queue's header and item tables owns every statement
//! on them. Each verb here is a sequence of steps, each a method of a trait
//! the store implements on a transaction it opened ([`ClaimStore`],
//! [`RenewStore`], [`PhaseStore`], [`OperatorStore`]), so no table name or SQL
//! leaves the store. This crate owns the order of the steps and the rules
//! between them: the lease bounds, canary and failure-budget pacing, the
//! per-node and cluster caps, the lease token, and which phase changes are
//! allowed. The store decides what a step reads (which items are
//! candidates, what holds a subject) and ends the transaction.
//!
//! Each step names the lock it takes. Taken in the order a verb runs them,
//! the locks follow one order: the header row, then the store's lease
//! handoff lock, then item rows.

use std::time::Duration;

mod lease;
mod operator;
mod outcome;
mod phase;

pub use lease::{Candidates, ClaimStore, Pacing, RenewStore, claim, renew};
pub use operator::{Changed, Locked, OperatorStore, Refusal, abort, pause, resume};
pub use outcome::{LEASE_EXPIRED, Outcome, RedriveReport};
pub use phase::{Drain, Leasing, PhaseStore, complete_if_drained, drained, pause_if_over_budget};

#[cfg(test)]
mod tests;

/// The longest lease any verb grants. A live lease blocks the down migration,
/// so a lease must lapse within an operator's patience.
pub const MAX_LEASE: Duration = Duration::from_secs(60 * 60);

/// The lease a verb grants for a requested `lease`: clamped to 1 second
/// through [`MAX_LEASE`]. A worker that holds the lease measures its own
/// deadline with the same clamp, from before it sends the call that grants
/// or renews the lease, so its deadline is never later than the database's,
/// which runs from when the database writes it.
pub fn clamp_lease(lease: Duration) -> Duration {
    lease.clamp(Duration::from_secs(1), MAX_LEASE)
}

/// Where a header is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum HeaderPhase {
    /// Upgrading one item, then waiting for an operator's resume.
    Canary,
    /// Upgrading items under the header's pacing.
    Rolling,
    /// Stopped by an operator or by the failure budget; nothing new is
    /// leased.
    Paused,
    Completed,
    Aborted,
}

impl HeaderPhase {
    /// Whether items may be leased in this phase.
    pub const fn leases(self) -> bool {
        matches!(self, Self::Canary | Self::Rolling)
    }

    /// Whether the header is over: it never changes phase again.
    pub const fn finished(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted)
    }
}

/// Where an item is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ItemState {
    Pending,
    InFlight,
    Done,
    Failed,
    Skipped,
}

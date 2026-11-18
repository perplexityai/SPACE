//! Phase changes a header makes on its own: pausing at its failure budget
//! and completing once drained.
//!
//! Each locks the header row before it reads the items, in a step of its
//! own. A single conditional update would read the items from a snapshot
//! taken before it waited for the lock: Postgres rechecks the header row it
//! waited on, but not the items. That holds at `READ COMMITTED`, where each
//! statement takes a new snapshot. Each moves a header only from the phase it
//! locked it in, so a phase an operator wrote first wins.

use crate::FailedItems;

/// What a phase change read of its header under the header's lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Leasing {
    /// The header is in canary; otherwise it's rolling.
    pub canary: bool,
    /// Failed items at which the header pauses.
    pub failure_budget: u64,
}

/// A header's items, as completion weighs them, from one snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Drain {
    /// Some item is pending or in flight.
    pub open: bool,
    pub failed: u64,
    /// Some item is done.
    pub any_done: bool,
}

/// The steps of [`pause_if_over_budget`] and [`complete_if_drained`], each a
/// statement on the store's open transaction.
pub trait PhaseStore: FailedItems {
    /// Lock the header row exclusively, if it leases (canary or rolling).
    /// `None` changes nothing.
    fn lock_leasing(&mut self)
    -> impl Future<Output = Result<Option<Leasing>, Self::Error>> + Send;

    /// The items, from one snapshot.
    fn drain(&mut self) -> impl Future<Output = Result<Drain, Self::Error>> + Send;

    /// Write the header paused, as no operator's change.
    fn write_paused(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Complete the drained header if the store's own evidence allows it:
    /// write it completed, finished on the database clock, as no operator's
    /// change. Runs under the header lock, after [`drained`], so evidence
    /// read here holds until the store's transaction ends. `false` when the
    /// store refuses, having written nothing.
    fn write_completed(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Pause the header if it leases and its failed items have reached its
/// failure budget. `true` when this call paused it.
pub async fn pause_if_over_budget<S: PhaseStore>(store: &mut S) -> Result<bool, S::Error> {
    let Some(leasing) = store.lock_leasing().await? else {
        return Ok(false);
    };
    if store.failed().await? < leasing.failure_budget {
        return Ok(false);
    }
    store.write_paused().await?;
    Ok(true)
}

/// Whether a leasing header with `drain` may complete: nothing is pending or
/// in flight, its failed items haven't reached its budget (a header over its
/// budget is the operator's to look at, so it pauses rather than completes),
/// and, in canary, no item is done (a canary with a done item waits for the
/// operator). Skipped items, and failed ones under the budget, don't hold it
/// open.
pub const fn drained(leasing: Leasing, drain: Drain) -> bool {
    !drain.open && drain.failed < leasing.failure_budget && !(leasing.canary && drain.any_done)
}

/// Complete the header if it leases, is [`drained`], and the store allows it
/// ([`PhaseStore::write_completed`]). The caller decides when no more items
/// will arrive. `true` when this call completed it. Nothing commits here:
/// the transaction is the store's.
pub async fn complete_if_drained<S: PhaseStore>(store: &mut S) -> Result<bool, S::Error> {
    let Some(leasing) = store.lock_leasing().await? else {
        return Ok(false);
    };
    if !drained(leasing, store.drain().await?) {
        return Ok(false);
    }
    store.write_completed().await
}

//! Operator phase changes: pause, resume and abort.
//!
//! Each locks the header row, refuses a transition its phase doesn't allow,
//! then compares the generation the caller expects, and writes the phase. A
//! refused transition, and a resume refused at the failure budget, are
//! reported before a stale generation, so a caller learns what the header is
//! now. A refusal writes nothing.
//!
//! A resume goes to rolling once any item is done, and otherwise back to
//! canary, whichever phase it started in: nothing has shown the rollout works
//! yet. It refuses while the failed items are at or over the budget, which
//! would pause the header again, unless the caller asks to retry them.

use crate::HeaderPhase;

#[cfg(test)]
mod tests;

/// What an operator change read of its header under the header's lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Locked {
    pub phase: HeaderPhase,
    pub generation: u64,
    pub failure_budget: u64,
}

/// What a resume left.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Resumed {
    /// Rolling, or canary while no item is done.
    pub phase: HeaderPhase,
    /// The generation the store's write returned.
    pub generation: u64,
    /// Failed items returned to pending.
    pub retried: u64,
}

/// What an abort left.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Aborted {
    /// The generation the store's write returned.
    pub generation: u64,
    /// In-flight items left to drain under their leases.
    pub draining: u64,
}

/// Why an operator change wrote nothing. Each verb returns it as the
/// store's error ([`OperatorStore::Error`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    NotFound,
    Finished(HeaderPhase),
    AlreadyPaused,
    NotResumable(HeaderPhase),
    AtFailureBudget { failed: u64, failure_budget: u64 },
    Stale { generation: u64, expected: u64 },
}

/// The steps of [`pause`], [`resume`] and [`abort`], each a statement on the
/// store's open transaction.
pub trait OperatorStore {
    /// A step's error, or the verb's refusal.
    type Error: From<Refusal>;

    /// Lock the header row exclusively, whatever its phase. `None` when
    /// there's no such header.
    fn lock(&mut self) -> impl Future<Output = Result<Option<Locked>, Self::Error>> + Send;

    /// Whether any item is done, and how many have failed, from one
    /// snapshot.
    fn progress(&mut self) -> impl Future<Output = Result<(bool, u64), Self::Error>> + Send;

    /// Return every failed item to pending with its attempts reset, keeping
    /// its error until a new outcome replaces it. How many it returned.
    fn retry_failed(&mut self) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// How many items are in flight.
    fn in_flight(&mut self) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Write `phase` as the operator's change, with the finish time on the
    /// database clock when it's [`HeaderPhase::finished`], and return the
    /// generation it left.
    fn write_phase(
        &mut self,
        phase: HeaderPhase,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}

fn check_generation(expected: Option<u64>, generation: u64) -> Result<(), Refusal> {
    match expected {
        Some(expected) if expected != generation => Err(Refusal::Stale {
            generation,
            expected,
        }),
        _ => Ok(()),
    }
}

/// Pause a canary or rolling header: nothing new is leased, and items in
/// flight finish under their leases. With `expected_generation`, only at that
/// generation. Returns the generation the pause left.
pub async fn pause<S: OperatorStore>(
    store: &mut S,
    expected_generation: Option<u64>,
) -> Result<u64, S::Error> {
    let locked = store.lock().await?.ok_or(Refusal::NotFound)?;
    match locked.phase {
        HeaderPhase::Completed | HeaderPhase::Aborted => {
            return Err(Refusal::Finished(locked.phase).into());
        }
        HeaderPhase::Paused => return Err(Refusal::AlreadyPaused.into()),
        HeaderPhase::Canary | HeaderPhase::Rolling => {}
    }
    check_generation(expected_generation, locked.generation)?;
    store.write_phase(HeaderPhase::Paused).await
}

/// Resume a paused header, or move a canary whose item is done on to
/// rolling; see the module docs. With `retry_failed`, failed items go back
/// to pending first.
pub async fn resume<S: OperatorStore>(
    store: &mut S,
    expected_generation: Option<u64>,
    retry_failed: bool,
) -> Result<Resumed, S::Error> {
    let locked = store.lock().await?.ok_or(Refusal::NotFound)?;
    if locked.phase.finished() {
        return Err(Refusal::Finished(locked.phase).into());
    }
    // Outcomes take no header lock, so a failure recorded after this read
    // can still reach the budget once the header resumes; the budget pause
    // then pauses it again.
    let (any_done, failed) = store.progress().await?;
    match locked.phase {
        HeaderPhase::Paused => {}
        HeaderPhase::Canary if any_done => {}
        phase => return Err(Refusal::NotResumable(phase).into()),
    }
    if failed >= locked.failure_budget && !retry_failed {
        return Err(Refusal::AtFailureBudget {
            failed,
            failure_budget: locked.failure_budget,
        }
        .into());
    }
    check_generation(expected_generation, locked.generation)?;
    let retried = if retry_failed {
        store.retry_failed().await?
    } else {
        0
    };
    let phase = if any_done {
        HeaderPhase::Rolling
    } else {
        HeaderPhase::Canary
    };
    let generation = store.write_phase(phase).await?;
    Ok(Resumed {
        phase,
        generation,
        retried,
    })
}

/// Abort an unfinished header: aborted is final. Items in flight keep their
/// leases, so each subject keeps one worker until its lease lapses.
pub async fn abort<S: OperatorStore>(
    store: &mut S,
    expected_generation: Option<u64>,
) -> Result<Aborted, S::Error> {
    let locked = store.lock().await?.ok_or(Refusal::NotFound)?;
    if locked.phase.finished() {
        return Err(Refusal::Finished(locked.phase).into());
    }
    check_generation(expected_generation, locked.generation)?;
    // Under the header lock, which claims take first, so no lease is
    // granted between this count and the phase write.
    let draining = store.in_flight().await?;
    let generation = store.write_phase(HeaderPhase::Aborted).await?;
    Ok(Aborted {
        generation,
        draining,
    })
}

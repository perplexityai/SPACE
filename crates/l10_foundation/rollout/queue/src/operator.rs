//! Operator phase changes: pause, resume and abort.
//!
//! Each locks the header row, refuses a transition its phase doesn't allow,
//! then compares the generation the caller expects, and writes the phase. A
//! refused transition, and a resume refused at the failure budget, are
//! reported before a stale generation, so a caller learns what the header is
//! now. A refusal writes nothing.
//!
//! A resume goes to rolling once any item is done, and otherwise back to
//! canary, whichever phase it started in: nothing has shown the upgrade works
//! yet. It refuses while the failed items are at or over the budget, which
//! would pause the header again, unless the caller asks to retry them.

use crate::HeaderPhase;

/// What an operator change read of its header under the header's lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Locked {
    pub phase: HeaderPhase,
    pub generation: u64,
    pub failure_budget: u64,
}

/// What an operator change left.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Changed {
    pub phase: HeaderPhase,
    /// The generation the store's write returned.
    pub generation: u64,
    /// Failed items a resume returned to pending.
    pub retried: u64,
    /// In-flight items an abort left to drain under their leases.
    pub draining: u64,
}

/// Why an operator change wrote nothing.
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
    type Error;

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
/// generation.
pub async fn pause<S: OperatorStore>(
    store: &mut S,
    expected_generation: Option<u64>,
) -> Result<Result<Changed, Refusal>, S::Error> {
    let Some(locked) = store.lock().await? else {
        return Ok(Err(Refusal::NotFound));
    };
    match locked.phase {
        HeaderPhase::Completed | HeaderPhase::Aborted => {
            return Ok(Err(Refusal::Finished(locked.phase)));
        }
        HeaderPhase::Paused => return Ok(Err(Refusal::AlreadyPaused)),
        HeaderPhase::Canary | HeaderPhase::Rolling => {}
    }
    if let Err(refusal) = check_generation(expected_generation, locked.generation) {
        return Ok(Err(refusal));
    }
    let generation = store.write_phase(HeaderPhase::Paused).await?;
    Ok(Ok(Changed {
        phase: HeaderPhase::Paused,
        generation,
        retried: 0,
        draining: 0,
    }))
}

/// Resume a paused header, or move a canary whose item is done on to
/// rolling; see the module docs. With `retry_failed`, failed items go back
/// to pending first.
pub async fn resume<S: OperatorStore>(
    store: &mut S,
    expected_generation: Option<u64>,
    retry_failed: bool,
) -> Result<Result<Changed, Refusal>, S::Error> {
    let Some(locked) = store.lock().await? else {
        return Ok(Err(Refusal::NotFound));
    };
    if locked.phase.finished() {
        return Ok(Err(Refusal::Finished(locked.phase)));
    }
    // Outcomes take no header lock: a failure recorded after this read can
    // still reach the budget once the header resumes (the budget pause then
    // pauses it again), and an item finishing done after a `false` read
    // leaves the resumed canary waiting at the canary gate until an operator
    // inspects that outcome and resumes to rolling.
    let (any_done, failed) = store.progress().await?;
    match locked.phase {
        HeaderPhase::Paused => {}
        HeaderPhase::Canary if any_done => {}
        phase => return Ok(Err(Refusal::NotResumable(phase))),
    }
    if failed >= locked.failure_budget && !retry_failed {
        return Ok(Err(Refusal::AtFailureBudget {
            failed,
            failure_budget: locked.failure_budget,
        }));
    }
    if let Err(refusal) = check_generation(expected_generation, locked.generation) {
        return Ok(Err(refusal));
    }
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
    Ok(Ok(Changed {
        phase,
        generation,
        retried,
        draining: 0,
    }))
}

/// Abort an unfinished header: aborted is final. Items in flight keep their
/// leases, so each subject keeps one worker until its lease lapses.
pub async fn abort<S: OperatorStore>(
    store: &mut S,
    expected_generation: Option<u64>,
) -> Result<Result<Changed, Refusal>, S::Error> {
    let Some(locked) = store.lock().await? else {
        return Ok(Err(Refusal::NotFound));
    };
    if locked.phase.finished() {
        return Ok(Err(Refusal::Finished(locked.phase)));
    }
    if let Err(refusal) = check_generation(expected_generation, locked.generation) {
        return Ok(Err(refusal));
    }
    // Under the header lock, which claims take first, so no lease is
    // granted between this count and the phase write. An outcome can still
    // land in between, since outcomes don't take the header lock;
    // `draining` is the count as read here.
    let draining = store.in_flight().await?;
    let generation = store.write_phase(HeaderPhase::Aborted).await?;
    Ok(Ok(Changed {
        phase: HeaderPhase::Aborted,
        generation,
        retried: 0,
        draining,
    }))
}

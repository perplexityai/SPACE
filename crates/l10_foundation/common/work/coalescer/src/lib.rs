//! Keyed coalescing of concurrent duplicate work.
//!
//! Many callers concurrently need the *same* unit of work done for the same
//! key — the canonical case is hundreds of connect requests all needing one
//! suspended sandbox woken. Without coalescing, every caller independently
//! executes the work's entry steps, and if the work's mutual exclusion lives
//! in the database (a `FOR UPDATE` claim row), every contender occupies a
//! pooled database connection while queued on that row lock. The work is
//! deduplicated; the *admission* is not, and admission cost scales with the
//! herd. A stampede of ~1,500 concurrent wakes for one sandbox saturates
//! every API replica's connection pool this way while the database itself
//! stays idle.
//!
//! [`WorkCoalescer`] collapses the herd in process memory, before any shared
//! resource is touched: the first caller for a key becomes the **owner** and
//! starts the work; every other caller **joins** and awaits the shared
//! outcome. The contract has four properties, each load-bearing:
//!
//! 1. **Joiners park, they never queue.** A joiner holds only a
//!    `watch::Receiver`; there is no path where it waits "its turn" to run
//!    the work. This is what makes the type an anti-stampede primitive
//!    rather than a mutex.
//! 2. **Work is detached from its initiator.** The work future runs on a
//!    [`TaskTracker`], not inside the owner's calling task, so a cancelled
//!    or timed-out owner never aborts work that joiners are waiting on.
//! 3. **Per-caller deadlines over shared work.** Every caller (owner
//!    included) awaits the outcome under its own deadline. Abandoning a
//!    wait never abandons the work.
//! 4. **Failure is a decision point, not a broadcast verdict.** The entry is
//!    removed *before* the outcome publishes, so after a failure the next
//!    `join_or_start` starts a fresh run — exactly one re-racer becomes the
//!    new owner — while [`WorkOutcome::Failed`] tells each waiting caller
//!    its role so it can choose to inherit the error or re-enter.
//!
//! Coalescing here is per-process. Cross-process mutual exclusion (a
//! database lease, a Redis lock) stays where it is; this type reduces the
//! contenders on that shared arbiter from one-per-request to one-per-process.
//!
//! This is a coalescer, not a limiter: it serves excess demand rather than
//! rejecting it. Admission caps (per-key semaphores, token buckets) are a
//! different primitive and compose from the outside.
//!
//! ```
//! use std::sync::Arc;
//! use pplx_space_common_work_coalescer::{WorkCoalescer, WorkOutcome};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(async {
//! let coalescer: Arc<WorkCoalescer<&'static str, u32, std::io::Error>> =
//!     Arc::new(WorkCoalescer::new());
//! let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
//!
//! let outcome = coalescer
//!     .join_or_start("wake:sandbox-1", deadline, || async { Ok(42) })
//!     .await
//!     .expect("work published before the deadline");
//!
//! match outcome {
//!     WorkOutcome::Owned(value) => assert_eq!(value, 42), // this caller ran the work
//!     WorkOutcome::Joined(value) => assert_eq!(value, 42), // rode along on a live run
//!     WorkOutcome::Failed { .. } => unreachable!(),
//! }
//! # });
//! ```

use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::task::TaskTracker;
use tracing::Instrument;

/// Shared outcome slot for one run: `None` until the run publishes.
type OutcomeSlot<V, E> = Option<Result<V, Arc<E>>>;

/// One live run for a key. `run_id` gives the entry an identity so cleanup
/// paths (the run's drop guard, a joiner that observed an abandoned channel)
/// only ever remove *their* generation, never a successor that reused the key.
struct LiveRun<V, E> {
    run_id: u64,
    span: tracing::Span,
    receiver: watch::Receiver<OutcomeSlot<V, E>>,
}

/// The caller's relationship to the run whose outcome it awaited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// This caller started the run.
    Owner,
    /// This caller joined a run another caller started.
    Joiner,
}

/// Outcome of [`WorkCoalescer::join_or_start`] once the run published.
#[derive(Debug, Clone)]
pub enum WorkOutcome<V, E> {
    /// This caller started the run and it succeeded.
    Owned(V),
    /// Another caller's run succeeded; this caller shared the outcome.
    Joined(V),
    /// The run failed. The entry was already removed before the failure
    /// published, so a caller that chooses to retry re-enters
    /// [`WorkCoalescer::join_or_start`] and at most one re-racer becomes the
    /// new owner. `role` lets owners and joiners choose differently — an
    /// owner typically propagates its own run's error, while a joiner
    /// typically re-enters.
    Failed { role: Role, error: Arc<E> },
}

/// Why [`WorkCoalescer::join_or_start`] returned without an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum JoinError {
    /// The caller's deadline elapsed before the run published. The run
    /// itself continues detached; later callers can still join it.
    #[error("deadline elapsed before the coalesced run published an outcome")]
    DeadlineElapsed,
    /// The run died without publishing (its task panicked). The entry is
    /// removed; the next caller starts a fresh run.
    #[error("the coalesced run was abandoned without publishing an outcome")]
    RunAbandoned,
}

/// Keyed single-runner-per-key coalescer. See the crate docs for the
/// contract; see [`WorkCoalescer::join_or_start`] for the one entry point.
pub struct WorkCoalescer<K, V, E> {
    runs: Arc<DashMap<K, LiveRun<V, E>>>,
    tracker: TaskTracker,
    next_run_id: AtomicU64,
}

impl<K, V, E> Default for WorkCoalescer<K, V, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, E> WorkCoalescer<K, V, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    /// A coalescer owning its own [`TaskTracker`] for detached runs.
    pub fn new() -> Self {
        Self::with_tracker(TaskTracker::new())
    }

    /// A coalescer spawning detached runs on the caller's tracker, so run
    /// futures participate in the owning service's graceful shutdown
    /// (`tracker.close()` + `tracker.wait()`). Shutdown-aware runs should
    /// still select on the service's `CancellationToken` inside the work
    /// future itself.
    pub fn with_tracker(tracker: TaskTracker) -> Self {
        Self {
            runs: Arc::new(DashMap::new()),
            tracker,
            next_run_id: AtomicU64::new(0),
        }
    }

    /// Join the live run for `key`, or start one.
    ///
    /// `make_work` is invoked only if this caller becomes the owner. The work
    /// future is spawned detached — no caller's cancellation or timeout
    /// aborts it. Every caller awaits the shared outcome under its own
    /// `deadline`; timing out abandons the wait, never the run.
    ///
    /// The live entry is removed immediately *before* the outcome publishes,
    /// in both the success and failure cases: callers arriving after a
    /// publish always start a fresh run, and failure handling can re-enter
    /// without a stale entry serving the dead run's error forever.
    pub async fn join_or_start<F>(
        &self,
        key: K,
        deadline: Instant,
        make_work: impl FnOnce() -> F,
    ) -> Result<WorkOutcome<V, E>, JoinError>
    where
        F: Future<Output = Result<V, E>> + Send + 'static,
    {
        let wait_span =
            tracing::info_span!("work_coalescer.wait", coalesce.role = tracing::field::Empty);
        let mut owned_run = None;
        let (role, mut receiver) = match self.runs.entry(key.clone()) {
            Entry::Occupied(live) => {
                // A joiner keeps its own request parent; the link identifies
                // the detached work even when another request started it.
                wait_span.follows_from(live.get().span.id());
                (Role::Joiner, live.get().receiver.clone())
            }
            Entry::Vacant(slot) => {
                // `make_work` runs under this key's map shard guard, so it
                // must stay a cheap future constructor. Building it before
                // the insert means a panicking `make_work` cannot leave a
                // driverless entry behind.
                let span = tracing::info_span!("work_coalescer.run");
                let work = span.in_scope(make_work);
                let run_id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
                let (sender, receiver) = watch::channel(None);
                slot.insert(LiveRun {
                    run_id,
                    span: span.clone(),
                    receiver: receiver.clone(),
                });
                owned_run = Some((work, run_id, sender, span));
                (Role::Owner, receiver)
            }
        };
        // Spawned after the shard guard above drops, so the run's own
        // retirement (which locks the same shard) can never contend with its
        // insertion. There is no await point between insert and spawn: from
        // this task's perspective the entry is never driverless, and other
        // threads that observe it early just wait on the watch as usual.
        if let Some((work, run_id, sender, span)) = owned_run {
            // The retirement guard is built here and moved into the run
            // future, not constructed inside its body: a spawned future can
            // be dropped before its first poll (runtime shutdown), and a
            // guard that only exists once the body runs would leave the key
            // permanently occupied by a closed channel.
            let retire = RetireRunOnDrop {
                runs: Arc::clone(&self.runs),
                key,
                run_id,
            };
            self.tracker
                .spawn(drive_run(retire, sender, work).instrument(span));
        }

        wait_span.record(
            "coalesce.role",
            match role {
                Role::Owner => "owner",
                Role::Joiner => "joiner",
            },
        );
        let published = tokio::time::timeout_at(
            deadline,
            receiver.wait_for(|slot: &OutcomeSlot<V, E>| slot.is_some()),
        )
        .instrument(wait_span)
        .await;
        match published {
            Err(_elapsed) => Err(JoinError::DeadlineElapsed),
            // The sender dropped without publishing: the run's task died
            // (panicked, or was dropped unpolled at runtime shutdown). Its
            // drop guard already removed the entry; report the abandonment
            // and let the caller start over.
            Ok(Err(_sender_dropped)) => Err(JoinError::RunAbandoned),
            Ok(Ok(slot)) => match slot.clone() {
                Some(Ok(value)) if role == Role::Owner => Ok(WorkOutcome::Owned(value)),
                Some(Ok(value)) => Ok(WorkOutcome::Joined(value)),
                Some(Err(error)) => Ok(WorkOutcome::Failed { role, error }),
                // Unreachable: `wait_for(is_some)` only yields published
                // slots. Mapped to abandonment rather than a panic so a
                // future regression degrades to a retryable error.
                None => Err(JoinError::RunAbandoned),
            },
        }
    }

    /// Number of live (unpublished) runs. Intended for metrics and tests.
    pub fn live_runs(&self) -> usize {
        self.runs.len()
    }
}

/// Drive one run to completion: await the work, retire the entry, publish.
///
/// Entry retirement is owned by `retire`, a drop guard the caller constructs
/// *before* spawning this future, so the map is cleared on every exit — a
/// panicking work future, and equally a run future dropped before its first
/// poll, whose body never executes. Waiters observe the dropped sender as
/// [`JoinError::RunAbandoned`] and the key is free for a fresh run.
async fn drive_run<K, V, E>(
    retire: RetireRunOnDrop<K, V, E>,
    sender: watch::Sender<OutcomeSlot<V, E>>,
    work: impl Future<Output = Result<V, E>>,
) where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    let outcome = work.await.map_err(Arc::new);
    // Retire-then-publish: once an outcome is observable, the key must
    // already accept a fresh run, or failure retries would rejoin the dead
    // run instead of re-racing.
    drop(retire);
    // Send fails only when every waiter is gone, which is fine: the run's
    // effects are the work's own responsibility, not the notification's.
    let _ = sender.send(Some(outcome));
}

/// Removes exactly this run's entry — `run_id` guards against removing a
/// successor run that reused the key after this run already retired.
struct RetireRunOnDrop<K, V, E>
where
    K: Eq + Hash,
{
    runs: Arc<DashMap<K, LiveRun<V, E>>>,
    key: K,
    run_id: u64,
}

impl<K, V, E> Drop for RetireRunOnDrop<K, V, E>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        self.runs
            .remove_if(&self.key, |_, live| live.run_id == self.run_id);
    }
}

#[cfg(test)]
mod tests;

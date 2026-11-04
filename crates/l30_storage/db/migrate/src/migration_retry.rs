//! Bounded retry for a control-plane Postgres migration run.
//!
//! A migration that takes `ACCESS EXCLUSIVE` on a live table bounds its own
//! wait with a short `SET LOCAL lock_timeout` so a queued `ALTER` cannot park
//! every writer behind itself. That bound is deliberate and stays as
//! authored — but on its own it turns any transient lock holder into a failed
//! deploy.
//!
//! A cascading delete can hold a lock longer than a migration's per-attempt
//! deadline. A migration job that runs before each deployment then blocks
//! later deployments until the pending migration succeeds.
//!
//! Retrying is what absorbs the contention, not a longer per-attempt wait.
//! Only a transactional migration failing its own statement with `55P03`
//! (`lock_not_available`) retries; a syntax error, a constraint violation, or
//! an out-of-order version still fails on the first attempt, and so does
//! anything that could have left a file half-applied.

use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use sqlx::migrate::{MigrateError, Migrator};
use sqlx::{Connection as _, PgPool};
use tracing::{info, warn};

use crate::sqlstate;

/// SQLSTATE Postgres raises when a statement gives up waiting for a lock:
/// `lock_timeout` fired, or an explicit `NOWAIT` found the lock held.
const LOCK_NOT_AVAILABLE: &str = "55P03";

/// `activeDeadlineSeconds` the migration Job runs under when it does not say:
/// the default for `--job-deadline-seconds`,
/// which the Job's chart sets from the same value it gives the Job spec, since
/// Kubernetes does not expose the deadline to the container. The retry window
/// is sized against it so a Job killed by its deadline is not one that spent a
/// meaningful share of it retrying.
pub const MIGRATION_JOB_ACTIVE_DEADLINE: Duration = Duration::from_secs(1800);

/// `space_migrator`'s role-level `lock_timeout`, which `bootstrap-roles`
/// defaults to `5min` (`--migrator-lock-timeout`) and `assume_migrator_role`
/// replays onto the `SET ROLE` path. The bound a migration inherits when it
/// sets none of its own: the worst case one attempt can cost, and so the cost
/// a retry must have room for.
pub(crate) const INHERITED_LOCK_TIMEOUT: Duration = Duration::from_secs(300);

/// Retry schedule for a migration run that loses a lock race.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LockRetryPolicy {
    /// Total attempts, counting the first.
    attempts: NonZeroU32,
    /// Delay after the first failure; doubles per retry up to `max_backoff`.
    initial_backoff: Duration,
    /// Ceiling the doubling backoff saturates at. At `DEFAULT` the schedule
    /// reaches it on the last backoff, so it only starts clamping if the
    /// attempt count grows.
    max_backoff: Duration,
    /// Ceiling on time spent *retrying*, measured from the first failure. No
    /// further attempt starts once retrying has cost this much.
    max_total_wait: Duration,
}

impl LockRetryPolicy {
    /// Five attempts spaced 1s, 2s, 4s, 8s, inside a five-minute window.
    ///
    /// Two bounds rather than one, because an attempt's cost is not this
    /// module's to choose. `Migrator::run` re-runs *every* pending migration,
    /// so one attempt costs the sum of their lock waits, not one
    /// `lock_timeout` — and most bundled migrations set no in-file bound at
    /// all, inheriting the session's. That is `space_migrator`'s role-level
    /// `lock_timeout`, which `bootstrap-roles` defaults to five minutes and
    /// exposes as `--migrator-lock-timeout`.
    ///
    /// At that bound the attempt budget alone would run one migration to
    /// ≈1,515s (5 × 300s plus 15s of backoff; 1,215s of that spent retrying)
    /// — most of the Job's 1800s
    /// `activeDeadlineSeconds`, before counting the migrations that applied
    /// successfully first, and a Job killed by its deadline reports nothing
    /// about why. (Only the failing migration repeats: `run_direct` re-reads
    /// the applied set each attempt and skips committed versions, and anything
    /// ordered after the failure never runs.) The window is set to the same
    /// five minutes, so retrying never spends longer than one inherited lock
    /// wait would have: at that bound it allows a single retry and then stops,
    /// ~600s instead of ~1,515s.
    ///
    /// It is measured from the first failure, not from run start, because a
    /// pass that applied ten migrations before losing a lock race on the
    /// eleventh must still get its retries — and the big batches are exactly
    /// the runs with the most contention. The cost is that there is always
    /// room for one retry, since the clock reads zero at the first failure.
    ///
    /// For the `lock_timeout = '5s'` migrations this exists for, the attempt
    /// count binds first and the window never fires: the run spans ~40s.
    pub(crate) const DEFAULT: Self = Self {
        attempts: NonZeroU32::new(5).expect("default attempt budget is non-zero"),
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(8),
        max_total_wait: Duration::from_secs(300),
    };

    /// `DEFAULT`, with the retry schedule sized against the Job's actual
    /// `activeDeadlineSeconds`. `DEFAULT`'s five-minute window is a sixth of
    /// the default deadline; a shorter Job deadline shrinks the window in the
    /// same proportion, and a longer one does not widen it, because the window
    /// is also bounded by one inherited lock wait (see `DEFAULT`).
    ///
    /// The window bounds when a retry may *start*, not how long it runs, so a
    /// retry that starts must also have room to wait out one inherited lock
    /// timeout and still leave the deadline's error visible rather than being
    /// killed mid-attempt. A retry's worst case is the first backoff plus one
    /// inherited wait; when four of those do not fit in the deadline (the same
    /// quarter share the window is sized for), retrying is disabled and the
    /// first failure is the reported one.
    pub(crate) fn for_job_deadline(deadline: Duration) -> Self {
        let retry_worst_case = Self::DEFAULT.initial_backoff + INHERITED_LOCK_TIMEOUT;
        let attempts = if deadline >= retry_worst_case * 4 {
            Self::DEFAULT.attempts
        } else {
            NonZeroU32::MIN
        };
        Self {
            attempts,
            max_total_wait: Self::DEFAULT.max_total_wait.min(deadline / 6),
            ..Self::DEFAULT
        }
    }

    /// Delay to wait after a failure that already waited `current`.
    fn next_backoff(self, current: Duration) -> Duration {
        current.saturating_mul(2).min(self.max_backoff)
    }

    /// Whether another attempt may start after `attempt` attempts have
    /// failed, `elapsed` has been spent retrying, and the next one would wait
    /// `backoff` first.
    ///
    /// Bounds when a new attempt may *start*, not how long one already in
    /// flight may run — that is the attempt's own `lock_timeout` to bound. So
    /// the retry adds at most `max_total_wait` plus one more pass on top of
    /// whatever the run would have cost anyway.
    fn may_retry(self, attempt: u32, elapsed: Duration, backoff: Duration) -> bool {
        attempt < self.attempts.get() && self.window_still_open(elapsed + backoff)
    }

    /// Whether the window still permits an attempt after `waited` spent
    /// retrying.
    ///
    /// The same rule [`Self::may_retry`] applies, minus the attempt budget, so
    /// the post-sleep re-read and the pre-sleep prediction cannot disagree.
    fn window_still_open(self, waited: Duration) -> bool {
        waited < self.max_total_wait
    }
}

/// A migration run that gave up: either it never won the lock inside the
/// policy's bounds, or it failed for a reason no retry can fix.
#[derive(Debug, thiserror::Error)]
#[error("migration run failed on attempt {attempt} of {budget}")]
pub struct MigrationRunFailed {
    /// Attempt the run gave up on.
    attempt: u32,
    /// Total attempts the policy allowed.
    budget: u32,
    #[source]
    source: MigrateError,
}

/// Apply every pending migration in `migrator`, retrying a lock timeout
/// under `policy`.
///
/// Returns the attempt the run succeeded on. Production discards it — the
/// `info!` below is what surfaces a retried run in the job log — but the
/// tests assert on it.
pub(crate) async fn run_up(
    pool: &PgPool,
    migrator: &Migrator,
    policy: LockRetryPolicy,
) -> Result<u32, MigrationRunFailed> {
    let budget = policy.attempts.get();
    let checksums = pplx_space_common_svc::sql::migration_checksums::AcceptedChecksums::embedded()
        .map_err(|source| MigrationRunFailed {
            attempt: 1,
            budget,
            source,
        })?;

    // Pin one connection for every attempt, and keep passing `&mut *conn` so
    // that stays true. sqlx takes a session-level `pg_advisory_lock` before it
    // applies anything and skips its own `unlock` on every error path, so a
    // retry landing on a *different* session would block on the lock the failed
    // attempt still holds. `lock_timeout` does bound advisory-lock waits, so
    // that ends in a `55P03` after the role-level five minutes — delivered as a
    // versionless `Execute`, which `classify` treats as fatal. Re-locking the same
    // session only increments its hold count. On success sqlx unlocks once, so
    // N attempts leave N-1 residual holds until process exit — inert while
    // `run_control_plane` returns immediately after, but a future
    // post-migration step taking the same advisory lock on a second connection
    // would deadlock on them.
    // Retain the lock between attempts: a competing migrator could otherwise
    // take it during backoff and make reacquisition fail with a non-retryable
    // `Execute`. Deployments should serialize migration
    // jobs for each database to avoid competing runners during retries.
    let mut conn = pool.acquire().await.map_err(|error| MigrationRunFailed {
        attempt: 1,
        budget,
        source: MigrateError::Execute(error),
    })?;

    let mut attempt = 1;
    let mut backoff = policy.initial_backoff;
    // Set at the first failure so the window bounds retrying rather than the
    // whole run: `Migrator::run` applies every pending migration in one call,
    // so a clock started before it would charge the retry budget for the
    // migrations that *succeeded* and deny a retry to the batch runs that need
    // it most.
    let mut retrying_since: Option<Instant> = None;
    loop {
        let error = match checksums.run("postgres", migrator, &mut *conn).await {
            Ok(()) => {
                if attempt > 1 {
                    info!(
                        attempt,
                        budget, "migrations applied after waiting out lock contention"
                    );
                }
                return Ok(attempt);
            }
            Err(error) => error,
        };

        let retrying_for = retrying_since.get_or_insert_with(Instant::now).elapsed();

        match classify(migrator, &error) {
            RetryVerdict::NonTransactional { version } => {
                warn!(
                    attempt,
                    version,
                    error = %error,
                    "lock timeout in a `-- no-transaction` migration; not retrying, \
                     because such a file can leave partial state behind (an INVALID \
                     index from a failed CONCURRENTLY build) that a re-run would adopt \
                     or collide with"
                );
            }
            RetryVerdict::Retry if policy.may_retry(attempt, retrying_for, backoff) => {
                warn!(
                    attempt,
                    budget,
                    backoff_secs = backoff.as_secs_f64(),
                    error = %error,
                    "migration lost a lock race; retrying"
                );
                // Flush before sleeping. sqlx only *queues* the failed
                // transaction's `ROLLBACK` into the write buffer — its own
                // `queue_simple_query` doc says "the next time this connection
                // is used" — so without this the server keeps the aborted
                // transaction open for the whole backoff, still holding every
                // lock the file's earlier statements took. A migration whose
                // first `ALTER` succeeded and whose second timed out would
                // then hold `ACCESS EXCLUSIVE` for seconds, which is the exact
                // harm the short `lock_timeout` exists to prevent. `ping` is
                // `write_sync` plus `wait_until_ready`, which flushes it.
                if let Err(error) = conn.ping().await {
                    warn!(
                        attempt,
                        error = %error,
                        "failed to flush the rolled-back migration transaction; \
                         retrying anyway, which will surface any broken connection"
                    );
                }

                tokio::time::sleep(backoff).await;

                // `may_retry` above predicted where the clock would be after
                // this sleep. Tokio only promises a sleep will not finish
                // *early*, so a throttled container or a starved runtime can
                // land us past the window — re-read it rather than trusting the
                // prediction, so "no further attempt starts once retrying has
                // cost this much" is true as written.
                let waited = retrying_since.map_or(Duration::ZERO, |since| since.elapsed());
                if !policy.window_still_open(waited) {
                    warn!(
                        attempt,
                        budget,
                        retrying_for_secs = waited.as_secs_f64(),
                        window_secs = policy.max_total_wait.as_secs_f64(),
                        error = %error,
                        "giving up on the migration: the backoff woke past the retry window"
                    );
                    return Err(MigrationRunFailed {
                        attempt,
                        budget,
                        source: error,
                    });
                }

                backoff = policy.next_backoff(backoff);
                attempt += 1;
                continue;
            }
            RetryVerdict::Retry => {
                let exhausted = if attempt < budget {
                    "the retry window closed with attempts to spare"
                } else {
                    "every attempt the budget allowed lost the lock race"
                };
                warn!(
                    attempt,
                    budget,
                    retrying_for_secs = retrying_for.as_secs_f64(),
                    window_secs = policy.max_total_wait.as_secs_f64(),
                    error = %error,
                    "giving up on the migration: {exhausted}"
                );
            }
            RetryVerdict::Fatal => {}
        }

        // Flush here too, for ordering rather than necessity: sqlx's
        // `PoolConnection::return_to_pool` does ping on release, but it *spawns*
        // that, so it is not synchronous with this function returning. Doing it
        // here means the aborted transaction and any `ACCESS EXCLUSIVE` the
        // failed file took are released before the caller sees the error, which
        // matters for any caller that logs the failure and keeps running.
        if let Err(flush_error) = conn.ping().await {
            warn!(error = %flush_error, "failed to flush the failed migration transaction");
        }

        return Err(MigrationRunFailed {
            attempt,
            budget,
            source: error,
        });
    }
}

/// What a failed migration run should do next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryVerdict {
    /// Not a lock timeout: no retry can fix it, so fail now.
    Fatal,
    /// A lock timeout inside a transactional migration — sqlx runs the file
    /// and its `_sqlx_migrations` row in one transaction, so the whole file
    /// rolled back and re-running it is safe.
    Retry,
    /// A lock timeout inside a `-- no-transaction` migration. sqlx runs
    /// those outside a transaction, so the file can have left partial state
    /// behind with no history row to record it.
    NonTransactional { version: i64 },
}

/// Only a transactional migration losing its own lock race is retryable.
///
/// `40P01` (`deadlock_detected`) is deliberately out of scope even though a
/// deadlocked transaction rolls back just as completely, so re-running it would
/// be equally safe. This retry was scoped to `lock_timeout` alone, and widening
/// what a deploy path retries is a decision to take deliberately rather than in
/// passing — but the gap is real: a multi-statement migration holding
/// `ACCESS EXCLUSIVE` from an earlier statement can deadlock against the
/// `DELETE FROM nodes` cascade, and `deadlock_timeout` (1s) fires below a 5s
/// `lock_timeout`, so that arrives as `40P01` and gets no retry.
///
/// Everything else is fatal, including a `55P03` that arrives as
/// `MigrateError::Execute`. That variant is tempting to retry — it covers the
/// advisory lock, the history table, and the applied-version read, none of
/// which leave partial state — but it also covers the `_sqlx_migrations`
/// insert that follows a `-- no-transaction` migration's DDL. sqlx's
/// `execute_migration` attaches the version to the DDL error only, so that
/// bookkeeping failure is indistinguishable from a pre-migration one, and
/// retrying it would re-run a file whose `CREATE INDEX CONCURRENTLY` already
/// succeeded. Losing retry coverage on the surrounding statements is the
/// cheaper half of that trade: none of them was the failure this exists for,
/// and treating them as fatal is exactly what the migrator did before.
fn classify(migrator: &Migrator, error: &MigrateError) -> RetryVerdict {
    let MigrateError::ExecuteMigration(source, version) = error else {
        return RetryVerdict::Fatal;
    };

    if !is_lock_not_available(source) {
        return RetryVerdict::Fatal;
    }

    if is_non_transactional(migrator, *version) {
        return RetryVerdict::NonTransactional { version: *version };
    }

    RetryVerdict::Retry
}

fn is_lock_not_available(error: &sqlx::Error) -> bool {
    sqlstate(error).as_deref() == Some(LOCK_NOT_AVAILABLE)
}

/// Whether the up-migration at `version` carries the `-- no-transaction`
/// marker. A reversible pair can differ, and `Migrator::run` only applies
/// up migrations, so the down half is excluded.
fn is_non_transactional(migrator: &Migrator, version: i64) -> bool {
    migrator.iter().any(|migration| {
        migration.version == version
            && !migration.migration_type.is_down_migration()
            && migration.no_tx
    })
}

#[cfg(test)]
mod tests;

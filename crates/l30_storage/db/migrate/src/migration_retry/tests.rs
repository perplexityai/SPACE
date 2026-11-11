use super::*;

use std::borrow::Cow;
use std::fmt;

use sqlx::SqlSafeStr as _;
use sqlx::error::{DatabaseError, ErrorKind};
use sqlx::migrate::{Migration, MigrationType};

/// Synthetic migration version shared with the live-Postgres child module.
/// It is used only in fabricated migrators, never to mutate `POSTGRES_MIGRATOR`.
const PROBE_VERSION: i64 = 20_260_825_055_948;

/// Minimal [`DatabaseError`] so the classifier can be exercised on an exact
/// SQLSTATE without a live server. sqlx's own `PgDatabaseError` cannot be
/// constructed outside the driver.
#[derive(Debug)]
struct FakeDatabaseError {
    code: &'static str,
}

impl fmt::Display for FakeDatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fake database error {}", self.code)
    }
}

impl std::error::Error for FakeDatabaseError {}

impl DatabaseError for FakeDatabaseError {
    fn message(&self) -> &str {
        "fake database error"
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.code))
    }

    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}

fn database_error(code: &'static str) -> sqlx::Error {
    sqlx::Error::Database(Box::new(FakeDatabaseError { code }))
}

fn test_migrator(no_tx: bool) -> Migrator {
    Migrator::with_migrations(vec![Migration::new(
        PROBE_VERSION,
        Cow::Borrowed("probe"),
        MigrationType::Simple,
        "SELECT 1".into_sql_str(),
        no_tx,
    )])
}

#[test]
fn lock_timeout_in_a_transactional_migration_retries() {
    assert_eq!(
        classify(
            &test_migrator(false),
            &MigrateError::ExecuteMigration(database_error("55P03"), PROBE_VERSION),
        ),
        RetryVerdict::Retry
    );
}

#[test]
fn a_versionless_lock_timeout_is_fatal_even_for_a_transactional_migrator() {
    // `MigrateError::Execute` covers the advisory lock, the history table, and
    // the applied-version read — but also the `_sqlx_migrations` insert that
    // follows a `-- no-transaction` migration's DDL, which sqlx reports
    // without the version. The variant cannot be told apart, so none of it
    // retries.
    assert_eq!(
        classify(
            &test_migrator(false),
            &MigrateError::Execute(database_error("55P03")),
        ),
        RetryVerdict::Fatal
    );
}

#[test]
fn a_no_transaction_migrations_bookkeeping_timeout_does_not_retry_its_ddl() {
    // The hazard the arm above exists for: sqlx's `execute_migration` runs a
    // no-transaction file's DDL, then inserts the history row, and attaches
    // the version to the DDL error only. A `55P03` on that insert means the
    // `CREATE INDEX CONCURRENTLY` may already have succeeded, so re-running
    // the file would collide with the index it built or adopt an INVALID one.
    assert_eq!(
        classify(
            &test_migrator(true),
            &MigrateError::Execute(database_error("55P03")),
        ),
        RetryVerdict::Fatal
    );
}

#[test]
fn lock_timeout_in_a_no_transaction_migration_does_not_retry() {
    assert_eq!(
        classify(
            &test_migrator(true),
            &MigrateError::ExecuteMigration(database_error("55P03"), PROBE_VERSION),
        ),
        RetryVerdict::NonTransactional {
            version: PROBE_VERSION
        }
    );
}

#[test]
fn a_down_half_marked_no_transaction_does_not_block_retrying_the_up_half() {
    // `Migrator::run` only applies up migrations, and a reversible pair can
    // differ: `DROP INDEX CONCURRENTLY` down, plain `CREATE INDEX` up.
    let migrator = Migrator::with_migrations(vec![
        Migration::new(
            PROBE_VERSION,
            Cow::Borrowed("probe"),
            MigrationType::ReversibleUp,
            "SELECT 1".into_sql_str(),
            false,
        ),
        Migration::new(
            PROBE_VERSION,
            Cow::Borrowed("probe"),
            MigrationType::ReversibleDown,
            "SELECT 2".into_sql_str(),
            true,
        ),
    ]);

    assert_eq!(
        classify(
            &migrator,
            &MigrateError::ExecuteMigration(database_error("55P03"), PROBE_VERSION),
        ),
        RetryVerdict::Retry
    );
}

#[test]
fn other_sqlstates_are_fatal() {
    let migrator = test_migrator(false);
    for code in [
        "42601", // syntax_error
        "42P01", // undefined_table
        "23505", // unique_violation
        "40P01", // deadlock_detected
        "57014", // query_canceled — statement_timeout, not a lock wait
    ] {
        assert_eq!(
            classify(
                &migrator,
                &MigrateError::ExecuteMigration(database_error(code), PROBE_VERSION),
            ),
            RetryVerdict::Fatal,
            "{code} must not retry"
        );
    }
}

#[test]
fn non_database_migrate_errors_are_fatal() {
    let migrator = test_migrator(false);
    assert_eq!(
        classify(&migrator, &MigrateError::VersionMismatch(PROBE_VERSION)),
        RetryVerdict::Fatal
    );
    assert_eq!(
        classify(&migrator, &MigrateError::Dirty(PROBE_VERSION)),
        RetryVerdict::Fatal
    );
    assert_eq!(
        classify(&migrator, &MigrateError::VersionMissing(PROBE_VERSION)),
        RetryVerdict::Fatal
    );
}

#[test]
fn backoff_doubles_up_to_the_ceiling() {
    let policy = LockRetryPolicy::DEFAULT;
    assert_eq!(
        policy.next_backoff(policy.initial_backoff),
        Duration::from_secs(2)
    );
    assert_eq!(
        policy.next_backoff(Duration::from_secs(4)),
        Duration::from_secs(8)
    );
    assert_eq!(
        policy.next_backoff(Duration::from_secs(8)),
        policy.max_backoff
    );
    assert_eq!(policy.next_backoff(Duration::MAX), policy.max_backoff);
}

/// Walks the policy's own decision functions to produce the schedule it would
/// really run, given a fixed per-attempt cost. Returns the attempt it stops on
/// and the total wall clock it spans.
///
/// Calls `may_retry` and `next_backoff` rather than re-deriving their
/// arithmetic, so a change to either surfaces here instead of drifting. Only
/// the clock is modelled — and note the two clocks differ: `retrying_for`
/// starts at zero on the first failure, while `total` counts the first attempt
/// too, which is what the Job's deadline sees.
fn simulate(policy: LockRetryPolicy, per_attempt: Duration) -> (u32, Duration) {
    // Capped because `may_retry` has two terminating conditions and a caller can
    // legitimately disable one (see the no-window case below). If the other
    // regresses too, an uncapped loop would wedge the test job instead of
    // failing it.
    const RUNAWAY: u32 = 1_000;

    let mut attempt = 1;
    let mut retrying_for = Duration::ZERO;
    let mut total = per_attempt;
    let mut backoff = policy.initial_backoff;
    while policy.may_retry(attempt, retrying_for, backoff) {
        retrying_for += backoff + per_attempt;
        total += backoff + per_attempt;
        backoff = policy.next_backoff(backoff);
        attempt += 1;
        assert!(
            attempt < RUNAWAY,
            "may_retry ran {RUNAWAY} times without stopping; whichever bound the \
             caller left enabled is not terminating the loop"
        );
    }
    (attempt, total)
}

#[test]
fn may_retry_stops_at_both_bounds() {
    let policy = LockRetryPolicy::DEFAULT;
    let backoff = Duration::from_secs(1);

    assert!(policy.may_retry(1, Duration::ZERO, backoff));
    assert!(
        !policy.may_retry(policy.attempts.get(), Duration::ZERO, backoff),
        "the attempt budget is inclusive: the last attempt must not schedule another"
    );
    assert!(
        !policy.may_retry(1, policy.max_total_wait - backoff, backoff),
        "a sleep that would land exactly on the ceiling must not be taken"
    );
    assert!(policy.may_retry(
        1,
        policy.max_total_wait - backoff - Duration::from_millis(1),
        backoff
    ));
}

#[test]
fn the_window_closes_exactly_at_its_ceiling() {
    let policy = LockRetryPolicy::DEFAULT;
    // The rule `run_up` re-reads after each backoff. It can fire whenever the
    // clock moved further than `may_retry` predicted — a late sleep, but also
    // the flush round trip, which goes to a server under lock contention. This
    // covers the predicate only, not that the loop still calls it: nothing does,
    // because reaching the re-read at all requires `elapsed + backoff < window`
    // and then overrunning it, which a test would have to race.
    assert!(policy.window_still_open(Duration::ZERO));
    assert!(policy.window_still_open(policy.max_total_wait - Duration::from_millis(1)));
    assert!(!policy.window_still_open(policy.max_total_wait));
    assert!(!policy.window_still_open(policy.max_total_wait + Duration::from_millis(1)));
    // And the pre-sleep prediction must be the same rule, so the two cannot
    // disagree about a boundary.
    assert!(!policy.may_retry(1, policy.max_total_wait, Duration::ZERO));
}

#[test]
fn default_policy_outlasts_a_transient_blocker() {
    let policy = LockRetryPolicy::DEFAULT;
    // A synthetic blocker longer than either per-attempt timeout below.
    let blocker = Duration::from_secs(8);

    // The only two in-file bounds the bundled migrations actually use.
    for per_attempt in [Duration::from_secs(1), Duration::from_secs(5)] {
        let (attempt, total) = simulate(policy, per_attempt);
        assert_eq!(
            attempt,
            policy.attempts.get(),
            "at {per_attempt:?} the attempt budget should bind, not the window"
        );
        // Twice the blocker, at the *tightest* bound the corpus uses. This is
        // what pins the attempt count: at 1s per attempt a budget of four
        // spans only 11s, which would clear the blocker but not the margin.
        assert!(
            total >= blocker * 2,
            "at {per_attempt:?} the run spans only {total:?}, under twice the {blocker:?} blocker"
        );
    }
}

#[test]
fn the_window_allows_exactly_one_retry_at_the_inherited_lock_timeout() {
    let policy = LockRetryPolicy::DEFAULT;
    // Most bundled migrations set no in-file `lock_timeout` and inherit the
    // session's. The window reads zero at the first failure, so one retry is
    // always allowed; the second attempt's cost must then exhaust it. Asserting
    // exactly two is what pins the window to the inherited bound — a window 3x
    // wider would allow two more attempts here and pass a "fewer than the
    // budget" test.
    for per_attempt in [INHERITED_LOCK_TIMEOUT, INHERITED_LOCK_TIMEOUT * 2] {
        let (attempt, _) = simulate(policy, per_attempt);
        assert_eq!(
            attempt, 2,
            "at {per_attempt:?} the window must allow one retry and deny a third attempt"
        );
    }
}

#[test]
fn the_window_is_sized_to_one_inherited_lock_wait() {
    // The doc rationale is that retrying never spends longer than a single
    // inherited lock wait would have. The simulate-based tests bracket
    // `max_total_wait` to roughly (30s, 303s], which a 60s window would also
    // satisfy while contradicting that rationale — so tie the two together.
    // Both sides move independently: `--migrator-lock-timeout`'s default lives
    // in `bootstrap_roles`, this window lives here.
    assert_eq!(
        LockRetryPolicy::DEFAULT.max_total_wait,
        INHERITED_LOCK_TIMEOUT
    );
}

/// Backoff the policy spends across `attempts` attempts, from its own
/// `next_backoff` rather than a re-derived sum.
fn backoff_total(policy: LockRetryPolicy, attempts: u32) -> Duration {
    let mut backoff = policy.initial_backoff;
    let mut total = Duration::ZERO;
    for _ in 1..attempts {
        total += backoff;
        backoff = policy.next_backoff(backoff);
    }
    total
}

/// Wall clock retrying *adds* to a run, over what the same run would have cost
/// with no retry at all, when each failed attempt waits out `per_attempt`.
///
/// Only the failing migration repeats — `run_direct` re-reads the applied set
/// each attempt and skips committed versions — so the addition is one
/// `per_attempt` per retry, plus the backoffs, and does not scale with how many
/// migrations were pending.
///
/// Deliberately the *marginal* cost, not the total: a migration that wins its
/// lock immediately and then rebuilds an index is bounded by the migrator's
/// `statement_timeout` (10min by default), not by any `lock_timeout`, so no
/// policy here can bound a whole run. What it can bound is its own contribution.
fn retry_addition(policy: LockRetryPolicy, per_attempt: Duration) -> Duration {
    let (attempts, _) = simulate(policy, per_attempt);
    per_attempt * (attempts - 1) + backoff_total(policy, attempts)
}

#[test]
fn the_default_deadline_yields_the_default_policy() {
    let sized = LockRetryPolicy::for_job_deadline(MIGRATION_JOB_ACTIVE_DEADLINE);
    assert_eq!(
        sized.max_total_wait,
        LockRetryPolicy::DEFAULT.max_total_wait
    );
    assert_eq!(sized.attempts, LockRetryPolicy::DEFAULT.attempts);
}

#[test]
fn a_shorter_job_deadline_shrinks_the_window_and_a_longer_one_does_not_widen_it() {
    let shorter = LockRetryPolicy::for_job_deadline(Duration::from_secs(1500));
    assert_eq!(shorter.max_total_wait, Duration::from_secs(250));
    assert_eq!(shorter.attempts, LockRetryPolicy::DEFAULT.attempts);
    let longer = LockRetryPolicy::for_job_deadline(Duration::from_secs(7200));
    assert_eq!(
        longer.max_total_wait,
        LockRetryPolicy::DEFAULT.max_total_wait
    );
}

#[test]
fn a_deadline_too_short_for_a_retry_to_finish_disables_retrying() {
    // A 600s Job: the first attempt can wait out the inherited 300s, and a
    // retry would start inside the window (1s elapsed) and then be killed by
    // the deadline mid-attempt, hiding the error. So no retry is offered, and
    // the policy never adds anything on top of the first attempt.
    let policy = LockRetryPolicy::for_job_deadline(Duration::from_secs(600));
    assert_eq!(policy.attempts.get(), 1);
    assert_eq!(
        retry_addition(policy, INHERITED_LOCK_TIMEOUT),
        Duration::ZERO
    );
    // The threshold is four retry worst cases (backoff + inherited wait), the
    // same quarter share the window is sized for.
    let just_enough = LockRetryPolicy::for_job_deadline(Duration::from_secs(1204));
    assert_eq!(just_enough.attempts, LockRetryPolicy::DEFAULT.attempts);
    let just_short = LockRetryPolicy::for_job_deadline(Duration::from_secs(1203));
    assert_eq!(just_short.attempts.get(), 1);
}

#[test]
fn the_window_is_what_keeps_retrying_a_small_slice_of_the_job_deadline() {
    // A Job killed by `activeDeadlineSeconds` reports nothing about why, so
    // retrying must not eat a meaningful share of it. At the inherited bound the
    // window allows one retry: ~301s, a sixth of the deadline.
    let deadline = MIGRATION_JOB_ACTIVE_DEADLINE;
    let policy = LockRetryPolicy::for_job_deadline(deadline);
    let bounded = retry_addition(policy, INHERITED_LOCK_TIMEOUT);
    assert!(
        bounded * 4 <= deadline,
        "retrying adds {bounded:?} at the inherited lock timeout, more than a \
         quarter of the Job's {deadline:?} deadline"
    );

    // And the window is what buys that: with only the attempt budget to stop it
    // the same contention adds ~1,215s, four times as much. Asserting the
    // multiple rather than another share of the deadline keeps this sensitive to
    // the two inputs it can actually observe — the attempt budget and the
    // inherited timeout — since the assertion above already covers the deadline.
    let unbounded = retry_addition(
        LockRetryPolicy {
            max_total_wait: Duration::MAX,
            ..policy
        },
        INHERITED_LOCK_TIMEOUT,
    );
    assert!(
        unbounded >= bounded * 3,
        "removing the window only takes retrying from {bounded:?} to \
         {unbounded:?}, so it is no longer the thing bounding this — check what \
         else in the policy changed"
    );
}

mod postgres_lock_retry_tests;

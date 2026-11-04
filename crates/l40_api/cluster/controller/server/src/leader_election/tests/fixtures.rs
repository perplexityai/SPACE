//! Fixtures for the leader-election tests: the in-memory [`FakeStore`], the
//! config builders, and the shared assertion helpers. Split out from the test
//! matrix in `mod.rs` to keep either file under the size where a reader stops
//! being able to hold it.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::time::Duration;

use pplx_space_common_core::types::PositiveDuration;
use tokio_util::sync::CancellationToken;

use crate::leader_election::store::{LeaseError, LeaseStore};
use crate::leader_election::{Campaign, LeaderElectionConfig};

/// In-memory [`LeaseStore`] modelling only current ownership (expiry semantics
/// are covered by `store::tests`). `acquire_or_renew` succeeds iff the
/// lease is free or already held by the caller. Two injectable fault modes drive
/// the renewal-loop and liveness-budget tests: `fail_every` errors every Nth
/// call (`1` = all of them, `2` = every other one so a run of failures is never
/// longer than one, `0` = never), and `hang` makes every call block forever.
/// One counter rather than a `fail` plus a `flaky` bool, so "fail all" and "fail
/// alternate" can't be requested at once.
#[derive(Default)]
pub(super) struct FakeStore {
    holder: Mutex<Option<String>>,
    fail_every: AtomicU64,
    hang: AtomicBool,
    calls: AtomicU64,
    /// Fails exactly the next call, then clears itself. Readiness bookkeeping
    /// can misbehave for a run of exactly one — a longer run self-heals through
    /// the already-gone branch.
    fail_once: AtomicBool,
    /// Milliseconds each call takes before answering. Real lease calls are two
    /// apiserver round-trips, so a slow-but-succeeding call is the normal shape
    /// of a brownout — and it is the only way to tell whether a deadline was
    /// anchored on the instant before the call or on its response.
    latency_ms: AtomicU64,
    /// The error shape the injected failures take. Separate from *whether* a
    /// call fails so a test can change the category mid-run without touching
    /// the cadence — which is how the interleaving cases are written.
    shape: Mutex<InjectedFailure>,
}

/// The error shapes a lease call can fail with, as the loop sees them. Named for
/// the wire shape rather than for the resulting
/// [`crate::leader_election::LeaseFailure`], so a test asks for "a 429" and the
/// classifier decides what that means — a test that asked for a category
/// directly would pass even if classification were wrong.
#[derive(Clone, Copy, Default, Debug)]
pub(super) enum InjectedFailure {
    /// The call never reached the apiserver. The default because this is the
    /// failure class that consumes the liveness budget.
    #[default]
    Transport,
    /// The apiserver answered with this status.
    Status(u16),
    /// This replica's clock is unusable.
    Clock,
}

impl InjectedFailure {
    fn to_error(self) -> LeaseError {
        match self {
            Self::Transport => {
                LeaseError::Kube(kube::Error::Service("connection reset by peer".into()))
            }
            Self::Status(code) => {
                LeaseError::Kube(kube::Error::Api(Box::new(kube::core::Status {
                    code,
                    ..Default::default()
                })))
            }
            Self::Clock => LeaseError::Clock,
        }
    }
}

impl FakeStore {
    pub(super) fn holder(&self) -> Option<String> {
        self.holder.lock().expect("lock").clone()
    }

    pub(super) fn calls(&self) -> u64 {
        self.calls.load(SeqCst)
    }

    pub(super) fn set_holder(&self, who: Option<&str>) {
        *self.holder.lock().expect("lock") = who.map(str::to_string);
    }

    /// `1` fails every call, `2` every other one, `0` none.
    pub(super) fn set_fail_every(&self, n: u64) {
        self.fail_every.store(n, SeqCst);
    }

    pub(super) fn set_hang(&self, on: bool) {
        self.hang.store(on, SeqCst);
    }

    pub(super) fn set_fail_once(&self) {
        self.fail_once.store(true, SeqCst);
    }

    /// Change the shape the injected failures take. Takes effect on the next
    /// call, so a test can flip categories partway through a failure run.
    pub(super) fn set_failure_shape(&self, shape: InjectedFailure) {
        *self.shape.lock().expect("lock") = shape;
    }

    pub(super) fn set_latency(&self, latency: Duration) {
        self.latency_ms.store(
            u64::try_from(latency.as_millis()).expect("test latency fits"),
            SeqCst,
        );
    }
}

impl LeaseStore for FakeStore {
    async fn acquire_or_renew(
        &self,
        _lease_name: &str,
        identity: &str,
        _ttl: Duration,
    ) -> Result<bool, LeaseError> {
        // Counted before the hang check on purpose: a hung call is still a call,
        // and `calls()` is what proves the loop kept polling. Incrementing after
        // would leave the counter at 0 forever in hang mode.
        let call = self.calls.fetch_add(1, SeqCst);
        if self.hang.load(SeqCst) {
            std::future::pending::<()>().await;
        }
        let latency = self.latency_ms.load(SeqCst);
        if latency > 0 {
            tokio::time::sleep(Duration::from_millis(latency)).await;
        }
        if self.fail_once.swap(false, SeqCst) {
            return Err(self.shape.lock().expect("lock").to_error());
        }
        let fail_every = self.fail_every.load(SeqCst);
        if fail_every != 0 && call.is_multiple_of(fail_every) {
            return Err(self.shape.lock().expect("lock").to_error());
        }
        let mut holder = self.holder.lock().expect("lock");
        match holder.as_deref() {
            None => {
                *holder = Some(identity.to_string());
                Ok(true)
            }
            Some(current) => Ok(current == identity),
        }
    }

    async fn release(&self, _lease_name: &str, identity: &str) -> Result<(), LeaseError> {
        let mut holder = self.holder.lock().expect("lock");
        if holder.as_deref() == Some(identity) {
            *holder = None;
        }
        Ok(())
    }
}

pub(super) fn test_config(identity: &str) -> LeaderElectionConfig {
    LeaderElectionConfig {
        identity: identity.to_string(),
        enabled: true,
        lease_ttl: Duration::from_secs(30),
        renew_deadline: Duration::from_secs(15),
        renew_interval: Duration::from_millis(20),
        retry_interval: Duration::from_millis(20),
        readiness_file: None,
        // Off by default in tests so the pre-existing cases keep exercising the
        // retry-forever standby loop; the liveness cases opt in explicitly.
        lease_failure_grace: None,
    }
}

/// Config with the liveness budget on. A paused clock lets the tests exercise
/// a 600s budget without spending that wall time.
pub(super) fn budgeted_config(identity: &str, grace: Duration) -> LeaderElectionConfig {
    LeaderElectionConfig {
        retry_interval: Duration::from_secs(2),
        lease_failure_grace: PositiveDuration::new(grace),
        ..test_config(identity)
    }
}

/// Drive `campaign` to completion and assert it gave up on the lease. Shared so
/// the two fault shapes that must both spend the budget can't drift apart, and
/// so a future `Campaign` variant is handled in one place.
///
/// Bounded even though the budget is supposed to end the loop: without it a
/// regression that stops the loop giving up hangs this test instead of failing
/// it, and on a paused clock it hangs while auto-advance spins the loop forever.
/// The bound is generous in virtual time, so it costs nothing.
pub(super) async fn expect_lease_unreachable(
    store: std::sync::Arc<FakeStore>,
    config: LeaderElectionConfig,
) {
    let grace = config
        .lease_failure_grace
        .expect("this helper is for the budget-enabled cases")
        .get();
    let outcome = tokio::time::timeout(
        grace * 10,
        crate::leader_election::campaign(store, config, CancellationToken::new()),
    )
    .await
    .expect("campaign must give up once the budget is spent, not retry forever");
    match outcome {
        Campaign::LeaseUnreachable => {}
        Campaign::Elected(_) => panic!("must not be elected while every lease call fails"),
        Campaign::ShutdownBeforeElected => panic!("shutdown was never requested"),
    }
}

/// Assert `campaign` is still standing by after `window` of virtual time. On a
/// paused clock this is exact rather than a wall-clock race: auto-advance jumps
/// to each timer in turn, so the future genuinely saw `window` elapse.
///
/// "Reached no decision" alone would also pass over a loop that stopped talking
/// to the lease entirely — a ticker that never re-arms, a starved task, an
/// accidental await on a never-ready future all look like "still standing by".
/// So this also requires the loop to have kept *calling*, which is the behavior
/// the callers actually depend on.
pub(super) async fn assert_still_standing_by(
    store: std::sync::Arc<FakeStore>,
    config: LeaderElectionConfig,
    window: Duration,
) {
    let retry_interval = config.retry_interval;
    let outcome = tokio::time::timeout(
        window,
        crate::leader_election::campaign(store.clone(), config, CancellationToken::new()),
    )
    .await;
    // An explicit match rather than `assert!(outcome.is_err())`: clippy's
    // `assertions_on_result_states` rejects the assert form, and its
    // `redundant_pattern_matching` rejects `matches!(_, Err(_))`, so the two
    // lints only agree on this shape. `Campaign` isn't `Debug`, so the
    // `unwrap_err()` the first lint suggests wouldn't compile either.
    match outcome {
        Err(_elapsed) => {}
        Ok(_) => panic!("campaign must keep standing by for the whole window, got a decision"),
    }
    // Half the theoretical attempts, so tick-cadence slack can't make this
    // brittle while still failing loudly on a loop that stopped retrying.
    // Float division so a sub-second retry interval can't divide by zero.
    let floor = u64::try_from((window.div_duration_f64(retry_interval) / 2.0).trunc() as i64)
        .expect("window is positive");
    assert!(
        store.calls() >= floor,
        "campaign must keep retrying the lease across the window: {} calls, expected at least {floor}",
        store.calls()
    );
}

/// Wait for the marker file to reach `present`, or fail. The marker is written by
/// a background task, so a test that sampled at a fixed instant would be racing
/// that task rather than asserting on it. Generous in virtual time.
pub(super) async fn wait_for_marker(path: &std::path::Path, present: bool) {
    for _ in 0..2_000 {
        if path.exists() == present {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "marker at {} never became {}",
        path.display(),
        if present { "present" } else { "absent" }
    );
}

/// A unique marker path under the temp dir. No `tempfile` dev-dependency in this
/// crate, and the readiness marker is a plain path the process creates and
/// removes itself, so a per-test name plus an explicit cleanup is enough.
pub(super) fn marker_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("space-cc-readiness-{name}-{}", std::process::id()))
}

/// The resolved budget must sit in `[base, base + spread]` — flooring picks the
/// base, then [`crate::leader_election::spread_across_replicas`] adds up to a quarter on top, so
/// nothing downstream can assert equality.
pub(super) fn assert_budget_near(actual: Option<PositiveDuration>, base: Duration, what: &str) {
    let actual = actual.expect("budget must be set").get();
    // A literal quarter, not `base / GRACE_SPREAD_DIVISOR`: deriving the
    // tolerance from the constant under test would let any spread size through.
    // This helper proves the floor/ceiling landed; the spread's magnitude is
    // pinned by `the_budget_is_spread_across_replica_identities`.
    let ceiling = base + base / 4;
    assert!(
        actual >= base && actual <= ceiling,
        "{what}: {actual:?} must be within [{base:?}, {ceiling:?}]"
    );
}

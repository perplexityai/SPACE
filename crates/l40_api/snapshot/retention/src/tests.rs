//! Snapshot-retention behavior and Postgres contract tests.
use pplx_space_common_core::types::{SandboxId, SnapshotId};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
use tracing_subscriber::layer::SubscriberExt;
use uuid::Uuid;

use super::backend::{
    DeletePrefixError, DeletePrefixOutcome, StorageBackend, StorageBackendRegistry,
};
use super::*;

mod concurrency;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeSink {
    /// Stateful protocol oracle for concurrent workflows. SQL transitions are
    /// covered by the Postgres lifecycle tests below.
    enforce_protocol: bool,
    backend_visits: Mutex<Vec<SnapshotId>>,
    backend_returns: Mutex<Vec<SnapshotId>>,
    candidates: Mutex<Vec<PruneCandidate>>,
    candidate_page_size: Option<usize>,
    claimed: Mutex<Vec<PruneCandidate>>,
    fail_candidate_scan: Mutex<bool>,
    fail_claimed_scan: Mutex<bool>,
    prune_calls: Mutex<Vec<SnapshotId>>,
    claim_calls: Mutex<Vec<SnapshotId>>,
    /// Ids whose `claim_prune` should return [`ClaimOutcome::Refused`].
    refuse_claim_ids: Mutex<Vec<SnapshotId>>,
    /// Ids whose `claim_prune` should return an `sqlx` error.
    claim_err_ids: Mutex<Vec<SnapshotId>>,
    /// Ids whose `finalize_prune` should return `AlreadyGone`.
    gone_ids: Mutex<Vec<SnapshotId>>,
    /// Ids whose `finalize_prune` should return `Pruned { floor_ok: false }`.
    breach_ids: Mutex<Vec<SnapshotId>>,
    /// Ids whose `finalize_prune` should return `Pruned { marker_ok: false }`.
    marker_breach_ids: Mutex<Vec<SnapshotId>>,
    /// Ids whose `finalize_prune` should return an `sqlx` error.
    fail_prune: Mutex<Vec<SnapshotId>>,
    last_keep_n: Mutex<Option<i64>>,
    last_min_age: Mutex<Option<Option<Duration>>>,
    /// Cursor the reconciler passed to the most recent `list_claimed`.
    last_claimed_after: Mutex<Option<Option<SnapshotId>>>,
}

impl FakeSink {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_candidates(&self, rows: Vec<PruneCandidate>) {
        *self.candidates.lock().unwrap() = rows;
    }

    fn set_claimed(&self, rows: Vec<PruneCandidate>) {
        *self.claimed.lock().unwrap() = rows;
    }

    fn fail_candidate_scan(&self) {
        *self.fail_candidate_scan.lock().unwrap() = true;
    }

    fn fail_claimed_scan(&self) {
        *self.fail_claimed_scan.lock().unwrap() = true;
    }

    fn refuse_claim(&self, id: SnapshotId) {
        self.refuse_claim_ids.lock().unwrap().push(id);
    }

    fn fail_claim(&self, id: SnapshotId) {
        self.claim_err_ids.lock().unwrap().push(id);
    }

    fn mark_gone(&self, id: SnapshotId) {
        self.gone_ids.lock().unwrap().push(id);
    }

    fn mark_breach(&self, id: SnapshotId) {
        self.breach_ids.lock().unwrap().push(id);
    }

    fn mark_marker_breach(&self, id: SnapshotId) {
        self.marker_breach_ids.lock().unwrap().push(id);
    }

    fn fail(&self, id: SnapshotId) {
        self.fail_prune.lock().unwrap().push(id);
    }

    fn prune_calls(&self) -> Vec<SnapshotId> {
        self.prune_calls.lock().unwrap().clone()
    }

    fn claim_calls(&self) -> Vec<SnapshotId> {
        self.claim_calls.lock().unwrap().clone()
    }

    fn last_keep_n(&self) -> Option<i64> {
        *self.last_keep_n.lock().unwrap()
    }

    fn last_claimed_after(&self) -> Option<Option<SnapshotId>> {
        *self.last_claimed_after.lock().unwrap()
    }
}

impl RetentionSink for Arc<FakeSink> {
    async fn list_candidates(
        &self,
        keep_n: i64,
        limit: i64,
        after: Option<SnapshotId>,
        min_age: Option<Duration>,
    ) -> Result<CandidatePage, sqlx::Error> {
        if *self.fail_candidate_scan.lock().unwrap() {
            return Err(sqlx::Error::PoolClosed);
        }
        self.last_keep_n.lock().unwrap().replace(keep_n);
        self.last_min_age.lock().unwrap().replace(min_age);
        let all = self.candidates.lock().unwrap().clone();
        let limit = usize::try_from(limit)
            .unwrap_or(usize::MAX)
            .min(self.candidate_page_size.unwrap_or(usize::MAX));
        let candidates: Vec<_> = all
            .into_iter()
            .filter(|c| after.map(|a| c.snapshot_id > a).unwrap_or(true))
            .take(limit)
            .collect();
        Ok(CandidatePage {
            scanned_through: candidates.last().map(|c| c.snapshot_id),
            exhausted: candidates.len() < limit,
            candidates,
        })
    }

    async fn claimed_scan_epoch(&self) -> Result<Option<ClaimedScanEpoch>, sqlx::Error> {
        if *self.fail_claimed_scan.lock().unwrap() {
            return Err(sqlx::Error::PoolClosed);
        }
        let claimed = self.claimed.lock().unwrap();
        Ok(claimed
            .iter()
            .map(|candidate| candidate.snapshot_id)
            .max()
            .map(|through| ClaimedScanEpoch { through }))
    }

    async fn list_claimed(
        &self,
        limit: i64,
        after: Option<SnapshotId>,
        through: SnapshotId,
    ) -> Result<Vec<PruneCandidate>, sqlx::Error> {
        if *self.fail_claimed_scan.lock().unwrap() {
            return Err(sqlx::Error::PoolClosed);
        }
        self.last_claimed_after.lock().unwrap().replace(after);
        let all = self.claimed.lock().unwrap().clone();
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(all
            .into_iter()
            .filter(|c| after.map(|a| c.snapshot_id > a).unwrap_or(true))
            .filter(|c| c.snapshot_id <= through)
            .take(limit)
            .collect())
    }

    async fn claim_prune(
        &self,
        candidate: PruneCandidate,
        _keep_n: i64,
        _min_age: Option<Duration>,
    ) -> Result<ClaimOutcome, sqlx::Error> {
        let snapshot_id = candidate.snapshot_id;
        self.claim_calls.lock().unwrap().push(snapshot_id);
        if self.claim_err_ids.lock().unwrap().contains(&snapshot_id) {
            return Err(sqlx::Error::PoolClosed);
        }
        if self.refuse_claim_ids.lock().unwrap().contains(&snapshot_id) {
            return Ok(ClaimOutcome::Refused(ClaimRefusalReason::KeepNFloor));
        }
        if self.enforce_protocol {
            self.candidates
                .lock()
                .unwrap()
                .retain(|row| row.snapshot_id != snapshot_id);
            self.claimed.lock().unwrap().push(candidate.clone());
        }
        Ok(ClaimOutcome::Claimed(candidate))
    }

    async fn finalize_prune(
        &self,
        snapshot_id: SnapshotId,
        _keep_n: i64,
    ) -> Result<PruneFinalize, sqlx::Error> {
        if self.enforce_protocol {
            assert!(self.backend_returns.lock().unwrap().contains(&snapshot_id));
            assert!(
                self.claimed
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|row| row.snapshot_id == snapshot_id)
            );
        }
        self.prune_calls.lock().unwrap().push(snapshot_id);
        if self.fail_prune.lock().unwrap().contains(&snapshot_id) {
            return Err(sqlx::Error::PoolClosed);
        }
        if self.gone_ids.lock().unwrap().contains(&snapshot_id) {
            return Ok(PruneFinalize::AlreadyGone);
        }
        if self.enforce_protocol {
            self.claimed
                .lock()
                .unwrap()
                .retain(|row| row.snapshot_id != snapshot_id);
        }
        let floor_ok = !self.breach_ids.lock().unwrap().contains(&snapshot_id);
        let marker_ok = !self
            .marker_breach_ids
            .lock()
            .unwrap()
            .contains(&snapshot_id);
        Ok(PruneFinalize::Pruned {
            floor_ok,
            marker_ok,
        })
    }
}

/// Canned backend error (kept `Clone` so the fake's outcome map can hold
/// it; `DeletePrefixError` itself isn't `Clone`).
#[derive(Clone)]
enum FakeErr {
    InvalidUri,
    Backend(String),
    OutOfScope,
}

/// Fake backend registered under the `s3` scheme. Maps a connection
/// string to a canned outcome/error; unmapped strings default to a
/// successful delete.
#[derive(Default)]
struct FakeBackend {
    delay: Duration,
    outcomes: Mutex<HashMap<String, Result<DeletePrefixOutcome, FakeErr>>>,
    calls: Mutex<Vec<String>>,
}

impl FakeBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_outcome(&self, conn: &str, outcome: DeletePrefixOutcome) {
        self.outcomes
            .lock()
            .unwrap()
            .insert(conn.to_string(), Ok(outcome));
    }

    fn set_error(&self, conn: &str, err: FakeErr) {
        self.outcomes
            .lock()
            .unwrap()
            .insert(conn.to_string(), Err(err));
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl StorageBackend for FakeBackend {
    fn scheme(&self) -> &'static str {
        "s3"
    }

    fn validate_route(&self, connection_string: &str) -> Result<(), DeletePrefixError> {
        // Mirror the real contract: routing verdicts (parse/scope) are
        // knowable without I/O, transient backend outcomes are not.
        let outcomes = match self.outcomes.lock() {
            Ok(outcomes) => outcomes,
            Err(poisoned) => poisoned.into_inner(),
        };
        match outcomes.get(connection_string) {
            Some(Err(FakeErr::InvalidUri)) => Err(DeletePrefixError::InvalidUri),
            Some(Err(FakeErr::OutOfScope)) => Err(DeletePrefixError::OutOfScope {
                backend: "s3",
                container: "other-bucket".into(),
            }),
            _ => Ok(()),
        }
    }

    fn delete_prefix<'a>(
        &'a self,
        connection_string: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<DeletePrefixOutcome, DeletePrefixError>> + Send + 'a>>
    {
        self.calls
            .lock()
            .unwrap()
            .push(connection_string.to_string());
        let result = match self
            .outcomes
            .lock()
            .unwrap()
            .get(connection_string)
            .cloned()
        {
            Some(Ok(outcome)) => Ok(outcome),
            Some(Err(FakeErr::InvalidUri)) => Err(DeletePrefixError::InvalidUri),
            Some(Err(FakeErr::Backend(message))) => Err(DeletePrefixError::Backend(message)),
            Some(Err(FakeErr::OutOfScope)) => Err(DeletePrefixError::OutOfScope {
                backend: "s3",
                container: "other-bucket".into(),
            }),
            None => Ok(DeletePrefixOutcome::Deleted { count: 1 }),
        };
        Box::pin(async move {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            result
        })
    }
}

fn registry_with(fake: Arc<FakeBackend>) -> StorageBackendRegistry {
    let mut registry = StorageBackendRegistry::new();
    registry.register(fake);
    registry
}

const TEST_BUCKET: &str = "example-snapshots";

fn fixture_uri(snap: &str) -> String {
    format!(
        "s3://{TEST_BUCKET}\
         /clusters/test-cluster/suspend_snapshots/aa/sandbox-{snap}/snap-{snap}\
         ?region=us-east-1"
    )
}

fn candidate(uri: &str, has_encrypted_fields: bool) -> PruneCandidate {
    PruneCandidate {
        snapshot_id: SnapshotId::new(),
        sandbox_id: SandboxId::new(),
        storage_location: Some(SnapshotStorageLocation::new(
            Uuid::now_v7(),
            uri.to_string(),
            has_encrypted_fields,
        )),
        age_secs: 7200,
    }
}

/// A candidate already in `state = 'deleting'`: a durable claim from a
/// prior tick or a crashed process, resurfaced for finish-forward.
fn claimed_candidate(uri: &str) -> PruneCandidate {
    candidate(uri, false)
}

fn candidate_route(candidate: &PruneCandidate) -> &str {
    candidate
        .storage_location
        .as_ref()
        .map(SnapshotStorageLocation::connection_string)
        .expect("test candidate has a storage route")
}

fn config(max_per_tick: u32) -> RetentionPolicy {
    RetentionPolicy {
        // Tick tests exercise the live prune path; dry-run is covered
        // separately by t14.
        dry_run: false,
        keep_n: 3,
        max_per_tick,
        max_concurrent_deletes: NonZeroU32::MIN,
        min_age: None,
    }
}

// -----------------------------------------------------------------
// T — tick behavior
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn slow_backend_does_not_spend_the_discovery_budget() {
    let sink = Arc::new(FakeSink {
        candidate_page_size: Some(1),
        ..Default::default()
    });
    let first = candidate(&fixture_uri("slow-first"), false);
    let second = candidate(&fixture_uri("slow-second"), false);
    sink.candidates.lock().unwrap().extend([first, second]);
    let backend = Arc::new(FakeBackend {
        delay: Duration::from_secs(6),
        ..Default::default()
    });
    let mut reconciler = Reconciler::new(config(2), sink, registry_with(backend.clone()));
    let report = reconciler.tick().await;
    assert_eq!(
        report.pruned, 2,
        "backend time must not prevent visiting the next sparse discovery page"
    );
    assert_eq!(backend.calls().len(), 2);
}

// Real latency is opt-in independently of nextest's --run-ignored all:
// SPACE_RETENTION_BENCHMARK_BACKEND_DELAY_MS=150 SPACE_CI_MAIN_ONLY_TESTS=1 \
//   SPACE_CI_REQUIRE_TEST_DEPS=1 \
//   cargo nextest run -p pplx-space-snapshot-retention -E 'test(benchmark_)' --no-capture
// Use 1 instead of 150 for a short real-timer smoke run. With no variable set,
// normal CI exercises the same workloads and assertions without backend sleeps.
// SPACE_CI_MAIN_ONLY_TESTS is what admits the Postgres-backed benchmark; the
// fake-sink ones run everywhere.
fn benchmark_backend_delay() -> Duration {
    match std::env::var("SPACE_RETENTION_BENCHMARK_BACKEND_DELAY_MS") {
        Ok(value) => {
            Duration::from_millis(value.parse().expect("backend delay must be milliseconds"))
        }
        Err(std::env::VarError::NotPresent) => Duration::ZERO,
        Err(error) => panic!("invalid benchmark backend delay: {error}"),
    }
}

/// True when this run includes the tests only main CI pays for.
///
/// A runtime gate rather than `#[ignore]`: the Rust suite runs nextest with
/// `--run-ignored all --ignore-default-filter`, so neither the attribute nor a
/// default filter expression can hold a test back, and the checks script
/// deliberately carries no test-name selectors. Reads its variable with the
/// same semantics as `SPACE_CI_REQUIRE_TEST_DEPS`: present, non-empty and not
/// `0` means on, so one variable can be set or unset per pipeline step.
fn main_only_tests() -> bool {
    std::env::var_os("SPACE_CI_MAIN_ONLY_TESTS")
        .is_some_and(|value| !value.is_empty() && value != "0")
}

/// Sparse/dense behavioral coverage, with optional real backend latency.
#[tokio::test(flavor = "current_thread")]
async fn benchmark_reconciler_sparse_and_dense_backend() {
    let delay = benchmark_backend_delay();
    for page_size in [100, 2_000] {
        let sink = Arc::new(FakeSink {
            candidate_page_size: Some(page_size),
            ..Default::default()
        });
        for i in 0..1_000 {
            sink.candidates
                .lock()
                .unwrap()
                .push(candidate(&fixture_uri(&format!("bench-{i}")), false));
        }
        let backend = Arc::new(FakeBackend {
            delay,
            ..Default::default()
        });
        let mut reconciler = Reconciler::new(config(1_000), sink, registry_with(backend.clone()));
        let started = std::time::Instant::now();
        let report = reconciler.tick().await;
        println!(
            "page_size={page_size} backend_delay_ms={} concurrency=1 cap=1000 elapsed_ms={} pruned={} backend_calls={}",
            delay.as_millis(),
            started.elapsed().as_millis(),
            report.pruned,
            backend.calls().len()
        );
        assert_eq!(report.pruned, 1_000);
        assert_eq!(backend.calls().len(), 1_000);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t1_empty_candidate_list_is_noop() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t1b_fresh_scan_gauges_require_a_completed_scan() {
    let sink = FakeSink::new();
    sink.fail_candidate_scan();
    let backend = FakeBackend::new();
    let mut reconciler = Reconciler::new(config(10), sink, registry_with(backend));

    let failure_counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let failed_capture = pplx_space_metrics::test_capture::capture_values_guard();
    let failed_report = reconciler.tick().await;
    let failed_values = failed_capture.finish();
    assert!(failure_counters.finish().iter().any(|counter| {
        counter.name == "snapshot_retention_sweeper.failed"
            && counter.tags.iter().any(|tag| tag == "reason:fresh_scan")
    }));
    assert_eq!(failed_report.scanned, 0);
    let scan_volume = failed_values
        .iter()
        .filter(|value| value.name == "snapshot_retention_sweeper.scan_volume")
        .collect::<Vec<_>>();
    assert_eq!(
        scan_volume.len(),
        1,
        "scan volume must be emitted once per tick"
    );
    assert!(
        scan_volume[0].value.abs() < f64::EPSILON,
        "a failed fresh scan observed no rows"
    );
    assert!(
        failed_values.iter().all(|value| {
            value.name != "snapshot_retention_sweeper.candidates_scanned"
                && value.name != "snapshot_retention_sweeper.rate_limited"
        }),
        "a failed fresh scan must not emit zero-valued liveness gauges: {failed_values:?}"
    );

    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let mut reconciler = Reconciler::new(config(10), sink, registry_with(backend));
    let success_capture = pplx_space_metrics::test_capture::capture_values_guard();
    let success_report = reconciler.tick().await;
    let success_values = success_capture.finish();
    assert_eq!(success_report.scanned, 0);
    for name in [
        "snapshot_retention_sweeper.scan_volume",
        "snapshot_retention_sweeper.claimed_scanned",
        "snapshot_retention_sweeper.candidates_scanned",
        "snapshot_retention_sweeper.rate_limited",
    ] {
        let matching = success_values
            .iter()
            .filter(|value| value.name == name)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "{name} must be emitted once");
        assert!(
            matching[0].value.abs() < f64::EPSILON,
            "{name} must report an empty scan"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t1c_scan_volume_combines_fresh_and_claimed_rows() {
    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(&fixture_uri("fresh-1"), false),
        candidate(&fixture_uri("fresh-2"), false),
    ]);
    sink.set_claimed(vec![claimed_candidate(&fixture_uri("claimed"))]);
    let backend = FakeBackend::new();
    let mut cfg = config(10);
    cfg.dry_run = true;
    let mut reconciler = Reconciler::new(cfg, sink, registry_with(backend));

    let values_capture = pplx_space_metrics::test_capture::capture_values_guard();
    let report = reconciler.tick().await;
    let values = values_capture.finish();

    assert_eq!(report.scanned, 2);
    assert_eq!(report.claimed_scanned, 1);
    let scan_volume = values
        .iter()
        .filter(|value| value.name == "snapshot_retention_sweeper.scan_volume")
        .collect::<Vec<_>>();
    assert_eq!(
        scan_volume.len(),
        1,
        "scan volume must be emitted once per tick"
    );
    assert!((scan_volume[0].value - 3.0).abs() < f64::EPSILON);
}

#[tokio::test(flavor = "current_thread")]
async fn t1d_claimed_scan_failure_is_loud_and_silences_its_heartbeat() {
    let sink = FakeSink::new();
    sink.fail_claimed_scan();
    let backend = FakeBackend::new();
    let mut reconciler = Reconciler::new(config(10), sink, registry_with(backend));

    let values_capture = pplx_space_metrics::test_capture::capture_values_guard();
    let counter_capture = pplx_space_metrics::test_capture::capture_counters_guard();
    let report = reconciler.tick().await;
    let counters = counter_capture.finish();
    let values = values_capture.finish();

    assert_eq!(report, TickReport::default());
    assert!(counters.iter().any(|counter| {
        counter.name == "snapshot_retention_sweeper.failed"
            && counter.tags.iter().any(|tag| tag == "reason:claimed_scan")
    }));
    assert!(
        values
            .iter()
            .all(|value| value.name != "snapshot_retention_sweeper.claimed_scanned"),
        "a failed claimed scan must not emit a healthy liveness gauge: {values:?}"
    );
    for name in [
        "snapshot_retention_sweeper.candidates_scanned",
        "snapshot_retention_sweeper.rate_limited",
    ] {
        assert!(
            values.iter().any(|value| value.name == name),
            "fresh work must retain its independent heartbeat: {values:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t2_unencrypted_candidate_is_pruned() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    backend.set_outcome(
        candidate_route(&c),
        DeletePrefixOutcome::Deleted { count: 4 },
    );
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.pruned, 1);
    assert_eq!(sink.prune_calls(), vec![c.snapshot_id]);
    assert_eq!(backend.calls(), vec![candidate_route(&c).to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn t3_empty_prefix_treated_as_already_gone() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    backend.set_outcome(candidate_route(&c), DeletePrefixOutcome::AlreadyGone);
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.already_gone, 1);
    assert_eq!(report.pruned, 0);
    assert_eq!(sink.prune_calls(), vec![c.snapshot_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn t3b_claimed_row_without_a_location_finalizes_as_already_gone() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let mut claimed = claimed_candidate(&fixture_uri("claimed-no-route"));
    claimed.storage_location = None;
    sink.set_claimed(vec![claimed.clone()]);
    let mut reconciler = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));

    let report = reconciler.tick().await;

    assert_eq!(report.already_gone, 1);
    assert_eq!(report.resumed, 1);
    assert_eq!(sink.prune_calls(), vec![claimed.snapshot_id]);
    assert!(backend.calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t3c_fresh_row_without_a_location_remains_unclaimed() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let mut missing = candidate(&fixture_uri("fresh-missing-route"), false);
    missing.storage_location = None;
    sink.set_candidates(vec![missing.clone()]);
    let mut reconciler = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));

    let report = reconciler.tick().await;

    assert_eq!(report.scanned, 1);
    assert_eq!(report.missing_locations, 1);
    assert!(sink.claim_calls().is_empty());
    assert!(sink.prune_calls().is_empty());
    assert!(backend.calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t4_encrypted_row_skipped() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    sink.set_candidates(vec![candidate(&fixture_uri("1"), true)]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.skipped_encrypted, 1);
    assert!(sink.prune_calls().is_empty());
    assert!(backend.calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t5_invalid_uri_skips_candidate_before_claim() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("invalid"), false);
    backend.set_error(candidate_route(&c), FakeErr::InvalidUri);
    sink.set_candidates(vec![c]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.parse_errors, 1);
    assert!(
        backend.calls().is_empty(),
        "the route must validate before the prefix is deleted"
    );
    assert!(
        sink.claim_calls().is_empty(),
        "an invalid route must not be claimed"
    );
    assert!(sink.prune_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t12b_failed_byte_deletes_still_consume_the_cap() {
    // The cap bounds destructive commitments: a claim that then fails its
    // byte delete has still de-advertised a row, so it must consume the
    // cap — otherwise one transient backend outage lets a single tick
    // claim the whole fetched page instead of `max_per_tick` rows.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c1 = candidate(&fixture_uri("1"), false);
    let c2 = candidate(&fixture_uri("2"), false);
    backend.set_error(candidate_route(&c1), FakeErr::Backend("throttled".into()));
    backend.set_error(candidate_route(&c2), FakeErr::Backend("throttled".into()));
    sink.set_candidates(vec![c1.clone(), c2.clone()]);
    let mut r = Reconciler::new(config(1), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.backend_errors, 1);
    assert_eq!(
        report.rate_limited, 1,
        "the second candidate must be capped"
    );
    assert_eq!(
        sink.claim_calls(),
        vec![c1.snapshot_id],
        "only one claim may commit under max_per_tick = 1"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t6_transient_backend_error_skips_db_delete() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    backend.set_error(candidate_route(&c), FakeErr::Backend("throttled".into()));
    sink.set_candidates(vec![c]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.backend_errors, 1);
    assert!(sink.prune_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t6a_claimed_backend_error_does_not_log_connection_credentials() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = claimed_candidate(&fixture_uri("secret"));
    let secret = "https://user:password@example.invalid";
    backend.set_error(candidate_route(&c), FakeErr::Backend(secret.to_string()));
    sink.set_claimed(vec![c]);

    let layer = CapturedEventsLayer::new();
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    let report = Reconciler::new(config(10), sink, registry_with(backend))
        .tick()
        .await;
    drop(guard);

    assert_eq!(report.backend_errors, 1);
    let captured = events.events();
    let event = captured
        .iter()
        .find(|event| field_value(event, "error_bytes").is_some())
        .expect("backend failure must retain bounded diagnostic metadata");
    assert_eq!(field_value(event, "error_bytes"), Some("37"));
    assert!(
        event.iter().all(|(_, value)| !value.contains(secret)),
        "backend error credentials must not appear in tracing fields: {event:?}"
    );
    assert!(
        field_value(event, "error").is_none(),
        "the raw backend error field must remain absent"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t7_db_prune_failure_after_reap_is_db_error() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    sink.fail(c.snapshot_id);
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.db_errors, 1);
    assert_eq!(report.pruned, 0);
    assert_eq!(backend.calls().len(), 1, "reap ran before the DB delete");
}

#[tokio::test(flavor = "current_thread")]
async fn t8_refused_claim_skips_without_backend_call() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    sink.refuse_claim(c.snapshot_id);
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.drift_skips, 1);
    assert_eq!(sink.claim_calls(), vec![c.snapshot_id]);
    assert!(
        backend.calls().is_empty(),
        "no destructive delete when the claim is refused"
    );
    assert!(sink.prune_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t8b_claim_error_is_distinct_from_db_error() {
    // The claim transaction errors. It rolls back cleanly — no blob is
    // reaped and no finalize is attempted — so this must count as
    // `claim_errors`, NOT `db_errors`: the two map to different
    // `failed{reason=...}` series so an on-call can tell a failing claim
    // from a failing post-reap finalize.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    sink.fail_claim(c.snapshot_id);
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.claim_errors, 1);
    assert_eq!(report.db_errors, 0, "no post-reap delete was attempted");
    assert_eq!(report.drift_skips, 0);
    assert_eq!(sink.claim_calls(), vec![c.snapshot_id]);
    assert!(
        backend.calls().is_empty(),
        "no destructive delete when the claim transaction fails"
    );
    assert!(sink.prune_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t8c_claimed_candidate_resumes_without_reclaiming() {
    // A row claimed by a prior tick (or a crashed process) resurfaces via
    // the recovery scan and finishes forward: no second claim, straight
    // to the reap + finalize, counted as both resumed and pruned.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = claimed_candidate(&fixture_uri("1"));
    sink.set_claimed(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.resumed, 1);
    assert_eq!(report.pruned, 1);
    assert!(
        sink.claim_calls().is_empty(),
        "an already-claimed row must not be re-claimed"
    );
    assert_eq!(backend.calls().len(), 1);
    assert_eq!(sink.prune_calls(), vec![c.snapshot_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn t8d_dry_run_parks_claimed_rows_loudly() {
    // Dry run touches nothing, including claims left behind by a previous
    // live configuration: the row stays `deleting` (de-advertised, bytes
    // intact) and is surfaced as a stuck claim rather than finished.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let mut c = claimed_candidate(&fixture_uri("1"));
    c.storage_location
        .as_mut()
        .expect("test candidate has a storage route")
        .has_encrypted_fields = true;
    sink.set_claimed(vec![c.clone()]);
    let mut cfg = config(10);
    cfg.dry_run = true;
    let mut r = Reconciler::new(cfg, sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.stuck_claims, 1);
    assert_eq!(
        report.skipped_encrypted, 0,
        "dry run must classify the claim as stuck"
    );
    assert_eq!(report.resumed, 0);
    assert_eq!(report.would_prune, 0, "a stuck claim is not a would-prune");
    assert!(sink.claim_calls().is_empty());
    assert!(backend.calls().is_empty());
    assert!(sink.prune_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t8d_live_encrypted_claim_is_parked_not_benignly_skipped() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let mut c = claimed_candidate(&fixture_uri("1"));
    c.storage_location
        .as_mut()
        .expect("test candidate has a storage route")
        .has_encrypted_fields = true;
    sink.set_claimed(vec![c]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));

    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    let report = r.tick().await;
    let counters = capture.finish();

    assert_eq!(report.skipped_encrypted, 1);
    assert!(backend.calls().is_empty());
    assert!(sink.prune_calls().is_empty());
    assert!(counters.iter().any(|counter| {
        counter.name == "snapshot_retention_sweeper.failed"
            && counter.tags.iter().any(|tag| tag == "reason:parked_claim")
    }));
    assert!(!counters.iter().any(|counter| {
        counter.name == "snapshot_retention_sweeper.failed"
            && counter
                .tags
                .iter()
                .any(|tag| tag == "reason:encrypted_unsupported")
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn t8g_claimed_scan_reaches_high_watermark_with_alternating_odd_budget() {
    // `max_per_tick = 3` alternates recovery pages of two and one. Epoch
    // completion must follow the cursor to its fixed high-water mark rather
    // than count pages using whichever page size happened to start it.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let claim_id = |i: u128| SnapshotId::from_uuid(Uuid::from_u128(i + 1));
    let mut claimed: Vec<PruneCandidate> = (0..8u128)
        .map(|i| {
            let uri = format!("s3://{TEST_BUCKET}/parked-{i}");
            backend.set_error(&uri, FakeErr::InvalidUri);
            PruneCandidate {
                snapshot_id: claim_id(i),
                ..claimed_candidate(&uri)
            }
        })
        .collect();
    sink.set_claimed(claimed.clone());
    let mut reconciler = Reconciler::new(config(3), sink.clone(), registry_with(backend.clone()));

    let first = reconciler.tick().await;
    assert_eq!(first.claimed_scanned, 2);
    let tail_uri = format!("s3://{TEST_BUCKET}/next-epoch");
    backend.set_error(&tail_uri, FakeErr::InvalidUri);
    claimed.push(PruneCandidate {
        snapshot_id: SnapshotId::from_uuid(Uuid::from_u128(100)),
        ..claimed_candidate(&tail_uri)
    });
    sink.set_claimed(claimed);

    for (budget, expected_page) in [(3, 1), (5, 3), (1, 0), (3, 2)] {
        reconciler.set_work_limits(NonZeroU32::new(budget).unwrap(), NonZeroU32::MIN);
        assert_eq!(reconciler.tick().await.claimed_scanned, expected_page);
    }

    let calls = backend.calls();
    for i in 0..8u128 {
        let uri = format!("s3://{TEST_BUCKET}/parked-{i}");
        assert_eq!(
            calls.iter().filter(|called| *called == &uri).count(),
            1,
            "every claim inside the starting high-water mark is visited once"
        );
    }
    assert!(
        !calls.contains(&tail_uri),
        "a higher-id claim waits for the next recovery epoch"
    );

    reconciler.tick().await;
    assert_eq!(
        sink.last_claimed_after(),
        Some(None),
        "reaching the high-water mark wraps the next epoch to its oldest claim"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t8h_one_unit_budget_alternates_recovery_and_fresh_work() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let claimed = claimed_candidate(&fixture_uri("claimed"));
    let fresh = candidate(&fixture_uri("fresh"), false);
    sink.set_claimed(vec![claimed.clone()]);
    sink.set_candidates(vec![fresh.clone()]);
    let mut reconciler = Reconciler::new(config(1), sink.clone(), registry_with(backend));

    let first = reconciler.tick().await;
    assert_eq!(first.resumed, 1);
    assert_eq!(first.pruned + first.already_gone, 1);
    assert!(sink.claim_calls().is_empty());

    let second = reconciler.tick().await;
    assert_eq!(second.claimed_scanned, 0);
    assert_eq!(second.pruned + second.already_gone, 1);
    assert_eq!(sink.claim_calls(), vec![fresh.snapshot_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn t9_finalize_already_gone_is_benign_drift() {
    // The snapshot row vanished between the reap and finalize (e.g. a
    // concurrent sandbox-delete cascade). Reaping its blobs was redundant;
    // no prune is counted and the floor isn't breached.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    sink.mark_gone(c.snapshot_id);
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.drift_skips, 1);
    assert_eq!(report.pruned, 0);
    assert_eq!(report.floor_breaches, 0);
    assert_eq!(sink.prune_calls(), vec![c.snapshot_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn t9b_floor_breach_still_prunes_but_is_surfaced() {
    // A newer snapshot was deleted concurrently in the reap→finalize
    // window, so the prune dropped the sandbox below keep_n. The row is
    // still removed (its blobs are gone → unrestorable), and the breach
    // is counted distinctly rather than silently leaving a `ready` row.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    sink.mark_breach(c.snapshot_id);
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.pruned, 1, "the unrestorable row is still removed");
    assert_eq!(report.floor_breaches, 1);
    assert_eq!(report.marker_breaches, 0);
    assert_eq!(report.drift_skips, 0);
    assert_eq!(sink.prune_calls(), vec![c.snapshot_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn t9c_marker_breach_still_prunes_but_is_surfaced() {
    // The marker changed after the claim and before finalization, so a live
    // restore target was destroyed. The row must still be removed because its
    // blobs are gone, and the breach stays distinct from a keep-N floor breach.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    sink.mark_marker_breach(c.snapshot_id);
    sink.set_candidates(vec![c.clone()]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.pruned, 1, "the unrestorable row is still removed");
    assert_eq!(report.marker_breaches, 1);
    assert_eq!(report.floor_breaches, 0, "distinct from a floor breach");
    assert_eq!(report.drift_skips, 0);
    assert_eq!(sink.prune_calls(), vec![c.snapshot_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn t10_out_of_scope_candidate_skipped() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    backend.set_error(candidate_route(&c), FakeErr::OutOfScope);
    sink.set_candidates(vec![c]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.bucket_out_of_scope, 1);
    assert!(sink.prune_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t11_unsupported_scheme_candidate_skipped() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new(); // registered under "s3"
    // A gs:// candidate has no registered backend → unsupported scheme,
    // and the s3 fake is never called.
    let c = candidate("gs://some-bucket/path/snap-1", false);
    sink.set_candidates(vec![c]);
    let mut r = Reconciler::new(config(10), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.unsupported_scheme, 1);
    assert!(backend.calls().is_empty());
    assert!(sink.prune_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t12_max_per_tick_caps_processing() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let rows: Vec<PruneCandidate> = (0..5)
        .map(|i| candidate(&fixture_uri(&i.to_string()), false))
        .collect();
    for c in &rows {
        backend.set_outcome(
            candidate_route(c),
            DeletePrefixOutcome::Deleted { count: 1 },
        );
    }
    sink.set_candidates(rows);
    let mut r = Reconciler::new(config(2), sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    // The tick fetches `2 * max_per_tick` rows (here 4 of the 5 staged),
    // prunes up to `max_per_tick` (2), and rate-limits the rest of what it
    // fetched (2). The 5th row isn't fetched this tick; the cursor picks it
    // up next tick.
    assert_eq!(report.pruned, 2);
    assert_eq!(report.rate_limited, 2);
    // Increasing the budget must continue past the first two rows, even
    // though this fixture retains finalized rows in its candidate list.
    r.set_work_limits(NonZeroU32::new(3).unwrap(), NonZeroU32::new(2).unwrap());
    assert_eq!(r.tick().await.pruned, 3);
    let calls = sink.prune_calls();
    assert_eq!(calls.len(), 5);
    assert_eq!(
        calls.iter().collect::<std::collections::HashSet<_>>().len(),
        5
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t13_keep_n_threaded_through_to_sink() {
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let mut cfg = config(10);
    cfg.keep_n = 7;
    let mut r = Reconciler::new(cfg, sink.clone(), registry_with(backend.clone()));
    let _ = r.tick().await;
    assert_eq!(sink.last_keep_n(), Some(7));
}

#[tokio::test(flavor = "current_thread")]
async fn t14_dry_run_counts_would_prune_without_side_effects() {
    // A candidate that passes the keep-N safety gate is counted as
    // `would_prune` but never reaches the destructive reap or the DB
    // finalize: no backend call, no finalize call, nothing in `pruned`.
    let sink = FakeSink::new();
    let backend = FakeBackend::new();
    let c = candidate(&fixture_uri("1"), false);
    sink.set_candidates(vec![c.clone()]);
    let mut cfg = config(10);
    cfg.dry_run = true;
    let mut r = Reconciler::new(cfg, sink.clone(), registry_with(backend.clone()));
    let report = r.tick().await;
    assert_eq!(report.would_prune, 1);
    assert_eq!(report.pruned, 0);
    assert_eq!(report.already_gone, 0);
    // Dry run stops before the claim: it must not write the `deleting`
    // tombstone, so the count is scan-level by design.
    assert!(
        sink.claim_calls().is_empty(),
        "dry-run must not take the durable claim"
    );
    assert!(
        backend.calls().is_empty(),
        "dry-run performs no S3 list/delete"
    );
    assert!(
        sink.prune_calls().is_empty(),
        "dry-run performs no DB finalize"
    );
}

// -----------------------------------------------------------------
// P — Postgres store
// -----------------------------------------------------------------

#[cfg(test)]
mod postgres {
    use pplx_space_common_core::sql;
    use pplx_space_common_core::types::{SandboxId, SnapshotId};
    use pplx_space_db_test_fixtures::{
        SEED_ORG_ID as SEEDED_ORG_ID, SEED_USER_ID as SEEDED_USER_ID, try_pg_test_db,
    };
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::super::store::SnapshotRetentionDbStore;
    use super::super::{
        ClaimOutcome, ClaimRefusalReason, PruneCandidate, PruneFinalize, RetentionSink,
    };

    use pplx_space_test_env::TestResult;

    #[tokio::test(flavor = "current_thread")]
    async fn discovery_candidate_cap_keeps_siblings_outside_the_raw_page() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let history = insert_sandbox(&db.pool).await?;
        // One long history spanning the 10,000-row raw ID page, with ages
        // increasing by id so every sibling probe finds its keeper on the
        // first index row it reads. The two newest rows deliberately tie: the
        // keep floor then has to break the tie on the higher UUID for id
        // 10000 to be a candidate at all, which is what pins the tuple
        // comparison in the probe.
        //
        // Ties are the expensive shape here — with every row tied, the probe
        // for candidate g filters g rows before it finds one that sorts
        // above g, so the scan is quadratic in the history. Two tied rows buy
        // the same tie-break coverage at a constant cost.
        sqlx::query(sql!(
            "INSERT INTO snapshots (id,sandbox_id,type,state,timestamp)
             SELECT ('20000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                    $1,'full','ready',
                    NOW() - INTERVAL '14 days' + (LEAST(g,10000) * INTERVAL '1 second')
               FROM generate_series(1,10001) g"
        ))
        .bind(history)
        .execute(&db.pool)
        .await?;
        // The row that makes this test's name true. Its keeper's id sorts
        // above every id in the first raw page while the candidate itself
        // sorts below all of them, so the first page's eligibility pass has
        // to reach a sibling the page does not contain. Bounding the sibling
        // probe by the page's id range drops this candidate and nothing else;
        // the long history above cannot show that, because by the time
        // paging reaches its late candidates their keeper is inside the
        // window.
        let crossing = insert_sandbox(&db.pool).await?;
        let crossing_candidate =
            SnapshotId::from_uuid(Uuid::from_u128(0x20000000000070008000000000000000));
        let crossing_keeper = Uuid::from_u128(0x2000000000007000800000000000fffe);
        sqlx::query(sql!(
            "INSERT INTO snapshots (id,sandbox_id,type,state,timestamp)
             VALUES ($1,$3,'full','ready',NOW()-INTERVAL '14 days'),
                    ($2,$3,'full','ready',NOW()-INTERVAL '13 days')"
        ))
        .bind(crossing_candidate)
        .bind(crossing_keeper)
        .bind(crossing)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id,storage_connection_string)
             SELECT id, 's3://test-bucket/snap/' || id FROM snapshots"
        ))
        .execute(&db.pool)
        .await?;
        let store = SnapshotRetentionDbStore::new(db.pool.clone());
        let mut cursor = None;
        let mut discovered = Vec::new();
        loop {
            let page = store
                .list_candidates(
                    1,
                    2000,
                    cursor,
                    Some(std::time::Duration::from_secs(604800)),
                )
                .await?;
            assert!(page.candidates.len() <= 2000);
            if !page.exhausted {
                assert!(page.scanned_through > cursor);
            }
            discovered.extend(page.candidates.into_iter().map(|c| c.snapshot_id));
            cursor = page.scanned_through;
            if page.exhausted {
                break;
            }
        }
        let expected: Vec<SnapshotId> = std::iter::once(crossing_candidate)
            .chain((1..=10000).map(|n| {
                SnapshotId::from_uuid(Uuid::from_u128(0x20000000000070008000000000000000 + n))
            }))
            .collect();
        assert_eq!(discovered, expected);
        let empty = store.list_candidates(1, 2000, cursor, None).await?;
        assert!(empty.candidates.is_empty() && empty.exhausted);
        assert_eq!(empty.scanned_through, None);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn discovery_rechecks_state_after_ids_and_retries_failed_eligibility() -> TestResult {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let sandbox = insert_sandbox(&db.pool).await?;
        let old = SnapshotId::from_uuid(Uuid::from_u128(0x20000000000070008000000000000001));
        let inserted = SnapshotId::from_uuid(Uuid::from_u128(0x20000000000070008000000000000002));
        let newest = SnapshotId::from_uuid(Uuid::from_u128(0x20000000000070008000000000000003));
        insert_snapshot_with_id(&db.pool, sandbox, old, "full", "ready", 1209600).await?;
        insert_snapshot_with_id(&db.pool, sandbox, newest, "full", "ready", 1209500).await?;
        for (fail_read, insert_between) in [(false, false), (true, false), (false, true)] {
            sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
                .bind(old)
                .execute(&db.pool)
                .await?;
            let armed = Arc::new(AtomicBool::new(false));
            let hook = armed.clone();
            // A one-connection pool holds the connection during this hook,
            // deterministically placing the mutation/fault between the reads.
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .after_release(move |conn, _| {
                    let fire = hook.swap(false, Ordering::SeqCst);
                    Box::pin(async move {
                        if fire {
                            if fail_read {
                                sqlx::query(sql!("SET search_path = pg_catalog"))
                                    .execute(conn)
                                    .await?;
                            } else if insert_between {
                                sqlx::query(sql!(
                                    "INSERT INTO snapshots (id,sandbox_id,type,state,timestamp)
                                     VALUES ($1,$2,'full','ready',NOW()-INTERVAL '14 days')"
                                ))
                                .bind(inserted)
                                .bind(sandbox)
                                .execute(conn)
                                .await?;
                            } else {
                                sqlx::query(sql!(
                                    "UPDATE snapshots SET state = 'deleting' WHERE id = $1"
                                ))
                                .bind(old)
                                .execute(conn)
                                .await?;
                            }
                        }
                        Ok(true)
                    })
                })
                .connect_with((*db.pool.connect_options()).clone())
                .await?;
            armed.store(true, Ordering::SeqCst);
            let store = SnapshotRetentionDbStore::new(pool.clone());
            let result = store.list_candidates(1, 2000, None, None).await;
            if fail_read {
                let error =
                    result.expect_err("eligibility read must fail without returning progress");
                assert_eq!(
                    error.as_database_error().and_then(|e| e.code()).as_deref(),
                    Some("42P01")
                );
                sqlx::query(sql!("SET search_path = public"))
                    .execute(&pool)
                    .await?;
                let retry = store.list_candidates(1, 2000, None, None).await?;
                assert_eq!(
                    retry
                        .candidates
                        .iter()
                        .map(|c| c.snapshot_id)
                        .collect::<Vec<_>>(),
                    vec![old]
                );
            } else {
                let page = result?;
                if insert_between {
                    // A concurrent insert inside the ID range was not supplied
                    // by discovery. It becomes visible on the next wrap.
                    assert_eq!(
                        page.candidates
                            .iter()
                            .map(|c| c.snapshot_id)
                            .collect::<Vec<_>>(),
                        vec![old]
                    );
                    let wrapped = store.list_candidates(1, 2000, None, None).await?;
                    assert_eq!(
                        wrapped
                            .candidates
                            .iter()
                            .map(|c| c.snapshot_id)
                            .collect::<Vec<_>>(),
                        vec![old, inserted]
                    );
                } else {
                    assert!(page.candidates.is_empty());
                }
                assert!(page.exhausted && page.scanned_through.is_some());
            }
            pool.close().await;
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_discovery_crosses_keepers_and_preserves_partial_budget_on_restart()
    -> TestResult {
        let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(1).await?
        else {
            return Ok(());
        };
        let pool = &db.pool;
        // Singleton histories are keepers even though every snapshot is older
        // than TTL. The eligible tail starts beyond two raw discovery pages.
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
             SELECT ('10000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                    $1::uuid, $2::uuid, 1 FROM generate_series(1,20001) g"
        ))
        .bind(SEEDED_USER_ID)
        .bind(SEEDED_ORG_ID)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (id,sandbox_id,type,state,timestamp)
             SELECT ('20000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                    ('10000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                    'full','ready',NOW()-INTERVAL '14 days'
               FROM generate_series(1,20001) g"
        ))
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id,storage_connection_string)
             SELECT id, 's3://test-bucket/snap/' || id FROM snapshots"
        ))
        .execute(pool)
        .await?;
        let tail_sandbox = insert_sandbox(pool).await?;
        let mut tail = Vec::new();
        for g in 1..=4 {
            let id = SnapshotId::from_uuid(Uuid::from_u128(0x30000000000070008000000000000000 + g));
            insert_snapshot_with_id(
                pool,
                tail_sandbox,
                id,
                "full",
                "ready",
                1_209_600 - g as i64,
            )
            .await?;
            tail.push(id);
        }
        let ttl = Some(std::time::Duration::from_secs(604_800));
        for mode in ["force_custom_plan", "force_generic_plan"] {
            sqlx::query(sql!("SELECT set_config('plan_cache_mode', $1, false)"))
                .bind(mode)
                .execute(pool)
                .await?;
            let store = SnapshotRetentionDbStore::new(pool.clone());
            let first = store.list_candidates(1, 2, None, ttl).await?;
            assert!(
                first.candidates.is_empty(),
                "the initial keeper prefix is ineligible"
            );
            assert!(!first.exhausted);
            let high = first
                .scanned_through
                .expect("empty eligible pages must advance");
            let restarted = store.list_candidates(1, 2, None, ttl).await?;
            assert_eq!(restarted.scanned_through, Some(high));
            let mut after = Some(high);
            let mut found = Vec::new();
            for _ in 0..10 {
                let page = store.list_candidates(1, 2, after, ttl).await?;
                found.extend(page.candidates.iter().map(|c| c.snapshot_id));
                if page.exhausted {
                    break;
                }
                assert!(page.scanned_through > after);
                after = page.scanned_through;
            }
            assert_eq!(
                found,
                tail[..3],
                "keep-N=1 preserves latest even beyond one-week TTL"
            );

            // A restarted controller traverses multiple empty pages in one tick.
            // Each tick may consume only one of a returned two-candidate batch.
            let mut policy = super::config(1);
            policy.keep_n = 1;
            policy.min_age = ttl;
            policy.dry_run = true;
            let mut reconciler = super::Reconciler::new(
                policy,
                store,
                super::registry_with(super::FakeBackend::new()),
            );
            for expected in &tail[..3] {
                let report = reconciler.tick().await;
                assert_eq!(report.would_prune, 1);
                assert_eq!(
                    reconciler.scan_after,
                    if *expected == tail[2] {
                        None
                    } else {
                        Some(*expected)
                    }
                );
            }
            assert_eq!(reconciler.tick().await.would_prune, 1);
            assert_eq!(reconciler.scan_after, Some(tail[0]));
            let store = SnapshotRetentionDbStore::new(pool.clone());
            assert!(store.claimed_scan_epoch().await?.is_none());
            sqlx::query(sql!(
                "UPDATE snapshots SET state = 'deleting' WHERE id = $1"
            ))
            .bind(tail[0])
            .execute(pool)
            .await?;
            let epoch = store.claimed_scan_epoch().await?.expect("sparse claim");
            assert_eq!(epoch.through, tail[0]);
            let claimed = store.list_claimed(1, None, epoch.through).await?;
            assert_eq!(claimed[0].snapshot_id, tail[0]);
            assert!(
                store
                    .list_claimed(1, Some(tail[0]), epoch.through)
                    .await?
                    .is_empty()
            );
            sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
                .bind(tail[0])
                .execute(pool)
                .await?;
        }
        Ok(())
    }

    /// Claimed recovery must not scale with the snapshot table. Statistics
    /// sampled while a tick holds many claims, followed by those claims
    /// finishing forward, leave a few old (low UUIDv7) claims under a large
    /// `deleting` estimate. Unfenced, `ORDER BY id DESC LIMIT 1` then walks the
    /// primary key backward and filters every unclaimed row before it reaches
    /// one, which is how the epoch query exceeds the statement timeout.
    #[tokio::test(flavor = "current_thread")]
    async fn claimed_recovery_reads_claims_not_the_table_under_stale_statistics() -> TestResult {
        use super::super::store::{CLAIMED_SCAN_EPOCH_SQL, LIST_CLAIMED_SQL};
        const ROWS: i64 = 50_000;
        const SAMPLED_CLAIMS: i64 = 1_000;
        const LIVE_CLAIMS: usize = 10;
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp)
             SELECT ('20000000-0000-7000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
                    $1, 'full',
                    CASE WHEN g <= $3 THEN 'deleting' ELSE 'ready' END,
                    NOW() - make_interval(secs => ($2 - g + 1)::double precision)
               FROM generate_series(1, $2) g"
        ))
        .bind(sandbox)
        .bind(ROWS)
        .bind(SAMPLED_CLAIMS)
        .execute(pool)
        .await?;
        sqlx::query(sql!("ANALYZE snapshots")).execute(pool).await?;
        let claims: Vec<SnapshotId> = sqlx::query_scalar(sql!(
            "SELECT id FROM snapshots WHERE state = 'deleting' ORDER BY id LIMIT $1"
        ))
        .bind(LIVE_CLAIMS as i64)
        .fetch_all(pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE snapshots SET state = 'ready' WHERE state = 'deleting' AND id > $1"
        ))
        .bind(claims[LIVE_CLAIMS - 1])
        .execute(pool)
        .await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let epoch = store.claimed_scan_epoch().await?.expect("live claims");
        assert_eq!(epoch.through, claims[LIVE_CLAIMS - 1]);
        let page = store.list_claimed(ROWS, None, epoch.through).await?;
        assert_eq!(
            page.iter().map(|c| c.snapshot_id).collect::<Vec<_>>(),
            claims
        );

        let filtered = |plan: Vec<String>| -> i64 {
            plan.iter()
                .filter_map(|line| line.split("Rows Removed by Filter: ").nth(1))
                .map(|n| n.trim().parse::<i64>().expect("filtered row count"))
                .sum()
        };
        let explain = |statement: &str| {
            format!("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {statement}")
        };
        let epoch_plan: Vec<String> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(explain(CLAIMED_SCAN_EPOCH_SQL)))
                .fetch_all(pool)
                .await?;
        let list_plan: Vec<String> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(explain(LIST_CLAIMED_SQL)))
                .bind(ROWS)
                .bind(None::<SnapshotId>)
                .bind(epoch.through)
                .fetch_all(pool)
                .await?;
        for plan in [epoch_plan, list_plan] {
            let shown = plan.join("\n");
            assert!(filtered(plan) < SAMPLED_CLAIMS, "{shown}");
        }
        Ok(())
    }

    /// Real store/claims/finalization and production deletion cap coverage,
    /// with optional real backend latency via `benchmark_backend_delay`.
    ///
    /// **Main CI only**, gated on `SPACE_CI_MAIN_ONLY_TESTS`. Seeding 100,000
    /// sandbox histories and committing 4,000 claims costs tens of seconds on
    /// every run, while the reconciler tick it exists to exercise is a
    /// fraction of a second of that. Nothing it asserts is unique to the
    /// scale: the delete cap is pinned by `t12_max_per_tick_caps_processing`
    /// and `benchmark_reconciler_sparse_and_dense_backend`, the
    /// discovery/claim/finalize round trip against real Postgres by
    /// `bounded_discovery_crosses_keepers_and_preserves_partial_budget_on_restart`
    /// and the `p3`..`p18` claim tests. What the full-scale run adds is
    /// confidence that the same numbers hold when the planner is working with
    /// production-sized statistics, which is worth one run per merge rather
    /// than one per pull request.
    #[tokio::test(flavor = "current_thread")]
    async fn benchmark_postgres_reconciler_current_and_week_policy() -> TestResult {
        if !super::main_only_tests() {
            // Deliberately not `report_dependency_skip`: every dependency this
            // test needs is present, and a SPACE-TEST-DEP-SKIP marker would
            // be counted by the CI soak audit as missing infrastructure.
            eprintln!(
                "skipping benchmark_postgres_reconciler_current_and_week_policy: \
                 main-CI-only test, set SPACE_CI_MAIN_ONLY_TESTS=1 to run it"
            );
            return Ok(());
        }
        let delay = super::benchmark_backend_delay();
        for (keep_n, sparse, min_age) in [
            (5, true, None),
            (1, false, Some(std::time::Duration::from_secs(604_800))),
        ] {
            let Some(db) = try_pg_test_db().await? else {
                return Ok(());
            };
            let pool = &db.pool;
            let histories = if sparse { 100_000 } else { 1_000 };
            sqlx::query(sql!(
                "INSERT INTO sandboxes(id,user_id,org_id,network_policy_generation)
                 SELECT ('10000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                        $1::uuid,$2::uuid,1 FROM generate_series(1,$3::int) g"
            ))
            .bind(SEEDED_USER_ID)
            .bind(SEEDED_ORG_ID)
            .bind(histories)
            .execute(pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO snapshots(id,sandbox_id,type,state,timestamp)
                 SELECT ('20000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                        ('10000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                        'full','ready',NOW()-INTERVAL '14 days'
                   FROM generate_series(1,$1::int) g"
            ))
            .bind(histories)
            .execute(pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO snapshots(id,sandbox_id,type,state,timestamp)
                 SELECT space_uuid_v7(),
                        ('10000000-0000-7000-8000-' || lpad(to_hex(g),12,'0'))::uuid,
                        'full','ready',NOW()-INTERVAL '1 day'
                   FROM generate_series(1,$1::int) g CROSS JOIN generate_series(1,$2::int) k
                  WHERE g % $3::int = 0"
            ))
            .bind(histories)
            .bind(keep_n)
            .bind(if sparse { 100 } else { 1 })
            .execute(pool)
            .await?;
            // Explicit ids: the column default is the PL/pgSQL
            // `space_uuid_v7()`, which costs about 8x a `gen_random_uuid()`
            // call and is paid 105,000 times here. Location ids are never
            // ordered on — discovery joins them by `snapshot_id` and the
            // unique constraint keeps the join one-to-one — so the id
            // generator is free to change. The snapshot ids above are not:
            // discovery pages by snapshot id, so their ordering relative to
            // the explicitly numbered rows is part of the fixture.
            sqlx::query(sql!(
                "INSERT INTO snapshot_storage_locations(id,snapshot_id,storage_connection_string)
                 SELECT gen_random_uuid(), id, 's3://test-bucket/snap/' || id FROM snapshots"
            ))
            .execute(pool)
            .await?;
            for query in [
                "ANALYZE snapshots",
                "ANALYZE sandboxes",
                "ANALYZE snapshot_storage_locations",
            ] {
                sqlx::query(sqlx::AssertSqlSafe(sql!(query)))
                    .execute(pool)
                    .await?;
            }
            let backend = std::sync::Arc::new(super::FakeBackend {
                delay,
                ..Default::default()
            });
            let mut policy = super::config(1_000);
            policy.keep_n = u32::try_from(keep_n)?;
            policy.min_age = min_age;
            let mut reconciler = super::Reconciler::new(
                policy,
                SnapshotRetentionDbStore::new(pool.clone()),
                super::registry_with(backend.clone()),
            );
            let started = std::time::Instant::now();
            let report = reconciler.tick().await;
            println!(
                "postgres=true sparse={sparse} keep_n={keep_n} ttl={min_age:?} candidate_cap=2000 delete_cap=1000 concurrency=1 backend_ms={} elapsed_ms={} pruned={} backend_calls={}",
                delay.as_millis(),
                started.elapsed().as_millis(),
                report.pruned,
                backend.calls().len()
            );
            assert_eq!(report.pruned, 1_000);
            assert_eq!(backend.calls().len(), 1_000);
            let remaining: i64 = sqlx::query_scalar(sql!("SELECT count(*) FROM snapshots"))
                .fetch_one(pool)
                .await?;
            assert_eq!(
                remaining,
                i64::from(histories) + 1_000 * i64::from(keep_n) - 1_000
            );
        }
        Ok(())
    }

    fn expect_claimed(outcome: ClaimOutcome) -> PruneCandidate {
        match outcome {
            ClaimOutcome::Claimed(candidate) => candidate,
            ClaimOutcome::Refused(reason) => {
                panic!("snapshot prune claim was unexpectedly refused: {reason}")
            }
        }
    }

    async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
        let sandbox_id = SandboxId::new();
        sqlx::query(
            "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
             VALUES ($1, $2::uuid, $3::uuid, 1)",
        )
        .bind(sandbox_id)
        .bind(SEEDED_USER_ID)
        .bind(SEEDED_ORG_ID)
        .execute(pool)
        .await?;
        Ok(sandbox_id)
    }

    /// Insert a snapshot dated `age_secs` in the past, plus its storage
    /// location. Callers insert oldest-first so the UUIDv7 ids increase
    /// with recency and agree with the `timestamp DESC` rank ordering.
    async fn insert_snapshot(
        pool: &PgPool,
        sandbox_id: SandboxId,
        snapshot_type: &str,
        state: &str,
        age_secs: i64,
    ) -> TestResult<SnapshotId> {
        let id = SnapshotId::new();
        insert_snapshot_with_id(pool, sandbox_id, id, snapshot_type, state, age_secs).await?;
        Ok(id)
    }

    async fn insert_snapshot_with_id(
        pool: &PgPool,
        sandbox_id: SandboxId,
        id: SnapshotId,
        snapshot_type: &str,
        state: &str,
        age_secs: i64,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp)
             VALUES ($1, $2, $3, $4, NOW() - make_interval(secs => $5::BIGINT))",
        )
        .bind(id)
        .bind(sandbox_id)
        .bind(snapshot_type)
        .bind(state)
        .bind(age_secs)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)",
        )
        .bind(id)
        .bind(format!("s3://test-bucket/snap/{id}?region=us-east-1"))
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn set_equal_timestamp(pool: &PgPool, ids: &[SnapshotId], age_secs: i64) -> TestResult {
        sqlx::query(
            "UPDATE snapshots
                SET timestamp = NOW() - make_interval(secs => $2::BIGINT)
              WHERE id = ANY($1)",
        )
        .bind(ids)
        .bind(age_secs)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn ranked_candidate_ids(
        pool: &PgPool,
        keep_n: i64,
        limit: i64,
        after: Option<SnapshotId>,
        min_age: Option<std::time::Duration>,
    ) -> TestResult<Vec<SnapshotId>> {
        let min_age_secs = min_age.map(|duration| duration.as_secs() as i64);
        // Keep the former production CTE intact as a test oracle. Comparing
        // complete pages against it makes semantic drift in the bounded
        // query visible without preserving the full-history scan at runtime.
        Ok(sqlx::query_scalar(
            "WITH ranked AS (
                 SELECT
                     s.id AS snapshot_id,
                     s.timestamp,
                     ROW_NUMBER() OVER (
                         PARTITION BY s.sandbox_id
                         ORDER BY s.timestamp DESC, s.id DESC
                     ) AS rn
                 FROM snapshots s
                 WHERE s.state = 'ready' AND s.type = 'full'
             )
             SELECT r.snapshot_id
             FROM ranked r
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = r.snapshot_id
             WHERE r.rn > $1
               AND ($3::UUID IS NULL OR r.snapshot_id > $3)
               AND ($4::BIGINT IS NULL
                    OR r.timestamp < NOW() - make_interval(secs => $4::BIGINT))
             ORDER BY r.snapshot_id ASC
             LIMIT $2",
        )
        .bind(keep_n)
        .bind(limit)
        .bind(after)
        .bind(min_age_secs)
        .fetch_all(pool)
        .await?)
    }

    /// Insert a `ready` full snapshot that carries an artifact identity,
    /// plus its storage location. The schema's identity check requires
    /// `artifact_origin` alongside the id and kind; `suspend` is the
    /// origin a live suspended instance's artifact carries.
    async fn insert_artifact_snapshot(
        pool: &PgPool,
        sandbox_id: SandboxId,
        age_secs: i64,
        artifact_id: Uuid,
        artifact_kind: &str,
    ) -> TestResult<SnapshotId> {
        // Reuse `insert_snapshot` so both helpers share one storage-location
        // shape; only the artifact identity is layered on afterwards.
        let id = insert_snapshot(pool, sandbox_id, "full", "ready", age_secs).await?;
        sqlx::query(
            "UPDATE snapshots
                SET local_artifact_id = $2,
                    artifact_kind = $3,
                    artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(id)
        .bind(artifact_id)
        .bind(artifact_kind)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// Point the sandbox's current-artifact marker at `artifact_id`, the
    /// identity a suspended restore resolves its source snapshot by.
    async fn pin_current_artifact(
        pool: &PgPool,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        artifact_kind: &str,
    ) -> TestResult<()> {
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = $3,
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(artifact_kind)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn snapshot_state(pool: &PgPool, id: SnapshotId) -> TestResult<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT state FROM snapshots WHERE id = $1")
                .bind(id)
                .fetch_optional(pool)
                .await?,
        )
    }

    async fn snapshot_exists(pool: &PgPool, id: SnapshotId) -> TestResult<bool> {
        Ok(
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM snapshots WHERE id = $1)")
                .bind(id)
                .fetch_one(pool)
                .await?,
        )
    }

    async fn location_exists(pool: &PgPool, snapshot_id: SnapshotId) -> TestResult<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM snapshot_storage_locations WHERE snapshot_id = $1)",
        )
        .bind(snapshot_id)
        .fetch_one(pool)
        .await?)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p1_candidate_probe_matches_rank_at_keep_floor() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let mut expected = Vec::new();
        let mut six_snapshot_sandbox = None;
        for count in 0_i64..=8 {
            let sandbox = insert_sandbox(pool).await?;
            if count == 6 {
                six_snapshot_sandbox = Some(sandbox);
            }
            let mut ids = Vec::new();
            for age in (1_i64..=count).rev().map(|rank| rank * 100) {
                ids.push(insert_snapshot(pool, sandbox, "full", "ready", age).await?);
            }
            expected.extend(ids.into_iter().take(usize::try_from((count - 5).max(0))?));
        }

        // These rows are interleaved with the ready/full history but
        // neither count toward the keep floor nor become candidates.
        let six_snapshot_sandbox = six_snapshot_sandbox.expect("six-snapshot fixture is present");
        insert_snapshot(pool, six_snapshot_sandbox, "full", "creating", 25).await?;
        insert_snapshot(pool, six_snapshot_sandbox, "full", "failed", 20).await?;
        insert_snapshot(pool, six_snapshot_sandbox, "disk", "ready", 15).await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let candidates = store.list_candidates(5, 100, None, None).await?.candidates;
        let got: Vec<SnapshotId> = candidates.iter().map(|c| c.snapshot_id).collect();
        let ranked = ranked_candidate_ids(pool, 5, 100, None, None).await?;

        expected.sort_unstable();
        assert_eq!(got, expected);
        assert_eq!(got, ranked);
        assert!(candidates.iter().all(|c| c.age_secs > 0));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p2_candidate_probe_preserves_ties_age_and_pagination() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        // All six snapshots have the same timestamp. The UUID is the
        // deterministic tie-breaker, so the lowest UUID is the only row
        // outside a keep-five set.
        let tie_sandbox = insert_sandbox(pool).await?;
        let tie_ids: Vec<SnapshotId> = (1..=6)
            .map(|n| SnapshotId::from_uuid(Uuid::from_u128(n)))
            .collect();
        for id in &tie_ids {
            insert_snapshot_with_id(pool, tie_sandbox, *id, "full", "ready", 7200).await?;
        }
        set_equal_timestamp(pool, &tie_ids, 7200).await?;

        // The candidate is old enough, while all five newer siblings are
        // younger than the grace period. Age applies only to the candidate;
        // the young siblings must still satisfy the keep-five floor.
        let mixed_age_sandbox = insert_sandbox(pool).await?;
        let old_candidate = insert_snapshot(pool, mixed_age_sandbox, "full", "ready", 7200).await?;
        for age in [50, 40, 30, 20, 10] {
            insert_snapshot(pool, mixed_age_sandbox, "full", "ready", age).await?;
        }

        // Two more old candidates make the first page full and the second
        // page short. A separate all-young history proves that an otherwise
        // eligible candidate is excluded by its own age.
        let paged_sandbox = insert_sandbox(pool).await?;
        let mut paged_ids = Vec::new();
        for age in [7700, 7600, 7500, 7400, 7300, 7200, 7100] {
            paged_ids.push(insert_snapshot(pool, paged_sandbox, "full", "ready", age).await?);
        }
        let young_sandbox = insert_sandbox(pool).await?;
        for age in [60, 50, 40, 30, 20, 10] {
            insert_snapshot(pool, young_sandbox, "full", "ready", age).await?;
        }

        let min_age = Some(std::time::Duration::from_secs(3600));
        let mut expected = [tie_ids[0], old_candidate, paged_ids[0], paged_ids[1]];
        expected.sort_unstable();

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let first_page = store.list_candidates(5, 3, None, min_age).await?.candidates;
        let first_ids: Vec<SnapshotId> = first_page
            .iter()
            .map(|candidate| candidate.snapshot_id)
            .collect();
        assert_eq!(first_ids, expected[..3]);
        assert_eq!(
            first_ids,
            ranked_candidate_ids(pool, 5, 3, None, min_age).await?
        );

        let after = first_ids.last().copied();
        let second_page = store
            .list_candidates(5, 3, after, min_age)
            .await?
            .candidates;
        let second_ids: Vec<SnapshotId> = second_page
            .iter()
            .map(|candidate| candidate.snapshot_id)
            .collect();
        assert_eq!(second_ids, expected[3..]);
        assert_eq!(
            second_ids,
            ranked_candidate_ids(pool, 5, 3, after, min_age).await?
        );
        assert_eq!(first_page.len(), 3, "the first page must reach its limit");
        assert_eq!(second_page.len(), 1, "the final page must be short");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p3_claim_prune_enforces_the_keep_n_floor() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let oldest = insert_snapshot(pool, sandbox, "full", "ready", 400).await?;
        let second = insert_snapshot(pool, sandbox, "full", "ready", 300).await?;
        let third = insert_snapshot(pool, sandbox, "full", "ready", 200).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        expect_claimed(store.claim_prune(oldest, 2).await?);
        expect_claimed(store.claim_prune(second, 2).await?);
        assert_eq!(
            store.claim_prune(third, 2).await?,
            ClaimOutcome::Refused(ClaimRefusalReason::KeepNFloor)
        );
        assert_eq!(
            snapshot_state(pool, oldest).await?.as_deref(),
            Some("deleting")
        );
        assert_eq!(snapshot_state(pool, third).await?.as_deref(), Some("ready"));
        assert!(location_exists(pool, oldest).await?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p3b_unusable_newer_snapshot_does_not_satisfy_keep_floor() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let store = SnapshotRetentionDbStore::new(pool.clone());
        for orphaned in [false, true] {
            let sandbox = insert_sandbox(pool).await?;
            let durable = insert_snapshot(pool, sandbox, "full", "ready", 200).await?;
            let unusable = insert_snapshot(pool, sandbox, "full", "ready", 100).await?;
            if orphaned {
                sqlx::query(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id = $1",
                )
                .bind(unusable)
                .execute(pool)
                .await?;
            } else {
                sqlx::query("DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1")
                    .bind(unusable)
                    .execute(pool)
                    .await?;
            }

            let candidates = store.list_candidates(1, 100, None, None).await?.candidates;
            assert!(
                candidates
                    .iter()
                    .all(|candidate| candidate.snapshot_id != durable),
                "an unusable sibling cannot authorize pruning the last durable copy"
            );
            assert_eq!(
                store.claim_prune(durable, 1).await?,
                ClaimOutcome::Refused(ClaimRefusalReason::KeepNFloor)
            );
            assert_eq!(
                snapshot_state(pool, durable).await?.as_deref(),
                Some("ready")
            );
            assert!(location_exists(pool, durable).await?);
            if orphaned {
                sqlx::query(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NULL WHERE snapshot_id = $1",
                )
                .bind(unusable)
                .execute(pool)
                .await?;
                assert!(
                    store
                        .list_candidates(1, 100, None, None)
                        .await?
                        .candidates
                        .iter()
                        .any(|candidate| candidate.snapshot_id == durable)
                );
                assert!(matches!(
                    store.claim_prune(durable, 1).await?,
                    ClaimOutcome::Claimed(_)
                ));
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p4_finalize_deletes_snapshot_and_storage_location() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let oldest = insert_artifact_snapshot(pool, sandbox, 300, Uuid::now_v7(), "full").await?;
        insert_snapshot(pool, sandbox, "full", "ready", 200).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        // Protocol order: claim (de-advertise) first, finalize after the
        // (out-of-band) reap.
        expect_claimed(store.claim_prune(oldest, 2).await?);
        assert_eq!(
            store.finalize_prune(oldest, 2).await?,
            PruneFinalize::Pruned {
                floor_ok: true,
                marker_ok: true
            }
        );
        assert!(!snapshot_exists(pool, oldest).await?);
        assert!(!location_exists(pool, oldest).await?);
        // A second finalize of the now-gone row is a benign no-op — the
        // crash-after-byte-delete recovery shape.
        assert_eq!(
            store.finalize_prune(oldest, 2).await?,
            PruneFinalize::AlreadyGone
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p4b_finalize_refuses_an_unclaimed_snapshot() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let snapshot = insert_snapshot(pool, sandbox, "full", "ready", 300).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        assert_eq!(
            store.finalize_prune(snapshot, 1).await?,
            PruneFinalize::NotClaimed
        );
        assert_eq!(
            snapshot_state(pool, snapshot).await?.as_deref(),
            Some("ready")
        );
        assert!(location_exists(pool, snapshot).await?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p5_finalize_below_floor_still_deletes_and_reports_breach() -> TestResult {
        // Regression for the reap→finalize race: even when fewer than
        // `keep_n` newer ready snapshots remain (a newer one was deleted
        // concurrently after the reap was authorized), finalize must still
        // delete the now-unrestorable row — never leave a `ready` row with
        // no backing storage — and report the floor breach.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let oldest = insert_snapshot(pool, sandbox, "full", "ready", 300).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 200).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        // The claim passes at keep_n = 2 with two newer ready siblings...
        expect_claimed(store.claim_prune(oldest, 2).await?);
        // ...then finalize re-counts at keep_n = 5 (standing in for an
        // external deleter shrinking the keep-set in the claim→finalize
        // window) → floor breached, yet the unrestorable row is still
        // removed.
        assert_eq!(
            store.finalize_prune(oldest, 5).await?,
            PruneFinalize::Pruned {
                floor_ok: false,
                marker_ok: true
            }
        );
        assert!(!snapshot_exists(pool, oldest).await?);
        assert!(!location_exists(pool, oldest).await?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p6_pinned_current_artifact_is_never_a_candidate() -> TestResult {
        // A suspended sandbox restores by *exact artifact identity*, not by
        // recency: its marker names the snapshot restore will load, and the
        // suspended selector has no newest-snapshot fallback. Recency rank
        // alone would surface that snapshot once `keep_n` newer ones exist,
        // and pruning it destroys the sandbox's only restore source.
        //
        // The newer rows here deliberately carry *no* artifact identity
        // while the sandbox *is* marked. That is the mirror of `p8` and the
        // only place the guard's `sn.local_artifact_id IS NOT NULL` arm is
        // exercised through the real query: without it those rows compare
        // `NULL = <uuid>` → NULL, and `NOT NULL` filters them out of the
        // candidate set instead of keeping them. Don't "simplify" this into
        // `p9` — that test gives every row an identity and loses this case.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let pinned_artifact = Uuid::now_v7();
        // Oldest-first. The oldest row carries the artifact the sandbox is
        // pinned to, so it is the oldest of five and has four newer ready
        // full siblings — well past keep_n=2.
        let pinned = insert_artifact_snapshot(pool, sandbox, 500, pinned_artifact, "full").await?;
        let mut newer = Vec::new();
        for age in [400, 300, 200, 100] {
            newer.push(insert_snapshot(pool, sandbox, "full", "ready", age).await?);
        }
        pin_current_artifact(pool, sandbox, pinned_artifact, "full").await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let candidates = store.list_candidates(2, 100, None, None).await?.candidates;
        let got: Vec<SnapshotId> = candidates.iter().map(|c| c.snapshot_id).collect();

        assert!(
            !got.contains(&pinned),
            "the sandbox's tracked current-artifact restore target must never \
             be a prune candidate, got {got:?}"
        );
        // The guard is targeted, not a blanket per-sandbox exemption: the
        // other out-of-keep-set snapshots stay prunable.
        for id in [newer[0], newer[1]] {
            assert!(got.contains(&id), "unpinned old snapshot {id} should prune");
        }
        assert_eq!(got.len(), 2);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7_claim_prune_refuses_the_pinned_current_artifact() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let pinned_artifact = Uuid::now_v7();
        let pinned = insert_artifact_snapshot(pool, sandbox, 500, pinned_artifact, "full").await?;
        for age in [400, 300, 200, 100] {
            insert_snapshot(pool, sandbox, "full", "ready", age).await?;
        }

        let store = SnapshotRetentionDbStore::new(pool.clone());
        pin_current_artifact(pool, sandbox, pinned_artifact, "full").await?;
        assert_eq!(
            store.claim_prune(pinned, 2).await?,
            ClaimOutcome::Refused(ClaimRefusalReason::ProtectedRestoreSource)
        );
        assert_eq!(
            snapshot_state(pool, pinned).await?.as_deref(),
            Some("ready")
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7b_claim_revalidates_marker_after_waiting_for_the_sandbox_lock() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let artifact = Uuid::now_v7();
        let candidate = insert_artifact_snapshot(pool, sandbox, 500, artifact, "full").await?;
        for age in [400, 300, 200, 100] {
            insert_snapshot(pool, sandbox, "full", "ready", age).await?;
        }

        let mut marker_writer = pool.begin().await?;
        sqlx::query("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE")
            .bind(sandbox)
            .fetch_one(&mut *marker_writer)
            .await?;

        let claim_pool = pool.clone();
        let claim = tokio::spawn(async move {
            SnapshotRetentionDbStore::new(claim_pool)
                .claim_prune(candidate, 2)
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !claim.is_finished(),
            "the claim must wait behind the marker writer's sandbox lock"
        );

        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox)
        .bind(artifact)
        .execute(&mut *marker_writer)
        .await?;
        marker_writer.commit().await?;

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), claim).await???;
        assert_eq!(
            outcome,
            ClaimOutcome::Refused(ClaimRefusalReason::ProtectedRestoreSource)
        );
        assert_eq!(
            snapshot_state(pool, candidate).await?.as_deref(),
            Some("ready"),
            "post-wait revalidation must refuse before committing the claim"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p8_markerless_sandbox_still_prunes_its_old_artifacts() -> TestResult {
        // Rollout-skew rows carry artifact identities on their snapshots but
        // a NULL marker. The guard must not over-match: a NULL marker shields
        // nothing, otherwise the sweep silently stops reclaiming anything.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let oldest = insert_artifact_snapshot(pool, sandbox, 500, Uuid::now_v7(), "full").await?;
        for age in [400, 300] {
            insert_artifact_snapshot(pool, sandbox, age, Uuid::now_v7(), "full").await?;
        }

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let got: Vec<SnapshotId> = store
            .list_candidates(2, 100, None, None)
            .await?
            .candidates
            .iter()
            .map(|c| c.snapshot_id)
            .collect();
        assert_eq!(got, vec![oldest], "markerless rows keep pruning by recency");
        expect_claimed(store.claim_prune(oldest, 2).await?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p9_marker_shields_only_its_exact_artifact_identity() -> TestResult {
        // The marker is matched on (id, kind) exactly, mirroring how the
        // restore selector resolves it. A different artifact id, or the same
        // id under a different kind, is a different artifact and stays
        // prunable — otherwise one marker would shield unrelated snapshots.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let pinned_artifact = Uuid::now_v7();
        let other_artifact = Uuid::now_v7();
        // Same artifact id as the marker but kind `disk`, so it is a
        // different artifact identity and must not be shielded.
        let wrong_kind =
            insert_artifact_snapshot(pool, sandbox, 600, pinned_artifact, "disk").await?;
        let wrong_id = insert_artifact_snapshot(pool, sandbox, 500, other_artifact, "full").await?;
        let pinned = insert_artifact_snapshot(pool, sandbox, 400, pinned_artifact, "full").await?;
        for age in [300, 200] {
            insert_snapshot(pool, sandbox, "full", "ready", age).await?;
        }
        pin_current_artifact(pool, sandbox, pinned_artifact, "full").await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let got: Vec<SnapshotId> = store
            .list_candidates(2, 100, None, None)
            .await?
            .candidates
            .iter()
            .map(|c| c.snapshot_id)
            .collect();
        assert!(
            !got.contains(&pinned),
            "exact marker match must be shielded"
        );
        assert!(
            got.contains(&wrong_kind),
            "same artifact id under a different kind is a different artifact"
        );
        assert!(got.contains(&wrong_id), "unrelated artifact stays prunable");
        expect_claimed(store.claim_prune(wrong_kind, 2).await?);
        expect_claimed(store.claim_prune(wrong_id, 2).await?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p11_restoring_retry_source_is_shielded_from_pruning() -> TestResult {
        // A row already in `restoring` does NOT restore by marker — it
        // retries from the pre-restore source window, deliberately ignoring
        // later artifacts a failed restore branch published after the row
        // entered `restoring`. Those later artifacts still count toward the
        // retention rank, so once `keep_n` of them pile up the retry's actual
        // source falls outside the keep-set. Recency rank plus the marker
        // guard alone would prune it and strand a sandbox mid-restore.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        // The pre-restore source, taken after the last placement move.
        let retry_source =
            insert_artifact_snapshot(pool, sandbox, 5000, Uuid::now_v7(), "full").await?;
        // Restore claims the row. The marker points at a *different* artifact
        // than the retry source — which is precisely why the marker guard
        // does not cover this case.
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'restoring',
                    placement_changed_at = NOW() - make_interval(secs => 6000),
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
        // `sandboxes_touch_lifecycle_changed_at` forces `lifecycle_changed_at`
        // to NOW() on any lifecycle change, so backdate it in a second update
        // that leaves `lifecycle` alone (the trigger only fires when the
        // lifecycle value actually changes). Without this the restore claim
        // would be "now" and no snapshot could postdate it.
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle_changed_at = NOW() - make_interval(secs => 4000)
              WHERE id = $1",
        )
        .bind(sandbox)
        .execute(pool)
        .await?;
        // Five later artifacts published by failed restore branches — all
        // postdating the restore claim, so the retry window excludes them
        // while the retention rank still counts them, pushing the real source
        // out to rank 6.
        let mut failed_branch = Vec::new();
        for age in [900, 800, 700, 600, 500] {
            failed_branch
                .push(insert_artifact_snapshot(pool, sandbox, age, Uuid::now_v7(), "full").await?);
        }

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let got: Vec<SnapshotId> = store
            .list_candidates(5, 100, None, None)
            .await?
            .candidates
            .iter()
            .map(|c| c.snapshot_id)
            .collect();
        assert!(
            !got.contains(&retry_source),
            "the pre-restore retry source must not be a candidate, got {got:?}"
        );
        assert_eq!(
            store.claim_prune(retry_source, 5).await?,
            ClaimOutcome::Refused(ClaimRefusalReason::ProtectedRestoreSource),
            "the pre-restore retry source must not be claimed for deletion"
        );
        // The failed-branch artifacts sit inside the keep-set, so nothing is
        // prunable here at all.
        for id in failed_branch {
            assert!(!got.contains(&id), "keep-set row {id} should not prune");
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p13_marker_on_a_restoring_row_is_not_a_breach() -> TestResult {
        // A `restoring` row's selector deliberately ignores the marker and
        // reads the pre-restore window instead, so the marker is not that
        // row's restore source. A failed restore branch publishing a late
        // artifact that becomes the marker is therefore not a destroyed
        // restore target, and must not raise the data-loss alarm — even
        // though the exact-marker predicate matches.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let artifact = Uuid::now_v7();
        let pinned = insert_artifact_snapshot(pool, sandbox, 500, artifact, "full").await?;
        for age in [400, 300, 200] {
            insert_snapshot(pool, sandbox, "full", "ready", age).await?;
        }
        let store = SnapshotRetentionDbStore::new(pool.clone());
        // The claim lands while the sandbox is unmarked and not restoring...
        expect_claimed(store.claim_prune(pinned, 2).await?);
        // ...then the row enters `restoring` with the marker pointing at the
        // claimed artifact — same fixture as `p10`, except mid-restore.
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'restoring',
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox)
        .bind(artifact)
        .execute(pool)
        .await?;

        assert_eq!(
            store.finalize_prune(pinned, 2).await?,
            PruneFinalize::Pruned {
                floor_ok: true,
                marker_ok: true
            },
            "the marker is not the restore source while the row is restoring, \
             so pruning it must not report a destroyed target"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p12_retry_window_membership_alone_is_not_a_breach() -> TestResult {
        // The deletion guard is deliberately wider than the restore selector
        // — it shields every row in a `restoring` row's retry window, while
        // the selector loads only the newest. Finalize's breach telemetry
        // must NOT inherit that width: a row that merely joined the window
        // after authorization has not destroyed anyone's restore target, and
        // reporting it would raise a false data-loss alarm on a metric whose
        // whole contract is "expected zero, investigate if not".
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let older = insert_artifact_snapshot(pool, sandbox, 5000, Uuid::now_v7(), "full").await?;
        // A newer in-window row — this is the one the selector would load.
        insert_artifact_snapshot(pool, sandbox, 4500, Uuid::now_v7(), "full").await?;
        for age in [300, 200] {
            insert_snapshot(pool, sandbox, "full", "ready", age).await?;
        }
        let store = SnapshotRetentionDbStore::new(pool.clone());
        // The claim lands before the restore starts...
        expect_claimed(store.claim_prune(older, 2).await?);
        // ...then the row enters `restoring` pinned to some *other*
        // artifact, so `older` is inside the retry window but is not the
        // exact marker.
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'restoring',
                    placement_changed_at = NOW() - make_interval(secs => 6000),
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;

        assert_eq!(
            store.finalize_prune(older, 2).await?,
            PruneFinalize::Pruned {
                floor_ok: true,
                marker_ok: true
            },
            "retry-window membership alone must not report a destroyed \
             restore target"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p10_finalize_reports_a_marker_that_landed_after_the_claim() -> TestResult {
        // The claim de-advertises the row, then the sandbox marker changes to
        // the claimed artifact before finalization.
        // Finalize must still delete the row (its blobs are gone, so it is
        // unrestorable) but report the breach so the loss is not silent.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let artifact = Uuid::now_v7();
        let oldest = insert_artifact_snapshot(pool, sandbox, 500, artifact, "full").await?;
        for age in [400, 300, 200] {
            insert_snapshot(pool, sandbox, "full", "ready", age).await?;
        }

        let store = SnapshotRetentionDbStore::new(pool.clone());
        // Claimed while unpinned...
        expect_claimed(store.claim_prune(oldest, 2).await?);
        // ...then the marker changes before the reap completes.
        pin_current_artifact(pool, sandbox, artifact, "full").await?;
        assert_eq!(
            store.finalize_prune(oldest, 2).await?,
            PruneFinalize::Pruned {
                floor_ok: true,
                marker_ok: false
            }
        );
        assert!(!snapshot_exists(pool, oldest).await?);
        assert!(!location_exists(pool, oldest).await?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p14_claimed_rows_finish_forward_through_the_recovery_scan() -> TestResult {
        // Finish-forward discovery: a `deleting` row (claimed by a crashed
        // process) must come back from `list_claimed`, bypassing the keep-N
        // floor, the grace, and the restore-source guard — none of which can
        // apply to a claim that can only move forward. A marker pointing at
        // the claimed artifact must not hide it either.
        // And the claim must be *invisible* to `list_candidates`: claims page
        // separately so a parked backlog cannot crowd out fresh work or drag
        // its cursor.
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let artifact = Uuid::now_v7();
        let claimed = insert_artifact_snapshot(pool, sandbox, 500, artifact, "full").await?;
        let claimed_second = insert_snapshot(pool, sandbox, "full", "ready", 400).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        // Simulate a crashed claimer holding two claims: the rows are
        // durably `deleting`, and the sandbox is even marked at the first
        // (worst case for rediscovery).
        sqlx::query("UPDATE snapshots SET state = 'deleting' WHERE id = ANY($1)")
            .bind(vec![claimed, claimed_second])
            .execute(pool)
            .await?;
        pin_current_artifact(pool, sandbox, artifact, "full").await?;

        let epoch = store
            .claimed_scan_epoch()
            .await?
            .expect("inserted claims establish a high-water mark");
        let through = epoch.through;
        let recovered = store.list_claimed(100, None, through).await?;
        let ids: Vec<SnapshotId> = recovered.iter().map(|c| c.snapshot_id).collect();
        assert!(
            ids.contains(&claimed) && ids.contains(&claimed_second),
            "every outstanding claim must surface through the recovery scan"
        );

        // The recovery page rotates on its own keyset cursor: a one-row
        // page, then a cursor past it, must reach the claim parked behind
        // the first — this is what keeps a large parked cohort from
        // permanently hiding the claims sorted after it.
        let (first, second) = if claimed < claimed_second {
            (claimed, claimed_second)
        } else {
            (claimed_second, claimed)
        };
        let page = store.list_claimed(1, None, through).await?;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].snapshot_id, first);
        let behind = store.list_claimed(1, Some(first), through).await?;
        assert_eq!(behind.len(), 1);
        assert_eq!(behind[0].snapshot_id, second);

        // No guard hides a claim, and no claim leaks into the fresh scan:
        // with every guard maximally permissive, `list_candidates` must
        // still return only `ready` rows.
        let candidates = store.list_candidates(1, 100, None, None).await?.candidates;
        assert!(
            candidates
                .iter()
                .all(|c| c.snapshot_id != claimed && c.snapshot_id != claimed_second),
            "claimed rows must never occupy the fresh candidate page"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p15_claimed_row_projects_one_optional_location() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let claimed = insert_snapshot(pool, sandbox, "full", "deleting", 500).await?;
        sqlx::query("DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1")
            .bind(claimed)
            .execute(pool)
            .await?;

        let store = SnapshotRetentionDbStore::new(pool.clone());
        let through = store
            .claimed_scan_epoch()
            .await?
            .expect("the committed claim establishes a high-water mark")
            .through;
        let recovered = store.list_claimed(100, None, through).await?;

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].snapshot_id, claimed);
        assert!(recovered[0].storage_location.is_none());

        sqlx::query(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)",
        )
        .bind(claimed)
        .bind(format!("s3://test-bucket/first/{claimed}?region=us-east-1"))
        .execute(pool)
        .await?;
        let recovered = store.list_claimed(100, None, through).await?;
        assert_eq!(recovered.len(), 1, "one claim must occupy one page row");
        assert_eq!(recovered[0].snapshot_id, claimed);
        assert!(recovered[0].storage_location.is_some());

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p16_unusable_location_is_projected_once_and_claim_refuses() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let store = SnapshotRetentionDbStore::new(pool.clone());

        for orphaned in [false, true] {
            let sandbox = insert_sandbox(pool).await?;
            let unusable = insert_snapshot(pool, sandbox, "full", "ready", 500).await?;
            insert_snapshot(pool, sandbox, "full", "ready", 100).await?;
            if orphaned {
                sqlx::query(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id = $1",
                )
                .bind(unusable)
                .execute(pool)
                .await?;
            } else {
                sqlx::query("DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1")
                    .bind(unusable)
                    .execute(pool)
                    .await?;
            }

            let candidates = store.list_candidates(1, 100, None, None).await?.candidates;
            let unusable_candidate = candidates
                .iter()
                .find(|candidate| candidate.snapshot_id == unusable)
                .expect("the unusable route should surface for repair telemetry");
            assert!(unusable_candidate.storage_location.is_none());
            assert_eq!(
                store.claim_prune(unusable, 1).await?,
                ClaimOutcome::Refused(ClaimRefusalReason::StorageLocationUnusable),
                "missing and orphaned locations are both unusable"
            );
            assert_eq!(
                snapshot_state(pool, unusable).await?.as_deref(),
                Some("ready")
            );
            if orphaned {
                sqlx::query(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NULL WHERE snapshot_id = $1",
                )
                .bind(unusable)
                .execute(pool)
                .await?;
                let page = store.list_candidates(1, 100, None, None).await?;
                let candidate = page
                    .candidates
                    .iter()
                    .find(|candidate| candidate.snapshot_id == unusable)
                    .expect("a repaired location remains discoverable");
                assert!(candidate.storage_location.is_some());
                assert!(matches!(
                    store.claim_prune(unusable, 1).await?,
                    ClaimOutcome::Claimed(_)
                ));
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p17_claim_revalidates_database_age_and_the_exact_location() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let age_sandbox = insert_sandbox(pool).await?;
        let aged = insert_snapshot(pool, age_sandbox, "full", "ready", 500).await?;
        insert_snapshot(pool, age_sandbox, "full", "ready", 100).await?;
        let store = SnapshotRetentionDbStore::new(pool.clone());
        let aged_candidate = store
            .list_candidates(1, 100, None, None)
            .await?
            .candidates
            .into_iter()
            .find(|candidate| candidate.snapshot_id == aged)
            .expect("the old snapshot is initially eligible");
        sqlx::query(
            "UPDATE snapshots
                SET timestamp = NOW() - INTERVAL '200 seconds'
              WHERE id = $1",
        )
        .bind(aged)
        .execute(pool)
        .await?;
        // Both age and floor fail here; classification reports the floor first.
        assert_eq!(
            <SnapshotRetentionDbStore as RetentionSink>::claim_prune(
                &store,
                aged_candidate.clone(),
                2,
                Some(std::time::Duration::from_secs(300)),
            )
            .await?,
            ClaimOutcome::Refused(ClaimRefusalReason::KeepNFloor),
        );
        assert_eq!(
            <SnapshotRetentionDbStore as RetentionSink>::claim_prune(
                &store,
                aged_candidate,
                1,
                Some(std::time::Duration::from_secs(300)),
            )
            .await?,
            ClaimOutcome::Refused(ClaimRefusalReason::AgeGate),
            "the claim uses the database clock rather than trusting scan-time age"
        );

        let route_sandbox = insert_sandbox(pool).await?;
        let route_snapshot = insert_snapshot(pool, route_sandbox, "full", "ready", 500).await?;
        insert_snapshot(pool, route_sandbox, "full", "ready", 100).await?;
        let route_candidate = store
            .list_candidates(1, 100, None, None)
            .await?
            .candidates
            .into_iter()
            .find(|candidate| candidate.snapshot_id == route_snapshot)
            .expect("the old snapshot is initially eligible");
        sqlx::query(
            "UPDATE snapshot_storage_locations
                SET storage_connection_string = $2
              WHERE snapshot_id = $1",
        )
        .bind(route_snapshot)
        .bind(format!(
            "s3://test-bucket/replaced/{route_snapshot}?region=us-east-1"
        ))
        .execute(pool)
        .await?;
        assert_eq!(
            <SnapshotRetentionDbStore as RetentionSink>::claim_prune(
                &store,
                route_candidate,
                1,
                None,
            )
            .await?,
            ClaimOutcome::Refused(ClaimRefusalReason::StorageLocationChanged),
            "a route changed after validation must never be claimed"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claim_refusal_identifies_missing_rows_and_snapshot_shape() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let snapshot = insert_snapshot(pool, sandbox, "full", "ready", 500).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;
        let store = SnapshotRetentionDbStore::new(pool.clone());
        let candidate = store
            .list_candidates(1, 100, None, None)
            .await?
            .candidates
            .into_iter()
            .find(|candidate| candidate.snapshot_id == snapshot)
            .unwrap();

        for missing_sandbox in [false, true] {
            let mut missing = candidate.clone();
            if missing_sandbox {
                missing.sandbox_id = SandboxId::new();
            } else {
                missing.snapshot_id = SnapshotId::new();
            }
            assert_eq!(
                <SnapshotRetentionDbStore as RetentionSink>::claim_prune(&store, missing, 1, None)
                    .await?,
                ClaimOutcome::Refused(ClaimRefusalReason::MissingSandboxOrSnapshot),
            );
        }
        for (kind, state, reason) in [
            ("full", "creating", ClaimRefusalReason::SnapshotNotReady),
            ("full", "failed", ClaimRefusalReason::SnapshotNotReady),
            ("full", "deleting", ClaimRefusalReason::SnapshotNotReady),
            ("disk", "ready", ClaimRefusalReason::SnapshotNotFull),
        ] {
            sqlx::query(sql!(
                "UPDATE snapshots SET type = $2, state = $3 WHERE id = $1"
            ))
            .bind(snapshot)
            .bind(kind)
            .bind(state)
            .execute(pool)
            .await?;
            assert_eq!(
                <SnapshotRetentionDbStore as RetentionSink>::claim_prune(
                    &store,
                    candidate.clone(),
                    1,
                    None
                )
                .await?,
                ClaimOutcome::Refused(reason),
            );
            assert_eq!(
                snapshot_state(pool, snapshot).await?.as_deref(),
                Some(state)
            );
            assert!(location_exists(pool, snapshot).await?);
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p18_active_fork_blocks_claim_until_the_operation_is_terminal() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let oldest = insert_snapshot(pool, sandbox, "full", "ready", 500).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;
        let store = SnapshotRetentionDbStore::new(pool.clone());
        let candidate = store
            .list_candidates(1, 100, None, None)
            .await?
            .candidates
            .into_iter()
            .find(|candidate| candidate.snapshot_id == oldest)
            .expect("the old snapshot is initially eligible");

        let operation_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO public_operations
                 (operation_id, org_id, kind, target_type, target_id, state)
             VALUES ($1, $2::UUID, 'fork_sandbox', 'sandbox', $3, 'queued')",
        )
        .bind(operation_id)
        .bind(SEEDED_ORG_ID)
        .bind(sandbox.to_string())
        .execute(pool)
        .await?;
        assert_eq!(
            <SnapshotRetentionDbStore as RetentionSink>::claim_prune(
                &store,
                candidate.clone(),
                1,
                None,
            )
            .await?,
            ClaimOutcome::Refused(ClaimRefusalReason::ActiveForkSource)
        );
        assert_eq!(
            snapshot_state(pool, oldest).await?.as_deref(),
            Some("ready")
        );

        sqlx::query("UPDATE public_operations SET state = 'succeeded' WHERE operation_id = $1")
            .bind(operation_id)
            .execute(pool)
            .await?;
        let claimed = expect_claimed(
            <SnapshotRetentionDbStore as RetentionSink>::claim_prune(
                &store,
                candidate.clone(),
                1,
                None,
            )
            .await?,
        );
        assert_eq!(
            claimed.storage_location, candidate.storage_location,
            "the committed claim returns the exact route authorized for deletion"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p19_claim_waits_for_the_fork_source_lifecycle_lock() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let sandbox = insert_sandbox(pool).await?;
        let oldest = insert_snapshot(pool, sandbox, "full", "ready", 500).await?;
        insert_snapshot(pool, sandbox, "full", "ready", 100).await?;
        let store = SnapshotRetentionDbStore::new(pool.clone());
        let candidate = store
            .list_candidates(1, 100, None, None)
            .await?
            .candidates
            .into_iter()
            .find(|candidate| candidate.snapshot_id == oldest)
            .expect("the old snapshot is initially eligible");

        let mut fork_guard = pool.begin().await?;
        pplx_space_sandbox_store::fork_source_lifecycle::lock_fork_source_lifecycle(
            &mut fork_guard,
            sandbox,
        )
        .await?;

        let claim_store = SnapshotRetentionDbStore::new(pool.clone());
        let mut claim = tokio::spawn(async move {
            <SnapshotRetentionDbStore as RetentionSink>::claim_prune(
                &claim_store,
                candidate,
                1,
                None,
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), &mut claim)
                .await
                .is_err(),
            "retention must wait while fork admission owns the source lock"
        );

        fork_guard.commit().await?;
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), claim)
            .await
            .expect("claim resumes after the fork lock is released")??;
        expect_claimed(outcome);
        assert_eq!(
            snapshot_state(pool, oldest).await?.as_deref(),
            Some("deleting")
        );
        Ok(())
    }
}

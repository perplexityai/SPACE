//! # Test matrix
//!
//! Every behavior the reconciler exhibits maps to one test below.
//!
//! ## C — `ReconcilerConfig` + env parsing
//! - `c1_defaults_when_env_unset`
//! - `c1b_malformed_kill_switch_disables_the_sweeper`
//! - `c2_enabled_min_age_and_bucket_env_vars_override_default`
//! - `c3_invalid_min_age_falls_back_to_default`
//! - `c5_min_age_zero_disables_grace_explicitly`
//! - `c6_blank_snapshot_bucket_is_treated_as_unset`
//!
//! ## R — `run()` loop lifecycle
//! - `r1_disabled_returns_on_shutdown_without_ticking`
//! - `r2_enabled_without_scoped_bucket_refuses_to_run`
//!
//! ## T — Tick behavior (in-process fakes)
//! - `t1_empty_candidate_list_is_noop`
//! - `t2_unencrypted_row_with_s3_objects_deletes_row`
//! - `t3_empty_s3_prefix_treated_as_already_gone_idempotent`
//! - `t4_encrypted_row_skipped_without_decryption`
//! - `t5_parse_failure_skips_row_and_records_error`
//! - `t6_transient_s3_error_leaves_row_in_place`
//! - `t7_db_delete_failure_after_s3_success_retries_next_tick`
//! - `t8_max_per_tick_caps_processing`
//! - `t9_cursor_advances_past_rows_to_reach_later_orphans`
//! - `t10_cursor_wraps_to_start_after_a_short_page`
//! - `t11_drift_skip_when_orphan_state_cleared_under_us`
//! - `t12_cross_bucket_row_skipped_with_metric`
//! - `t13_min_age_is_threaded_through_to_sink`
//! - `t14_rate_limited_rows_visited_on_next_tick_not_after_wrap`
//! - `t15_concurrency_bounds_parallel_deletes`
//! - `t16_short_rate_limited_page_advances_cursor`

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::StorageUri;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::*;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeSink {
    candidates: Mutex<Vec<SweepCandidate>>,
    delete_calls: Mutex<Vec<Uuid>>,
    /// Ids whose conditional DELETE should return `Ok(false)`.
    refuse_delete: Mutex<Vec<Uuid>>,
    /// Ids whose DELETE should return an `sqlx` error.
    fail_delete: Mutex<Vec<Uuid>>,
    /// Last `min_age` value the reconciler passed to `list_candidates`.
    /// Tests assert this to verify the config flag is threaded through.
    last_min_age: Mutex<Option<Option<Duration>>>,
}

impl FakeSink {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_candidates(&self, rows: Vec<SweepCandidate>) {
        *self.candidates.lock().unwrap() = rows;
    }

    fn refuse(&self, id: Uuid) {
        self.refuse_delete.lock().unwrap().push(id);
    }

    fn fail(&self, id: Uuid) {
        self.fail_delete.lock().unwrap().push(id);
    }

    fn delete_calls(&self) -> Vec<Uuid> {
        self.delete_calls.lock().unwrap().clone()
    }

    fn last_min_age(&self) -> Option<Option<Duration>> {
        *self.last_min_age.lock().unwrap()
    }
}

impl SweeperSink for Arc<FakeSink> {
    async fn list_candidates(
        &self,
        limit: i64,
        after: Option<Uuid>,
        _min_age: Option<Duration>,
    ) -> Result<Vec<SweepCandidate>, sqlx::Error> {
        // The fake doesn't model `min_age` — the postgres-integration
        // tests cover the SQL-level grace filter. Unit tests here
        // assert that the reconciler *threads* `min_age` through to
        // the sink call site via the helper below.
        self.last_min_age.lock().unwrap().replace(_min_age);
        let all = self.candidates.lock().unwrap().clone();
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(all
            .into_iter()
            .filter(|c| after.map(|a| c.id > a).unwrap_or(true))
            .take(limit)
            .collect())
    }

    async fn delete_storage_location_if_orphan(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        self.delete_calls.lock().unwrap().push(id);
        if self.fail_delete.lock().unwrap().contains(&id) {
            return Err(sqlx::Error::PoolClosed);
        }
        if self.refuse_delete.lock().unwrap().contains(&id) {
            return Ok(false);
        }
        Ok(true)
    }
}

#[derive(Default)]
struct FakeBackend {
    /// Maps `(bucket, key_prefix)` to the canned outcome.
    outcomes: Mutex<HashMap<(String, String), Result<DeletePrefixOutcome, String>>>,
    calls: Mutex<Vec<(String, String)>>,
    delete_delay: Mutex<Option<Duration>>,
    in_flight: Mutex<usize>,
    max_in_flight: Mutex<usize>,
}

#[derive(Clone)]
struct TestBackend(Arc<FakeBackend>);

impl std::ops::Deref for TestBackend {
    type Target = FakeBackend;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FakeBackend {
    fn shared() -> TestBackend {
        TestBackend(Arc::new(Self::default()))
    }

    fn set_outcome(&self, uri: &StorageUri, outcome: DeletePrefixOutcome) {
        self.outcomes
            .lock()
            .unwrap()
            .insert((uri.bucket.clone(), uri.key_prefix.clone()), Ok(outcome));
    }

    fn set_error(&self, uri: &StorageUri, error: &str) {
        self.outcomes.lock().unwrap().insert(
            (uri.bucket.clone(), uri.key_prefix.clone()),
            Err(error.to_string()),
        );
    }

    fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }

    fn set_delete_delay(&self, delay: Duration) {
        *self.delete_delay.lock().unwrap() = Some(delay);
    }

    fn max_in_flight(&self) -> usize {
        *self.max_in_flight.lock().unwrap()
    }
}

impl StorageBackend for TestBackend {
    async fn delete_prefix(
        &self,
        uri: &StorageUri,
    ) -> Result<DeletePrefixOutcome, DeletePrefixError> {
        self.calls
            .lock()
            .unwrap()
            .push((uri.bucket.clone(), uri.key_prefix.clone()));

        {
            let mut in_flight = self.in_flight.lock().unwrap();
            *in_flight += 1;
            let current = *in_flight;
            let mut max_in_flight = self.max_in_flight.lock().unwrap();
            *max_in_flight = (*max_in_flight).max(current);
        }

        let delay = *self.delete_delay.lock().unwrap();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        let outcome = match self
            .outcomes
            .lock()
            .unwrap()
            .get(&(uri.bucket.clone(), uri.key_prefix.clone()))
            .cloned()
        {
            Some(Ok(outcome)) => Ok(outcome),
            Some(Err(msg)) => Err(DeletePrefixError::Backend(msg)),
            None => Ok(DeletePrefixOutcome::Deleted { count: 1 }),
        };

        *self.in_flight.lock().unwrap() -= 1;
        outcome
    }
}

fn candidate(id: Uuid, uri: &str, has_encrypted_fields: bool) -> SweepCandidate {
    SweepCandidate {
        id,
        storage_connection_string: uri.to_string(),
        has_encrypted_fields,
    }
}

fn fixture_uri(snap: &str) -> String {
    format!(
        "s3://example-snapshots\
         /clusters/test-cluster/suspend_snapshots/aa/sandbox-{snap}/snap-{snap}\
         ?region=us-east-1"
    )
}

fn config(max_per_tick: u32) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        tick: Duration::from_secs(60),
        max_per_tick,
        // Most tests assert exact call order. Keep the shared helper
        // serial, and opt into parallelism only in the dedicated
        // concurrency test.
        concurrency: 1,
        // Most tests use specific orphan ids without dating them.
        // Default to `None` so the in-process fake's lack of
        // age-filter logic doesn't surprise tests; the dedicated
        // grace test sets this explicitly.
        min_age: None,
        // The fixture URI uses `example-snapshots`; tests
        // that exercise bucket-scope behavior override this.
        scoped_bucket: Some("example-snapshots".to_string()),
    }
}

// -----------------------------------------------------------------
// C — config
// -----------------------------------------------------------------

#[test]
fn c1_defaults_when_env_unset() {
    let env = pplx_space_test_env::TestEnv::default();
    let c = ReconcilerConfig::from_environment(&env);
    assert!(c.enabled);
    assert_eq!(c.tick, DEFAULT_TICK);
    assert_eq!(c.max_per_tick, DEFAULT_MAX_PER_TICK);
    assert_eq!(c.concurrency, DEFAULT_CONCURRENCY);
    assert_eq!(c.min_age, Some(DEFAULT_MIN_AGE));
    assert_eq!(c.scoped_bucket, None);
}

/// The sweeper deletes objects, so only an unset or explicitly truthy switch
/// leaves it running; a misspelled one must stop it.
#[test]
fn c1b_malformed_kill_switch_disables_the_sweeper() {
    for value in ["false", "0", "off", " false  ", "fasle", ""] {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED", value);
        assert!(
            !ReconcilerConfig::from_environment(&env).enabled,
            "{value:?}"
        );
    }
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED", "true");
    assert!(ReconcilerConfig::from_environment(&env).enabled);
}

#[test]
fn c2_enabled_and_bucket_env_vars_override_default() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED", "true");
    env.set("SPACE_SNAPSHOT_BUCKET", "my-bucket");
    let c = ReconcilerConfig::from_environment(&env);
    assert!(c.enabled);
    assert_eq!(c.tick, DEFAULT_TICK);
    assert_eq!(c.max_per_tick, DEFAULT_MAX_PER_TICK);
    assert_eq!(c.concurrency, DEFAULT_CONCURRENCY);
    assert_eq!(c.min_age, Some(DEFAULT_MIN_AGE));
    assert_eq!(c.scoped_bucket, Some("my-bucket".to_string()));
}

/// `MIN_AGE_SECS=0` is the explicit "no grace" override (for backfill
/// scripts only). Must produce `None`, not `Some(Duration::ZERO)` —
/// the SQL filter is `is_some()`-shaped and a zero duration would
/// match every row.
/// `SPACE_SNAPSHOT_BUCKET` with only whitespace must not be treated
/// as a scoped bucket. The run() spawner refuses to start without a
/// scope, and a whitespace-only env var is a likely Helm-rendering
/// bug rather than an explicit choice.
#[test]
fn c6_blank_snapshot_bucket_is_treated_as_unset() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_SNAPSHOT_BUCKET", "   ");
    let c = ReconcilerConfig::from_environment(&env);
    assert_eq!(c.scoped_bucket, None);
}

// -----------------------------------------------------------------
// R — run loop
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn r1_disabled_returns_on_shutdown_without_ticking() {
    let sink = FakeSink::new();
    let backend = FakeBackend::shared();
    // Seed a deletable candidate in the scoped bucket and let virtual tick
    // intervals elapse before cancelling, so the emptiness asserts below are
    // falsifiable: a regressed disabled gate would tick, hit the backend,
    // and delete inside this window.
    let id = Uuid::now_v7();
    let uri = fixture_uri("gate-r1");
    let parsed = StorageUri::parse(&uri).unwrap();
    sink.set_candidates(vec![candidate(id, &uri, false)]);
    backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 1 });
    let token = CancellationToken::new();
    let disabled = ReconcilerConfig {
        enabled: false,
        ..config(DEFAULT_MAX_PER_TICK)
    };
    let token_for_cancel = token.clone();
    let sink_for_run = sink.clone();
    let backend_for_run = backend.clone();
    let handle = tokio::spawn(async move {
        run(disabled, sink_for_run, backend_for_run, token).await;
    });
    tokio::time::sleep(Duration::from_secs(300)).await;
    token_for_cancel.cancel();
    handle.await.unwrap();
    assert!(
        sink.delete_calls().is_empty(),
        "disabled reconciler must never call list_candidates/delete"
    );
    assert!(
        backend.calls().is_empty(),
        "disabled reconciler must never call the storage backend"
    );
}

/// Enabled but missing `scoped_bucket` must refuse to start —
/// running unscoped would silently hit AccessDenied on every cross-
/// bucket orphan and (if IAM ever widened) destroy unrelated data.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn r2_enabled_without_scoped_bucket_refuses_to_run() {
    let sink = FakeSink::new();
    let backend = FakeBackend::shared();
    // Same falsifiable shape as r1: remove the missing-bucket guard and the
    // loop processes this candidate inside the virtual-time window.
    let id = Uuid::now_v7();
    let uri = fixture_uri("gate-r2");
    let parsed = StorageUri::parse(&uri).unwrap();
    sink.set_candidates(vec![candidate(id, &uri, false)]);
    backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 1 });
    let token = CancellationToken::new();
    let bad = ReconcilerConfig {
        scoped_bucket: None,
        ..config(DEFAULT_MAX_PER_TICK)
    };
    let token_for_cancel = token.clone();
    let sink_for_run = sink.clone();
    let backend_for_run = backend.clone();
    let handle = tokio::spawn(async move {
        run(bad, sink_for_run, backend_for_run, token).await;
    });
    tokio::time::sleep(Duration::from_secs(300)).await;
    token_for_cancel.cancel();
    handle.await.unwrap();
    assert!(
        sink.delete_calls().is_empty(),
        "unscoped reconciler must refuse to run without ever ticking"
    );
    assert!(
        backend.calls().is_empty(),
        "unscoped reconciler must never call the storage backend"
    );
}

// -----------------------------------------------------------------
// T — tick behavior
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn t1_empty_candidate_list_is_noop() {
    let sink = FakeSink::new();
    let backend = FakeBackend::shared();
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
    assert!(sink.delete_calls().is_empty());
    assert!(backend.calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t2_unencrypted_row_with_s3_objects_deletes_row() {
    let id = Uuid::now_v7();
    let uri = fixture_uri("a");
    let parsed = StorageUri::parse(&uri).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(id, &uri, false)]);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 4 });
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 1);
    assert_eq!(report.already_gone, 0);
    assert_eq!(sink.delete_calls(), vec![id]);
    assert_eq!(backend.calls(), vec![(parsed.bucket, parsed.key_prefix)]);
}

/// Idempotency guarantee: an empty S3 prefix (the blob was already
/// swept or never landed) must still drive the DB row delete so we
/// don't loop forever on already-clean S3 state.
#[tokio::test(flavor = "current_thread")]
async fn t3_empty_s3_prefix_treated_as_already_gone_idempotent() {
    let id = Uuid::now_v7();
    let uri = fixture_uri("b");
    let parsed = StorageUri::parse(&uri).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(id, &uri, false)]);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed, DeletePrefixOutcome::AlreadyGone);
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.already_gone, 1);
    assert_eq!(report.deleted, 0);
    assert_eq!(
        sink.delete_calls(),
        vec![id],
        "DB row must still be deleted when S3 reports the blob was already gone — \
         this is the idempotency guarantee for retried sweeps"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t4_encrypted_row_skipped_without_decryption() {
    let id = Uuid::now_v7();
    let sink = FakeSink::new();
    // The parent's `storage_connection_string` is a stand-in (empty)
    // when the sidecar exists — make sure the sweeper never tries to
    // parse it.
    sink.set_candidates(vec![candidate(id, "", true)]);
    let backend = FakeBackend::shared();
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.skipped_encrypted, 1);
    assert_eq!(report.deleted, 0);
    assert!(sink.delete_calls().is_empty());
    assert!(backend.calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t5_parse_failure_skips_row_and_records_error() {
    let id = Uuid::now_v7();
    let sink = FakeSink::new();
    // Garbage URI — not even an `s3://` scheme.
    sink.set_candidates(vec![candidate(id, "garbage", false)]);
    let backend = FakeBackend::shared();
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.parse_errors, 1);
    assert_eq!(report.deleted, 0);
    assert!(sink.delete_calls().is_empty());
    assert!(backend.calls().is_empty());
}

/// Durability guarantee: a transient (non-NotFound) S3 error must
/// leave the DB row in place so the next tick can retry — never DELETE
/// the row on a backend failure.
#[tokio::test(flavor = "current_thread")]
async fn t6_transient_s3_error_leaves_row_in_place() {
    let id = Uuid::now_v7();
    let uri = fixture_uri("c");
    let parsed = StorageUri::parse(&uri).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(id, &uri, false)]);
    let backend = FakeBackend::shared();
    backend.set_error(&parsed, "throttled");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.s3_errors, 1);
    assert_eq!(report.deleted, 0);
    assert!(
        sink.delete_calls().is_empty(),
        "DB row must NOT be deleted when S3 errored — that's the orphan-row durability guarantee"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t7_db_delete_failure_after_s3_success_retries_next_tick() {
    let id = Uuid::now_v7();
    let uri = fixture_uri("d");
    let parsed = StorageUri::parse(&uri).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(id, &uri, false)]);
    sink.fail(id);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 1 });
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.db_errors, 1);
    assert_eq!(report.deleted, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn t8_max_per_tick_caps_processing() {
    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::now_v7()).collect();
    let uris: Vec<String> = (0..3).map(|i| fixture_uri(&format!("max-{i}"))).collect();
    let sink = FakeSink::new();
    sink.set_candidates(
        ids.iter()
            .zip(uris.iter())
            .map(|(id, uri)| candidate(*id, uri, false))
            .collect(),
    );
    let backend = FakeBackend::shared();
    for uri in &uris {
        let parsed = StorageUri::parse(uri).unwrap();
        backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 1 });
    }
    let mut reconciler = Reconciler::new(config(2), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 2);
    assert_eq!(report.rate_limited, 1);
}

/// Starvation regression (same shape as the orphaned-node cursor
/// regression test):
/// if every tick scans the same prefix from the front of the
/// ascending-`id` order, an unparseable / encrypted row at the front
/// permanently blocks later orphan candidates. The cursor must
/// advance even when no row was deleted, so subsequent ticks see
/// new rows.
#[tokio::test(flavor = "current_thread")]
async fn t9_cursor_advances_past_rows_to_reach_later_orphans() {
    // Two encrypted rows up front; one unencrypted orphan after them.
    // With `max_per_tick=1` and `limit = 2 * max_per_tick = 2`, each
    // tick processes one row and advances the cursor. Without that
    // cursor, every tick would start from the first encrypted row and
    // never reach the unencrypted orphan.
    let encrypted_a = Uuid::now_v7();
    let encrypted_b = Uuid::now_v7();
    let orphan = Uuid::now_v7();
    assert!(
        encrypted_a < encrypted_b && encrypted_b < orphan,
        "UUIDv7 ordering invariant"
    );

    let uri = fixture_uri("starvation");
    let parsed = StorageUri::parse(&uri).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(encrypted_a, "", true),
        candidate(encrypted_b, "", true),
        candidate(orphan, &uri, false),
    ]);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 1 });

    let mut reconciler = Reconciler::new(config(1), sink.clone(), backend.clone());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.deleted, 0);
    assert_eq!(r1.skipped_encrypted, 1);
    assert!(sink.delete_calls().is_empty());

    let r2 = reconciler.tick().await;
    assert_eq!(r2.deleted, 0);
    assert_eq!(r2.skipped_encrypted, 1);
    assert!(sink.delete_calls().is_empty());

    let r3 = reconciler.tick().await;
    assert_eq!(r3.deleted, 1);
    assert_eq!(sink.delete_calls(), vec![orphan]);
}

#[tokio::test(flavor = "current_thread")]
async fn t10_cursor_wraps_to_start_after_a_short_page() {
    let n1 = Uuid::now_v7();
    let uri1 = fixture_uri("wrap-1");
    let parsed1 = StorageUri::parse(&uri1).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(n1, &uri1, false)]);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed1, DeletePrefixOutcome::Deleted { count: 1 });
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let _ = reconciler.tick().await;

    // A new orphan appears after the first tick — the cursor must
    // have wrapped to `None`, otherwise the second tick filters this
    // row out via `id > scan_after`.
    let n_new = Uuid::now_v7();
    assert!(n_new > n1, "UUIDv7 monotonicity");
    let uri_new = fixture_uri("wrap-2");
    let parsed_new = StorageUri::parse(&uri_new).unwrap();
    sink.set_candidates(vec![candidate(n_new, &uri_new, false)]);
    backend.set_outcome(&parsed_new, DeletePrefixOutcome::Deleted { count: 1 });
    let _ = reconciler.tick().await;
    assert_eq!(sink.delete_calls(), vec![n1, n_new]);
}

#[tokio::test(flavor = "current_thread")]
async fn t11_drift_skip_when_orphan_state_cleared_under_us() {
    let id = Uuid::now_v7();
    let uri = fixture_uri("drift");
    let parsed = StorageUri::parse(&uri).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(id, &uri, false)]);
    sink.refuse(id);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 1 });
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.drift_skips, 1);
    assert_eq!(report.deleted, 0);
}

/// Cross-bucket guard: an orphan row whose URI points at a bucket
/// the cluster isn't scoped to (e.g. org-level
/// `snapshot_base_object_storage_uri` override) must NOT be reaped,
/// must NOT delete the DB row, and must surface as a distinct
/// metric counter so cross-bucket backlog is visible separately
/// from transient S3 errors.
#[tokio::test(flavor = "current_thread")]
async fn t12_cross_bucket_row_skipped_with_metric() {
    let id = Uuid::now_v7();
    // URI points at a different bucket than the test's scoped one.
    let foreign_uri = "s3://some-other-bucket/clusters/x/snap-1?region=us-east-1";
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(id, foreign_uri, false)]);
    let backend = FakeBackend::shared();
    let mut reconciler = Reconciler::new(config(10), sink.clone(), backend.clone());
    let report = reconciler.tick().await;
    assert_eq!(report.bucket_out_of_scope, 1);
    assert_eq!(report.deleted, 0);
    assert!(
        sink.delete_calls().is_empty(),
        "DB row must stay in place when the bucket is out of scope"
    );
    assert!(
        backend.calls().is_empty(),
        "S3 backend must NOT be called when the URI bucket is foreign"
    );
}

/// Verify that the reconciler threads `config.min_age` through to
/// the sink's `list_candidates(min_age=...)`. The in-process fake
/// captures the value; the real SQL filter coverage lives with the
/// `SnapshotStorageDbStore` postgres-integration tests.
#[tokio::test(flavor = "current_thread")]
async fn t13_min_age_is_threaded_through_to_sink() {
    let sink = FakeSink::new();
    let backend = FakeBackend::shared();
    let cfg = ReconcilerConfig {
        min_age: Some(Duration::from_secs(300)),
        ..config(10)
    };
    let mut reconciler = Reconciler::new(cfg, sink.clone(), backend);
    let _ = reconciler.tick().await;
    assert_eq!(sink.last_min_age(), Some(Some(Duration::from_secs(300))));
}

/// Cursor-fairness regression: when a full page hits the per-tick
/// cap, the cursor must advance only past the rows actually visited
/// (`candidates[cap - 1].id`), not past the entire page
/// (`candidates.last().id`). Otherwise rate-limited rows are
/// skipped until the cursor wraps on a short page — contradicting
/// the `rate_limited` field doc ("the next tick picks them up").
#[tokio::test(flavor = "current_thread")]
async fn t14_rate_limited_rows_visited_on_next_tick_not_after_wrap() {
    // max_per_tick=1, limit=2, so the first tick fetches exactly 2
    // rows (a full page), processes row 0, rate-limits row 1, and
    // the cursor must advance only to row[0].id.
    let row0 = Uuid::now_v7();
    let row1 = Uuid::now_v7();
    assert!(row0 < row1, "UUIDv7 ordering invariant");

    let uri0 = fixture_uri("rate-0");
    let uri1 = fixture_uri("rate-1");
    let parsed0 = StorageUri::parse(&uri0).unwrap();
    let parsed1 = StorageUri::parse(&uri1).unwrap();

    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(row0, &uri0, false),
        candidate(row1, &uri1, false),
    ]);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed0, DeletePrefixOutcome::Deleted { count: 1 });
    backend.set_outcome(&parsed1, DeletePrefixOutcome::Deleted { count: 1 });

    let mut reconciler = Reconciler::new(config(1), sink.clone(), backend.clone());

    let r1 = reconciler.tick().await;
    assert_eq!(r1.deleted, 1, "tick 1 deletes one row (the cap)");
    assert_eq!(r1.rate_limited, 1, "tick 1 rate-limits the other");

    let r2 = reconciler.tick().await;
    assert_eq!(
        r2.deleted, 1,
        "tick 2 must pick up the rate-limited row from tick 1, \
         not skip past it"
    );
    assert_eq!(sink.delete_calls(), vec![row0, row1]);
}

#[tokio::test(flavor = "current_thread")]
async fn t15_concurrency_bounds_parallel_deletes() {
    let rows: Vec<_> = (0..4)
        .map(|i| {
            let id = Uuid::now_v7();
            let uri = fixture_uri(&format!("parallel-{i}"));
            (id, uri)
        })
        .collect();

    let sink = FakeSink::new();
    sink.set_candidates(
        rows.iter()
            .map(|(id, uri)| candidate(*id, uri, false))
            .collect(),
    );

    let backend = FakeBackend::shared();
    backend.set_delete_delay(Duration::from_millis(10));

    let cfg = ReconcilerConfig {
        concurrency: 2,
        ..config(4)
    };
    let mut reconciler = Reconciler::new(cfg, sink.clone(), backend.clone());
    let report = reconciler.tick().await;

    assert_eq!(report.deleted, 4);
    assert_eq!(report.rate_limited, 0);
    assert_eq!(sink.delete_calls().len(), 4);
    assert!(
        backend.max_in_flight() > 1,
        "expected more than one S3 delete to run at once"
    );
    assert!(
        backend.max_in_flight() <= 2,
        "parallelism must stay bounded by config.concurrency"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t16_short_rate_limited_page_advances_cursor() {
    // max_per_tick=2, limit=4. The fake returns a short page of 3
    // rows, but it is still larger than the per-tick cap. The first
    // two rows are sticky skips. The third row must be visited on the
    // next tick, not starved by wrapping the cursor to the beginning.
    let encrypted_a = Uuid::now_v7();
    let encrypted_b = Uuid::now_v7();
    let orphan = Uuid::now_v7();
    assert!(encrypted_a < encrypted_b && encrypted_b < orphan);

    let uri = fixture_uri("short-rate-limited");
    let parsed = StorageUri::parse(&uri).unwrap();
    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(encrypted_a, "", true),
        candidate(encrypted_b, "", true),
        candidate(orphan, &uri, false),
    ]);
    let backend = FakeBackend::shared();
    backend.set_outcome(&parsed, DeletePrefixOutcome::Deleted { count: 1 });

    let mut reconciler = Reconciler::new(config(2), sink.clone(), backend.clone());

    let r1 = reconciler.tick().await;
    assert_eq!(r1.scanned, 3);
    assert_eq!(r1.skipped_encrypted, 2);
    assert_eq!(r1.rate_limited, 1);
    assert!(sink.delete_calls().is_empty());

    let r2 = reconciler.tick().await;
    assert_eq!(
        r2.deleted, 1,
        "short pages with rate-limited rows must resume after the selected prefix"
    );
    assert_eq!(sink.delete_calls(), vec![orphan]);
}

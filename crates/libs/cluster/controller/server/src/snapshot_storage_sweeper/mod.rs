//! Periodic reconciler that reaps S3 blobs left behind by suspended
//! sandbox deletes.
//!
//! ## Why this exists
//!
//! `delete_with_dependents` (in
//! `crates/libs/sandbox/store/src/delete.rs`) detaches
//! `snapshot_storage_locations` rows from the snapshot chain on sandbox
//! delete — sets `snapshot_id = NULL` and stamps `orphaned_at = NOW()`
//! — so the cascading `sandboxes -> snapshots` DELETE can complete
//! without tripping the `RESTRICT` FK. The detached rows survive
//! intentionally so a background sweeper can reap the S3 blob without
//! holding up the user-facing DELETE.
//!
//! This is that sweeper.
//!
//! ## Design
//!
//! Sibling to [`super::orphaned_nodes`] / [`super::drained_node_terminator`]:
//!
//! - One env-driven [`ReconcilerConfig`] with a master enable gate. Off
//!   by default everywhere; flip on per-environment.
//! - Two pluggable seams:
//!   - [`SweeperSink`] — Postgres queries (candidate scan, conditional
//!     DELETE).
//!   - [`pplx_space_snapshot_storage::StorageBackend`] — object-store
//!     prefix deletion. Production S3 support lives in the dedicated
//!     `pplx-space-snapshot-storage` library; in-process fakes live in
//!     this module's tests.
//! - The reconciler walks candidates (`orphaned_at IS NOT NULL`,
//!   ordered by `id` so a cursor can make forward progress past stuck
//!   rows) and for each one:
//!   1. Parses the `storage_connection_string` into a
//!      [`pplx_space_common_core::types::StorageUri`]. Parse failure is
//!      logged + counted as `failed{reason:parse_uri}`; the row is
//!      left in place so the failure stays visible until repaired.
//!   2. Calls [`pplx_space_snapshot_storage::StorageBackend::delete_prefix`]. The backend lists
//!      every object under the prefix and batch-deletes them. An empty
//!      list is treated as success (idempotent) so retrying after a
//!      partial delete still terminates.
//!   3. On success, conditionally DELETEs the
//!      `snapshot_storage_locations` row. The
//!      `snapshot_storage_location_encrypted_fields` sidecar (when
//!      present) cascades; non-orphan rows are refused by the SQL
//!      guard `orphaned_at IS NOT NULL`.
//!
//! ## Encryption sidecar
//!
//! The DB schema has a `snapshot_storage_location_encrypted_fields`
//! sidecar wrapping `storage_connection_string` as BYTEA, keyed off a
//! per-org `wrapped_dek`. Today no production code writes that sidecar,
//! so every orphan row carries its URI plaintext on the parent. The
//! sweeper still detects sidecar rows defensively — when one is found
//! it logs + counts `failed{reason:encrypted_unsupported}` and leaves
//! the row alone. Adding decryption is a follow-up once the encryption
//! migration writes the sidecar; making the sweeper crash on an
//! encrypted row would be a regression once that path lights up.
//!
//! ## Why this isn't in the API service
//!
//! Per repo convention "no background threads in API servers" — the
//! API process must stay stateless. Cluster-singleton sweeps live in
//! the dedicated `space-cluster-controller` binary, added via a
//! `tokio::spawn` line in `super::reconcilers::run_reconcilers`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::stream::{self, StreamExt};
use pplx_space_common_core::types::StorageUri;
use pplx_space_metrics::snapshot_storage_sweeper as sweeper_metrics;
#[cfg(test)]
use pplx_space_snapshot_storage::DeletePrefixError;
use pplx_space_snapshot_storage::{DeletePrefixOutcome, StorageBackend};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::env_config;

pub mod store;

#[cfg(test)]
mod tests;

pub use store::SnapshotStorageDbStore;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_MAX_PER_TICK: u32 = 100;
pub(crate) const DEFAULT_CONCURRENCY: usize = 8;
/// Default grace period before an orphan row becomes sweep-eligible.
/// Long enough to cover a typical in-flight suspend-snapshot upload
/// from sbx-mgr — if the row's `orphaned_at` was stamped while
/// vol-mgr was mid-upload (sandbox DELETEd before the upload
/// finished), we want to give the upload a chance to finish writing
/// objects under the prefix before we list+delete it. Sweeping a
/// still-uploading prefix would leave late-arriving objects without
/// a DB anchor for any future sweep.
pub(crate) const DEFAULT_MIN_AGE: Duration = Duration::from_secs(
    pplx_space_common_core::types::SNAPSHOT_STORAGE_SWEEPER_DEFAULT_MIN_AGE_SECONDS,
);

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Enabled by default; operators can disable it with
    /// `SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED=false`.
    pub enabled: bool,
    pub tick: Duration,
    /// Hard cap on how many DELETEs a single tick may issue. Bounds
    /// blast radius if the SQL filter ever regressed and started
    /// surfacing live rows.
    pub max_per_tick: u32,
    /// Number of storage prefixes to sweep concurrently within one
    /// tick. `max_per_tick` is the total per-loop work budget;
    /// `concurrency` controls how many rows inside that budget may be
    /// in the S3-list/delete + DB-delete path at the same time.
    pub concurrency: usize,
    /// Minimum age an orphan row must have before the sweeper will
    /// reap it. `None` means no grace (immediate eligibility) and is
    /// intentionally hazardous — sweeping a freshly-orphaned row
    /// before the in-flight suspend-snapshot upload finishes leaks
    /// every late object as a permanent S3 orphan with no DB row to
    /// re-find it. `Some(Duration::ZERO)` would have the same hazard
    /// without flagging it in the type — see the user-CLAUDE.md rule
    /// on `Option<Duration>` vs zero-sentinel for the rationale.
    ///
    /// Disabled (`None`) is only appropriate for backfill / one-shot
    /// scripts that reap a known-fixed set of long-stale rows; the
    /// default-enabled production path always carries a grace.
    pub min_age: Option<Duration>,
    /// Cluster-scoped S3 bucket the sweeper is authorized to act on.
    /// Orphan rows whose `storage_connection_string` points at a
    /// different bucket (e.g. an org-level
    /// `snapshot_base_object_storage_uri` override) are out of
    /// scope; the sweeper records them as
    /// `failed{reason:bucket_out_of_scope}` and leaves them in
    /// place. Sweeping arbitrary buckets would require either
    /// broader IAM (dangerous) or per-org credentials (separate
    /// design); both are explicitly out of scope here.
    ///
    /// Sourced from `SPACE_SNAPSHOT_BUCKET` (same env var sbx-mgr
    /// reads for the writer-side) so the writer and the reaper
    /// share a single source of truth on which bucket this cluster
    /// owns. `None` means the env var wasn't set; the spawner
    /// refuses to start the reconciler in that state so a
    /// misconfigured deploy can't end up sweeping cross-bucket.
    pub scoped_bucket: Option<String>,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick: DEFAULT_TICK,
            max_per_tick: DEFAULT_MAX_PER_TICK,
            concurrency: DEFAULT_CONCURRENCY,
            min_age: Some(DEFAULT_MIN_AGE),
            scoped_bucket: None,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.snapshotStorageSweeper.*`,
    /// except `scoped_bucket` which is sourced from the cluster-wide
    /// `SPACE_SNAPSHOT_BUCKET`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            // Ships enabled and deletes objects, so a malformed value must
            // disable it rather than leave it running.
            enabled: config.destructive_gate("SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED"),
            tick: default.tick,
            max_per_tick: default.max_per_tick,
            concurrency: default.concurrency,
            min_age: default.min_age,
            scoped_bucket: env
                .var("SPACE_SNAPSHOT_BUCKET")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        }
    }
}

/// One orphan storage-location row surfaced by [`SweeperSink::list_candidates`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepCandidate {
    pub id: Uuid,
    /// Plaintext URI on the parent row. Only meaningful when
    /// `has_encrypted_fields` is `false`; when the sidecar exists this
    /// holds a stand-in (empty string) and the real URI is encrypted
    /// in the sidecar.
    pub storage_connection_string: String,
    /// True iff a matching `snapshot_storage_location_encrypted_fields`
    /// row exists. Used by the sweeper to short-circuit on encrypted
    /// rows until the encryption-aware path lands.
    pub has_encrypted_fields: bool,
}

/// Postgres-side surface. Production impl: [`SnapshotStorageDbStore`].
pub trait SweeperSink: Send + Sync + 'static {
    /// Return up to `limit` rows that look like sweep candidates:
    /// `orphaned_at IS NOT NULL`, `snapshot_id IS NULL`,
    /// `id > after`, and (when `min_age` is `Some`)
    /// `orphaned_at < NOW() - min_age` so newly orphaned rows
    /// whose suspend-snapshot upload may still be in flight aren't
    /// reaped before the upload finishes. Ordered by `id` ASC. Each
    /// row carries the `has_encrypted_fields` flag derived from an
    /// `EXISTS` subquery against the sidecar table.
    ///
    /// `min_age = None` means "no grace" — every orphan row is
    /// eligible the moment `orphaned_at` is stamped. Only safe for
    /// backfill / one-shot reapers where you know every input row
    /// has had time to settle.
    fn list_candidates(
        &self,
        limit: i64,
        after: Option<Uuid>,
        min_age: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<Vec<SweepCandidate>, sqlx::Error>> + Send;

    /// Conditional DELETE. Returns `Ok(true)` if the row was deleted.
    /// Returns `Ok(false)` (without error) if the row drifted out of
    /// the orphan state — e.g. somehow `orphaned_at` was cleared
    /// between scan and DELETE.
    ///
    /// The matching `snapshot_storage_location_encrypted_fields` row
    /// (when present) cascades via the FK declared in migration 050.
    fn delete_storage_location_if_orphan(
        &self,
        id: Uuid,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: usize,
    pub deleted: usize,
    pub already_gone: usize,
    /// Rows skipped because they carry an encryption sidecar. The
    /// sweeper doesn't yet decrypt; revisit once the encryption path
    /// is wired.
    pub skipped_encrypted: usize,
    /// Rows skipped because `storage_connection_string` failed to
    /// parse as a valid `s3://` URI. Terminal until the row is
    /// manually repaired.
    pub parse_errors: usize,
    /// Rows skipped because the parsed URI points at a bucket the
    /// sweeper isn't scoped to (e.g. an org-level
    /// `snapshot_base_object_storage_uri` override). Terminal from
    /// this reconciler's perspective until cross-bucket sweep is
    /// added — see [`ReconcilerConfig::scoped_bucket`].
    pub bucket_out_of_scope: usize,
    /// S3 list/delete failures. Transient; retried next tick.
    pub s3_errors: usize,
    /// DB DELETE failures after a successful S3 reap. Transient;
    /// the next tick re-checks S3 (idempotent) and retries.
    pub db_errors: usize,
    /// Rows whose conditional DELETE returned `rows_affected = 0`
    /// (orphan state was cleared under us, somehow). Re-evaluated
    /// next tick.
    pub drift_skips: usize,
    /// Candidates that the per-tick cap rate-limited; the next tick
    /// picks them up.
    pub rate_limited: usize,
}

pub struct Reconciler<S: SweeperSink, B: StorageBackend> {
    config: ReconcilerConfig,
    sink: Arc<S>,
    backend: Arc<B>,
    /// Cursor for the rolling sweep. Advances to the last-scanned
    /// `snapshot_storage_locations.id` at the end of every tick;
    /// resets to `None` when a page returns fewer than `limit` rows
    /// (signaling the end of the candidate set). Without this cursor
    /// a `max_per_tick` smaller than the number of stuck rows at the
    /// front of the ascending-`id` order would permanently starve
    /// later orphans — every tick would scan the same prefix.
    scan_after: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateOutcome {
    Deleted,
    AlreadyGone,
    SkippedEncrypted,
    ParseError,
    BucketOutOfScope,
    S3Error,
    DbError,
    DriftSkip,
}

impl<S: SweeperSink, B: StorageBackend> Reconciler<S, B> {
    pub fn new(config: ReconcilerConfig, sink: S, backend: B) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
            backend: Arc::new(backend),
            scan_after: None,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let started_at = Instant::now();
        let mut report = TickReport::default();
        // Pull a page twice as large as `max_per_tick` so the tick can
        // tell whether rows remain after the selected batch. We only
        // process the first `max_per_tick` candidates; the cursor
        // advances to the last processed row so rate-limited rows are
        // visited on the next tick instead of being skipped.
        let limit = i64::from(self.config.max_per_tick.saturating_mul(2));
        let candidates = match self
            .sink
            .list_candidates(limit, self.scan_after, self.config.min_age)
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "snapshot_storage_sweeper: list_candidates failed"
                );
                sweeper_metrics::loop_latency(started_at);
                return report;
            }
        };
        report.scanned = candidates.len();
        sweeper_metrics::candidates_scanned(candidates.len() as u64);
        let cap = usize::try_from(self.config.max_per_tick).unwrap_or(usize::MAX);

        // Advance the cursor to the last selected row whenever a
        // page has more candidates than the per-tick cap. That makes
        // rate-limited rows the first rows seen on the next tick,
        // even when the page itself is short (`len < limit`). Only
        // wrap to the start when a short page fits entirely under the
        // cap, which lets newly-orphaned lower-id rows be picked up.
        let selected_len = candidates.len().min(cap);
        let rate_limited = candidates.len().saturating_sub(selected_len);
        let next_cursor = if selected_len == 0 {
            None
        } else if rate_limited > 0 {
            candidates.get(selected_len - 1).map(|c| c.id)
        } else if (candidates.len() as i64) < limit {
            None
        } else {
            candidates.last().map(|c| c.id)
        };
        self.scan_after = next_cursor;

        let mut selected = Vec::with_capacity(selected_len);
        for candidate in candidates {
            if selected.len() >= cap {
                report.rate_limited += 1;
                continue;
            }
            selected.push(candidate);
        }

        let concurrency = self.config.concurrency.max(1);
        let outcomes = stream::iter(selected)
            .map(|candidate| {
                let sink = Arc::clone(&self.sink);
                let backend = Arc::clone(&self.backend);
                let scoped_bucket = self.config.scoped_bucket.clone();
                async move {
                    process_candidate(candidate, sink, backend, scoped_bucket.as_deref()).await
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        for outcome in outcomes {
            match outcome {
                CandidateOutcome::Deleted => report.deleted += 1,
                CandidateOutcome::AlreadyGone => report.already_gone += 1,
                CandidateOutcome::SkippedEncrypted => report.skipped_encrypted += 1,
                CandidateOutcome::ParseError => report.parse_errors += 1,
                CandidateOutcome::BucketOutOfScope => report.bucket_out_of_scope += 1,
                CandidateOutcome::S3Error => report.s3_errors += 1,
                CandidateOutcome::DbError => report.db_errors += 1,
                CandidateOutcome::DriftSkip => report.drift_skips += 1,
            }
        }

        sweeper_metrics::rate_limited(report.rate_limited as u64);
        sweeper_metrics::loop_latency(started_at);
        report
    }
}

async fn process_candidate<S: SweeperSink, B: StorageBackend>(
    candidate: SweepCandidate,
    sink: Arc<S>,
    backend: Arc<B>,
    scoped_bucket: Option<&str>,
) -> CandidateOutcome {
    if candidate.has_encrypted_fields {
        tracing::warn!(
            storage_location_id = %candidate.id,
            "snapshot_storage_sweeper: skipping encrypted row — \
             decryption path not yet implemented; revisit once \
             the sidecar write path lights up"
        );
        sweeper_metrics::failed("encrypted_unsupported");
        return CandidateOutcome::SkippedEncrypted;
    }

    let uri = match StorageUri::parse(&candidate.storage_connection_string) {
        Ok(parsed) => parsed,
        Err(err) => {
            tracing::warn!(
                storage_location_id = %candidate.id,
                error = %err,
                "snapshot_storage_sweeper: failed to parse \
                 storage_connection_string as s3:// URI; row \
                 left in place for manual repair"
            );
            sweeper_metrics::failed("parse_uri");
            return CandidateOutcome::ParseError;
        }
    };

    // Bucket-scope guard: this cluster's IAM only authorizes
    // the configured `snapshots_bucket`. Orphan rows pointing
    // at a different bucket (org-level
    // `snapshot_base_object_storage_uri` overrides) would
    // otherwise hit `AccessDenied` forever — surface them as
    // a distinct counter so dashboards can spot cross-bucket
    // backlog separately from transient S3 errors.
    if let Some(scoped) = scoped_bucket
        && uri.bucket != scoped
    {
        tracing::warn!(
            storage_location_id = %candidate.id,
            candidate_bucket = %uri.bucket,
            scoped_bucket = %scoped,
            "snapshot_storage_sweeper: candidate URI points at a \
             bucket this cluster isn't scoped to; leaving in place"
        );
        sweeper_metrics::failed("bucket_out_of_scope");
        return CandidateOutcome::BucketOutOfScope;
    }

    let delete_started = Instant::now();
    let outcome = match backend.delete_prefix(&uri).await {
        Ok(o) => o,
        Err(err) => {
            tracing::warn!(
                storage_location_id = %candidate.id,
                bucket = %uri.bucket,
                key_prefix = %uri.key_prefix,
                error = %err,
                "snapshot_storage_sweeper: S3 delete failed; \
                 will retry next tick"
            );
            sweeper_metrics::failed("s3_delete");
            return CandidateOutcome::S3Error;
        }
    };
    sweeper_metrics::s3_delete_latency(delete_started);

    let (outcome_label, candidate_outcome) = match &outcome {
        DeletePrefixOutcome::Deleted { count } => {
            tracing::info!(
                storage_location_id = %candidate.id,
                bucket = %uri.bucket,
                key_prefix = %uri.key_prefix,
                objects_deleted = count,
                "snapshot_storage_sweeper: cleared S3 prefix"
            );
            ("deleted", CandidateOutcome::Deleted)
        }
        DeletePrefixOutcome::AlreadyGone => {
            tracing::info!(
                storage_location_id = %candidate.id,
                bucket = %uri.bucket,
                key_prefix = %uri.key_prefix,
                "snapshot_storage_sweeper: S3 prefix was already empty"
            );
            ("already_gone", CandidateOutcome::AlreadyGone)
        }
    };

    match sink.delete_storage_location_if_orphan(candidate.id).await {
        Ok(true) => {
            sweeper_metrics::deleted(outcome_label);
            candidate_outcome
        }
        Ok(false) => {
            tracing::info!(
                storage_location_id = %candidate.id,
                "snapshot_storage_sweeper: DELETE refused — \
                 orphan state changed between scan and delete"
            );
            // Drift is a defense-in-depth signal: there's no
            // production writer of `orphaned_at = NULL` /
            // `snapshot_id != NULL` today, so this counter is
            // expected to stay at zero. A non-zero series
            // means either an admin tool is racing the sweep
            // or a future schema change has introduced a
            // writer we missed — both are worth surfacing in
            // dashboards rather than only in tracing logs.
            sweeper_metrics::failed("drift");
            CandidateOutcome::DriftSkip
        }
        Err(err) => {
            tracing::warn!(
                storage_location_id = %candidate.id,
                error = %err,
                "snapshot_storage_sweeper: DB DELETE failed after \
                 successful S3 reap; will retry next tick (S3 delete \
                 is idempotent)"
            );
            sweeper_metrics::failed("db_delete");
            CandidateOutcome::DbError
        }
    }
}

/// Drive the reconciler loop. Returns when `shutdown_token` fires.
///
/// When `config.enabled = false`, the function logs the
/// inert state once and then waits on the shutdown token without ever
/// running a tick — same shape as `orphaned_nodes::run`.
pub async fn run<S: SweeperSink, B: StorageBackend>(
    config: ReconcilerConfig,
    sink: S,
    backend: B,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "snapshot-storage-sweeper reconciler is disabled \
             (SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED=false); \
             waiting on shutdown without running any ticks"
        );
        shutdown_token.cancelled().await;
        tracing::info!("snapshot-storage-sweeper reconciler stopping");
        return;
    }

    // Refuse to run without a configured bucket scope. Letting the
    // sweeper run unscoped against arbitrary URIs in the DB would
    // either fail with AccessDenied forever on cross-bucket orphans
    // (irritating but harmless), or — if IAM ever got widened — let
    // a malformed row destroy data in an unrelated bucket. The env
    // gate is `SPACE_SNAPSHOT_BUCKET` (same var sbx-mgr reads for
    // the writer side); deploying the sweeper without it is a
    // misconfiguration.
    if config.scoped_bucket.is_none() {
        tracing::error!(
            "snapshot-storage-sweeper is enabled but SPACE_SNAPSHOT_BUCKET is not set; \
             refusing to run without a bucket scope. \
             Set SPACE_SNAPSHOT_BUCKET to the cluster-owned snapshots bucket and redeploy."
        );
        shutdown_token.cancelled().await;
        tracing::info!("snapshot-storage-sweeper reconciler stopping");
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        max_per_tick = config.max_per_tick,
        concurrency = config.concurrency,
        min_age_secs = ?config.min_age.map(|d| d.as_secs()),
        scoped_bucket = config.scoped_bucket.as_deref().unwrap_or(""),
        "starting snapshot-storage-sweeper reconciler"
    );
    let mut reconciler = Reconciler::new(config.clone(), sink, backend);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate first tick without processing. Gives a
    // fresh deploy / env-var flip a full window to settle before any
    // S3 deletes happen.
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let _ = reconciler.tick().await;
            }
            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }

    tracing::info!("snapshot-storage-sweeper reconciler stopping");
}

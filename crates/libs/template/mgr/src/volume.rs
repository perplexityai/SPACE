//! Volume-pull glue for [`crate::service::TemplateManagerService`].
//!
//! Scoped and explicit-tag `GetTemplate` requests resolve an exact release before
//! reading the local cache. Unscoped name-only and exact-digest requests retain
//! local-first lookup. A resolved cache miss materializes that same release:
//!
//! 1. Asking `ClusterApi.FindTemplateRelease` (cluster-role node-api,
//!    over TCP with SPACE internal bearer auth) for the release
//!    matching the requested full digest or tag (default `latest`). Tenant
//!    requests require an org authorization acknowledgement from ClusterApi.
//! 2. Disk recovery: a prior cache-miss pull whose caller's future got
//!    dropped may have left a complete artifact set in the digest dir
//!    (`metadata.json`, `manifest.json`, `rootfs.ext4.zst`,
//!    `rootfs.ext4`) without ever landing a DB row. [`try_recover_from_disk`]
//!    re-registers the row and returns; if any file is missing we fall
//!    through to the pull path.
//! 3. Issuing three synchronous `PullFile` RPCs against the on-node
//!    `space-volume-manager` daemon — one each for `metadata.json`,
//!    `manifest.json`, and `rootfs.ext4.zst`. Volume-manager handles
//!    credmgr token exchange and the actual S3 GETs; template-manager
//!    never talks to S3 directly. The three pulls run in parallel and
//!    write directly into the digest dir — vol-mgr's per-file
//!    `.partial → rename` provides per-file atomicity, and vol-mgr's
//!    skip-if-present elides any re-download of files already on disk
//!    from a prior partial attempt.
//! 4. Materializing local `rootfs.ext4` in expanded runtime shape via
//!    [`crate::rootfs_zstd::decompress_rootfs`] (process-unique tmp
//!    filename + atomic rename, so two overlapping decompresses from a
//!    cancellation race can't corrupt each other) and verifying its
//!    digest against the resolved release `hash`. Mismatch is the only
//!    hard failure on this path.
//! 5. Atomically rewriting `manifest.json` with normalized fields (via
//!    [`write_file_atomic`]) and inserting a `ready` build row in
//!    SQLite.
//!
//! There is no staging dir. A process-local flight map coalesces
//! concurrent pulls for the same `(template, content_hash)` so repeated
//! `GetTemplate` misses wait on the owner instead of starting duplicate
//! rootfs materializations. The owner work runs in a detached task; if one
//! caller's future is cancelled, vol-mgr's `BackgroundWorker` keeps any
//! started `PullFile` job running, `spawn_blocking` decompresses continue,
//! and other callers keep waiting for the same commit.
//!
//! ## Storage URI format
//!
//! Connection strings returned by `ClusterApi.FindTemplateRelease` are
//! self-describing: every bit needed to issue an S3 GET is encoded in
//! the URI itself, so neither template-manager nor volume-manager has
//! to read object-store env vars.
//!
//! ```text
//! s3://<bucket>/<key-prefix>?region=<region>[&endpoint=<url>][&path_style=true|false]
//! ```
//!
//! - `region` (required) — bucket region, e.g. `us-east-2`.
//! - `endpoint` (optional) — custom S3-compatible endpoint
//!   (e.g. `https://minio:9000`). Triggers `path_style=true` by default.
//! - `path_style` (optional) — explicit override for path-style
//!   addressing.

use crate::rootfs_backing::RootfsBackingPolicy;
use pplx_space_common_svc::template_lookup::TemplateLookupScope;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pplx_space_common_core::host::linux::paths::{
    TEMPLATE_CACHE_DIR_MODE, TEMPLATE_CACHE_FILE_MODE, normalize_template_cache_tree,
    set_template_cache_permissions,
};
use pplx_space_common_core::types::{
    ContentHash, OrgId, StorageUri, SubjectId, TEMPLATE_ROOTFS_FILE, TemplateName,
    ZSTD_FILENAME_SUFFIX,
};
use pplx_space_common_svc::bootstrap::GrpcClientTrace;
use pplx_space_node_api_client::cluster::connect_from_env as connect_cluster_api_from_env;
use pplx_space_node_api_client::proto::{
    FindTemplateReleaseByHashRequest, FindTemplateReleaseRequest,
    ListLatestTemplateReleasesRequest, TemplateVisibility,
};
use pplx_space_volmgr_client::{
    RESTORE_PRIORITY, VOLMGR_SOCKET, VolumeManagerClient,
    proto::{ExistingFilePolicy, PullFileRequest, SnapshotArtifact},
};
use tokio::sync::Notify;
use tonic::Status;
use tonic::transport::Channel;
use tracing::{Instrument as _, debug, info, warn};

/// Concrete client type returned by `pplx_space_volmgr_client::connect`.
/// The vol-mgr client now wraps the gRPC `Channel` in `GrpcClientTrace`
/// to propagate tracing context across the
/// Unix-socket boundary, so the type we hold in `pull_one` is no longer
/// the bare `Channel`.
type VolmgrClient = VolumeManagerClient<GrpcClientTrace<Channel>>;

const TEMPLATE_DIR: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_DIR;

/// Default runtime passed to `FindTemplateRelease`. The cluster API accepts
/// both full names and aliases, but the release column currently stores the
/// legacy Firecracker alias `fc`, so this caller sends the stored spelling.
pub(crate) const DEFAULT_RUNTIME: &str = pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS;

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, context: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(context, "recovering poisoned mutex");
            poisoned.into_inner()
        }
    }
}

/// Why a [`VolumePuller::pull`] was issued. Surfaced as the `trigger:`
/// metric tag on every pull-path metric so dashboards can split S3
/// download bandwidth, latency, and coalesce counts between caller-
/// driven and prefetcher-driven work.
///
/// Threaded as a typed parameter rather than a tag string so adding a
/// new caller forces a compile-time decision about which bucket it
/// falls into. Keep the variant set small — this is a
/// cardinality-bounded metric dimension, not free-form.
#[derive(Copy, Clone, Debug)]
pub enum PullTrigger {
    /// A user-facing request hit the local-miss path and is waiting on
    /// the pull. Latency here is what the caller experiences.
    OnDemand,
    /// Background prefetch driven by hardcoded `(template, tag)` tuples
    /// resolved via `ClusterApi.FindTemplateRelease`. Intent-driven
    /// warmer for tags like `main`/`prod`/`latest`. Artifact digests
    /// reached this way are also recorded in `template_pinned_tags` so
    /// a future GC won't evict them while the tag still resolves there.
    PrefetchTag,
    /// Background prefetch driven by the per-template "latest N
    /// releases" warmer, resolved via
    /// `ClusterApi.ListLatestTemplateReleases`. Artifact digests
    /// reached this way are recorded in `template_pinned_releases` so
    /// a future GC won't evict them while they're still in the
    /// latest-N window.
    PrefetchLatestRelease,
}

impl PullTrigger {
    pub(crate) fn as_tag(&self) -> &'static str {
        match self {
            Self::OnDemand => "on_demand",
            Self::PrefetchTag => "prefetch_tag",
            Self::PrefetchLatestRelease => "prefetch_latest_release",
        }
    }

    /// Whether this pull was initiated by the background prefetch loop
    /// (either pass) rather than a user-facing request. Prefetch pulls
    /// are subject to the template-visibility gate at the final resolve
    /// (see [`prefetch_pull_blocked`]); on-demand pulls are not — they
    /// serve an already-authorized sandbox create.
    pub(crate) fn is_prefetch(&self) -> bool {
        match self {
            Self::OnDemand => false,
            Self::PrefetchTag | Self::PrefetchLatestRelease => true,
        }
    }
}

/// Former opt-in switch for the visibility gate. The gate is now
/// unconditional; the name is kept only so startup can warn when a
/// deployment still sets it.
pub(crate) const RETIRED_ENV_VISIBILITY_ENFORCEMENT: &str =
    "SPACE_TEMPLATE_PREFETCH_VISIBILITY_ENFORCEMENT";

/// Whether a resolved release must NOT be downloaded for this caller.
/// Prefetch triggers fail closed on any final visibility other than a
/// confirmed `Generic` — including `Unspecified`, which means "not known
/// to be generic". This is the last line of the visibility gate: the
/// prefetch tick's decision ran one RPC earlier, and the re-resolve here
/// is the freshest answer before bytes hit the disk, so a visibility
/// flip between the two must abort the download.
///
/// No setting opens this gate. Prefetch runs on every node, so an absent
/// or unreadable visibility must never distribute a template beyond the
/// nodes the control plane placed it on.
pub(crate) fn prefetch_pull_blocked(trigger: PullTrigger, visibility: TemplateVisibility) -> bool {
    trigger.is_prefetch() && visibility != TemplateVisibility::Generic
}

/// Cloneable failure classification published to coalesced pull callers.
/// Detailed source errors are logged where they occur before conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PullFailure {
    #[error("template release is missing from cluster metadata")]
    ClusterMetadataMissing,
    #[error("cluster metadata lookup failed")]
    ClusterMetadataUnavailable,
    #[error("cluster metadata returned an invalid release")]
    ClusterMetadataInvalid,
    #[error("template is not generic; prefetch refused at the final resolve")]
    TemplateNotGeneric,
    #[error("template name is invalid")]
    InvalidTemplateName,
    #[error("cluster metadata returned an invalid content hash")]
    InvalidContentHash,
    #[error("local template database is unavailable")]
    LocalDatabaseUnavailable,
    #[error("template build eviction is in progress")]
    EvictionInProgress,
    #[error("template release has no remote storage locations")]
    NoStorageLocations,
    #[error("template release has no usable remote storage locations")]
    NoUsableStorageLocations,
    #[error("local template storage is unavailable")]
    LocalStorageUnavailable,
    #[error("volume manager is unavailable")]
    VolumeManagerUnavailable,
    #[error("template artifact is missing from object storage")]
    StorageObjectMissing,
    #[error("template object storage is unavailable")]
    StorageUnavailable,
    #[error("template manifest is malformed")]
    ManifestMalformed,
    #[error("template manifest contains an unsafe filename")]
    ManifestUnsafeFilename,
    #[error("template manifest has no artifacts")]
    ManifestEmpty,
    #[error("template manifest is missing an artifact digest")]
    ManifestDigestMissing,
    #[error("template manifest is missing a compressed artifact digest")]
    ManifestCompressedDigestMissing,
    #[error("template execution metadata is invalid")]
    ExecutionMetadataInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PullFailureClass {
    Missing,
    Retryable,
    InvalidInput,
    FailedPrecondition,
    DataLoss,
}

impl PullFailure {
    pub(crate) const fn metric_reason(self) -> &'static str {
        match self {
            Self::ClusterMetadataMissing => "cluster_metadata_missing",
            Self::ClusterMetadataUnavailable | Self::ClusterMetadataInvalid => {
                "cluster_metadata_error"
            }
            Self::TemplateNotGeneric => "template_not_generic",
            Self::InvalidTemplateName | Self::InvalidContentHash => "invalid_input",
            Self::LocalDatabaseUnavailable => "local_db_error",
            Self::EvictionInProgress => "eviction_in_progress",
            Self::NoStorageLocations => "no_storage_locations",
            Self::NoUsableStorageLocations => "no_usable_storage",
            Self::LocalStorageUnavailable => "local_io_error",
            Self::VolumeManagerUnavailable => "vol_mgr_unreachable",
            Self::StorageObjectMissing => "storage_object_missing",
            Self::StorageUnavailable => "storage_unreachable",
            Self::ManifestMalformed => "manifest_parse_failed",
            Self::ManifestUnsafeFilename => "manifest_unsafe_filename",
            Self::ManifestEmpty => "manifest_empty",
            Self::ManifestDigestMissing => "manifest_digest_missing",
            Self::ManifestCompressedDigestMissing => "manifest_compressed_digest_missing",
            Self::ExecutionMetadataInvalid => "execution_metadata_invalid",
        }
    }

    pub(crate) const fn class(self) -> PullFailureClass {
        match self {
            Self::ClusterMetadataMissing | Self::StorageObjectMissing => PullFailureClass::Missing,
            Self::InvalidTemplateName => PullFailureClass::InvalidInput,
            Self::NoStorageLocations | Self::TemplateNotGeneric => {
                PullFailureClass::FailedPrecondition
            }
            Self::ClusterMetadataInvalid
            | Self::InvalidContentHash
            | Self::NoUsableStorageLocations
            | Self::ManifestMalformed
            | Self::ManifestUnsafeFilename
            | Self::ManifestEmpty
            | Self::ManifestDigestMissing
            | Self::ManifestCompressedDigestMissing
            | Self::ExecutionMetadataInvalid => PullFailureClass::DataLoss,
            Self::ClusterMetadataUnavailable
            | Self::LocalDatabaseUnavailable
            | Self::EvictionInProgress
            | Self::LocalStorageUnavailable
            | Self::VolumeManagerUnavailable
            | Self::StorageUnavailable => PullFailureClass::Retryable,
        }
    }
}

impl PullFailureClass {
    const fn metric_result(self) -> &'static str {
        match self {
            Self::Missing => "not_found",
            Self::Retryable => "unavailable",
            Self::InvalidInput => "invalid_argument",
            Self::FailedPrecondition => "failed_precondition",
            Self::DataLoss => "data_loss",
        }
    }

    const fn is_retryable(self) -> bool {
        matches!(self, Self::Retryable)
    }
}

/// Outcome of [`VolumePuller::pull`].
#[derive(Debug)]
pub enum PullOutcome {
    /// Build was successfully materialized to disk + inserted in DB.
    /// The caller should re-run the local lookup, which now succeeds.
    Materialized,
    /// The pull could not materialize the build. The caller maps the typed
    /// failure to its corresponding gRPC status and metric reason.
    ClassifiedFailure(PullFailure),
    /// A downstream service rejected the request with a retryable status.
    /// The caller propagates this `Status` directly.
    Retryable(Status),
    /// A downstream service returned a status that should be preserved.
    DownstreamFailure {
        failure: PullFailure,
        status: Status,
    },
    /// Rootfs hash didn't match the resolved release's hash.
    HashMismatch(Status),
}

/// Pulls a template build from the cluster object-store volume.
///
/// Cheaply cloneable handle shared by on-demand `GetTemplate` requests and
/// prefetch. The local in-flight map is keyed by resolved `(name,
/// content_hash)`, after tag/hash lookup, so all callers that need the same
/// digest wait on one materialization task. Entries are removed on task
/// completion or drop; this is not a cache.
#[derive(Clone, Default)]
pub struct VolumePuller {
    pub(crate) backing_policy: RootfsBackingPolicy,
    in_flight: Arc<Mutex<HashMap<PullKey, Arc<InFlightPull>>>>,
    /// Optional handle to tmplmgr's shared durable background worker
    /// (see [`crate::background_jobs`]). When set, `try_pull_from`
    /// enqueues a `template_stamp` job after any non-`FailedPrecondition`
    /// finalize failure so the stamp converges in the background. When
    /// `None` (the test constructor), failures stay warn-and-continue —
    /// tests don't need a worker because they aren't asserting recovery.
    background_worker: Option<
        pplx_space_common_jobs::BackgroundWorker<
            crate::background_jobs::TemplateBackgroundExecutor,
        >,
    >,
}

impl VolumePuller {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a puller wired to the service's shared durable
    /// background worker. Used by `TemplateManagerService::new`; tests
    /// use [`Self::new`] and rely on the no-op fallback path.
    pub fn new_with_background_worker(
        background_worker: pplx_space_common_jobs::BackgroundWorker<
            crate::background_jobs::TemplateBackgroundExecutor,
        >,
    ) -> Self {
        Self {
            backing_policy: RootfsBackingPolicy::default(),
            in_flight: Arc::default(),
            background_worker: Some(background_worker),
        }
    }

    /// Materialize a build from the cluster-managed object-store volume.
    /// Returns a typed failure classification or preserves a downstream status
    /// when the build cannot be materialized.
    ///
    /// Cluster lookup is routed by which selector the caller supplies:
    /// when `req_hash` is non-empty the call goes to
    /// `ClusterApi.FindTemplateReleaseByHash(name, hash)` (the digest
    /// is queried directly against `template_releases.hash`); otherwise
    /// it goes to `ClusterApi.FindTemplateRelease(name, runtime, tag)`
    /// with `tag = req_tag` (or `"latest"` when both are empty). The
    /// resolved hash from whichever response wins is the integrity
    /// ground truth — the rootfs is verified against that, not against
    /// the caller's `req_hash`.
    ///
    /// `trigger` is recorded as the `trigger:` metric tag on every
    /// pull-path metric (and on the tracing span) so dashboards can split
    /// on-demand vs prefetch work.
    ///
    /// **Cancellation semantics.** If one caller's future is dropped
    /// (e.g., gRPC deadline exceeded), the resolved pull task continues
    /// and other callers for the same `(template, hash)` keep waiting on
    /// the shared flight. If the task itself is aborted or the process
    /// exits, the next `GetTemplate` recovers from whatever is on disk:
    ///
    /// - All commit artifacts present (`metadata.json`, `manifest.json`,
    ///   `rootfs.ext4.zst`, `rootfs.ext4`) → recovery upserts the DB row
    ///   and returns.
    /// - Some source files present, no decompressed rootfs → vol-mgr's
    ///   skip-if-present short-circuits the re-pulls and we just
    ///   decompress + commit.
    /// - Nothing or a partial set on disk → fresh pull.
    ///
    /// This keeps caller cancellation from multiplying rootfs
    /// decompressions while preserving the recover-from-disk behavior for
    /// process death.
    pub async fn pull(
        &self,
        db: Arc<std::sync::Mutex<rusqlite::Connection>>,
        name: &str,
        req_hash: &str,
        req_tag: &str,
        trigger: PullTrigger,
    ) -> PullOutcome {
        self.pull_in_scope(
            db,
            name,
            req_hash,
            req_tag,
            trigger,
            Some(TemplateLookupScope::Cluster),
        )
        .await
    }

    #[tracing::instrument(
        skip_all,
        fields(
            template = name,
            trigger = trigger.as_tag(),
            req_hash = req_hash,
            req_tag = req_tag,
            content_hash = tracing::field::Empty,
        )
    )]
    pub(crate) async fn pull_in_scope(
        &self,
        db: Arc<std::sync::Mutex<rusqlite::Connection>>,
        name: &str,
        req_hash: &str,
        req_tag: &str,
        trigger: PullTrigger,
        scope: Option<TemplateLookupScope>,
    ) -> PullOutcome {
        let started = Instant::now();
        let base_tags = pull_metric_tags(name, req_hash, req_tag, trigger);
        let mut path_taken = PullPath::FreshPull;

        let spec = PullSpec {
            name,
            req_hash,
            req_tag,
            trigger,
        };
        let outcome = pull_inner(self, db, spec, &base_tags, &mut path_taken, scope).await;

        pplx_space_metrics::template::pull_duration(
            &base_tags,
            outcome_label(&outcome, path_taken),
            started,
        );

        outcome
    }

    /// Materializes a release the caller already resolved through
    /// ClusterApi in `resolve_elapsed`. Records `template.pull.duration_ms`
    /// like [`Self::pull`] does: tagged with the caller's original selector
    /// and trigger, and measuring release resolution plus materialization.
    /// The caller's own work between the two (local cache probe, snapshot
    /// policy lookup) is excluded, as it is for [`Self::pull`], so both
    /// paths report the same interval under the same tags. Resolution
    /// failures never reach this method, so unlike [`Self::pull`] it records
    /// no `cluster_metadata_*` outcomes and no `template.pull.find_release_ms`.
    pub(crate) async fn materialize_release(
        &self,
        db: Arc<std::sync::Mutex<rusqlite::Connection>>,
        spec: PullSpec<'_>,
        resolve_elapsed: Duration,
        release: ResolvedRelease,
    ) -> PullOutcome {
        let started = Instant::now()
            .checked_sub(resolve_elapsed)
            .unwrap_or_else(Instant::now);
        let tags = pull_metric_tags(spec.name, spec.req_hash, spec.req_tag, spec.trigger);
        let result = self
            .pull_resolved(
                db,
                spec.name.to_string(),
                release.hash,
                release.storage_uris,
                &tags,
            )
            .await;
        let outcome = result.outcome.into_outcome();

        pplx_space_metrics::template::pull_duration(
            &tags,
            outcome_label(&outcome, result.path),
            started,
        );

        outcome
    }

    async fn pull_resolved(
        &self,
        db: Arc<std::sync::Mutex<rusqlite::Connection>>,
        name: String,
        resolved_hash: String,
        storage_uris: Vec<String>,
        base_tags: &[String],
    ) -> PullTaskResult {
        let key = PullKey::new(name.clone(), resolved_hash.clone());
        loop {
            let (flight, started) = self.start_or_join_flight(key.clone());
            if started {
                let guard = PullFlightGuard::new(
                    key.clone(),
                    Arc::clone(&flight),
                    Arc::clone(&self.in_flight),
                );
                let db = Arc::clone(&db);
                let name = name.clone();
                let resolved_hash = resolved_hash.clone();
                let storage_uris = storage_uris.clone();
                let base_tags = base_tags.to_vec();
                let background_worker = self.background_worker.clone();
                let backing_policy = self.backing_policy;
                // The caller that starts the pull waits for it below, so the
                // pull runs under that caller's span.
                tokio::spawn(
                    async move {
                        let (outcome, path_taken) = pull_resolved_owner(
                            db,
                            &name,
                            &resolved_hash,
                            &storage_uris,
                            &base_tags,
                            background_worker.as_ref(),
                            backing_policy,
                        )
                        .await;
                        guard.finish(PullTaskResult::new(&outcome, path_taken));
                    }
                    .in_current_span(),
                );
            } else {
                info!(
                    template = %name,
                    content_hash = %resolved_hash,
                    "joining in-flight template pull"
                );
            }

            match flight.wait_for_result().await {
                Some(mut result) => {
                    if !started && result.mark_coalesced_if_materialized() {
                        pplx_space_metrics::template::pull_coalesced(base_tags);
                    }
                    return result;
                }
                None => {
                    warn!(
                        template = %name,
                        content_hash = %resolved_hash,
                        "in-flight template pull ended without a result; retrying as owner"
                    );
                }
            }
        }
    }

    fn start_or_join_flight(&self, key: PullKey) -> (Arc<InFlightPull>, bool) {
        let mut in_flight = lock_or_recover(&self.in_flight, "template pull in-flight map");
        if let Some(existing) = in_flight.get(&key) {
            return (Arc::clone(existing), false);
        }

        let flight = Arc::new(InFlightPull::new());
        in_flight.insert(key, Arc::clone(&flight));
        (flight, true)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PullKey {
    name: String,
    content_hash: String,
}

impl PullKey {
    fn new(name: impl Into<String>, content_hash: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content_hash: content_hash.into(),
        }
    }
}

#[derive(Clone, Debug)]
struct PullTaskResult {
    outcome: PullOutcomeSnapshot,
    path: PullPath,
}

impl PullTaskResult {
    fn new(outcome: &PullOutcome, path: PullPath) -> Self {
        Self {
            outcome: PullOutcomeSnapshot::from(outcome),
            path,
        }
    }

    fn mark_coalesced_if_materialized(&mut self) -> bool {
        if self.outcome.is_materialized() {
            self.path = PullPath::Coalesced;
            return true;
        }
        false
    }
}

#[derive(Clone, Debug)]
enum PullOutcomeSnapshot {
    Materialized,
    ClassifiedFailure(PullFailure),
    Retryable(Status),
    DownstreamFailure {
        failure: PullFailure,
        status: Status,
    },
    HashMismatch(Status),
}

impl PullOutcomeSnapshot {
    fn is_materialized(&self) -> bool {
        matches!(self, Self::Materialized)
    }

    fn into_outcome(self) -> PullOutcome {
        match self {
            Self::Materialized => PullOutcome::Materialized,
            Self::ClassifiedFailure(failure) => PullOutcome::ClassifiedFailure(failure),
            Self::Retryable(status) => PullOutcome::Retryable(status),
            Self::DownstreamFailure { failure, status } => {
                PullOutcome::DownstreamFailure { failure, status }
            }
            Self::HashMismatch(status) => PullOutcome::HashMismatch(status),
        }
    }
}

impl From<&PullOutcome> for PullOutcomeSnapshot {
    fn from(outcome: &PullOutcome) -> Self {
        match outcome {
            PullOutcome::Materialized => Self::Materialized,
            PullOutcome::ClassifiedFailure(failure) => Self::ClassifiedFailure(*failure),
            PullOutcome::Retryable(status) => Self::Retryable(status.clone()),
            PullOutcome::DownstreamFailure { failure, status } => Self::DownstreamFailure {
                failure: *failure,
                status: status.clone(),
            },
            PullOutcome::HashMismatch(status) => Self::HashMismatch(status.clone()),
        }
    }
}

#[derive(Default)]
struct InFlightPull {
    notify: Notify,
    state: Mutex<InFlightPullState>,
}

impl InFlightPull {
    fn new() -> Self {
        Self::default()
    }

    async fn wait_for_result(&self) -> Option<PullTaskResult> {
        loop {
            let mut notified = std::pin::pin!(self.notify.notified());
            {
                let state = lock_or_recover(&self.state, "template pull state");
                if let Some(result) = state.result.clone() {
                    return Some(result);
                }
                if state.released {
                    return None;
                }
                notified.as_mut().enable();
            }
            notified.await;
        }
    }
}

#[derive(Default)]
struct InFlightPullState {
    result: Option<PullTaskResult>,
    released: bool,
}

struct PullFlightGuard {
    key: PullKey,
    flight: Arc<InFlightPull>,
    in_flight: Arc<Mutex<HashMap<PullKey, Arc<InFlightPull>>>>,
    released: bool,
}

impl PullFlightGuard {
    fn new(
        key: PullKey,
        flight: Arc<InFlightPull>,
        in_flight: Arc<Mutex<HashMap<PullKey, Arc<InFlightPull>>>>,
    ) -> Self {
        Self {
            key,
            flight,
            in_flight,
            released: false,
        }
    }

    fn finish(mut self, result: PullTaskResult) {
        {
            let mut state = lock_or_recover(&self.flight.state, "template pull state");
            state.result = Some(result);
            state.released = true;
        }
        self.release();
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        {
            let mut state = lock_or_recover(&self.flight.state, "template pull state");
            state.released = true;
        }
        {
            let mut in_flight = lock_or_recover(&self.in_flight, "template pull in-flight map");
            if in_flight
                .get(&self.key)
                .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
            {
                in_flight.remove(&self.key);
            }
        }
        self.released = true;
        self.flight.notify.notify_waiters();
    }
}

impl Drop for PullFlightGuard {
    fn drop(&mut self) {
        self.release();
    }
}

/// Which path the caller ended up taking, used solely for metric tagging
/// on `template.pull.duration_ms{result=...}`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PullPath {
    /// Pulled fresh from object storage (cluster API → vol-mgr).
    FreshPull,
    /// Filesystem already had the artifacts; just re-registered the DB row.
    Recovered,
    /// DB row was present from a prior call — no work needed.
    DbHit,
    /// Another caller already owned the same resolved digest pull.
    Coalesced,
}

/// One pull's identity as the caller specified it: the template name,
/// the optional digest/tag lookup keys, and which caller class
/// initiated it. Groups what [`VolumePuller::pull`] receives so the
/// inner helpers stay within clippy's argument budget, and carries the
/// original selector into [`VolumePuller::materialize_release`].
#[derive(Copy, Clone)]
pub(crate) struct PullSpec<'a> {
    pub(crate) name: &'a str,
    pub(crate) req_hash: &'a str,
    pub(crate) req_tag: &'a str,
    pub(crate) trigger: PullTrigger,
}

async fn pull_inner(
    puller: &VolumePuller,
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
    spec: PullSpec<'_>,
    base_tags: &[String],
    path_taken: &mut PullPath,
    scope: Option<TemplateLookupScope>,
) -> PullOutcome {
    let name = spec.name;
    // 1. Resolve (release.hash, storage_uris) via ClusterApi.
    let release = match find_release(spec, base_tags, scope).await {
        Ok(found) => found,
        Err(FindReleaseError::NotFound) => {
            return PullOutcome::ClassifiedFailure(PullFailure::ClusterMetadataMissing);
        }
        Err(FindReleaseError::Unavailable) => {
            return PullOutcome::ClassifiedFailure(PullFailure::ClusterMetadataUnavailable);
        }
        Err(FindReleaseError::InvalidResponse) => {
            return PullOutcome::ClassifiedFailure(PullFailure::ClusterMetadataInvalid);
        }
        Err(FindReleaseError::Status(status)) => {
            return PullOutcome::DownstreamFailure {
                failure: PullFailure::ClusterMetadataUnavailable,
                status,
            };
        }
    };

    // The catalog has enforced generic-only selection in the same SQL snapshot
    // as the release and locations. Cached visibility is only an additional
    // conservative gate; it cannot authorize the final prefetch resolution.
    if prefetch_pull_blocked(spec.trigger, release.visibility) {
        warn!(
            template = name,
            content_hash = %release.hash,
            visibility = ?release.visibility,
            trigger = spec.trigger.as_tag(),
            "prefetch pull aborted: template is not confirmed generic at the final resolve"
        );
        return PullOutcome::ClassifiedFailure(PullFailure::TemplateNotGeneric);
    }

    let ResolvedRelease {
        hash: resolved_hash,
        storage_uris,
        ..
    } = release;

    // Record the resolved hash on the surrounding `pull` span so every log
    // line emitted below this point carries both `template` and `content_hash`
    // automatically — without us threading them as explicit parameters through
    // every helper.
    tracing::Span::current().record("content_hash", resolved_hash.as_str());
    info!(
        template = name,
        content_hash = %resolved_hash,
        storage_locations = storage_uris.len(),
        "resolved template release; starting pull"
    );

    let result = puller
        .pull_resolved(db, name.to_string(), resolved_hash, storage_uris, base_tags)
        .await;
    *path_taken = result.path;
    result.outcome.into_outcome()
}

/// Picks the lookup tag for `FindTemplateRelease`: `req_tag` if set,
/// else `"latest"`. Only used on the tag-based lookup path —
/// `find_release_inner` routes hash-based callers to
/// `FindTemplateReleaseByHash` directly, so `req_hash` is never
/// re-fed into a tag lookup.
fn lookup_tag(req_tag: &str) -> String {
    if !req_tag.is_empty() {
        req_tag.to_string()
    } else {
        "latest".to_string()
    }
}

fn pull_metric_tags(
    name: &str,
    req_hash: &str,
    req_tag: &str,
    trigger: PullTrigger,
) -> Vec<String> {
    pplx_space_metrics::template::request_tags(name, req_hash, req_tag, trigger.as_tag())
}

fn outcome_label(outcome: &PullOutcome, path: PullPath) -> &'static str {
    match outcome {
        PullOutcome::Materialized => match path {
            PullPath::FreshPull => "materialized",
            PullPath::Recovered => "recovered",
            PullPath::DbHit => "db_hit",
            PullPath::Coalesced => "coalesced",
        },
        PullOutcome::ClassifiedFailure(failure) => failure.class().metric_result(),
        PullOutcome::Retryable(_) => "retryable",
        PullOutcome::DownstreamFailure { .. } => "downstream_error",
        PullOutcome::HashMismatch(_) => "hash_mismatch",
    }
}

fn record_phase(base_tags: &[String], phase: &'static str, result: &'static str, started: Instant) {
    pplx_space_metrics::template::pull_s3_phase(base_tags, phase, result, started);
}

async fn pull_resolved_owner(
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
    name: &str,
    resolved_hash: &str,
    storage_uris: &[String],
    base_tags: &[String],
    background_worker: Option<
        &pplx_space_common_jobs::BackgroundWorker<
            crate::background_jobs::TemplateBackgroundExecutor,
        >,
    >,
    backing_policy: RootfsBackingPolicy,
) -> (PullOutcome, PullPath) {
    // DB fast-path: a prior call (or startup recovery) already committed
    // this build. This check intentionally runs inside the owner task too:
    // a caller may have won the flight after another caller finished between
    // its first local miss and the flight-map insert.
    if local_build_exists(&db, name, resolved_hash) {
        info!(
            template = name,
            content_hash = %resolved_hash,
            "build already present in DB; no pull needed"
        );
        return (PullOutcome::Materialized, PullPath::DbHit);
    }

    // Disk recovery: a prior cache-miss pull (possibly one whose caller
    // was cancelled) may have left a complete artifact set in the digest
    // dir without finishing the DB upsert. Re-register from disk.
    if try_recover_from_disk(&db, name, resolved_hash).await {
        return (PullOutcome::Materialized, PullPath::Recovered);
    }

    // Fresh pull. Vol-mgr deduplicates concurrent PullFile calls server-side,
    // and its skip-if-present check elides re-downloads of files already on
    // disk from a prior incomplete attempt.
    let outcome = match do_pull(
        db,
        name,
        resolved_hash,
        storage_uris,
        base_tags,
        background_worker,
        backing_policy,
    )
    .await
    {
        Ok(()) => PullOutcome::Materialized,
        Err(PullError::Failure(failure)) => PullOutcome::ClassifiedFailure(failure),
        Err(PullError::Retryable(status)) => PullOutcome::Retryable(status),
        Err(PullError::Failed(status)) => PullOutcome::DownstreamFailure {
            failure: PullFailure::StorageUnavailable,
            status,
        },
        Err(PullError::HashMismatch { expected, actual }) => {
            PullOutcome::HashMismatch(Status::data_loss(format!(
                "rootfs.ext4 hash mismatch: expected {expected}, got {actual}"
            )))
        }
    };
    (outcome, PullPath::FreshPull)
}

/// Connects to the cluster-role node-api and asks
/// `FindTemplateRelease(name, runtime, tag)`.
async fn find_release(
    spec: PullSpec<'_>,
    base_tags: &[String],
    scope: Option<TemplateLookupScope>,
) -> Result<ResolvedRelease, FindReleaseError> {
    let started = Instant::now();
    let PullSpec {
        name,
        req_hash,
        req_tag,
        trigger,
    } = spec;
    let generic_only = trigger.is_prefetch();
    let result =
        find_release_with_requirement(name, req_hash, req_tag, None, generic_only, scope).await;
    let result_label = match &result {
        Ok(_) => "ok",
        Err(FindReleaseError::NotFound) => "not_found",
        Err(
            FindReleaseError::Unavailable
            | FindReleaseError::InvalidResponse
            | FindReleaseError::Status(_),
        ) => "error",
    };
    pplx_space_metrics::template::pull_find_release(base_tags, result_label, started);
    // Propagate the ResolvedRelease (hash + storage URIs + template
    // visibility) as-is; pull_inner reads `.visibility` for the prefetch
    // final-resolve gate and destructures the rest. FindReleaseError is
    // returned unchanged for the caller to classify.
    result
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FindReleaseError {
    #[error("template release not found")]
    NotFound,
    #[error("cluster metadata service is unavailable")]
    Unavailable,
    #[error("cluster metadata service returned an invalid release")]
    InvalidResponse,
    #[error("cluster metadata lookup failed: {0}")]
    Status(#[source] Status),
}

fn classify_find_release_status(status: Status) -> FindReleaseError {
    match status.code() {
        tonic::Code::NotFound => FindReleaseError::NotFound,
        tonic::Code::Internal | tonic::Code::Unknown => FindReleaseError::Unavailable,
        _ => FindReleaseError::Status(status),
    }
}

/// An immutable release resolved by ClusterApi, authorized when caller-scoped.
/// `visibility` is advisory prefetch metadata, not authorization evidence.
pub(crate) struct ResolvedRelease {
    pub(crate) hash: String,
    pub(crate) storage_uris: Vec<String>,
    pub(crate) visibility: TemplateVisibility,
}

pub(crate) async fn find_release_inner(
    name: &str,
    req_hash: &str,
    req_tag: &str,
    caller_org: Option<OrgId>,
) -> Result<ResolvedRelease, FindReleaseError> {
    find_release_in_scope(
        name,
        req_hash,
        req_tag,
        caller_org,
        Some(TemplateLookupScope::Cluster),
    )
    .await
}

pub(crate) async fn find_release_in_scope(
    name: &str,
    req_hash: &str,
    req_tag: &str,
    caller_org: Option<OrgId>,
    scope: Option<TemplateLookupScope>,
) -> Result<ResolvedRelease, FindReleaseError> {
    find_release_with_requirement(name, req_hash, req_tag, caller_org, false, scope).await
}

async fn find_release_with_requirement(
    name: &str,
    req_hash: &str,
    req_tag: &str,
    caller_org: Option<OrgId>,
    generic_only: bool,
    scope: Option<TemplateLookupScope>,
) -> Result<ResolvedRelease, FindReleaseError> {
    let mut client = connect_cluster_api_from_env().await.map_err(|error| {
        warn!(error = %error, "cluster api connect failed");
        FindReleaseError::Unavailable
    })?;

    // Route to the right cluster RPC based on which lookup key the
    // caller supplied. Hash-based lookup hits `template_releases.hash`
    // directly — the canonical home for the digest, no dependence on
    // any auto-tag-by-hash convention. Tag-based lookup goes through
    // `template_release_tags`. Runtime is needed for the tag path
    // (since the same tag can map to different runtime artifacts),
    // but not for the hash path (digests are runtime-scoped).
    let resp = if !req_hash.is_empty() {
        let hash = req_hash.to_string();
        let mut request = tonic::Request::new(FindTemplateReleaseByHashRequest {
            generic_only,
            caller_org_id: caller_org.map(|org| org.to_string()),
            template_name: name.to_string(),
            hash: hash.clone(),
        });
        if let Some(scope) = scope {
            scope.attach(&mut request);
        }
        match client.find_template_release_by_hash(request).await {
            Ok(r) => r,
            Err(status) => {
                if status.code() == tonic::Code::NotFound {
                    debug!(
                        template = name,
                        %hash,
                        "FindTemplateReleaseByHash returned NotFound",
                    );
                } else {
                    warn!(
                        error = %status,
                        template = name,
                        %hash,
                        "FindTemplateReleaseByHash failed",
                    );
                }
                return Err(classify_find_release_status(status));
            }
        }
    } else {
        let tag = lookup_tag(req_tag);
        let mut request = tonic::Request::new(FindTemplateReleaseRequest {
            generic_only,
            caller_org_id: caller_org.map(|org| org.to_string()),
            template_name: name.to_string(),
            runtime: DEFAULT_RUNTIME.to_string(),
            tag: tag.clone(),
        });
        if let Some(scope) = scope {
            scope.attach(&mut request);
        }
        match client.find_template_release(request).await {
            Ok(r) => r,
            Err(status) => {
                if status.code() == tonic::Code::NotFound {
                    debug!(template = name, %tag, "FindTemplateRelease returned NotFound");
                } else {
                    warn!(error = %status, template = name, %tag, "FindTemplateRelease failed");
                }
                return Err(classify_find_release_status(status));
            }
        }
    };

    if let Some(scope) = scope {
        scope
            .observe_response(&resp)
            .map_err(FindReleaseError::Status)?;
    }
    let resp = resp.into_inner();
    parse_release_response(name, req_hash, caller_org, generic_only, resp)
}

fn parse_release_response(
    name: &str,
    req_hash: &str,
    caller_org: Option<OrgId>,
    generic_only: bool,
    resp: pplx_space_node_api_client::proto::FindTemplateReleaseResponse,
) -> Result<ResolvedRelease, FindReleaseError> {
    // Read the enum accessor before destructuring `resp.release` — taking
    // the release out of the struct is a partial move, after which `resp`
    // can no longer be borrowed as a whole.
    if (generic_only && !resp.generic_authorized)
        || resp.authorized_org_id != caller_org.map(|org| org.to_string())
    {
        return Err(FindReleaseError::InvalidResponse);
    }
    let visibility = resp.template_visibility();
    let release = match resp.release {
        Some(r) => r,
        None => {
            warn!("FindTemplateRelease returned empty release");
            return Err(FindReleaseError::InvalidResponse);
        }
    };
    let hash = release
        .hash
        .parse::<ContentHash>()
        .map_err(|_| FindReleaseError::InvalidResponse)?;
    if release.template_name != name
        || release.runtime != DEFAULT_RUNTIME
        || (!req_hash.is_empty() && req_hash.parse::<ContentHash>().ok() != Some(hash))
    {
        return Err(FindReleaseError::InvalidResponse);
    }
    let storage_uris: Vec<String> = resp
        .storage_locations
        .into_iter()
        .map(|s| s.connection_string)
        .filter(|s| !s.is_empty())
        .collect();
    if storage_uris.is_empty() {
        debug!(
            template = name,
            hash = %release.hash,
            "release is local-only; no object-store storage_locations registered"
        );
    }
    Ok(ResolvedRelease {
        hash: hash.to_string(),
        storage_uris,
        visibility,
    })
}

/// Materialized rootfs after the manifest-driven pull plan settles.
/// `compressed` is `Some((hash, size))` when the manifest declared a
/// `.zst` sibling (decompression produced the rootfs and gave us the
/// compressed digest as a side effect) and `None` for legacy templates
/// that only published the uncompressed artifact.
#[derive(Debug)]
struct RootfsMaterialization {
    rootfs_size: u64,
    rootfs_hash: String,
    compressed: Option<(String, u64)>,
}

/// One file we plan to download as part of `do_pull`'s parallel join.
/// Built from `manifest.files` by [`build_manifest_pull_plan`], which
/// applies the dedup rule (download only `X.zst` when both `X` and
/// `X.zst` are listed with a well-formed zstd descriptor).
#[derive(Debug, Clone)]
struct ManifestPullEntry {
    manifest_key: String,
    s3_uri: String,
    local_path: String,
    phase: &'static str,
    raw: bool,
}

#[derive(Debug, thiserror::Error)]
enum PullError {
    #[error(transparent)]
    Failure(#[from] PullFailure),
    #[error("retryable volume pull failure: {0}")]
    Retryable(#[source] Status),
    #[error("volume pull failed: {0}")]
    Failed(#[source] Status),
    #[error("template artifact hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PullStatusDisposition {
    Missing,
    Unreachable,
    Retryable,
    Failed,
}

fn pull_status_disposition(status: &Status) -> PullStatusDisposition {
    match status.code() {
        tonic::Code::NotFound => PullStatusDisposition::Missing,
        tonic::Code::ResourceExhausted => PullStatusDisposition::Retryable,
        tonic::Code::Internal
        | tonic::Code::Unknown
        | tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Aborted => PullStatusDisposition::Unreachable,
        _ => PullStatusDisposition::Failed,
    }
}

fn prefer_retryable_failure(current: PullFailure, candidate: PullFailure) -> PullFailure {
    if current.class().is_retryable() && !candidate.class().is_retryable() {
        return current;
    }
    candidate
}

fn finish_pull_failures(classified: PullFailure, downstream_status: Option<Status>) -> PullError {
    if classified.class().is_retryable() {
        return PullError::Failure(classified);
    }
    downstream_status
        .map(PullError::Failed)
        .unwrap_or(PullError::Failure(classified))
}

async fn do_pull(
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
    name: &str,
    content_hash: &str,
    storage_uris: &[String],
    base_tags: &[String],
    background_worker: Option<
        &pplx_space_common_jobs::BackgroundWorker<
            crate::background_jobs::TemplateBackgroundExecutor,
        >,
    >,
    backing_policy: RootfsBackingPolicy,
) -> Result<(), PullError> {
    // Validate name + hash once — both are URI-independent.
    pplx_space_common_core::types::TemplateName::parse(name.to_string()).map_err(|err| {
        warn!(error = %err, name, "invalid template name; volume pull skipped");
        PullError::Failure(PullFailure::InvalidTemplateName)
    })?;
    if content_hash
        .parse::<pplx_space_common_core::types::ContentHash>()
        .is_err()
    {
        warn!(content_hash, "release hash is not a valid ContentHash");
        return Err(PullError::Failure(PullFailure::InvalidContentHash));
    }

    // Early-abort guard: refuse to pull a build that's mid-eviction
    // (the eviction loop has flipped its DB row to `state='deleting'`
    // in Phase 1, but the disk delete / row delete haven't finished
    // yet). Without this, the pull would write artifacts that
    // eviction is about to delete, and the upsert at the end would
    // either be no-op'd by the `state != 'deleting'` guard in
    // `upsert_build` (leaving on-disk orphans) or silently resurrect
    // the doomed row.
    //
    // The caller's `GetTemplate` handler surfaces this as retryable
    // `Unavailable`. Retrying after eviction completes succeeds: the row is
    // gone, `resolve_template` misses, and the next pull starts cleanly.
    let n = name.to_string();
    let h = content_hash.to_string();
    let db_for_check = Arc::clone(&db);
    let is_deleting = tokio::task::spawn_blocking(move || {
        let conn = db_for_check.lock().map_err(|_| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                Some("template DB mutex poisoned".to_string()),
            )
        })?;
        crate::db::is_build_marked_deleting(&conn, &n, &h)
    })
    .await
    .map_err(|err| {
        warn!(error = %err, "is_build_marked_deleting spawn_blocking panicked");
        PullError::Failure(PullFailure::LocalDatabaseUnavailable)
    })?
    .map_err(|err| {
        warn!(error = %err, "is_build_marked_deleting failed");
        PullError::Failure(PullFailure::LocalDatabaseUnavailable)
    })?;
    if is_deleting {
        info!(
            template = name,
            content_hash,
            "refusing pull: build is in state='deleting' (mid-eviction); retry after eviction completes"
        );
        return Err(PullError::Failure(PullFailure::EvictionInProgress));
    }

    if storage_uris.is_empty() {
        debug!(
            template = name,
            content_hash,
            "release is local-only and not cached on this node; skipping object-store pull"
        );
        return Err(PullError::Failure(PullFailure::NoStorageLocations));
    }

    // Iterate every parseable storage_location so replication / failover
    // entries get a real chance. A failure on URI[0] falls through to URI[1],
    // except for admission rejection and hash mismatch. HashMismatch is fatal
    // because it means the cluster metadata disagrees with content-addressed
    // bytes; trying further URIs would mask that corruption.
    let mut tried_any = false;
    let mut last_failure = PullFailure::NoUsableStorageLocations;
    let mut last_downstream_status = None;
    for uri in storage_uris {
        let parts = match StorageUri::parse(uri) {
            Ok(p) => p,
            Err(err) => {
                warn!(uri = %uri, error = %err, "skipping unusable storage_location");
                continue;
            }
        };
        tried_any = true;
        match try_pull_from(
            Arc::clone(&db),
            name,
            content_hash,
            &parts,
            base_tags,
            background_worker,
            backing_policy,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(PullError::HashMismatch { expected, actual }) => {
                return Err(PullError::HashMismatch { expected, actual });
            }
            Err(error @ PullError::Retryable(_)) => return Err(error),
            Err(PullError::Failed(status)) => {
                warn!(
                    uri = %uri,
                    error = %status,
                    "pull from storage_location returned a permanent status; trying next location",
                );
                last_downstream_status = Some(status);
            }
            Err(PullError::Failure(failure)) => {
                last_failure = prefer_retryable_failure(last_failure, failure);
                warn!(
                    uri = %uri,
                    reason = failure.metric_reason(),
                    error = %failure,
                    "pull from storage_location failed; trying next location",
                );
            }
        }
    }

    if !tried_any {
        warn!(
            ?storage_uris,
            "no usable storage_location; skipping object-store pull"
        );
    }
    Err(finish_pull_failures(last_failure, last_downstream_status))
}

async fn try_pull_from(
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
    name: &str,
    content_hash: &str,
    parts: &StorageUri,
    base_tags: &[String],
    background_worker: Option<
        &pplx_space_common_jobs::BackgroundWorker<
            crate::background_jobs::TemplateBackgroundExecutor,
        >,
    >,
    backing_policy: RootfsBackingPolicy,
) -> Result<(), PullError> {
    // Parse the template name into the typed identity that becomes the
    // SubjectId we hand to volume-manager. This is also the *only* boundary
    // parse we need on the local-input side; credmgr-side identity (AWS STS
    // exchange, IAM scoping) is owned by volume-manager.
    let template_name = TemplateName::parse(name.to_string()).map_err(|err| {
        warn!(error = %err, template = name, content_hash, "invalid template name; volume pull skipped");
        PullError::Failure(PullFailure::InvalidTemplateName)
    })?;
    let subject = SubjectId::Template(template_name);

    info!(
        template = name,
        content_hash,
        bucket = %parts.bucket,
        key_prefix = %parts.key_prefix,
        region = %parts.region,
        endpoint = parts.endpoint.as_deref().unwrap_or(""),
        path_style = parts.path_style,
        "materializing template build via volume-manager"
    );

    // Write directly into the content-addressed digest dir. No staging dir:
    // vol-mgr's per-file `.partial → rename` gives us per-file atomicity, the
    // digest dir is named by `content_hash` (so a stale dir from a prior
    // partial attempt can't be confused for a different build), and the next
    // GetTemplate's recovery picks up wherever we left off if our caller's
    // future is dropped.
    let name_dir = format!("{TEMPLATE_DIR}/{name}");
    if let Err(err) = tokio::fs::create_dir_all(&name_dir).await {
        warn!(error = %err, template = name, content_hash, dir = %name_dir, "create name dir failed; falling through");
        return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
    }
    if let Err(err) = set_template_cache_permissions(Path::new(&name_dir), TEMPLATE_CACHE_DIR_MODE)
    {
        warn!(error = %err, template = name, content_hash, dir = %name_dir, "set name dir permissions failed; falling through");
        return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
    }
    let digest_dir = format!("{TEMPLATE_DIR}/{name}/{content_hash}");
    let digest_dir_existed = Path::new(&digest_dir).exists();
    // Arm the cleanup guard before any subvolume create / file write
    // touches the digest dir. Any error path below — including the
    // create_subvolume fallback's plain-dir path, vol-mgr connect
    // failure, manifest parse failure, hash mismatch, and atomic write
    // failure — drops the guard, which wipes the dir so the next
    // caller's `digest_dir_existed` adoption can't pick up partial
    // bytes. Disarmed below right before the final `Ok(())` once the
    // sqlite upsert has committed.
    let cleanup_guard = PullCleanupGuard::new(name, content_hash, &digest_dir);
    if !digest_dir_existed {
        // Create the digest dir as a btrfs subvolume via node-manager.
        // The ioctl requires CAP_SYS_ADMIN, so delegate to privileged
        // node-manager. An unprivileged direct call would return EPERM and
        // fall back to a plain directory, breaking btrfs-send's subvolume
        // precondition. Non-btrfs hosts still fall back to plain mkdir.
        match crate::btrfs_via_nodemgr::create_template_subvolume(name, content_hash).await {
            Ok(_path) => {
                tracing::debug!(
                    template = name,
                    content_hash,
                    dir = %digest_dir,
                    "on-demand pull: digest dir created as btrfs subvolume (via nodemgr)"
                );
            }
            Err(err) => {
                tracing::info!(
                    template = name,
                    content_hash,
                    dir = %digest_dir,
                    error = %err,
                    "on-demand pull: nodemgr btrfs subvolume create failed; falling back to plain directory"
                );
                if let Err(err) = tokio::fs::create_dir_all(&digest_dir).await {
                    warn!(error = %err, template = name, content_hash, dir = %digest_dir, "create digest dir failed; falling through");
                    return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
                }
            }
        }
    }
    if let Err(err) =
        set_template_cache_permissions(Path::new(&digest_dir), TEMPLATE_CACHE_DIR_MODE)
    {
        warn!(error = %err, template = name, content_hash, dir = %digest_dir, "set digest dir permissions failed; falling through");
        return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
    }

    // Connect to volume-manager once; clone the client per parallel pull.
    // Tonic's generated client requires &mut self for each call, so a clone
    // per concurrent call is the cheapest way to fan out three RPCs.
    let client = pplx_space_volmgr_client::connect(VOLMGR_SOCKET)
        .await
        .map_err(|err| {
            warn!(
                error = %err,
                template = name,
                content_hash,
                socket = %VOLMGR_SOCKET,
                "vol-mgr connect failed; volume pull skipped"
            );
            PullError::Failure(PullFailure::VolumeManagerUnavailable)
        })?;

    let metadata_path = format!("{digest_dir}/metadata.json");
    let manifest_path = format!("{digest_dir}/manifest.json");

    // 1. Pull metadata.json + manifest.json in parallel. These are tiny
    //    (sub-kB) and unconditional — every build has both, and they're
    //    not listed in `manifest.files` (the manifest can't list itself).
    let metadata_fut = pull_one(
        client.clone(),
        &subject,
        build_object_uri(parts, "metadata.json"),
        metadata_path.clone(),
        PullOptions::control("metadata"),
        base_tags,
    );
    let manifest_fut = pull_one(
        client.clone(),
        &subject,
        build_object_uri(parts, "manifest.json"),
        manifest_path.clone(),
        PullOptions::control("manifest"),
        base_tags,
    );
    let (meta_bytes, man_bytes) = tokio::try_join!(metadata_fut, manifest_fut)?;
    pplx_space_metrics::template::pull_s3_bytes(base_tags, Some(&parts.region), meta_bytes);
    pplx_space_metrics::template::pull_s3_bytes(base_tags, Some(&parts.region), man_bytes);

    // 2. Read + parse the manifest from disk so the rest of `do_pull`
    //    decides what to pull based on what the manifest actually
    //    declares, not on a hard-coded `(rootfs.ext4, rootfs.ext4.zst)`
    //    set. Older templates published before zstd existed list only
    //    `rootfs.ext4` in `files`; modern ones list both.
    let metadata_bytes = match tokio::fs::read(&metadata_path).await {
        Ok(b) => b,
        Err(err) => {
            warn!(
                error = %err,
                template = name,
                content_hash,
                path = %metadata_path,
                "read staging metadata failed; falling through"
            );
            return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
        }
    };
    let manifest_bytes = match tokio::fs::read(&manifest_path).await {
        Ok(b) => b,
        Err(err) => {
            warn!(
                error = %err,
                template = name,
                content_hash,
                path = %manifest_path,
                "read staging manifest failed; falling through"
            );
            return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
        }
    };
    // Parse the manifest explicitly so a malformed `manifest.json`
    // surfaces as `manifest_parse_failed` instead of silently
    // collapsing to an empty `TemplateManifest::default()` (which would
    // then hit the `manifest_empty` branch below, masking the real
    // failure mode from logs + metrics). Mirrors the recovery path's
    // parse handling.
    let parsed_manifest: pplx_space_common_core::types::TemplateManifest =
        match serde_json::from_slice(&manifest_bytes) {
            Ok(m) => m,
            Err(err) => {
                warn!(
                    error = %err,
                    template = name,
                    content_hash,
                    path = %manifest_path,
                    "manifest parse failed during pull"
                );
                return Err(PullError::Failure(PullFailure::ManifestMalformed));
            }
        };
    // Defense-in-depth: a tampered or corrupted `manifest.json` could
    // otherwise steer file writes out of the digest dir, since every
    // `manifest.files` key (and `bootable_artifact`) is interpolated
    // into a local path below. See [`TemplateManifest::validate_filenames`]
    // for the rejection rules. Run at the manifest parse boundary,
    // once per pull, before anything reads `manifest.files`.
    if let Err(err) = parsed_manifest.validate_filenames() {
        warn!(
            template = name,
            content_hash,
            error = %err,
            "manifest filename validation rejected the pulled manifest"
        );
        return Err(PullError::Failure(PullFailure::ManifestUnsafeFilename));
    }

    let metadata = crate::service::validate_guest_documents(&metadata_bytes, &manifest_bytes)
        .map_err(|error| {
            warn!(template = name, %error, "invalid pulled template documents");
            PullError::Failure(PullFailure::ExecutionMetadataInvalid)
        })?;

    if metadata.guest_image.is_some() {
        warn!(
            template = name,
            "refusing unsupported or local-only guest image in template pull"
        );
        return Err(PullError::Failure(PullFailure::ExecutionMetadataInvalid));
    }
    // 3. Build the download plan from `manifest.files` with the dedup
    //    rule: when both `X` and `X.zst` are listed (with a well-formed
    //    zstd descriptor), drop `X` and pull only `X.zst`. Decompression
    //    regenerates `X` on disk. See [`build_manifest_pull_plan`].
    let pull_plan = build_manifest_pull_plan(&parsed_manifest, parts, &digest_dir);
    if pull_plan.is_empty() {
        warn!(
            template = name,
            content_hash,
            "manifest declares no files to pull; refusing to materialize an empty build"
        );
        return Err(PullError::Failure(PullFailure::ManifestEmpty));
    }
    info!(
        template = name,
        content_hash,
        files = ?pull_plan.iter().map(|e| &e.manifest_key).collect::<Vec<_>>(),
        "vol-mgr pull plan resolved from manifest"
    );

    // 4. Parallel-pull every entry in the plan. `try_join_all` mirrors
    //    the previous `try_join!`'s short-circuit semantics: the first
    //    failure aborts the join and the remaining pulls keep running on
    //    vol-mgr's side (vol-mgr's `BackgroundWorker` doesn't observe our
    //    cancellation).
    let pull_futs: Vec<_> = pull_plan
        .iter()
        .map(|entry| {
            pull_one(
                client.clone(),
                &subject,
                entry.s3_uri.clone(),
                entry.local_path.clone(),
                PullOptions::bulk(entry.phase, entry.raw),
                base_tags,
            )
        })
        .collect();
    let pull_bytes = futures_util::future::try_join_all(pull_futs).await?;
    for bytes in &pull_bytes {
        pplx_space_metrics::template::pull_s3_bytes(base_tags, Some(&parts.region), *bytes);
    }
    let manifest_wire_bytes: u64 = pull_bytes.iter().sum();
    info!(
        template = name,
        content_hash,
        rootfs_layout = if parsed_manifest.has_compressed_rootfs() {
            "compressed"
        } else {
            "legacy"
        },
        manifest_wire_bytes,
        "vol-mgr pulls completed; staging on disk"
    );

    // 5. Decompress every zstd entry in the manifest whose uncompressed
    //    sibling is also listed — that's the dedup rule's other half.
    //    Runs in parallel across entries via `spawn_blocking +
    //    join_all`. Each decoder rehashes its decoded bytes and refuses
    //    to publish them unless they match the manifest's expanded
    //    digest; the bootable rootfs first checks that digest equals the
    //    resolved release hash.
    let rootfs_filename = if parsed_manifest.bootable_artifact.is_empty() {
        TEMPLATE_ROOTFS_FILE
    } else {
        parsed_manifest.bootable_artifact.as_str()
    };
    let rootfs_outcome = materialize_rootfs(
        &parsed_manifest,
        &digest_dir,
        rootfs_filename,
        base_tags,
        name,
        content_hash,
        backing_policy,
    )
    .await?;

    // 6. Verify the digest against the resolved release hash.
    if rootfs_outcome.rootfs_hash != content_hash {
        warn!(
            template = name,
            expected = content_hash,
            actual = %rootfs_outcome.rootfs_hash,
            rootfs_size = rootfs_outcome.rootfs_size,
            "rootfs hash mismatch; cluster metadata is inconsistent with object bytes"
        );
        // `decompress_rootfs` has already atomically renamed the (verified-
        // hash-wrong) bytes onto rootfs.ext4. Without cleanup, the next
        // GetTemplate for this `(name, content_hash)` would see all four
        // artifacts present, treat the dir as an orphaned-but-valid build,
        // and silently upsert a `state=ready` DB row pointing at
        // unverified bytes. Wipe the dir so recovery declines and a fresh
        // pull is forced.
        cleanup_digest_dir_after_hash_mismatch(name, content_hash, Path::new(&digest_dir)).await;
        return Err(PullError::HashMismatch {
            expected: content_hash.to_string(),
            actual: rootfs_outcome.rootfs_hash,
        });
    }

    let rootfs_size = rootfs_outcome.rootfs_size;
    let artifact_size = rootfs_size;
    let manifest = crate::service::normalize_template_manifest(
        parsed_manifest,
        name,
        content_hash,
        &metadata,
        rootfs_size,
        rootfs_outcome
            .compressed
            .as_ref()
            .map(|(hash, size)| (hash.as_str(), *size)),
    )
    .map_err(|error| {
        warn!(
            template = name,
            content_hash,
            %error,
            "template execution metadata validation failed"
        );
        PullError::Failure(PullFailure::ExecutionMetadataInvalid)
    })?;
    let manifest_json = serde_json::to_string_pretty(&manifest).unwrap_or_default();
    // Atomic write: stream to a process-unique sibling tmp file and rename
    // onto manifest.json. Two concurrent decompresses produce byte-identical
    // bytes here, and `rename` is atomic, so either writer wins safely.
    if let Err(err) = write_file_atomic(Path::new(&manifest_path), manifest_json.as_bytes()).await {
        warn!(
            error = %err,
            template = name,
            content_hash,
            path = %manifest_path,
            "atomic write of normalized manifest failed; falling through"
        );
        return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
    }
    let build_date = manifest.build_date.clone();
    let runtime = if manifest.runtime.is_empty() {
        pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.to_string()
    } else {
        manifest.runtime.clone()
    };
    let arch = arch_from(&metadata, &manifest);

    if let Err(err) = normalize_template_cache_tree(Path::new(&digest_dir)) {
        warn!(
            error = %err,
            template = name,
            content_hash,
            dir = %digest_dir,
            "normalize digest dir permissions failed; falling through"
        );
        return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
    }

    // Stamp received_uuid + toggle the subvolume to read-only BEFORE
    // committing `state = "ready"`. The upsert below is what makes the
    // build externally observable as ready — to substrate's
    // cached-ready filter (via `node_state_sync::mark_template_dirty`),
    // to in-process GetTemplate queries that read the sqlite row, and
    // to any out-of-process inspection. If we ran this AFTER the upsert,
    // a consumer racing the post-step would see a "ready" template whose
    // received_uuid isn't stamped and whose subvolume isn't yet RO — the
    // btrfs-send codec would reject the build as a portable send parent
    // (it verifies `received_uuid` matches the deterministic UUIDv5),
    // and `btrfs send -p` against a writable parent is rejected by the
    // kernel anyway. The btrfs-send codec correctness invariant
    // ("state=ready ⇒ usable as a portable send parent") only holds
    // when the on-disk subvolume is fully baked before the row commits.
    //
    // Always attempt finalize; nodemgr re-checks whether the digest
    // dir is a subvolume on its own side, so the privileged answer is
    // authoritative regardless of what an unprivileged local probe
    // would say.
    //
    // FailedPrecondition from nodemgr (`needs subvolume`) is a
    // benign skip — the digest dir was installed as a plain dir
    // (non-btrfs host, or a future create_subvolume fallback path);
    // those templates remain usable via the legacy create-and-
    // reflink + Full-codec paths, just without btrfs-send
    // portability. Any other status surfaces as a warn — the
    // template lands as state=ready regardless (the codec is
    // best-effort).
    match crate::btrfs_via_nodemgr::finalize_template_subvolume(name, content_hash).await {
        Ok(received_uuid) => tracing::debug!(
            template = name,
            content_hash,
            %received_uuid,
            "on-demand pull: stamped deterministic received_uuid + set RO (via nodemgr)"
        ),
        Err(crate::btrfs_via_nodemgr::BtrfsViaNodemgrError::Rpc { status, .. })
            if status.code() == tonic::Code::FailedPrecondition =>
        {
            tracing::debug!(
                template = name,
                content_hash,
                reason = %status.message(),
                "on-demand pull: digest dir not a subvolume; skipping finalize \
                 (template will use Full codec only)"
            );
        }
        Err(err) => {
            tracing::warn!(
                template = name,
                content_hash,
                error = %err,
                "on-demand pull: nodemgr finalize-template-subvolume failed inline; \
                 enqueuing durable retry — build commits as state='ready' for \
                 the Full codec immediately; btrfs-send codec stays unavailable \
                 until the durable retry succeeds"
            );
            // Enqueue a durable stamp-retry job. The job runs the
            // finalize call against nodemgr with exponential backoff;
            // `FailedPrecondition` from a future attempt resolves as
            // permanent (digest dir is genuinely not a subvolume),
            // anything else stays retriable. Idempotent: dedup_key is
            // `{name}@{content_hash}`, so concurrent pulls of the
            // same build coalesce to one in-flight job. Best-effort
            // submission — if the worker isn't wired (tests) or
            // submit fails, we still commit state='ready' so the Full
            // codec is unblocked.
            if let Some(worker) = background_worker {
                let payload = crate::background_jobs::StampPayload {
                    name: name.to_string(),
                    content_hash: content_hash.to_string(),
                };
                let subject = format!("template:{name}@{content_hash}");
                let dedup = payload.dedup_key();
                match worker.submit_json(
                    crate::background_jobs::STAMP_JOB_KIND,
                    &payload,
                    Some(&subject),
                    Some(&dedup),
                ) {
                    Ok(job_id) => tracing::info!(
                        template = name,
                        content_hash,
                        job_id = %job_id,
                        "enqueued durable template_stamp retry"
                    ),
                    Err(submit_err) => tracing::warn!(
                        template = name,
                        content_hash,
                        error = %submit_err,
                        "failed to enqueue durable template_stamp retry"
                    ),
                }
            }
        }
    }

    // Now commit the build row. The artifacts on disk are in their
    // final shape — including RO + stamped received_uuid above — so
    // this is the only remaining commit step.
    let n = name.to_string();
    let h = content_hash.to_string();
    let db_for_mark = Arc::clone(&db);
    let phase_started = Instant::now();
    let upsert = tokio::task::spawn_blocking(move || {
        let conn = db.lock().map_err(|_| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                Some("template DB mutex poisoned".to_string()),
            )
        })?;
        crate::db::upsert_build(
            &conn,
            &crate::db::BuildUpsert {
                name: &n,
                content_hash: &h,
                build_date: &build_date,
                manifest_json: &manifest_json,
                runtime: &runtime,
                arch: &arch,
                artifact_size_bytes: artifact_size,
                state: "ready",
            },
        )
    })
    .await;

    match upsert {
        Ok(Ok(())) => {
            record_phase(base_tags, "db_upsert", "ok", phase_started);
            info!(
                template = name,
                content_hash,
                bytes = artifact_size,
                manifest_wire_bytes,
                "materialized template build from object-store volume"
            );
            crate::node_state_sync::mark_template_dirty(&db_for_mark, name);
            cleanup_guard.disarm();
            Ok(())
        }
        Ok(Err(err)) => {
            record_phase(base_tags, "db_upsert", "error", phase_started);
            warn!(
                error = %err,
                template = name,
                content_hash,
                "DB upsert_build failed after volume pull"
            );
            Err(PullError::Failure(PullFailure::LocalDatabaseUnavailable))
        }
        Err(err) => {
            record_phase(base_tags, "db_upsert", "error", phase_started);
            warn!(
                error = %err,
                template = name,
                content_hash,
                "spawn_blocking for DB upsert_build panicked"
            );
            Err(PullError::Failure(PullFailure::LocalDatabaseUnavailable))
        }
    }
}

/// Re-register a digest dir whose artifacts are already on disk but whose
/// DB row is missing — the typical residue of a cancelled GetTemplate whose
/// caller dropped after the local commit work finished but before the upsert
/// landed.
///
/// All four artifacts (`metadata.json`, `manifest.json`, `rootfs.ext4.zst`,
/// `rootfs.ext4`) must be present; an absent set falls through to the regular
/// pull path, which re-uses any already-downloaded files via vol-mgr's
/// skip-if-present check and decompresses as needed.
///
/// We trust the `rootfs.ext4` bytes without re-hashing because of two
/// invariants:
///
/// 1. **Verify-before-rename in [`crate::rootfs_zstd::decompress_rootfs`].**
///    The decompressor streams into a sibling tmp file and only renames
///    onto `rootfs.ext4` once it has verified the full digest over the
///    decoded bytes; the caller (`try_pull_from`) compares that hash against
///    the resolved release hash and rejects on mismatch. A file at
///    `rootfs.ext4` therefore reflects bytes that passed verification.
/// 2. **Content-addressed dir naming.** `digest_dir` ends in `<content_hash>`,
///    so a stray file from a different build can't end up here under normal
///    operation — there is no path by which a wrong-hash rootfs lands at
///    this exact spot.
///
/// **If either invariant is ever broken** (e.g. someone removes the verify
/// step in `decompress_rootfs`, or an operator manually drops a file into a
/// digest dir), recovery will silently re-register the wrong bytes. The
/// defense-in-depth fix would be a digest check of `rootfs.ext4` on every
/// recovery, but that's a multi-second cost on a 1 GB rootfs for a cold
/// path. We accept the trust on the basis that the invariants are local to
/// this crate.
async fn try_recover_from_disk(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    content_hash: &str,
) -> bool {
    let digest_dir = format!("{TEMPLATE_DIR}/{name}/{content_hash}");
    match recover_from_digest_dir(db, name, content_hash, Path::new(&digest_dir)).await {
        Ok(rootfs_size) => {
            info!(
                template = name,
                content_hash,
                dir = %digest_dir,
                rootfs_size,
                "recovered template build from disk; DB row re-registered"
            );
            crate::node_state_sync::mark_template_dirty(db, name);
            true
        }
        Err(reason) => {
            // Expected outcomes (missing file, parse fail) log inside the
            // helper; nothing more to say here.
            tracing::debug!(
                template = name,
                content_hash,
                dir = %digest_dir,
                reason,
                "disk recovery did not apply"
            );
            false
        }
    }
}

/// Side-effect-isolated core of [`try_recover_from_disk`]: reads the digest
/// dir, normalizes the manifest, atomically rewrites it, and upserts the DB
/// row. Returns the rootfs size on success (for logging by the wrapper), or
/// a short reason string when the digest dir doesn't qualify for recovery.
///
/// Split out from `try_recover_from_disk` so tests can drive it with a
/// `tempfile::tempdir()` and an `Connection::open_in_memory()` DB —
/// proving the "if files are on disk but DB row is missing, the next
/// caller commits the DB row" half of the cancellation-safety contract.
/// External side effects (the `mark_template_dirty` enqueue) stay in the
/// wrapper so this function is fully self-contained.
async fn recover_from_digest_dir(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    content_hash: &str,
    digest_dir: &Path,
) -> Result<u64, &'static str> {
    let metadata_path = digest_dir.join("metadata.json");
    let manifest_path = digest_dir.join("manifest.json");

    // Read metadata + manifest first; recovery is manifest-driven, so we
    // can't decide what other files to check for until we know what the
    // manifest declares.
    let metadata_bytes = tokio::fs::read(&metadata_path)
        .await
        .map_err(|_| "metadata_missing")?;
    let manifest_bytes = tokio::fs::read(&manifest_path)
        .await
        .map_err(|_| "manifest_missing")?;

    let manifest: pplx_space_common_core::types::TemplateManifest =
        match serde_json::from_slice(&manifest_bytes) {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    error = %err,
                    template = name,
                    content_hash,
                    path = %manifest_path.display(),
                    "manifest parse failed during recovery; falling through to fresh pull"
                );
                return Err("manifest_parse_failed");
            }
        };
    // Same path-injection defense as the pull path. The on-disk
    // manifest is whatever the last writer left (potentially the raw
    // S3 object from an interrupted pull), so we cannot assume it's
    // already been validated.
    if let Err(err) = manifest.validate_filenames() {
        warn!(
            template = name,
            content_hash,
            error = %err,
            "manifest filename validation rejected the on-disk manifest during recovery"
        );
        return Err("manifest_unsafe_filename");
    }

    // The bootable artifact is always required (it's what the VM boots
    // off, regardless of what the manifest's `files` map happens to
    // contain). Fall back to the canonical `rootfs.ext4` filename when
    // the manifest doesn't declare a bootable artifact — common for
    // the raw, pre-normalized manifests vol-mgr writes mid-pull.
    let rootfs_filename = if manifest.bootable_artifact.is_empty() {
        TEMPLATE_ROOTFS_FILE
    } else {
        manifest.bootable_artifact.as_str()
    };
    let rootfs_path = digest_dir.join(rootfs_filename);
    let rootfs_meta = tokio::fs::metadata(&rootfs_path)
        .await
        .map_err(|_| "rootfs_missing")?;
    if !rootfs_meta.is_file() {
        return Err("rootfs_not_a_file");
    }

    // Every other file the manifest declares must also be on disk. Vol-
    // mgr's per-file atomic rename means a present file is whole, so
    // existence is a sufficient liveness check.
    //
    // Manifest-driven: a legacy template that only lists `rootfs.ext4`
    // recovers without a `.zst` on disk; a new-format template that
    // lists both still requires both. Skips the bootable artifact since
    // it's already validated above.
    for key in manifest.files.keys() {
        if key == rootfs_filename {
            continue;
        }
        let path = digest_dir.join(key);
        let meta = tokio::fs::metadata(&path).await.map_err(|_| {
            // `&'static str` reason; concrete missing key goes to the
            // tracing event in the wrapper, not the return value.
            "manifest_file_missing"
        })?;
        if !meta.is_file() {
            return Err("manifest_file_not_a_file");
        }
    }
    let metadata: pplx_space_common_core::types::TemplateMetadata =
        match serde_json::from_slice(&metadata_bytes) {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    error = %err,
                    template = name,
                    content_hash,
                    path = %metadata_path.display(),
                    "metadata parse failed during recovery; falling through to fresh pull"
                );
                return Err("metadata_parse_failed");
            }
        };
    let rootfs_size = rootfs_meta.len();

    // Re-normalize the manifest before commit: the on-disk manifest may be
    // the raw object vol-mgr wrote, not yet patched by
    // `normalize_template_manifest`, if a previous attempt was cancelled
    // between rootfs.ext4 rename and the atomic manifest re-write. Without
    // this, recovery would store an unnormalized manifest with `state=ready`
    // and downstream readers would see a missing `content_hash`,
    // `bootable_artifact`, and `files` entries.
    //
    // We pass `None` for the compressed_rootfs entry: we don't have
    // decompress stats here and re-hashing the .zst on every cold recovery
    // would dominate the cost. Downstream tolerates the missing files entry
    // for rootfs.ext4.zst on recovered builds.
    let normalized = crate::service::normalize_template_manifest(
        manifest,
        name,
        content_hash,
        &metadata,
        rootfs_size,
        None,
    )
    .map_err(|error| {
        warn!(
            template = name,
            content_hash,
            %error,
            "recovered template execution metadata validation failed"
        );
        "execution_metadata_invalid"
    })?;
    let manifest_json = serde_json::to_string_pretty(&normalized).unwrap_or_default();

    // Rewrite the on-disk manifest atomically so subsequent recoveries (or
    // any other reader of the digest dir) see the normalized form too.
    // Failure here is non-fatal — we still have the in-memory normalized
    // copy for the DB upsert.
    if let Err(err) = write_file_atomic(&manifest_path, manifest_json.as_bytes()).await {
        warn!(
            error = %err,
            template = name,
            content_hash,
            path = %manifest_path.display(),
            "atomic rewrite of recovered manifest failed; proceeding with DB upsert anyway"
        );
    }

    let build_date = normalized.build_date.clone();
    let runtime = if normalized.runtime.is_empty() {
        pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.to_string()
    } else {
        normalized.runtime.clone()
    };
    let arch = arch_from(&metadata, &normalized);

    let n = name.to_string();
    let h = content_hash.to_string();
    let db_for_upsert = Arc::clone(db);
    let upsert = tokio::task::spawn_blocking(move || {
        let conn = db_for_upsert.lock().map_err(|_| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                Some("template DB mutex poisoned".to_string()),
            )
        })?;
        crate::db::upsert_build(
            &conn,
            &crate::db::BuildUpsert {
                name: &n,
                content_hash: &h,
                build_date: &build_date,
                manifest_json: &manifest_json,
                runtime: &runtime,
                arch: &arch,
                artifact_size_bytes: rootfs_size,
                state: "ready",
            },
        )
    })
    .await;

    match upsert {
        Ok(Ok(())) => Ok(rootfs_size),
        Ok(Err(err)) => {
            warn!(
                error = %err,
                template = name,
                content_hash,
                "DB upsert_build failed during recovery; falling through to fresh pull"
            );
            Err("db_upsert_failed")
        }
        Err(err) => {
            warn!(
                error = %err,
                template = name,
                content_hash,
                "spawn_blocking for DB upsert_build panicked during recovery"
            );
            Err("db_upsert_panicked")
        }
    }
}

/// Local sync fallback for the privileged delete RPC. Production
/// callers go through nodemgr's `BtrfsDeleteTemplateSubvolume`
/// because tmplmgr no longer has `CAP_SYS_ADMIN` to delete RO
/// subvolumes locally. This helper is the fallback used when the RPC
/// errors transiently: on a plain-dir digest it wipes the artifact
/// files, which is enough to disarm recovery
/// (`recover_from_digest_dir` requires the manifest-declared files to
/// be present, so removing `rootfs.ext4` alone is enough) even when
/// the empty subvolume root persists.
///
/// Subvolume-aware order:
///  1. If RO (a prior install ran far enough to flip it), clear the
///     flag first — `btrfs subvolume delete` rejects RO targets.
///  2. Try `delete_subvolume_if_present`. On non-btrfs hosts / plain-dir
///     installs this returns an error, which we tolerate and fall
///     through to `remove_dir_all` for the directory case.
///  3. Plain `remove_dir_all` as the fallback path.
pub(crate) fn cleanup_digest_dir_sync(digest_dir: &Path) -> std::io::Result<()> {
    if !digest_dir.exists() {
        return Ok(());
    }
    if pplx_space_common_core::host::btrfs::is_subvolume(digest_dir) {
        // The RO clear is what makes both delete attempts below
        // possible on an installed (read-only) build; a refusal here
        // means the `remove_dir_all` fallthrough will EROFS, so record
        // it — otherwise that EROFS is indistinguishable from the
        // pre-subvolume-aware bug this path fixed.
        if let Err(err) = pplx_space_common_core::host::btrfs::set_readonly(digest_dir, false) {
            warn!(
                dir = %digest_dir.display(),
                %err,
                "clearing subvolume read-only flag failed ahead of local delete"
            );
        }
        if pplx_space_common_core::host::btrfs::delete_subvolume_if_present(digest_dir).is_ok() {
            return Ok(());
        }
    }
    std::fs::remove_dir_all(digest_dir)
}

/// Run [`cleanup_digest_dir_sync`] on the blocking pool so the async
/// caller's runtime isn't blocked by `btrfs subvolume show` /
/// `remove_dir_all` syscalls. Used as the fallback when the
/// privileged `delete_template_subvolume` RPC against nodemgr fails.
async fn cleanup_digest_dir_local_fallback(digest_dir: PathBuf) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || cleanup_digest_dir_sync(&digest_dir))
        .await
        .map_err(|join_err| std::io::Error::other(format!("spawn_blocking join: {join_err}")))?
}

async fn cleanup_digest_dir_after_hash_mismatch(name: &str, content_hash: &str, digest_dir: &Path) {
    match crate::btrfs_via_nodemgr::delete_template_subvolume(name, content_hash).await {
        Ok(()) => {
            info!(
                dir = %digest_dir.display(),
                reason = "hash_mismatch",
                "removed digest dir to prevent recovery from adopting bad bytes (via nodemgr)"
            );
            return;
        }
        Err(err) => warn!(
            error = %err,
            dir = %digest_dir.display(),
            "nodemgr delete_template_subvolume failed after hash mismatch; trying local fallback"
        ),
    }
    // Local fallback: even if the privileged subvolume-delete path
    // EPERMs (production tmplmgr is non-root), the recursive
    // file-removal still wipes the artifact files so the next call's
    // recovery cannot mistake unverified bytes for a ready build.
    if let Err(err) = cleanup_digest_dir_local_fallback(digest_dir.to_path_buf()).await {
        warn!(
            error = %err,
            dir = %digest_dir.display(),
            "local fallback cleanup also failed after hash mismatch; recovery may adopt bad bytes on the next call"
        );
    } else {
        info!(
            dir = %digest_dir.display(),
            reason = "hash_mismatch",
            "removed digest dir via local fallback after nodemgr RPC failed"
        );
    }
}

/// Best-effort RAII cleanup for an in-flight `pull_inner` digest dir.
///
/// On entry, `pull_inner` creates the digest dir (as a btrfs subvolume
/// when possible) and proceeds through ~14 fallible steps — vol-mgr
/// connect, metadata + manifest fetch, hash verify, manifest rewrite,
/// permission normalization, …, sqlite upsert. Without a guard, any
/// `return Err` between subvolume create and the final upsert leaks an
/// empty subvolume that the next retry adopts via `digest_dir_existed`,
/// permanently blocking a clean install of that `(name, content_hash)`.
///
/// The guard is armed at digest-dir creation and explicitly
/// [`Self::disarm`]ed right before `pull_inner` returns `Ok(())`. On any
/// other exit path, Drop spawns an async `BtrfsDeleteTemplateSubvolume`
/// RPC against node-manager — fire-and-forget on the calling tokio
/// runtime. Drop itself is sync (Rust requirement) so it can't await;
/// the spawn means the cleanup runs eventually unless the runtime is
/// already shutting down, in which case the next pull's recovery
/// catches up via `cleanup_digest_dir_after_hash_mismatch`.
struct PullCleanupGuard {
    name: String,
    content_hash: String,
    digest_dir: Option<PathBuf>,
}

impl PullCleanupGuard {
    fn new(name: &str, content_hash: &str, digest_dir: impl Into<PathBuf>) -> Self {
        Self {
            name: name.to_string(),
            content_hash: content_hash.to_string(),
            digest_dir: Some(digest_dir.into()),
        }
    }

    /// Mark the install successful. Drop becomes a no-op.
    fn disarm(mut self) {
        self.digest_dir = None;
    }
}

impl Drop for PullCleanupGuard {
    fn drop(&mut self) {
        let Some(dir) = self.digest_dir.take() else {
            return;
        };
        let name = std::mem::take(&mut self.name);
        let content_hash = std::mem::take(&mut self.content_hash);

        // Cleanup goes through node-manager (the privileged delete
        // path). Drop must be sync, so we spawn a fire-and-forget
        // task on the current tokio runtime. If no runtime is
        // available (Drop fired outside an async context — shouldn't
        // happen during pull_inner, but be defensive), the next
        // pull's hash-mismatch / recovery cleanup catches up.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!(
                dir = %dir.display(),
                template = %name,
                content_hash = %content_hash,
                "PullCleanupGuard fired outside a tokio runtime; partial \
                 digest dir may remain until the next recovery pass picks it up"
            );
            return;
        };
        handle.spawn(async move {
            match crate::btrfs_via_nodemgr::delete_template_subvolume(&name, &content_hash).await {
                Ok(()) => {
                    info!(
                        dir = %dir.display(),
                        reason = "pull_failed",
                        "removed digest dir on pull failure (via nodemgr)"
                    );
                    return;
                }
                Err(err) => warn!(
                    error = %err,
                    dir = %dir.display(),
                    "nodemgr delete_template_subvolume failed on pull failure; trying local fallback"
                ),
            }
            // Same fallback as `cleanup_digest_dir_after_hash_mismatch`:
            // wipe the artifact files locally so the next pull's
            // recovery doesn't adopt partial bytes when the nodemgr
            // RPC is transiently unavailable.
            if let Err(err) = cleanup_digest_dir_local_fallback(dir.clone()).await {
                warn!(
                    error = %err,
                    dir = %dir.display(),
                    "local fallback cleanup also failed on pull failure; \
                     recovery may adopt partial bytes on the next call"
                );
            } else {
                info!(
                    dir = %dir.display(),
                    reason = "pull_failed",
                    "removed digest dir via local fallback after nodemgr RPC failed"
                );
            }
        });
    }
}

/// Stream `contents` to a process-unique sibling tmp file and atomically
/// rename onto `path`. Two concurrent writers — possible when a cancelled
/// future's `spawn_blocking` is still running and a fresh caller arrives —
/// write to distinct tmp files; whichever rename lands last defines the
/// final bytes. Used for the normalized `manifest.json` write, where both
/// writers produce identical content so either is correct.
async fn write_file_atomic(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("write");
    let tmp = path.with_file_name(format!("{file_name}.{pid}-{n}.tmp"));
    tokio::fs::write(&tmp, contents).await?;
    tokio::fs::rename(&tmp, path).await
}

/// Percent-encode the characters that would break our query parser if they
/// appeared in a query value: `&` (splits pairs), `?` and `#` (URL-part
/// boundaries), `%` (to avoid double-decoding by any downstream consumer),
/// and whitespace. Everything else passes through, including the `:` and
/// `/` that legitimately appear in endpoint URLs.
///
/// `StorageUri::parse` itself doesn't percent-decode (today), so we only
/// have to protect the in-band separators — not implement a full RFC 3986
/// encoder.
fn pct_encode_query_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' | '?' | '#' | '%' | ' ' | '\t' | '\n' | '\r' => {
                let mut buf = [0u8; 4];
                for &b in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// Build a fully self-describing `s3://...` URI for one artifact under the
/// release's storage prefix. Volume-manager owns S3 client construction and
/// credentials, so all it needs from us is this URI.
///
/// `storage.endpoint` and `storage.region` are percent-encoded for the
/// characters that would break the outer URI's query parser
/// (`StorageUri::parse` splits on `&` and `=`). A registrar that emits an
/// endpoint containing `&`, `?`, `#`, or whitespace therefore round-trips
/// safely rather than producing a `MalformedQuery` on the vol-mgr side.
fn build_object_uri(storage: &StorageUri, file_name: &str) -> String {
    let prefix = storage.key_prefix.trim_matches('/');
    let mut uri = if prefix.is_empty() {
        format!("s3://{}/{}", storage.bucket, file_name)
    } else {
        format!("s3://{}/{}/{}", storage.bucket, prefix, file_name)
    };
    uri.push_str(&format!(
        "?region={}",
        pct_encode_query_value(&storage.region)
    ));
    if let Some(ep) = storage.endpoint.as_deref() {
        uri.push_str(&format!("&endpoint={}", pct_encode_query_value(ep)));
    }
    if storage.path_style {
        uri.push_str("&path_style=true");
    }
    uri
}

#[derive(Clone, Copy)]
struct PullOptions {
    phase: &'static str,
    raw: bool,
    priority: i32,
}

impl PullOptions {
    const fn control(phase: &'static str) -> Self {
        Self {
            phase,
            raw: false,
            priority: RESTORE_PRIORITY,
        }
    }

    const fn bulk(phase: &'static str, raw: bool) -> Self {
        Self {
            phase,
            raw,
            priority: 0,
        }
    }
}

fn build_pull_file_request(
    subject_id: String,
    s3_uri: String,
    local_path: String,
    options: PullOptions,
) -> PullFileRequest {
    PullFileRequest {
        subject_id,
        s3_uri,
        local_path,
        raw: options.raw,
        file_mode: Some(TEMPLATE_CACHE_FILE_MODE),
        existing_file_policy: ExistingFilePolicy::LegacySkipPresent as i32,
        priority: options.priority,
        snapshot_artifact: SnapshotArtifact::Unspecified as i32,
        transfer_scope_id: None,
    }
}

/// Issue one synchronous `PullFile` RPC against volume-manager.
///
/// Volume-manager streams the object to `<local_path>.partial` and renames to
/// `local_path` on success, so on `Ok` the caller can read `local_path` and
/// trust it is whole. Volume-manager surfaces retriable job failures as
/// `Unavailable` and permanent failures as `FailedPrecondition`; this caller
/// tries every replica before returning the final status.
async fn pull_one(
    mut client: VolmgrClient,
    subject: &SubjectId,
    s3_uri: String,
    local_path: String,
    options: PullOptions,
    base_tags: &[String],
) -> Result<u64, PullError> {
    let phase = options.phase;
    let phase_started = Instant::now();
    let subject_id = subject.to_string();
    let resp = client
        .pull_file(build_pull_file_request(
            subject_id.clone(),
            s3_uri.clone(),
            local_path.clone(),
            options,
        ))
        .await;
    match resp {
        Ok(ok) => {
            record_phase(base_tags, phase, "ok", phase_started);
            let bytes = ok.into_inner().bytes_downloaded;
            tracing::debug!(
                phase,
                subject_id = %subject_id,
                s3_uri = %s3_uri,
                local_path = %local_path,
                bytes,
                "vol-mgr pull_file completed"
            );
            Ok(bytes)
        }
        Err(status) => match pull_status_disposition(&status) {
            PullStatusDisposition::Missing => {
                record_phase(base_tags, phase, "not_found", phase_started);
                warn!(
                    phase,
                    subject_id = %subject_id,
                    s3_uri = %s3_uri,
                    local_path = %local_path,
                    "vol-mgr pull_file returned NotFound"
                );
                Err(PullError::Failure(PullFailure::StorageObjectMissing))
            }
            PullStatusDisposition::Retryable => {
                record_phase(base_tags, phase, "retryable", phase_started);
                info!(
                    error = %status,
                    phase,
                    subject_id = %subject_id,
                    s3_uri = %s3_uri,
                    local_path = %local_path,
                    "vol-mgr rejected pull; propagating retryable status"
                );
                Err(PullError::Retryable(status))
            }
            PullStatusDisposition::Unreachable => {
                record_phase(base_tags, phase, "error", phase_started);
                warn!(
                    error = %status,
                    phase,
                    subject_id = %subject_id,
                    s3_uri = %s3_uri,
                    local_path = %local_path,
                    "vol-mgr pull_file failed; falling through"
                );
                Err(PullError::Failure(PullFailure::StorageUnavailable))
            }
            PullStatusDisposition::Failed => {
                record_phase(base_tags, phase, "failed", phase_started);
                warn!(
                    error = %status,
                    phase,
                    subject_id = %subject_id,
                    s3_uri = %s3_uri,
                    local_path = %local_path,
                    "vol-mgr rejected pull with a non-retryable status"
                );
                Err(PullError::Failed(status))
            }
        },
    }
}

/// Build the parallel-pull plan from `manifest.files`. Applies one
/// dedup rule: when the manifest lists both `X` and `X.zst` (with a
/// well-formed zstd descriptor on the latter), drop `X` — only `X.zst`
/// is pulled and decompression by [`materialize_rootfs`] regenerates
/// `X` on disk. This avoids paying for the same bytes twice and keeps
/// the manifest as the single source of truth about which files
/// actually exist in S3.
///
/// Single-file manifests (legacy templates that only ship
/// `rootfs.ext4`) come through unchanged, yielding a plan with exactly
/// one entry. Manifests that only declare a `.zst` (no uncompressed
/// sibling) also come through unchanged; we decompress to produce the
/// uncompressed file regardless.
fn build_manifest_pull_plan(
    manifest: &pplx_space_common_core::types::TemplateManifest,
    storage: &StorageUri,
    digest_dir: &str,
) -> Vec<ManifestPullEntry> {
    let mut plan = Vec::with_capacity(manifest.files.len());
    for (key, file) in &manifest.files {
        // Skip an uncompressed entry when its `.zst` sibling is listed
        // with a usable zstd descriptor. Decompression regenerates it
        // after the parallel pull settles.
        let zst_sibling_key = format!("{key}{ZSTD_FILENAME_SUFFIX}");
        if manifest
            .files
            .get(&zst_sibling_key)
            .is_some_and(|sibling| sibling.is_zstd_compressed())
        {
            continue;
        }

        let raw = file.is_zstd_compressed();
        // Phase tag dispatches on bootable artifact + compression so the
        // existing `rootfs_zstd` dashboards stay stable: the rootfs's
        // `.zst` keeps the `rootfs_zstd` label, the uncompressed rootfs
        // gets `rootfs_legacy`, and any future non-rootfs files fall
        // through to a generic `manifest_file` bucket (none today).
        let phase = if key == &manifest.bootable_artifact {
            "rootfs_legacy"
        } else if key == &format!("{}{ZSTD_FILENAME_SUFFIX}", manifest.bootable_artifact) && raw {
            "rootfs_zstd"
        } else {
            "manifest_file"
        };
        plan.push(ManifestPullEntry {
            manifest_key: key.clone(),
            s3_uri: build_object_uri(storage, key),
            local_path: format!("{digest_dir}/{key}"),
            phase,
            raw,
        });
    }
    plan
}

/// Materialize the bootable rootfs after the manifest pull plan
/// settled. Two paths:
///
/// - **Compressed-pair manifest** — `files` lists both the
///   uncompressed rootfs and its `.zst` sibling. The pull plan
///   downloaded only the `.zst`; here we decompress. The compressed
///   digest is verified against the manifest, and the expanded digest
///   is recomputed from the decoded bytes and verified the same way.
///   For the bootable rootfs the manifest digest must first equal the
///   resolved release hash, so the decoded bytes are bound to the
///   release rather than to whatever the artifact prefix declares.
/// - **Uncompressed-only manifest** — `files` lists just the rootfs.
///   The pull plan downloaded it directly; here we stream-hash it for
///   SHA verification. Equally valid under the manifest-driven design
///   — see [`crate::rootfs_zstd::hash_rootfs_file`].
///
/// Either way the returned `rootfs_hash` is what the caller compares
/// against `content_hash` to decide HashMismatch.
/// One post-pull decompression: a zstd entry in the manifest whose
/// uncompressed sibling is also declared. The dedup rule in
/// [`build_manifest_pull_plan`] is the symmetric half — it skips the
/// uncompressed entry at download time on the promise that this step
/// will regenerate it from the `.zst`.
#[derive(Debug, Clone)]
struct DecompressTarget {
    /// Filename of the uncompressed sibling that decompression
    /// regenerates. Equals the compressed manifest key minus the
    /// `.zst` suffix.
    uncompressed_name: String,
    compressed_path: String,
    uncompressed_path: String,
    /// Expected algorithm-prefixed digest of the decompressed bytes per the manifest's
    /// `files[uncompressed_name].digest`. Compressed-pair
    /// materialization requires this digest and compares the rehashed
    /// expanded bytes against it.
    expected_uncompressed_digest: String,
    /// Expected SHA-256 of the on-disk `.zst` bytes per the manifest's
    /// `files[<compressed key>].digest`. The decompressor emits the
    /// compressed digest as a side effect; comparing it here catches
    /// drift between the manifest the publisher wrote and the
    /// compressed object the bucket served. Empty when the manifest
    /// didn't carry one.
    expected_compressed_digest: String,
}

fn build_decompress_targets(
    manifest: &pplx_space_common_core::types::TemplateManifest,
    digest_dir: &str,
) -> Vec<DecompressTarget> {
    let mut targets = Vec::new();
    for (key, entry) in &manifest.files {
        if !entry.is_zstd_compressed() {
            continue;
        }
        let Some(stem) = key.strip_suffix(ZSTD_FILENAME_SUFFIX) else {
            continue;
        };
        let Some(sibling) = manifest.files.get(stem) else {
            // No uncompressed sibling declared. The `.zst` is on disk
            // (downloaded by the pull plan), but nothing asks us to
            // produce a decompressed form.
            continue;
        };
        targets.push(DecompressTarget {
            uncompressed_name: stem.to_string(),
            compressed_path: format!("{digest_dir}/{key}"),
            uncompressed_path: format!("{digest_dir}/{stem}"),
            expected_uncompressed_digest: sibling.digest.clone(),
            expected_compressed_digest: entry.digest.clone(),
        });
    }
    targets
}

/// Fail fast on a manifest that cannot be verified: both digests of a
/// compressed pair must be present, and the bootable rootfs's declared
/// digest must be the resolved release hash. Passing proves only that
/// the manifest is well-formed; the decoded bytes are still rehashed
/// and compared after decompression.
fn check_manifest_digests_for_target(
    target: &DecompressTarget,
    rootfs_filename: &str,
    content_hash: &str,
) -> Result<(), PullError> {
    if target.expected_uncompressed_digest.is_empty() {
        return Err(PullError::Failure(PullFailure::ManifestDigestMissing));
    }
    if target.expected_compressed_digest.is_empty() {
        return Err(PullError::Failure(
            PullFailure::ManifestCompressedDigestMissing,
        ));
    }
    if target.uncompressed_name == rootfs_filename
        && target.expected_uncompressed_digest != content_hash
    {
        return Err(PullError::HashMismatch {
            expected: content_hash.to_string(),
            actual: target.expected_uncompressed_digest.clone(),
        });
    }
    Ok(())
}

#[tracing::instrument(skip_all, err)]
async fn materialize_rootfs(
    manifest: &pplx_space_common_core::types::TemplateManifest,
    digest_dir: &str,
    rootfs_filename: &str,
    base_tags: &[String],
    name: &str,
    content_hash: &str,
    backing_policy: RootfsBackingPolicy,
) -> Result<RootfsMaterialization, PullError> {
    let rootfs_path = format!("{digest_dir}/{rootfs_filename}");
    // Decompress every zstd-paired entry the manifest declares — not
    // just the bootable rootfs. Each runs on its own `spawn_blocking`
    // and they fan out via `try_join_all` so a manifest with several
    // compressed pairs decodes in parallel. Each decompression rehashes
    // the decoded bytes; the manifest's declared digests are checked
    // up front only so an unverifiable manifest fails before any
    // decoding starts.
    let targets = build_decompress_targets(manifest, digest_dir);
    let mut decompress_jobs = Vec::with_capacity(targets.len());
    for target in &targets {
        match check_manifest_digests_for_target(target, rootfs_filename, content_hash) {
            Ok(()) => {}
            Err(error) => {
                match &error {
                    PullError::HashMismatch { expected, actual } => {
                        warn!(
                            template = name,
                            file = %target.uncompressed_name,
                            expected,
                            actual,
                            "manifest rootfs digest disagrees with resolved release hash"
                        );
                    }
                    PullError::Failure(failure) => {
                        warn!(
                            template = name,
                            file = %target.uncompressed_name,
                            reason = failure.metric_reason(),
                            error = %failure,
                            "manifest compressed pair is missing required digest"
                        );
                    }
                    PullError::Retryable(status) => {
                        warn!(
                            template = name,
                            file = %target.uncompressed_name,
                            error = %status,
                            "rootfs materialization was rejected"
                        );
                    }
                    PullError::Failed(status) => {
                        warn!(
                            template = name,
                            file = %target.uncompressed_name,
                            error = %status,
                            "rootfs materialization failed"
                        );
                    }
                }
                cleanup_digest_dir_after_hash_mismatch(name, content_hash, Path::new(digest_dir))
                    .await;
                return Err(error);
            }
        }
        decompress_jobs.push((
            target.compressed_path.clone(),
            target.uncompressed_path.clone(),
            target.expected_uncompressed_digest.clone(),
            if target.uncompressed_name == rootfs_filename {
                backing_policy
            } else {
                RootfsBackingPolicy::Inherit
            },
        ));
    }
    let phase_started = Instant::now();
    let decompress_futs = decompress_jobs.into_iter().map(
        |(compressed, uncompressed, expected_digest, policy)| async move {
            let span = tracing::Span::current();
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    crate::rootfs_zstd::decompress_rootfs(
                        Path::new(&compressed),
                        Path::new(&uncompressed),
                        Some(&expected_digest),
                        policy,
                    )
                })
            })
            .await
        },
    );
    let decompress_results = futures_util::future::join_all(decompress_futs).await;
    let mut rootfs_outcome: Option<RootfsMaterialization> = None;
    for (target, joined) in targets.iter().zip(decompress_results) {
        let stats = match joined {
            Ok(Ok(stats)) => stats,
            Ok(Err(crate::rootfs_zstd::RootfsZstdError::ExpandedDigestMismatch {
                expected,
                actual,
            })) => {
                // The decoder left nothing at the output path; wipe the
                // rest of the digest dir so recovery cannot adopt the
                // other pulled artifacts around the missing rootfs.
                let phase = decompress_phase(target, rootfs_filename);
                record_phase(base_tags, phase, "expanded_digest_mismatch", phase_started);
                warn!(
                    template = name,
                    file = %target.uncompressed_name,
                    expected = %expected,
                    actual = %actual,
                    "decoded bytes disagree with the manifest's expanded digest"
                );
                cleanup_digest_dir_after_hash_mismatch(name, content_hash, Path::new(digest_dir))
                    .await;
                return Err(PullError::HashMismatch { expected, actual });
            }
            Ok(Err(error)) => {
                let phase = decompress_phase(target, rootfs_filename);
                record_phase(base_tags, phase, error.reason(), phase_started);
                warn!(
                    error = %error,
                    template = name,
                    content_hash,
                    source = %target.compressed_path,
                    target = %target.uncompressed_path,
                    "decompress manifest entry failed; falling through"
                );
                return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
            }
            Err(error) => {
                let phase = decompress_phase(target, rootfs_filename);
                record_phase(base_tags, phase, "error", phase_started);
                warn!(
                    error = %error,
                    template = name,
                    content_hash,
                    source = %target.compressed_path,
                    "decompress manifest entry task failed; falling through"
                );
                return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
            }
        };

        // Defense-in-depth SHA verification: vol-mgr wrote the
        // compressed bytes via `.partial → rename` but doesn't itself
        // verify content, so a corrupt object at the bucket would
        // round-trip through decompression. Compare the compressed
        // bytes against the manifest. The expanded bytes were already
        // verified by the decoder before it published them: for the
        // rootfs target the manifest digest equals the release hash
        // (checked above), so that is what binds the bytes that boot to
        // the release.
        if stats.compressed_hash != target.expected_compressed_digest {
            warn!(
                template = name,
                file = %target.uncompressed_name,
                expected = %target.expected_compressed_digest,
                actual = %stats.compressed_hash,
                "manifest .zst digest mismatch — compressed object disagrees with manifest"
            );
            cleanup_digest_dir_after_hash_mismatch(name, content_hash, Path::new(digest_dir)).await;
            return Err(PullError::HashMismatch {
                expected: target.expected_compressed_digest.clone(),
                actual: stats.compressed_hash,
            });
        }

        if target.uncompressed_name == rootfs_filename {
            record_phase(base_tags, "rootfs_zstd_decompress", "ok", phase_started);
            rootfs_outcome = Some(RootfsMaterialization {
                rootfs_size: stats.rootfs_size,
                rootfs_hash: stats.rootfs_hash,
                compressed: Some((stats.compressed_hash, stats.compressed_size)),
            });
        } else {
            record_phase(base_tags, "manifest_file_decompress", "ok", phase_started);
        }
    }
    if let Some(outcome) = rootfs_outcome {
        return Ok(outcome);
    }

    // Uncompressed-only manifest: `files` declared no `.zst` sibling
    // for the bootable rootfs, so the pull plan downloaded the rootfs
    // directly. Stream-hash it using the release identity.
    let algorithm = content_hash
        .parse::<pplx_space_common_core::types::Digest>()
        .map_err(|error| PullError::Failed(tonic::Status::invalid_argument(error.to_string())))?
        .algorithm();
    let hash_source = rootfs_path.to_string();
    let phase_started = Instant::now();
    let span = tracing::Span::current();
    let (rootfs_hash, rootfs_size) = match tokio::task::spawn_blocking(move || {
        span.in_scope(|| {
            tracing::info_span!("rootfs.raw_setup")
                .in_scope(|| backing_policy.prepare_raw(Path::new(&hash_source)))
                .map_err(|source| crate::rootfs_zstd::RootfsZstdError::Io {
                    operation: "set raw rootfs backing policy",
                    path: hash_source.clone(),
                    source,
                })?;
            crate::rootfs_zstd::hash_rootfs_file(Path::new(&hash_source), algorithm)
        })
    })
    .await
    {
        Ok(Ok(stats)) => {
            record_phase(base_tags, "rootfs_legacy_hash", "ok", phase_started);
            stats
        }
        Ok(Err(error)) => {
            record_phase(
                base_tags,
                "rootfs_legacy_hash",
                error.reason(),
                phase_started,
            );
            warn!(
                error = %error,
                template = name,
                content_hash,
                path = %rootfs_path,
                "hash legacy rootfs failed; falling through"
            );
            return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
        }
        Err(error) => {
            record_phase(base_tags, "rootfs_legacy_hash", "error", phase_started);
            warn!(
                error = %error,
                template = name,
                content_hash,
                "hash legacy rootfs task failed; falling through"
            );
            return Err(PullError::Failure(PullFailure::LocalStorageUnavailable));
        }
    };
    Ok(RootfsMaterialization {
        rootfs_size,
        rootfs_hash,
        compressed: None,
    })
}

/// Metric phase tag for one decompression: keep the rootfs's
/// pre-existing `rootfs_zstd_decompress` tag stable so dashboards
/// don't break, and bucket every other manifest entry into
/// `manifest_file_decompress`. Today only the rootfs is paired, so the
/// latter is forward-compat.
fn decompress_phase(target: &DecompressTarget, rootfs_filename: &str) -> &'static str {
    if target.uncompressed_name == rootfs_filename {
        "rootfs_zstd_decompress"
    } else {
        "manifest_file_decompress"
    }
}

pub(crate) fn local_build_exists(
    db: &Arc<std::sync::Mutex<rusqlite::Connection>>,
    name: &str,
    content_hash: &str,
) -> bool {
    let Ok(conn) = db.lock() else {
        warn!(
            template = name,
            content_hash, "template DB mutex poisoned while checking local build"
        );
        return false;
    };
    matches!(crate::db::get_build(&conn, name, content_hash), Ok(Some(_)))
}

/// Resolution outcome for a single `(template_name, tag)` lookup used
/// by the tag-prefetch loop. Distinguishing `Unresolved` from
/// `Error` matters because the former is normal (a template that
/// doesn't define the tag) while the latter is operational noise.
pub(crate) enum TagResolveOutcome {
    Resolved {
        hash: String,
        /// Template-level visibility reported by the cluster alongside
        /// the release. The prefetch loop only warms `Generic`
        /// templates; see the visibility gate in `prefetch::tick`.
        visibility: TemplateVisibility,
    },
    Unresolved,
    Error,
}

/// Resolve `(template_name, tag) → artifact_digest` via
/// `ClusterApi.FindTemplateRelease`. Used by the tag-prefetch loop to
/// (a) record the pin in `template_pinned_tags` and (b) decide whether
/// a pull dispatch is needed. Errors and unresolved lookups are
/// non-fatal — prefetch is best-effort.
///
/// This intentionally bypasses the metric reporting on
/// [`find_release`]: the prefetch tick already emits its own
/// `template.prefetch.tag.candidates` counter with the outcome, and
/// double-counting under both `template.pull.find_release_ms` and the
/// prefetch metric would muddy on-demand find-release latency.
pub(crate) async fn resolve_tag(template_name: &str, tag: &str) -> TagResolveOutcome {
    match find_release_inner(template_name, "", tag, None).await {
        Ok(release) => TagResolveOutcome::Resolved {
            hash: release.hash,
            visibility: release.visibility,
        },
        Err(FindReleaseError::NotFound) => TagResolveOutcome::Unresolved,
        Err(
            FindReleaseError::Unavailable
            | FindReleaseError::InvalidResponse
            | FindReleaseError::Status(_),
        ) => TagResolveOutcome::Error,
    }
}

/// Minimal projection of one entry from
/// `ClusterApi.ListLatestTemplateReleases` for the prefetcher. Only
/// the fields needed to drive pulls + record release pins are
/// surfaced; storage locations are deliberately dropped because the
/// pull path re-resolves them via `FindTemplateReleaseByHash` so a
/// single coalesce point handles both the on-demand and prefetch
/// codepaths.
pub(crate) struct LatestRelease {
    pub(crate) release_id: String,
    pub(crate) content_hash: String,
    pub(crate) created_at: String,
}

pub(crate) enum LatestReleasesOutcome {
    /// May be empty: the template exists cluster-side but has no
    /// releases registered yet. The caller treats this as a normal
    /// resting state, not an error.
    ///
    /// `visibility` combines the response-level field with a fold
    /// across the per-entry fields (see [`combine_response_visibility`]).
    /// The response-level field is what keeps the signal alive for a
    /// template with zero remaining releases — an org-specific verdict
    /// must reach the prefetch loop even when nothing resolves anymore,
    /// or its stale pins would never drop. `Unspecified` when the
    /// cluster couldn't determine it (old server/schema).
    Resolved {
        visibility: TemplateVisibility,
        releases: Vec<LatestRelease>,
    },
    /// Cluster-side error (RPC failure, validation rejection, ...);
    /// counted as `resolve_error` and skipped without touching the
    /// pin table for this template.
    Error,
}

/// Fold two visibility reports for the *same* template into the most
/// restrictive one, for prefetch purposes: a confirmed `OrgSpecific`
/// beats everything (the loop must actively drop pins), `Unspecified`
/// beats `Generic` (not known to be generic → don't warm). Disagreeing
/// entries can only happen during cluster-side skew; resolving toward
/// the restrictive answer keeps the data-isolation property.
pub(crate) fn fold_visibility(a: TemplateVisibility, b: TemplateVisibility) -> TemplateVisibility {
    use TemplateVisibility::{Generic, OrgSpecific, Unspecified};
    match (a, b) {
        (OrgSpecific, _) | (_, OrgSpecific) => OrgSpecific,
        (Unspecified, _) | (_, Unspecified) => Unspecified,
        (Generic, Generic) => Generic,
    }
}

/// Combine the response-level visibility with the fold of the per-entry
/// fields into the template-level verdict the prefetch loop acts on.
/// With no entries the response-level field stands alone — that's what
/// lets a zero-release org-specific template still report `OrgSpecific`
/// so its stale pins drop. With entries, the two sources are folded
/// restrictively so a disagreement (cluster-side skew or bug) can never
/// soften a verdict.
pub(crate) fn combine_response_visibility(
    response_level: TemplateVisibility,
    entry_fold: Option<TemplateVisibility>,
) -> TemplateVisibility {
    match entry_fold {
        Some(entries) => fold_visibility(response_level, entries),
        None => response_level,
    }
}

/// Fetch the most recent `limit` releases for `(template_name,
/// DEFAULT_RUNTIME)` via `ClusterApi.ListLatestTemplateReleases`.
/// Same metric-bypass rationale as [`resolve_tag`]: the prefetch
/// tick emits its own per-template counter and we don't want this
/// path to skew on-demand find-release latency.
pub(crate) async fn list_latest_releases(template_name: &str, limit: u32) -> LatestReleasesOutcome {
    let mut client = match connect_cluster_api_from_env().await {
        Ok(client) => client,
        Err(error) => {
            warn!(error = %error, "cluster api connect failed");
            return LatestReleasesOutcome::Error;
        }
    };

    let resp = match client
        .list_latest_template_releases(ListLatestTemplateReleasesRequest {
            template_name: template_name.to_string(),
            runtime: DEFAULT_RUNTIME.to_string(),
            limit,
        })
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => {
            warn!(
                error = %status,
                template = template_name,
                limit,
                "ListLatestTemplateReleases failed",
            );
            return LatestReleasesOutcome::Error;
        }
    };

    let response_visibility = resp.template_visibility();
    let mut releases = Vec::with_capacity(resp.entries.len());
    let mut entry_fold: Option<TemplateVisibility> = None;
    for entry in resp.entries {
        let entry_visibility = entry.template_visibility();
        entry_fold = Some(match entry_fold {
            Some(folded) => fold_visibility(folded, entry_visibility),
            None => entry_visibility,
        });
        let Some(release) = entry.release else {
            warn!(
                template = template_name,
                "ListLatestTemplateReleases entry missing release; skipping",
            );
            continue;
        };
        if release.hash.is_empty() {
            warn!(
                template = template_name,
                "ListLatestTemplateReleases entry has empty hash; skipping",
            );
            continue;
        }
        releases.push(LatestRelease {
            release_id: release.id,
            content_hash: release.hash,
            created_at: release.created_at,
        });
    }
    LatestReleasesOutcome::Resolved {
        visibility: combine_response_visibility(response_visibility, entry_fold),
        releases,
    }
}

/// Mirrors the `template_arch` extraction in service.rs's import path:
/// reads `arch` from metadata if present, else manifest, else falls
/// back to `"aarch64"`. Kept separate from
/// `crate::service::import_helpers::template_arch` so the volume-pull
/// path doesn't need to reach into the service module's privacy
/// surface, but the behavior matches.
fn arch_from(
    metadata: &pplx_space_common_core::types::TemplateMetadata,
    manifest: &pplx_space_common_core::types::TemplateManifest,
) -> String {
    if let Some(a) = metadata.arch.as_deref().filter(|s| !s.is_empty()) {
        return a.to_string();
    }
    if !manifest.arch.is_empty() {
        return manifest.arch.clone();
    }
    "aarch64".to_string()
}

#[cfg(test)]
pub(crate) mod tests;

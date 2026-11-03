//! Scheduled-snapshot retention: which snapshots each tier keeps, the prune
//! that deletes the rest, and the gate that stops captures while the GC
//! staging those deletes leave piles up.

use std::collections::HashMap;
use std::time::Duration;

use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};
use pplx_space_metrics::snapshot::SchedulerCaptureSkip;
use tokio::sync::Mutex;

use super::{CheckpointContext, SandboxSnapshotState};
use pplx_space_common_core::types::ArtifactKind;
use pplx_space_node_mgr_client::NodeManagerGrpcClient;

use crate::local_artifacts::{CheckoutError, LocalArtifacts, NamedSnapshotDelete};

/// Snapshot retention per tier. Each tier's count covers the gap to the next tier.
/// d-1m:  keep 5  → covers minutes 1-5 (until d-5m takes over)
/// d-5m:  keep 1  → covers minutes 5-10 (until f-10m takes over)
/// f-10m: keep 6  → covers 10min-1h (until f-1h takes over)
/// f-1h:  keep 6  → covers 1h-6h (until f-6h takes over)
/// f-6h:  keep 4  → covers 6h-24h
pub(super) const KEEP_D_1M: usize = 5;
pub(super) const KEEP_D_5M: usize = 1;
pub(super) const KEEP_F_10M: usize = 6;
pub(super) const KEEP_F_1H: usize = 6;
pub(super) const KEEP_F_6H: usize = 4;

/// Scheduled captures stop while the sandbox's unowned GC staging under
/// `snapshots/` numbers at least this many directories: every capture is
/// surplus a later prune moves aside, so while DeleteCacheArtifact keeps
/// failing more captures would only add staging. Five is more than a healthy
/// tick adds for the next prune to stage (its d-1m, its d-5m and one full
/// checkpoint), so one stuck directory, or one tick's failed deletes, never
/// blocks a capture. It is also no more than the 1m tier retains
/// ([`KEEP_D_1M`]), so stuck staging stops growing from captures at about one
/// disk tier's worth of snapshots.
pub(super) const UNOWNED_STAGING_LIMIT: usize = 5;

/// How long retention waits before retrying a staging delete that
/// node-manager answered `Unavailable`. DeleteCacheArtifact refuses rather
/// than waits while another sandbox mutation (pause or resume, VM capture or
/// restore, a port change, a launch) holds the sandbox's claim, and such
/// claims are usually short.
const UNAVAILABLE_RETRY_BACKOFF: Duration = Duration::from_millis(300);

/// How long one tick's staging deletes may take in total: this prune's own
/// staging, the one leftover retried and each `Unavailable` retry share it.
/// DeleteCacheArtifact queues for node-manager's delete capacity and then the
/// sandbox's artifact lock, and the node's scheduler loop awaits each
/// sandbox's tick in turn, so deletes that never answer would stall every
/// sandbox's scheduled snapshots on the node. A healthy delete answers well
/// within a second. A prune can stage many snapshots at once (pinned or leased
/// ones pile up past their tier's keep while retention skips them, and one
/// prune stages them all once released), so the bound is per tick, not per
/// delete: a tick's deletes hold the loop for at most this long, a quarter of
/// the scheduler's 60 s tick, however many there are. The delete running when
/// it is spent is left like a persistent `Unavailable`: node-manager may still
/// finish it, which is harmless because deletes are idempotent. The deletes
/// not reached are deferred to later ticks and the orphan sweep.
pub(super) const STAGING_DELETE_BUDGET: Duration = Duration::from_secs(15);

/// Keep only the newest entries seen for each scheduled tier. A partial walk
/// may delete an entry only after seeing enough newer entries to retain.
pub(super) fn retention_candidate(
    retained: &mut [std::collections::BTreeSet<(String, String)>; 5],
    name: String,
) -> Option<String> {
    // GC staging keeps the tier prefix of the snapshot it once was. It is no
    // snapshot: it must neither take a retained slot nor be pruned by name.
    if crate::local_artifacts::is_gc_staging_name(&name) {
        return None;
    }
    let (index, keep, timestamp) = [
        ("d-1m-", 0, KEEP_D_1M),
        ("1m-", 0, KEEP_D_1M),
        ("d-5m-", 1, KEEP_D_5M),
        ("5m-", 1, KEEP_D_5M),
        ("f-10m-", 2, KEEP_F_10M),
        ("f-1h-", 3, KEEP_F_1H),
        ("f-6h-", 4, KEEP_F_6H),
    ]
    .iter()
    .find_map(|(prefix, index, keep)| {
        name.strip_prefix(prefix)
            .map(|timestamp| (*index, *keep, timestamp.to_owned()))
    })?;
    let entries = retained.get_mut(index)?;
    entries.insert((timestamp, name));
    (entries.len() > keep)
        .then(|| entries.pop_first())
        .flatten()
        .map(|(_, name)| name)
}

/// Prune `sandbox_id`'s scheduled snapshots under `cache_root` (a test's own
/// directory, or the sandbox cache), delete the staging the prune leaves, and
/// then, unless too much unowned staging remains, `prepare` and `capture`. The
/// prune and the capture each run under the sandbox's disk artifact
/// publication locks, so backup selection never races a snapshot's creation
/// or its retention deletion. The capture depends on nothing the prune read,
/// so the locks are released between them, which lets the gate below count
/// the staging left after this tick's unlocked deletes.
///
/// The staging is deleted with the locks released: the pruned rows are gone,
/// so nothing owns it, and under the locks a node-manager deletion backlog
/// would stall the sandbox's publication, rollback, export and backup
/// enqueue. Besides this prune's own staging, the listing shows unowned
/// staging that an earlier delete, by any deleter, failed to remove
/// ([`Staged::delete`] says which of it this tick retries). All of a tick's
/// deletes share one [`STAGING_DELETE_BUDGET`], so however much staging a
/// prune leaves, they hold the node's scheduler loop for at most that long;
/// the staging they do not reach is deferred, and counted below. The locks
/// cover only the listing: which listed staging no row owns is looked up
/// after they are released.
///
/// A failed prune captures nothing: the listing failed, or the by-name delete
/// of a snapshot without a row (an uncatalogued DeleteBtrfsSnapshot) failed,
/// so that snapshot's bytes are still in place. A failed staging delete is
/// not a failed prune: the row is already gone. The capture is gated on a
/// bound, not on every staging delete succeeding: it is skipped, and the tick
/// fails, only while the unowned staging left under `snapshots/` after this
/// tick's deletes numbers at least [`UNOWNED_STAGING_LIMIT`]; below that,
/// leftovers wait for a later tick or the orphan sweep. The count is taken
/// from disk, so it survives a restart. While deletes keep failing, the ticks
/// below the limit each add at most what one prune moves aside, and once a
/// prune's deletes leave the limit reached no capture adds more: later prunes
/// can stage only snapshots that already existed. The count is stored in
/// `unowned_staging` for the gate's metrics and events, unless the prune
/// failed, when the listing may be partial.
///
/// The gate accepts two cases:
/// - Staging another deleter is still deleting (DeleteSnapshots with
///   `all=true`, say) has no row, so it counts as unowned until that delete
///   ends. Enough of it can close the gate for about a tick; the count
///   re-checks each entry after this tick's deletes, which narrows that window.
/// - [`UNOWNED_STAGING_LIMIT`] or more directories that never delete keep
///   the sandbox's scheduled captures and checkpoints stopped for as long as
///   they stay. That is the growth bound working as designed. It shows as
///   the `scheduled_snapshots_blocked` event for the sandbox, a sustained
///   `capture_skipped{reason:staging_backlog}`, and this function's warn on
///   every blocked tick with the `sandbox_id`. The `unowned_staging` gauge
///   sums the node's sandboxes, so it alone does not name the sandbox.
///
/// `prepare`, the guest flush and host sync, runs after the staging deletes,
/// with the locks released as the flush always ran, so the delete time does
/// not separate it from the capture. Its value is handed to `capture` and
/// returned.
pub(super) async fn prune_and_capture<T>(
    node_client: &mut NodeManagerGrpcClient,
    ctx: &CheckpointContext,
    sandbox_id: &str,
    cache_root: &str,
    unowned_staging: &mut Option<usize>,
    prepare: impl AsyncFnOnce(&mut NodeManagerGrpcClient) -> Result<T, String>,
    capture: impl AsyncFnOnce(&mut NodeManagerGrpcClient, &T) -> Result<(), String>,
) -> Result<T, String> {
    let artifacts = LocalArtifacts::new(ctx.db.clone());
    let mut staged = Staged::default();
    let pruned = {
        let (_wake_guard, _artifact_guard) = ctx
            .auto_lifecycle
            .artifact_publication_locks(sandbox_id, ArtifactKind::Disk)
            .await;
        prune_scheduled_snapshots(node_client, &artifacts, sandbox_id, cache_root, &mut staged)
            .await
    };
    let remaining = staged.delete(node_client, &artifacts).await;
    if let Err(error) = pruned {
        pplx_space_metrics::snapshot::scheduler_capture_skipped(SchedulerCaptureSkip::PruneFailed);
        return Err(error);
    }
    *unowned_staging = Some(remaining);
    if remaining >= UNOWNED_STAGING_LIMIT {
        pplx_space_metrics::snapshot::scheduler_capture_skipped(
            SchedulerCaptureSkip::StagingBacklog,
        );
        tracing::warn!(
            sandbox_id,
            unowned_staging = remaining,
            limit = UNOWNED_STAGING_LIMIT,
            "scheduled snapshot capture blocked: unowned GC staging under snapshots/ \
             reached the limit; the full checkpoint waits too"
        );
        return Err(format!(
            "scheduled snapshot capture blocked: {remaining} unowned GC staging \
             directories under snapshots/ (limit {UNOWNED_STAGING_LIMIT})"
        ));
    }
    let prepared = prepare(node_client).await?;
    let (_wake_guard, _artifact_guard) = ctx
        .auto_lifecycle
        .artifact_publication_locks(sandbox_id, ArtifactKind::Disk)
        .await;
    capture(node_client, &prepared).await?;
    Ok(prepared)
}

/// The GC staging a prune hands back for deletion once the locks are released.
#[derive(Default)]
struct Staged {
    /// What this prune moved aside: each snapshot name with its staging path.
    own: Vec<(String, String)>,
    /// The other GC staging the listing showed: each with its artifact id,
    /// its name and its path. Which of it is unowned is looked up once the
    /// locks are released.
    listed: Vec<(LocalArtifactId, String, String)>,
}

/// How one retention staging delete ended.
#[derive(Debug, PartialEq)]
enum StagingDelete {
    Deleted,
    /// Node-manager answered `Unavailable` twice: a sandbox mutation still
    /// held its claim. Transient, so it is not counted against the gate.
    Unavailable,
    /// No answer before the tick's [`STAGING_DELETE_BUDGET`] was spent.
    /// Left, and not counted, like [`StagingDelete::Unavailable`].
    TimedOut,
    Failed,
}

impl Staged {
    /// Delete this prune's own staging and, of the unowned leftovers, only
    /// the oldest: the lowest artifact id (UUIDv7 for most artifacts, so
    /// usually the oldest created). The orphan sweep's own task retries the
    /// rest, so a backlog that other deleters left cannot stall the node's
    /// scheduler loop, which awaits each sandbox's tick in turn.
    ///
    /// The deletes run in turn within one [`STAGING_DELETE_BUDGET`]. The
    /// first that times out has spent it, so the deletes after it are not
    /// attempted this tick: they are deferred, left on disk for a later tick
    /// or the orphan sweep.
    ///
    /// Returns how many unowned staging directories remain, counted after the
    /// deletes: each directory a delete failed to remove, each deferred one
    /// and each leftover not retried, if it still exists and no row owns it,
    /// so what the orphan sweep removed meanwhile does not count. Each that
    /// this tick's delete left [`StagingDelete::Unavailable`] or
    /// [`StagingDelete::TimedOut`] is not counted.
    async fn delete(
        mut self,
        node_client: &mut NodeManagerGrpcClient,
        artifacts: &LocalArtifacts,
    ) -> usize {
        self.listed.sort();
        let listed: Vec<(String, String)> = self
            .listed
            .into_iter()
            .map(|(_, name, staging)| (name, staging))
            .collect();
        let unowned = unowned_staging(
            artifacts,
            listed.iter().map(|(_, staging)| staging.clone()).collect(),
        )
        .await;
        let mut leftover = listed
            .into_iter()
            .zip(unowned)
            .filter_map(|(entry, unowned)| unowned.then_some(entry));
        let retried = leftover.next();
        let mut recount: Vec<String> = leftover.map(|(_, staging)| staging).collect();
        let budget = tokio::time::Instant::now() + STAGING_DELETE_BUDGET;
        let mut deletes = self.own.iter().chain(&retried);
        for (snapshot_name, staging) in deletes.by_ref() {
            match delete_staging(node_client, staging, snapshot_name, budget).await {
                StagingDelete::Failed => recount.push(staging.clone()),
                StagingDelete::TimedOut => break,
                StagingDelete::Deleted | StagingDelete::Unavailable => {}
            }
        }
        recount.extend(deletes.map(|(_, staging)| staging.clone()));
        unowned_staging(artifacts, recount)
            .await
            .into_iter()
            .filter(|unowned| *unowned)
            .count()
    }
}

/// Delete one staging directory by `budget`, the end of the tick's
/// [`STAGING_DELETE_BUDGET`], retrying once after
/// [`UNAVAILABLE_RETRY_BACKOFF`] if node-manager answers `Unavailable`.
async fn delete_staging(
    node_client: &mut NodeManagerGrpcClient,
    staging: &str,
    snapshot_name: &str,
    budget: tokio::time::Instant,
) -> StagingDelete {
    let delete = async |node_client: &mut NodeManagerGrpcClient| {
        crate::snapshot_gc::delete_gc_staging(node_client, staging, "retention", snapshot_name)
            .await
    };
    let unavailable = |status: &tonic::Status| status.code() == tonic::Code::Unavailable;
    let attempts = async {
        let result = delete(node_client).await;
        if !result.as_ref().is_err_and(unavailable) {
            return result;
        }
        tokio::time::sleep(UNAVAILABLE_RETRY_BACKOFF).await;
        delete(node_client).await
    };
    let Ok(result) = tokio::time::timeout_at(budget, attempts).await else {
        tracing::warn!(
            staging,
            budget_secs = STAGING_DELETE_BUDGET.as_secs(),
            "retention spent its GC staging delete budget: it leaves this staging, and the \
             tick's deletes after it, to a later tick or the orphan sweep; node-manager may \
             still finish this delete"
        );
        return StagingDelete::TimedOut;
    };
    match result {
        Ok(()) => StagingDelete::Deleted,
        Err(status) if unavailable(&status) => {
            tracing::info!(
                staging,
                "retention leaves GC staging that node-manager twice answered Unavailable \
                 to a later tick or the orphan sweep"
            );
            StagingDelete::Unavailable
        }
        Err(_) => StagingDelete::Failed,
    }
}

/// Record a tick's unowned staging count for `sandbox_id`, as retention
/// observed it, and emit a sandbox event when the count closes or reopens the
/// capture gate. A tick that observed none keeps the last count. The state is
/// in memory, so after a restart or a re-registration the first count that
/// finds the gate closed reports it again, and one that finds it open reports
/// nothing: a gate that reopens across a restart or re-registration has its
/// `scheduled_snapshots_blocked` without a matching
/// `scheduled_snapshots_unblocked`.
pub(super) async fn record_unowned_staging(
    sandboxes: &Mutex<HashMap<String, SandboxSnapshotState>>,
    sandbox_id: &str,
    observed: Option<usize>,
) {
    let Some(count) = observed else {
        return;
    };
    let previous = {
        let mut map = sandboxes.lock().await;
        let Some(state) = map.get_mut(sandbox_id) else {
            return;
        };
        state.unowned_staging.replace(count)
    };
    let blocks = |count: usize| count >= UNOWNED_STAGING_LIMIT;
    let details = SandboxEventDetails::default()
        .trigger(SandboxEventTrigger::System)
        .reason("gc_staging_backlog");
    let (event_type, details) = match (previous.is_some_and(blocks), blocks(count)) {
        (false, true) => (
            SandboxEventType::ScheduledSnapshotsBlocked,
            details.error(format!(
                "{count} unowned GC staging directories under snapshots/ \
                 (limit {UNOWNED_STAGING_LIMIT})"
            )),
        ),
        (true, false) => (SandboxEventType::ScheduledSnapshotsUnblocked, details),
        _ => return,
    };
    emit_sandbox_event(
        sandbox_id,
        event_type,
        SandboxEventSource::SbxManager,
        details,
    );
}

/// Emit the node's unowned retention staging: the sum of each scheduled
/// sandbox's last recorded count.
pub(super) async fn emit_unowned_staging_gauge(
    sandboxes: &Mutex<HashMap<String, SandboxSnapshotState>>,
) {
    let total = sandboxes
        .lock()
        .await
        .values()
        .filter_map(|state| state.unowned_staging)
        .sum();
    pplx_space_metrics::snapshot::scheduler_unowned_staging(total);
}

/// For each of `paths`, whether it still exists as GC staging that no row
/// owns: a stat, then [`crate::snapshot_gc::gc_pending_is_unowned`]. It runs
/// on the blocking pool, as the orphan sweep runs it, since the ownership
/// lookup reads SQLite. If that task fails, no path is unowned.
async fn unowned_staging(artifacts: &LocalArtifacts, paths: Vec<String>) -> Vec<bool> {
    let (artifacts, count) = (artifacts.clone(), paths.len());
    match tokio::task::spawn_blocking(move || {
        paths
            .iter()
            .map(|path| {
                let path = std::path::Path::new(path);
                path.exists() && crate::snapshot_gc::gc_pending_is_unowned(&artifacts, path)
            })
            .collect()
    })
    .await
    {
        Ok(unowned) => unowned,
        Err(error) => {
            tracing::warn!(%error,
                "retaining GC staging after ownership lookup task failed");
            vec![false; count]
        }
    }
}

/// Delete the scheduled snapshots outside each tier's retention. Each
/// catalogued snapshot's GC staging, with its name, goes to `staged.own`, and
/// each other GC staging the listing shows to `staged.listed`.
async fn prune_scheduled_snapshots(
    node_client: &mut NodeManagerGrpcClient,
    artifacts: &LocalArtifacts,
    sandbox_id: &str,
    cache_root: &str,
    staged: &mut Staged,
) -> Result<(), String> {
    let sandbox = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| error.to_string())?;
    let mut pages = pplx_space_node_mgr_client::SnapshotPages::new(sandbox, false);
    let mut retained = std::array::from_fn(|_| std::collections::BTreeSet::new());
    while let Some(snapshots) = pages
        .next(node_client)
        .await
        .map_err(|e| format!("list snapshots: {e}"))?
    {
        for snapshot in snapshots {
            if crate::local_artifacts::is_gc_staging_name(&snapshot.name) {
                let staging = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir_in(
                    cache_root,
                    sandbox_id,
                    &snapshot.name,
                );
                // A later page can list what this prune staged on an earlier one.
                if let Some(id) = crate::local_artifacts::gc_staging_id(&snapshot.name)
                    && !staged.own.iter().any(|(_, own)| *own == staging)
                {
                    staged.listed.push((id, snapshot.name, staging));
                }
                continue;
            }
            let Some(name) = retention_candidate(&mut retained, snapshot.name) else {
                continue;
            };
            // A snapshot without a row is deleted by name: node-manager's
            // DeleteBtrfsSnapshot deletes this path. A catalogued one is
            // renamed to its GC staging sibling, which the caller deletes
            // with DeleteCacheArtifact once it releases its locks.
            let delete_path = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir_in(
                cache_root, sandbox_id, &name,
            );
            let request = pplx_space_node_mgr_client::proto::DeleteBtrfsSnapshotRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: name.clone(),
            };
            let delete_in_place =
                async { node_client.delete_btrfs_snapshot(request).await.map(drop) };
            match artifacts
                .delete_named_snapshot(sandbox_id, &name, &delete_path, delete_in_place)
                .await
            {
                NamedSnapshotDelete::Deleted { staging } => {
                    staged
                        .own
                        .extend(staging.map(|staging| (name.clone(), staging)));
                }
                // The prune runs before every scheduled capture, so a snapshot
                // it keeps must not stop the rest of retention or capture, and
                // one a later prune can retry (pinned, leased or still being
                // deleted) logs only at debug.
                NamedSnapshotDelete::Refused(
                    error @ (CheckoutError::Pinned(_) | CheckoutError::NotReady { .. }),
                ) => {
                    tracing::debug!(sandbox_id, snapshot_name = %name, %error,
                        "retention retains a snapshot whose local artifact is in use");
                }
                NamedSnapshotDelete::Refused(error) => {
                    tracing::warn!(sandbox_id, snapshot_name = %name, %error,
                        "retention retains a snapshot whose local artifact cannot be deleted");
                }
                NamedSnapshotDelete::PathDiffers { local_path } => {
                    tracing::warn!(sandbox_id, snapshot_name = %name, local_path,
                        "retention retains a snapshot whose publication path differs");
                }
                NamedSnapshotDelete::BytesNotDeleted(error) => {
                    return Err(format!("prune {name}: {error}"));
                }
            }
        }
    }
    Ok(())
}

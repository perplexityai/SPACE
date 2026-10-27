//! Scheduled-snapshot retention: which snapshots each tier keeps, and the
//! prune that deletes the rest.

use pplx_space_common_core::types::SandboxId;

use super::CheckpointContext;
use pplx_space_common_core::types::ArtifactKind;
use pplx_space_node_mgr_client::NodeManagerGrpcClient;

use crate::local_artifacts::{CheckoutError, NamedSnapshotDelete};

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
/// directory, or the sandbox cache) and run `capture`, both under the
/// sandbox's disk artifact publication locks, so backup selection never races
/// a snapshot's creation or its retention deletion. `capture` runs only after
/// a successful prune, so a failed byte delete blocks further growth.
///
/// The pruned snapshots' GC staging is deleted after the locks are released,
/// whatever the prune or capture returned: their rows are gone, so nothing
/// owns it, and under the locks a node-manager deletion backlog would stall
/// the sandbox's publication, rollback, export and backup enqueue. A failed
/// staging delete is the orphan sweep's.
pub(super) async fn prune_and_capture(
    node_client: &mut NodeManagerGrpcClient,
    ctx: &CheckpointContext,
    sandbox_id: &str,
    cache_root: &str,
    capture: impl AsyncFnOnce(&mut NodeManagerGrpcClient) -> Result<(), String>,
) -> Result<(), String> {
    let mut staged = Vec::new();
    let result = {
        let (_wake_guard, _artifact_guard) = ctx
            .auto_lifecycle
            .artifact_publication_locks(sandbox_id, ArtifactKind::Disk)
            .await;
        match prune_scheduled_snapshots(node_client, ctx, sandbox_id, cache_root, &mut staged).await
        {
            Ok(()) => capture(node_client).await,
            Err(error) => Err(error),
        }
    };
    for (snapshot_name, staging) in &staged {
        let _ =
            crate::snapshot_gc::delete_gc_staging(node_client, staging, "retention", snapshot_name)
                .await;
    }
    result
}

/// Delete the scheduled snapshots outside each tier's retention. Each
/// catalogued snapshot's GC staging, with its name, goes to `staged`.
async fn prune_scheduled_snapshots(
    node_client: &mut NodeManagerGrpcClient,
    ctx: &CheckpointContext,
    sandbox_id: &str,
    cache_root: &str,
    staged: &mut Vec<(String, String)>,
) -> Result<(), String> {
    let sandbox = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| error.to_string())?;
    let artifacts = crate::local_artifacts::LocalArtifacts::new(ctx.db.clone());
    let mut pages = pplx_space_node_mgr_client::SnapshotPages::new(sandbox, false);
    let mut retained = std::array::from_fn(|_| std::collections::BTreeSet::new());
    while let Some(snapshots) = pages
        .next(node_client)
        .await
        .map_err(|e| format!("list snapshots: {e}"))?
    {
        for snapshot in snapshots {
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
                    staged.extend(staging.map(|staging| (name.clone(), staging)));
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

//! Scheduled-snapshot retention: which snapshots each tier keeps, and the
//! prune that deletes the rest.

use pplx_space_common_core::types::SandboxId;

use super::CheckpointContext;
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

pub(super) async fn prune_scheduled_snapshots(
    node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    ctx: &CheckpointContext,
    sandbox_id: &str,
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
            let request = pplx_space_node_mgr_client::proto::DeleteBtrfsSnapshotRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: name.clone(),
            };
            let delete_bytes = async { node_client.delete_btrfs_snapshot(request).await.map(drop) };
            // Node-manager deletes this sandbox's snapshot of that name here.
            let delete_path =
                pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(sandbox_id, &name);
            match artifacts
                .delete_named_snapshot(sandbox_id, &name, &delete_path, delete_bytes)
                .await
            {
                NamedSnapshotDelete::Deleted => {}
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

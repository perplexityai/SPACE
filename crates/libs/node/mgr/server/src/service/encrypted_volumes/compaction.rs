//! The compaction step of a suspend's staging: decide from the volume's
//! recorded baseline while it is still writable, record the baseline it needs,
//! and build the compacted copy to publish once it is read-only. It never
//! fails the staging; anything it can't finish publishes the volume as it is.

use std::path::{Path, PathBuf};

use pplx_space_common_core::types::SandboxId;
use pplx_space_metrics::byok::{COMPACTION_COMPACTED, COMPACTION_SKIPPED};

use crate::encrypted_volume::compact::{self, BaselineState, Decision};

/// A volume due for compaction, as measured before it went read-only.
pub(super) struct Plan {
    used: u64,
    baseline: BaselineState,
}

/// Decide for the writable volume mounted at `mount_dir` and record its
/// baseline. A volume due for compaction records its current size first, so
/// a compaction that fails or is retried is not attempted again until the
/// volume doubles once more. `Some` when the staging should publish a
/// compacted copy.
pub(super) async fn plan(
    sandbox_id: &SandboxId,
    volume_name: &str,
    backing_path: &Path,
    mount_dir: &Path,
    fresh: bool,
) -> Option<Plan> {
    let measured: Result<Option<(u64, BaselineState)>, crate::rootfs_dmcrypt::DmCryptError> =
        async {
            if !compact::is_volume_mounted(sandbox_id, volume_name, mount_dir).await? {
                return Ok(None);
            }
            let used = compact::used_bytes(mount_dir).await?;
            let baseline = compact::read_baseline(mount_dir).await?;
            Ok(Some((used, baseline)))
        }
        .await;
    let (used, baseline) = match measured {
        Ok(Some(measured)) => measured,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(%sandbox_id, %error, "failed to measure encrypted volume for compaction");
            return None;
        }
    };
    let decision = compact::decide(used, baseline, fresh);
    if decision != Decision::Keep
        && let Err(error) = compact::record_baseline(mount_dir, used).await
    {
        tracing::warn!(%sandbox_id, %error, "failed to record an encrypted volume's compaction baseline");
    }
    if decision != Decision::Compact {
        return None;
    }
    let allocated_bytes = compact::allocated_bytes(backing_path).await.ok();
    tracing::info!(
        %sandbox_id,
        ?baseline,
        used_bytes = used,
        allocated_bytes,
        "encrypted volume is due for compaction"
    );
    Some(Plan { used, baseline })
}

/// Build the compacted copy of the read-only volume. `None` when it can't be
/// finished; the caller then publishes the volume itself.
pub(super) async fn copy(
    sandbox_id: &SandboxId,
    volume_name: &str,
    backing_path: &Path,
    mount_dir: &Path,
    plan: Plan,
) -> Option<PathBuf> {
    let started = std::time::Instant::now();
    let skipped = |error: String| {
        pplx_space_metrics::byok::volume_compaction(COMPACTION_SKIPPED);
        tracing::warn!(
            %sandbox_id,
            error,
            baseline = ?plan.baseline,
            used_bytes = plan.used,
            "encrypted volume compaction stopped; publishing the volume as it is"
        );
        None
    };
    let dek = match super::fetch_sandbox_volume_dek(sandbox_id, None).await {
        Ok(dek) => dek,
        Err(status) => return skipped(status.message().to_string()),
    };
    match compact::build_compacted_copy(compact::CompactParams {
        sandbox_id: *sandbox_id,
        volume_name,
        backing_path,
        mount_dir,
        dek: &dek,
    })
    .await
    {
        Ok(copy) => {
            pplx_space_metrics::byok::volume_compaction(COMPACTION_COMPACTED);
            pplx_space_metrics::byok::volume_compaction_duration(started);
            let after_allocated_bytes = compact::allocated_bytes(&copy.backing_path).await.ok();
            tracing::info!(
                %sandbox_id,
                before_used_bytes = plan.used,
                after_used_bytes = copy.used,
                after_allocated_bytes,
                elapsed_ms = started.elapsed().as_millis(),
                "compacted encrypted volume for publication"
            );
            Some(copy.backing_path)
        }
        Err(error) => skipped(error.to_string()),
    }
}

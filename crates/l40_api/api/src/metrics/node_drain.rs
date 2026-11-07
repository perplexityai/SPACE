//! Local snapshot promotion, which the cluster controller's drain reconciler
//! records and the API server's admin force-upload route records too. The
//! drain reconciler's other metrics are the controller's `metrics::node_drain`.

use pplx_space_metrics_emit::{counter, tag};

/// Reconciler-driven promotion of a local-only suspended snapshot to
/// S3, so the source node can finish draining. `outcome` is one of:
///
/// - `"backup_enqueued"` — the reconciler enqueued a backup of the current
///   full local artifact on the on-node sbx-mgr's durable worker. The sandbox drops out of the
///   local-only list on the next tick once the durable worker
///   registers a matching `state='ready'` backup row.
/// - `"failed"` — the on-node RPC errored or timed out. The reconciler
///   retries on the next tick. See `node_drain.failures` (`failure` in the
///   cluster controller's `metrics::node_drain`) for the matching
///   per-phase error counter.
/// - `"failed_artifact_missing"` — terminal: the on-node RPC reported
///   that the local snapshot files have been pruned and no S3 copy
///   exists. The reconciler auto-marks the row `lifecycle = 'failed'`
///   so drain can proceed; the data was already lost before this
///   tick.
/// - `"node_gone_gave_up_remote_snapshot"` — the node's kube Node is gone
///   and its EC2 instance is confirmed terminated, so the backup enqueue
///   can never reach it; the reconciler adopted the newest ready remote
///   snapshot instead (the sandbox stays `suspended` and recoverable).
/// - `"node_gone_lost"` — same node-gone trigger, but no ready remote
///   snapshot exists; the local artifact died with the instance, so the
///   row was marked `lifecycle = 'failed'` (terminal, data already lost).
/// - `"unnamed_withheld"` — nothing was sent: the row names no artifact
///   and a gentle drain forbids the unnamed current-instance backup.
///   Re-evaluated next tick.
/// - `"node_gone_retry"` — same node-gone trigger, but neither the adopt
///   nor the mark-lost guard fired this tick (in-flight upload row, stale
///   ready snapshot, or drift); re-evaluated next tick.
pub fn local_snapshot_promotion(node_id: &str, outcome: &str) {
    counter(
        "node_drain.local_snapshot_promotion",
        1,
        &[tag("node", node_id), tag("outcome", outcome)],
    );
}

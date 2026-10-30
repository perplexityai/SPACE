//! `BeginArtifactBackup`'s execution-generation fence: the registration of a
//! capture whose runtime the control plane superseded is refused.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ExecutionGeneration, SandboxId};
use pplx_space_node_api_client::cluster::backup_execution_superseded_status;
use tonic::Status;

/// Refuse a new execution-bound backup whose generation the control plane has
/// superseded: a newer execution is active or has materialized a pod, or this
/// generation's own execution was retired as `replaced` (an abandoned restore
/// claim, a started-recovery replacement, an activation its node rolled back
/// with nothing the capture could be the only copy of).
/// Either way no restore can adopt the runtime that captured the artifact.
///
/// A teardown or idle suspend runs on the node for whatever runtime the pod
/// carried, including such a runtime: a reaped orphaned restore pod's VM, say,
/// whose restore was rolled back. Its snapshot would be the newest ready full
/// snapshot of a markerless row, so the next restore would select it. The
/// refusal names the generation ([`backup_execution_superseded_status`]) so the
/// node releases that runtime instead of retrying.
///
/// Any other state is admitted, including an execution retired for another
/// reason (a suspend the transient-lifecycle sweeper aged out, a pod found
/// missing) with nothing newer: its capture may be the sandbox's latest state,
/// so it registers as it did before this fence. That holds on a row left on
/// no node too, where the source-node check then refuses the capture without
/// a verdict and the node keeps it resident: a ready earlier snapshot isn't
/// proof that the last execution's later writes are disposable. The predicate
/// is the generation, which retirement works in; it doesn't tell apart two
/// attempts of the same still-active generation.
///
/// The caller applies it to an artifact's first registration only. A capture
/// the control plane admitted once was taken while its execution was current
/// and may be what started recovery is restoring from, so a retry of it
/// registers whatever happened since.
///
/// `FOR SHARE` holds the execution rows at and above the generation against a
/// concurrent retire until commit, and a retire that commits while this waits
/// is re-read. A newer execution is inserted under the sandbox row's
/// `FOR UPDATE`, which conflicts with the key-share lock the snapshot insert
/// already holds, so it can't appear unseen either; the order is the
/// sandbox-then-execution order the substrate store uses.
pub(crate) async fn refuse_superseded_backup_execution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> Result<(), Status> {
    let executions: Vec<(i64, bool, String, bool)> = sqlx::query_as(sql!(
        "SELECT generation, active, state,
                pod_materialized_at IS NOT NULL OR NULLIF(BTRIM(pod_uid), '') IS NOT NULL
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
            AND generation >= $2
            FOR SHARE"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("backup execution lookup: {e}")))?;
    // A newer execution supersedes this one once it's active or has
    // materialized a pod. A newer claim abandoned before it created one never
    // ran, so it takes nothing from this execution. `pod_materialized_at` is
    // the set-once record that the execution created a pod, which the
    // substrate's execution store writes in the same statements that record
    // or clear a `pod_uid`; `pod_uid` alone is current state, which
    // re-materialization, failed-recovery pod absence and a cross-node
    // re-drive's reset clear after the pod may have run. The record is NULL on
    // rows that predate it until the backfill stamps them, so a non-blank
    // `pod_uid` counts too, and a row whose `pod_uid` was cleared before it
    // was stamped reads as never having run, as it did before the record
    // existed.
    //
    // The store records a pod's UID when it creates the pod, before the
    // scheduler places it, so a created pod counts even if it never started:
    // a source-pinned restore pod ruled Unschedulable leaves its execution
    // stamped after the cross-node re-drive deletes it and resets the row.
    // That's deliberate. Counting only pods with a recorded IP or state would
    // miss a pod that ran before either was recorded, and admitting a capture
    // over a pod that ran registers stale bytes as the latest snapshot. A
    // capture refused this way stays on its node unless a backed copy of the
    // sandbox releases it.
    let superseded =
        executions
            .iter()
            .any(|(execution_generation, active, state, materialized)| {
                if *execution_generation > generation.get() {
                    *active || *materialized
                } else {
                    !*active && state == "replaced"
                }
            });
    if !superseded {
        return Ok(());
    }
    Err(backup_execution_superseded_status(
        format!(
            "execution generation {generation} of sandbox {sandbox_id} was superseded; no \
             restore can adopt the runtime that captured this artifact"
        ),
        generation,
    ))
}

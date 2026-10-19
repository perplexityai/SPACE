//! `BeginArtifactBackup`'s execution-generation fence: the registration of a
//! capture whose runtime the control plane superseded is refused.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ExecutionGeneration, SandboxId};
use pplx_space_node_api_client::cluster::backup_execution_superseded_status;
use tonic::Status;

/// Refuse a new execution-bound backup whose generation the control plane has
/// superseded: a newer execution is active or has materialized a pod, or this
/// generation's own execution was retired as `replaced` (an abandoned restore
/// claim, a started-recovery replacement). Either way no restore can adopt the runtime
/// that captured the artifact.
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
/// so it registers as it did before this fence. The predicate is the
/// generation, which retirement works in; it doesn't tell apart two attempts
/// of the same still-active generation.
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
        "SELECT generation, active, state, NULLIF(BTRIM(pod_uid), '') IS NOT NULL
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
    // materialized a pod. A newer claim abandoned before that never ran, so it
    // takes nothing from this execution.
    //
    // Residual window: `pod_uid` is current state, not a record that a pod
    // ran, so a newer execution can stop counting after it ran. The capture
    // of generation N registers when both hold:
    // - N was retired as something other than `replaced`, so the own-row
    //   branch below doesn't refuse it.
    // - A newer execution is inactive and its `pod_uid` was cleared after it
    //   ran. Two writers clear it on an active row whose pod may have run:
    //   pod re-materialization (`claim_active_execution_pod_materialization`
    //   in the API substrate's `store/pod_placement.rs`) and failed-recovery
    //   pod absence (`converge_failed_recovery_after_pod_absence_if_generation`
    //   in `store/pod_tracking.rs`). A third, the cross-node re-drive's reset
    //   (`reset_active_execution_to_creating_if_generation` in
    //   `store/execution_rows.rs`), clears it for a deleted pod whose row has
    //   no IP; that includes a pod that ran only if the pod was later
    //   observed without an IP, which `update_active_execution_pod_metadata`
    //   assigns from the observed pod. The newer-row branch reads only `active` and `pod_uid`, so
    //   the row's retirement state doesn't matter: the
    //   `finish_active_execution*` and `finish_fenced_execution_in` writers in
    //   `store/execution_rows.rs` retire it under any state, and
    //   re-materialization leaves it in the podless `creating` shape
    //   `abandon_unmaterialized_create_claim_by_generation` retires as
    //   `replaced`.
    // Closing it needs a set-once materialization marker on the execution
    // row.
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

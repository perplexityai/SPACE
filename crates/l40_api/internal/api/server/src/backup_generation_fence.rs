//! `BeginArtifactBackup`'s execution-generation fence: the registration of a
//! capture whose runtime the control plane superseded is refused.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ExecutionGeneration, SandboxId};
use pplx_space_node_api_client::cluster::backup_execution_superseded_status;
use pplx_space_sandbox_portability::execution_supersession::{
    ExecutionSupersessionRow, execution_generation_superseded,
};
use tonic::Status;

/// One execution row as the fence's query selects it, decoded by column name
/// so a reordered `SELECT` can't swap two of the flags.
#[derive(sqlx::FromRow)]
struct FenceRow {
    generation: i64,
    active: bool,
    replaced: bool,
    materialized: bool,
}

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
/// no node too, whose source is the execution's pod node when that resolves
/// to one live worker (see `pplx_space_sandbox_store::SnapshotCaptureSource`).
/// When it doesn't, as for a pod that was never placed, the source falls
/// back to the target (to no node, and so to a refusal of every caller, when
/// there's no target either), the source-node check refuses any other node
/// without a verdict, and that node keeps the capture resident: a ready
/// earlier snapshot isn't proof that the last execution's later writes are
/// disposable. The predicate is the generation, which retirement works in;
/// it doesn't tell apart two attempts of the same still-active generation.
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
    let rows: Vec<FenceRow> = sqlx::query_as(sql!(
        "SELECT generation, active, state = 'replaced' AS replaced,
                pod_materialized_at IS NOT NULL OR NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                    AS materialized
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
    let executions: Vec<ExecutionSupersessionRow> = rows
        .into_iter()
        .map(|row| ExecutionSupersessionRow {
            generation: row.generation,
            active: row.active,
            replaced: row.replaced,
            materialized: row.materialized,
        })
        .collect();
    // A capture refused here stays on its node unless a backed copy of the
    // sandbox releases it. On a row that still exists, the orphaned-restore-pod
    // GC reaps only pods this same predicate superseded when it last read the
    // rows.
    let superseded = execution_generation_superseded(&executions, generation.get());
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

/// Refuse a registration attributed to `generation`'s pod node once a newer
/// execution is active or has materialized a pod: the sandbox has run
/// elsewhere since, so that node no longer stands in for the target.
/// `pplx_space_sandbox_store::snapshot_capture_source` names the pod node
/// without checking for newer executions; the fence refuses it on a first
/// registration, and this check, read after the snapshot insert, on the
/// attempt the fence skips: a fresh attempt for bytes another attempt
/// registered. That insert's key-share lock on the sandbox row keeps a newer
/// execution from being inserted, and `FOR SHARE` keeps one from
/// materializing, until commit.
pub(crate) async fn refuse_overtaken_execution_attribution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
) -> Result<(), Status> {
    let newer_ran: Vec<bool> = sqlx::query_scalar(sql!(
        "SELECT active
                OR pod_materialized_at IS NOT NULL
                OR NULLIF(BTRIM(pod_uid), '') IS NOT NULL
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
            AND generation > $2
            FOR SHARE"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("backup execution lookup: {e}")))?;
    if newer_ran.contains(&true) {
        return Err(Status::permission_denied(format!(
            "a newer execution of sandbox {sandbox_id} than generation {generation} is \
             active or has run a pod, so that generation's pod node no longer uploads its \
             backups"
        )));
    }
    Ok(())
}

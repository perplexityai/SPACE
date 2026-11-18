//! [`DrainV2Sink`] for the production sink: the v2 loop's statements over
//! `nodes`, its evacuation record, and its node-api calls.

use std::str::FromStr;

use pplx_space_api::controllers::substrate::connect_internal_node_api_client_for_target;
use pplx_space_sbx_mgr_client::proto::{DrainEvacuated, DrainNodeRequest, DrainNodeResponse};
use uuid::Uuid;

use pplx_space_node_registration_store::drain_v2::{
    LockedV2Node, StoredV2Report, advance_v2_drained, evacuated_still_suspended, finish_v2_cancel,
    let_go_v2_node, list_v2_node_rows, read_locked_v2_node, record_terminate_held,
    record_v2_dispatched, record_v2_error, record_v2_report, record_v2_terminated_observed,
    return_v2_drained, write_fence,
};

use super::*;
use crate::node_drain::guarded_delete::{self, GuardedDelete, NodeIdentity};
use crate::node_drain::v2::{
    CordonFailure, DrainCallError, DrainV2Sink, EvacuationRecord, Fence, NodeMachine,
    TerminatedDelete, V2Node,
};

/// How long one `DrainNode` call may take; the node answers without waiting.
const DRAIN_NODE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the direct Kubernetes Node GET may take, and a cordon's lift.
const NODE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long an active execution owns a restore, which the fence's release
/// of a suspended row reads as a restore in flight.
fn execution_ownership_ttl_secs() -> i64 {
    i64::try_from(
        pplx_space_rpc_contracts::restore_ownership::RESTORE_EXECUTION_OWNERSHIP_TTL.as_secs(),
    )
    .unwrap_or(i64::MAX)
}

pub(crate) async fn list_v2_nodes(pool: &PgPool) -> Result<Vec<V2Node>, sqlx::Error> {
    let rows = list_v2_node_rows(pool).await?;
    Ok(rows
        .into_iter()
        .map(|row| V2Node {
            id: row.id,
            report: row.report(),
            phase: row.drain_phase,
            generation: row
                .generation
                .and_then(|generation| u64::try_from(generation).ok()),
            deadline_at: row.deadline_at,
            evacuate: row.evacuate,
            k8s_node_name: row.k8s_node_name,
            k8s_node_uid: row.k8s_node_uid,
            instance_id: row.instance_id,
            heartbeat_fresh: row.heartbeat_fresh,
            fence: row.fence,
            dispatched_at: row.dispatched_at,
            terminated_observed_at: row.terminated_observed_at,
            decrement_capacity: row.decrement_capacity,
            db_now: row.db_now,
        })
        .collect())
}

/// Lock `node_id`'s row `FOR UPDATE` and read its v2 state; `None` when the
/// row is gone.
async fn lock_v2_node(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
) -> Result<Option<LockedV2Node>, sqlx::Error> {
    if !guarded_delete::lock_node(tx, node_id).await? {
        return Ok(None);
    }
    read_locked_v2_node(tx, node_id).await
}

/// The terminate transaction of the v2 node under `generation`, under its
/// row lock, once the terminating tick's fresh check found nothing pending
/// or blocked: it must still be that generation's `terminating` node,
/// unfenced. It then cleans up the references of the rows that no longer
/// need the node, the only sandbox-row writes the v2 loop makes: the
/// durable stopped rows still at home on it are handed off
/// ([`pplx_space_sandbox_store::drain_terminate::hand_off_durable_stopped`],
/// the stopped handoff a start makes) and the durable suspended rows naming
/// it released
/// ([`pplx_space_sandbox_store::drain_terminate::release_durable_suspended`]),
/// each with its execution on the node retired. Then nothing may name the
/// node any more (the require-empty predicate,
/// [`pplx_space_sandbox_store::drain_terminate::node_references`]), and
/// `drain_status.terminate_instance` records `node_k8s.instance_id`, the
/// instance to terminate. A refusal of the predicate records what holds the
/// node in `drain_last_error` and commits the cleanup.
pub(crate) async fn fence_v2_node(
    pool: &PgPool,
    node_id: NodeId,
    generation: i64,
) -> Result<Fence, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(node) = lock_v2_node(&mut tx, node_id).await? else {
        return Ok(Fence::Moved);
    };
    if !node.is_v2("terminating", generation) || node.fence.is_some() {
        return Ok(Fence::Moved);
    }
    let Some(instance_id) = node.instance_id else {
        return Ok(Fence::NoInstance);
    };
    pplx_space_sandbox_store::drain_terminate::hand_off_durable_stopped(&mut tx, node_id).await?;
    pplx_space_sandbox_store::drain_terminate::release_durable_suspended(
        &mut tx,
        node_id,
        node.k8s_node_name.as_deref(),
        execution_ownership_ttl_secs(),
    )
    .await?;
    let references = pplx_space_sandbox_store::drain_terminate::node_references(
        &mut *tx,
        node_id,
        node.k8s_node_name.as_deref(),
        guarded_delete::REFERENCE_SAMPLE,
    )
    .await?;
    if !references.is_empty() {
        let held = format!("terminate held: {references}");
        record_terminate_held(&mut tx, node_id, &held).await?;
        tx.commit().await?;
        return Ok(Fence::Held(held));
    }
    write_fence(&mut tx, node_id, &instance_id).await?;
    tx.commit().await?;
    Ok(Fence::Fenced(instance_id))
}

/// Let go of the v2 node under `generation`, in the phase and with the
/// fence the tick read, under its row lock.
pub(crate) async fn let_go_v2(
    pool: &PgPool,
    node_id: NodeId,
    generation: i64,
    phase: Option<&str>,
    fence: Option<&str>,
    trace: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !guarded_delete::lock_node(&mut tx, node_id).await? {
        return Ok(false);
    }
    let cleared = let_go_v2_node(&mut *tx, node_id, generation, phase, fence, trace).await?;
    tx.commit().await?;
    Ok(cleared)
}

/// Delete the row of the `terminating` v2 node under `generation` whose fenced
/// instance `fence` EC2 says is terminated, its Node gone: under the row
/// lock, the row must still carry that fence with a dispatch, and with the
/// record that EC2 listed the instance `terminated`, which an instance EC2
/// no longer lists can't stand in for, and record that instance; then the
/// guarded delete runs, holding the uid the tick read, a stale heartbeat and
/// that nothing names the node ([`guarded_delete::delete_locked_node`]). A
/// row or execution guard that refuses lets the node go in the same
/// transaction instead.
pub(crate) async fn delete_v2_terminated(
    pool: &PgPool,
    node: &V2Node,
    generation: i64,
    fence: &str,
) -> Result<TerminatedDelete, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(locked) = lock_v2_node(&mut tx, node.id).await? else {
        return Ok(TerminatedDelete::Wait("the row is gone".to_owned()));
    };
    if locked.fence.as_deref() != Some(fence) {
        return Ok(if locked.fence.is_none() {
            TerminatedDelete::Unfenced
        } else {
            TerminatedDelete::Wait("the row carries another fence".to_owned())
        });
    }
    if !locked.is_v2("terminating", generation) || !locked.dispatched {
        return Ok(TerminatedDelete::Wait(
            "the row is no longer the generation's dispatched terminating node".to_owned(),
        ));
    }
    if !locked.terminated_observed {
        return Ok(TerminatedDelete::Wait(
            "the row does not record that EC2 listed the fenced instance terminated".to_owned(),
        ));
    }
    let deleted = guarded_delete::delete_locked_node(
        &mut tx,
        &NodeIdentity {
            node_id: node.id,
            managed_by: SubstrateKind::Kubernetes,
            k8s_node_name: locked.k8s_node_name.as_deref(),
            k8s_node_uid: node.k8s_node_uid.as_deref(),
            instance_id: Some(fence),
        },
    )
    .await?;
    match deleted {
        GuardedDelete::Deleted => {
            tx.commit().await?;
            Ok(TerminatedDelete::Deleted)
        }
        GuardedDelete::Referenced(references) => {
            let trace = format!("v2 drain let go after terminating {fence}: {references}");
            let_go_v2_node(
                &mut *tx,
                node.id,
                generation,
                Some("terminating"),
                Some(fence),
                &trace,
            )
            .await?;
            tx.commit().await?;
            Ok(TerminatedDelete::LetGo(trace))
        }
        GuardedDelete::HeartbeatFresh => Ok(TerminatedDelete::Wait(
            "the node's heartbeat is fresh".to_owned(),
        )),
        GuardedDelete::IdentityMoved => Ok(TerminatedDelete::Wait(
            "the node's Kubernetes uid or recorded instance moved".to_owned(),
        )),
    }
}

/// Finish the cancel of the undrained v2 drain of `node_id` under
/// `generation`, once the node answered the cancel: under the node's row
/// lock, and only while the row is still that generation's cancel (a v2
/// engine with no phase), lift the node's cordon with `uncordon` (passed
/// the Node's name and the uid its registration recorded), then clear the
/// v2 columns, in one transaction. A drain of the node started since the
/// tick listed it took the row lock to write itself, so it finds no such
/// row and nothing is lifted or cleared; one that starts now waits for this
/// transaction and cordons after it. Returns whether the columns were
/// cleared; an uncordon that fails clears nothing.
pub(crate) async fn finish_v2_cancel_locked<F, Fut>(
    pool: &PgPool,
    node_id: NodeId,
    generation: i64,
    uncordon: F,
) -> Result<bool, String>
where
    F: FnOnce(String, Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let sql_error = |error: sqlx::Error| error.to_string();
    let mut tx = pool.begin().await.map_err(sql_error)?;
    let Some(node) = lock_v2_node(&mut tx, node_id).await.map_err(sql_error)? else {
        return Ok(false);
    };
    if !node.is_v2_cancel(generation) {
        return Ok(false);
    }
    let Some(node_name) = node.k8s_node_name else {
        return Err("the node has no Kubernetes name whose cordon to lift".to_owned());
    };
    uncordon(node_name, node.k8s_node_uid).await?;
    let cleared = finish_v2_cancel(&mut *tx, node_id, generation)
        .await
        .map_err(sql_error)?;
    tx.commit().await.map_err(sql_error)?;
    Ok(cleared)
}

/// The artifact a node reported a sandbox evacuated with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EvacuatedArtifact {
    /// A full snapshot of a suspended sandbox: the row is left `suspended`
    /// for its next call to restore it, on another node.
    Full,
    /// A stopped sandbox's disk: the row stays `stopped` at home until its
    /// next start, or the fence, hands it off.
    Disk,
}

/// Record one evacuation the node on `node_id` reported.
///
/// * **full snapshot**: write nothing. The evacuation is recorded once the
///   row reads `suspended` on the reported artifact and the catalog holds a
///   ready, stored full snapshot of it, or once the row has moved on from
///   it: it is homed on another node, or it pins another artifact with a
///   ready, stored full snapshot. A node keeps a suspended sandbox's cache
///   after the sandbox was restored elsewhere, so it can report an artifact
///   the row no longer pins. A row that still pins the reported artifact
///   waits for it, so a sandbox whose only copy is the node's is never
///   recorded before its upload. A row still `started`, `paused` or
///   `suspending` on the node isn't projected yet; one that runs on another
///   node, with an active execution there, has moved on.
/// * **disk**: write nothing either. The evacuation is recorded once the
///   row's disk pin, which the internal API's ingest projects from the
///   node's publication, names the reported disk and that disk is this
///   stop's durable disk
///   ([`pplx_space_sandbox_store::drain_restore::stopped_disk_durable`]:
///   ready, stored and taken since the stop, re-checked here whether or not
///   the row is pinned), or once the row has moved on: gone, out of
///   `stopped`, or no longer at home on the node (handed off, re-placed).
///   The row stays `stopped` at home for its next start, or the fence, to
///   hand off.
///
/// A row that no longer needs anything is [`EvacuationRecord::Recorded`].
pub(crate) async fn record_v2_evacuation(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    kind: EvacuatedArtifact,
    artifact: Uuid,
) -> Result<EvacuationRecord, String> {
    let sql_error = |error: sqlx::Error| error.to_string();
    if kind == EvacuatedArtifact::Disk {
        return record_v2_disk(pool, node_id, sandbox_id, artifact)
            .await
            .map_err(sql_error);
    }
    type Row = (String, Option<NodeId>, Option<NodeId>, Option<Uuid>, bool);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.active_node_id, s.target_node_id, s.current_local_artifact_id,
                EXISTS (
                  SELECT 1 FROM sandbox_k8s_executions e
                   WHERE e.sandbox_id = s.id
                     AND e.active
                     AND e.k8s_node_name IS DISTINCT FROM
                         (SELECT k8s_node_name FROM node_k8s WHERE node_id = $2)
                )
           FROM sandboxes s
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .fetch_optional(pool)
    .await
    .map_err(sql_error)?;
    let Some((lifecycle, active_node_id, target_node_id, pin, executes_elsewhere)) = row else {
        return Ok(EvacuationRecord::Recorded);
    };
    match lifecycle.as_str() {
        "suspended" => {}
        "started" | "paused" | "suspending" if active_node_id == Some(node_id) => {
            return Ok(EvacuationRecord::NotYet(
                "the node's suspend isn't projected onto the row yet".to_owned(),
            ));
        }
        // Running again, on an execution elsewhere: a user's call moved it.
        "started" | "paused" | "suspending" if executes_elsewhere => {
            return Ok(EvacuationRecord::Recorded);
        }
        "started" | "paused" | "suspending" => {
            return Ok(EvacuationRecord::NotYet(
                "the row runs nowhere the control plane knows of yet".to_owned(),
            ));
        }
        _ => return Ok(EvacuationRecord::Recorded),
    }
    let ready = |artifact: Uuid| async move {
        pplx_space_snapshot_store::ready_full_snapshot_of_artifact(
            pool,
            sandbox_id,
            pplx_space_snapshot_model::LocalArtifactId::from_uuid(artifact),
        )
        .await
        .map(|snapshot| snapshot.is_some())
        .map_err(sql_error)
    };
    if pin == Some(artifact) {
        return Ok(if ready(artifact).await? {
            EvacuationRecord::Recorded
        } else {
            EvacuationRecord::NotYet(
                "the catalog doesn't hold the artifact as ready yet".to_owned(),
            )
        });
    }
    // Moved on from the reported artifact: homed on another node, or
    // pinning another artifact that is durable.
    let home = target_node_id.or(active_node_id);
    if home.is_some_and(|home| home != node_id) {
        return Ok(EvacuationRecord::Recorded);
    }
    if let Some(pin) = pin
        && ready(pin).await?
    {
        return Ok(EvacuationRecord::Recorded);
    }
    Ok(EvacuationRecord::NotYet(
        "the row pins another artifact than the one the node reported, not yet durable".to_owned(),
    ))
}

/// [`record_v2_evacuation`] of a stopped sandbox's disk `artifact`: a read.
async fn record_v2_disk(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    artifact: Uuid,
) -> Result<EvacuationRecord, sqlx::Error> {
    static READ: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let durable = pplx_space_sandbox_store::drain_restore::stopped_disk_durable("s");
        sql!(format!(
            "SELECT s.lifecycle = 'stopped' AND s.active_node_id = $2,
                    s.current_local_artifact_kind = 'disk'
                        AND s.current_local_artifact_id = $3,
                    {durable}
               FROM sandboxes s
              WHERE s.id = $1"
        ))
    });
    let row: Option<(Option<bool>, Option<bool>, bool)> =
        sqlx::query_as(sqlx::AssertSqlSafe(READ.as_str()))
            .bind(sandbox_id)
            .bind(node_id)
            .bind(artifact)
            .fetch_optional(pool)
            .await?;
    let Some((at_home, pinned, durable)) = row else {
        return Ok(EvacuationRecord::Recorded);
    };
    if at_home != Some(true) {
        return Ok(EvacuationRecord::Recorded);
    }
    Ok(match (pinned == Some(true), durable) {
        (true, true) => EvacuationRecord::Recorded,
        (false, _) => EvacuationRecord::NotYet(
            "the row's disk pin doesn't name the reported disk yet".to_owned(),
        ),
        (true, false) => EvacuationRecord::NotYet(
            "the pinned disk isn't a ready, stored disk taken since the stop".to_owned(),
        ),
    })
}

fn evacuated_artifact(kind: i32) -> Option<EvacuatedArtifact> {
    use pplx_space_sbx_mgr_client::proto::ArtifactKind;
    match ArtifactKind::try_from(kind).ok()? {
        ArtifactKind::Full => Some(EvacuatedArtifact::Full),
        ArtifactKind::Disk => Some(EvacuatedArtifact::Disk),
        ArtifactKind::Unspecified => None,
    }
}

fn generation_i64(generation: u64) -> Result<i64, String> {
    i64::try_from(generation).map_err(|_| format!("generation {generation} is above i64::MAX"))
}

impl SubstrateSink {
    /// Lift the cordon of the Kubernetes Node `node_name`, as `:uncordon`
    /// does, once its UID is still `stored_uid`, the one registration
    /// recorded: a same-name replacement isn't the drained node's. A Node
    /// that is gone has no cordon to lift.
    async fn uncordon_v2_node(
        &self,
        node_name: String,
        stored_uid: Option<String>,
    ) -> Result<(), String> {
        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.substrate.kubernetes().k8s.client.clone());
        match tokio::time::timeout(NODE_LOOKUP_TIMEOUT, nodes.get_opt(&node_name)).await {
            Ok(Ok(None)) => return Ok(()),
            Ok(Ok(Some(node))) => {
                if let (Some(stored), Some(current)) =
                    (stored_uid.as_deref(), node.metadata.uid.as_deref())
                    && stored != current
                {
                    return Err(format!(
                        "the Kubernetes Node {node_name} is another one than the drained \
                         node registered; not lifting its cordon"
                    ));
                }
            }
            Ok(Err(error)) => return Err(error.to_string()),
            Err(_) => return Err("the Node lookup timed out".to_owned()),
        }
        match tokio::time::timeout(
            NODE_LOOKUP_TIMEOUT,
            self.substrate.kubernetes().set_node_schedulability(
                NodeSchedulabilityRequest {
                    substrate_node_id: &node_name,
                },
                NodeSchedulability::Schedulable,
            ),
        )
        .await
        {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err("lifting the cordon timed out".to_owned()),
        }
    }
}

impl DrainV2Sink for SubstrateSink {
    async fn list_v2_nodes(&self) -> Result<Vec<V2Node>, String> {
        list_v2_nodes(&self.pool)
            .await
            .map_err(|error| error.to_string())
    }

    async fn ensure_cordon(&self, node_id: NodeId) -> Result<(), CordonFailure> {
        NodeDrainSink::ensure_node_unschedulable(self, node_id)
            .await
            .map(|_| ())
            .map_err(|error| match error {
                CordonError::StaleIdentity => CordonFailure::UidMismatch(error.to_string()),
                error => CordonFailure::Other(error.to_string()),
            })
    }

    async fn drain_node(
        &self,
        node_id: NodeId,
        request: DrainNodeRequest,
    ) -> Result<DrainNodeResponse, DrainCallError> {
        let target = node_api_row(&self.pool, node_id)
            .await
            .map_err(|error| {
                DrainCallError::other(format!("node endpoint lookup failed: {error}"))
            })?
            .ok_or_else(|| DrainCallError::other(format!("node {node_id} not found")))?;
        let mut client = connect_internal_node_api_client_for_target(
            self.substrate.kubernetes(),
            &target.as_target(),
            DRAIN_NODE_TIMEOUT,
        )
        .await
        .map_err(|error| DrainCallError::other(error.to_string()))?;
        client
            .drain_node(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| DrainCallError::status(&status))
    }

    async fn record_evacuation(
        &self,
        node_id: NodeId,
        evacuated: &DrainEvacuated,
    ) -> Result<EvacuationRecord, String> {
        let sandbox_id = SandboxId::from_str(&evacuated.sandbox_id)
            .map_err(|error| format!("invalid sandbox id {}: {error}", evacuated.sandbox_id))?;
        let artifact = Uuid::from_str(&evacuated.artifact_id)
            .map_err(|error| format!("invalid artifact id {}: {error}", evacuated.artifact_id))?;
        let kind = evacuated_artifact(evacuated.artifact_kind)
            .ok_or_else(|| format!("unknown artifact kind {}", evacuated.artifact_kind))?;
        record_v2_evacuation(&self.pool, node_id, sandbox_id, kind, artifact).await
    }

    async fn record_report(
        &self,
        node_id: NodeId,
        generation: u64,
        report: StoredV2Report,
        running: Vec<String>,
        evacuated: Vec<String>,
    ) -> Result<bool, String> {
        // Only a sandbox id the count can look up joins the observed set.
        let running: Vec<String> = running
            .into_iter()
            .filter(|sandbox_id| SandboxId::from_str(sandbox_id).is_ok())
            .collect();
        record_v2_report(
            &self.pool,
            node_id,
            generation_i64(generation)?,
            &report,
            &running,
            &evacuated,
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn advance_drained(&self, node_id: NodeId, generation: u64) -> Result<bool, String> {
        advance_v2_drained(&self.pool, node_id, generation_i64(generation)?)
            .await
            .map_err(|error| error.to_string())
    }

    async fn return_drained(
        &self,
        node_id: NodeId,
        generation: u64,
        refused: &str,
    ) -> Result<bool, String> {
        return_v2_drained(&self.pool, node_id, generation_i64(generation)?, refused)
            .await
            .map_err(|error| error.to_string())
    }

    async fn finish_cancel(&self, node_id: NodeId, generation: u64) -> Result<bool, String> {
        finish_v2_cancel_locked(
            &self.pool,
            node_id,
            generation_i64(generation)?,
            |node_name, uid| self.uncordon_v2_node(node_name, uid),
        )
        .await
    }

    async fn evacuated_still_suspended(
        &self,
        after: Duration,
    ) -> Result<HashMap<NodeId, u64>, String> {
        Ok(evacuated_still_suspended(&self.pool, after.as_secs_f64())
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|(node_id, count)| (node_id, u64::try_from(count).unwrap_or(0)))
            .collect())
    }

    async fn record_error(&self, node_id: NodeId, message: &str) {
        if let Err(error) = record_v2_error(&self.pool, node_id, message).await {
            tracing::warn!(node_id = %node_id, %error, "node_drain v2: recording an error failed");
        }
        tracing::warn!(node_id = %node_id, error = message, "node_drain v2");
    }

    async fn fence(&self, node_id: NodeId, generation: u64) -> Result<Fence, String> {
        fence_v2_node(&self.pool, node_id, generation_i64(generation)?)
            .await
            .map_err(|error| error.to_string())
    }

    async fn record_dispatched(
        &self,
        node_id: NodeId,
        generation: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        record_v2_dispatched(
            &self.pool,
            node_id,
            generation_i64(generation)?,
            instance_id,
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn record_terminated_observed(
        &self,
        node_id: NodeId,
        generation: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        record_v2_terminated_observed(
            &self.pool,
            node_id,
            generation_i64(generation)?,
            instance_id,
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn node_exists(&self, k8s_node_name: &str) -> Result<bool, String> {
        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.substrate.kubernetes().k8s.client.clone());
        match tokio::time::timeout(NODE_LOOKUP_TIMEOUT, nodes.get_opt(k8s_node_name)).await {
            Ok(Ok(node)) => Ok(node.is_some()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("the Node lookup timed out".to_owned()),
        }
    }

    async fn node_machine(&self, k8s_node_name: &str) -> Result<NodeMachine, String> {
        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.substrate.kubernetes().k8s.client.clone());
        match tokio::time::timeout(NODE_LOOKUP_TIMEOUT, nodes.get_opt(k8s_node_name)).await {
            Ok(Ok(None)) => Ok(NodeMachine::Gone),
            Ok(Ok(Some(node))) => Ok(NodeMachine::Live(
                node.spec
                    .as_ref()
                    .and_then(|spec| spec.provider_id.as_deref())
                    .and_then(pplx_space_k8s_node_id::Ec2InstanceId::from_provider_id)
                    .map(|instance| instance.as_str().to_owned()),
            )),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("the Node lookup timed out".to_owned()),
        }
    }

    async fn delete_terminated(
        &self,
        node: &V2Node,
        generation: u64,
        fence: &str,
    ) -> Result<TerminatedDelete, String> {
        delete_v2_terminated(&self.pool, node, generation_i64(generation)?, fence)
            .await
            .map_err(|error| error.to_string())
    }

    async fn let_go(&self, node: &V2Node, generation: u64, trace: &str) -> Result<bool, String> {
        let fence = node
            .fence
            .as_deref()
            .filter(|_| node.phase.as_deref() == Some("terminating"));
        let_go_v2(
            &self.pool,
            node.id,
            generation_i64(generation)?,
            node.phase.as_deref(),
            fence,
            trace,
        )
        .await
        .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests;

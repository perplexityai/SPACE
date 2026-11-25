//! [`DrainV2Sink`] for the production sink, [`V2Sink`]: the v2 loop's
//! statements over `nodes`, its evacuation record, and its Kubernetes and
//! node-api calls.

use std::str::FromStr;

use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};

use pplx_space_api::controllers::substrate::connect_internal_node_api_client_for_target;
use pplx_space_sbx_mgr_client::proto::{DrainEvacuated, DrainNodeRequest, DrainNodeResponse};
use uuid::Uuid;

use pplx_space_node_registration_store::drain_v2::{
    LockedV2Node, StoredV2Report, add_v2_evacuated, advance_v2_drained, evacuated_still_suspended,
    finish_v2_cancel, let_go_v2_node, list_v2_node_rows, read_locked_v2_node,
    record_terminate_held, record_v2_dispatched, record_v2_error, record_v2_report,
    record_v2_terminated_observed, return_v2_drained, v2_node_as_listed, write_fence,
};

use super::*;
use crate::node_drain::guarded_delete::{self, GuardedDelete, NodeIdentity};
use crate::node_drain::v2::{
    CordonFailure, DrainCallError, DrainV2Sink, EvacuationRecord, Fence, NodeMachine,
    TerminatedDelete, V2Node,
};

/// How long one `DrainNode` call may take; the node answers without waiting.
const DRAIN_NODE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the direct Kubernetes Node GET may take, and each cordon or
/// lift's patch.
const NODE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);

/// The v2 loop's production sink ([`DrainV2Sink`]): its statements over
/// `nodes` on `pool`, and its Kubernetes and node-api calls through
/// `substrate`. It carries none of v1's drain configuration.
pub struct V2Sink {
    pool: PgPool,
    substrate: Arc<KubernetesSubstrate>,
}

impl V2Sink {
    pub fn new(pool: PgPool, substrate: Arc<KubernetesSubstrate>) -> Self {
        Self { pool, substrate }
    }

    /// Whether `node`'s row still reads as the tick listed it: its v2 drain
    /// in the listed phase, under the listed generation, on the listed
    /// Kubernetes UID and recorded instance ([`v2_node_as_listed`]). The
    /// loop reads it just before each effect on the node.
    async fn still_listed(&self, node: &V2Node) -> Result<bool, String> {
        still_listed(&self.pool, node).await
    }

    fn nodes(&self) -> kube::Api<Node> {
        kube::Api::all(self.substrate.k8s.client.clone())
    }
}

/// [`V2Sink::still_listed`] on `pool`. A node listed without a generation
/// is no drain the loop acts on.
async fn still_listed(pool: &PgPool, node: &V2Node) -> Result<bool, String> {
    let Some(generation) = node.generation else {
        return Ok(false);
    };
    v2_node_as_listed(
        pool,
        node.id,
        node.phase.as_deref(),
        generation_i64(generation)?,
        node.k8s_node_uid.as_deref(),
        node.instance_id.as_deref(),
    )
    .await
    .map_err(|error| format!("re-reading the node's row failed: {error}"))
}

/// What a cordon or its lift does, given what a GET of the Node under the
/// drain's node name answered.
#[derive(Clone, Debug, PartialEq)]
enum NodePatch {
    /// The Node is gone: there is no cordon to set or lift.
    Gone,
    /// The Node under the name is another one than the drain's: its UID
    /// isn't the listed one.
    AnotherNode,
    /// This merge patch, which names the Node's UID, so the API server
    /// refuses it once the Node under the name is another one.
    Patch(serde_json::Value),
}

/// The patch that makes the Node `node` (the GET's answer, `None` for a
/// 404) `unschedulable` or schedulable, when its UID is `listed_uid`, the
/// one the tick listed. A listing without a UID compares nothing; the patch
/// then names the UID the GET read.
fn node_patch(listed_uid: Option<&str>, node: Option<&Node>, unschedulable: bool) -> NodePatch {
    let Some(node) = node else {
        return NodePatch::Gone;
    };
    let current = node.metadata.uid.as_deref();
    if let (Some(listed), Some(current)) = (listed_uid, current)
        && listed != current
    {
        return NodePatch::AnotherNode;
    }
    NodePatch::Patch(match current.or(listed_uid) {
        Some(uid) => serde_json::json!({
            "metadata": {"uid": uid},
            "spec": {"unschedulable": unschedulable},
        }),
        None => serde_json::json!({"spec": {"unschedulable": unschedulable}}),
    })
}

/// Whether the API server refused a patch because the UID it names isn't
/// the UID of the Node under the name. It answers such a merge patch as an
/// invalid change of the immutable `metadata.uid` (422, a cause on that
/// field); a UID precondition conflict (409) means the same.
fn refused_for_another_uid(error: &kube::Error) -> bool {
    let kube::Error::Api(status) = error else {
        return false;
    };
    status.is_conflict()
        || (status.is_invalid()
            && status.details.as_ref().is_some_and(|details| {
                details
                    .causes
                    .iter()
                    .any(|cause| cause.field == "metadata.uid")
            }))
}

/// Why a cordon or its lift didn't converge.
#[derive(Debug)]
enum PatchFailure {
    /// The Node under the name is another one than the drain's.
    AnotherNode(String),
    Other(String),
}

/// Make the Kubernetes Node `node_name` `unschedulable` or schedulable,
/// once its UID is `listed_uid`, the one the tick listed: a GET, the UID
/// compare, then a merge patch that names the UID, each bounded by
/// [`NODE_LOOKUP_TIMEOUT`]. A Node that is gone, at the GET or at the
/// patch, is left as it is.
async fn set_v2_node_schedulability(
    nodes: &kube::Api<Node>,
    node_name: &str,
    listed_uid: Option<&str>,
    unschedulable: bool,
) -> Result<(), PatchFailure> {
    let another = || {
        PatchFailure::AnotherNode(format!(
            "the Kubernetes Node {node_name} is another one than the drained node registered"
        ))
    };
    let node = match tokio::time::timeout(NODE_LOOKUP_TIMEOUT, nodes.get_opt(node_name)).await {
        Ok(Ok(node)) => node,
        Ok(Err(error)) => return Err(PatchFailure::Other(error.to_string())),
        Err(_) => return Err(PatchFailure::Other("the Node lookup timed out".to_owned())),
    };
    let patch = match node_patch(listed_uid, node.as_ref(), unschedulable) {
        NodePatch::Gone => return Ok(()),
        NodePatch::AnotherNode => return Err(another()),
        NodePatch::Patch(patch) => patch,
    };
    match tokio::time::timeout(
        NODE_LOOKUP_TIMEOUT,
        nodes.patch(node_name, &PatchParams::default(), &Patch::Merge(&patch)),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        // Deleted between the GET and the PATCH: gone, as a GET finds it.
        Ok(Err(kube::Error::Api(status))) if status.is_not_found() => Ok(()),
        Ok(Err(error)) if refused_for_another_uid(&error) => Err(another()),
        Ok(Err(error)) => Err(PatchFailure::Other(error.to_string())),
        Err(_) => Err(PatchFailure::Other("the Node patch timed out".to_owned())),
    }
}

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
    if !guarded_delete::lock_node(&mut **tx, node_id).await? {
        return Ok(None);
    }
    read_locked_v2_node(&mut **tx, node_id)
        .await
        .map_err(sqlx::Error::from)
}

/// Add every row still naming the `drained` v2 node under `generation` that
/// the terminate would release to the drain's `evacuated_total`: its
/// suspended rows whose state a durable snapshot holds and whose restore or
/// activation isn't in flight
/// ([`pplx_space_sandbox_store::drain_terminate::releasable_suspended_rows_naming`]).
/// A v2 drain never moves a stopped sandbox, so none is counted. It takes
/// no row lock: the read is a plain read, and the write re-checks that the
/// row is still that generation's `drained` node
/// ([`add_v2_evacuated`]). A `drain_status` write only, made only when a row
/// isn't counted yet; no sandbox row is written. Returns the ids it found,
/// which a row no longer that generation's `drained` node didn't add.
pub(crate) async fn count_v2_drained_evacuations(
    pool: &PgPool,
    node_id: NodeId,
    generation: i64,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let releasable = pplx_space_sandbox_store::drain_terminate::releasable_suspended_rows_naming(
        pool,
        node_id,
        execution_ownership_ttl_secs(),
    )
    .await?;
    // The node evicts a durable copy on its own, and one it evicted before a
    // report named it evacuated leaves the report: these rows are the
    // count's only witness of it.
    if !releasable.is_empty() {
        let ids: Vec<String> = releasable.iter().map(ToString::to_string).collect();
        add_v2_evacuated(pool, node_id, generation, "drained", &ids).await?;
    }
    Ok(releasable)
}

/// The terminate transaction of the v2 node under `generation`, under its
/// row lock, once the terminating tick's fresh check found nothing pending
/// or blocked: it must still be that generation's `terminating` node,
/// unfenced. It then cleans up the references of the rows that no longer
/// need the node, the only sandbox-row writes the v2 loop makes: the
/// durable suspended rows naming it are released
/// ([`pplx_space_sandbox_store::drain_terminate::release_durable_suspended`]),
/// each with its execution on the node retired, and added to the drain's
/// `evacuated_total`, so a row the node evicted before any report or
/// drained tick named it is counted too. Then nothing may name the
/// node any more (the require-empty predicate,
/// [`pplx_space_sandbox_store::drain_terminate::node_references`]), and
/// `drain_status.terminate_instance` records `node_k8s.instance_id`, the
/// instance to terminate. A refusal of the predicate records what holds the
/// node in `drain_last_error` and commits the cleanup, the count included. A v2 drain never
/// moves a stopped sandbox: the node reports one pending, so the fresh check
/// never lets this run while the node holds one, and a stopped row that
/// still names the node holds it like any other reference.
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
    let released = pplx_space_sandbox_store::drain_terminate::release_durable_suspended(
        &mut *tx,
        node_id,
        node.k8s_node_name.as_deref(),
        execution_ownership_ttl_secs(),
    )
    .await?;
    if !released.is_empty() {
        let ids: Vec<String> = released.iter().map(ToString::to_string).collect();
        add_v2_evacuated(&mut *tx, node_id, generation, "terminating", &ids).await?;
    }
    let references = pplx_space_sandbox_store::drain_terminate::node_references(
        &mut *tx,
        node_id,
        node.k8s_node_name.as_deref(),
        guarded_delete::REFERENCE_SAMPLE,
    )
    .await?;
    if !references.is_empty() {
        let held = format!("terminate held: {references}");
        record_terminate_held(&mut *tx, node_id, &held).await?;
        tx.commit().await?;
        return Ok(Fence::Held(held));
    }
    write_fence(&mut *tx, node_id, &instance_id).await?;
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
    if !guarded_delete::lock_node(&mut *tx, node_id).await? {
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

/// Finish the cancel of the undrained v2 drain of `node` under the
/// generation the tick listed, once the node answered the cancel: while the
/// row still reads as the tick listed it, lift the node's cordon with
/// `uncordon` (passed the Node's name and the UID the tick listed), then
/// clear the v2 columns with the cancel's guarded statement, which matches
/// only while the row is still that generation's cancel (a v2 engine with
/// no phase). No row lock spans the lift. While one controller runs ticks,
/// the API refuses a `:drain` until the clear lands, so no drain of the
/// node starts and cordons in between; a lift from an outgoing
/// controller's last tick can still land after a new drain's cordon, which
/// that drain's next tick restores.
/// Returns whether the columns were cleared; a lift that fails clears
/// nothing, since no tick lists the row once its engine is cleared.
pub(crate) async fn finish_v2_cancel_after_lift<F, Fut>(
    pool: &PgPool,
    node: &V2Node,
    uncordon: F,
) -> Result<bool, String>
where
    F: FnOnce(String, Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    // A listing with a phase isn't a cancel, and the generation the
    // re-read compares only changes with a new drain's start.
    let Some(generation) = node.generation else {
        return Ok(false);
    };
    if node.phase.is_some() || !still_listed(pool, node).await? {
        return Ok(false);
    }
    let generation = generation_i64(generation)?;
    let Some(node_name) = node.k8s_node_name.clone() else {
        return Err("the node has no Kubernetes name whose cordon to lift".to_owned());
    };
    uncordon(node_name, node.k8s_node_uid.clone()).await?;
    finish_v2_cancel(pool, node.id, generation)
        .await
        .map_err(|error| sqlx::Error::from(error).to_string())
}

/// Why a stopped sandbox's disk reported evacuated isn't recorded.
pub(crate) const DISK_NOT_RECORDED: &str =
    "a stopped sandbox's disk: drain v2 doesn't move stopped sandboxes";

/// Record one evacuation the node on `node_id` reported, `evacuated`: a
/// full snapshot through [`record_v2_evacuation`]. A v2 drain doesn't move a
/// stopped sandbox, so a stopped sandbox's disk is never recorded
/// ([`DISK_NOT_RECORDED`]): it stays unrecorded, and the drain isn't
/// finished. No current node build reports one, so it is logged as an
/// unexpected entry, on each tick that reads it.
pub(crate) async fn record_v2_evacuated(
    pool: &PgPool,
    node_id: NodeId,
    evacuated: &DrainEvacuated,
) -> Result<EvacuationRecord, String> {
    use pplx_space_sbx_mgr_client::proto::ArtifactKind;
    let sandbox_id = SandboxId::from_str(&evacuated.sandbox_id)
        .map_err(|error| format!("invalid sandbox id {}: {error}", evacuated.sandbox_id))?;
    match ArtifactKind::try_from(evacuated.artifact_kind) {
        Ok(ArtifactKind::Full) => {}
        Ok(ArtifactKind::Disk) => {
            tracing::warn!(
                node_id = %node_id,
                sandbox_id = %sandbox_id,
                artifact_id = evacuated.artifact_id,
                "node_drain v2: unexpected evacuated entry: the node reported a stopped \
                 sandbox's disk, which a v2 drain doesn't move; not recording it"
            );
            return Ok(EvacuationRecord::NotYet(DISK_NOT_RECORDED.to_owned()));
        }
        Ok(ArtifactKind::Unspecified) | Err(_) => {
            return Err(format!("unknown artifact kind {}", evacuated.artifact_kind));
        }
    }
    let artifact = Uuid::from_str(&evacuated.artifact_id)
        .map_err(|error| format!("invalid artifact id {}: {error}", evacuated.artifact_id))?;
    record_v2_evacuation(pool, node_id, sandbox_id, artifact).await
}

/// Record one full-snapshot evacuation the node on `node_id` reported:
/// recorded once the sandbox no longer needs the node, by the sandbox
/// store's one default-deny answer
/// ([`pplx_space_sandbox_store::drain_terminate::evacuation_verdict`]), and
/// not yet, with why, while it may. Writes nothing. A `stopped` row still
/// homed on the node is never recorded
/// ([`pplx_space_sandbox_store::drain_terminate::evacuation_verdict::STOPPED_HELD`]): a v2
/// drain doesn't move it, so the drain stays unfinished with that reason
/// rather than reaching `drained` and being held at the fence.
pub(crate) async fn record_v2_evacuation(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    artifact: Uuid,
) -> Result<EvacuationRecord, String> {
    use pplx_space_sandbox_store::drain_terminate::evacuation_verdict::{
        EvacuationVerdict, evacuation_verdict,
    };
    match evacuation_verdict(pool, node_id, sandbox_id, artifact)
        .await
        .map_err(|error| sqlx::Error::from(error).to_string())?
    {
        EvacuationVerdict::Released(_) => Ok(EvacuationRecord::Recorded),
        EvacuationVerdict::Holds(reason) => Ok(EvacuationRecord::NotYet(reason)),
    }
}

fn generation_i64(generation: u64) -> Result<i64, String> {
    i64::try_from(generation).map_err(|_| format!("generation {generation} is above i64::MAX"))
}

impl DrainV2Sink for V2Sink {
    async fn list_v2_nodes(&self) -> Result<Vec<V2Node>, String> {
        list_v2_nodes(&self.pool)
            .await
            .map_err(|error| error.to_string())
    }

    async fn ensure_cordon(&self, node: &V2Node) -> Result<(), CordonFailure> {
        if !self
            .still_listed(node)
            .await
            .map_err(CordonFailure::Other)?
        {
            return Err(CordonFailure::Moved);
        }
        let Some(node_name) = node.k8s_node_name.as_deref() else {
            return Err(CordonFailure::Other(format!(
                "node {} has no Kubernetes name to cordon",
                node.id
            )));
        };
        set_v2_node_schedulability(&self.nodes(), node_name, node.k8s_node_uid.as_deref(), true)
            .await
            .map_err(|failure| match failure {
                PatchFailure::AnotherNode(error) => CordonFailure::UidMismatch(error),
                PatchFailure::Other(error) => CordonFailure::Other(error),
            })
    }

    async fn drain_node(
        &self,
        node: &V2Node,
        request: DrainNodeRequest,
    ) -> Result<DrainNodeResponse, DrainCallError> {
        if !self
            .still_listed(node)
            .await
            .map_err(DrainCallError::other)?
        {
            return Err(DrainCallError::moved());
        }
        let node_id = node.id;
        let target = node_api_row(&self.pool, node_id)
            .await
            .map_err(|error| {
                DrainCallError::other(format!("node endpoint lookup failed: {error}"))
            })?
            .ok_or_else(|| DrainCallError::other(format!("node {node_id} not found")))?;
        let mut client = connect_internal_node_api_client_for_target(
            self.substrate.as_ref(),
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
        record_v2_evacuated(&self.pool, node_id, evacuated).await
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

    async fn finish_cancel(&self, node: &V2Node) -> Result<bool, String> {
        let nodes = self.nodes();
        finish_v2_cancel_after_lift(&self.pool, node, |node_name, uid| async move {
            set_v2_node_schedulability(&nodes, &node_name, uid.as_deref(), false)
                .await
                .map_err(|failure| match failure {
                    PatchFailure::AnotherNode(error) => {
                        format!("{error}; not lifting its cordon")
                    }
                    PatchFailure::Other(error) => error,
                })
        })
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

    async fn count_drained_evacuations(
        &self,
        node_id: NodeId,
        generation: u64,
    ) -> Result<Vec<SandboxId>, String> {
        count_v2_drained_evacuations(&self.pool, node_id, generation_i64(generation)?)
            .await
            .map_err(|error| error.to_string())
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
        let nodes = self.nodes();
        match tokio::time::timeout(NODE_LOOKUP_TIMEOUT, nodes.get_opt(k8s_node_name)).await {
            Ok(Ok(node)) => Ok(node.is_some()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("the Node lookup timed out".to_owned()),
        }
    }

    async fn node_machine(&self, k8s_node_name: &str) -> Result<NodeMachine, String> {
        let nodes = self.nodes();
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

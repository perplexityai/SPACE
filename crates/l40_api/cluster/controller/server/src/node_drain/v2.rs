//! The node drain's v2 loop.
//!
//! A v2 drain is a forced drain the admin route started with `engine: v2`
//! (`nodes.drain_engine = 'v2'`), which wrote the epoch the node is sealed
//! under (`drain_status.epoch`) in the transaction that moved the phase,
//! before any node call. The node does the work: sandbox-manager's
//! `DrainNode` seals it and makes every resident durable elsewhere. This loop
//! asks it, each tick, and records what it reports:
//!
//! - **`draining` and `drained`**: reconcile the cordon, call `DrainNode`
//!   under the epoch, and for each sandbox the node reports evacuated,
//!   enqueue the drain restore operation that brings a suspended one back to
//!   its prior lifecycle elsewhere, in one transaction and at most once per
//!   snapshot, or release a stopped one from the node with its disk pinned,
//!   for its next start or wake to materialize on any node. The report is stored in
//!   `drain_status`. A `draining` node moves to `drained` once the report is
//!   `done` under the row's own epoch and every evacuation is recorded.
//! - **`terminating`**: `:terminate` moved it here, gated on a fresh `done`
//!   report under the row's epoch. Terminate it through the cluster's
//!   terminator, then flag that it did (`drain_status.terminated`), so it is
//!   not terminated again. The row stays, with no tombstone: the dead-node
//!   verb, which a v2 cluster runs `live` with EC2 as its authority, judges
//!   the node by the ordinary evidence (heartbeat stale, Node gone, instance
//!   terminated), reconciles what is left on it and deletes the row
//!   (`dead_node`).
//! - **undrained** (no phase, `drain_status.cancel`): cancel the epoch on the
//!   node until it is unsealed, then clear the v2 columns.
//!
//! Every v1 writer keyed on drain phase skips a v2 row, so this loop is the
//! only one that moves one.

use std::time::Duration;

use pplx_space_common_core::types::NodeId;
use pplx_space_sbx_mgr_client::proto::{DrainEvacuated, DrainNodeResponse, DrainPrior};
use tokio_util::sync::CancellationToken;

use super::{Ec2InstanceTerminator, InstanceTerminator, TerminateError};

/// The v2 loop's terminator: EC2, where a terminated node's row has an
/// owner, or a refusal that leaves the node `terminating`.
pub enum V2Terminator {
    Ec2(Ec2InstanceTerminator),
    Refuse,
}

impl InstanceTerminator for V2Terminator {
    async fn terminate(&self, node_id: NodeId) -> Result<(), TerminateError> {
        match self {
            Self::Ec2(terminator) => terminator.terminate(node_id).await,
            Self::Refuse => Err(TerminateError::Provider(
                "a v2 node is terminated only through the aws terminator with the dead-node \
                 verb live, which deletes its row"
                    .to_owned(),
            )),
        }
    }
}

/// A v2 drain's node, as the loop reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V2Node {
    pub id: NodeId,
    pub phase: Option<String>,
    /// `drain_status.epoch`; the admin route writes it with the phase.
    pub epoch: Option<u64>,
    /// `drain_status.cancel`: the drain was undrained.
    pub cancel: bool,
    /// `drain_status.terminated`: the terminator acted on the node.
    pub terminated: bool,
}

/// What recording one reported evacuation found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvacuationRecord {
    /// Nothing more to do for it: its restore is enqueued or its stopped row
    /// released, by this call or an earlier one, or the sandbox has moved on.
    Recorded,
    /// Not yet, and why: the catalog doesn't hold the artifact as ready, the
    /// node's suspend isn't projected onto the row yet, or the row can't
    /// take the report.
    NotYet(String),
}

impl EvacuationRecord {
    pub const fn is_recorded(&self) -> bool {
        matches!(self, Self::Recorded)
    }
}

/// What the v2 loop reads and writes.
pub trait DrainV2Sink: Send + Sync + 'static {
    fn list_v2_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<V2Node>, String>> + Send;
    /// Converge the node's cordon, as the v1 drain does.
    fn ensure_cordon(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn drain_node(
        &self,
        node_id: NodeId,
        epoch: u64,
        cancel: bool,
    ) -> impl std::future::Future<Output = Result<DrainNodeResponse, String>> + Send;
    fn record_evacuation(
        &self,
        node_id: NodeId,
        evacuated: &DrainEvacuated,
    ) -> impl std::future::Future<Output = Result<EvacuationRecord, String>> + Send;
    /// Store `report` under `epoch`; false when the row's epoch moved on.
    fn record_report(
        &self,
        node_id: NodeId,
        epoch: u64,
        report: serde_json::Value,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// `draining -> drained` under `epoch`, with a stored `done` report for
    /// it.
    fn advance_drained(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Clear an undrained row's v2 columns once its epoch is cancelled.
    fn finish_cancel(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    fn record_error(
        &self,
        node_id: NodeId,
        message: &str,
    ) -> impl std::future::Future<Output = ()> + Send;
    /// Flag that the terminator acted on `node_id` under `epoch`; false when
    /// the row is no longer that epoch's terminating v2 node.
    fn record_terminated(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
}

/// What one tick did, for tests and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct V2TickReport {
    pub drained: Vec<NodeId>,
    pub cancelled: Vec<NodeId>,
    pub terminated: Vec<NodeId>,
}

/// The JSON `drain_status.report` holds: the node's answer and what the loop
/// recorded of it.
pub fn report_json(
    response: &DrainNodeResponse,
    records: &[(String, EvacuationRecord)],
) -> serde_json::Value {
    let prior = |value: i32| match DrainPrior::try_from(value) {
        Ok(DrainPrior::Started) => "started",
        Ok(DrainPrior::Paused) => "paused",
        Ok(DrainPrior::Stopped) => "stopped",
        _ => "unknown",
    };
    serde_json::json!({
        "sealed_epoch": response.sealed_epoch,
        "done": response.done,
        "evacuated": response.evacuated.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "prior": prior(entry.prior),
            "artifact_id": entry.artifact_id,
        })).collect::<Vec<_>>(),
        "pending": response.pending.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "action": entry.action,
        })).collect::<Vec<_>>(),
        "blocked": response.blocked.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "reason": entry.reason,
        })).collect::<Vec<_>>(),
        "unrecorded": records.iter().filter_map(|(sandbox_id, record)| match record {
            EvacuationRecord::NotYet(reason) => Some(serde_json::json!({
                "sandbox_id": sandbox_id,
                "reason": reason,
            })),
            _ => None,
        }).collect::<Vec<_>>(),
    })
}

/// How many v2 nodes one tick calls at once: each call waits on its node,
/// and a slow node must not hold up the rest.
const NODE_CONCURRENCY: usize = 8;

/// What one tick did to a node.
enum NodeStep {
    Cancelled,
    Drained,
    Terminated,
}

/// One pass over every v2 node, [`NODE_CONCURRENCY`] at a time.
pub async fn tick<S: DrainV2Sink, T: InstanceTerminator>(sink: &S, terminator: &T) -> V2TickReport {
    use futures_util::StreamExt as _;

    let mut report = V2TickReport::default();
    let nodes = match sink.list_v2_nodes().await {
        Ok(nodes) => nodes,
        Err(error) => {
            tracing::warn!(%error, "node_drain v2: listing v2 nodes failed");
            return report;
        }
    };
    let steps: Vec<_> = futures_util::stream::iter(nodes)
        .map(|node| async move { (node.id, step(sink, terminator, &node).await) })
        .buffered(NODE_CONCURRENCY)
        .collect()
        .await;
    for (node_id, step) in steps {
        match step {
            Some(NodeStep::Cancelled) => report.cancelled.push(node_id),
            Some(NodeStep::Drained) => report.drained.push(node_id),
            Some(NodeStep::Terminated) => report.terminated.push(node_id),
            None => {}
        }
    }
    report
}

async fn step<S: DrainV2Sink, T: InstanceTerminator>(
    sink: &S,
    terminator: &T,
    node: &V2Node,
) -> Option<NodeStep> {
    let Some(epoch) = node.epoch else {
        // The admin route writes the epoch with the phase; a row without
        // one gets no call, sealed under nothing.
        tracing::warn!(node_id = %node.id, "node_drain v2: drain_status has no epoch");
        return None;
    };
    match node.phase.as_deref() {
        None if node.cancel => cancel(sink, node.id, epoch)
            .await
            .then_some(NodeStep::Cancelled),
        None => None,
        Some("draining") | Some("drained") => {
            drain(sink, node, epoch).await.then_some(NodeStep::Drained)
        }
        // Terminated already: the dead-node verb owns it now.
        Some("terminating") if node.terminated => None,
        Some("terminating") => terminate(sink, terminator, node.id, epoch)
            .await
            .then_some(NodeStep::Terminated),
        Some(other) => {
            tracing::warn!(node_id = %node.id, phase = other, "node_drain v2: unknown phase");
            None
        }
    }
}

/// Ask the node, record what it reports, and advance a `draining` node that
/// is done. Returns whether the node moved to `drained`.
async fn drain<S: DrainV2Sink>(sink: &S, node: &V2Node, epoch: u64) -> bool {
    if let Err(error) = sink.ensure_cordon(node.id).await {
        sink.record_error(node.id, &format!("cordon convergence failed: {error}"))
            .await;
    }
    let response = match sink.drain_node(node.id, epoch, false).await {
        Ok(response) => response,
        Err(error) => {
            crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
            sink.record_error(node.id, &format!("DrainNode failed: {error}"))
                .await;
            return false;
        }
    };
    let mut records = Vec::with_capacity(response.evacuated.len());
    for evacuated in &response.evacuated {
        let record = match sink.record_evacuation(node.id, evacuated).await {
            Ok(record) => record,
            Err(error) => EvacuationRecord::NotYet(format!("recording failed: {error}")),
        };
        records.push((evacuated.sandbox_id.clone(), record));
    }
    let all_recorded = records.iter().all(|(_, record)| record.is_recorded());
    let unrecorded = records
        .iter()
        .filter(|(_, record)| !record.is_recorded())
        .count();
    crate::metrics::node_drain::v2_report(
        &node.id.to_string(),
        response.blocked.len() as u64,
        (response.pending.len() + unrecorded) as u64,
    );
    if !response.blocked.is_empty() {
        tracing::warn!(
            node_id = %node.id,
            blocked = response.blocked.len(),
            "node_drain v2: the node reports residents it can't make durable; the drain \
             can't end until they are resolved"
        );
    }
    match sink
        .record_report(node.id, epoch, report_json(&response, &records))
        .await
    {
        Ok(true) => {}
        Ok(false) => return false,
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("storing the drain report failed: {error}"),
            )
            .await;
            return false;
        }
    }
    if node.phase.as_deref() != Some("draining")
        || !response.done
        || response.sealed_epoch != Some(epoch)
        || !all_recorded
    {
        return false;
    }
    match sink.advance_drained(node.id, epoch).await {
        Ok(moved) => moved,
        Err(error) => {
            sink.record_error(node.id, &format!("advancing to drained failed: {error}"))
                .await;
            false
        }
    }
}

/// Cancel an undrained node's epoch, then clear its v2 columns once the node
/// no longer holds the seal. Returns whether the columns were cleared.
async fn cancel<S: DrainV2Sink>(sink: &S, node_id: NodeId, epoch: u64) -> bool {
    match sink.drain_node(node_id, epoch, true).await {
        Ok(response) if response.sealed_epoch != Some(epoch) => {
            match sink.finish_cancel(node_id, epoch).await {
                Ok(cleared) => cleared,
                Err(error) => {
                    sink.record_error(node_id, &format!("clearing the cancel failed: {error}"))
                        .await;
                    false
                }
            }
        }
        Ok(_) => false,
        Err(error) => {
            sink.record_error(node_id, &format!("DrainNode cancel failed: {error}"))
                .await;
            false
        }
    }
}

/// Terminate a v2 node and flag it. Returns whether the flag was written.
async fn terminate<S: DrainV2Sink, T: InstanceTerminator>(
    sink: &S,
    terminator: &T,
    node_id: NodeId,
    epoch: u64,
) -> bool {
    match terminator.terminate(node_id).await {
        Ok(()) => match sink.record_terminated(node_id, epoch).await {
            Ok(recorded) => recorded,
            Err(error) => {
                sink.record_error(
                    node_id,
                    &format!("recording the termination failed: {error}"),
                )
                .await;
                false
            }
        },
        Err(TerminateError::StaleIdentity) => false,
        Err(error) => {
            sink.record_error(node_id, &format!("terminate failed: {error}"))
                .await;
            false
        }
    }
}

/// Run the v2 loop every `tick_every` until `shutdown`.
pub async fn run<S: DrainV2Sink, T: InstanceTerminator>(
    sink: S,
    terminator: T,
    tick_every: Duration,
    shutdown: CancellationToken,
) {
    tracing::info!(
        tick_secs = tick_every.as_secs(),
        "starting the node drain's v2 loop"
    );
    let mut interval = tokio::time::interval(tick_every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = interval.tick() => {}
        }
        let report = tick(&sink, &terminator).await;
        if report != V2TickReport::default() {
            tracing::info!(?report, "node_drain v2: tick");
        }
    }
    tracing::info!("node drain's v2 loop stopping");
}

#[cfg(test)]
mod tests;

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
//!   node, and take back what the drain evacuated: the node hands back each
//!   sandbox the drain suspended, the loop records it as a drain records it
//!   (enqueueing the restore to its prior lifecycle, which may land on the
//!   node again), and the node forgets the ones recorded. Once the node is
//!   unsealed with nothing left to hand back, clear the v2 columns.
//!
//! An evacuation the loop can't record for [`UNRECORDED_BOUND`] is reported
//! `blocked`, as the node reports work that doesn't settle.
//!
//! Every v1 writer keyed on drain phase skips a v2 row, so this loop is the
//! only one that moves one.

use std::collections::BTreeMap;
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
    /// Since when, in Unix seconds, each evacuation the last stored report
    /// names unrecorded has been.
    pub unrecorded_since: BTreeMap<String, i64>,
}

/// How long an evacuation may stay unrecorded before the report names it
/// `blocked`: the node's own bound on a transition it doesn't drive.
pub const UNRECORDED_BOUND: Duration = Duration::from_secs(15 * 60);

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
    /// `forget`, with `cancel`: the evacuations recorded since a cancel call
    /// reported them, for the node to forget.
    fn drain_node(
        &self,
        node_id: NodeId,
        epoch: u64,
        cancel: bool,
        forget: &[String],
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

/// What the loop recorded of one report: each evacuation's record, and
/// since when each one still unrecorded has been, in Unix seconds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recording {
    pub records: Vec<(String, EvacuationRecord)>,
    pub since: BTreeMap<String, i64>,
    pub now: i64,
}

impl Recording {
    /// The unrecorded evacuations, with their reason and since when.
    fn unrecorded(&self) -> impl Iterator<Item = (&str, &str, i64)> {
        self.records
            .iter()
            .filter_map(|(sandbox_id, record)| match record {
                EvacuationRecord::NotYet(reason) => Some((
                    sandbox_id.as_str(),
                    reason.as_str(),
                    self.since.get(sandbox_id).copied().unwrap_or(self.now),
                )),
                EvacuationRecord::Recorded => None,
            })
    }

    /// The unrecorded evacuations past [`UNRECORDED_BOUND`], with why.
    fn overdue(&self) -> Vec<(&str, String)> {
        let bound = i64::try_from(UNRECORDED_BOUND.as_secs()).unwrap_or(i64::MAX);
        self.unrecorded()
            .filter(|(_, _, since)| self.now.saturating_sub(*since) >= bound)
            .map(|(sandbox_id, reason, _)| {
                (
                    sandbox_id,
                    format!(
                        "not recorded for over {} minutes: {reason}",
                        UNRECORDED_BOUND.as_secs() / 60
                    ),
                )
            })
            .collect()
    }

    fn all_recorded(&self) -> bool {
        self.records.iter().all(|(_, record)| record.is_recorded())
    }
}

/// The JSON `drain_status.report` holds: the node's answer and what the loop
/// recorded of it. An evacuation unrecorded past [`UNRECORDED_BOUND`] is
/// `blocked` as well.
pub fn report_json(response: &DrainNodeResponse, recording: &Recording) -> serde_json::Value {
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
        })).chain(recording.overdue().into_iter().map(|(sandbox_id, reason)| serde_json::json!({
            "sandbox_id": sandbox_id,
            "reason": reason,
        }))).collect::<Vec<_>>(),
        "unrecorded": recording.unrecorded().map(|(sandbox_id, reason, since)| serde_json::json!({
            "sandbox_id": sandbox_id,
            "reason": reason,
            "since": since,
        })).collect::<Vec<_>>(),
    })
}

/// Unix seconds now, the clock `Recording::since` is kept in.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}

/// Record each evacuation `response` reports, carrying over since when each
/// unrecorded one has been from the node's last stored report.
async fn record_evacuations<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    response: &DrainNodeResponse,
) -> Recording {
    let now = unix_now();
    let mut recording = Recording {
        now,
        ..Recording::default()
    };
    for evacuated in &response.evacuated {
        let record = match sink.record_evacuation(node.id, evacuated).await {
            Ok(record) => record,
            Err(error) => EvacuationRecord::NotYet(format!("recording failed: {error}")),
        };
        if !record.is_recorded() {
            recording.since.insert(
                evacuated.sandbox_id.clone(),
                node.unrecorded_since
                    .get(&evacuated.sandbox_id)
                    .copied()
                    .unwrap_or(now),
            );
        }
        recording
            .records
            .push((evacuated.sandbox_id.clone(), record));
    }
    recording
}

/// Publish what `response` and the loop's `recording` of it leave to do:
/// an evacuation unrecorded past the bound is blocked, one within it pending.
fn publish_report(node: &V2Node, response: &DrainNodeResponse, recording: &Recording) {
    let unrecorded = recording.unrecorded().count();
    let overdue = recording.overdue().len();
    crate::metrics::node_drain::v2_report(
        &node.id.to_string(),
        (response.blocked.len() + overdue) as u64,
        (response.pending.len() + unrecorded - overdue) as u64,
    );
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
        None if node.cancel => cancel(sink, node, epoch)
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
    let cordon_error = sink
        .ensure_cordon(node.id)
        .await
        .err()
        .map(|error| format!("cordon convergence failed: {error}"));
    if let Some(error) = &cordon_error {
        sink.record_error(node.id, error).await;
    }
    let response = match sink.drain_node(node.id, epoch, false, &[]).await {
        Ok(response) => response,
        Err(error) => {
            crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
            sink.record_error(node.id, &format!("DrainNode failed: {error}"))
                .await;
            return false;
        }
    };
    let recording = record_evacuations(sink, node, &response).await;
    publish_report(node, &response, &recording);
    let all_recorded = recording.all_recorded();
    if !response.blocked.is_empty() {
        tracing::warn!(
            node_id = %node.id,
            blocked = response.blocked.len(),
            "node_drain v2: the node reports residents it can't make durable; the drain \
             can't end until they are resolved"
        );
    }
    match sink
        .record_report(node.id, epoch, report_json(&response, &recording))
        .await
    {
        // Storing the report clears the last error; a cordon that failed
        // this tick keeps its error until one succeeds.
        Ok(true) => {
            if let Some(error) = &cordon_error {
                sink.record_error(node.id, error).await;
            }
        }
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

/// Cancel an undrained node's epoch and take back what the drain
/// evacuated: record each sandbox the node hands back, which enqueues its
/// restore, and have the node forget the ones recorded. Clear the v2 columns
/// once the node no longer holds the seal and has nothing left to hand back.
/// Returns whether the columns were cleared.
async fn cancel<S: DrainV2Sink>(sink: &S, node: &V2Node, epoch: u64) -> bool {
    let call = |forget: Vec<String>| async move {
        let answer = sink.drain_node(node.id, epoch, true, &forget).await;
        if let Err(error) = &answer {
            crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
            sink.record_error(node.id, &format!("DrainNode cancel failed: {error}"))
                .await;
        }
        answer.ok()
    };
    let Some(response) = call(Vec::new()).await else {
        return false;
    };
    if response.sealed_epoch == Some(epoch) {
        return false;
    }
    let recording = record_evacuations(sink, node, &response).await;
    let recorded: Vec<String> = recording
        .records
        .iter()
        .filter(|(_, record)| record.is_recorded())
        .map(|(sandbox_id, _)| sandbox_id.clone())
        .collect();
    let response = if recorded.is_empty() {
        response
    } else {
        let Some(response) = call(recorded).await else {
            return false;
        };
        response
    };
    match sink
        .record_report(node.id, epoch, report_json(&response, &recording))
        .await
    {
        Ok(true) => {}
        Ok(false) => return false,
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("storing the cancel's report failed: {error}"),
            )
            .await;
            return false;
        }
    }
    // An earlier node answers a cancel with none of these lists: it has
    // nothing to hand back.
    let handed_back =
        response.evacuated.is_empty() && response.pending.is_empty() && response.blocked.is_empty();
    if !handed_back {
        publish_report(node, &response, &recording);
        return false;
    }
    match sink.finish_cancel(node.id, epoch).await {
        Ok(cleared) => {
            if cleared {
                crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            }
            cleared
        }
        Err(error) => {
            sink.record_error(node.id, &format!("clearing the cancel failed: {error}"))
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
        Err(TerminateError::StaleIdentity) => {
            tracing::warn!(
                node_id = %node_id,
                "node_drain v2: the terminator found the node's identity stale; the next \
                 tick terminates it again"
            );
            false
        }
        Err(error) => {
            crate::metrics::node_drain::v2_terminate_failure(&node_id.to_string());
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

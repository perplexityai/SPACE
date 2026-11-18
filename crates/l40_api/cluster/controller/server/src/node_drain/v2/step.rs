//! The per-node step: what one tick does to a node, and the drain call.

use super::*;

/// How long an evacuation may stay unrecorded before the report names it
/// `blocked`: the node's own bound on a transition it doesn't drive.
pub const UNRECORDED_BOUND: Duration = Duration::from_secs(15 * 60);

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
/// `blocked` as well, unless the node already reports its sandbox blocked.
pub fn report_json(response: &DrainNodeResponse, recording: &Recording) -> serde_json::Value {
    let prior = |value: i32| match DrainPrior::try_from(value) {
        Ok(DrainPrior::Started) => "started",
        Ok(DrainPrior::Paused) => "paused",
        Ok(DrainPrior::Stopped) => "stopped",
        _ => "unknown",
    };
    serde_json::json!({
        "sealed_epoch": response.sealed_epoch,
        "gentle": response.gentle_epoch.is_some(),
        "done": response.done,
        "evacuated": response.evacuated.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "prior": prior(entry.prior),
            "artifact_id": entry.artifact_id,
        })).collect::<Vec<_>>(),
        "pending": response.pending.iter().map(|entry| if entry.reason.is_empty() {
            serde_json::json!({
                "sandbox_id": entry.sandbox_id,
                "action": entry.action,
            })
        } else {
            serde_json::json!({
                "sandbox_id": entry.sandbox_id,
                "action": entry.action,
                "reason": entry.reason,
            })
        }).collect::<Vec<_>>(),
        "blocked": response.blocked.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "reason": entry.reason,
        })).chain(overdue_unblocked(response, recording).map(|(sandbox_id, reason)| serde_json::json!({
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

/// The evacuations unrecorded past the bound whose sandbox the node doesn't
/// already report blocked, with why.
fn overdue_unblocked<'a>(
    response: &'a DrainNodeResponse,
    recording: &'a Recording,
) -> impl Iterator<Item = (&'a str, String)> {
    recording.overdue().into_iter().filter(|(sandbox_id, _)| {
        !response
            .blocked
            .iter()
            .any(|entry| entry.sandbox_id.as_deref() == Some(*sandbox_id))
    })
}

/// How many of the node's rows `response` and the loop's `recording` of it
/// leave blocked, pending and held, each row counted once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct OutstandingRows {
    /// The node reports the row blocked, or its evacuation is unrecorded
    /// past the bound. A blocked entry that names no sandbox counts on its
    /// own.
    pub(super) blocked: u64,
    /// Not blocked, and the node reports it pending with work under way,
    /// its window's `queued` and `disk` among the reasons, or its evacuation
    /// is unrecorded within the bound.
    pub(super) pending: u64,
    /// Not blocked, and every pending entry the node reports for it is a
    /// gentle drain's `hold`: a running sandbox the drain waits for, with
    /// nothing started for it.
    pub(super) held: u64,
}

pub(super) fn outstanding_rows(
    response: &DrainNodeResponse,
    recording: &Recording,
) -> OutstandingRows {
    let mut blocked: BTreeSet<&str> = recording
        .overdue()
        .into_iter()
        .map(|(sandbox_id, _)| sandbox_id)
        .collect();
    let mut unnamed = 0_u64;
    for entry in &response.blocked {
        match entry.sandbox_id.as_deref() {
            Some(sandbox_id) => {
                blocked.insert(sandbox_id);
            }
            None => unnamed += 1,
        }
    }
    let working: BTreeSet<&str> = response
        .pending
        .iter()
        .filter(|entry| entry.action != HOLD_ACTION)
        .map(|entry| entry.sandbox_id.as_str())
        .chain(recording.unrecorded().map(|(sandbox_id, _, _)| sandbox_id))
        .filter(|sandbox_id| !blocked.contains(sandbox_id))
        .collect();
    let held: BTreeSet<&str> = response
        .pending
        .iter()
        .filter(|entry| entry.action == HOLD_ACTION)
        .map(|entry| entry.sandbox_id.as_str())
        .filter(|sandbox_id| !blocked.contains(sandbox_id) && !working.contains(sandbox_id))
        .collect();
    OutstandingRows {
        blocked: blocked.len() as u64 + unnamed,
        pending: working.len() as u64,
        held: held.len() as u64,
    }
}

/// Publish what `response` and the loop's `recording` of it leave to do,
/// as [`outstanding_rows`] counts it.
fn publish_report(node: &V2Node, response: &DrainNodeResponse, recording: &Recording) {
    let rows = outstanding_rows(response, recording);
    crate::metrics::node_drain::v2_report(
        &node.id.to_string(),
        rows.blocked,
        rows.pending,
        rows.held,
    );
}

pub(super) async fn step<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    evidence: Option<&InstanceEvidence>,
    backoff: &DispatchBackoff,
) -> Option<NodeStep> {
    let Some(epoch) = node.epoch else {
        // The admin route writes the epoch with the phase; a row without
        // one gets no call, sealed under nothing.
        tracing::warn!(node_id = %node.id, "node_drain v2: drain_status has no epoch");
        return None;
    };
    if let Some(fence) = node.fenced() {
        let evidence = evidence?;
        return terminated_or_dispatch(sink, authority, node, epoch, fence, evidence, backoff)
            .await;
    }
    if let Some(evidence) = evidence {
        let instance = node.instance_id.as_deref().unwrap_or("<none>");
        if *evidence == InstanceEvidence::Terminated {
            let trace =
                format!("v2 drain let go: instance {instance} terminated outside the drain");
            return let_go(sink, node, epoch, &trace)
                .await
                .then_some(NodeStep::LetGo);
        }
        sink.record_error(
            node.id,
            &format!(
                "the node's heartbeat is stale; EC2 answers {evidence} for its instance {instance}"
            ),
        )
        .await;
    }
    match node.phase.as_deref() {
        None if node.cancel => cancel(sink, node, epoch)
            .await
            .then_some(NodeStep::Cancelled),
        None => None,
        Some("draining") if node.waiting => {
            escalate(sink, node, epoch).await;
            // Fenced, but not sealed: it waits for a slot.
            if let Err(error) = sink.ensure_cordon(node.id).await {
                sink.record_error(node.id, &format!("cordon convergence failed: {error}"))
                    .await;
            }
            None
        }
        Some("draining") | Some("drained") => {
            let drained = drain(sink, node, epoch).await;
            if node.phase.as_deref() == Some("drained") {
                release(sink, node.id, epoch).await;
            }
            drained.then_some(NodeStep::Drained)
        }
        Some("terminating") => fence(sink, authority, node.id, epoch).await,
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
    let gentle = escalate(sink, node, epoch).await;
    let response = match sink.drain_node(node.id, drain_request(epoch, gentle)).await {
        Ok(response) => response,
        Err(error) => {
            crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
            let error = if gentle && error.refused_by_an_older_node() {
                format!(
                    "gentle DrainNode failed: {error}; the node's sandbox-manager, or its \
                     node-api relay, predates gentle drains and refused the call before it \
                     sealed anything: upgrade the node, or escalate the drain with mode forced"
                )
            } else if gentle {
                format!("gentle DrainNode failed: {error}")
            } else {
                format!("DrainNode failed: {error}")
            };
            sink.record_error(node.id, &error).await;
            return false;
        }
    };
    if gentle && response.gentle_epoch != Some(epoch) {
        // Only a node that drained gently under the epoch says so; one that
        // didn't may have suspended running sandboxes, and its report isn't
        // the gentle drain's.
        crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
        sink.record_error(
            node.id,
            &format!(
                "the node answered the gentle DrainNode under epoch {epoch} without \
                 acknowledging it (gentle_epoch {:?}); the drain records nothing from it",
                response.gentle_epoch
            ),
        )
        .await;
        return false;
    }
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
        let answer = sink
            .drain_node(node.id, cancel_request(epoch, &forget))
            .await;
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

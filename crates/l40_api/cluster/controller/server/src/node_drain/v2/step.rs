//! The per-node step: what one tick does to a node, the drain call, and
//! what the loop stores of the node's report.

use pplx_space_node_registration_store::drain_v2::{
    StoredV2ArtifactKind, StoredV2Blocked, StoredV2Evacuated, StoredV2Pending, StoredV2Unrecorded,
    StoredV2Wait,
};
use pplx_space_sbx_mgr_client::proto::{DrainReport, DrainWait, drain_node_response};

use super::*;

/// How many of a refused terminate's reasons its `drain_last_error` names.
const REFUSAL_REASONS: usize = 3;

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

    /// The evacuations recorded, which join `evacuated_total`.
    fn recorded(&self) -> Vec<String> {
        self.records
            .iter()
            .filter(|(_, record)| record.is_recorded())
            .map(|(sandbox_id, _)| sandbox_id.clone())
            .collect()
    }

    fn all_recorded(&self) -> bool {
        self.records.iter().all(|(_, record)| record.is_recorded())
    }
}

/// How the stored report names an evacuated sandbox's artifact kind.
fn artifact_kind(kind: i32) -> StoredV2ArtifactKind {
    use pplx_space_sbx_mgr_client::proto::ArtifactKind;
    match ArtifactKind::try_from(kind) {
        Ok(ArtifactKind::Full) => StoredV2ArtifactKind::Full,
        Ok(ArtifactKind::Disk) => StoredV2ArtifactKind::Disk,
        _ => StoredV2ArtifactKind::Unknown,
    }
}

/// How the stored report names a pending entry's wait: one this build
/// doesn't know is unspecified, still pending.
fn wait(wait: i32) -> StoredV2Wait {
    match DrainWait::try_from(wait) {
        Ok(DrainWait::Hold) => StoredV2Wait::Hold,
        Ok(DrainWait::Moving) => StoredV2Wait::Moving,
        Ok(DrainWait::External) => StoredV2Wait::External,
        Ok(DrainWait::Unspecified) | Err(_) => StoredV2Wait::Unspecified,
    }
}

/// The node's drain report under `generation`, or why the answer isn't
/// one: a drain call is answered with a drain report, sealed under the
/// call's own generation.
fn drain_report(response: &DrainNodeResponse, generation: u64) -> Result<&DrainReport, String> {
    match &response.result {
        Some(drain_node_response::Result::Drain(report)) if response.generation == generation => {
            Ok(report)
        }
        Some(drain_node_response::Result::Drain(_)) => Err(format!(
            "the node answered DrainNode under generation {generation} sealed under generation {}",
            response.generation
        )),
        Some(drain_node_response::Result::Cancel(_)) => Err(format!(
            "the node answered DrainNode under generation {generation} with a cancel's result"
        )),
        None => Err(format!(
            "the node answered DrainNode under generation {generation} with no result"
        )),
    }
}

/// What `drain_status.report` holds: the node's report, what the loop
/// recorded of it (`unrecorded`), when it stored it (`reported_at`, the
/// recording's `now`) and the stall clock (`evacuated_grew_at`, see
/// [`evacuated_grew_at`]). Every stamp is Unix seconds by the database clock
/// but a pending entry's `since`, which is the node's.
pub fn stored_report(
    report: &DrainReport,
    recording: &Recording,
    evacuated_grew_at: i64,
) -> StoredV2Report {
    StoredV2Report {
        reported_at: recording.now,
        evacuated_grew_at,
        evacuated: report
            .evacuated
            .iter()
            .map(|entry| StoredV2Evacuated {
                sandbox_id: entry.sandbox_id.clone(),
                artifact_kind: artifact_kind(entry.artifact_kind),
                artifact_id: entry.artifact_id.clone(),
            })
            .collect(),
        pending: report
            .pending
            .iter()
            .map(|entry| StoredV2Pending {
                sandbox_id: entry.sandbox_id.clone(),
                wait: wait(entry.wait),
                running: entry.running,
                reason: entry.reason.clone(),
                since: entry.since.as_ref().map(|since| since.seconds),
                pool_member: entry.pool_member,
            })
            .collect(),
        blocked: report
            .blocked
            .iter()
            .map(|entry| StoredV2Blocked {
                reason: entry.reason.clone(),
            })
            .collect(),
        unrecorded: recording
            .unrecorded()
            .map(|(sandbox_id, reason, since)| StoredV2Unrecorded {
                sandbox_id: sandbox_id.to_owned(),
                reason: reason.to_owned(),
                since,
            })
            .collect(),
    }
}

/// Why a drain is stalled at `now`, if it is: past its deadline (a drain
/// without one is), not finished, and the set of sandboxes its node reports
/// evacuated hasn't grown for `bound`, counted from the later of when it
/// last grew ([`evacuated_grew_at`]) and the deadline. Before its deadline a
/// drain is never stalled, however long it holds: its running sandboxes
/// wait for the deadline. `pending` and `blocked` are what the report still
/// lists, for the message.
pub fn stalled(
    finished: bool,
    deadline_at: Option<i64>,
    evacuated_grew_at: i64,
    now: i64,
    bound: Duration,
    pending: usize,
    blocked: usize,
) -> Option<String> {
    let bound_secs = i64::try_from(bound.as_secs()).unwrap_or(i64::MAX);
    if finished || deadline_at.is_some_and(|deadline_at| now < deadline_at) {
        return None;
    }
    let since = deadline_at.map_or(evacuated_grew_at, |deadline_at| {
        evacuated_grew_at.max(deadline_at)
    });
    (now.saturating_sub(since) >= bound_secs).then(|| {
        format!(
            "the drain is stalled: no sandbox evacuated for over {} past its deadline \
             ({pending} pending, {blocked} blocked)",
            humanize(bound)
        )
    })
}

/// `15 minutes`, `90 seconds`: a bound as the stall message names it.
fn humanize(bound: Duration) -> String {
    let secs = bound.as_secs();
    if secs.is_multiple_of(60) && secs >= 60 {
        format!("{} minutes", secs / 60)
    } else {
        format!("{secs} seconds")
    }
}

/// The stall clock a report stores: the last time the set of sandboxes the
/// node reports evacuated grew (now, when `report` names one the stored
/// report didn't, or on the first report; the stored stamp otherwise), or
/// the deadline once it has passed, if the set hasn't grown since, so the
/// stamp plus the stall bound is when the drain stalls.
pub fn evacuated_grew_at(node: &V2Node, report: &DrainReport) -> i64 {
    let now = node.db_now;
    let grew_at = match node.stored_evacuated() {
        Some((stored, since))
            if report
                .evacuated
                .iter()
                .all(|entry| stored.contains(entry.sandbox_id.as_str())) =>
        {
            since
        }
        _ => now,
    };
    match node.deadline_at {
        Some(deadline_at) if now >= deadline_at => grew_at.max(deadline_at),
        _ => grew_at,
    }
}

/// Record each evacuation `report` lists, carrying over since when each
/// unrecorded one has been from the node's last stored report.
async fn record_evacuations<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    report: &DrainReport,
) -> Recording {
    let now = node.db_now;
    let stored_since = node.unrecorded_since();
    let mut recording = Recording {
        now,
        ..Recording::default()
    };
    for evacuated in &report.evacuated {
        let record = match sink.record_evacuation(node.id, evacuated).await {
            Ok(record) => record,
            Err(error) => EvacuationRecord::NotYet(format!("recording failed: {error}")),
        };
        if !record.is_recorded() {
            recording.since.insert(
                evacuated.sandbox_id.clone(),
                stored_since
                    .get(evacuated.sandbox_id.as_str())
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

/// How many of the node's rows `report` and the loop's `recording` of it
/// leave blocked, pending and held, each row counted once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct OutstandingRows {
    /// Node-wide problems the node reports `blocked`, each counted once.
    pub(super) blocked: u64,
    /// The rows the node reports pending with work under way or waited
    /// for, or whose evacuation is unrecorded.
    pub(super) pending: u64,
    /// The rows every pending entry the node reports for is a hold: a
    /// running sandbox the drain leaves running until its deadline, with
    /// nothing started for it.
    pub(super) held: u64,
}

pub(super) fn outstanding_rows(report: &DrainReport, recording: &Recording) -> OutstandingRows {
    let hold = DrainWait::Hold as i32;
    let working: BTreeSet<&str> = report
        .pending
        .iter()
        .filter(|entry| entry.wait != hold)
        .map(|entry| entry.sandbox_id.as_str())
        .chain(recording.unrecorded().map(|(sandbox_id, _, _)| sandbox_id))
        .collect();
    let held: BTreeSet<&str> = report
        .pending
        .iter()
        .filter(|entry| entry.wait == hold)
        .map(|entry| entry.sandbox_id.as_str())
        .filter(|sandbox_id| !working.contains(sandbox_id))
        .collect();
    OutstandingRows {
        blocked: report.blocked.len() as u64,
        pending: working.len() as u64,
        held: held.len() as u64,
    }
}

/// Publish what `report` and the loop's `recording` of it leave to do, as
/// [`outstanding_rows`] counts it, and whether the drain is stalled.
fn publish_report(node: &V2Node, report: &DrainReport, recording: &Recording, stalled: bool) {
    let rows = outstanding_rows(report, recording);
    crate::metrics::node_drain::v2_report(
        &node.id.to_string(),
        rows.blocked,
        rows.pending,
        rows.held,
        stalled,
    );
}

pub(super) async fn step<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    evidence: Option<&InstanceEvidence>,
    backoff: &DispatchBackoff,
    config: &V2LoopConfig,
) -> Option<NodeStep> {
    let Some(generation) = node.generation else {
        // The admin route writes the generation with the phase; a row
        // without one gets no call, sealed under nothing.
        tracing::warn!(node_id = %node.id, "node_drain v2: drain_status has no generation");
        return None;
    };
    if let Some(fence) = node.fenced() {
        let evidence = evidence?;
        return terminated_or_dispatch(
            sink, authority, node, generation, fence, evidence, backoff, config,
        )
        .await;
    }
    if let Some(evidence) = evidence
        && let Some(step) = let_go_unfenced(sink, node, generation, evidence).await
    {
        return step;
    }
    match node.phase.as_deref() {
        None => cancel(sink, node, generation)
            .await
            .then_some(NodeStep::Cancelled),
        Some("draining") => drain(sink, node, generation, config)
            .await
            .then_some(NodeStep::Drained),
        // A drained node waits for `:terminate` or `:undrain`. It is still
        // sealed, so its tick keeps its cordon; it writes no sandbox row and
        // makes no node call, and counts the durable rows still homed on it
        // evacuated.
        Some("drained") => {
            let _ = converge_cordon(sink, node).await;
            count_drained(sink, node.id, generation).await;
            None
        }
        Some("terminating") => terminate(sink, authority, node, generation).await,
        Some(other) => {
            tracing::warn!(node_id = %node.id, phase = other, "node_drain v2: unknown phase");
            None
        }
    }
}

/// Let go of an unfenced node whose heartbeat is stale, with EC2's answer
/// for its recorded instance: `terminated` now, or not listed once the
/// drain recorded it `terminated`, and the machine EC2 answered about the
/// one the node registered ([`super::cleanup::unregistered_machine`]). The
/// first `terminated` answer is recorded. `Some` when the tick is done with
/// the node: let go, or moved on. Any other answer is recorded as the last
/// error, and the node's step goes on.
async fn let_go_unfenced<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    evidence: &InstanceEvidence,
) -> Option<Option<NodeStep>> {
    let instance = node.instance_id.as_deref().unwrap_or("<none>");
    if *evidence == InstanceEvidence::Terminated
        && node.terminated_observed_at.is_none()
        && let Some(asked) = node.asked_instance()
    {
        match sink
            .record_terminated_observed(node.id, generation, asked)
            .await
        {
            Ok(true) => {}
            Ok(false) => return Some(None),
            Err(error) => {
                sink.record_error(
                    node.id,
                    &format!("recording that {asked} is terminated failed: {error}"),
                )
                .await
            }
        }
    }
    let terminated = match evidence {
        InstanceEvidence::Terminated => true,
        InstanceEvidence::NotListed => node.terminated_observed_at.is_some(),
        _ => false,
    };
    if !terminated {
        sink.record_error(
            node.id,
            &format!(
                "the node's heartbeat is stale; EC2 answers {evidence} for its instance {instance}"
            ),
        )
        .await;
        return None;
    }
    if let Some(why) = super::cleanup::unregistered_machine(sink, node, instance).await {
        sink.record_error(
            node.id,
            &format!(
                "EC2 answers terminated for the node's recorded instance {instance}, but {why}; \
                 not letting it go"
            ),
        )
        .await;
        return Some(None);
    }
    let trace = format!("v2 drain let go: instance {instance} terminated outside the drain");
    Some(
        let_go(sink, node, generation, &trace)
            .await
            .then_some(NodeStep::LetGo),
    )
}

/// The node's answer to one drain call, as a step reads it.
enum DrainAnswer {
    /// A drain report under the drain's generation.
    Report(DrainReport),
    /// Another `DrainNode` call was in progress on the node.
    Aborted,
    /// The call failed, or the answer isn't the drain's: why.
    Failed(String),
}

/// Call `DrainNode` under `generation`, evacuating once the deadline has
/// passed.
async fn call_drain<S: DrainV2Sink>(sink: &S, node: &V2Node, generation: u64) -> DrainAnswer {
    match sink
        .drain_node(node.id, drain_request(generation, node.evacuate))
        .await
    {
        Ok(response) => match drain_report(&response, generation) {
            Ok(report) => DrainAnswer::Report(report.clone()),
            Err(why) => DrainAnswer::Failed(format!("{why}; it records nothing from the answer")),
        },
        Err(error) if error.aborted() => DrainAnswer::Aborted,
        Err(error) => DrainAnswer::Failed(format!("DrainNode failed: {error}")),
    }
}

/// Record the evacuations `report` lists and store it, with the evacuations
/// recorded and the sandboxes it lists running. Returns the recording, and
/// whether the row still carries the generation; `None` when storing it
/// failed, which is recorded.
async fn store<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    report: &DrainReport,
    grew_at: i64,
) -> Option<(Recording, bool)> {
    let recording = record_evacuations(sink, node, report).await;
    // A running warm-pool member isn't a user's sandbox: the observed set
    // holds user sandboxes only.
    let running = report
        .pending
        .iter()
        .filter(|entry| entry.running && !entry.pool_member)
        .map(|entry| entry.sandbox_id.clone())
        .collect();
    match sink
        .record_report(
            node.id,
            generation,
            stored_report(report, &recording, grew_at),
            running,
            recording.recorded(),
        )
        .await
    {
        Ok(stored) => Some((recording, stored)),
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("storing the drain report failed: {error}"),
            )
            .await;
            None
        }
    }
}

/// Converge `node`'s cordon ([`DrainV2Sink::ensure_cordon`]): a v2 node
/// stays cordoned in every phase where it may still be sealed, `draining`,
/// `drained`, `terminating` and its cancel pending, so nothing is placed on
/// it. Each failure is recorded as the node's last error. `Err` when the
/// Kubernetes Node under the name has another UID than the one registration
/// recorded: the drain's node isn't the one a call would reach, so the
/// caller makes no call. `Ok(Some(error))` for any other failure, which the
/// caller goes on past; `Ok(None)` once converged.
async fn converge_cordon<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
) -> Result<Option<String>, String> {
    match sink.ensure_cordon(node.id).await {
        Ok(()) => Ok(None),
        Err(CordonFailure::UidMismatch(error)) => {
            sink.record_error(
                node.id,
                &format!("cordon convergence failed: {error}; not calling DrainNode"),
            )
            .await;
            Err(error)
        }
        Err(CordonFailure::Other(error)) => {
            let error = format!("cordon convergence failed: {error}");
            sink.record_error(node.id, &error).await;
            Ok(Some(error))
        }
    }
}

/// Ask the node, record what it reports, mark the drain stalled when it is,
/// and advance a node that has finished. Returns whether the node moved to
/// `drained`.
async fn drain<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    config: &V2LoopConfig,
) -> bool {
    let Ok(cordon_error) = converge_cordon(sink, node).await else {
        return false;
    };
    let report = match call_drain(sink, node, generation).await {
        DrainAnswer::Report(report) => report,
        DrainAnswer::Aborted => {
            tracing::debug!(
                node_id = %node.id,
                "node_drain v2: another DrainNode call is in progress on the node; skipping it \
                 this tick"
            );
            return false;
        }
        DrainAnswer::Failed(error) => {
            crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
            sink.record_error(node.id, &error).await;
            return false;
        }
    };
    let grew_at = evacuated_grew_at(node, &report);
    let Some((recording, stored)) = store(sink, node, generation, &report, grew_at).await else {
        return false;
    };
    let finished =
        report.pending.is_empty() && report.blocked.is_empty() && recording.all_recorded();
    let stall = stalled(
        finished,
        node.deadline_at,
        grew_at,
        node.db_now,
        config.stall_bound,
        report.pending.len(),
        report.blocked.len(),
    );
    // A report the generation guard rejected publishes nothing: the current
    // generation's tick publishes its own.
    if !stored {
        return false;
    }
    publish_report(node, &report, &recording, stall.is_some());
    if !report.blocked.is_empty() {
        tracing::warn!(
            node_id = %node.id,
            blocked = report.blocked.len(),
            "node_drain v2: the node reports node-wide problems; the drain can't end until \
             they are resolved"
        );
    }
    // Storing the report clears the last error; a cordon that failed this
    // tick keeps its error until one succeeds, and a stalled drain says so
    // for as long as it lasts.
    for error in [&cordon_error, &stall].into_iter().flatten() {
        sink.record_error(node.id, error).await;
    }
    if !finished {
        return false;
    }
    match sink.advance_drained(node.id, generation).await {
        Ok(moved) => moved,
        Err(error) => {
            sink.record_error(node.id, &format!("advancing to drained failed: {error}"))
                .await;
            false
        }
    }
}

/// A terminating node's one fresh check: the report must list nothing
/// pending or blocked, and every evacuation it lists must be recorded,
/// before the loop fences. Any other outcome returns the
/// node to `drained` with why ([`DrainV2Sink::return_drained`]), and the
/// operator retries `:terminate`. The node's cordon is converged first; a
/// Node under the name with another UID refuses the terminate without a
/// call. With no terminating authority, the node isn't called: it waits for
/// an operator, who may still undrain it.
async fn terminate<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    generation: u64,
) -> Option<NodeStep> {
    if !authority.terminates() {
        sink.record_error(node.id, super::cleanup::REFUSED_TERMINATE)
            .await;
        return None;
    }
    let refused = if let Err(error) = converge_cordon(sink, node).await {
        format!("terminate refused: cordon convergence failed: {error}; not calling DrainNode")
    } else {
        match call_drain(sink, node, generation).await {
            DrainAnswer::Report(report) => {
                let grew_at = evacuated_grew_at(node, &report);
                let (recording, stored) = store(sink, node, generation, &report, grew_at).await?;
                if !stored {
                    return None;
                }
                if !report.pending.is_empty() || !report.blocked.is_empty() {
                    format!(
                        "terminate refused: the node reports {} pending, {} blocked: {}",
                        report.pending.len(),
                        report.blocked.len(),
                        first_reasons(&report)
                    )
                } else if recording.all_recorded() {
                    return fence(sink, node.id, generation).await;
                } else {
                    // The draining path treats the same report as
                    // unfinished: an evacuation not yet recorded hasn't
                    // left the node's books.
                    let unrecorded: Vec<_> = recording.unrecorded().collect();
                    format!(
                        "terminate refused: the node reports 0 pending, 0 blocked, and {} \
                         evacuated not yet recorded: {}",
                        unrecorded.len(),
                        unrecorded
                            .iter()
                            .take(REFUSAL_REASONS)
                            .map(|(sandbox_id, reason, _)| format!("{sandbox_id}: {reason}"))
                            .collect::<Vec<_>>()
                            .join("; ")
                    )
                }
            }
            DrainAnswer::Aborted => {
                "terminate refused: DrainNode failed: another DrainNode call is in progress on the \
             node"
                    .to_owned()
            }
            DrainAnswer::Failed(error) => format!("terminate refused: {error}"),
        }
    };
    match sink.return_drained(node.id, generation, &refused).await {
        Ok(true) => {
            tracing::warn!(node_id = %node.id, refused, "node_drain v2: returned the node to drained");
            Some(NodeStep::Returned)
        }
        Ok(false) => None,
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("{refused}; returning the node to drained failed: {error}"),
            )
            .await;
            None
        }
    }
}

/// The first [`REFUSAL_REASONS`] reasons a report isn't empty, blockers
/// first: each node-wide reason, then each pending sandbox with its reason.
fn first_reasons(report: &DrainReport) -> String {
    report
        .blocked
        .iter()
        .map(|entry| entry.reason.clone())
        .chain(report.pending.iter().map(|entry| {
            if entry.reason.is_empty() {
                entry.sandbox_id.clone()
            } else {
                format!("{}: {}", entry.sandbox_id, entry.reason)
            }
        }))
        .take(REFUSAL_REASONS)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Cancel an undrained node's generation: the node unseals and hands
/// nothing back. Its cordon is converged first, since the node may still be
/// sealed; a Node under the name with another UID is not called, and the
/// next tick retries. Once it answers the cancel, lift the cordon, which
/// `:undrain` leaves on a v2 node so that nothing is placed on a node still
/// sealed, and clear the v2 columns, both only while the row is still this
/// generation's cancel ([`DrainV2Sink::finish_cancel`]). Anything but a
/// cancel's answer, an `Aborted` call included, is unanswered and retried
/// next tick. Returns whether the columns were cleared.
async fn cancel<S: DrainV2Sink>(sink: &S, node: &V2Node, generation: u64) -> bool {
    if converge_cordon(sink, node).await.is_err() {
        return false;
    }
    let unanswered = match sink.drain_node(node.id, cancel_request(generation)).await {
        Ok(DrainNodeResponse {
            result: Some(drain_node_response::Result::Cancel(_)),
            ..
        }) => None,
        Ok(_) => Some(format!(
            "the node answered the cancel of generation {generation} without a cancel's result"
        )),
        Err(error) => Some(format!("DrainNode cancel failed: {error}")),
    };
    if let Some(error) = unanswered {
        crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
        sink.record_error(node.id, &error).await;
        return false;
    }
    match sink.finish_cancel(node.id, generation).await {
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

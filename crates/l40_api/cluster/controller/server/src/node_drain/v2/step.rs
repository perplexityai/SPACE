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
    /// running sandbox the drain leaves running until its deadline, whose
    /// pinned artifact's upload may be under way.
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

pub(super) async fn step<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    evidence: Option<&InstanceEvidence>,
    backoff: &DispatchBackoff,
    config: &V2LoopConfig,
    warn_pacer: &PerNodeWarnPacer,
) -> NodeOutcome {
    let Some(generation) = node.generation else {
        // The admin route writes the generation with the phase; a row
        // without one gets no call, sealed under nothing.
        if warn_pacer.due(node.id, "no_generation") {
            tracing::warn!(node_id = %node.id, "node_drain v2: drain_status has no generation");
        }
        return NodeOutcome::kept();
    };
    if let Some(fence) = node.fenced() {
        // No answer this tick: the dispatch backoff isn't due.
        let Some(evidence) = evidence else {
            return NodeOutcome::kept();
        };
        return terminated_or_dispatch(
            sink, authority, node, generation, fence, evidence, backoff, config,
        )
        .await;
    }
    let mut note = None;
    if let Some(evidence) = evidence {
        match let_go_unfenced(sink, node, generation, evidence).await {
            ControlFlow::Break(outcome) => return outcome,
            ControlFlow::Continue(found) => note = Some(found),
        }
    }
    let outcome = match node.phase.as_deref() {
        None => cancel(sink, node, generation).await,
        Some("draining") => drain(sink, node, generation, config).await,
        Some("drained") => drained(sink, node, generation, warn_pacer).await,
        Some("terminating") => terminate(sink, authority, node, generation, config).await,
        Some(other) => {
            if warn_pacer.due(node.id, "unknown_phase") {
                tracing::warn!(node_id = %node.id, phase = other, "node_drain v2: unknown phase");
            }
            NodeOutcome::kept()
        }
    };
    outcome.or_note(note)
}

/// Let go of an unfenced node whose heartbeat is stale, with EC2's answer
/// for its recorded instance: `terminated` now, or not listed once the
/// drain recorded it `terminated`, and the machine EC2 answered about the
/// one the node registered ([`super::cleanup::unregistered_machine`]). The
/// first `terminated` answer is recorded. `Break` with the tick's outcome
/// when the tick is done with the node: let go, moved on, or waiting on a
/// live machine. Otherwise `Continue` with what EC2 answered, which the
/// node's status shows when its step has nothing else to report.
async fn let_go_unfenced<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    evidence: &InstanceEvidence,
) -> ControlFlow<NodeOutcome, String> {
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
            Ok(false) => return ControlFlow::Break(NodeOutcome::kept()),
            // EC2 answers `terminated` now, so this tick doesn't need the
            // record; the next one that sees it records it.
            Err(error) => tracing::warn!(
                node_id = %node.id,
                %error,
                "node_drain v2: recording that {asked} is terminated failed"
            ),
        }
    }
    let terminated = match evidence {
        InstanceEvidence::Terminated => true,
        InstanceEvidence::NotListed => node.terminated_observed_at.is_some(),
        _ => false,
    };
    if !terminated {
        return ControlFlow::Continue(format!(
            "the node's heartbeat is stale; EC2 answers {evidence} for its instance {instance}"
        ));
    }
    if let Some(why) = super::cleanup::unregistered_machine(sink, node, instance).await {
        return ControlFlow::Break(NodeOutcome::says(format!(
            "EC2 answers terminated for the node's recorded instance {instance}, but {why}; \
             not letting it go"
        )));
    }
    let trace = format!("v2 drain let go: instance {instance} terminated outside the drain");
    ControlFlow::Break(let_go(sink, node, generation, &trace).await)
}

/// The node's answer to one drain call, as a step reads it.
enum DrainAnswer {
    /// A drain report under the drain's generation.
    Report(DrainReport),
    /// Another `DrainNode` call was in progress on the node.
    Aborted,
    /// No call was made: the row moved on since the tick listed it.
    Moved,
    /// The call failed, or the answer isn't the drain's: why.
    Failed(String),
}

/// Call `DrainNode` under `generation`, evacuating once the deadline has
/// passed.
async fn call_drain<S: DrainV2Sink>(sink: &S, node: &V2Node, generation: u64) -> DrainAnswer {
    match sink
        .drain_node(node, drain_request(generation, node.evacuate))
        .await
    {
        Ok(response) => match drain_report(&response, generation) {
            Ok(report) => DrainAnswer::Report(report.clone()),
            Err(why) => DrainAnswer::Failed(format!("{why}; it records nothing from the answer")),
        },
        Err(error) if error.aborted() => DrainAnswer::Aborted,
        Err(error) if error.is_moved() => DrainAnswer::Moved,
        Err(error) => DrainAnswer::Failed(format!("DrainNode failed: {error}")),
    }
}

/// A drain report the tick stored, and what it shows.
struct Stored {
    /// The report as stored, whose [`StoredV2Report::finished`] is the one
    /// rule of a finished drain.
    report: StoredV2Report,
    /// What the gauges read for it.
    gauges: Gauges,
    /// Why the drain is stalled, if it is ([`stalled`]).
    stall: Option<String>,
    recording: Recording,
}

/// Record the evacuations `report` lists and store it, with the evacuations
/// recorded and the sandboxes it lists running, and what its gauges read.
/// The draining and the terminating tick store a report the same way, so
/// the same report reads the same. `Ok(None)` when the row no longer
/// carries the generation; `Err` with the status when storing failed.
async fn store<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    report: &DrainReport,
    config: &V2LoopConfig,
) -> Result<Option<Stored>, String> {
    let recording = record_evacuations(sink, node, report).await;
    let grew_at = evacuated_grew_at(node, report);
    let stored = stored_report(report, &recording, grew_at);
    let stall = stalled(
        stored.finished(),
        node.deadline_at,
        grew_at,
        node.db_now,
        config.stall_bound,
        report.pending.len(),
        report.blocked.len(),
    );
    let gauges = Gauges::Report {
        rows: outstanding_rows(report, &recording),
        stalled: stall.is_some(),
    };
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
            stored.clone(),
            running,
            recording.recorded(),
        )
        .await
    {
        Ok(true) => Ok(Some(Stored {
            report: stored,
            gauges,
            stall,
            recording,
        })),
        // The current generation's tick stores, and publishes, its own.
        Ok(false) => Ok(None),
        Err(error) => Err(format!("storing the drain report failed: {error}")),
    }
}

/// Why the cordon's convergence leaves the node uncalled this tick.
enum NoCall {
    /// The Kubernetes Node under the name is another one than the drain's:
    /// the status, which says so.
    AnotherNode(String),
    /// The row moved on since the tick listed it.
    Moved,
}

/// Converge `node`'s cordon ([`DrainV2Sink::ensure_cordon`]): a v2 node
/// stays cordoned in every phase where it may still be sealed, `draining`,
/// `drained`, `terminating` and its cancel pending, so nothing is placed on
/// it. `Err` when the Kubernetes Node under the name has another UID than
/// the one the tick listed: the drain's node isn't the one a call would
/// reach, so the caller makes no call; and when the row moved on since the
/// tick listed it. `Ok(Some(status))` for any other failure, which the
/// caller goes on past; `Ok(None)` once converged.
async fn converge_cordon<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
) -> Result<Option<String>, NoCall> {
    match sink.ensure_cordon(node).await {
        Ok(()) => Ok(None),
        Err(CordonFailure::Moved) => Err(NoCall::Moved),
        Err(CordonFailure::UidMismatch(error)) => Err(NoCall::AnotherNode(format!(
            "cordon convergence failed: {error}; not calling DrainNode"
        ))),
        Err(CordonFailure::Other(error)) => Ok(Some(format!("cordon convergence failed: {error}"))),
    }
}

/// Ask the node, record what it reports, and advance a node that has
/// finished. The status is the stall, else a cordon that failed, else none.
async fn drain<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    config: &V2LoopConfig,
) -> NodeOutcome {
    let cordon_error = match converge_cordon(sink, node).await {
        Ok(error) => error,
        Err(NoCall::Moved) => return NodeOutcome::kept(),
        Err(NoCall::AnotherNode(error)) => return NodeOutcome::says(error),
    };
    let report = match call_drain(sink, node, generation).await {
        DrainAnswer::Report(report) => report,
        DrainAnswer::Aborted => {
            tracing::debug!(
                node_id = %node.id,
                "node_drain v2: another DrainNode call is in progress on the node; skipping it \
                 this tick"
            );
            return NodeOutcome::kept();
        }
        DrainAnswer::Moved => return NodeOutcome::kept(),
        DrainAnswer::Failed(error) => return NodeOutcome::unanswered(error),
    };
    let stored = match store(sink, node, generation, &report, config).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return NodeOutcome::kept(),
        Err(error) => return NodeOutcome::says(error),
    };
    let mut outcome = NodeOutcome {
        step: None,
        status: stored
            .stall
            .or(cordon_error)
            .map_or(Status::Clear, Status::Says),
        gauges: stored.gauges,
    };
    if !stored.report.finished() {
        return outcome;
    }
    match sink.advance_drained(node.id, generation).await {
        Ok(true) => outcome.step = Some(NodeStep::Drained),
        Ok(false) => {}
        Err(error) => {
            outcome.status = Status::Says(format!("advancing to drained failed: {error}"));
        }
    }
    outcome
}

/// A drained node waits for `:terminate` or `:undrain`. It is still sealed,
/// so its tick keeps its cordon; it writes no sandbox row and makes no node
/// call, and counts evacuated every row still naming it that the
/// terminate would release. Its status is a failed count, else a failed
/// cordon, else none. A node a terminate returned here keeps why
/// ([`TERMINATE_REFUSED`]) until the next `:terminate` or `:undrain`, its
/// tick's own failures included, which then go only to a paced warning;
/// only a let-go that waits on the node's machine replaces it
/// ([`let_go_unfenced`]).
async fn drained<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    warn_pacer: &PerNodeWarnPacer,
) -> NodeOutcome {
    let refused = node
        .last_error
        .as_deref()
        .is_some_and(|error| error.starts_with(TERMINATE_REFUSED));
    let cordon_error = match converge_cordon(sink, node).await {
        Ok(error) => error,
        Err(NoCall::Moved) => return NodeOutcome::kept(),
        Err(NoCall::AnotherNode(error)) => Some(error),
    };
    match count_drained(sink, node.id, generation)
        .await
        .err()
        .or(cordon_error)
    {
        Some(error) if refused => {
            if warn_pacer.due(node.id, "drained_failure") {
                tracing::warn!(
                    node_id = %node.id,
                    error,
                    "node_drain v2: a drained node's tick failed; keeping the terminate's refusal"
                );
            }
            NodeOutcome::kept()
        }
        Some(error) => NodeOutcome::says(error),
        None if refused => NodeOutcome::kept(),
        None => NodeOutcome::default(),
    }
}

/// A terminating node's one fresh check: the report must list nothing
/// pending or blocked, and every evacuation it lists must be recorded
/// ([`StoredV2Report::finished`]), before the loop fences. A node that
/// records no instance to terminate, a Node under the name with another UID
/// (without a call), a failed call, or a report that isn't finished returns
/// the node to `drained` with why ([`DrainV2Sink::return_drained`]), and the
/// operator retries `:terminate`. A call refused because another one is in
/// progress on the node (`Aborted`) changed nothing: the node stays
/// `terminating` and the next tick checks again, its status saying so. With
/// no terminating authority, the node isn't called: it waits for an
/// operator, who may still undrain it.
async fn terminate<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    generation: u64,
    config: &V2LoopConfig,
) -> NodeOutcome {
    if !authority.terminates() {
        return NodeOutcome::says(super::cleanup::NOT_AWS_TERMINATOR);
    }
    if node.instance_id.is_none() {
        return return_drained(sink, node, generation, NO_INSTANCE_TO_TERMINATE).await;
    }
    let refused = match converge_cordon(sink, node).await {
        Err(NoCall::Moved) => return NodeOutcome::kept(),
        Err(NoCall::AnotherNode(error)) => format!("{TERMINATE_REFUSED}: {error}"),
        Ok(_) => match call_drain(sink, node, generation).await {
            DrainAnswer::Report(report) => {
                let stored = match store(sink, node, generation, &report, config).await {
                    Ok(Some(stored)) => stored,
                    Ok(None) => return NodeOutcome::kept(),
                    Err(error) => return NodeOutcome::says(error),
                };
                let refused = if stored.report.finished() {
                    return fence(sink, node, generation)
                        .await
                        .with_gauges(stored.gauges);
                } else if !report.pending.is_empty() || !report.blocked.is_empty() {
                    format!(
                        "{TERMINATE_REFUSED}: the node reports {} pending, {} blocked: {}",
                        report.pending.len(),
                        report.blocked.len(),
                        first_reasons(&report)
                    )
                } else {
                    // An evacuation not yet recorded hasn't left the node's
                    // books.
                    let unrecorded: Vec<_> = stored.recording.unrecorded().collect();
                    format!(
                        "{TERMINATE_REFUSED}: the node reports 0 pending, 0 blocked, and {} \
                         evacuated not yet recorded: {}",
                        unrecorded.len(),
                        unrecorded
                            .iter()
                            .take(REFUSAL_REASONS)
                            .map(|(sandbox_id, reason, _)| format!("{sandbox_id}: {reason}"))
                            .collect::<Vec<_>>()
                            .join("; ")
                    )
                };
                // Back in `drained`, the node keeps the refused report's
                // rows, which its refusal names, and isn't stalled.
                return return_drained(sink, node, generation, &refused)
                    .await
                    .with_gauges(stored.gauges.unstalled());
            }
            DrainAnswer::Aborted => return NodeOutcome::says(TERMINATE_WAITS_FOR_A_CALL),
            DrainAnswer::Moved => return NodeOutcome::kept(),
            // A failed fresh call changes no gauge: back in `drained`,
            // nothing calls the node, so it isn't unanswered.
            DrainAnswer::Failed(error) => {
                return return_drained(
                    sink,
                    node,
                    generation,
                    &format!("{TERMINATE_REFUSED}: {error}"),
                )
                .await;
            }
        },
    };
    return_drained(sink, node, generation, &refused).await
}

/// The status of a terminating node whose fresh check found another
/// `DrainNode` call in progress on the node.
pub(super) const TERMINATE_WAITS_FOR_A_CALL: &str = "terminate waiting: another DrainNode call is \
     in progress on the node; the fresh check runs again next tick";

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
/// next tick retries, and a row that moved on since the tick listed it is
/// skipped. Once it answers the cancel, lift the cordon, which `:undrain`
/// leaves on a v2 node so that nothing is placed on a node still sealed,
/// then clear the v2 columns, only after the lift and while the row is
/// still this generation's cancel ([`DrainV2Sink::finish_cancel`]). Anything
/// but a cancel's answer, an `Aborted` call included, is unanswered and
/// retried next tick.
async fn cancel<S: DrainV2Sink>(sink: &S, node: &V2Node, generation: u64) -> NodeOutcome {
    match converge_cordon(sink, node).await {
        Ok(_) => {}
        Err(NoCall::Moved) => return NodeOutcome::kept(),
        Err(NoCall::AnotherNode(error)) => return NodeOutcome::says(error),
    }
    let unanswered = match sink.drain_node(node, cancel_request(generation)).await {
        Ok(DrainNodeResponse {
            result: Some(drain_node_response::Result::Cancel(_)),
            ..
        }) => None,
        Err(error) if error.is_moved() => return NodeOutcome::kept(),
        Ok(_) => Some(format!(
            "the node answered the cancel of generation {generation} without a cancel's result"
        )),
        Err(error) => Some(format!("DrainNode cancel failed: {error}")),
    };
    if let Some(error) = unanswered {
        return NodeOutcome::unanswered(error);
    }
    match sink.finish_cancel(node).await {
        Ok(true) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            NodeOutcome::moved(NodeStep::Cancelled)
        }
        Ok(false) => NodeOutcome::kept(),
        Err(error) => NodeOutcome::says(format!("clearing the cancel failed: {error}")),
    }
}

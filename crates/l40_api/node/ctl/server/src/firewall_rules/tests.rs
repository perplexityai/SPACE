use pplx_space_host_layout::identities::{LinuxUser, users};
use pplx_space_node_api_client::proto::node_ctl_server::NodeCtl as _;
use pplx_space_node_api_client::proto::{
    StateReconciliationReason as Reason, TriggerStateReconciliationRequest,
};
use pplx_space_node_mgr_client::proto::{
    FirewallRulesCensus as Census, FirewallRulesCensusGap, FirewallRulesCensusGapKind as Gap,
    FirewallRulesSlotEvidence, FirewallRulesSlotOutcome as Slot,
    FirewallRulesSlotReason as SlotReason,
};
use pplx_space_uds_peer_auth::PeerIdentity;
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::{NodeCtlService, WorkItem};

/// Past twice the longest hop's bound: what hasn't happened by then won't.
const QUIET: Duration = Duration::from_secs(240);
const NODE: &str = "node-from-fetch";

type Reply<T> = oneshot::Sender<Result<T, Status>>;

#[derive(Debug)]
enum Call {
    Fetch(Reply<FetchFirewallRulesResponse>),
    Sync(
        node_manager::SyncFirewallRulesRequest,
        Duration,
        Reply<node_manager::SyncFirewallRulesResponse>,
    ),
    Report(
        ReportFirewallRulesRequest,
        Reply<ReportFirewallRulesResponse>,
    ),
}

/// Hands each hop to the test, which answers as the control plane and as
/// node-manager; a dropped reply never answers.
struct Scripted(mpsc::UnboundedSender<Call>);

async fn answer<T>(answer: oneshot::Receiver<Result<T, Status>>) -> Result<T, Status> {
    match answer.await {
        Ok(result) => result,
        Err(_dropped) => std::future::pending().await,
    }
}

impl Hops for Scripted {
    async fn fetch(&self) -> Result<FetchFirewallRulesResponse, Status> {
        let (reply, receiver) = oneshot::channel();
        self.0.send(Call::Fetch(reply)).expect("test is listening");
        answer(receiver).await
    }

    async fn sync(
        &self,
        request: node_manager::SyncFirewallRulesRequest,
        budget: Duration,
    ) -> Result<node_manager::SyncFirewallRulesResponse, Status> {
        let (reply, receiver) = oneshot::channel();
        self.0
            .send(Call::Sync(request, budget, reply))
            .expect("test is listening");
        answer(receiver).await
    }

    async fn report(
        &self,
        request: ReportFirewallRulesRequest,
    ) -> Result<ReportFirewallRulesResponse, Status> {
        let (reply, receiver) = oneshot::channel();
        self.0
            .send(Call::Report(request, reply))
            .expect("test is listening");
        answer(receiver).await
    }
}

/// The real handler with delivery on, and what reaches each side of it.
struct Node {
    service: NodeCtlService,
    calls: mpsc::UnboundedReceiver<Call>,
    drains: mpsc::Receiver<WorkItem>,
    _delivery: AbortOnDropHandle<()>,
}

impl Node {
    /// Delivery on, its startup Fetch not yet answered.
    fn start() -> Self {
        let (calls_tx, calls) = mpsc::unbounded_channel();
        let (tx, drains) = mpsc::channel(8);
        let (wake, delivery) = spawn(Scripted(calls_tx));
        let service = NodeCtlService::new(
            tx.clone(),
            crate::ClusterReconcileTrigger::new(tx, NODE),
            Arc::new(std::sync::Mutex::new(
                rusqlite::Connection::open_in_memory().expect("open in-memory SQLite"),
            )),
            Some(wake),
        );
        Self {
            service,
            calls,
            drains,
            _delivery: delivery,
        }
    }

    async fn trigger(
        &self,
        caller: LinuxUser,
        reasons: &[Reason],
        diagnostic: &str,
    ) -> Result<(), Status> {
        let mut request = Request::new(TriggerStateReconciliationRequest {
            reasons: reasons.iter().map(|reason| *reason as i32).collect(),
            diagnostic_reason: diagnostic.to_string(),
        });
        request.extensions_mut().insert(PeerIdentity::new(
            caller.uid,
            caller.primary_group.gid,
            None,
        ));
        tokio::time::timeout(QUIET, self.service.trigger_state_reconciliation(request))
            .await
            .expect("the handler doesn't wait on delivery")
            .map(drop)
    }

    async fn wake(&self) {
        self.trigger(users::SPACE_NODEAPI, &[Reason::FirewallRulesChanged], "")
            .await
            .expect("node-api may wake node-ctl");
    }

    async fn next(&mut self) -> Call {
        tokio::time::timeout(QUIET, self.calls.recv())
            .await
            .expect("a hop within the quiet window")
            .expect("delivery task alive")
    }

    async fn fetch(&mut self) -> Reply<FetchFirewallRulesResponse> {
        let Call::Fetch(reply) = self.next().await else {
            panic!("expected a fetch")
        };
        reply
    }

    async fn sync(
        &mut self,
    ) -> (
        node_manager::SyncFirewallRulesRequest,
        Reply<node_manager::SyncFirewallRulesResponse>,
    ) {
        self.sync_within().await.0
    }

    /// The next Sync, and the budget it gave node-manager.
    async fn sync_within(
        &mut self,
    ) -> (
        (
            node_manager::SyncFirewallRulesRequest,
            Reply<node_manager::SyncFirewallRulesResponse>,
        ),
        Duration,
    ) {
        let Call::Sync(request, budget, reply) = self.next().await else {
            panic!("expected a sync")
        };
        ((request, reply), budget)
    }

    /// Started, its startup Fetch answered with nothing to do.
    async fn started() -> Self {
        let mut node = Self::start();
        node.fetch().await.send(Ok(nothing())).unwrap();
        node
    }

    async fn report(
        &mut self,
    ) -> (
        ReportFirewallRulesRequest,
        Reply<ReportFirewallRulesResponse>,
    ) {
        let Call::Report(request, reply) = self.next().await else {
            panic!("expected a report")
        };
        (request, reply)
    }

    /// A wake whose Fetch answers `fetched`.
    async fn fetched(&mut self, fetched: FetchFirewallRulesResponse) {
        self.wake().await;
        self.fetch().await.send(Ok(fetched)).unwrap();
    }

    async fn quiet(&mut self) {
        let call = tokio::time::timeout(QUIET, self.calls.recv()).await;
        assert!(call.is_err(), "unexpected hop: {call:?}");
    }
}

/// Fetch's answer with no assignment and no round.
fn nothing() -> FetchFirewallRulesResponse {
    FetchFirewallRulesResponse {
        node_id: NODE.to_string(),
        assignment: None,
        pending_round: None,
    }
}

/// Fetch's answer with only `pending` pending.
fn asked(pending: FirewallRulesPendingRound) -> FetchFirewallRulesResponse {
    FetchFirewallRulesResponse {
        pending_round: Some(pending),
        ..nothing()
    }
}

/// Round `round` about `generation`, issued `age` ago.
fn round(round: u64, generation: u64, age: Duration) -> FirewallRulesPendingRound {
    FirewallRulesPendingRound {
        round,
        issued_at: Some((SystemTime::now() - age).into()),
        assignment_generation: Some(generation),
    }
}

/// The Sync asking only `round` about `generation`.
fn asking(round: u64, generation: u64) -> node_manager::SyncFirewallRulesRequest {
    node_manager::SyncFirewallRulesRequest {
        node_id: NODE.to_string(),
        assignment: None,
        question: Some(node_manager::FirewallRulesQuestion {
            round: Some(round),
            assignment_generation: Some(generation),
        }),
    }
}

/// An unfavorable, incomplete answer to `round` about `generation`: slot 3
/// enforces other rules, slot 9 is unread, and three managed namespaces are
/// no slot's.
fn answered(round: u64, generation: u64) -> node_manager::SyncFirewallRulesResponse {
    let slot =
        |slot_id, slot_generation, outcome: Slot, reason: SlotReason| FirewallRulesSlotEvidence {
            slot_id: Some(slot_id),
            slot_generation: Some(slot_generation),
            outcome: outcome.into(),
            reason: reason.into(),
        };
    let observation = node_manager::FirewallRulesObservation {
        node_id: NODE.to_string(),
        round: Some(round),
        assignment_generation: Some(generation),
        revision_id: format!("00000000-0000-4000-8000-{generation:012}"),
        manager_epoch: Some(1),
        census: Census::Incomplete.into(),
        census_gaps: vec![FirewallRulesCensusGap {
            kind: Gap::Unread.into(),
            slot_id: Some(9),
        }],
        slots: vec![
            slot(3, 5, Slot::Failed, SlotReason::RulesMismatch),
            slot(9, 2, Slot::Unknown, SlotReason::Unconfirmed),
        ],
        extra_namespaces: Some(3),
    };
    node_manager::SyncFirewallRulesResponse {
        observation: Some(observation),
        ..Default::default()
    }
}

/// The Report of `answered(round, generation)`.
fn reported(round: u64, generation: u64) -> ReportFirewallRulesRequest {
    let observation = answered(round, generation).observation.unwrap();
    ReportFirewallRulesRequest {
        node_id: NODE.to_string(),
        round: Some(round),
        assignment_generation: Some(generation),
        revision_id: format!("00000000-0000-4000-8000-{generation:012}"),
        manager_epoch: Some(1),
        census: Census::Incomplete.into(),
        census_gaps: observation.census_gaps,
        slots: observation.slots,
        extra_namespaces: Some(3),
    }
}

fn outcome(outcome: FirewallRulesReportOutcome) -> Result<ReportFirewallRulesResponse, Status> {
    Ok(ReportFirewallRulesResponse {
        outcome: outcome.into(),
    })
}

/// Fetch's answer assigning `revision` at `generation`, and the Sync it must
/// become.
fn assigned(
    generation: u64,
    revision: &str,
) -> (
    FetchFirewallRulesResponse,
    node_manager::SyncFirewallRulesRequest,
) {
    let revision_id = format!("00000000-0000-4000-8000-{generation:012}");
    (
        FetchFirewallRulesResponse {
            node_id: NODE.to_string(),
            assignment: Some(FirewallRulesAssignment {
                generation,
                revision_id: revision_id.clone(),
                revision: revision.to_string(),
            }),
            pending_round: None,
        },
        node_manager::SyncFirewallRulesRequest {
            node_id: NODE.to_string(),
            assignment: Some(node_manager::FirewallRulesAssignment {
                generation,
                revision_id,
                revision: revision.to_string(),
            }),
            question: None,
        },
    )
}

#[tokio::test(start_paused = true)]
async fn a_wake_during_delivery_fetches_once_more_and_only_typed_authorized_wakes_count() {
    let mut node = Node::started().await;
    let (b, sync_b) = assigned(2, "revision B");
    let (c, sync_c) = assigned(3, "revision C");

    // Alongside the firewall reason, an ordinary reason still drains.
    node.trigger(
        users::SPACE_NODEAPI,
        &[Reason::FirewallRulesChanged, Reason::NodeUpdated],
        "",
    )
    .await
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(QUIET, node.drains.recv()).await,
        Ok(Some(WorkItem::DrainClusterQueue))
    ));
    node.fetch().await.send(Ok(b)).unwrap();
    let (request, held_b) = node.sync().await;
    assert_eq!(request, sync_b);

    // C is current by the time any of these wakes is answered.
    for _ in 0..3 {
        node.wake().await;
    }
    held_b.send(Ok(Default::default())).unwrap();
    node.fetch().await.send(Ok(c)).unwrap();
    let (request, reply) = node.sync().await;
    assert_eq!(request, sync_c);
    reply.send(Ok(Default::default())).unwrap();

    // A same-spelled diagnostic, an ordinary reason and a refused caller.
    node.trigger(users::SPACE_NODEAPI, &[], "firewall_rules_changed")
        .await
        .unwrap();
    node.trigger(users::SPACE_NODEAPI, &[Reason::NodeUpdated], "")
        .await
        .unwrap();
    node.trigger(users::SPACE_SBXMGR, &[Reason::FirewallRulesChanged], "")
        .await
        .unwrap_err();
    node.quiet().await;
}

#[tokio::test(start_paused = true)]
async fn each_wake_syncs_only_a_fetched_assignment_at_most_once() {
    let mut node = Node::started().await;
    let (empty, sync_empty) = assigned(1, r#"{"schema_version":1,"rules":[]}"#);

    node.wake().await;
    node.fetch()
        .await
        .send(Err(Status::unavailable("cluster API down")))
        .unwrap();

    // A round without its issue time can't be bounded, so it isn't asked about.
    let mut undated = round(1, 1, Duration::ZERO);
    undated.issued_at = None;
    node.fetched(asked(undated)).await;

    // An empty ruleset is an instruction; a failed Sync isn't resent.
    node.wake().await;
    node.fetch().await.send(Ok(empty.clone())).unwrap();
    let (request, reply) = node.sync().await;
    assert_eq!(request, sync_empty);
    reply
        .send(Err(Status::unavailable("node-manager restarting")))
        .unwrap();

    // The next wake redelivers, and a Sync that never answers times out.
    node.wake().await;
    node.fetch().await.send(Ok(empty)).unwrap();
    let _never_answered = node.sync().await;
    node.wake().await;
    node.fetch().await;
}

// Catches a pending round left for a later wake, a question inferred for an
// older peer's round, a generation rewritten, or a report of a refusal, of an
// answer to another round, node or generation, or of a failed Sync.
#[tokio::test(start_paused = true)]
async fn only_an_answer_to_the_fetched_question_is_reported() {
    use FirewallRulesReportOutcome::Accepted;
    let mut node = Node::start();
    let (assignment, sync_assignment) = assigned(13, "revision C");
    let age = Duration::from_secs(5);

    // Pending before start: the startup Fetch asks it.
    node.fetch()
        .await
        .send(Ok(asked(round(7, 12, age))))
        .unwrap();
    let (request, reply) = node.sync().await;
    assert_eq!(request, asking(7, 12));
    let refused = node_manager::SyncFirewallRulesResponse {
        unanswered: "refused".into(),
        ..Default::default()
    };
    reply.send(Ok(refused)).unwrap();

    // Both, neither rewritten, and answers to anything else.
    let both = FetchFirewallRulesResponse {
        pending_round: Some(round(8, 12, age)),
        ..assignment.clone()
    };
    let mut other_node = answered(8, 12);
    other_node.observation.as_mut().unwrap().node_id = "another-node".into();
    let other_answers = [
        Ok(answered(7, 12)),
        Ok(answered(8, 13)),
        Ok(other_node),
        Err(Status::unavailable("node-manager restarting")),
    ];
    for other in other_answers {
        node.fetched(both.clone()).await;
        let (request, reply) = node.sync().await;
        assert_eq!(
            request,
            node_manager::SyncFirewallRulesRequest {
                question: asking(8, 12).question,
                ..sync_assignment.clone()
            }
        );
        reply.send(other).unwrap();
    }

    // An older peer's round has no generation, so only the assignment goes.
    let mut older = round(8, 12, age);
    older.assignment_generation = None;
    node.fetched(FetchFirewallRulesResponse {
        pending_round: Some(older),
        ..assignment
    })
    .await;
    let (request, reply) = node.sync().await;
    assert_eq!(request, sync_assignment);
    reply.send(Ok(answered(8, 12))).unwrap();

    node.fetched(asked(round(9, 12, age))).await;
    let (request, reply) = node.sync().await;
    assert_eq!(request, asking(9, 12));
    reply.send(Ok(answered(9, 12))).unwrap();
    let (report, reply) = node.report().await;
    assert_eq!(report, reported(9, 12));
    reply.send(outcome(Accepted)).unwrap();
    node.quiet().await;
}

// Catches a lost acknowledgement taken as delivery or answered with a Sync, a
// resend that changes the evidence, unbounded resends, a refusal resent, or a
// delayed answer relabeled as the newer round.
#[tokio::test(start_paused = true)]
async fn only_the_same_report_is_resent_and_only_for_a_lost_answer() {
    use FirewallRulesReportOutcome::{Duplicate, NotPending};
    let mut node = Node::started().await;
    let age = Duration::from_secs(5);

    node.fetched(asked(round(7, 12, age))).await;
    let (_, reply) = node.sync().await;
    reply.send(Ok(answered(7, 12))).unwrap();
    // Unanswered, then failed as a commit can be: each is sent again.
    let (first, _lost) = node.report().await;
    let (second, reply) = node.report().await;
    reply.send(Err(Status::internal("commit"))).unwrap();
    let (third, reply) = node.report().await;
    assert_eq!([first, second], [reported(7, 12), reported(7, 12)]);
    assert_eq!(third, reported(7, 12));
    reply.send(outcome(Duplicate)).unwrap();
    node.quiet().await;

    // Two resends at most.
    node.fetched(asked(round(8, 12, age))).await;
    let (_, reply) = node.sync().await;
    reply.send(Ok(answered(8, 12))).unwrap();
    for _ in 0..3 {
        let (_, reply) = node.report().await;
        reply.send(Err(Status::internal("commit"))).unwrap();
    }
    node.quiet().await;

    // Round 10 opens while round 9's answer is on its way.
    node.fetched(asked(round(9, 12, age))).await;
    let (_, held) = node.sync().await;
    node.wake().await;
    held.send(Ok(answered(9, 12))).unwrap();
    let (late, reply) = node.report().await;
    assert_eq!(late, reported(9, 12));
    reply.send(outcome(NotPending)).unwrap();
    node.fetch()
        .await
        .send(Ok(asked(round(10, 12, age))))
        .unwrap();
    let (request, _) = node.sync().await;
    assert_eq!(request, asking(10, 12));
}

// Catches an observation cut at the assignment's budget or waiting past
// node-manager's limit, a question asked without time left to report it, a
// timed-out Sync taken as an answer, a round not asked again, or delivery
// that outlives its handle.
#[tokio::test(start_paused = true)]
async fn the_round_bounds_its_observation_and_report() {
    use FirewallRulesReportOutcome::Accepted;
    let mut node = Node::started().await;
    let fresh = Duration::from_secs(100);

    node.fetched(asked(round(7, 12, fresh))).await;
    let ((_, reply), budget) = node.sync_within().await;
    assert_eq!(budget, OBSERVE_BUDGET);
    tokio::time::sleep(Duration::from_secs(30)).await;
    reply.send(Ok(answered(7, 12))).unwrap();
    let (report, reply) = node.report().await;
    assert_eq!(report, reported(7, 12));
    reply.send(outcome(Accepted)).unwrap();

    // Too old to report in time, so only the assignment goes.
    let (assignment, sync_assignment) = assigned(12, "revision B");
    node.fetched(FetchFirewallRulesResponse {
        pending_round: Some(round(8, 12, Duration::from_secs(280))),
        ..assignment
    })
    .await;
    let ((request, reply), budget) = node.sync_within().await;
    assert_eq!((request, budget), (sync_assignment, SYNC_BUDGET));
    reply.send(Ok(Default::default())).unwrap();

    // An older round leaves its Report time; a Sync that never answers ends
    // the pass, and the next one asks again.
    let older = Duration::from_secs(200);
    node.fetched(asked(round(9, 12, older))).await;
    let (_never_answered, budget) = node.sync_within().await;
    assert!(
        (SYNC_BUDGET..OBSERVE_BUDGET).contains(&budget),
        "{budget:?}"
    );
    node.wake().await;
    node.fetch()
        .await
        .send(Ok(asked(round(9, 12, older))))
        .unwrap();
    let (request, reply) = node.sync().await;
    assert_eq!(request, asking(9, 12));
    reply.send(Ok(answered(9, 12))).unwrap();

    // Dropping the handle during a resend's backoff stops every hop.
    let (_, reply) = node.report().await;
    reply.send(Err(Status::internal("commit"))).unwrap();
    let Node {
        mut calls,
        _delivery,
        ..
    } = node;
    drop(_delivery);
    let next = tokio::time::timeout(QUIET, calls.recv()).await;
    assert!(matches!(next, Ok(None)), "{next:?}");
}

use pplx_space_host_layout::identities::{LinuxUser, users};
use pplx_space_node_api_client::proto::node_ctl_server::NodeCtl as _;
use pplx_space_node_api_client::proto::{
    FirewallRulesPendingRound, StateReconciliationReason as Reason,
    TriggerStateReconciliationRequest,
};
use pplx_space_uds_peer_auth::PeerIdentity;
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::{NodeCtlService, WorkItem};

/// Twice the longer hop budget: what hasn't happened by then won't.
const QUIET: Duration = Duration::from_secs(20);
const NODE: &str = "node-from-fetch";

type Reply<T> = oneshot::Sender<Result<T, Status>>;

#[derive(Debug)]
enum Call {
    Fetch(Reply<FetchFirewallRulesResponse>),
    Sync(
        node_manager::SyncFirewallRulesRequest,
        Reply<node_manager::SyncFirewallRulesResponse>,
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
    ) -> Result<node_manager::SyncFirewallRulesResponse, Status> {
        let (reply, receiver) = oneshot::channel();
        self.0
            .send(Call::Sync(request, reply))
            .expect("test is listening");
        answer(receiver).await
    }
}

/// The real handler with delivery on, and what reaches each side of it.
struct Node {
    service: NodeCtlService,
    calls: mpsc::UnboundedReceiver<Call>,
    drains: mpsc::Receiver<WorkItem>,
}

impl Node {
    fn start() -> Self {
        let (calls_tx, calls) = mpsc::unbounded_channel();
        let (tx, drains) = mpsc::channel(8);
        let service = NodeCtlService {
            cluster_reconcile_trigger: crate::ClusterReconcileTrigger::new(tx.clone(), NODE),
            tx,
            db: Arc::new(std::sync::Mutex::new(
                rusqlite::Connection::open_in_memory().expect("open in-memory SQLite"),
            )),
            peer_authorizer: crate::peer_authorizer(),
            firewall_rules: Some(spawn(Scripted(calls_tx))),
        };
        Self {
            service,
            calls,
            drains,
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
        let Call::Sync(request, reply) = self.next().await else {
            panic!("expected a sync")
        };
        (request, reply)
    }

    async fn quiet(&mut self) {
        let call = tokio::time::timeout(QUIET, self.calls.recv()).await;
        assert!(call.is_err(), "unexpected hop: {call:?}");
    }
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
        },
    )
}

#[tokio::test(start_paused = true)]
async fn a_wake_during_delivery_fetches_once_more_and_only_typed_authorized_wakes_count() {
    let mut node = Node::start();
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
    let mut node = Node::start();
    let (empty, sync_empty) = assigned(1, r#"{"schema_version":1,"rules":[]}"#);

    node.wake().await;
    node.fetch()
        .await
        .send(Err(Status::unavailable("cluster API down")))
        .unwrap();

    // No assignment, even with a round pending, leaves node-manager alone.
    node.wake().await;
    node.fetch()
        .await
        .send(Ok(FetchFirewallRulesResponse {
            node_id: NODE.to_string(),
            assignment: None,
            pending_round: Some(FirewallRulesPendingRound {
                round: 1,
                issued_at: None,
            }),
        }))
        .unwrap();

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

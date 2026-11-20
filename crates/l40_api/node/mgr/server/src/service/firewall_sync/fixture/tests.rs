//! `SyncFirewallRules` through the real handler, peer check, assignment store
//! and slot apply, against the slot apply's fake nft. Slot 0 is as eligible as
//! the bound slot, so an apply there would show. `linux` repeats one delivery
//! against nftables.

use std::fs::File;
use std::io;

use pplx_space_host_layout::identities::users;
use pplx_space_uds_peer_auth::PeerIdentity;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::{Code, Request, Response};

use super::*;
use crate::proto::FirewallRulesAssignment;
use crate::service::tests::service_tests::service_for_unit_tests;
use crate::slot_pool::firewall_rules::tests::{
    BOUND, FakeHost, Fault, Hook, NODE, bounded, busy, document, hook, installed, set_slot,
};
use crate::slot_pool::{NamespaceIdentity, NetworkPoolConfig, SlotState};

mod linux;

const SLOT: u32 = 1;
/// Within `BOUND`, so a reply always comes before the test gives up.
const WAIT: Duration = Duration::from_secs(5);

struct Bound {
    _dir: TempDir,
    path: PathBuf,
    host: Arc<FakeHost>,
    /// The bound slot's namespace.
    namespace: NamespaceIdentity,
    service: NodeManagerService,
}

/// Two leased slots, `SLOT` bound, and `accepted` already in the record.
async fn bind(fault: Option<Fault>, accepted: Option<u64>, wait: Duration) -> Bound {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    if let Some(generation) = accepted {
        let document = document(generation);
        FirewallAssignments::open(&path)
            .unwrap()
            .accept(NODE, incoming(generation, &document))
            .unwrap();
    }
    let host = FakeHost::new(fault);
    host.name(0, "uts");
    let namespace = host.name(SLOT, "net");
    let mut service = service_for_unit_tests().await;
    service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
    for slot in [0, SLOT] {
        set_slot(&service.pool, slot, SlotState::Leased, 1);
    }
    let fake = host.clone();
    let nft = move |netns: &File, args: &[&str], stdin: &[u8], _| {
        fake.reply(netns, args, stdin)
            .ok_or_else(|| io::ErrorKind::TimedOut.into())
    };
    let service = service
        .with_firewall_sync(FirewallSyncFixture {
            node_id: NODE,
            slot_id: SLOT,
            assignment_path: path.clone(),
            host: host.clone(),
            nft: Arc::new(nft),
            wait,
        })
        .unwrap();
    Bound {
        _dir: dir,
        path,
        host,
        namespace,
        service,
    }
}

fn incoming(generation: u64, document: &str) -> IncomingAssignment<'_> {
    IncomingAssignment {
        node_id: NODE,
        generation,
        revision_id: Uuid::from_u128(generation.into()),
        document,
    }
}

fn delivery(generation: u64) -> SyncFirewallRulesRequest {
    SyncFirewallRulesRequest {
        node_id: NODE.to_string(),
        assignment: Some(FirewallRulesAssignment {
            generation,
            revision_id: Uuid::from_u128(generation.into()).to_string(),
            revision: document(generation),
        }),
    }
}

fn node_ctl() -> PeerIdentity {
    let user = users::SPACE_NODECTL;
    PeerIdentity::new(user.uid, user.primary_group.gid, None)
}

async fn sync(
    service: &NodeManagerService,
    body: SyncFirewallRulesRequest,
) -> Result<SyncFirewallRulesResponse, Status> {
    let mut request = Request::new(body);
    request.extensions_mut().insert(node_ctl());
    bounded(service.sync_firewall_rules(request))
        .await
        .map(Response::into_inner)
}

/// Acceptance and dispatch as one comparable value.
fn reply(
    result: Result<SyncFirewallRulesResponse, Status>,
) -> (u64, FirewallAssignmentAcceptance, Outcome) {
    let reply = result.unwrap();
    let outcome = reply.dispatch.as_ref().unwrap().outcome();
    (reply.accepted_generation, reply.acceptance(), outcome)
}

/// One gate on `hook`: fires `entered`, then waits until `release` is sent or dropped.
fn gate(slot: &Hook) -> (oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (enter, entered) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    hook(slot, move || {
        let _ = enter.send(());
        let _ = released.recv_timeout(BOUND);
    });
    (entered, release)
}

/// Waits until `generation` is the one armed.
async fn armed(service: &NodeManagerService, generation: u64) {
    let sync = service.firewall_sync.clone().unwrap();
    let mut wanted = sync.wanted.subscribe();
    bounded(wanted.wait_for(|wanted| wanted.as_ref().is_some_and(|w| w.generation == generation)))
        .await
        .unwrap();
}

/// Until the slot apply's job returns.
async fn applied(service: &NodeManagerService) {
    service.pool.task_tracker.close();
    bounded(service.pool.task_tracker.wait()).await;
}

// Catches a delivery accepted from any peer but node-ctl, or without a binding.
#[tokio::test]
async fn only_node_ctl_reaches_the_binding() {
    let bound = bind(None, None, WAIT).await;
    let unbound = service_for_unit_tests().await;
    let ctl_group = users::SPACE_NODECTL.primary_group.gid;
    let peer = |uid, gid| Some(PeerIdentity::new(uid, gid, None));
    for (service, peer, code) in [
        (&bound.service, None, Code::Unauthenticated),
        (&bound.service, peer(0, 0), Code::PermissionDenied),
        // In the broader node-api list.
        (
            &bound.service,
            peer(users::SPACE_NODEAPI.uid, ctl_group),
            Code::PermissionDenied,
        ),
        // Shares node-ctl's group.
        (
            &bound.service,
            peer(users::SPACE_SBXMGR.uid, ctl_group),
            Code::PermissionDenied,
        ),
        (&unbound, Some(node_ctl()), Code::FailedPrecondition),
    ] {
        let mut request = Request::new(delivery(12));
        if let Some(peer) = peer.clone() {
            request.extensions_mut().insert(peer);
        }
        let error = bounded(service.sync_firewall_rules(request))
            .await
            .unwrap_err();
        assert_eq!(error.code(), code, "{peer:?}");
    }
    assert!(!bound.path.exists(), "a refused delivery was written");
    assert_eq!(bound.host.calls(), []);
}

// Catches an invalid, foreign, stale, conflicting or unwritten delivery
// written or applied.
#[tokio::test]
async fn refused_deliveries_neither_write_nor_apply() {
    let bound = bind(None, None, WAIT).await;
    let node = |node_id: String| SyncFirewallRulesRequest {
        node_id,
        ..delivery(12)
    };
    let assignment = |edit: fn(&mut FirewallRulesAssignment)| {
        let mut body = delivery(12);
        edit(body.assignment.as_mut().unwrap());
        body
    };
    let missing = SyncFirewallRulesRequest {
        assignment: None,
        ..delivery(12)
    };
    let refused = [
        (missing, Code::InvalidArgument),
        (
            node(NODE.as_uuid().simple().to_string()),
            Code::InvalidArgument,
        ),
        (
            assignment(|a| a.revision_id = a.revision_id.to_uppercase()),
            Code::InvalidArgument,
        ),
        (
            assignment(|a| a.revision = "{}".into()),
            Code::InvalidArgument,
        ),
        (
            node(Uuid::from_u128(2).to_string()),
            Code::FailedPrecondition,
        ),
    ];
    for (body, code) in refused {
        let error = sync(&bound.service, body.clone()).await.unwrap_err();
        assert_eq!(error.code(), code, "{body:?}");
    }
    assert!(!bound.path.exists(), "a refused delivery was written");

    sync(&bound.service, delivery(12)).await.unwrap();
    let record = std::fs::read(&bound.path).unwrap();
    let conflicting = assignment(|a| a.revision = document(13));
    for (body, code) in [
        (delivery(11), Code::Aborted),
        (conflicting, Code::FailedPrecondition),
    ] {
        assert_eq!(sync(&bound.service, body).await.unwrap_err().code(), code);
    }
    assert_eq!(std::fs::read(&bound.path).unwrap(), record);
    assert_eq!(bound.host.installs().len(), 1);

    let unwritable = bind(None, None, WAIT).await;
    std::fs::remove_dir(unwritable.path.parent().unwrap()).unwrap();
    let error = sync(&unwritable.service, delivery(12)).await.unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert_eq!(unwritable.host.calls(), []);
}

// Catches an apply before the record is durable, one to a slot other than the
// bound one, or acceptance reported as installation.
#[tokio::test]
async fn a_fresh_assignment_is_durable_before_the_bound_slot_applies_it() {
    let bound = bind(None, None, WAIT).await;
    let (path, (durable, seen)) = (bound.path.clone(), oneshot::channel());
    hook(&bound.host.looked_up, move || {
        let reopened = FirewallAssignments::open(&path).unwrap();
        let _ = durable.send(
            reopened
                .current()
                .unwrap()
                .map(|current| current.generation),
        );
    });
    let replied = reply(sync(&bound.service, delivery(12)).await);
    assert_eq!(bounded(seen).await.unwrap(), Some(12));
    assert_eq!(
        replied,
        (
            12,
            FirewallAssignmentAcceptance::Committed,
            Outcome::Matched
        )
    );
    assert_eq!(bound.host.installs(), [bound.namespace]);
}

// Catches a dispatch tied to its caller, a replay taken as proof of an
// attempt, an unbounded wait, or a second submission for one generation.
#[tokio::test]
async fn deliveries_of_a_generation_share_its_one_attempt() {
    // Accepted by an earlier owner, so every delivery here is a replay.
    let bound = bind(None, Some(12), Duration::from_millis(100)).await;
    let record = std::fs::read(&bound.path).unwrap();
    let (entered, release) = gate(&bound.host.looked_up);
    let first = tokio::spawn({
        let service = bound.service.clone();
        async move { sync(&service, delivery(12)).await }
    });
    // Accepted and claimed, but nothing submitted.
    bounded(entered).await.unwrap();
    first.abort();
    assert!(bounded(first).await.unwrap_err().is_cancelled());

    let replayed = FirewallAssignmentAcceptance::Replayed;
    let waiting = reply(sync(&bound.service, delivery(12)).await);
    assert_eq!(waiting, (12, replayed, Outcome::Pending));
    release.send(()).unwrap();
    applied(&bound.service).await;
    let finished = reply(sync(&bound.service, delivery(12)).await);
    assert_eq!(finished, (12, replayed, Outcome::Matched));
    assert_eq!(bound.host.installs(), [bound.namespace]);
    assert_eq!(std::fs::read(&bound.path).unwrap(), record);
}

// Catches a busy slot reported as attempted, or not retried on redelivery.
#[tokio::test]
async fn a_busy_slot_runs_nothing_until_redelivery() {
    let bound = bind(None, None, WAIT).await;
    let held = busy(&bound.service.pool, SLOT);
    let refused = sync(&bound.service, delivery(12)).await.unwrap();
    let dispatch = refused.dispatch.unwrap();
    assert_eq!(dispatch.outcome(), Outcome::NotRun);
    assert!(
        dispatch.detail.contains("owned by other work"),
        "{dispatch:?}"
    );
    assert_eq!(bound.host.calls(), []);

    drop(held);
    let retried = reply(sync(&bound.service, delivery(12)).await);
    assert_eq!(
        retried,
        (12, FirewallAssignmentAcceptance::Replayed, Outcome::Matched)
    );
    assert_eq!(bound.host.installs(), [bound.namespace]);
}

// Catches a submitted generation submitted again, an outcome misreported, or
// work after an attempt whose kernel state is unknown.
#[tokio::test]
async fn an_attempt_is_never_resubmitted() {
    for (fault, reset, outcome, ends) in [
        (Some(Fault::Drops), false, Outcome::Mismatched, false),
        (None, true, Outcome::Stale, false),
        (Some(Fault::Unlistable), false, Outcome::Inconclusive, true),
        (Some(Fault::Panics), false, Outcome::Unknown, true),
    ] {
        let bound = bind(fault, None, WAIT).await;
        if reset {
            let pool = bound.service.pool.clone();
            hook(&bound.host.installed, move || {
                set_slot(&pool, SLOT, SlotState::Dirty, 1);
            });
        }
        let committed = FirewallAssignmentAcceptance::Committed;
        let first = reply(sync(&bound.service, delivery(12)).await);
        assert_eq!(first, (12, committed, outcome));
        let record = std::fs::read(&bound.path).unwrap();
        let again = sync(&bound.service, delivery(12)).await;
        if ends {
            assert_eq!(again.unwrap_err().code(), Code::FailedPrecondition);
            let newer = sync(&bound.service, delivery(13)).await.unwrap_err();
            assert_eq!(newer.code(), Code::FailedPrecondition, "{outcome:?}");
        } else {
            let replayed = FirewallAssignmentAcceptance::Replayed;
            assert_eq!(reply(again), (12, replayed, outcome));
        }
        assert_eq!(std::fs::read(&bound.path).unwrap(), record, "{outcome:?}");
        assert_eq!(bound.host.installs().len(), 1, "{outcome:?}");
    }
}

// Catches older work run or reported after a newer generation is accepted.
#[tokio::test]
async fn a_newer_generation_supersedes_older_work() {
    let bound = bind(None, None, WAIT).await;
    let (entered, release) = gate(&bound.host.installed);
    let deliver = |generation| {
        let service = bound.service.clone();
        tokio::spawn(async move { reply(sync(&service, delivery(generation)).await) })
    };
    // 11 is submitted, 12 waits behind it, then 13 replaces 12.
    let running = deliver(11);
    bounded(entered).await.unwrap();
    let queued = deliver(12);
    armed(&bound.service, 12).await;
    let newest = deliver(13);
    armed(&bound.service, 13).await;
    release.send(()).unwrap();

    let committed = FirewallAssignmentAcceptance::Committed;
    let expected = [
        (running, Outcome::Superseded),
        (queued, Outcome::Superseded),
        (newest, Outcome::Matched),
    ];
    for (generation, (delivered, outcome)) in (11..).zip(expected) {
        let replied = bounded(delivered).await.unwrap();
        assert_eq!(replied, (generation, committed, outcome));
    }
    assert_eq!(bound.host.installs(), [bound.namespace; 2]);
    let tables = bound.host.tables.lock().unwrap();
    assert_eq!(tables[&bound.namespace], installed(13));
}

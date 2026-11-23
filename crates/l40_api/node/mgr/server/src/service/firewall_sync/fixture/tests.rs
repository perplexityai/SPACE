//! `SyncFirewallRules` through the real handler, peer check, assignment store
//! and slot apply, against the slot apply's fake nft. Slot 0 is as eligible as
//! the bound slot, so an apply there would show. `linux` repeats one delivery
//! against nftables.

use std::fs::File;
use std::io;

use pplx_space_host_layout::identities::users;
use pplx_space_uds_peer_auth::{LocalRole, PeerIdentity, UdsPeerAccount, UdsPeerAuthorizer};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::{Code, Request, Response};

use super::*;
use crate::firewall::rules::install::InstallOutcome;
use crate::proto::FirewallRulesAssignment;
use crate::service::tests::service_tests::service_for_unit_tests;
use crate::slot_pool::firewall_rules::tests::{
    BOUND, FakeHost, Fault, Hook, NODE, bounded, busy, document, hook, installed, set_slot,
};
use crate::slot_pool::{
    NamespaceIdentity, NetworkPoolConfig, NotRunnable, SlotFirewallAttempt, SlotHealth, SlotState,
    SlotStoreError,
};

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

fn nothing_armed(service: &NodeManagerService) -> bool {
    let sync = service.firewall_sync.clone().unwrap();
    sync.wanted.borrow().is_none()
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
    // Unbound, so an admin let through fails later with FAILED_PRECONDITION.
    let mut admin = service_for_unit_tests().await;
    let admin_uid = 4242;
    let account =
        UdsPeerAccount::role_with_fallback_uid("space-test-admin", LocalRole::Admin, admin_uid);
    admin.peer_authorizer = UdsPeerAuthorizer::new("space-node-manager", [account]);
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
        (&admin, peer(admin_uid, admin_uid), Code::PermissionDenied),
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
    assert!(
        nothing_armed(&bound.service),
        "a refused delivery was dispatched"
    );

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
    assert!(nothing_armed(&unwritable.service));
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

// Catches a dispatch tied to its caller, a joining delivery that starts a
// second attempt, an unbounded wait, or a completed attempt submitted again.
#[tokio::test]
async fn deliveries_of_a_generation_share_its_one_attempt() {
    let bound = bind(None, None, Duration::from_millis(100)).await;
    let (entered, release) = gate(&bound.host.installed);
    let mut request = Request::new(delivery(12));
    request.extensions_mut().insert(node_ctl());
    // Polled once, which starts the acceptance, then dropped.
    {
        let mut first = std::pin::pin!(bound.service.sync_firewall_rules(request));
        let waker = std::task::Waker::noop();
        let polled = first
            .as_mut()
            .poll(&mut std::task::Context::from_waker(waker));
        assert!(polled.is_pending());
    }
    // No caller waits, yet the accepted generation reaches nft.
    bounded(entered).await.unwrap();
    let record = std::fs::read(&bound.path).unwrap();

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

// Catches a replay taken as proof of an attempt, a busy slot reported as
// attempted, or one not retried on redelivery.
#[tokio::test]
async fn a_busy_slot_runs_nothing_until_redelivery() {
    // Accepted by an earlier owner, so every delivery here is a replay.
    let bound = bind(None, Some(12), WAIT).await;
    let held = busy(&bound.service.pool, SLOT);
    let refused = sync(&bound.service, delivery(12)).await.unwrap();
    assert_eq!(refused.acceptance(), FirewallAssignmentAcceptance::Replayed);
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

fn attempted(readback: Verification, current: Result<(), Stale>) -> SlotFirewallReport {
    SlotFirewallReport::Attempted(Box::new(SlotFirewallAttempt {
        node_id: NODE,
        assignment_generation: 12,
        revision_id: Uuid::from_u128(12),
        slot_id: SLOT,
        slot_generation: 1,
        sandbox_id: None,
        namespace: NamespaceIdentity {
            device: 1,
            inode: 1,
        },
        submitted: InstallOutcome::Committed,
        readback,
        current,
    }))
}

// Catches an outcome misreported, or a binding that goes on after a changed
// target or an unknown result, or ends after anything else.
#[tokio::test]
async fn each_report_is_one_outcome_and_only_drift_or_unknown_ends() {
    let cancelled = tokio::spawn(std::future::pending::<()>());
    cancelled.abort();
    let unknown = cancelled.await.unwrap_err();
    let not_run = |not_run| Ok(SlotFirewallReport::NotRun(not_run));
    let attempt = |readback, current| Ok(attempted(readback, current));
    let current = |readback| attempt(readback, Ok(()));
    let stale = |stale| attempt(Verification::Match, Err(stale));
    let ineligible = NotRun::Ineligible {
        state: SlotState::Dirty,
        health: SlotHealth::Healthy,
    };
    let gone = || io::ErrorKind::NotFound.into();
    let mismatch = || Verification::Mismatch(String::new());
    let inconclusive = || Verification::Inconclusive(String::new());
    let assignment = || Stale::Assignment(NotRunnable::NoAssignment);
    let poisoned = || SlotStoreError::LockPoisoned;
    let rows = [
        (Outcome::Unknown, true, vec![Err(unknown)]),
        (
            Outcome::NotRun,
            false,
            vec![
                not_run(NotRun::Busy),
                not_run(NotRun::ShuttingDown),
                not_run(NotRunnable::NoAssignment.into()),
                not_run(poisoned().into()),
            ],
        ),
        (
            Outcome::NotRun,
            true,
            vec![
                not_run(NotRun::NoSlot),
                not_run(ineligible),
                not_run(NotRun::Changed),
                not_run(NotRun::Namespace(gone())),
            ],
        ),
        (Outcome::Matched, false, vec![current(Verification::Match)]),
        (Outcome::Mismatched, false, vec![current(mismatch())]),
        (
            Outcome::Inconclusive,
            true,
            vec![
                current(inconclusive()),
                attempt(inconclusive(), Err(assignment())),
            ],
        ),
        (Outcome::Stale, false, vec![stale(assignment())]),
        (
            Outcome::Stale,
            true,
            vec![
                stale(Stale::Slot),
                stale(Stale::SlotStore(poisoned())),
                stale(Stale::Namespace(Err(gone()))),
                attempt(mismatch(), Err(Stale::Slot)),
            ],
        ),
    ];
    for (outcome, ends, reports) in rows {
        for report in reports {
            let (done, ended) = dispatch(&report);
            assert_eq!((done.outcome(), ended), (outcome, ends), "{report:?}");
        }
    }
}

// Catches a mismatched generation submitted again, work run against a
// namespace replaced between attempts, a binding-ending result not reaching the
// reply, or a later sequential delivery admitted, written or applied.
#[tokio::test]
async fn a_target_replaced_between_attempts_ends_the_binding() {
    let bound = bind(Some(Fault::Drops), None, WAIT).await;
    let committed = FirewallAssignmentAcceptance::Committed;
    let first = reply(sync(&bound.service, delivery(12)).await);
    assert_eq!(first, (12, committed, Outcome::Mismatched));
    let again = reply(sync(&bound.service, delivery(12)).await);
    let replayed = FirewallAssignmentAcceptance::Replayed;
    assert_eq!(again, (12, replayed, Outcome::Mismatched));
    bound.host.name(SLOT, "ipc");
    let replaced = reply(sync(&bound.service, delivery(13)).await);
    assert_eq!(replaced, (13, committed, Outcome::NotRun));
    let record = std::fs::read(&bound.path).unwrap();
    for generation in [13, 14] {
        let refused = sync(&bound.service, delivery(generation))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::FailedPrecondition);
        assert_eq!(refused.message(), "the firewall rule sync binding ended");
    }
    assert_eq!(std::fs::read(&bound.path).unwrap(), record);
    assert_eq!(bound.host.installs(), [bound.namespace]);
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

// Catches work queued behind an attempt that ends the binding run, or reported
// as anything but not run.
#[tokio::test]
async fn work_queued_when_the_binding_ends_never_runs() {
    let bound = bind(Some(Fault::Unlistable), None, WAIT).await;
    let (entered, release) = gate(&bound.host.installed);
    let deliver = |generation| {
        let service = bound.service.clone();
        tokio::spawn(async move { sync(&service, delivery(generation)).await.unwrap() })
    };
    let ending = deliver(11);
    bounded(entered).await.unwrap();
    let queued = deliver(12);
    armed(&bound.service, 12).await;
    release.send(()).unwrap();

    let ending = bounded(ending).await.unwrap().dispatch.unwrap();
    assert_eq!(ending.outcome(), Outcome::Superseded);
    let queued = bounded(queued).await.unwrap().dispatch.unwrap();
    assert_eq!(
        (queued.outcome(), queued.detail.as_str()),
        (Outcome::NotRun, "the binding ended")
    );
    // Only the worker is left on the tracker, so this waits for it to exit.
    bound.service.device_op_tasks.close();
    bounded(bound.service.device_op_tasks.wait()).await;
    assert_eq!(bound.host.installs(), [bound.namespace]);
}

//! Gated starts through `launch_admitted_runtime` under a node-wide binding,
//! against the fake nft and a scripted launcher that records what it ran under.

use std::sync::Mutex;

use pplx_space_node_runtime_core::proto::{LaunchRuntimeRequest, LaunchRuntimeResponse};
use pplx_space_sandbox_model::SandboxActivationAttemptId;
use serde_json::Value;

use super::*;
use crate::firewall::assignment::tests::make_unavailable;
use crate::net::SlotInfo;
use crate::runtime_supervisor_client::tests::{SupervisorScript, scripted_in_process_client};
use crate::slot_pool::NewLeaseMemoryReservation;

/// The accepted generation and the leased slot's table as the launcher ran.
type Seen = (Option<u64>, Option<Vec<Value>>);

type Launched = Arc<Mutex<Vec<Seen>>>;

/// Two ready slots under a node-wide binding, with `accepted` in the record.
async fn bind_all(fault: Option<Fault>, accepted: Option<u64>) -> Bound {
    let ready = [SlotState::Ready; 2];
    bind_slots(BoundSlots::All, fault, accepted, WAIT, &ready).await
}

/// A pooled create's lease of the lowest ready slot.
pub(super) async fn pooled(service: &NodeManagerService) -> SlotInfo {
    let reservation = NewLeaseMemoryReservation {
        memory_reservation_mib: 8320,
    };
    let id = pplx_space_sandbox_model::SandboxId::new().to_string();
    let lease = service.pool.allocate(&id, None, Some(reservation));
    bounded(lease).await.unwrap()
}

/// A launcher that records what it ran under, then answers with `reply`.
fn launcher(
    bound: &mut Bound,
    lease: &SlotInfo,
    reply: impl Fn() -> Result<(), Status> + Send + Sync + 'static,
) -> Launched {
    let launched = Launched::default();
    let (seen, host) = (launched.clone(), bound.host.clone());
    let assignments = bound
        .service
        .firewall_sync
        .clone()
        .unwrap()
        .assignments
        .clone();
    let namespace = identity(&bound.host.link(lease.slot));
    bound.service.runtime_supervisor = scripted_in_process_client(SupervisorScript {
        launch: Some(Arc::new(move |_| {
            let accepted = assignments.current().ok().flatten();
            let table = host.tables.lock().unwrap().get(&namespace).cloned();
            seen.lock()
                .unwrap()
                .push((accepted.map(|accepted| accepted.generation), table));
            reply().map(|()| LaunchRuntimeResponse::default())
        })),
        ..Default::default()
    });
    launched
}

fn identity(path: &Path) -> NamespaceIdentity {
    let entry = std::fs::metadata(path).unwrap();
    NamespaceIdentity {
        device: entry.dev(),
        inode: entry.ino(),
    }
}

/// Launches `lease`'s runtime into the namespace entry `namespace`.
pub(super) async fn start(
    service: &NodeManagerService,
    lease: &SlotInfo,
    namespace: &Path,
) -> Result<(), Status> {
    let request = LaunchRuntimeRequest {
        sandbox_id: lease.sandbox_id.clone(),
        execution_id: SandboxActivationAttemptId::new().to_string(),
        runtime: pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32,
        launcher_binary: Some("/runtime/bin/jailer".into()),
        runtime_binary: "/runtime/bin/firecracker".into(),
        workspace_root: "/runtime/jails".into(),
        network_namespace: namespace.display().to_string(),
        ..Default::default()
    };
    bounded(service.launch_admitted_runtime(lease, request))
        .await
        .map(drop)
}

/// Why a start in `a_start_that_does_not_verify_never_launches` must not launch.
#[derive(Clone, Copy, Debug)]
enum Refusal {
    NoAssignment,
    Unavailable,
    /// The sweep matched the slot while idle; the lease's own readback mismatches.
    MatchedIdleThenMismatched,
    Inconclusive,
    UnreadableNamespace,
    /// The slot is leased again after the launch read its lease.
    LeasedAgain,
    /// The launch names slot 1's namespace entry.
    OtherNamespace,
}

// Catches a start launched without a usable assignment, on an idle slot's
// earlier result, on a readback that mismatched, was inconclusive or never
// ran, or for an occupant or namespace other than the launch's own.
#[tokio::test]
async fn a_start_that_does_not_verify_never_launches() {
    use Refusal::*;
    for case in [
        NoAssignment,
        Unavailable,
        MatchedIdleThenMismatched,
        Inconclusive,
        UnreadableNamespace,
        LeasedAgain,
        OtherNamespace,
    ] {
        let fault = matches!(case, Inconclusive).then_some(Fault::Unlistable);
        let accepted = (!matches!(case, NoAssignment | MatchedIdleThenMismatched)).then_some(12);
        let mut bound = bind_all(fault, accepted).await;
        if let MatchedIdleThenMismatched = case {
            sync_all(&bound.service, 12).await;
            let matched = Some(Outcome::Matched);
            let swept = passed(&bound.service, 12).await.unwrap();
            assert_eq!(swept, [(0, matched), (1, matched)]);
        }
        let lease = pooled(&bound.service).await;
        let launched = launcher(&mut bound, &lease, || Ok(()));
        let mut link = bound.host.link(0);
        match case {
            NoAssignment | Inconclusive => {}
            Unavailable => {
                make_unavailable(&bound.service.firewall_sync.clone().unwrap().assignments);
            }
            MatchedIdleThenMismatched => {
                let (fake, first) = (bound.host.clone(), bound.namespaces[0]);
                hook(&bound.host.installed, move || {
                    fake.tables.lock().unwrap().insert(first, installed(11));
                });
            }
            UnreadableNamespace => std::fs::remove_file(&link).unwrap(),
            LeasedAgain => {
                let again = i64::try_from(lease.generation + 1).unwrap();
                set_slot(&bound.service.pool, 0, SlotState::Leased, again);
            }
            OtherNamespace => link = bound.host.link(1),
        }
        let installs = bound.host.installs().len();
        let refused = start(&bound.service, &lease, &link).await.unwrap_err();
        assert_eq!(
            refused.code(),
            Code::FailedPrecondition,
            "{case:?}: {refused}"
        );
        assert_eq!(*launched.lock().unwrap(), [], "{case:?}");
        if let MatchedIdleThenMismatched = case {
            assert_eq!(
                bound.host.installs().len(),
                installs + 1,
                "no fresh install"
            );
        }
    }
}

// Catches a start that launches on its proof of 12 after 13 is accepted, lets
// 13 be accepted between its verification and its launch, or skips its own
// verification of 13.
#[tokio::test(flavor = "multi_thread")]
async fn acceptance_is_ordered_against_a_start() {
    // 13 is accepted first, so the start verifies 13 itself after 13's sweep.
    let mut bound = bind_all(None, Some(12)).await;
    let lease = pooled(&bound.service).await;
    let launched = launcher(&mut bound, &lease, || Ok(()));
    sync_all(&bound.service, 13).await;
    start(&bound.service, &lease, &bound.host.link(0))
        .await
        .unwrap();
    assert_eq!(*launched.lock().unwrap(), [(Some(13), Some(installed(13)))]);
    let [first, second] = bound.namespaces[..] else {
        unreachable!()
    };
    let expected = [first, second, first].map(|namespace| (namespace, installed(13)));
    assert_eq!(bound.host.submissions(), expected);

    // The start wins: 13 queues behind it from its install of 12 to its launch.
    let mut bound = bind_all(None, Some(12)).await;
    let lease = pooled(&bound.service).await;
    let launched = launcher(&mut bound, &lease, || Ok(()));
    let accepting = queued_behind_a_start(&bound, &lease).await;
    let committed = FirewallAssignmentAcceptance::Committed;
    assert_eq!(
        bounded(accepting).await.unwrap().unwrap().acceptance(),
        committed
    );
    assert_eq!(*launched.lock().unwrap(), [(Some(12), Some(installed(12)))]);
}

/// Pauses a start of `lease` after its install, queues a delivery of 13 behind
/// it, and returns that delivery once the start returned.
async fn queued_behind_a_start(
    bound: &Bound,
    lease: &SlotInfo,
) -> tokio::task::JoinHandle<Result<SyncFirewallRulesResponse, Status>> {
    let binding = bound.service.firewall_sync.clone().unwrap();
    let (entered, release) = gate(&bound.host.installed);
    let (service, leased, link) = (bound.service.clone(), lease.clone(), bound.host.link(0));
    let starting = tokio::spawn(async move { start(&service, &leased, &link).await });
    bounded(entered).await.unwrap();
    assert!(
        binding.order.try_write().is_err(),
        "the start does not hold the order"
    );
    let service = bound.service.clone();
    let accepting = tokio::spawn(async move { sync(&service, delivery(13)).await });
    let accepted = || binding.assignments.current().unwrap().unwrap().generation;
    // A queued writer takes the lock's free permits, so readers then fail.
    bounded(async {
        while binding.order.try_read().is_ok() && accepted() == 12 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(accepted(), 12, "13 was accepted during the start");
    release.send(()).unwrap();
    let _ = bounded(starting).await.unwrap();
    accepting
}

// Catches a launch whose outcome is unknown treated as never started: the
// acceptance queued behind it and any later start must be refused.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_launch_outcome_ends_the_binding() {
    let mut bound = bind_all(None, Some(12)).await;
    let lease = pooled(&bound.service).await;
    let launched = launcher(&mut bound, &lease, || {
        Err(Status::deadline_exceeded("launch timed out"))
    });
    let accepting = queued_behind_a_start(&bound, &lease).await;
    let refused = bounded(accepting).await.unwrap().unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert_eq!(launched.lock().unwrap().len(), 1);

    let refused = start(&bound.service, &lease, &bound.host.link(0))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert_eq!(launched.lock().unwrap().len(), 1);
    assert_eq!(bound.host.installs(), [bound.namespaces[0]]);
    let sync = bound.service.firewall_sync.clone().unwrap();
    assert_eq!(sync.assignments.current().unwrap().unwrap().generation, 12);
}

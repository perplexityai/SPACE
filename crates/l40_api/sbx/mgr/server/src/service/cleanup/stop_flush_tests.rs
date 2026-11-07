//! A stop's resume of a paused VM so its guest can flush, on a sealed node.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_node_mgr_client::proto::{
    GetVmStateRequest, GetVmStateResponse, ResumeSandboxRequest, ResumeSandboxResponse,
    VmState as WireVmState,
};

use super::{
    SandboxManagerService, SandboxManagerServiceOptions, StopFlushError, within_stop_flush_budget,
};
use crate::activation_seal::InFlightActivationKind;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::service::tests::service_tests::insert_test_sandbox;
use crate::state::SandboxState;

type Observed = Arc<Mutex<Option<Vec<(InFlightActivationKind, String)>>>>;

fn in_flight(service: &SandboxManagerService) -> Vec<(InFlightActivationKind, String)> {
    service
        .activation_seal
        .status()
        .in_flight
        .into_iter()
        .map(|in_flight| (in_flight.kind, in_flight.sandbox_id))
        .collect()
}

/// While the node is sealed, a stop still resumes a paused VM so its guest
/// can flush, and the resume and the flush after it are in flight as a stop
/// flush.
#[tokio::test]
async fn a_sealed_stop_resumes_a_paused_vm_for_its_flush_under_its_token() {
    let scratch = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Stopping);
    let at_resume: Observed = Arc::default();
    let service_slot: Arc<Mutex<Option<SandboxManagerService>>> = Arc::default();
    let node = FakeNodeManager::default()
        .unary("GetVmState", |_: GetVmStateRequest| {
            Ok(GetVmStateResponse {
                state: WireVmState::Paused as i32,
            })
        })
        .unary("ResumeSandbox", {
            let at_resume = Arc::clone(&at_resume);
            let service_slot = Arc::clone(&service_slot);
            move |_: ResumeSandboxRequest| {
                let service = service_slot.lock().unwrap().clone().unwrap();
                *at_resume.lock().unwrap() = Some(in_flight(&service));
                Ok(ResumeSandboxResponse {})
            }
        })
        .serve();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            ..SandboxManagerServiceOptions::for_test_in(scratch.path())
        },
    );
    *service_slot.lock().unwrap() = Some(service.clone());
    service.activation_seal.seal(&service.db, 6).unwrap();

    let at_flush: Observed = Arc::default();
    let mut stop_flush = None;
    within_stop_flush_budget(
        Duration::from_secs(30),
        service.resume_paused_vm_before_flush(&id, &mut stop_flush),
        || async {
            *at_flush.lock().unwrap() = Some(in_flight(&service));
            Ok::<(), StopFlushError>(())
        },
    )
    .await
    .expect("a sealed node still resumes the VM a stop flushes");
    drop(stop_flush);

    let expected = Some(vec![(InFlightActivationKind::StopFlush, id.clone())]);
    assert_eq!(*at_resume.lock().unwrap(), expected, "{:?}", node.calls());
    assert_eq!(*at_flush.lock().unwrap(), expected);
    service.activation_seal.assert_quiesced();
    service_slot.lock().unwrap().take();
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(30), service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

/// A resume that never answers is cut off by the flush budget, and the VM it
/// may still be running stays counted as a stop flush through the stop's node
/// cleanup; once the stop returns, nothing is in flight.
#[tokio::test]
async fn a_stop_flush_cut_off_by_its_budget_stays_in_flight_until_node_cleanup_returns() {
    use pplx_space_node_mgr_client::proto::{CleanupSandboxRequest, CleanupSandboxResponse};

    use super::SynchronousCleanupRequest;
    use crate::db::CleanupCompletion;
    use crate::db::cleanup_jobs::GuestFlush;
    use crate::service::cleanup_executor::CleanupWait;

    let scratch = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Stopping);
    let (resumed, mut resume_sent) = tokio::sync::mpsc::unbounded_channel();
    let at_cleanup: Observed = Arc::default();
    let service_slot: Arc<Mutex<Option<SandboxManagerService>>> = Arc::default();
    let node = FakeNodeManager::default()
        .unary("GetVmState", |_: GetVmStateRequest| {
            Ok(GetVmStateResponse {
                state: WireVmState::Paused as i32,
            })
        })
        .unary_async("ResumeSandbox", move |_: ResumeSandboxRequest| {
            resumed.send(()).unwrap();
            std::future::pending::<Result<ResumeSandboxResponse, tonic::Status>>()
        })
        .unary("CleanupSandbox", {
            let at_cleanup = Arc::clone(&at_cleanup);
            let service_slot = Arc::clone(&service_slot);
            move |_: CleanupSandboxRequest| {
                let service = service_slot.lock().unwrap().clone().unwrap();
                *at_cleanup.lock().unwrap() = Some(in_flight(&service));
                Ok(CleanupSandboxResponse::default())
            }
        })
        .serve();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            ..SandboxManagerServiceOptions::for_test_in(scratch.path())
        },
    );
    *service_slot.lock().unwrap() = Some(service.clone());
    service.activation_seal.seal(&service.db, 6).unwrap();

    let label = pplx_space_common_core::types::SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let stop = tokio::spawn({
        let service = service.clone();
        let id = id.clone();
        async move {
            service
                .run_cleanup_to_completion_using_admission(
                    SynchronousCleanupRequest {
                        sandbox_id: &id,
                        uuid8: label.as_str(),
                        keep_filesystem: true,
                        context: "stop flush budget",
                        completion: CleanupCompletion::MarkStopped,
                        immediate_kill: false,
                        guest_flush: GuestFlush::BeforeTermination,
                        published_artifact: None,
                    },
                    None,
                    None,
                    // A stop's own wait has a deadline as long as the flush
                    // budget, which the paused clock below would also reach.
                    CleanupWait::Attempt,
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), resume_sent.recv())
        .await
        .expect("the stop resumes the paused VM")
        .unwrap();
    assert_eq!(
        in_flight(&service),
        [(InFlightActivationKind::StopFlush, id.clone())]
    );
    // The resume never answers; let the flush budget run out.
    tokio::time::pause();
    stop.await
        .unwrap()
        .expect("the stop goes on to clean the VM up after its flush budget");
    tokio::time::resume();

    assert_eq!(
        *at_cleanup.lock().unwrap(),
        Some(vec![(InFlightActivationKind::StopFlush, id.clone())]),
        "{:?}",
        node.calls()
    );
    assert_eq!(
        node.calls(),
        ["GetVmState", "ResumeSandbox", "CleanupSandbox"]
    );
    service.activation_seal.assert_quiesced();
    service_slot.lock().unwrap().take();
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(30), service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

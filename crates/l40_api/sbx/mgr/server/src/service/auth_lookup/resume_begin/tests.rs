//! A Resume without an activation policy whose node-local SQLite
//! `sandboxes.state` row leaves `paused` while the resume is in its pre-wake
//! policy gate, or is deleted then.
//!
//! The row is a warm-pool member, so the gate has no net-gateway to confirm
//! and returns at once; a test barrier then holds the resume before its
//! `resuming` write while the test moves the row. Node-manager is a fake that
//! answers only `ResumeSandbox`, with the error a VM that is gone gets.
//!
//! The same row and fake show that a resume is admitted, without waiting for
//! activation limiter capacity, before node-manager is asked to resume the VM.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::{
    AutoLifecyclePolicy, ExecutionGeneration, RouteGeneration, SandboxId, SandboxOwner,
    SandboxRuntimeLabel, SpaceApiRouteIdentity,
};
use pplx_space_node_mgr_client::proto::{ResumeSandboxRequest, ResumeSandboxResponse};
use tonic::{Code, Status};

use super::BEGIN_BARRIER;
use crate::db::test_fixtures::state_write_fixtures::snapshot;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::proto::ResumeRequest;
use crate::service::activation_support::internal_request;
use crate::service::cleanup_executor::TestAdmissionBarrier;
use crate::service::start_support::{
    ActivationAdmissionConfig, ActivationAdmissionKind, ActivationAdmissionLimiter,
    LimitedActivationKind, REQUESTED_VM_ACTIVATIONS,
};
use crate::service::{SandboxManagerService, SandboxManagerServiceOptions};
use crate::state::SandboxState;

const WAIT: Duration = Duration::from_secs(30);

/// What the resume's write finds.
#[derive(Clone, Copy, Debug)]
enum Found {
    State(SandboxState),
    /// The row was deleted.
    Missing,
}

/// What the resume answers.
#[derive(Debug)]
enum Expected {
    /// The write applies, and the resume asks node-manager to thaw the VM.
    Resumes,
    /// `Aborted` with this message, every row unchanged and no node-manager
    /// call.
    Aborted(String),
}

/// Wildcard-free, so a new lifecycle state does not compile until it is
/// placed.
fn expected(found: Found) -> Expected {
    let not_applied = |detail: &str| {
        Expected::Aborted(format!(
            "begin resident resume: sandbox state write was not applied: {detail}"
        ))
    };
    let state = match found {
        // The message a resume reported before it had a from-set.
        Found::Missing => return not_applied("missing"),
        Found::State(state) => state,
    };
    match state {
        SandboxState::Paused => Expected::Resumes,
        // The generic guards refuse these before the from-set is consulted,
        // with the messages a resume reported before it had a from-set.
        SandboxState::Stopping
        | SandboxState::Deleting
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed => not_applied("superseded_by_cleanup_episode"),
        SandboxState::Quarantined => not_applied("blocked_by_quarantine"),
        // The generic guards admit these; only the from-set refuses them.
        SandboxState::Creating
        | SandboxState::Created
        | SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopped
        | SandboxState::Pausing
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Suspended
        | SandboxState::Restoring
        | SandboxState::Failed => Expected::Aborted(format!(
            "begin resident resume: sandbox state write was superseded: sandbox is {state}"
        )),
    }
}

/// A paused warm-pool member whose runtime columns are populated: the test
/// process's pid, route generation 3 and execution generation 7. Nothing in
/// the resume path probes them before its write.
fn insert_paused_pool_member(conn: &crate::db::Connection, sandbox_id: SandboxId, scratch: &str) {
    let id = sandbox_id.to_string();
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: &id,
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
            state: SandboxState::Paused,
            template: "seed",
            template_hash: Some("hash"),
            user: &SandboxOwner::parse("user").unwrap(),
            runtime: "fc",
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: Some("seed-pool"),
            volume_encryption: false,
            process_env: &BTreeMap::new(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
    crate::db::update_runtime_fields(
        conn,
        sandbox_id,
        crate::db::RuntimeFields {
            uid: 10_000,
            slot: 3,
            guest_cid: 33,
            pid: std::process::id(),
            spaced_host_pid: Some(10_001),
            netgtw_pid: 10_002,
            vsock_uds_path: &format!("{scratch}/v.sock"),
            api_socket_path: &format!("{scratch}/api.sock"),
            spaced_endpoint_proof: None,
            space_api_route_identity: SpaceApiRouteIdentity {
                route_generation: Some(RouteGeneration::new(3).unwrap()),
                execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
                ..SpaceApiRouteIdentity::default()
            },
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        },
    )
    .unwrap();
}

/// Move the paused row to what the write should find. `stopped` is reached by
/// the writers Stop runs, which take neither the wake nor the artifact lock: a
/// non-blocking Stop's admission of the paused VM, then its completion. Every
/// other state is written directly, and `Missing` deletes the row.
fn move_row(conn: &crate::db::Connection, id: &str, found: Found) {
    let state = match found {
        Found::Missing => {
            crate::db::delete_sandbox(conn, id).unwrap();
            assert!(crate::db::get_sandbox(conn, id).unwrap().is_none());
            return;
        }
        Found::State(state) => state,
    };
    match state {
        SandboxState::Paused => {}
        SandboxState::Stopped => crate::db::test_fixtures::state_write_fixtures::stop(conn, id),
        SandboxState::CleanupRequired | SandboxState::CleanupFailed => {
            crate::db::test_fixtures::state_write_fixtures::write_status(conn, id, state);
        }
        state => {
            // The schema ties the restore columns to `restoring`.
            conn.execute(
                "UPDATE sandboxes
                    SET state = ?1,
                        restore_origin = CASE WHEN ?1 = 'restoring' THEN 'local' END,
                        restore_phase = CASE WHEN ?1 = 'restoring' THEN 'activation' END
                  WHERE id = ?2",
                crate::db::fixture::params![state.as_str(), id],
            )
            .unwrap();
        }
    }
    let row = crate::db::get_sandbox(conn, id).unwrap().unwrap();
    assert_eq!(row.state, state, "{state}: the row moved");
}

/// Every case runs before the assertion, so a failure lists each case that
/// answered differently.
#[tokio::test]
async fn policy_free_resume_writes_resuming_only_over_paused() {
    let cases = SandboxState::all()
        .into_iter()
        .map(Found::State)
        .chain([Found::Missing]);
    let mut mismatches = Vec::new();
    for found in cases {
        let scratch = tempfile::tempdir().unwrap();
        let node_manager = FakeNodeManager::default()
            .unary(
                "ResumeSandbox",
                |_: ResumeSandboxRequest| -> Result<ResumeSandboxResponse, Status> {
                    Err(Status::not_found("no Firecracker process for the sandbox"))
                },
            )
            .serve();
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_paused_pool_member(&conn, sandbox_id, &scratch.path().to_string_lossy());
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_manager: node_manager.connector(),
                ..SandboxManagerServiceOptions::for_test_in(scratch.path())
            },
        );
        let barrier = Arc::new(TestAdmissionBarrier {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let resume = tokio::spawn(BEGIN_BARRIER.scope(Arc::clone(&barrier), {
            let service = service.clone();
            let id = id.clone();
            async move {
                service
                    .resume(internal_request(ResumeRequest {
                        sandbox_id: id,
                        ..Default::default()
                    }))
                    .await
                    .map(|_| ())
            }
        }));
        tokio::time::timeout(WAIT, barrier.entered.notified())
            .await
            .expect("the resume reaches its write within 30 seconds");

        let before = {
            let db = service.db_conn().unwrap();
            move_row(&db, &id, found);
            snapshot(&db)
        };
        barrier.release.add_permits(1);
        let result = tokio::time::timeout(WAIT, resume)
            .await
            .expect("the resume finishes within 30 seconds")
            .unwrap()
            .map_err(|status| (status.code(), status.message().to_owned()));
        let calls = node_manager.calls();

        let expected = expected(found);
        let matched = match &expected {
            Expected::Resumes => {
                matches!(result, Err((Code::Internal, _)))
                    && calls.iter().any(|call| call == "ResumeSandbox")
            }
            Expected::Aborted(message) => {
                result == Err((Code::Aborted, message.clone()))
                    && snapshot(&service.db_conn().unwrap()) == before
                    && calls.is_empty()
            }
        };
        if !matched {
            mismatches.push(format!(
                "{found:?}: expected {expected:?}; got {result:?}, node-manager calls \
                 {calls:?}, rows unchanged: {}",
                snapshot(&service.db_conn().unwrap()) == before
            ));
        }
        service.shutdown_token.cancel();
        service.task_tracker.close();
        tokio::time::timeout(WAIT, service.task_tracker.wait())
            .await
            .expect("service tasks stop within 30 seconds of shutdown");
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

/// Which public call resumes the paused VM.
#[derive(Clone, Copy, Debug)]
enum ResumeEntry {
    Resume,
    Wake,
}

/// A resident resume, by Resume or by a blocking Wake, passes the VM
/// launch-time admission before it asks node-manager to thaw the VM. While
/// the node is sealed the admission refuses it: node-manager is never asked
/// and the row stays `paused`.
#[rstest::rstest]
#[tokio::test]
async fn resident_resume_is_admitted_before_the_vm_resumes(
    #[values(ResumeEntry::Resume, ResumeEntry::Wake)] entry: ResumeEntry,
    #[values(false, true)] sealed: bool,
) {
    let scratch = tempfile::tempdir().unwrap();
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let admitted_at_resume = Arc::new(Mutex::new(None));
    let in_flight_at_resume = Arc::new(Mutex::new(None));
    let seal_slot = Arc::new(Mutex::new(
        None::<Arc<crate::activation_seal::ActivationSeal>>,
    ));
    let node_manager = FakeNodeManager::default()
        .unary("ResumeSandbox", {
            let admitted = Arc::clone(&admitted);
            let admitted_at_resume = Arc::clone(&admitted_at_resume);
            let in_flight_at_resume = Arc::clone(&in_flight_at_resume);
            let seal_slot = Arc::clone(&seal_slot);
            move |_: ResumeSandboxRequest| -> Result<ResumeSandboxResponse, Status> {
                *admitted_at_resume.lock().unwrap() = Some(admitted.lock().unwrap().clone());
                let seal = seal_slot.lock().unwrap().clone().unwrap();
                *in_flight_at_resume.lock().unwrap() = Some(
                    seal.status()
                        .in_flight
                        .into_iter()
                        .map(|in_flight| (in_flight.kind, in_flight.sandbox_id))
                        .collect::<Vec<_>>(),
                );
                Err(Status::not_found("no Firecracker process for the sandbox"))
            }
        })
        .serve();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    insert_paused_pool_member(&conn, sandbox_id, &scratch.path().to_string_lossy());
    let mut service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node_manager.connector(),
            ..SandboxManagerServiceOptions::for_test_in(scratch.path())
        },
    );
    // The limiter's only slot is held, so a resume that waited for limiter
    // capacity would not reach node-manager.
    service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
        ActivationAdmissionConfig {
            max_concurrent_activations: 1,
            refill_rps: 100.0,
            burst: 100.0,
        },
    ));
    let _held = service
        .activation_admission_limiter
        .try_acquire(LimitedActivationKind::Cold)
        .expect("the test holds the limiter's only slot");

    *seal_slot.lock().unwrap() = Some(Arc::clone(&service.activation_seal));
    if sealed {
        service.activation_seal.seal(&service.db, 5).unwrap();
    }
    let resume = async {
        match entry {
            ResumeEntry::Resume => service
                .resume(internal_request(ResumeRequest {
                    sandbox_id: sandbox_id.to_string(),
                    ..Default::default()
                }))
                .await
                .map(drop),
            ResumeEntry::Wake => service
                .wake(internal_request(crate::proto::WakeRequest {
                    sandbox_id: sandbox_id.to_string(),
                    blocking: true,
                    ..Default::default()
                }))
                .await
                .map(drop),
        }
    };
    let result = tokio::time::timeout(
        WAIT,
        REQUESTED_VM_ACTIVATIONS.scope(Arc::clone(&admitted), resume),
    )
    .await
    .expect("the resume finishes within 30 seconds");

    if sealed {
        let refused = result.expect_err("a sealed node refuses the resume");
        assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
        assert!(
            refused.message().contains(
                pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER
            ),
            "{refused:?}"
        );
        assert_eq!(*admitted_at_resume.lock().unwrap(), None);
        assert_eq!(
            service
                .lookup_sandbox(&sandbox_id.to_string())
                .unwrap()
                .state,
            SandboxState::Paused
        );
    } else {
        assert_eq!(
            result.map_err(|status| status.code()),
            Err(Code::Internal),
            "the fake answers ResumeSandbox with the error a VM that is gone gets"
        );
        assert_eq!(
            *admitted_at_resume.lock().unwrap(),
            Some(vec![ActivationAdmissionKind::ResidentResume]),
            "node-manager was asked to resume the VM only after it requested admission"
        );
        assert_eq!(
            *in_flight_at_resume.lock().unwrap(),
            Some(vec![(
                crate::activation_seal::InFlightActivationKind::ResidentResume,
                sandbox_id.to_string()
            )]),
            "the resume is in flight while it thaws the VM"
        );
    }
    service.activation_seal.assert_quiesced();
    assert_eq!(
        *admitted.lock().unwrap(),
        vec![ActivationAdmissionKind::ResidentResume]
    );
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(WAIT, service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

/// A Resume that already holds its state-writer lease and is waiting on the
/// sandbox's wake lock when the seal lands is refused where it is admitted,
/// once it gets the lock, not where it took its lease: node-manager is never
/// asked to resume the VM and the row stays `paused`.
#[tokio::test]
async fn a_resume_waiting_on_the_wake_lock_when_the_seal_lands_is_refused_at_admission() {
    let scratch = tempfile::tempdir().unwrap();
    let node_manager = FakeNodeManager::default()
        .unary(
            "ResumeSandbox",
            |_: ResumeSandboxRequest| -> Result<ResumeSandboxResponse, Status> {
                Ok(ResumeSandboxResponse {})
            },
        )
        .serve();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_paused_pool_member(&conn, sandbox_id, &scratch.path().to_string_lossy());
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node_manager.connector(),
            ..SandboxManagerServiceOptions::for_test_in(scratch.path())
        },
    );

    let wake_lock = service.auto_lifecycle.wake_lock(&id);
    let held = wake_lock.lock().await;
    let lease = service
        .state_writer_lease("/space.sandbox.manager.v1.SandboxManager/Resume")
        .expect("the resume is admitted past restart admission");
    let requested = Arc::new(Mutex::new(Vec::new()));
    let resume = tokio::spawn(crate::service::drain::with_state_writer(
        lease,
        REQUESTED_VM_ACTIVATIONS.scope(Arc::clone(&requested), {
            let service = service.clone();
            let id = id.clone();
            async move {
                service
                    .resume(internal_request(ResumeRequest {
                        sandbox_id: id,
                        ..Default::default()
                    }))
                    .await
                    .map(drop)
            }
        }),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!resume.is_finished(), "the resume waits on the wake lock");
    assert!(requested.lock().unwrap().is_empty());

    service.activation_seal.seal(&service.db, 9).unwrap();
    drop(held);
    let refused = tokio::time::timeout(WAIT, resume)
        .await
        .expect("the resume answers once it has the lock")
        .unwrap()
        .expect_err("the seal landed while the resume waited");
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
    assert!(
        refused
            .message()
            .contains(pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
    assert_eq!(
        *requested.lock().unwrap(),
        vec![ActivationAdmissionKind::ResidentResume],
        "the resume reached admission"
    );
    assert!(
        node_manager.calls().is_empty(),
        "{:?}",
        node_manager.calls()
    );
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Paused
    );
    service.activation_seal.assert_quiesced();
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(WAIT, service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

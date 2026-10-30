//! Stop's and Delete's cleanup admission from every node-local SQLite
//! `sandboxes.state` source state, and the Stop RPC's runtime gate in front of
//! it.
//!
//! Stop admission does not consult `SandboxState::transition`, unlike Kill: it
//! supersedes any activation and writes `stopping` over every state the generic
//! guards do not refuse, a `stopping` row included. Delete admission writes
//! `deleting` the same way, over a `deleting` row too, unless the row's suspend
//! state has not reached the cluster. The expected outcomes are written out
//! here, not derived from that writer.
//!
//! The Stop RPC admits only a sandbox whose runtime is live: it resolves its
//! target with `running_only`, which selects nothing unless the sandbox's
//! `fc.pid` resolves. A `creating`, `created`, `stopped` or `suspended` row has
//! no Firecracker process, so those admissions are reachable only by calling
//! admission directly. Only a non-blocking Stop persists its intent.
//!
//! The admission sweep calls admission directly, because an admitted Stop
//! RPC's watchdog removal stats the fixed sidecar pid path. Node cleanup
//! reaches only the executor's test dispatch, which holds until shutdown.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_svc::PeerIdentity;
use tonic::Request;

use super::{
    CleanupAdmission, CleanupAdmissionError, SandboxManagerService, SandboxManagerServiceOptions,
    StopRequest,
};
use crate::db::CleanupCompletion;
use crate::db::test_fixtures::state_write_fixtures::{assert_unchanged, snapshot};
use crate::service::tests::service_tests::insert_test_sandbox;
use crate::state::SandboxState;

const WAIT: Duration = Duration::from_secs(30);

/// A test service whose host dependencies resolve under `scratch`, with node
/// cleanup held by a test dispatch.
fn isolated_service(
    conn: crate::db::Connection,
    scratch: &std::path::Path,
) -> SandboxManagerService {
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions::for_test_in(scratch),
    );
    // A dispatch that outlived shutdown would keep its executor task, and the
    // tracker, from finishing.
    let shutdown = service.shutdown_token.clone();
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |_, _| {
        let shutdown = shutdown.clone();
        Box::pin(async move {
            shutdown.cancelled().await;
            Err(tonic::Status::unavailable("test node cleanup shut down"))
        })
    }));
    service
}

async fn shutdown(service: SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(WAIT, service.task_tracker.wait())
        .await
        .expect("cleanup tasks stop within 30 seconds of shutdown");
}

/// A row in `state`, with a local restore's columns when it is `restoring`.
fn insert_row(conn: &crate::db::Connection, state: SandboxState) -> SandboxId {
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_test_sandbox(conn, &id, state);
    if state == SandboxState::Restoring {
        conn.execute(
            "UPDATE sandboxes SET restore_origin = 'local', restore_phase = 'activation'
              WHERE id = ?1",
            [&id],
        )
        .unwrap();
    }
    sandbox_id
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Admission {
    /// Admitted, and the RPC can reach it: Stop only for a live runtime.
    Admitted,
    /// Admitted, but only by calling admission directly: the state has no
    /// Firecracker process, so the Stop RPC's runtime gate selects nothing.
    AdmittedOnlyDirectly,
    /// Refused: Stop with `Unavailable` and this message, Delete (`None`)
    /// because the row's suspend state has not reached the cluster.
    Refused(Option<&'static str>),
}

/// What Stop admission answers for a plain row in `state`. Wildcard-free, so
/// a new lifecycle state does not compile until it is placed.
fn admission(state: SandboxState) -> Admission {
    match state {
        SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopping
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspending
        | SandboxState::Restoring
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed
        | SandboxState::Failed => Admission::Admitted,
        SandboxState::Creating
        | SandboxState::Created
        | SandboxState::Stopped
        | SandboxState::Suspended => Admission::AdmittedOnlyDirectly,
        // The generic guards leave a `deleting` row to its delete and a
        // `quarantined` row to quarantine.
        SandboxState::Deleting => Admission::Refused(Some(
            "cleanup lifecycle claim was superseded: sandbox state write was not applied: \
             superseded_by_cleanup_episode",
        )),
        SandboxState::Quarantined => Admission::Refused(Some(
            "cleanup lifecycle claim was superseded: sandbox state write was not applied: \
             blocked_by_quarantine",
        )),
    }
}

/// What Delete admission answers for a plain row in `state`. Wildcard-free,
/// so a new lifecycle state does not compile until it is placed.
fn delete_admission(state: SandboxState) -> Admission {
    match state {
        SandboxState::Creating
        | SandboxState::Created
        | SandboxState::Starting
        | SandboxState::Started
        | SandboxState::Stopping
        | SandboxState::Stopped
        | SandboxState::Deleting
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing
        | SandboxState::Suspended
        | SandboxState::Restoring
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed
        | SandboxState::Quarantined
        | SandboxState::Failed => Admission::Admitted,
        // Its suspend state has not reached the cluster yet.
        SandboxState::Suspending => Admission::Refused(None),
    }
}

/// The lifecycle cleanup an admission sweep runs.
#[derive(Clone, Copy, Debug)]
enum Cleanup {
    Stop,
    Delete,
}

impl Cleanup {
    fn completion(self) -> CleanupCompletion {
        match self {
            Self::Stop => CleanupCompletion::MarkStopped,
            Self::Delete => CleanupCompletion::DeleteRow,
        }
    }

    fn target(self) -> SandboxState {
        match self {
            Self::Stop => SandboxState::Stopping,
            Self::Delete => SandboxState::Deleting,
        }
    }

    fn keep_filesystem(self) -> bool {
        match self {
            Self::Stop => true,
            Self::Delete => false,
        }
    }

    fn admission(self, state: SandboxState) -> Admission {
        match self {
            Self::Stop => admission(state),
            Self::Delete => delete_admission(state),
        }
    }

    /// Whether `result` is this cleanup's refusal of a row in `state`; for
    /// Stop, with exactly `message`.
    fn refused(
        self,
        state: SandboxState,
        message: Option<&str>,
        result: Result<CleanupAdmission, CleanupAdmissionError>,
    ) -> bool {
        match (self, result) {
            (Self::Stop, Err(error)) => {
                let status = error.into_status();
                (status.code(), Some(status.message())) == (tonic::Code::Unavailable, message)
            }
            (
                Self::Delete,
                Err(CleanupAdmissionError::SuspendPublicationPending { state: found, .. }),
            ) => found == state,
            (Self::Delete, Err(_)) | (Self::Stop | Self::Delete, Ok(_)) => false,
        }
    }
}

/// Admit `cleanup` over a plain row in every source state, with and without
/// a persisted intent. An admitted cleanup leaves the row in its target state
/// with its intent and cleanup job; a refused one changes nothing.
///
/// Admission never consults the shutdown token, but the cleanup executor it
/// starts does not start once the service is shut down. Each case cancels the
/// token and waits for the service's own tasks to exit before admitting, so
/// neither the executor nor its recovery scan runs: the refused cases'
/// whole-database comparison sees admission's writes alone, and an admitted
/// row stays as admission left it. The empty task tracker after admission
/// checks that.
async fn sweep_every_source_state(cleanup: Cleanup) {
    for persist_intent in [true, false] {
        for state in SandboxState::all() {
            let case = format!(
                "{cleanup:?} of {state} ({:?}), persist intent {persist_intent}",
                cleanup.admission(state)
            );
            let conn = crate::db::test_conn();
            let sandbox_id = insert_row(&conn, state);
            let id = sandbox_id.to_string();
            let scratch = tempfile::tempdir().unwrap();
            let service = isolated_service(conn, scratch.path());
            // The service's own background tasks exit on cancellation; once
            // they have, any tracked task is one admission started.
            service.shutdown_token.cancel();
            tokio::time::timeout(WAIT, async {
                while !service.task_tracker.is_empty() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{case}: service tasks outlived shutdown"));
            let before = snapshot(&service.db_conn().unwrap());

            let result = tokio::time::timeout(
                WAIT,
                service.admit_selected_cleanup(
                    sandbox_id,
                    cleanup.completion(),
                    false,
                    persist_intent,
                    None,
                    false,
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("{case}: admission waited longer than {WAIT:?}"));
            assert!(
                service.task_tracker.is_empty(),
                "{case}: no cleanup executor started"
            );

            {
                let db = service.db_conn().unwrap();
                if let Admission::Refused(message) = cleanup.admission(state) {
                    assert!(
                        cleanup.refused(state, message, result),
                        "{case}: expected a refusal"
                    );
                    assert_unchanged(&case, &before, &db);
                } else {
                    assert!(
                        matches!(result, Ok(CleanupAdmission::Persisted { .. })),
                        "{case}: expected a persisted admission"
                    );
                    let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
                    assert_eq!(row.state, cleanup.target(), "{case}");
                    assert_eq!(
                        (row.cleanup_completion, row.cleanup_keep_filesystem),
                        if persist_intent {
                            (Some(cleanup.completion()), Some(cleanup.keep_filesystem()))
                        } else {
                            (None, None)
                        },
                        "{case}"
                    );
                    assert!(
                        pplx_space_common_jobs::db::find_active_job(
                            db.raw(),
                            crate::db::cleanup_jobs::KIND,
                            &id
                        )
                        .unwrap()
                        .is_some(),
                        "{case}: admission persists its cleanup job"
                    );
                }
            }
            shutdown(service).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stop_admission_answers_every_source_state() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    sweep_every_source_state(Cleanup::Stop).await;
}

#[tokio::test(flavor = "current_thread")]
async fn delete_admission_answers_every_source_state() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    sweep_every_source_state(Cleanup::Delete).await;
}

/// The Stop RPC selects nothing without a live runtime, whatever the row's
/// state, and admits nothing. A live runtime is admitted, as
/// `stop_selects_only_a_live_runtime_and_both_treat_an_empty_template_filter_as_none`
/// pins.
#[tokio::test(flavor = "current_thread")]
async fn stop_rpc_admits_nothing_without_a_live_runtime() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    for blocking in [true, false] {
        for state in SandboxState::all() {
            let case = format!("{state}, blocking {blocking}");
            let conn = crate::db::test_conn();
            let sandbox_id = insert_row(&conn, state);
            let scratch = tempfile::tempdir().unwrap();
            let service = isolated_service(conn, scratch.path());
            let before = snapshot(&service.db_conn().unwrap());
            let mut request = Request::new(StopRequest {
                sandbox_id: sandbox_id.to_string(),
                template_filter: None,
                blocking,
                immediate_kill: false,
            });
            request
                .extensions_mut()
                .insert(PeerIdentity::new(0, 0, None));

            let response = tokio::time::timeout(WAIT, service.stop(request))
                .await
                .unwrap_or_else(|_| panic!("{case}: Stop waited longer than {WAIT:?}"))
                .unwrap_or_else(|status| panic!("{case}: {status:?}"));

            assert!(response.into_inner().stopped_ids.is_empty(), "{case}");
            assert_unchanged(&case, &before, &service.db_conn().unwrap());
            shutdown(service).await;
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn exited_runtime_recovery_requires_death_and_the_current_execution() {
    use pplx_space_node_runtime_core::launch::ProcessHandle;
    for (exited, stale, busy, saturated) in [
        (false, false, false, false),
        (false, false, false, true),
        (true, true, false, false),
        (true, false, true, false),
        (true, false, false, false),
        (true, false, false, true),
    ] {
        let scratch = tempfile::tempdir().unwrap();
        let conn = crate::db::test_conn();
        let id = insert_row(&conn, SandboxState::Started);
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let pinned = ProcessHandle::open(child.id()).unwrap();
        crate::db::update_runtime_fields(
            &conn,
            id,
            crate::db::RuntimeFields {
                uid: 10000,
                slot: 1,
                guest_cid: 3,
                pid: child.id(),
                spaced_host_pid: None,
                netgtw_pid: 0,
                vsock_uds_path: "/synthetic/v.sock",
                api_socket_path: "/synthetic/control.sock",
                spaced_endpoint_proof: None,
                space_api_route_identity: Default::default(),
                route_identity_provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            },
        )
        .unwrap();
        let service = isolated_service(conn, scratch.path());
        let mut observed = service.lookup_sandbox(&id.to_string()).unwrap();
        if stale {
            observed.node_execution_id =
                Some(pplx_space_common_core::types::SandboxActivationAttemptId::new());
        }
        if exited {
            child.kill().unwrap();
            tokio::time::timeout(WAIT, async {
                while !pinned.has_exited().unwrap() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        let fence = if busy {
            Some(service.cleanup_lock(&id.to_string()).lock_owned().await)
        } else {
            None
        };
        let permits = if saturated {
            Some(
                Arc::clone(&service.cleanup_io)
                    .acquire_many_owned(service.cleanup_io.available_permits().try_into().unwrap())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        // Running-process probes and durable exit admission must complete even
        // while unrelated filesystem cleanups consume every I/O permit.
        let admission = tokio::time::timeout(WAIT, service.admit_exited_runtime(observed))
            .await
            .unwrap()
            .unwrap();
        drop(permits);
        drop(fence);
        assert_eq!(admission.is_some(), exited && !stale && !busy);
        assert_eq!(
            service.lookup_sandbox(&id.to_string()).unwrap().state,
            if exited && !stale && !busy {
                SandboxState::Stopping
            } else {
                SandboxState::Started
            }
        );
        drop(admission);
        if !exited {
            child.kill().unwrap();
        }
        child.wait().unwrap();
        shutdown(service).await;
    }
}

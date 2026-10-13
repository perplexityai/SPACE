//! Stop's cleanup admission from every node-local SQLite `sandboxes.state`
//! source state, and the Stop RPC's runtime gate in front of it.
//!
//! Stop admission does not consult `SandboxState::transition`, unlike Kill: it
//! supersedes any activation and writes `stopping` through the generic writer,
//! so every state the generic writer does not refuse is admitted. The
//! expected outcomes are written out here, not derived from that writer.
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

use super::{CleanupAdmission, SandboxManagerService, SandboxManagerServiceOptions, StopRequest};
use crate::db::CleanupCompletion;
use crate::db::tests::state_write_fixtures::{assert_unchanged, snapshot};
use crate::service::tests::service_tests::insert_test_sandbox;
use crate::state::SandboxState;

const WAIT: Duration = Duration::from_secs(30);

/// A test service whose host dependencies resolve under `scratch`, with node
/// cleanup held by a test dispatch.
fn isolated_service(
    conn: rusqlite::Connection,
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
fn insert_row(conn: &rusqlite::Connection, state: SandboxState) -> SandboxId {
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
    /// Admitted, and the Stop RPC can reach it for a live runtime.
    Admitted,
    /// Admitted, but only by calling admission directly: the state has no
    /// Firecracker process, so the Stop RPC's runtime gate selects nothing.
    AdmittedOnlyDirectly,
    /// Refused with `Unavailable`.
    Refused,
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
        // The generic writer leaves a `deleting` row to its delete and a
        // `quarantined` row to quarantine.
        SandboxState::Deleting | SandboxState::Quarantined => Admission::Refused,
    }
}

/// Assert one admission's outcome: an admitted Stop leaves the row `stopping`
/// with its intent and cleanup job; a refused one changes nothing.
fn check_admission(
    case: &str,
    service: &SandboxManagerService,
    id: &str,
    state: SandboxState,
    persist_intent: bool,
    result: Result<CleanupAdmission, super::CleanupAdmissionError>,
    before: &crate::db::tests::state_write_fixtures::Snapshot,
) {
    let db = service.db_conn().unwrap();
    if admission(state) == Admission::Refused {
        let status = match result {
            Err(error) => error.into_status(),
            Ok(_) => panic!("{case}: expected a refusal"),
        };
        assert_eq!(
            status.code(),
            tonic::Code::Unavailable,
            "{case}: {status:?}"
        );
        assert_unchanged(case, before, &db);
        return;
    }
    assert!(
        matches!(result, Ok(CleanupAdmission::Persisted { .. })),
        "{case}: expected a persisted admission"
    );
    let row = crate::db::get_sandbox(&db, id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Stopping, "{case}");
    assert_eq!(
        (row.cleanup_completion, row.cleanup_keep_filesystem),
        if persist_intent {
            (Some(CleanupCompletion::MarkStopped), Some(true))
        } else {
            (None, None)
        },
        "{case}"
    );
    assert!(
        pplx_space_common_jobs::db::find_active_job(&db, crate::db::cleanup_jobs::KIND, id)
            .unwrap()
            .is_some(),
        "{case}: admission persists its cleanup job"
    );
}

/// Admission never consults the shutdown token, but the cleanup executor it
/// starts does not start once the service is shut down. Each case cancels the
/// token and waits for the service's own tasks to exit before admitting, so
/// neither the executor nor its recovery scan runs: the refused cases'
/// whole-database comparison sees admission's writes alone, and an admitted
/// row stays as admission left it. The empty task tracker after admission
/// checks that.
#[tokio::test(flavor = "current_thread")]
async fn stop_admission_answers_every_source_state() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    // A non-blocking Stop persists its intent; a blocking one does not.
    for persist_intent in [true, false] {
        for state in SandboxState::all() {
            let case = format!(
                "{state} ({:?}), persist intent {persist_intent}",
                admission(state)
            );
            let conn = crate::db::test_conn();
            let sandbox_id = insert_row(&conn, state);
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
                    CleanupCompletion::MarkStopped,
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

            check_admission(
                &case,
                &service,
                &sandbox_id.to_string(),
                state,
                persist_intent,
                result,
                &before,
            );
            shutdown(service).await;
        }
    }
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

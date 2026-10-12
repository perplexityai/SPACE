//! Lifecycle handlers driven against an in-process node-manager.
//!
//! The service is built with every host dependency redirected: the
//! node-manager connector reaches a [`FakeNodeManager`], the host-control and
//! credential-manager sockets are test-owned paths nothing binds, and no
//! runtime is ever live.

use super::service_tests::{insert_test_sandbox, runtime_fields};
use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::service::activation_support::internal_request;
use pplx_space_common_core::types::ExecutionGeneration;
use pplx_space_node_mgr_client::proto::{
    CleanupSandboxRequest, CleanupSandboxResponse, StopProcessRequest, StopProcessResponse,
};

/// A service whose node-manager clients all come from `node_manager` and whose
/// template lookups all go to `template_resolver`.
pub(crate) fn isolated_service(
    conn: rusqlite::Connection,
    node_manager: NodeManagerConnector,
    template_resolver: Arc<dyn crate::template_resolver::TemplateResolver>,
    sockets: &Path,
) -> SandboxManagerService {
    SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            host_control_socket_path: sockets.join("absent-host-control.sock"),
            credential_manager_socket_path: sockets.join("absent-credential-manager.sock"),
            sandbox_runtime_live: Arc::new(|_| false),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            local_node_id: Some(NodeId::from_uuid(
                uuid::Uuid::parse_str("019f8000-0000-7000-8000-0000000000fe").unwrap(),
            )),
            node_role: None,
            node_managed_by: None,
            route_ed25519_verification_bundle: None,
            node_manager,
            template_resolver,
        },
    )
}

/// [`isolated_service`] whose template lookups are all refused.
pub(crate) fn service_with_node_manager(
    conn: rusqlite::Connection,
    node_manager: NodeManagerConnector,
    sockets: &Path,
) -> SandboxManagerService {
    isolated_service(
        conn,
        node_manager,
        Arc::new(
            crate::template_resolver::fake::FakeTemplateResolver::refusing(Status::unavailable(
                "template-manager is outside this test",
            )),
        ),
        sockets,
    )
}

#[tokio::test]
async fn fatal_kill_signal_failure_still_schedules_admitted_cleanup() {
    let stops = Arc::new(Mutex::new(Vec::new()));
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let node_manager = FakeNodeManager::default()
        .unary("StopProcess", {
            let stops = Arc::clone(&stops);
            move |request: StopProcessRequest| {
                stops.lock().unwrap().push(request);
                Err::<StopProcessResponse, _>(Status::unavailable("injected StopProcess refusal"))
            }
        })
        .unary("CleanupSandbox", {
            let cleanups = Arc::clone(&cleanups);
            move |request: CleanupSandboxRequest| {
                cleanups.lock().unwrap().push(request);
                Ok(CleanupSandboxResponse::default())
            }
        })
        .serve();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let sockets = tempfile::tempdir().unwrap();
    let service = service_with_node_manager(conn, node_manager.connector(), sockets.path());
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
    let execution_fence = service
        .lookup_sandbox(&sandbox_id_string)
        .unwrap()
        .node_execution_fence();
    assert!(execution_fence.is_some());

    let error = service
        .kill(internal_request(KillRequest {
            sandbox_id: sandbox_id_string.clone(),
            signal: 9,
            blocking: false,
        }))
        .await
        .expect_err("node-manager's refusal must fail fatal signal dispatch");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error.message().contains("send fatal signal failed")
            && error.message().contains("injected StopProcess refusal"),
        "the caller must see node-manager's refusal: {}",
        error.message()
    );
    // The signal reached node-manager once, fenced to the observed execution.
    assert_eq!(
        *stops.lock().unwrap(),
        [StopProcessRequest {
            execution_fence: execution_fence.clone(),
            sandbox_id: sandbox_id_string.clone(),
            signal: 9,
            pid: None,
        }]
    );

    // The cleanup admitted before the signal still runs: node-manager stops
    // the same execution, keeping its filesystem, and the row ends stopped.
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.lookup_sandbox(&sandbox_id_string).unwrap().state != SandboxState::Stopped {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("fatal signal failure must still run the admitted cleanup to completion");
    assert_eq!(
        *cleanups.lock().unwrap(),
        [CleanupSandboxRequest {
            execution_fence,
            sandbox_id: sandbox_id_string,
            force: true,
            keep_filesystem: true,
            return_after_slot_release: true,
            immediate_kill: true,
        }]
    );
    assert_eq!(node_manager.calls(), ["StopProcess", "CleanupSandbox"]);
}

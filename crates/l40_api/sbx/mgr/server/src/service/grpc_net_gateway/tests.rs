//! The cycle's socket handover and its fallback, against an in-process
//! node-manager.

use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::service::tests::service_tests::insert_test_sandbox_with_runtime;
use pplx_space_node_mgr_client::proto::{LaunchRequest, LaunchResponse, StopProcessResponse};

/// The pid `insert_test_sandbox_with_runtime` records for the gateway.
const OLD_PID: u32 = 10_002;
const NEW_PID: u32 = 40_004;

/// How the node answers the cycle's handover launch.
type ReplaceAnswer = fn() -> Result<LaunchResponse, Status>;

#[tokio::test]
async fn a_handover_that_is_not_taken_stops_and_relaunches_the_gateway() {
    // An older supervisor refuses a gateway without launch evidence, a
    // handover can fail after the old gateway stopped, and an older node
    // returns the live gateway. Each still cycles it. `stopped` is
    // StopProcess's answer: false when the old gateway is already gone.
    let cases: [(&str, ReplaceAnswer, bool, _); 3] = [
        (
            "unrecorded",
            || {
                Err(Status::failed_precondition(
                    "gateway has no valid launch evidence; cycle the gateway",
                ))
            },
            true,
            NetGatewayKillOutcome::Sigterm,
        ),
        (
            "failed after stopping",
            || Err(Status::internal("launch net-gateway replacement")),
            false,
            NetGatewayKillOutcome::OldProcessAlreadyGone,
        ),
        (
            "old node",
            || {
                Ok(LaunchResponse {
                    pid: OLD_PID,
                    inherited_gateway_sockets: 0,
                })
            },
            true,
            NetGatewayKillOutcome::Sigterm,
        ),
    ];
    for (case, replace, stopped, outcome) in cases {
        let launches = Arc::new(Mutex::new(Vec::new()));
        let stops = Arc::new(Mutex::new(Vec::new()));
        let node_manager = FakeNodeManager::default()
            .unary("Launch", {
                let launches = Arc::clone(&launches);
                move |request: LaunchRequest| {
                    launches.lock().unwrap().push(request.replace_gateway_pid);
                    match request.replace_gateway_pid {
                        Some(_) => replace(),
                        None => Ok(LaunchResponse {
                            pid: NEW_PID,
                            inherited_gateway_sockets: 0,
                        }),
                    }
                }
            })
            .unary("StopProcess", {
                let stops = Arc::clone(&stops);
                move |request: StopProcessRequest| {
                    stops.lock().unwrap().push((request.pid, request.signal));
                    Ok(StopProcessResponse { success: stopped })
                }
            })
            .serve();
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_manager: node_manager.connector(),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let sandbox = service.lookup_sandbox(&id).unwrap();
        let uid = sandbox.uid.unwrap();
        service
            .watchdog
            .add_sandbox(crate::watchdog::SidecarInfo {
                template: sandbox.template.clone(),
                guest_image: Default::default(),
                node_execution_id: sandbox.node_execution_id,
                sandbox_id: id.clone(),
                created_at: None,
                uid: Some(uid),
                runtime_pid: None,
                spaced_host_pid: sandbox.spaced_host_pid,
                netgtw_pid: Some(OLD_PID),
                vsock_uds_path: None,
                process_env: BTreeMap::new(),
                network_tx_bytes: 0,
                network_rx_bytes: 0,
            })
            .await;

        let cycled = service
            .replace_or_relaunch_netgtw(&sandbox, uid, OLD_PID, true)
            .await;

        assert_eq!(cycled, Ok((NEW_PID, 0, outcome)), "{case}");
        assert_eq!(
            *launches.lock().unwrap(),
            [Some(OLD_PID), None],
            "{case}: the handover names the old gateway, the relaunch does not"
        );
        assert_eq!(
            *stops.lock().unwrap(),
            [(Some(OLD_PID), libc::SIGTERM as u32)],
            "{case}"
        );
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().netgtw_pid,
            Some(NEW_PID),
            "{case}"
        );
    }
}

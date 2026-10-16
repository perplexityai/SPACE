use super::*;

#[tokio::test]
async fn gateway_identity_mapping_preserves_exact_pid_and_lifecycle_authority() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    conn.execute(
        pplx_space_common_core::sql!(
            "INSERT INTO sandboxes (id,uuid8,state,template,user,runtime,created_at,updated_at)
             VALUES (?1,'gateway1','started','seed','deploy','fc','1','1')"
        ),
        crate::db::fixture::params![id],
    )
    .unwrap();
    let owner = 10_042;
    let isolated = pplx_space_common_core::host::linux::gateway::gateway_uid(
        pplx_space_common_core::types::LinuxUid::new(owner),
    )
    .unwrap()
    .get();
    crate::db::update_netgtw_pid(&conn, &id, owner, 4242).unwrap();
    let service = SandboxManagerService::new_with_options(
        conn,
        super::super::SandboxManagerServiceOptions::for_test(),
    );
    for (pid, uid, allowed) in [
        (4242, owner, true),
        (4242, isolated, true),
        (4243, isolated, false),
        (4242, isolated + 1, false),
        (4242, 0, false),
    ] {
        assert_eq!(
            service
                .lookup_sandbox_by_net_gateway_process(pid, uid)
                .is_ok(),
            allowed
        );
    }
    {
        let conn = service.db.lock().unwrap();
        assert_eq!(
            crate::db::get_sandbox(&conn, &id).unwrap().unwrap().uid,
            Some(owner)
        );
        crate::db::test_fixtures::state_write_fixtures::force_state(
            &conn,
            &id,
            SandboxState::Stopped,
        );
    }
    service
        .lookup_sandbox_by_net_gateway_process(4242, isolated)
        .err()
        .expect("stopped gateway denied");
}

#[test]
fn connection_failure_rolls_back_only_the_admitted_resume() {
    for state in [
        SandboxState::Resuming,
        SandboxState::Deleting,
        SandboxState::Stopping,
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        conn.execute(
            "INSERT INTO sandboxes (id,uuid8,state,template,user,runtime,created_at,updated_at)
             VALUES (?1,'sandbox1',?2,'seed','deploy','fc','1','1')",
            crate::db::fixture::params![id, state.as_str()],
        )
        .unwrap();
        let db = crate::db::SharedConnection::new(conn);
        let error = resident_resume_connect_error(
            &db,
            &id,
            super::super::tail_support::NodeManagerConnectError::Client(
                "injected disconnect".into(),
            ),
        );
        let conn = db.lock().unwrap();
        let expected = if state == SandboxState::Resuming {
            SandboxState::Paused
        } else {
            state
        };
        assert_eq!(
            crate::db::get_sandbox(&conn, &id).unwrap().unwrap().state,
            expected
        );
        assert_eq!(
            error.code(),
            if state == SandboxState::Resuming {
                tonic::Code::Unavailable
            } else {
                tonic::Code::Aborted
            }
        );
    }
}

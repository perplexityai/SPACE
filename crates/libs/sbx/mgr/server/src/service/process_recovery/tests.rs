use super::*;
use crate::service::cleanup_support::require_state_write_from;
use crate::service::tests::service_tests::insert_test_sandbox_with_runtime;
use pplx_space_common_core::types::SandboxId;

#[tokio::test]
async fn recovery_activation_rechecks_state_after_snapshot_exclusion() {
    let id = SandboxId::new().to_string();
    let conn = crate::db::test_conn();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let service = SandboxManagerService::for_test(conn);
    let lock = service.auto_lifecycle.artifact_lock(&id);
    let capture = lock.clone().lock_owned().await;
    let mut activation = Box::pin(service.recovery_activation_scope(&id, None));
    assert!(futures_util::poll!(&mut activation).is_pending());
    {
        let conn = service.db_conn().unwrap();
        require_state_write_from(
            crate::db::update_state_from(&conn, &id, &crate::db::edges::PAUSE_BEGIN),
            "prepare paused test sandbox",
        )
        .unwrap();
        require_state_write_from(
            crate::db::update_state_from(&conn, &id, &crate::db::edges::PAUSE_COMPLETE),
            "prepare paused test sandbox",
        )
        .unwrap();
    }
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Paused
    );
    drop(capture);
    assert_eq!(
        activation.await.err().unwrap().code(),
        tonic::Code::FailedPrecondition
    );
    drop(
        lock.try_lock()
            .expect("refusal releases snapshot exclusion"),
    );
    service.shutdown_token.cancel();
}

#[tokio::test]
async fn recovery_activation_holds_snapshot_exclusion_through_confirmation() {
    let id = SandboxId::new().to_string();
    let conn = crate::db::test_conn();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let service = SandboxManagerService::for_test(conn);
    let scope = service.recovery_activation_scope(&id, None).await.unwrap();
    let lock = service.auto_lifecycle.artifact_lock(&id);
    lock.try_lock().unwrap_err();
    assert_eq!(
        scope.sandbox.node_execution_id,
        service.lookup_sandbox(&id).unwrap().node_execution_id
    );
    drop(scope);
    drop(lock.try_lock().unwrap());
    service.shutdown_token.cancel();
}

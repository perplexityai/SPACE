use super::*;

#[tokio::test(start_paused = true)]
async fn activation_cooldown_waits_only_before_new_work_within_budget() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    super::super::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Resuming);
    crate::db::restore_resident_resume_state(&conn, &id, SandboxState::Paused).unwrap();
    let service = SandboxManagerService::new(conn);
    let error = service
        .wait_activation_cooldown(&id, Duration::from_millis(30))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    for state in [
        SandboxState::Started,
        SandboxState::Starting,
        SandboxState::Resuming,
        SandboxState::Restoring,
    ] {
        service
            .db_conn()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = ?2 WHERE id = ?1",
                rusqlite::params![id, state.as_str()],
            )
            .unwrap();
        assert!(
            service
                .wait_activation_cooldown(&id, Duration::from_secs(10))
                .await
                .unwrap()
                > Duration::from_secs(9)
        );
    }
    service
        .db_conn()
        .unwrap()
        .execute("UPDATE sandboxes SET state = 'paused' WHERE id = ?1", [&id])
        .unwrap();
    let waiter = service.wait_activation_cooldown(&id, Duration::from_secs(2));
    tokio::pin!(waiter);
    tokio::select! {
        biased;
        result = &mut waiter => panic!("cooldown returned early: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
    service
        .db_conn()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'started', activation_retry_after = NULL WHERE id = ?1",
            [&id],
        )
        .unwrap();
    assert!(waiter.await.unwrap() > Duration::from_secs(1));
}

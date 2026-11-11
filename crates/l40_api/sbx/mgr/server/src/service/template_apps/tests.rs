use super::*;

fn runtime(policy: &str) -> TemplateRuntime {
    serde_json::from_value(serde_json::json!({"schema_version": 2, "apps": [
        {"name": "web", "command": ["/bin/sleep", "infinity"], "user": "root",
         "working_directory": "/", "startup_timeout_secs": 3,
         "startup_policy": policy, "readiness": {"type": "launch"}},
        {"name": "worker", "command": ["/bin/sleep", "infinity"], "user": "root",
         "working_directory": "/", "startup_timeout_secs": 6,
         "readiness": {"type": "launch"}}
    ]}))
    .unwrap()
}
fn unit(name: &str, active: &str, sub: &str, started: bool) -> String {
    format!(
        "Id=space-runtime@{name}.service\nLoadState=loaded\nActiveState={active}\nSubState={sub}\nInactiveExitTimestampMonotonic={}\n\n",
        u8::from(started)
    )
}

#[test]
fn snapshot_app_policy_matrix() {
    for (active, sub, started, settled) in [
        ("active", "running", true, true),
        ("activating", "start-post", true, false),
        ("activating", "auto-restart", true, false),
        ("failed", "failed", true, true),
        ("inactive", "dead", true, true),
        ("inactive", "dead", false, false),
    ] {
        for policy in ["required", "best-effort"] {
            let runtime = runtime(policy);
            let batch =
                unit("web", active, sub, started) + &unit("worker", "active", "running", true);
            let result = evaluate(&runtime, &batch, Duration::ZERO);
            if active == "active" {
                assert_eq!(result.unwrap(), Some(vec![]));
            } else if !settled {
                assert_eq!(result.unwrap(), None);
            } else if policy == "required" {
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
            } else {
                assert_eq!(result.unwrap().unwrap().len(), 1);
            }
        }
    }
}

#[test]
fn independent_budgets_and_no_sticky_readiness() {
    let runtime = runtime("best-effort");
    let pending = unit("web", "activating", "start-post", true)
        + &unit("worker", "activating", "auto-restart", true);
    assert_eq!(
        evaluate(&runtime, &pending, Duration::from_secs(5)).unwrap(),
        None
    );
    assert_eq!(
        evaluate(&runtime, &pending, Duration::from_secs(8))
            .unwrap()
            .unwrap()
            .len(),
        2
    );
    // A previously ready app is re-observed while waiting for another app.
    let now_failed =
        unit("web", "failed", "failed", true) + &unit("worker", "active", "running", true);
    assert_eq!(
        evaluate(&runtime, &now_failed, Duration::from_secs(2))
            .unwrap()
            .unwrap(),
        ["web: failed"]
    );
    evaluate(
        &super::tests::runtime("required"),
        &now_failed,
        Duration::ZERO,
    )
    .unwrap_err();
}

#[test]
fn missing_backend_is_retried_but_missing_unit_is_terminal() {
    for policy in ["required", "best-effort"] {
        let runtime = runtime(policy);
        assert_eq!(
            evaluate(&runtime, "", Duration::from_secs(1)).unwrap(),
            None
        );
        let expired = evaluate(&runtime, "", Duration::from_secs(8));
        assert_eq!(expired.is_err(), policy == "required");
        let missing = "Id=space-runtime@web.service\nLoadState=not-found\n\n".to_owned()
            + &unit("worker", "active", "running", true);
        let result = evaluate(&runtime, &missing, Duration::ZERO);
        assert_eq!(result.is_err(), policy == "required");
        if let Ok(warnings) = result {
            assert_eq!(warnings.unwrap(), ["web: unit unavailable"]);
        }
    }
}

#[tokio::test]
async fn snapshot_observer_refuses_tenant_pool_and_nonrunning_sandboxes() {
    use pplx_space_common_core::host::linux::identities::users;
    for (owner, state, pool, expected) in [
        (
            SandboxOwner::parse("tenant").unwrap(),
            SandboxState::Started,
            None,
            tonic::Code::PermissionDenied,
        ),
        (
            TEMPLATE_MANAGER_SANDBOX_OWNER.clone(),
            SandboxState::Paused,
            None,
            tonic::Code::FailedPrecondition,
        ),
        (
            TEMPLATE_MANAGER_SANDBOX_OWNER.clone(),
            SandboxState::Started,
            Some("seed"),
            tonic::Code::FailedPrecondition,
        ),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, state);
        crate::db::update_user(&conn, &id, &owner).unwrap();
        crate::db::set_pool_template(&conn, &id, pool).unwrap();
        let service = SandboxManagerService::for_test(conn);
        let mut request = Request::new(crate::proto::WaitTemplateAppsRequest {
            sandbox_id: id.clone(),
        });
        request.extensions_mut().insert(PeerIdentity::new(
            users::SPACE_TMPLMGR.uid,
            users::SPACE_TMPLMGR.primary_group.gid,
            None,
        ));
        assert_eq!(
            service
                .wait_template_apps(request)
                .await
                .unwrap_err()
                .code(),
            expected
        );
        // A rejected observation cannot leave the lifecycle locked.
        drop(service.auto_lifecycle.wake_lock(&id).try_lock().unwrap());
    }
}

#[test]
fn observer_errors_respect_the_declared_startup_policy() {
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::NotFound,
        tonic::Code::ResourceExhausted,
    ] {
        for policy in ["required", "best-effort"] {
            let result = apply_startup_policy(
                &runtime(policy),
                Err(Status::new(code, "private backend detail")),
            );
            if policy == "required" {
                assert_eq!(result.unwrap_err().code(), code);
            } else {
                let warnings = result.unwrap();
                assert_eq!(warnings.len(), 2);
                assert!(
                    warnings
                        .iter()
                        .all(|warning| !warning.contains("private backend detail"))
                );
            }
        }
    }
    assert_eq!(
        apply_startup_policy(&runtime("required"), Ok(vec![])).unwrap(),
        Vec::<String>::new()
    );
}

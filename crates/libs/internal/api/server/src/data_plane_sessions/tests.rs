use super::*;

#[test]
fn stored_counter_conversion_rejects_negative_values() {
    i64_to_u64(-1, "route_generation").unwrap_err();
    revocation_state(-1, "active", OffsetDateTime::UNIX_EPOCH, None, 0).unwrap_err();
}

#[test]
fn sandbox_runtime_access_marks_terminal_lifecycles_unavailable() {
    assert_eq!(
        sandbox_runtime_access_decision(Some(SandboxLifecycle::Stopped), false),
        SandboxRuntimeAccessDecision::TerminalUnavailable
    );
    assert_eq!(
        sandbox_runtime_access_decision(Some(SandboxLifecycle::Failed), false),
        SandboxRuntimeAccessDecision::TerminalUnavailable
    );
}

#[test]
fn sandbox_runtime_access_marks_wake_paths_retryable() {
    for lifecycle in [
        SandboxLifecycle::Suspended,
        SandboxLifecycle::Suspending,
        SandboxLifecycle::Pausing,
        SandboxLifecycle::Paused,
        SandboxLifecycle::Restoring,
        SandboxLifecycle::Resuming,
        SandboxLifecycle::Starting,
    ] {
        assert_eq!(
            sandbox_runtime_access_decision(Some(lifecycle), false),
            SandboxRuntimeAccessDecision::RetryableConverging
        );
    }
    assert_eq!(
        sandbox_runtime_access_decision(Some(SandboxLifecycle::Started), true),
        SandboxRuntimeAccessDecision::RetryableConverging
    );
}

#[test]
fn sandbox_runtime_access_marks_stable_or_unknown_lifecycles_without_ecw_policy() {
    assert_eq!(
        sandbox_runtime_access_decision(Some(SandboxLifecycle::Started), false),
        SandboxRuntimeAccessDecision::Ready
    );
    assert_eq!(
        sandbox_runtime_access_decision(Some(SandboxLifecycle::Created), false),
        SandboxRuntimeAccessDecision::Ready
    );
    assert_eq!(
        sandbox_runtime_access_decision(None, false),
        SandboxRuntimeAccessDecision::Unknown
    );
}

#[test]
fn command_ready_does_not_expose_internal_permission_denied() {
    let status = sandbox_service_route_command_ready_status(Status::permission_denied(
        "route token is required",
    ));

    assert_eq!(status.code(), Code::Unavailable);
    assert!(status.message().contains("command readiness failed"));
}

#[test]
fn route_backend_endpoint_preserves_registration_and_formats_ip_fallbacks() {
    for (direct, address, expected_authority) in [
        (None, Some("198.51.100.7"), Some("198.51.100.7:9800")),
        (Some(" "), Some("2001:db8::1"), Some("[2001:db8::1]:9800")),
        (None, Some(" ::1 "), Some("[::1]:9800")),
        (None, Some("not-an-ip"), None),
        (None, Some(""), None),
        (None, None, None),
    ] {
        let endpoint = node_api_backend_endpoint(direct, address);
        match expected_authority {
            Some(authority) => {
                let endpoint = tonic::transport::Endpoint::from_shared(endpoint.unwrap()).unwrap();
                assert_eq!(endpoint.uri().authority().unwrap().as_str(), authority);
            }
            None => assert!(endpoint.is_none()),
        }
    }
    for fallback in [None, Some("2001:db8::1"), Some("not-an-ip")] {
        assert_eq!(
            node_api_backend_endpoint(Some("https://registered-worker:9443"), fallback).as_deref(),
            Some("https://registered-worker:9443"),
        );
    }
}
#[test]
fn ingress_wake_requires_live_unrevoked_session_even_for_internal_route_refresh() {
    let now = OffsetDateTime::UNIX_EPOCH;
    for (status, expires, allowed) in [
        ("active", Some(now + time::Duration::seconds(1)), true),
        ("revoked", Some(now + time::Duration::seconds(1)), false),
        ("active", Some(now), false),
        ("active", None, false),
    ] {
        assert_eq!(
            validate_ingress_session_access(status, expires, now).is_ok(),
            allowed
        );
    }
}

#[test]
fn delegated_recovery_only_repairs_ingress_availability() {
    for code in [
        Code::Unavailable,
        Code::FailedPrecondition,
        Code::NotFound,
        Code::Aborted,
        Code::Unauthenticated,
        Code::PermissionDenied,
        Code::InvalidArgument,
        Code::ResourceExhausted,
        Code::Internal,
        Code::DeadlineExceeded,
    ] {
        let error = Status::new(code, "synthetic route failure");
        assert!(
            !activation_may_repair(false, &error),
            "non-ingress {code:?}"
        );
        assert_eq!(
            activation_may_repair(true, &error),
            matches!(
                code,
                Code::Unavailable | Code::FailedPrecondition | Code::NotFound | Code::Aborted
            )
        );
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_current_worker_leaves_time_for_delegated_recovery() {
    for budget in [Duration::from_millis(400), Duration::from_secs(15)] {
        let start = tokio::time::Instant::now();
        let deadline = start + budget;
        let probe = tokio::time::timeout_at(
            deadline,
            prepare_current_ingress_worker(deadline, true, std::future::pending()),
        )
        .await;
        assert_eq!(
            probe
                .expect("probe must finish before caller deadline")
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        assert!(deadline - tokio::time::Instant::now() >= budget * 3 / 4);
        // Recovery that fits the original deadline remains possible after a
        // stalled probe, rather than losing the entire wait on the old route.
        tokio::time::timeout_at(deadline, tokio::time::sleep(budget / 2))
            .await
            .unwrap();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(400);
    let prepare = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    };
    prepare_current_ingress_worker(deadline, false, prepare)
        .await
        .unwrap();
}

/// The attach statement itself stores the route the resolver derives, so the
/// resolver finds nothing left to write and skips `update_route_row`; where
/// the SQL derivation would ever disagree, the resolver's comparison catches
/// it and rewrites the route.
#[tokio::test(flavor = "current_thread")]
async fn attach_statement_stores_the_route_the_resolver_derives() {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db()
        .await
        .expect("test database")
    else {
        return;
    };
    let pool = &db.pool;
    for address in [
        "10.0.0.56",
        "fd00::56",
        "fd00:0:0:1::56",
        "::ffff:10.0.0.56",
    ] {
        let node_id = NodeId::new();
        let sandbox_id = SandboxId::new();
        let session_id = format!("term_{}", uuid::Uuid::now_v7());
        crate::tests::insert_data_plane_worker_node(pool, node_id, address)
            .await
            .unwrap();
        crate::tests::insert_data_plane_active_sandbox(pool, sandbox_id, node_id)
            .await
            .unwrap();
        crate::tests::insert_data_plane_session(pool, &session_id, sandbox_id)
            .await
            .unwrap();
        sqlx::query(sql!(
            "UPDATE data_plane_sessions
             SET token_hash = 'statement-token',
                 token_expires_at = NOW() + INTERVAL '5 minutes',
                 scopes = ARRAY['terminal:connect']
             WHERE session_id = $1"
        ))
        .bind(&session_id)
        .execute(pool)
        .await
        .unwrap();

        let row = fetch_and_persist_route_row(
            pool,
            &session_id,
            None,
            RoutePersistence {
                token_sha256: "statement-token",
                required_scope: "terminal:connect",
            },
        )
        .await
        .unwrap();

        assert_eq!(row.current_node_id, Some(node_id), "{address}");
        assert_eq!(
            row.backend_endpoint,
            resolve_backend_endpoint(&row),
            "{address}: SQL and Rust derive the same endpoint"
        );
        assert_eq!(
            row.backend_transport.as_deref(),
            Some(BACKEND_TRANSPORT_GRPC),
            "{address}"
        );
        assert_eq!(row.route_generation.get(), 2, "{address}");

        // A caller without the token only reads.
        let read = fetch_and_persist_route_row(
            pool,
            &session_id,
            None,
            RoutePersistence {
                token_sha256: "other-token",
                required_scope: "terminal:connect",
            },
        )
        .await
        .unwrap();
        assert_eq!(read.route_generation.get(), 2, "{address}");
    }
}

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

/// A session whose stored route is stale is re-pointed by `update_route_row`,
/// which re-reads the sandbox. A suspend and a restore can commit between the
/// resolver's first read, which saw the sandbox `started`, and that update; the
/// refreshed row then names the restoring replacement, and must be refused
/// retryably rather than signed and returned.
#[tokio::test(flavor = "current_thread")]
async fn a_route_refresh_refuses_a_sandbox_that_started_restoring()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", uuid::Uuid::now_v7());
    crate::tests::insert_data_plane_worker_node(pool, node_id, "10.0.0.45").await?;
    crate::tests::insert_data_plane_active_sandbox(pool, sandbox_id, node_id).await?;
    crate::tests::insert_active_execution(pool, sandbox_id, "10.80.0.45", 2).await?;
    crate::tests::insert_data_plane_session(pool, &session_id, sandbox_id).await?;
    let first = route_rows::fetch_route_row(pool, &session_id, None).await?;
    assert_eq!(first.sandbox_lifecycle, SandboxLifecycle::Started);
    // A same-node suspend and restore commit after that read.
    sqlx::query("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1")
        .bind(sandbox_id)
        .execute(pool)
        .await?;

    let refreshed =
        route_rows::update_route_row(pool, &session_id, node_id, "10.0.0.45:50051", "grpc").await;

    let error = refreshed.expect_err("a restoring sandbox must not be routed to");
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error:?}");
    assert!(error.message().contains("restoring"), "{error:?}");
    // The refused refresh changed no route.
    let after = route_rows::fetch_route_row(pool, &session_id, None).await?;
    assert_eq!(
        (
            after.route_generation,
            after.current_node_id,
            after.backend_endpoint
        ),
        (
            first.route_generation,
            first.current_node_id,
            first.backend_endpoint
        )
    );
    Ok(())
}

/// Rest a sandbox in `lifecycle` with no node, as a pause, stop or suspend
/// leaves it, with only an operation of `kind` in `state` for it, if any.
async fn rest_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    lifecycle: SandboxLifecycle,
    operation: Option<(&str, &str)>,
) -> Result<(), sqlx::Error> {
    use pplx_space_common_core::sql;
    let target_id = pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $2, placement = 'unplaced', active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(lifecycle.as_str())
    .execute(pool)
    .await?;
    sqlx::query(sql!("DELETE FROM public_operations WHERE target_id = $1"))
        .bind(&target_id)
        .execute(pool)
        .await?;
    if let Some((kind, state)) = operation {
        sqlx::query(sql!(
            "INSERT INTO public_operations (operation_id, org_id, kind, target_type, target_id, state)
             VALUES ($1, $2, $3, 'sandbox', $4, $5)"
        ))
        .bind(uuid::Uuid::now_v7())
        .bind(pplx_space_db_test_fixtures::SEED_ORG_ID)
        .bind(kind)
        .bind(&target_id)
        .bind(state)
        .execute(pool)
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_route_is_at_rest_only_when_nothing_is_about_to_place_the_sandbox()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use SandboxLifecycle::{Paused, Restoring, Started, Stopped, Suspended};
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let org_id = OrgId::from_uuid(pplx_space_db_test_fixtures::SEED_ORG_ID);
    crate::tests::insert_data_plane_worker_node(pool, node_id, "10.0.0.46").await?;
    crate::tests::insert_data_plane_active_sandbox(pool, sandbox_id, node_id).await?;

    assert_eq!(route_at_rest(pool, org_id, SandboxId::new()).await, None);
    for lifecycle in [Restoring, Started] {
        rest_sandbox(pool, sandbox_id, lifecycle, None).await?;
        assert_eq!(
            route_at_rest(pool, org_id, sandbox_id).await,
            None,
            "{lifecycle:?}"
        );
    }
    rest_sandbox(pool, sandbox_id, Suspended, None).await?;
    assert_eq!(
        route_at_rest(pool, org_id, sandbox_id).await,
        Some(Suspended)
    );

    for (state, at_rest) in [
        ("queued", None),
        ("running", None),
        ("succeeded", Some(Suspended)),
    ] {
        rest_sandbox(pool, sandbox_id, Suspended, Some(("wake_sandbox", state))).await?;
        assert_eq!(
            route_at_rest(pool, org_id, sandbox_id).await,
            at_rest,
            "with a {state} wake"
        );
    }

    // Each action that brings the sandbox back to running, queued before the
    // dispatcher claims it and moves the lifecycle: a client that follows a
    // stopped sandbox's `:start` hint and attaches at once waits for it.
    for (lifecycle, kind) in [
        (Stopped, "start_sandbox"),
        (Suspended, "restore_sandbox"),
        (Suspended, "resume_sandbox"),
        (Paused, "resume_sandbox"),
        (Suspended, "wake_sandbox"),
    ] {
        rest_sandbox(pool, sandbox_id, lifecycle, Some((kind, "queued"))).await?;
        assert_eq!(
            route_at_rest(pool, org_id, sandbox_id).await,
            None,
            "{lifecycle:?} with a queued {kind}"
        );
    }
    // An operation that leaves it at rest gives it no node.
    rest_sandbox(pool, sandbox_id, Stopped, Some(("stop_sandbox", "queued"))).await?;
    assert_eq!(route_at_rest(pool, org_id, sandbox_id).await, Some(Stopped));
    Ok(())
}

/// The resolver reads the route row, then the pending activations. A wake
/// that finishes in between must not leave the row's old lifecycle paired
/// with "nothing is coming": the lifecycle is read again with the operations.
#[tokio::test]
async fn a_wake_finishing_after_the_route_row_was_read_leaves_the_route_retryable()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_common_core::sql;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", uuid::Uuid::now_v7());
    crate::tests::insert_data_plane_worker_node(pool, node_id, "10.0.0.47").await?;
    crate::tests::insert_data_plane_active_sandbox(pool, sandbox_id, node_id).await?;
    crate::tests::insert_data_plane_session(pool, &session_id, sandbox_id).await?;
    rest_sandbox(
        pool,
        sandbox_id,
        SandboxLifecycle::Suspended,
        Some(("wake_sandbox", "running")),
    )
    .await?;
    let row = route_rows::fetch_route_row(pool, &session_id, None).await?;
    assert_eq!(row.sandbox_lifecycle, SandboxLifecycle::Suspended);

    // The wake places the sandbox and succeeds.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started', placement = 'placed', active_node_id = $2, target_node_id = $2
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'succeeded' WHERE target_id = $1"
    ))
    .bind(pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string())
    .execute(pool)
    .await?;

    assert_eq!(route_at_rest(pool, row.org_id, row.sandbox_id).await, None);
    Ok(())
}

#[tokio::test]
async fn only_a_definite_no_from_the_lifecycle_owner_leaves_a_route_at_rest() {
    struct Owner(
        Result<bool, tonic::Code>,
        std::sync::Mutex<Vec<ImplicitWakeActivity>>,
    );
    #[tonic::async_trait]
    impl crate::ingress_activation::IngressActivation for Owner {
        async fn prepare(&self, _: SandboxId, _: std::time::Duration) -> Result<(), Status> {
            Ok(())
        }
        async fn queue_implicit_wake(
            &self,
            _: SandboxId,
            activity: ImplicitWakeActivity,
            _: std::time::Duration,
        ) -> Result<bool, Status> {
            self.1.lock().unwrap().push(activity);
            self.0
                .map_err(|code| Status::new(code, "synthetic owner failure"))
        }
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let sandbox_id = SandboxId::new();
    let access = ImplicitWakeActivity::Access;
    // Unknown is not "nothing can wake it": the attach stays retryable.
    assert!(implicit_wake_queued(None, sandbox_id, access, deadline).await);
    for (answer, queued) in [
        (Ok(true), true),
        (Ok(false), false),
        (Err(tonic::Code::Unavailable), true),
        // An API from before the request.
        (Err(tonic::Code::Unimplemented), true),
    ] {
        let owner = Owner(answer, Default::default());
        assert_eq!(
            implicit_wake_queued(Some(&owner), sandbox_id, access, deadline).await,
            queued,
            "{answer:?}"
        );
        assert_eq!(
            *owner.1.lock().unwrap(),
            vec![access],
            "the activity is asked about"
        );
    }
}

/// The node judges a first terminal or process attach, which starts the
/// process, as a command, and a reconnect to a process that already started,
/// or any file-watch call, as access; the wake the resolver asks for follows.
#[test]
fn a_reattach_asks_for_the_wake_the_node_would_allow() {
    use ImplicitWakeActivity::{Access, Command};
    for (kind, process_id, activity) in [
        ("process", None, Command),
        ("terminal", None, Command),
        ("process", Some("proc-1"), Access),
        ("terminal", Some("proc-1"), Access),
        ("file_watch", None, Access),
        ("not-a-kind", None, Access),
    ] {
        assert_eq!(
            waiting_activity(kind, process_id),
            activity,
            "{kind} {process_id:?}"
        );
    }
}

/// A stale-route reset re-opens the port exposure applied on the sandbox's
/// active node only for the route generation the resolver hands out now: a
/// Kubernetes sandbox's active execution generation, or a Direct sandbox's
/// route identity generation, since a Direct sandbox has no execution row.
#[tokio::test]
async fn a_service_route_reset_is_fenced_by_the_current_route_generation()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_common_core::sql;
    use pplx_space_common_core::types::SubstrateKind;
    const PORT: u16 = 8080;
    const GENERATION: i64 = 3;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    for substrate in [SubstrateKind::Kubernetes, SubstrateKind::Space] {
        for (route_generation, applied_on_active_node, reset) in [
            (GENERATION - 1, true, false),
            (GENERATION, false, false),
            (GENERATION, true, true),
        ] {
            let case = format!(
                "{substrate} sandbox, route generation {route_generation}, \
                 applied on the active node: {applied_on_active_node}"
            );
            let node_id = NodeId::new();
            let sandbox_id = SandboxId::new();
            sqlx::query(sql!(
                "INSERT INTO nodes (id, address, role, managed_by, state)
                 VALUES ($1, '10.0.0.48'::INET, 'worker', $2, 'ready')"
            ))
            .bind(node_id)
            .bind(substrate)
            .execute(pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO sandboxes (
                     id, user_id, org_id, managed_by, lifecycle, placement,
                     active_node_id, target_node_id, network_policy_generation
                 )
                 VALUES (
                     $1,
                     '00000000-0000-0000-0000-000000000010',
                     '00000000-0000-0000-0000-000000000001',
                     $2, 'started', 'placed', $3, $3, 1
                 )"
            ))
            .bind(sandbox_id)
            .bind(substrate)
            .bind(node_id)
            .execute(pool)
            .await?;
            match substrate {
                SubstrateKind::Kubernetes => {
                    crate::tests::insert_active_execution(
                        pool,
                        sandbox_id,
                        "10.80.0.48",
                        GENERATION,
                    )
                    .await?;
                }
                SubstrateKind::Space => {
                    sqlx::query(sql!(
                        "INSERT INTO sandbox_direct_route_identities (
                             sandbox_id, route_generation, current_node_id, substrate_kind,
                             execution_generation, activation_attempt_id
                         )
                         VALUES ($1, 1, $2, $3, $4, $5)"
                    ))
                    .bind(sandbox_id)
                    .bind(node_id)
                    .bind(substrate)
                    .bind(GENERATION)
                    .bind(Uuid::now_v7())
                    .execute(pool)
                    .await?;
                }
            }
            let applied_node_id = if applied_on_active_node {
                node_id
            } else {
                NodeId::new()
            };
            sqlx::query(sql!(
                "INSERT INTO sandbox_port_exposures (
                     sandbox_id, protocol, guest_port, visibility, source, desired_state,
                     apply_state, last_applied_node_id, last_bridge_status, last_observed_at
                 )
                 VALUES ($1, 'tcp', $2, 'private', 'native', 'open',
                         'applied', $3, 'loopback_forwarded', NOW())"
            ))
            .bind(sandbox_id)
            .bind(i32::from(PORT))
            .bind(applied_node_id)
            .execute(pool)
            .await?;

            let response = Box::pin(reset_sandbox_service_route(
                pool,
                UnstableResetSandboxServiceRouteRequest {
                    route: Some(crate::proto::SandboxServiceRouteKey {
                        sandbox_id: sandbox_id.to_string(),
                        port: u32::from(PORT),
                        kind: SandboxServiceRouteKind::ServicePort as i32,
                    }),
                    route_generation: u64::try_from(route_generation)?,
                    diagnostic_message: Some("synthetic stale route".to_string()),
                },
            ))
            .await?;

            let expected = if reset {
                UnstableResetSandboxServiceRouteResult::Reset
            } else {
                UnstableResetSandboxServiceRouteResult::Noop
            };
            assert_eq!(
                (response.result, response.reconcile_enqueued),
                (expected as i32, reset),
                "{case}"
            );
            let (apply_state, last_applied_node_id): (String, Option<NodeId>) =
                sqlx::query_as(sql!(
                    "SELECT apply_state, last_applied_node_id
                     FROM sandbox_port_exposures
                     WHERE sandbox_id = $1"
                ))
                .bind(sandbox_id)
                .fetch_one(pool)
                .await?;
            let expected_exposure = if reset {
                ("pending".to_string(), None)
            } else {
                ("applied".to_string(), Some(applied_node_id))
            };
            assert_eq!(
                (apply_state, last_applied_node_id),
                expected_exposure,
                "{case}"
            );
            let queued: i64 = sqlx::query_scalar(sql!(
                "SELECT COUNT(*) FROM node_reconcile_queue
                 WHERE node_id = $1 AND element = $2"
            ))
            .bind(node_id)
            .bind(sandbox_id.to_string())
            .fetch_one(pool)
            .await?;
            assert_eq!(queued, i64::from(reset), "{case}");
        }
    }
    Ok(())
}

/// An activation owner that queues every wake it is asked for and records it.
#[derive(Default)]
struct QueuingOwner(std::sync::Mutex<Vec<ImplicitWakeActivity>>);

#[tonic::async_trait]
impl crate::ingress_activation::IngressActivation for QueuingOwner {
    async fn prepare(&self, _: SandboxId, _: std::time::Duration) -> Result<(), Status> {
        Err(Status::internal("a reattach must not be prepared"))
    }
    async fn queue_implicit_wake(
        &self,
        _: SandboxId,
        activity: ImplicitWakeActivity,
        _: std::time::Duration,
    ) -> Result<bool, Status> {
        self.0.lock().unwrap().push(activity);
        Ok(true)
    }
}

/// A worker that idle-suspends a Direct sandbox clears `active_node_id` but
/// leaves its Direct route identity naming the worker, which no longer holds
/// it. A reattach to that row must queue its wake, as one to the same
/// Kubernetes row does, and not route to the worker; a placed Direct row
/// still routes to its worker.
#[tokio::test(flavor = "current_thread")]
async fn a_reattach_wakes_an_unplaced_sandbox_whatever_its_direct_route_names()
-> pplx_space_test_env::TestResult {
    use pplx_space_common_core::sql;
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let authority = crate::tests::test_route_ed25519_authority();
    let node_id = NodeId::new();
    let endpoint = "https://direct-worker-a:9802";
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.48'::INET, 'worker', 'space', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(endpoint)
    .execute(pool)
    .await?;

    #[derive(Debug, Clone, Copy)]
    enum Row {
        DirectSuspended,
        KubernetesSuspended,
        DirectStarted,
    }
    for case in [
        Row::KubernetesSuspended,
        Row::DirectStarted,
        Row::DirectSuspended,
    ] {
        let sandbox_id = SandboxId::new();
        let session_id = format!("term_{}", uuid::Uuid::now_v7());
        crate::tests::insert_data_plane_active_sandbox(pool, sandbox_id, node_id).await?;
        crate::tests::insert_data_plane_session(pool, &session_id, sandbox_id).await?;
        match case {
            Row::DirectSuspended | Row::DirectStarted => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(pool)
                .await?;
                sqlx::query(sql!(
                    "INSERT INTO sandbox_direct_route_identities (
                         sandbox_id, route_generation, current_node_id, substrate_kind,
                         execution_generation, activation_attempt_id
                     )
                     VALUES ($1, 3, $2, 'space', 3, $3)"
                ))
                .bind(sandbox_id)
                .bind(node_id)
                .bind(uuid::Uuid::now_v7())
                .execute(pool)
                .await?;
            }
            Row::KubernetesSuspended => {
                crate::tests::insert_active_execution(pool, sandbox_id, "10.80.0.48", 3).await?;
            }
        }
        if matches!(case, Row::DirectSuspended | Row::KubernetesSuspended) {
            rest_sandbox(pool, sandbox_id, SandboxLifecycle::Suspended, None).await?;
        }

        let owner = QueuingOwner::default();
        let resolved = Box::pin(resolve_session_route(
            Duration::from_secs(5),
            Some(&owner),
            pool,
            Some(&authority),
            Some(DataPlaneSessionRefreshAuthority::Granted),
            ResolveDataPlaneSessionRouteRequest {
                session_id,
                protocol_session_id: None,
                token_sha256: None,
                required_scope: None,
                requested_transport: None,
            },
        ))
        .await;
        let queued = owner.0.lock().unwrap().clone();
        match case {
            Row::DirectSuspended | Row::KubernetesSuspended => {
                let error = resolved.expect_err(&format!("{case:?} must not be routed"));
                assert_eq!(
                    error.code(),
                    Code::FailedPrecondition,
                    "{case:?}: {error:?}"
                );
                assert!(
                    error.message().contains("not assigned to a node"),
                    "{case:?}: {error:?}"
                );
                assert_eq!(
                    queued,
                    vec![ImplicitWakeActivity::Command],
                    "{case:?} queues its wake"
                );
            }
            Row::DirectStarted => {
                let response = resolved?;
                let backend = response.backend.expect("a placed row has a backend");
                assert_eq!(backend.node_id, node_id.to_string());
                assert_eq!(backend.endpoint, endpoint);
                assert!(queued.is_empty(), "a placed row queues no wake");
            }
        }
        // The service route reads the same placement.
        let service = fetch_sandbox_service_route_row(pool, sandbox_id).await?;
        assert_eq!(
            service.active_node_id,
            matches!(case, Row::DirectStarted).then_some(node_id),
            "{case:?}"
        );
    }
    Ok(())
}

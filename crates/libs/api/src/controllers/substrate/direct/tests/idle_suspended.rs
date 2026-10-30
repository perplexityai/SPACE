//! A sandbox the node suspended for idleness: its row is `suspended` and
//! unplaced, `active_node_id` is NULL, and `target_node_id` names the worker
//! holding the local artifact. Wake and delete must reach that worker.
use super::*;

/// The node-published idle-suspend shape: a suspended resident on `node_id`,
/// a row whose only placement is `target_node_id = node_id`, and the route
/// identity of the execution the node suspended, which its publication
/// leaves in place (so `Sandbox::active_node_id` still reads `node_id`).
async fn seed_idle_suspended(
    pool: &PgPool,
    state: &SimState,
    sandbox_id: SandboxId,
    node_id: NodeId,
    endpoint: &str,
) -> TestResult {
    seed_worker(pool, node_id, endpoint).await?;
    seed_idle_suspended_sandbox(pool, state, sandbox_id, node_id, None).await
}

async fn seed_worker(pool: &PgPool, node_id: NodeId, endpoint: &str) -> TestResult {
    insert_ready_worker(pool, node_id, endpoint, "9.0.0").await?;
    insert_installed_component(
        pool,
        node_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    Ok(())
}

/// [`seed_idle_suspended`] on an already seeded worker. `local_policy` is an
/// on-node override the resident holds with Local authority.
async fn seed_idle_suspended_sandbox(
    pool: &PgPool,
    state: &SimState,
    sandbox_id: SandboxId,
    node_id: NodeId,
    local_policy: Option<&AutoLifecyclePolicy>,
) -> TestResult {
    insert_sandbox(pool, sandbox_id, Some(node_id)).await?;
    insert_direct_route_provenance(pool, sandbox_id, 1).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities
         (sandbox_id, route_generation, current_node_id, substrate_kind,
          execution_generation, activation_attempt_id)
         VALUES ($1, 1, $2, 'space', 1, $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(SandboxActivationAttemptId::new())
    .execute(pool)
    .await?;
    insert_observed_source(pool, sandbox_id, node_id).await?;
    let snapshot_id = insert_full_snapshot(pool, sandbox_id, node_id, "9.0.0", "ready").await?;
    insert_snapshot_component(
        pool,
        snapshot_id,
        "rootfs",
        "system",
        Some("sha256:rootfs"),
        None,
        None,
    )
    .await?;
    let mut resident = creating_record(sandbox_id);
    resident.status = "running".to_owned();
    if local_policy.is_some() {
        resident.auto_lifecycle_authority = PolicyAuthority::Local;
    }
    state.insert_new(resident).await?;
    if let Some(policy) = local_policy {
        use prost::Message;
        state
            .apply_access_deadline(
                &sandbox_id.to_string(),
                None,
                Some(crate::controllers::auto_lifecycle::policy_to_proto(policy).encode_to_vec()),
            )
            .await?;
    }
    state.suspend_to_shared_dir(&sandbox_id.to_string()).await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct Placement {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    restore_lease_token: Option<Uuid>,
}

async fn placement(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Placement> {
    Ok(sqlx::query_as::<_, Placement>(sql!(
        "SELECT lifecycle, placement, active_node_id, restore_lease_token
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(pool)
    .await?)
}

#[rstest::rstest]
#[case::explicit(None)]
#[case::command(Some(ExecutionActivityKind::Command))]
#[tokio::test(flavor = "current_thread")]
async fn wake_restores_an_idle_suspended_sandbox_on_its_artifact_node(
    #[case] wake_activity: Option<ExecutionActivityKind>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    seed_idle_suspended(
        &fixture.pool,
        &state,
        sandbox_id,
        node_id,
        server.endpoint(),
    )
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");
    assert_eq!(sandbox.active_node_id, Some(node_id));

    Box::pin(fixture.substrate.wake_execution(
        pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
        &sandbox,
        Duration::from_secs(5),
        wake_activity,
        crate::controllers::substrate::RecoveryCharge::Exempt,
    ))
    .await?;

    assert_eq!(state.get(&sandbox_id.to_string()).await?.status, "running");
    assert_eq!(
        placement(&fixture.pool, sandbox_id).await?,
        Placement {
            lifecycle: "started".to_owned(),
            placement: "on_node".to_owned(),
            active_node_id: Some(node_id),
            restore_lease_token: None,
        }
    );
    drop(server);
    Ok(())
}

/// The node holding the idle-suspended record may carry a Local override of
/// the auto-lifecycle policy. An implicit wake honors it as a placed wake
/// does: the cluster policy, which callers check first, is not enough. Like
/// the worker, it refuses only a dormant record: one already running under
/// another wake's restore lease is that restore's to finish.
#[tokio::test(flavor = "current_thread")]
async fn implicit_wake_of_an_idle_suspended_sandbox_honors_the_local_override() -> TestResult {
    #[derive(Debug)]
    enum Expect {
        Refused,
        Woken,
        RestoreInFlight,
    }

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    seed_worker(&fixture.pool, node_id, server.endpoint()).await?;
    let no_wake = crate::e2b::auto_lifecycle_policy(60, true, false);
    let wake_on_access = crate::e2b::auto_lifecycle_policy(60, true, true);

    for (label, local_policy, restore_in_flight, expect) in [
        ("override denies", &no_wake, false, Expect::Refused),
        ("override allows", &wake_on_access, false, Expect::Woken),
        (
            "override denies a record another restore started",
            &no_wake,
            true,
            Expect::RestoreInFlight,
        ),
    ] {
        let sandbox_id = SandboxId::new();
        seed_idle_suspended_sandbox(
            &fixture.pool,
            &state,
            sandbox_id,
            node_id,
            Some(local_policy),
        )
        .await?;
        let lease_token = Uuid::now_v7();
        if restore_in_flight {
            state.set_status(&sandbox_id.to_string(), "running").await?;
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET lifecycle = 'restoring',
                        restore_lease_expires_at = NOW() + INTERVAL '1 hour',
                        restore_lease_token = $2
                  WHERE id = $1"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(lease_token)
            .execute(&fixture.pool)
            .await?;
        }
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
            .await?
            .expect("sandbox row");

        let result = Box::pin(fixture.substrate.wake_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            &sandbox,
            Duration::from_secs(5),
            Some(ExecutionActivityKind::Command),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        ))
        .await;

        let status = state.get(&sandbox_id.to_string()).await?.status;
        let row = placement(&fixture.pool, sandbox_id).await?;
        match expect {
            Expect::Woken => {
                result.unwrap_or_else(|error| panic!("{label}: {error:?}"));
                assert_eq!(status, "running", "{label}");
                assert_eq!(row.lifecycle, "started", "{label}");
            }
            Expect::Refused => {
                let error = match result {
                    Ok(_) => panic!("{label}: a denied implicit wake must not restore"),
                    Err(error) => error,
                };
                assert!(
                    matches!(error, SubstrateError::Conflict { .. }),
                    "{label}: {error:?}"
                );
                assert_eq!(
                    status, "suspended",
                    "{label}: the record must stay suspended"
                );
                assert_eq!(
                    row,
                    Placement {
                        lifecycle: "suspended".to_owned(),
                        placement: "unplaced".to_owned(),
                        active_node_id: None,
                        restore_lease_token: None,
                    },
                    "{label}"
                );
            }
            Expect::RestoreInFlight => {
                let error = match result {
                    Ok(_) => panic!("{label}: the lease owner's restore must not be joined"),
                    Err(error) => error,
                };
                assert!(
                    matches!(error, SubstrateError::RestoreInFlight { .. }),
                    "{label}: {error:?}"
                );
                assert_eq!(status, "running", "{label}");
                assert_eq!(
                    row,
                    Placement {
                        lifecycle: "restoring".to_owned(),
                        placement: "unplaced".to_owned(),
                        active_node_id: None,
                        restore_lease_token: Some(lease_token),
                    },
                    "{label}"
                );
            }
        }
    }
    drop(server);
    Ok(())
}

/// An implicit access wake of a row with no route receipt, whose only
/// placement is `target_node_id`, honors that node's Local override too.
#[tokio::test(flavor = "current_thread")]
async fn implicit_access_wake_of_a_target_only_row_honors_the_local_override() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    seed_worker(&fixture.pool, node_id, server.endpoint()).await?;
    let no_wake = crate::e2b::auto_lifecycle_policy(60, true, false);
    let wake_on_access = crate::e2b::auto_lifecycle_policy(60, true, true);

    for (label, local_policy, expect_wake) in [
        ("override denies", &no_wake, false),
        ("override allows", &wake_on_access, true),
    ] {
        let sandbox_id = SandboxId::new();
        seed_idle_suspended_sandbox(
            &fixture.pool,
            &state,
            sandbox_id,
            node_id,
            Some(local_policy),
        )
        .await?;
        sqlx::query(sql!(
            "DELETE FROM sandbox_direct_route_identities WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&fixture.pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(sqlx::types::Json(&wake_on_access))
        .execute(&fixture.pool)
        .await?;
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
            .await?
            .expect("sandbox row");
        assert_eq!(sandbox.active_node_id, None, "{label}");

        let result = Box::pin(fixture.substrate.access_runtime_client(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("ingress"),
            &sandbox,
            ExecutionActivityKind::Access,
            Duration::from_secs(5),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        ))
        .await;

        let status = state.get(&sandbox_id.to_string()).await?.status;
        let row = placement(&fixture.pool, sandbox_id).await?;
        if expect_wake {
            result.unwrap_or_else(|error| panic!("{label}: {error}"));
            assert_eq!(status, "running", "{label}");
            assert_eq!(row.lifecycle, "started", "{label}");
        } else {
            let error = match result {
                Ok(_) => panic!("{label}: a denied implicit wake must not restore"),
                Err(error) => error,
            };
            assert_eq!(error.status(), axum::http::StatusCode::CONFLICT, "{label}");
            assert_eq!(
                status, "suspended",
                "{label}: the record must stay suspended"
            );
            assert_eq!(row.lifecycle, "suspended", "{label}");
            assert_eq!(row.restore_lease_token, None, "{label}");
        }
    }
    drop(server);
    Ok(())
}

/// A wake that raced the node's suspend can leave the record live on the
/// artifact node while the row is `suspended` with no active node. A wake
/// adopts that record when it runs the row's current execution generation:
/// it places the row back on that node and wakes it there, with no restore
/// and no new generation. A record from an older generation, a live restore
/// lease, or a row that left the lifecycle the wake read is not adopted; the
/// restore path then answers, and its cleanup never deletes a live copy.
#[tokio::test(flavor = "current_thread")]
async fn wake_adopts_a_live_record_on_its_artifact_node() -> TestResult {
    #[derive(Debug, Clone, Copy)]
    enum Row {
        Suspended,
        RestoreLeaseHeld,
        MovedTo(&'static str),
    }
    #[derive(Debug)]
    enum Expect {
        Adopted,
        RestoredElsewhereKeepingSource,
        RestoreInFlight,
        Untouched,
        RestoredElsewhereDeletingSource,
    }

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let source_id = NodeId::new();
    let other_id = NodeId::new();
    let (source_server, source) = start_sim_worker(source_id).await?;
    let (other_server, other) = start_sim_worker(other_id).await?;
    seed_worker(&fixture.pool, source_id, source_server.endpoint()).await?;
    seed_worker(&fixture.pool, other_id, other_server.endpoint()).await?;

    // (label, record status, record execution generation, row, source is a
    // restore candidate, expectation). The row's execution generation is 1.
    for (label, record_status, record_generation, row, source_is_candidate, expect) in [
        (
            "live record at the row's generation",
            "running",
            Some(1),
            Row::Suspended,
            true,
            Expect::Adopted,
        ),
        (
            "live record from an older generation",
            "running",
            Some(0),
            Row::Suspended,
            true,
            Expect::RestoredElsewhereKeepingSource,
        ),
        (
            "live record under another wake's restore lease",
            "running",
            Some(1),
            Row::RestoreLeaseHeld,
            true,
            Expect::RestoreInFlight,
        ),
        (
            "live record, row now deleting",
            "running",
            Some(1),
            Row::MovedTo("deleting"),
            true,
            Expect::Untouched,
        ),
        (
            "live record, row now quarantined",
            "running",
            Some(1),
            Row::MovedTo("quarantined"),
            true,
            Expect::Untouched,
        ),
        (
            "suspended record on a node that is not a candidate",
            "suspended",
            Some(1),
            Row::Suspended,
            false,
            Expect::RestoredElsewhereDeletingSource,
        ),
    ] {
        let sandbox_id = SandboxId::new();
        seed_idle_suspended_sandbox(&fixture.pool, &source, sandbox_id, source_id, None).await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations
                 (snapshot_id, storage_connection_string, org_id)
             SELECT id, 's3://bucket/snapshots/sandbox', org_id
               FROM snapshots
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&fixture.pool)
        .await?;
        source
            .set_route_identity_provenance(
                &sandbox_id.to_string(),
                Some(1),
                record_generation,
                None,
            )
            .await?;
        if record_status != "suspended" {
            source
                .set_status(&sandbox_id.to_string(), record_status)
                .await?;
        }
        if !source_is_candidate {
            sqlx::query(sql!(
                "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .execute(&fixture.pool)
            .await?;
        }
        let lease_token = Uuid::now_v7();
        if let Row::RestoreLeaseHeld = row {
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET lifecycle = 'restoring',
                        restore_lease_expires_at = NOW() + INTERVAL '1 hour',
                        restore_lease_token = $2
                  WHERE id = $1"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(lease_token)
            .execute(&fixture.pool)
            .await?;
        }
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
            .await?
            .expect("sandbox row");
        // The wake carries the lifecycle it read; the row moves on after.
        if let Row::MovedTo(lifecycle) = row {
            sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                .bind(sandbox_id.as_uuid())
                .bind(lifecycle)
                .execute(&fixture.pool)
                .await?;
        }

        let result = Box::pin(fixture.substrate.wake_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            &sandbox,
            Duration::from_secs(5),
            None,
            crate::controllers::substrate::RecoveryCharge::Exempt,
        ))
        .await;

        let source_status = source
            .get(&sandbox_id.to_string())
            .await
            .ok()
            .map(|record| record.status);
        let other_status = other
            .get(&sandbox_id.to_string())
            .await
            .ok()
            .map(|record| record.status);
        let placed = placement(&fixture.pool, sandbox_id).await?;
        let (route_node, route_generation, last_generation) =
            sqlx::query_as::<_, (NodeId, i64, i64)>(sql!(
                "SELECT route.current_node_id, route.execution_generation,
                        provenance.last_execution_generation
                   FROM sandbox_direct_route_identities route
                   JOIN sandbox_direct_route_provenance provenance
                     ON provenance.sandbox_id = route.sandbox_id
                  WHERE route.sandbox_id = $1"
            ))
            .bind(sandbox_id.as_uuid())
            .fetch_one(&fixture.pool)
            .await?;
        let restored_elsewhere = Placement {
            lifecycle: "started".to_owned(),
            placement: "on_node".to_owned(),
            active_node_id: Some(other_id),
            restore_lease_token: None,
        };
        match expect {
            Expect::Adopted => {
                let activation = result.unwrap_or_else(|error| panic!("{label}: {error:?}"));
                assert_eq!(
                    activation.binding.runtime_endpoint.uri().port_u16(),
                    Some(source_server.port()),
                    "{label}: the wake must reach the record's worker"
                );
                assert_eq!(activation.binding.execution_generation, 1, "{label}");
                assert_eq!(
                    placed,
                    Placement {
                        lifecycle: "started".to_owned(),
                        placement: "on_node".to_owned(),
                        active_node_id: Some(source_id),
                        restore_lease_token: None,
                    },
                    "{label}"
                );
                assert_eq!(
                    (route_node, route_generation, last_generation),
                    (source_id, 1, 1),
                    "{label}: an adoption reserves no generation"
                );
                assert_eq!(source_status.as_deref(), Some("running"), "{label}");
                assert_eq!(other_status, None, "{label}: restored elsewhere");
            }
            Expect::RestoredElsewhereKeepingSource => {
                result.unwrap_or_else(|error| panic!("{label}: {error:?}"));
                assert_eq!(placed, restored_elsewhere, "{label}");
                assert_eq!(other_status.as_deref(), Some("running"), "{label}");
                assert_eq!(
                    source_status.as_deref(),
                    Some("running"),
                    "{label}: the live copy must not be deleted"
                );
            }
            Expect::RestoreInFlight => {
                let error = match result {
                    Ok(_) => panic!("{label}: the lease owner's restore must not be joined"),
                    Err(error) => error,
                };
                assert!(
                    matches!(error, SubstrateError::RestoreInFlight { .. }),
                    "{label}: {error:?}"
                );
                assert_eq!(
                    placed,
                    Placement {
                        lifecycle: "restoring".to_owned(),
                        placement: "unplaced".to_owned(),
                        active_node_id: None,
                        restore_lease_token: Some(lease_token),
                    },
                    "{label}"
                );
                assert_eq!(other_status, None, "{label}");
            }
            Expect::Untouched => {
                assert!(result.is_err(), "{label}: the wake must not succeed");
                let Row::MovedTo(lifecycle) = row else {
                    unreachable!("{label}")
                };
                assert_eq!(
                    placed,
                    Placement {
                        lifecycle: lifecycle.to_owned(),
                        placement: "unplaced".to_owned(),
                        active_node_id: None,
                        restore_lease_token: None,
                    },
                    "{label}"
                );
                assert_eq!(source_status.as_deref(), Some("running"), "{label}");
                assert_eq!(other_status, None, "{label}");
            }
            Expect::RestoredElsewhereDeletingSource => {
                result.unwrap_or_else(|error| panic!("{label}: {error:?}"));
                assert_eq!(placed, restored_elsewhere, "{label}");
                assert_eq!(other_status.as_deref(), Some("running"), "{label}");
                assert_eq!(
                    source_status, None,
                    "{label}: the source copy must be cleaned up"
                );
            }
        }
    }
    drop(source_server);
    drop(other_server);
    Ok(())
}

/// The race's other ending: the woken worker's completion wrote `started`
/// after the node's suspend projection cleared `active_node_id`. The next
/// lookup adopts the record when it runs the row's current execution
/// generation; one from an older generation is still unassigned.
#[tokio::test(flavor = "current_thread")]
async fn lookup_of_a_started_row_without_an_active_node_adopts_its_live_record() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    seed_worker(&fixture.pool, node_id, server.endpoint()).await?;

    for (label, record_generation, expect_adopted) in [
        ("record at the row's generation", 1, true),
        ("record from an older generation", 0, false),
    ] {
        let sandbox_id = SandboxId::new();
        seed_idle_suspended_sandbox(&fixture.pool, &state, sandbox_id, node_id, None).await?;
        state
            .set_route_identity_provenance(
                &sandbox_id.to_string(),
                Some(1),
                Some(record_generation),
                None,
            )
            .await?;
        state.set_status(&sandbox_id.to_string(), "running").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'started', placement = 'on_node' WHERE id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&fixture.pool)
        .await?;

        let result = Box::pin(fixture.substrate.runtime_client(
            sandbox_id,
            crate::controllers::substrate::RecoveryCharge::Exempt,
        ))
        .await;

        let row = placement(&fixture.pool, sandbox_id).await?;
        if expect_adopted {
            result.unwrap_or_else(|error| panic!("{label}: {error}"));
            assert_eq!(row.active_node_id, Some(node_id), "{label}");
            assert_eq!(row.lifecycle, "started", "{label}");
        } else {
            let error = match result {
                Ok(_) => panic!("{label}: a stale record must not be adopted"),
                Err(error) => error,
            };
            assert_eq!(
                error.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "{label}"
            );
            assert_eq!(row.active_node_id, None, "{label}");
        }
    }
    drop(server);
    Ok(())
}

/// A placed suspended sandbox still wakes through the worker's Wake RPC, not
/// the restore lease: the row keeps the lifecycle the caller finalizes.
#[tokio::test(flavor = "current_thread")]
async fn wake_of_a_placed_suspended_sandbox_still_asks_its_worker() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    seed_idle_suspended(
        &fixture.pool,
        &state,
        sandbox_id,
        node_id,
        server.endpoint(),
    )
    .await?;
    let policy = crate::e2b::auto_lifecycle_policy(60, true, true);
    place_suspended_sandbox(&fixture.pool, sandbox_id, node_id, &policy).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");

    Box::pin(fixture.substrate.wake_execution(
        pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
        &sandbox,
        Duration::from_secs(5),
        None,
        crate::controllers::substrate::RecoveryCharge::Exempt,
    ))
    .await?;

    assert_eq!(state.get(&sandbox_id.to_string()).await?.status, "running");
    assert_eq!(
        placement(&fixture.pool, sandbox_id).await?,
        Placement {
            lifecycle: "suspended".to_owned(),
            placement: "on_node".to_owned(),
            active_node_id: Some(node_id),
            restore_lease_token: None,
        }
    );
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delete_of_an_idle_suspended_sandbox_tears_down_its_artifact_node() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    seed_idle_suspended(
        &fixture.pool,
        &state,
        sandbox_id,
        node_id,
        server.endpoint(),
    )
    .await?;

    Box::pin(fixture.substrate.delete_execution(sandbox_id)).await?;

    assert!(
        state.get(&sandbox_id.to_string()).await.is_err(),
        "the artifact node must delete its local sandbox"
    );
    drop(server);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delete_without_any_node_placement_stays_unavailable() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, None).await?;

    let error = Box::pin(fixture.substrate.delete_execution(sandbox_id))
        .await
        .expect_err("no node can be reached");

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    Ok(())
}

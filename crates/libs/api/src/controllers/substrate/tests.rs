use super::*;
use pplx_space_common_core::types::SandboxId;
use pplx_space_node_api_sim::SimWorkerHarness;
use pplx_space_sbx_mgr_client::proto as sbx_proto;

#[test]
fn unknown_lifecycle_cannot_become_dead_or_missing_probe_evidence() {
    let id = Uuid::now_v7();
    let missing = Uuid::now_v7();
    for lifecycle in [Some(0), Some(-1), Some(19), None] {
        for firecracker_alive in [false, true] {
            let states = observed_sandbox_states(sbx_proto::StateResponse {
                sandboxes: vec![sbx_proto::SandboxInfo {
                    sandbox_id: id.to_string(),
                    lifecycle,
                    firecracker_alive,
                    ..Default::default()
                }],
                missing_ids: vec![missing.to_string()],
            });
            assert!(!states.contains_key(&SandboxId::from_uuid(id)));
            assert_eq!(
                states.get(&SandboxId::from_uuid(missing)),
                Some(&ObservedSandboxState::Missing)
            );
        }
    }
    {
        let states = observed_sandbox_states(sbx_proto::StateResponse {
            sandboxes: vec![sbx_proto::SandboxInfo {
                sandbox_id: id.to_string(),
                lifecycle: Some(sbx_proto::SandboxLifecycleState::Stopped as i32),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert!(matches!(
            states.get(&SandboxId::from_uuid(id)),
            Some(ObservedSandboxState::DormantNoArtifact { .. })
        ));
    }
}

#[test]
fn local_token_unavailable_is_unreachable_not_wedged() {
    // A local `SpaceBearerInterceptor` token-source failure (unmounted /
    // unreadable / mid-rotation projected token) surfaces as UNAVAILABLE.
    // It must be classified transient so `probe_command_ready` returns
    // `Unreachable`, never `Wedged` — otherwise the transient-lifecycle
    // sweeper would reap a healthy sandbox on a local credential blip.
    assert!(probe_status_is_transient(tonic::Code::Unavailable));
    // Genuine remote/guest INTERNAL stays non-transient -> `Wedged`, which
    // the sweeper does treat as wedge evidence. Keep that boundary intact.
    assert!(!probe_status_is_transient(tonic::Code::Internal));
    assert!(!probe_status_is_transient(tonic::Code::Unauthenticated));
}

fn full_artifact(local_artifact_id: LocalArtifactId) -> sbx_proto::PublishedArtifact {
    sbx_proto::PublishedArtifact {
        local_artifact_id: local_artifact_id.to_string(),
        artifact_kind: sbx_proto::ArtifactKind::Full as i32,
        origin: sbx_proto::ArtifactOrigin::Suspend as i32,
        snapshot_name: None,
    }
}

#[test]
fn suspend_rpc_aborted_maps_to_conflict_others_to_failed() {
    // `Aborted` is the suspend-vs-wake arbitration verdict from sbx-mgr
    // (the suspend queued behind a concurrent wake and was rejected as
    // superseded) — a caller-resolvable 409, never a 500.
    let superseded = suspend_rpc_error(tonic::Status::aborted(
        "suspend superseded by a concurrent wake",
    ));
    assert!(
        matches!(&superseded, SubstrateError::Conflict { message } if message.contains("superseded")),
        "unexpected mapping: {superseded:?}"
    );
    {
        use axum::response::IntoResponse;
        assert_eq!(
            ControllerError::from(superseded).into_response().status(),
            axum::http::StatusCode::CONFLICT
        );
    }

    let failed = suspend_rpc_error(tonic::Status::internal("snapshot create failed"));
    assert!(
        matches!(failed, SubstrateError::Failed { .. }),
        "non-Aborted suspend errors keep the Failed shape"
    );
}

#[test]
fn blocking_budget_sits_below_node_rpc_timeout_which_sits_below_alb_idle() {
    // The timeout hierarchy the whole change rests on:
    //   blocking budget  <  node-RPC channel timeout  <  ALB idle timeout
    // The budget must finish (and a structured response flush) before tonic
    // severs the channel; the channel timeout must stay well below the ALB.
    const { assert!(BLOCKING_WAIT_MARGIN_SECS > 0, "margin must leave headroom") };
    assert!(
        NODE_RPC_BLOCKING_BUDGET < NODE_RPC_TIMEOUT,
        "blocking budget {NODE_RPC_BLOCKING_BUDGET:?} must be below the node-RPC timeout {NODE_RPC_TIMEOUT:?}",
    );
    assert_eq!(
        NODE_RPC_BLOCKING_BUDGET,
        Duration::from_secs(NODE_RPC_TIMEOUT_SECS - BLOCKING_WAIT_MARGIN_SECS),
    );
    const { assert!(NODE_RPC_TIMEOUT_SECS < ALB_IDLE_TIMEOUT_SECS) };
}

#[test]
fn node_api_endpoint_is_scheme_aware() {
    if !pplx_space_test_env::enter_test_process(&[("SPACE_INTERNAL_RPC_INSECURE_DEV", None)]) {
        return;
    }

    let connect = Duration::from_secs(3);
    let request = Duration::from_secs(10);

    // Bare host/port inputs advertise the secure default.
    let bare = node_api_endpoint("worker-1:9800", connect, request).unwrap();
    assert_eq!(bare.uri().scheme_str(), Some("https"));
    assert_eq!(bare.uri().host(), Some("worker-1"));
    assert_eq!(bare.uri().port_u16(), Some(9800));

    // An `http://`-qualified input is not double-prefixed.
    let http = node_api_endpoint("http://worker-1:9800", connect, request).unwrap();
    assert_eq!(http.uri().scheme_str(), Some("http"));
    assert_eq!(http.uri().host(), Some("worker-1"));
    assert_eq!(http.uri().port_u16(), Some(9800));

    // Endpoint construction handles addressing; the shared TLS tests own
    // connection policy and server-identity validation.
    let https = node_api_endpoint("https://worker-1:9800", connect, request).unwrap();
    assert_eq!(https.uri().scheme_str(), Some("https"));
}

async fn start_local_worker_server()
-> Result<(SimWorkerHarness, String), Box<dyn std::error::Error + Send + Sync>> {
    // No bearer-token env option requested: the raw client built by
    // `node_api_endpoint_connects_to_local_worker_node_api` below skips the
    // bearer interceptor entirely, so there is no reason to mutate
    // process-wide env (`SPACE_INTERNAL_TOKEN_ENV`) just to satisfy token
    // lookup.
    let harness = SimWorkerHarness::builder("node-api-endpoint-test-node")
        .start()
        .await?;
    let endpoint = harness.endpoint().to_string();
    Ok((harness, endpoint))
}

#[tokio::test(flavor = "current_thread")]
async fn node_api_endpoint_connects_to_local_worker_node_api()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let (_server, addr) = start_local_worker_server().await?;
    let endpoint = node_api_endpoint(&addr, Duration::from_secs(3), Duration::from_secs(10))?;
    // Build a raw client directly from the endpoint under test. The sim
    // `SimWorkerApiService` does not validate auth, so we skip the bearer
    // interceptor used by `connect_worker` and avoid mutating process-wide
    // env (`SPACE_INTERNAL_TOKEN_ENV`) just to satisfy token lookup.
    let mut client = WorkerApiClient::new(endpoint.connect().await?);

    client
        .spaced_wait_ready(SpacedWaitReadyRequest {
            sandbox_id: "local-node-api-endpoint-test".to_string(),
            timeout: None,
        })
        .await?;

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn kept_worker_channels_serve_every_call_to_a_worker()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let (server, addr) = start_local_worker_server().await?;
    let channels =
        worker_channels::WorkerChannels::new(Duration::from_secs(3), Duration::from_secs(10));
    for sandbox in ["kept-channel-first", "kept-channel-second"] {
        let mut client = channels.client(&addr).await?;
        client
            .spaced_wait_ready(SpacedWaitReadyRequest {
                sandbox_id: sandbox.to_string(),
                timeout: None,
            })
            .await?;
    }
    assert_eq!(
        channels.len(),
        1,
        "the second call reuses the first channel"
    );
    drop(server);
    Ok(())
}

#[tokio::test]
async fn suspended_backup_accepts_current_or_exact_artifact_through_generic_rpc()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let server = SimWorkerHarness::builder("artifact-backup-test-node")
        .start()
        .await?;
    let mut client = retrying_worker_node_api(server.endpoint(), Duration::from_secs(30))?;
    let sandbox_id = SandboxId::new();
    let exact_id = LocalArtifactId::new();
    for (source, expected_id) in [
        (BackupSource::CurrentInstance, None),
        (BackupSource::LocalArtifact(exact_id), Some(exact_id)),
    ] {
        let enqueued = enqueue_artifact_backup_with_node_api(
            &mut client,
            server.endpoint(),
            sandbox_id,
            ArtifactKind::Full,
            source,
        )
        .await?;
        assert_eq!(enqueued.artifact.kind, ArtifactKind::Full);
        if let Some(expected_id) = expected_id {
            assert_eq!(enqueued.artifact.id, expected_id);
        }
    }
    Ok(())
}

#[test]
fn probe_status_transient_codes_defer_others_are_wedge_evidence() {
    // Transient transport/overload codes must DEFER (re-probe next tick),
    // never reap — a node-mgr/proxy blip must not destroy a healthy,
    // abandoned preserved-create sandbox.
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::ResourceExhausted,
        tonic::Code::Aborted,
    ] {
        assert!(
            probe_status_is_transient(code),
            "{code:?} should defer, not reap"
        );
    }
    // The guest was reached but isn't ready — positive wedge evidence, not
    // transient. (`DeadlineExceeded` = the guest didn't become ready within
    // the wait; `FailedPrecondition` = the guest reports not-started.)
    for code in [
        tonic::Code::DeadlineExceeded,
        tonic::Code::FailedPrecondition,
        tonic::Code::Internal,
        tonic::Code::Unknown,
        tonic::Code::NotFound,
    ] {
        assert!(
            !probe_status_is_transient(code),
            "{code:?} should count as wedge evidence, not defer"
        );
    }
}

#[cfg(feature = "kubernetes")]
#[test]
fn create_wait_preserves_node_work_only_on_timeout() {
    assert!(create_wait_should_preserve_node_work(
        &SubstrateError::Timeout {
            message: "slow".to_string(),
            phase: None,
        }
    ));
    for err in [
        SubstrateError::Failed {
            message: "boom".to_string(),
        },
        SubstrateError::Unavailable {
            message: "down".to_string(),
        },
        SubstrateError::Conflict {
            message: "race".to_string(),
        },
        SubstrateError::NotFound {
            message: "gone".to_string(),
        },
        SubstrateError::RestoreContextUnavailable {
            message: "restore state missing".to_string(),
        },
    ] {
        assert!(
            !create_wait_should_preserve_node_work(&err),
            "non-timeout error must not preserve: {err:?}"
        );
    }
}

#[test]
fn substrate_timeout_maps_to_retryable_503() {
    let err = ControllerError::from(SubstrateError::Timeout {
        message: "still converging".to_string(),
        phase: None,
    });
    assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.retry_after(), Some(GATEWAY_RETRY_AFTER));
}

#[test]
fn restore_in_flight_classification_does_not_depend_on_message_wording() {
    let error = SubstrateError::restore_in_flight(
        "another recovery owner still holds the sandbox restore lease",
    );

    let controller_error = ControllerError::from(error);
    assert_eq!(
        controller_error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(controller_error.retry_after(), Some(GATEWAY_RETRY_AFTER));
    assert_eq!(
        controller_error.retry_after_cause(),
        Some(ControllerRetryAfterCause::RestoreInFlight)
    );
}

#[test]
fn restore_source_pending_is_retryable_restore_work() {
    let error = SubstrateError::restore_source_pending(SandboxId::new());

    let controller_error = ControllerError::from(error);
    assert_eq!(
        controller_error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(controller_error.retry_after(), Some(GATEWAY_RETRY_AFTER));
    assert_eq!(
        controller_error.retry_after_cause(),
        Some(ControllerRetryAfterCause::RestoreInFlight)
    );
}

#[test]
fn restore_in_flight_retry_after_derives_from_lease_remaining() {
    let sub_floor = ControllerError::from(SubstrateError::restore_in_flight_with_lease(
        "lease held",
        Duration::from_millis(200),
    ));
    assert_eq!(sub_floor.retry_after(), Some(RESTORE_RETRY_AFTER_FLOOR));

    let within = ControllerError::from(SubstrateError::restore_in_flight_with_lease(
        "lease held",
        Duration::from_secs(3),
    ));
    assert_eq!(within.retry_after(), Some(Duration::from_secs(3)));

    let beyond = ControllerError::from(SubstrateError::restore_in_flight_with_lease(
        "lease held",
        Duration::from_secs(90),
    ));
    assert_eq!(beyond.retry_after(), Some(GATEWAY_RETRY_AFTER));
}

#[test]
fn api_boot_mode_defaults_to_fallthrough_without_custom_shape() {
    assert_eq!(
        CreateBootMode::from_api(None, false).unwrap(),
        CreateBootMode::Fallthrough
    );
}

#[test]
fn api_boot_mode_defaults_to_cold_with_custom_shape() {
    assert_eq!(
        CreateBootMode::from_api(None, true).unwrap(),
        CreateBootMode::Cold
    );
}

#[test]
fn api_boot_mode_rejects_warm_modes_with_custom_shape() {
    let snapshot_error = CreateBootMode::from_api(Some("snapshot"), true).unwrap_err();
    assert!(snapshot_error.to_string().contains("require cold boot"));

    let pool_error = CreateBootMode::from_api(Some("pool"), true).unwrap_err();
    assert!(pool_error.to_string().contains("require cold boot"));
}

#[test]
fn api_boot_mode_accepts_explicit_warm_modes_without_custom_shape() {
    assert_eq!(
        CreateBootMode::from_api(Some("snapshot"), false).unwrap(),
        CreateBootMode::BaseSnapshot
    );
    assert_eq!(
        CreateBootMode::from_api(Some("pool"), false).unwrap(),
        CreateBootMode::Pool
    );
}

#[test]
fn committed_suspend_response_keeps_valid_artifact_identity() {
    let sandbox_id = SandboxId::new();
    let local_artifact_id = LocalArtifactId::new();
    let response = sbx_proto::SuspendResponse {
        published_artifact: Some(full_artifact(local_artifact_id)),
    };

    let artifact =
        PublishedLocalArtifact::from_committed_suspend_response(response, sandbox_id, "test")
            .expect("valid committed suspend artifact should parse");
    assert_eq!(artifact.id, local_artifact_id);
    assert_eq!(artifact.kind, ArtifactKind::Full);
}

#[test]
fn committed_suspend_response_downgrades_unusable_artifact_identity() {
    let response = sbx_proto::SuspendResponse {
        published_artifact: None,
    };

    assert!(
        PublishedLocalArtifact::from_committed_suspend_response(
            response,
            SandboxId::new(),
            "test",
        )
        .is_none(),
        "committed suspend must remain successful when artifact metadata is unusable"
    );
}

/// The producer half of the substrate's contention classification: what
/// `db_context` turns each SQLSTATE into.
///
/// Every `sqlx` statement in the substrate reaches this one function, so its
/// two dispositions are asserted here rather than at each call site. The
/// consumer halves are asserted separately —
/// `a_lost_lock_race_reaches_the_surface_as_a_marked_retryable_503` below for
/// the HTTP disposition, `controllers::sandbox::reconnect_metrics_tests` for
/// the reconnect metric — because a mapper can classify correctly into a
/// variant nothing downstream reads.
#[tokio::test(flavor = "current_thread")]
async fn db_context_separates_a_lost_lock_race_from_a_real_failure()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let error =
            Err::<(), _>(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await)
                .db_context("failed to lock the sandbox row")
                .expect_err("the statement failed");
        assert!(
            matches!(error, SubstrateError::LockContention { .. }),
            "{sqlstate} must classify as contention, got {error:?}"
        );
    }

    // `57014` is a statement timeout, not a lock race: the statement was too
    // slow once and is likely to be too slow again, so it is not folded in.
    // `23505` is a real conflict — retrying repeats it.
    for sqlstate in ["57014", "23505"] {
        let error =
            Err::<(), _>(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await)
                .db_context("failed to lock the sandbox row")
                .expect_err("the statement failed");
        assert!(
            matches!(error, SubstrateError::Failed { .. }),
            "{sqlstate} must stay a failure, got {error:?}"
        );
        // The failure keeps its context so the log names the statement.
        assert!(error.to_string().contains("failed to lock the sandbox row"));
    }
    Ok(())
}

/// A contended substrate statement must reach the client as a retryable 503
/// attributed to *this service's database* — not as a 500, and not as an
/// unmarked 503 the surface translators re-label as a fleet condition.
///
/// The marking is the load-bearing half. Both `Unavailable` and
/// `LockContention` are 503s, so a test that only asserted the status would
/// pass on the pre-`LockContention` mapping, which sent every contended
/// statement to the public surface as `capacity_unavailable`/`Cluster` ("the
/// fleet is out of capacity") and to ECW as `Node`/`BackendStatus`.
#[test]
fn a_lost_lock_race_reaches_the_surface_as_a_marked_retryable_503() {
    let contended: ControllerError = SubstrateError::LockContention {
        message: "failed to lock sandbox for execution materialization: 55P03".to_string(),
    }
    .into();
    assert_eq!(
        contended.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(contended.is_lock_contention());
    // No database detail in the client-facing message: a 503 body reaches the
    // caller verbatim, and the classification site already logged the rest.
    assert!(
        !contended.to_string().contains("55P03"),
        "the SQLSTATE leaked into the response: {contended}"
    );

    // The other direction: a genuine fleet 503 must not be mistaken for a
    // lock race, or the translators would attribute an out-of-capacity
    // rejection to Postgres.
    let fleet: ControllerError = SubstrateError::Unavailable {
        message: "no worker has capacity".to_string(),
    }
    .into();
    assert_eq!(fleet.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(!fleet.is_lock_contention());

    let failed: ControllerError = SubstrateError::Failed {
        message: "failed to parse sandbox execution lifecycle".to_string(),
    }
    .into();
    assert_eq!(
        failed.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(!failed.is_lock_contention());
}

/// The warm-pool store sits inside substrate transactions, so its errors need
/// the same split — and its `Sql` variant is `#[error(transparent)]`, which is
/// what lets the classification cross the store boundary without the message
/// changing.
#[tokio::test(flavor = "current_thread")]
async fn pool_store_contention_crosses_the_store_boundary() -> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };
    let sql = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await;
    let rendered = sql.to_string();
    let error = Err::<(), _>(pplx_space_cluster_pool::PoolError::Sql(sql))
        .db_context("failed to delete claimed pool slot")
        .expect_err("the statement failed");
    assert!(
        matches!(error, SubstrateError::LockContention { .. }),
        "got {error:?}"
    );
    assert!(
        error.to_string().contains(&rendered),
        "the transparent wrapper changed the message: {error}"
    );

    // A slot-state violation is the store's own invariant, not a lost race.
    let invalid = Err::<(), _>(pplx_space_cluster_pool::PoolError::InvalidSlotState(
        "bogus".to_string(),
    ))
    .db_context("failed to delete claimed pool slot")
    .expect_err("the statement failed");
    assert!(
        matches!(invalid, SubstrateError::Failed { .. }),
        "got {invalid:?}"
    );
    Ok(())
}

/// The `SubstrateError` twin of the post-side-effect demotion.
///
/// Applied to a whole result at a path boundary, not per statement — which is
/// the point. `persist_pod_node_placement` runs five statements once the pod
/// exists, and exempting only its `UPDATE` left `begin`, a role upsert, a row
/// lock, and `commit` still answering retryable on a create whose pod is
/// already live.
#[test]
fn demoting_a_substrate_error_after_a_side_effect_clears_the_contention_variant() {
    let demoted = SubstrateError::LockContention {
        message: "failed to persist sandbox Kubernetes placement: 55P03".to_string(),
    }
    .demoted_after_side_effect();
    assert!(
        matches!(demoted, SubstrateError::Failed { .. }),
        "got {demoted:?}"
    );

    // And it survives the conversion as a 500, since that conversion is where
    // the retryable disposition would otherwise be reconstituted.
    let surfaced: ControllerError = demoted.into();
    assert_eq!(
        surfaced.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(!surfaced.is_lock_contention());

    // Every other variant passes through untouched.
    let unavailable = SubstrateError::Unavailable {
        message: "node is gone".to_string(),
    }
    .demoted_after_side_effect();
    assert!(matches!(unavailable, SubstrateError::Unavailable { .. }));
}

/// The node-group catalog read is a `sqlx` boundary that does not go through
/// `db_context`, so its classification is asserted separately.
///
/// The pod-build path mapped its whole error enum — including the variant that
/// wraps a `sqlx::Error` — onto `SubstrateError::Failed`. That made it the one
/// store read on create/restore still reporting a bounded lock wait as
/// permanent. Retrying is safe: the resolve runs while the pod spec is being
/// built and every variant fails the build closed, so nothing exists yet for a
/// retry to duplicate.
///
/// Asserts all three SQLSTATEs and, in the same loop, that a non-contention
/// database error still lands on `Failed` — a guard that matched too broadly
/// would turn every catalog failure into a retryable 503 and hide real ones.
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn a_contended_node_group_catalog_read_is_retryable() -> pplx_space_test_env::TestResult {
    use crate::node_group_placement::{NodeGroupCatalogError, NodeGroupPlacementResolveError};

    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let sql = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await;
        let mapped = SubstrateError::from(NodeGroupPlacementResolveError::Catalog(
            NodeGroupCatalogError::Database(sql),
        ));
        assert!(
            matches!(mapped, SubstrateError::LockContention { .. }),
            "{sqlstate} on the catalog read must classify as contention, got {mapped:?}"
        );
    }

    // A real database failure keeps the 500. `RowNotFound` is not a contention
    // SQLSTATE, so the guard must not claim it.
    let mapped = SubstrateError::from(NodeGroupPlacementResolveError::Catalog(
        NodeGroupCatalogError::Database(sqlx::Error::RowNotFound),
    ));
    assert!(
        matches!(mapped, SubstrateError::Failed { .. }),
        "a non-contention catalog failure must stay a 500, got {mapped:?}"
    );

    // And the sibling variants are untouched: the guard sits ahead of them, so
    // a too-broad match would show up here first.
    let absent = SubstrateError::from(NodeGroupPlacementResolveError::TemplateAbsent {
        template: "t".to_string(),
    });
    assert!(
        matches!(absent, SubstrateError::Failed { .. }),
        "a missing templates row is still a control-plane inconsistency, got {absent:?}"
    );
    Ok(())
}

/// The admin drain routes read `supports_node_drain` to decide whether to
/// commit a phase before patching the node. If it claimed support where the
/// patch is `Unsupported`, a refused patch would follow a committed phase;
/// if it denied support where the patch works, the routes would refuse a
/// real drain.
#[tokio::test(flavor = "current_thread")]
async fn node_drain_capability_matches_set_node_schedulability() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(1))
        .connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .expect("lazy pool should parse");
    #[cfg_attr(not(feature = "kubernetes"), allow(unused_mut))]
    let mut substrates = vec![Substrate::from_direct(direct::DirectSubstrate::new(
        pool.clone(),
    ))];
    #[cfg(feature = "kubernetes")]
    substrates.push(Substrate::from_kubernetes(
        kubernetes::tests::substrate_with_store(pool),
    ));

    for substrate in substrates {
        let result = substrate
            .set_node_schedulability(
                NodeSchedulabilityRequest {
                    substrate_node_id: "worker-a",
                },
                NodeSchedulability::Unschedulable,
            )
            .await;
        assert_eq!(
            matches!(result, Err(SubstrateError::Unsupported { .. })),
            !substrate.capabilities().supports_node_drain,
            "{} substrate: {result:?}",
            substrate.managed_by()
        );
    }
}

/// The fork executor reads `node_addressing` to decide which node-api target
/// field an active copy must carry. If it named a field the substrate's
/// `resolve_node_api_endpoint` does not read, a target the executor accepts
/// would fail to resolve, and one that resolves would be judged incomplete, so
/// the copy waits on a fallback-worker lookup (retrying when no worker is
/// ready) that fills only the missing field.
#[tokio::test(flavor = "current_thread")]
async fn node_addressing_names_the_field_endpoint_resolution_reads() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(1))
        .connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .expect("lazy pool should parse");
    let stored_endpoint = "192.0.2.10:50051";
    let k8s_node_name = "node-a";
    #[cfg_attr(not(feature = "kubernetes"), allow(unused_mut))]
    let mut cases = vec![(
        Substrate::from_direct(direct::DirectSubstrate::new(pool.clone())),
        NodeManagedBy::Space,
        stored_endpoint.to_string(),
    )];
    #[cfg(feature = "kubernetes")]
    {
        let mut node_api_pod =
            kubernetes::tests::node_api_pod_on_node("space-worker-node-a", k8s_node_name);
        node_api_pod
            .status
            .as_mut()
            .expect("fixture pod has a status")
            .pod_ip = Some("192.0.2.20".to_string());
        let substrate = kubernetes::tests::substrate_with_inventory_caches_and_pool(
            vec![node_api_pod],
            Vec::new(),
            pool,
        );
        let pod_endpoint = format!("192.0.2.20:{}", substrate.k8s.node_api_port);
        cases.push((
            Substrate::from_kubernetes(substrate),
            NodeManagedBy::Kubernetes,
            pod_endpoint,
        ));
    }

    for (substrate, managed_by, expected) in cases {
        let target = |direct_endpoint, substrate_node_id| NodeApiTarget {
            managed_by,
            role: NodeRole::Worker,
            direct_endpoint,
            substrate_node_id,
        };
        let endpoint_only = target(Some(stored_endpoint), None);
        let node_id_only = target(None, Some(k8s_node_name));
        let (declared_only, other_only) = match substrate.capabilities().node_addressing {
            NodeAddressing::DirectEndpoint => (endpoint_only, node_id_only),
            NodeAddressing::SubstrateNodeId => (node_id_only, endpoint_only),
        };

        let resolved = substrate.resolve_node_api_endpoint(&declared_only).await;
        assert_eq!(
            resolved.as_ref().ok(),
            Some(&expected),
            "{} substrate, declared field only: {resolved:?}",
            substrate.managed_by()
        );
        let refused = substrate.resolve_node_api_endpoint(&other_only).await;
        assert!(
            matches!(refused, Err(SubstrateError::NotFound { .. })),
            "{} substrate, declared field missing: {refused:?}",
            substrate.managed_by()
        );
    }
}

/// **Three dispositions, and two of them refuse.**
///
/// The wire type is `optional string`, so "no pin", "empty string" and
/// "malformed" all arrive through one field. Only the first is a real "no": the
/// other two are pins we cannot identify, and reporting either as pinless would
/// let a stopped evacuation open on a row whose next publish reuses bytes
/// nobody can attribute — then destroy the rootfs on the strength of them.
///
/// An earlier revision split them, treating `""` as pinless and a malformed id
/// as pinned. The table is asserted here rather than through the RPC because
/// the tests that could reach it before injected the *result*, which left the
/// dispositions themselves unexercised — a mutation swapping either arm passed.
#[test]
fn a_reported_pin_is_pinless_only_when_genuinely_absent() {
    let sandbox_id = SandboxId::from_uuid(Uuid::now_v7());
    let real = Uuid::now_v7();

    assert_eq!(
        parse_reported_pin(Some(&real.to_string()), sandbox_id),
        (Some(LocalArtifactId::from_uuid(real)), false),
        "a readable id is the pin, and is not flagged unreadable"
    );
    assert_eq!(
        parse_reported_pin(None, sandbox_id),
        (None, false),
        "an absent field is the only real `no pin` — the one answer a fresh evacuation \
         may proceed on"
    );
    assert_eq!(
        parse_reported_pin(Some(""), sandbox_id),
        (None, true),
        "an empty string is a pin we cannot identify, not an absent one: treating it as \
         pinless would let a teardown run on evidence nobody could read"
    );
    assert_eq!(
        parse_reported_pin(Some("not-a-uuid"), sandbox_id),
        (None, true),
        "and so is a malformed id — the two are equally unreadable and must not get \
         opposite dispositions"
    );
    assert_eq!(
        parse_reported_pin(Some("  "), sandbox_id),
        (None, true),
        "whitespace is present-but-unreadable too; only a genuinely missing field is a no"
    );
}

/// The node's capture time travels with its pin and never without one: a
/// readable pin carries it, while a time beside no pin or beside an unreadable
/// one is dropped, so a time can't be attributed to an artifact nobody can name.
/// The row's stopped-since time comes through unchanged in every case.
#[test]
fn the_capture_time_is_carried_only_beside_a_readable_pin() {
    use pplx_space_sbx_mgr_client::proto::{SandboxInfo, SandboxLifecycleState};
    let sandbox_id = SandboxId::from_uuid(Uuid::now_v7());
    let pin = Uuid::now_v7();
    let at = |seconds| prost_types::Timestamp { seconds, nanos: 0 };
    let reported = |current_local_artifact_id: Option<String>| {
        node_reported_artifact_from_info(
            SandboxInfo {
                sandbox_id: sandbox_id.to_string(),
                lifecycle: Some(SandboxLifecycleState::Stopped as i32),
                current_local_artifact_id,
                current_local_artifact_created_at: Some(at(200)),
                state_changed_at: Some(at(100)),
                ..SandboxInfo::default()
            },
            sandbox_id,
        )
    };

    let readable = reported(Some(pin.to_string()));
    assert_eq!(
        readable.pinned_artifact,
        Some(LocalArtifactId::from_uuid(pin))
    );
    assert_eq!(readable.pin_captured_at, Some(at(200)));
    assert_eq!(readable.stopped_since, Some(at(100)));

    for (shape, raw) in [
        ("no pin", None),
        ("an unreadable pin", Some("nope".to_owned())),
    ] {
        let answer = reported(raw);
        assert_eq!(answer.pinned_artifact, None, "{shape}");
        assert_eq!(
            answer.pin_captured_at, None,
            "{shape}: the time names no artifact"
        );
        assert_eq!(answer.stopped_since, Some(at(100)), "{shape}");
    }
}

/// **A pin is never shown current on an invalid time.** A stop time before the
/// epoch would read as older than every capture, so a corrupt one could make
/// any pin look current; an invalid capture time could do the same in the
/// other direction. Either makes the order unknown, so the pin is refused like
/// one the node reports with no time. The same pin with valid times is current.
#[test]
fn a_pin_with_an_invalid_capture_or_stop_time_is_never_current() {
    let at = |seconds, nanos| Some(prost_types::Timestamp { seconds, nanos });
    let node = |pin_captured_at, stopped_since| NodeReportedArtifact {
        pinned_artifact: Some(LocalArtifactId::from_uuid(Uuid::now_v7())),
        pin_captured_at,
        stopped_since,
        ..NodeReportedArtifact::default()
    };

    let valid = node(at(1_700_000_100, 0), at(1_700_000_000, 0));
    assert_eq!(valid.pin_capture_order(), Some(std::cmp::Ordering::Greater));
    assert!(valid.pin_shown_current());

    for (shape, captured, stopped) in [
        ("negative stop time", at(1_700_000_100, 0), at(-1, 0)),
        ("negative capture time", at(-1, 0), at(-5, 0)),
        (
            "stop time nanos out of range",
            at(1_700_000_100, 0),
            at(1_700_000_000, -1),
        ),
        (
            "capture time nanos out of range",
            at(1_700_000_100, 1_000_000_000),
            at(1_700_000_000, 0),
        ),
    ] {
        let invalid = node(captured, stopped);
        assert_eq!(invalid.pin_capture_order(), None, "{shape}");
        assert!(!invalid.pin_shown_current(), "{shape}");
    }
}

/// A pin the node reports without a capture time, which is how it reports an
/// artifact an older binary recorded, isn't shown current however recent its
/// row's stop. The same pin reported with a capture time no earlier than the
/// stop is.
#[test]
fn a_pin_reported_without_a_capture_time_is_never_current() {
    use pplx_space_sbx_mgr_client::proto::{SandboxInfo, SandboxLifecycleState};
    let sandbox_id = SandboxId::from_uuid(Uuid::now_v7());
    let at = |seconds| prost_types::Timestamp { seconds, nanos: 0 };
    let reported = |current_local_artifact_created_at| {
        node_reported_artifact_from_info(
            SandboxInfo {
                sandbox_id: sandbox_id.to_string(),
                lifecycle: Some(SandboxLifecycleState::Stopped as i32),
                current_local_artifact_id: Some(Uuid::now_v7().to_string()),
                current_local_artifact_created_at,
                state_changed_at: Some(at(100)),
                ..SandboxInfo::default()
            },
            sandbox_id,
        )
    };
    let older = reported(None);
    assert!(older.pinned_artifact.is_some());
    assert!(!older.pin_shown_current());
    assert!(reported(Some(at(100))).pin_shown_current());
    assert!(reported(Some(at(160))).pin_shown_current());
}

/// The order is a fact about the node's two times, compared exactly, and it's
/// unknown whenever either is missing: an older node that reports no capture
/// time, or a row whose stopped-since time didn't parse, has no order rather
/// than a default one.
#[test]
fn the_capture_order_is_known_only_when_the_node_reports_both_times() {
    let at = |seconds| Some(prost_types::Timestamp { seconds, nanos: 0 });
    let order = |pin_captured_at, stopped_since| {
        NodeReportedArtifact {
            pin_captured_at,
            stopped_since,
            ..NodeReportedArtifact::default()
        }
        .pin_capture_order()
    };
    assert_eq!(order(at(101), at(100)), Some(std::cmp::Ordering::Greater));
    assert_eq!(order(at(100), at(100)), Some(std::cmp::Ordering::Equal));
    assert_eq!(order(at(99), at(100)), Some(std::cmp::Ordering::Less));
    assert_eq!(order(None, at(100)), None);
    assert_eq!(order(at(101), None), None);
}

/// A capture in the stop's own second or later shows the pin current; an
/// earlier capture or an unknown order doesn't.
#[test]
fn a_pin_is_current_from_the_stops_own_second() {
    let at = |seconds| Some(prost_types::Timestamp { seconds, nanos: 0 });
    for (shape, captured, stopped, current) in [
        ("later", at(101), at(100), true),
        ("same second", at(100), at(100), true),
        ("earlier", at(99), at(100), false),
        ("no capture time", None, at(100), false),
        ("no stop time", at(101), None, false),
    ] {
        let node = NodeReportedArtifact {
            pin_captured_at: captured,
            stopped_since: stopped,
            ..NodeReportedArtifact::default()
        };
        assert_eq!(node.pin_shown_current(), current, "{shape}");
    }
}

/// The property: the node's answer never reads as "stopped, pinless, no live
/// VM" unless the node actually said all three. A process the node's live probe
/// sees running marks the VM alive whatever the watchdog's cached flag says,
/// and a degraded stub — the node's raw lifecycle with every other field at
/// its default — is flagged rather than passed off as a full answer.
#[test]
fn the_node_answer_is_never_a_false_all_clear() {
    use pplx_space_sbx_mgr_client::proto::{SandboxInfo, SandboxLifecycleState};
    let sandbox_id = SandboxId::from_uuid(Uuid::now_v7());
    let stopped = |info: SandboxInfo| SandboxInfo {
        sandbox_id: sandbox_id.to_string(),
        lifecycle: Some(SandboxLifecycleState::Stopped as i32),
        ..info
    };

    let quiet = node_reported_artifact_from_info(stopped(SandboxInfo::default()), sandbox_id);
    assert!(quiet.present && quiet.reports_stopped);
    assert!(!quiet.vm_alive, "no signal of a live process");
    assert!(!quiet.state_degraded);

    let watchdog = node_reported_artifact_from_info(
        stopped(SandboxInfo {
            firecracker_alive: true,
            ..SandboxInfo::default()
        }),
        sandbox_id,
    );
    assert!(watchdog.vm_alive, "the watchdog's flag alone is enough");

    let probed = node_reported_artifact_from_info(
        stopped(SandboxInfo {
            namespace: Some("sns-live".to_owned()),
            ..SandboxInfo::default()
        }),
        sandbox_id,
    );
    assert!(
        probed.vm_alive,
        "the node sets `namespace` only from a successful live probe, so it outranks a \
         stale-false watchdog flag"
    );

    let degraded = node_reported_artifact_from_info(
        stopped(SandboxInfo {
            state_degraded: true,
            ..SandboxInfo::default()
        }),
        sandbox_id,
    );
    assert!(
        degraded.state_degraded,
        "a stub whose pin, liveness and lifecycle are defaults must be marked, or it reads as \
         the one shape an evacuation may open on"
    );
}

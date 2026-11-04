use pplx_space_common_core::types::NodeId;

use super::*;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};

use pplx_space_test_env::TestResult;

async fn set_drain(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    phase: Option<&str>,
    mode: Option<&str>,
) -> TestResult {
    sqlx::query("UPDATE nodes SET drain_phase = $2, drain_mode = $3 WHERE id = $1")
        .bind(node_id)
        .bind(phase)
        .bind(mode)
        .execute(pool)
        .await?;
    Ok(())
}

/// A gentle drain must not refuse to open a sandbox service port.
///
/// This guard used to read `drain_phase` alone and reject on any non-NULL
/// value. Under a forced drain that is right: the evacuation pipeline is moving
/// the sandbox, so opening a route to where it currently sits races the move.
/// Under a gentle drain the sandbox is deliberately left where it is and keeps
/// serving traffic — and opening a service port *is* serving traffic. Rejecting
/// it made ingress the one access path a gentle drain still broke, while every
/// user-API path stayed open.
///
/// The truth table is `NodeDrainState::blocks_placement`, shared with the
/// user-API drain gates so the two cannot drift.
#[tokio::test(flavor = "current_thread")]
async fn gentle_draining_node_does_not_block_a_service_port_open() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    // Gentle + draining: the wake gate is open, so ingress must be too.
    set_drain(&db.pool, node_id, Some("draining"), Some("gentle")).await?;
    ensure_sandbox_service_route_node_not_draining(&db.pool, sandbox_id, node_id)
        .await
        .expect("a gentle drain must not block opening a service port");

    // An unset mode reads as gentle, so it must behave the same way. This is
    // the case every row predating the column is in.
    set_drain(&db.pool, node_id, Some("draining"), None).await?;
    ensure_sandbox_service_route_node_not_draining(&db.pool, sandbox_id, node_id)
        .await
        .expect("an unset drain mode reads as gentle, so it must not block either");

    Ok(())
}

/// The cases that must still be refused, so the fix above cannot be mistaken
/// for removing the guard.
///
/// `forced` + `draining` races the evacuation pipeline. `drained` and
/// `terminating` block in *either* mode, because the node is about to be
/// destroyed and a route to it would point at nothing.
#[tokio::test(flavor = "current_thread")]
async fn forced_and_post_draining_phases_still_block_a_service_port_open() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    for (phase, mode) in [
        ("draining", Some("forced")),
        ("drained", Some("forced")),
        ("drained", Some("gentle")),
        ("drained", None),
        ("terminating", Some("forced")),
        ("terminating", Some("gentle")),
        ("terminating", None),
    ] {
        set_drain(&db.pool, node_id, Some(phase), mode).await?;
        let err = ensure_sandbox_service_route_node_not_draining(&db.pool, sandbox_id, node_id)
            .await
            .expect_err(&format!("phase {phase} mode {mode:?} must block"));
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    Ok(())
}

/// An unrecognized phase is persisted state corruption, not "not draining".
/// Service-port opening must fail closed instead of routing traffic to a node
/// whose drain state this server cannot interpret.
#[tokio::test(flavor = "current_thread")]
async fn unrecognized_drain_phase_fails_closed_for_service_port_open() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    // Normal writes cannot create this state because the schema constraint
    // rejects unknown phases. Drop it in this isolated database to model
    // corruption or a newer writer whose vocabulary this server cannot parse.
    sqlx::query("ALTER TABLE nodes DROP CONSTRAINT nodes_drain_phase_check")
        .execute(&db.pool)
        .await?;
    set_drain(
        &db.pool,
        node_id,
        Some("unrecognized-phase"),
        Some("gentle"),
    )
    .await?;
    let err = ensure_sandbox_service_route_node_not_draining(&db.pool, sandbox_id, node_id)
        .await
        .expect_err("an unrecognized drain phase must fail closed");

    assert_eq!(err.code(), Code::Internal);
    Ok(())
}

/// The old worker prepares loopback only through this explicit RPC, not ingress
/// attachment. No completion/capability advertisement is provided by the fixture.
#[tokio::test(flavor = "current_thread")]
async fn legacy_worker_receives_bridge_preparation_with_protection_enabled() -> TestResult {
    use pplx_space_node_api_client::proto::worker_api_server::WorkerApiServer;
    use pplx_space_node_api_sim::{SimState, SimWorkerApiService};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("ingress-test-token")),
        ),
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
    ]) {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let dir = tempfile::tempdir()?;
    let state = SimState::new(
        dir.path().join("snapshots"),
        dir.path().join("sandboxes"),
        node_id.to_string(),
    );
    let requests = Arc::new(AtomicUsize::new(0));
    let captured = requests.clone();
    let legacy_worker = WorkerApiServer::with_interceptor(
        SimWorkerApiService::new(state),
        move |request: Request<()>| {
            captured.fetch_add(1, Ordering::SeqCst);
            Ok(request)
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?.to_string();
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(legacy_worker)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let _task = tokio_util::task::AbortOnDropHandle::new(task);
    let local_service = TcpListener::bind("127.0.0.1:0").await?;
    let port = GuestPort::try_from_u16(local_service.local_addr()?.port())?;
    let authority = crate::tests::test_route_ed25519_authority();
    ensure_sandbox_service_port_ingress_path(
        &db.pool,
        SandboxServicePortRouteIdentity {
            sandbox_id: SandboxId::new(),
            active_node_id: node_id,
            route_generation: RouteGeneration::new(1)?,
            execution_generation: Some(ExecutionGeneration::new(1)?),
            pod_uid: None,
            substrate_kind: Some(NodeManagedBy::Space),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        },
        port,
        &endpoint,
        Some(&authority),
    )
    .await?;
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2,
        "a fresh service needs AllowPort and SpacedOpenLoopbackBridge before old-worker ingress"
    );
    Ok(())
}

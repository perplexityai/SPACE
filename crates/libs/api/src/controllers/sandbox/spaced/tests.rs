use pplx_space_common_core::types::{NodeId, SandboxId};

use pplx_space_common_core::sql;
use std::{
    convert::Infallible,
    future::{Ready, ready},
    net::SocketAddr,
    task::{Context as TaskContext, Poll},
};

use super::*;
use axum::body::Bytes;
use http_body_util::Full;
use pplx_space_common_core::types::NodeDrainMode;
use pplx_space_db_test_fixtures::pg_test_db;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use pplx_space_test_env::TestResult;

struct FakeReconnectNodeApi {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for FakeReconnectNodeApi {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct FakeReconnectNodeApiService;

impl tower::Service<axum::http::Request<tonic::body::Body>> for FakeReconnectNodeApiService {
    type Response = axum::http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: axum::http::Request<tonic::body::Body>) -> Self::Future {
        let mut response =
            axum::http::Response::new(tonic::body::Body::new(Full::new(Bytes::new())));
        response
            .headers_mut()
            .insert("grpc-status", "12".parse().unwrap());
        ready(Ok(response))
    }
}

impl tonic::server::NamedService for FakeReconnectNodeApiService {
    const NAME: &'static str = "space.node.api.v1.WorkerApi";
}

async fn start_fake_reconnect_node_api() -> TestResult<FakeReconnectNodeApi> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(FakeReconnectNodeApiService)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    Ok(FakeReconnectNodeApi { addr, task })
}

async fn insert_direct_sandbox_on_node(
    pool: &PgPool,
    node_api_addr: SocketAddr,
    drain_phase: Option<&str>,
) -> TestResult<Sandbox> {
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();

    // A drain phase implies `drain_mode = 'forced'` here: the access-path gate
    // this fixture exists to exercise only fires for a forced drain, because a
    // gentle one has no evacuation pipeline for the caller to race and is
    // supposed to keep serving traffic. A NULL mode resolves to gentle
    // (`NodeDrainMode::from_column_str`), which would leave the gate open.
    let drain_mode = drain_phase.map(|_| NodeDrainMode::Forced);
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode)
         VALUES ($1, '127.0.0.1'::INET, 'worker', 'space', 'registered', $2, $3)"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(drain_mode)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint)
         VALUES ($1, 'reconnect test node', $2)"
    ))
    .bind(node_id)
    .bind(node_api_addr.to_string())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id,
             user_id,
             org_id,
             managed_by,
             lifecycle,
             placement,
             active_node_id,
             template_id,
             content_hash,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space',
             'started',
             'on_node',
             $2,
             'seed',
             'fake-content-hash',
             1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;

    fetch_sandbox(pool, sandbox_id)
        .await?
        .ok_or_else(|| "inserted sandbox was not found".into())
}

#[tokio::test]
async fn process_stream_open_timeout_error_is_request_timeout_without_retry_marker() {
    let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
        .await
        .expect_err("pending future times out");
    let err = process_stream_open_timeout_error(
        "Process.Start",
        SandboxId::from_uuid(Uuid::nil()),
        Duration::from_secs(8),
        elapsed,
    );

    assert_eq!(err.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(err.retry_after(), None);
}

#[tokio::test]
async fn runtime_operation_timeout_error_is_request_timeout_without_retry_marker() {
    let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
        .await
        .expect_err("pending future times out");
    let err = runtime_operation_timeout_error(
        "spaced_stat",
        SandboxId::from_uuid(Uuid::nil()),
        Duration::from_secs(10),
        elapsed,
    );

    assert_eq!(err.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(err.retry_after(), None);
    assert!(err.to_string().contains("spaced_stat"));
}

#[test]
fn stream_open_retry_accepts_node_api_drain_status() {
    let status = tonic::Status::unavailable(pplx_space_node_api_client::NODE_API_DRAINING_MESSAGE);

    let retry = should_retry_node_api_stream_open(&status);

    assert!(retry);
}

#[test]
fn stream_open_retry_accepts_shutdown_transport_status() {
    let status = tonic::Status::unknown("transport error");

    let retry = should_retry_node_api_stream_open(&status);

    assert!(retry);
}

#[test]
fn stream_open_retry_accepts_unavailable_transport_close_status() {
    let status = tonic::Status::unavailable("connection reset");

    let retry = should_retry_node_api_stream_open(&status);

    assert!(retry);
}

#[test]
fn stream_open_retry_accepts_internal_transport_close_status() {
    let status = tonic::Status::internal("transport is closing");

    let retry = should_retry_node_api_stream_open(&status);

    assert!(retry);
}

#[test]
fn stream_open_retry_rejects_plain_unavailable_status() {
    let status = tonic::Status::unavailable("backend overloaded");

    let retry = should_retry_node_api_stream_open(&status);

    assert!(!retry);
}

#[test]
fn stream_open_retry_rejects_plain_unknown_status() {
    let status = tonic::Status::unknown("application returned an unknown error");

    let retry = should_retry_node_api_stream_open(&status);

    assert!(!retry);
}

#[test]
fn stream_open_retry_rejects_application_status() {
    let status = tonic::Status::failed_precondition("sandbox not command-ready");

    let retry = should_retry_node_api_stream_open(&status);

    assert!(!retry);
}

#[tokio::test]
async fn reconnect_node_api_is_blocked_by_access_drain_gate() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let node_api = start_fake_reconnect_node_api().await?;
    let sandbox = insert_direct_sandbox_on_node(&db.pool, node_api.addr, Some("draining")).await?;
    let substrate = Substrate::Direct(Box::new(
        crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
    ));
    let node_api_addr = node_api.addr.to_string();
    let endpoint = crate::controllers::substrate::node_api_endpoint(
        &node_api_addr,
        std::time::Duration::from_secs(3),
        crate::controllers::substrate::NODE_RPC_TIMEOUT,
    )?;
    let client = crate::controllers::substrate::connect_worker(endpoint).await?;
    let mut spaced_client = SpacedSandboxClient {
        client,
        substrate,
        pool: db.pool.clone(),
        sandbox,
        space_api_route_token: None,
        route_ed25519_authority: None,
        metric_identity: SandboxApiMetricIdentity::ecw("test"),
        activity: Some(ExecutionActivityKind::Access),
        cluster_admission: None,
    };

    let err = spaced_client
        .reconnect_node_api()
        .await
        .expect_err("reconnect must preserve the access-path drain gate");

    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert!(
        err.to_string().contains("draining"),
        "expected drain conflict, got {err}"
    );
    Ok(())
}

#[test]
fn a_kept_connection_is_retired_after_a_timeout_or_transport_close_only() {
    // A timeout is how a peer lost without closing the socket looks, so the
    // next caller dials and finds out; application errors keep the channel.
    for status in [
        tonic::Status::deadline_exceeded("deadline exceeded"),
        tonic::Status::cancelled("Timeout expired"),
        tonic::Status::unavailable(pplx_space_node_api_client::NODE_API_DRAINING_MESSAGE),
        tonic::Status::unavailable("connection reset"),
    ] {
        assert!(should_retire_kept_connection(&status), "{status:?}");
    }
    for status in [
        tonic::Status::not_found("no such file"),
        tonic::Status::unavailable("backend overloaded"),
        tonic::Status::permission_denied("denied"),
    ] {
        assert!(!should_retire_kept_connection(&status), "{status:?}");
    }
}

use std::collections::BTreeMap;

use axum::http::StatusCode;
use pplx_space_accounts::NodeId;
use pplx_space_sbx_mgr_client::proto::{
    DrainBlocked, DrainEvacuated, DrainPending, PreviewDrainNodeResponse,
};
use pplx_space_sql_locator::sql;

use super::*;
use crate::routes::admin::nodes::{V2DrainPreview, V2DrainProbe, V2DrainReport};
use crate::routes::admin::test_fixtures::{TestResult, admin_router_harness_with};

fn report() -> PreviewDrainNodeResponse {
    PreviewDrainNodeResponse {
        sealed_epoch: Some(7),
        evacuated: vec![DrainEvacuated {
            sandbox_id: "sbx-stopped".into(),
            prior: DrainPrior::Stopped as i32,
            artifact_id: "artifact-1".into(),
        }],
        pending: vec![DrainPending {
            sandbox_id: "sbx-running".into(),
            action: "suspend".into(),
            reason: String::new(),
        }],
        blocked: vec![DrainBlocked {
            sandbox_id: Some("sbx-stuck".into()),
            reason: "quarantined".into(),
        }],
        would_finish: false,
        unchecked: vec!["orphan_inventory".into()],
    }
}

/// The preview keeps every item the node reported, under the keys of the
/// stored v2 report, and the scans it skipped.
#[test]
fn the_preview_body_keeps_every_item_the_node_reported() {
    let node_id = NodeId::new();
    assert_eq!(
        preview_body(node_id, &report()),
        serde_json::json!({
            "node_id": node_id,
            "sealed_epoch": 7,
            "would_finish": false,
            "evacuated": [
                {"sandbox_id": "sbx-stopped", "prior": "stopped", "artifact_id": "artifact-1"},
            ],
            "pending": [{"sandbox_id": "sbx-running", "action": "suspend"}],
            "blocked": [{"sandbox_id": "sbx-stuck", "reason": "quarantined"}],
            "unchecked": ["orphan_inventory"],
        })
    );
}

/// What each answer of the node means to a caller: an older node-api or
/// sandbox-manager, a node that didn't answer, or another failure, each with
/// its own status and `reason`. A refused argument is the node's failure: a
/// preview carries none an older node could refuse.
#[test]
fn each_failure_of_the_node_answers_its_own_status_and_reason() {
    use tonic::Code;
    for (code, status, reason) in [
        (
            Code::Unimplemented,
            StatusCode::NOT_IMPLEMENTED,
            "node_version_unsupported",
        ),
        (
            Code::InvalidArgument,
            StatusCode::BAD_GATEWAY,
            "node_preview_failed",
        ),
        (
            Code::Unavailable,
            StatusCode::BAD_GATEWAY,
            "node_unreachable",
        ),
        (
            Code::DeadlineExceeded,
            StatusCode::BAD_GATEWAY,
            "node_unreachable",
        ),
        (
            Code::PermissionDenied,
            StatusCode::BAD_GATEWAY,
            "node_preview_failed",
        ),
        (
            Code::Internal,
            StatusCode::BAD_GATEWAY,
            "node_preview_failed",
        ),
    ] {
        let (answered, body) =
            DrainPreviewError::from_status(tonic::Status::new(code, "from the node")).response();
        assert_eq!(
            (answered, body.0["reason"].as_str()),
            (status, Some(reason)),
            "{code:?}"
        );
        assert!(
            body.0["error"]
                .as_str()
                .is_some_and(|error| error.contains("from the node")),
            "{code:?}: {}",
            body.0
        );
    }
}

/// Answers a preview for each node, named by its Kubernetes name or the
/// endpoint a Direct node registered, with its script.
struct ScriptedPreviews(BTreeMap<String, Result<PreviewDrainNodeResponse, DrainPreviewError>>);

impl V2DrainProbe for ScriptedPreviews {
    fn drain_node<'a>(
        &'a self,
        _state: &'a AppState,
        _node_id: NodeId,
        _epoch: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<V2DrainReport, String>> + Send + 'a>,
    > {
        Box::pin(std::future::ready(Err(
            "a preview never makes a drain call".to_owned(),
        )))
    }

    fn preview<'a>(
        &'a self,
        _state: &'a AppState,
        target: &'a crate::controllers::substrate::OwnedNodeApiTarget,
    ) -> V2DrainPreview<'a> {
        let script = target
            .substrate_node_id
            .as_ref()
            .or(target.direct_endpoint.as_ref())
            .and_then(|name| self.0.get(name))
            .cloned()
            .unwrap_or_else(|| Err(DrainPreviewError::Unreachable("no script".into())));
        Box::pin(std::future::ready(script))
    }
}

/// A node row; a Kubernetes one with its EC2 instance recorded, unless
/// `instance` is false.
async fn insert_node(
    pool: &sqlx::PgPool,
    role: &str,
    managed_by: &str,
    // The Kubernetes node name, or a Direct node's node-api endpoint.
    name: &str,
) -> TestResult<NodeId> {
    insert_node_with(pool, role, managed_by, name, true).await
}

async fn insert_node_with(
    pool: &sqlx::PgPool,
    role: &str,
    managed_by: &str,
    name: &str,
    instance: bool,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.47'::INET, $2::TEXT, $3::TEXT, 'registered')"
    ))
    .bind(node_id)
    .bind(role)
    .bind(managed_by)
    .execute(pool)
    .await?;
    if managed_by == "k8s" {
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, instance_id) VALUES ($1, $2, $3)"
        ))
        .bind(node_id)
        .bind(name)
        .bind(instance.then(|| format!("i-{node_id}")))
        .execute(pool)
        .await?;
    } else {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(name)
        .execute(pool)
        .await?;
    }
    Ok(node_id)
}

/// Answers every preview with `response` after `delay`.
struct SlowPreview {
    delay: std::time::Duration,
    response: PreviewDrainNodeResponse,
}

impl V2DrainProbe for SlowPreview {
    fn drain_node<'a>(
        &'a self,
        _state: &'a AppState,
        _node_id: NodeId,
        _epoch: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<V2DrainReport, String>> + Send + 'a>,
    > {
        Box::pin(std::future::ready(Err(
            "a preview never makes a drain call".to_owned(),
        )))
    }

    fn preview<'a>(
        &'a self,
        _state: &'a AppState,
        _target: &'a crate::controllers::substrate::OwnedNodeApiTarget,
    ) -> V2DrainPreview<'a> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            Ok(self.response.clone())
        })
    }
}

/// A transaction holding every other read of `nodes` until it ends.
async fn lock_nodes(pool: &sqlx::PgPool) -> TestResult<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut lock = pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *lock)
        .await?;
    Ok(lock)
}

/// The status and body of a preview's answer.
async fn answer_of(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, serde_json::from_slice(&bytes).expect("JSON body"))
}

/// A preview whose read of the node's rows doesn't finish within its
/// deadline answers a database timeout, naming the read, not a node it
/// couldn't reach, and never asks the node.
#[tokio::test(flavor = "current_thread")]
async fn a_database_read_past_the_deadline_is_not_an_unreachable_node() -> TestResult {
    let Some(harness) = admin_router_harness_with(|state| {
        state.v2_drain_probe = std::sync::Arc::new(ScriptedPreviews(BTreeMap::new()));
    })
    .await?
    else {
        return Ok(());
    };
    let pool = &harness.db.pool;
    let node = insert_node(pool, "worker", "k8s", "held").await?;
    let lock = lock_nodes(pool).await?;
    let answered = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        preview_within(&harness.state, node, std::time::Duration::from_millis(300)),
    )
    .await;
    lock.rollback().await?;
    let (status, body) = answer_of(answered.expect("the deadline bounds the database read")).await;
    assert_eq!(
        (status, body["reason"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("database_timeout")),
        "{body}"
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("reading the node from the database")),
        "{body}"
    );
    Ok(())
}

/// Each way a preview's read of the node's rows can fail, by its answer. A
/// read the database's own bounds ended, a connection the pool didn't hand
/// out in time, a statement canceled (`57014`) or a lock wait past
/// `lock_timeout` (`55P03`), is a 503 `database_timeout` naming the read and
/// why. Anything else is answered as a read: a lost serialization or
/// deadlock race is the read-only 503, never the write-race 409, and the
/// rest a 500.
#[tokio::test(flavor = "current_thread")]
async fn each_failed_read_answers_as_a_read_and_a_timed_out_one_names_the_read() -> TestResult {
    use crate::routes::admin::test_fixtures::postgres_fixture;
    use pplx_space_db_test_fixtures::raise_sqlstate;
    let Some(db) = postgres_fixture().await? else {
        return Ok(());
    };
    let timed_out = Some("database_timeout");
    let cases = [
        (
            "57014",
            raise_sqlstate(&db.pool, "57014").await,
            StatusCode::SERVICE_UNAVAILABLE,
            timed_out,
            "canceled",
        ),
        (
            "55P03",
            raise_sqlstate(&db.pool, "55P03").await,
            StatusCode::SERVICE_UNAVAILABLE,
            timed_out,
            "lock timeout",
        ),
        (
            "pool",
            sqlx::Error::PoolTimedOut,
            StatusCode::SERVICE_UNAVAILABLE,
            timed_out,
            "acquire timeout",
        ),
        (
            "40P01",
            raise_sqlstate(&db.pool, "40P01").await,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            "concurrent change",
        ),
        (
            "40001",
            raise_sqlstate(&db.pool, "40001").await,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            "concurrent change",
        ),
        (
            "23505",
            raise_sqlstate(&db.pool, "23505").await,
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
            "Internal server error",
        ),
        (
            "io",
            sqlx::Error::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
            "Internal server error",
        ),
    ];
    let far = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    for (case, error, status, reason, says) in cases {
        let answer = read_node_rows::<()>(
            NodeId::new(),
            "the node's drain columns",
            far,
            |step: &str| step.to_owned(),
            std::future::ready(Err(error)),
        )
        .await
        .expect_err("the read failed");
        let (answered, body) = answer_of(*answer).await;
        assert_eq!(
            (answered, body["reason"].as_str()),
            (status, reason),
            "{case}: {body}"
        );
        let message = body["error"].as_str().unwrap_or_default();
        assert!(message.contains(says), "{case}: {body}");
        assert_eq!(
            message.contains("read of the node's drain columns"),
            reason.is_some(),
            "{case}: {body}"
        );
    }
    // Each `raise_sqlstate` fails a statement on the connection it borrows;
    // close the pool so nextest doesn't report them as leaked.
    db.pool.close().await;
    Ok(())
}

/// A pool to the harness's database whose connections carry `options`, and
/// at most `max` of them, waiting `acquire` for one.
async fn bounded_pool(
    harness: &crate::routes::admin::test_fixtures::AdminRouterHarness,
    options: &[(&str, &str)],
    max: u32,
    acquire: std::time::Duration,
) -> TestResult<sqlx::PgPool> {
    let connect = harness
        .db
        .database_url
        .parse::<sqlx::postgres::PgConnectOptions>()?
        .options(options.iter().copied());
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(acquire)
        .connect_with(connect)
        .await?)
}

/// The database's own bounds end a preview's read of the node long before
/// the preview's deadline: the pool's acquire timeout with every connection
/// taken, `statement_timeout`, and `lock_timeout`, the last two on a read
/// held behind a lock on `nodes`. Each answers 503 `database_timeout` naming
/// the read, and the node is never asked.
#[tokio::test(flavor = "current_thread")]
async fn a_read_the_database_bounds_end_answers_a_database_timeout() -> TestResult {
    let Some(harness) = admin_router_harness_with(|state| {
        state.v2_drain_probe = std::sync::Arc::new(ScriptedPreviews(BTreeMap::from([(
            "bounded".to_owned(),
            Ok(report()),
        )])));
    })
    .await?
    else {
        return Ok(());
    };
    let node = insert_node(&harness.db.pool, "worker", "k8s", "bounded").await?;
    let short = std::time::Duration::from_millis(200);
    for (case, options, why) in [
        ("pool", &[][..], "acquire timeout"),
        (
            "statement_timeout",
            &[("statement_timeout", "200")][..],
            "canceled",
        ),
        (
            "lock_timeout",
            &[("lock_timeout", "200")][..],
            "lock timeout",
        ),
    ] {
        let pool = bounded_pool(&harness, options, 1, short).await?;
        let mut state = harness.state.clone();
        state.pool = pool.clone();
        // The pool's one connection taken, or `nodes` locked for the read to
        // wait behind.
        let held = if case == "pool" {
            Some(pool.acquire().await?)
        } else {
            None
        };
        let lock = if case == "pool" {
            None
        } else {
            Some(lock_nodes(&harness.db.pool).await?)
        };
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            preview_within(&state, node, std::time::Duration::from_secs(30)),
        )
        .await;
        drop(held);
        if let Some(lock) = lock {
            lock.rollback().await?;
        }
        pool.close().await;
        let (status, body) = answer_of(answered.expect("the database's bound ends the read")).await;
        assert_eq!(
            (status, body["reason"].as_str()),
            (StatusCode::SERVICE_UNAVAILABLE, Some("database_timeout")),
            "{case}: {body}"
        );
        let message = body["error"].as_str().unwrap_or_default();
        assert!(
            message.contains("the preview's read of the node from the database timed out")
                && message.contains(why),
            "{case}: {body}"
        );
    }
    Ok(())
}

/// One deadline bounds the reads and the node call together: a read held
/// for most of it leaves the node too little time, and the preview runs out
/// in the node call, naming it, with the node's refusals. Given the whole
/// deadline after its reads, the node would have answered.
#[tokio::test(flavor = "current_thread")]
async fn the_deadline_bounds_the_reads_and_the_node_call_together() -> TestResult {
    let Some(harness) = admin_router_harness_with(|state| {
        state.v2_drain_probe = std::sync::Arc::new(SlowPreview {
            delay: std::time::Duration::from_millis(800),
            response: PreviewDrainNodeResponse::default(),
        });
    })
    .await?
    else {
        return Ok(());
    };
    let pool = &harness.db.pool;
    let node = insert_node_with(pool, "worker", "k8s", "slow", false).await?;
    let lock = lock_nodes(pool).await?;
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        lock.rollback().await
    });
    let (status, body) =
        answer_of(preview_within(&harness.state, node, std::time::Duration::from_secs(1)).await)
            .await;
    release.await.expect("release task")?;
    assert_eq!(
        (status, body["reason"].as_str()),
        (StatusCode::BAD_GATEWAY, Some("node_unreachable")),
        "{body}"
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("the node didn't answer the call")),
        "{body}"
    );
    assert_eq!(
        body["refusals"],
        serde_json::json!([
            "drain engine v2 needs the node's EC2 instance recorded: the drain terminates that \
             instance, and asks EC2 about it when the node stops answering"
        ])
    );
    Ok(())
}

/// A tonic `grpc.health.v1.Health` peer that fails every call one way.
#[derive(Clone, Copy)]
enum FailingPeer {
    /// Never answers, so the client's request timeout ends the call.
    Hold,
    /// Panics, with no catch-panic layer in front of it.
    Panic,
}

impl tonic::server::NamedService for FailingPeer {
    const NAME: &'static str = "grpc.health.v1.Health";
}

impl tower::Service<axum::http::Request<tonic::body::Body>> for FailingPeer {
    type Response = axum::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: axum::http::Request<tonic::body::Body>) -> Self::Future {
        let peer = *self;
        Box::pin(async move {
            match peer {
                Self::Hold => std::future::pending().await,
                Self::Panic => panic!("the handler panicked"),
            }
        })
    }
}

/// The status a call to `peer` fails with, through a real tonic channel
/// with a short request timeout.
async fn status_from(peer: FailingPeer) -> tonic::Status {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(peer)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_millis(200));
    let channel = pplx_space_common_grpc::connect_tcp_channel_lazy(endpoint, None).unwrap();
    tonic_health::pb::health_client::HealthClient::new(channel)
        .check(tonic_health::pb::HealthCheckRequest::default())
        .await
        .unwrap_err()
}

/// The client's request timeout and a server panic both surface as
/// `Cancelled`: the timeout, carrying `tonic::TimeoutExpired`, is a node
/// that didn't answer; the panic is a failure of the node's.
#[tokio::test]
async fn a_request_timeout_is_unreachable_and_a_server_panic_is_a_failure() {
    for (peer, reason) in [
        (FailingPeer::Hold, "node_unreachable"),
        (FailingPeer::Panic, "node_preview_failed"),
    ] {
        let status = status_from(peer).await;
        assert_eq!(status.code(), tonic::Code::Cancelled, "{status:?}");
        let (_, body) = DrainPreviewError::from_status(status).response();
        assert_eq!(body.0["reason"], reason, "{}", body.0);
    }
}

/// A node-api that predates `PreviewDrainNode` serves no such method, and
/// its gRPC server answers the call `Unimplemented`, as a sandbox-manager
/// that predates it answers the node-api relaying it: the preview reports
/// that node as too old to preview, not as one that failed.
#[tokio::test]
async fn a_node_without_the_preview_rpc_is_too_old_to_preview() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_, older) = tonic_health::server::health_reporter();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(older)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(5));
    let channel = pplx_space_common_grpc::connect_tcp_channel_lazy(endpoint, None).unwrap();
    let status =
        pplx_space_node_api_client::proto::worker_api_internal_client::WorkerApiInternalClient::new(
            channel,
        )
        .preview_drain_node(pplx_space_sbx_mgr_client::proto::PreviewDrainNodeRequest {})
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unimplemented, "{status:?}");
    let (answered, body) = DrainPreviewError::from_status(status).response();
    assert_eq!(
        (answered, body.0["reason"].as_str()),
        (
            StatusCode::NOT_IMPLEMENTED,
            Some("node_version_unsupported")
        ),
        "{}",
        body.0
    );
}

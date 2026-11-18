use std::collections::BTreeMap;

use axum::http::{Method, StatusCode};
use pplx_space_accounts::NodeId;
use pplx_space_sbx_mgr_client::proto::{
    ArtifactKind, DrainBlocked, DrainEvacuated, DrainPending, DrainReport, DrainWait,
    PreviewDrainNodeResponse,
};
use pplx_space_sql_locator::sql;

use super::*;
use crate::routes::admin::nodes::{V2DrainPreview, V2DrainProbe};
use crate::routes::admin::test_fixtures::{TestResult, admin_router_harness_with};

fn report() -> PreviewDrainNodeResponse {
    PreviewDrainNodeResponse {
        report: Some(DrainReport {
            evacuated: vec![DrainEvacuated {
                sandbox_id: "sbx-stopped".into(),
                artifact_kind: ArtifactKind::Disk as i32,
                artifact_id: "artifact-1".into(),
            }],
            pending: vec![
                DrainPending {
                    sandbox_id: "sbx-running".into(),
                    wait: DrainWait::Moving as i32,
                    running: true,
                    ..DrainPending::default()
                },
                DrainPending {
                    sandbox_id: "sbx-stuck".into(),
                    wait: DrainWait::External as i32,
                    running: false,
                    reason: "quarantined".into(),
                    since: Some(prost_types::Timestamp {
                        seconds: 1_800_000_000,
                        nanos: 0,
                    }),
                    pool_member: true,
                },
                DrainPending {
                    sandbox_id: "sbx-new".into(),
                    wait: 9,
                    ..DrainPending::default()
                },
            ],
            blocked: vec![DrainBlocked {
                reason: "orphan inventory scan failed".into(),
            }],
        }),
    }
}

/// The preview keeps every item the node reported, under the keys of the
/// stored v2 report: a wait this build doesn't know is unspecified.
#[test]
fn the_preview_body_keeps_every_item_the_node_reported() {
    let node_id = NodeId::new();
    assert_eq!(
        preview_body(node_id, &report().report.unwrap()),
        serde_json::json!({
            "node_id": node_id,
            "evacuated": [
                {"sandbox_id": "sbx-stopped", "artifact_kind": "disk", "artifact_id": "artifact-1"},
            ],
            "pending": [
                {"sandbox_id": "sbx-running", "wait": "moving", "running": true, "pool_member": false},
                {
                    "sandbox_id": "sbx-stuck",
                    "wait": "external",
                    "running": false,
                    "reason": "quarantined",
                    "since": 1_800_000_000,
                    "pool_member": true,
                },
                {"sandbox_id": "sbx-new", "wait": "unspecified", "running": false, "pool_member": false},
            ],
            "blocked": [{"reason": "orphan inventory scan failed"}],
        })
    );
}

/// What each answer of the node means to a caller: an older node-api or
/// sandbox-manager, a node that didn't answer, one busy with another preview
/// (retryable, with `Retry-After`), a sealed node, or another failure, each
/// with its own status and `reason`. A refused argument is the node's
/// failure: a preview carries none an older node could refuse.
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
        (Code::Aborted, StatusCode::SERVICE_UNAVAILABLE, "node_busy"),
        (
            Code::FailedPrecondition,
            StatusCode::CONFLICT,
            "orphaned_seal",
        ),
    ] {
        let error = DrainPreviewError::from_status(tonic::Status::new(code, "from the node"));
        assert_eq!(
            error.retry_after().is_some(),
            code == Code::Aborted,
            "{code:?}"
        );
        let (answered, body) = error.response();
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

/// Each table a preview could write: the node's own rows.
async fn node_rows(pool: &sqlx::PgPool, node_id: NodeId) -> TestResult<serde_json::Value> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT jsonb_build_object(
                    'node', (SELECT to_jsonb(n) FROM nodes n WHERE n.id = $1),
                    'node_k8s', (SELECT to_jsonb(k) FROM node_k8s k WHERE k.node_id = $1),
                    'node_direct', (SELECT to_jsonb(d) FROM node_direct d WHERE d.node_id = $1))"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

/// An operator previews a node through the admin router: the node's report
/// comes back as the node gave it, nothing is written to the node's rows
/// or the audit log, and only an operator may ask. A Direct node is asked
/// through the endpoint it registered. An unknown node is 404, and a cluster
/// or builder node 400; a node that can't be reached and one too old to
/// preview each answer their own error.
#[tokio::test(flavor = "current_thread")]
async fn an_operator_previews_a_node_and_nothing_is_written() -> TestResult {
    let scripts = BTreeMap::from([
        ("preview-ok".to_owned(), Ok(report())),
        ("127.0.0.1:9801".to_owned(), Ok(report())),
        (
            "preview-unreachable".to_owned(),
            Err(DrainPreviewError::Unreachable("connection refused".into())),
        ),
        (
            "preview-older".to_owned(),
            Err(DrainPreviewError::from_status(
                tonic::Status::unimplemented("unknown method PreviewDrainNode"),
            )),
        ),
    ]);
    let Some(harness) = admin_router_harness_with(|state| {
        state.v2_drain_probe = std::sync::Arc::new(ScriptedPreviews(scripts));
    })
    .await?
    else {
        return Ok(());
    };
    let pool = &harness.db.pool;
    let node = insert_node(pool, "worker", "k8s", "preview-ok").await?;
    let path = format!("/nodes/{node}/drain-v2-preview");
    let before = node_rows(pool, node).await?;

    let (status, body) = harness.as_operator(Method::GET, &path, &[], None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, preview_body(node, &report().report.unwrap()));
    harness
        .assert_refused_to_non_operators(Method::GET, &path, None)
        .await;
    assert_eq!(node_rows(pool, node).await?, before);
    assert_eq!(
        harness.audit_events(&path).await,
        Vec::<serde_json::Value>::new()
    );

    let (status, _) = harness
        .as_operator(
            Method::GET,
            &format!("/nodes/{}/drain-v2-preview", NodeId::new()),
            &[],
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    for (role, managed_by, name, expected, reason) in [
        (
            "cluster",
            "k8s",
            "preview-ok",
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            "builder",
            "k8s",
            "preview-ok",
            StatusCode::BAD_REQUEST,
            None,
        ),
        ("worker", "space", "127.0.0.1:9801", StatusCode::OK, None),
        (
            "worker",
            "k8s",
            "preview-unreachable",
            StatusCode::BAD_GATEWAY,
            Some("node_unreachable"),
        ),
        (
            "worker",
            "k8s",
            "preview-older",
            StatusCode::NOT_IMPLEMENTED,
            Some("node_version_unsupported"),
        ),
    ] {
        let other = insert_node(pool, role, managed_by, name).await?;
        let (status, body) = harness
            .as_operator(
                Method::GET,
                &format!("/nodes/{other}/drain-v2-preview"),
                &[],
                None,
            )
            .await;
        assert_eq!(
            (status, body["reason"].as_str()),
            (expected, reason),
            "{role} {managed_by} {name}: {body}"
        );
    }
    Ok(())
}

/// What `:drain` would refuse a v2 drain of the node for comes ahead of the
/// node's own blockers, each a node-level one, and keeps a drain of the node
/// from finishing: a Kubernetes node without a recorded EC2 instance, and a Direct
/// node, which is also without one. A Kubernetes node with its instance
/// recorded gets the node's answer as it is.
#[tokio::test(flavor = "current_thread")]
async fn the_preview_reports_what_drain_would_refuse_the_node_for() -> TestResult {
    let finished = PreviewDrainNodeResponse {
        report: Some(DrainReport::default()),
    };
    let unreachable = || Err(DrainPreviewError::Unreachable("connection refused".into()));
    let scripts = BTreeMap::from([
        ("recorded".to_owned(), Ok(finished.clone())),
        ("unrecorded".to_owned(), Ok(report())),
        ("127.0.0.1:9802".to_owned(), Ok(finished.clone())),
        ("recorded-unreachable".to_owned(), unreachable()),
        ("unrecorded-unreachable".to_owned(), unreachable()),
        ("127.0.0.1:9803".to_owned(), unreachable()),
    ]);
    let Some(harness) = admin_router_harness_with(|state| {
        state.v2_drain_probe = std::sync::Arc::new(ScriptedPreviews(scripts));
    })
    .await?
    else {
        return Ok(());
    };
    let pool = &harness.db.pool;
    const KUBERNETES_ONLY: &str = "drain engine v2 drains Kubernetes nodes only";
    const NO_INSTANCE: &str = "drain engine v2 needs the node's EC2 instance recorded: the \
                               drain terminates that instance, and asks EC2 about it when the \
                               node stops answering";
    let node_level = |reasons: &[&str]| -> Vec<serde_json::Value> {
        reasons
            .iter()
            .map(|reason| serde_json::json!({"reason": reason}))
            .collect()
    };
    for (managed_by, name, instance, refusals, node_blocked) in [
        ("k8s", "recorded", true, vec![], vec![]),
        (
            "k8s",
            "unrecorded",
            false,
            vec![NO_INSTANCE],
            preview_body(NodeId::new(), &report().report.unwrap())["blocked"]
                .as_array()
                .unwrap()
                .clone(),
        ),
        (
            "space",
            "127.0.0.1:9802",
            false,
            vec![KUBERNETES_ONLY, NO_INSTANCE],
            vec![],
        ),
    ] {
        let node = insert_node_with(pool, "worker", managed_by, name, instance).await?;
        let (status, body) = harness
            .as_operator(
                Method::GET,
                &format!("/nodes/{node}/drain-v2-preview"),
                &[],
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{name}: {body}");
        let mut expected = node_level(&refusals);
        expected.extend(node_blocked);
        assert_eq!(body["blocked"], serde_json::Value::from(expected), "{name}");
    }

    // A node that can't answer still gets the admin API's refusals, every
    // one, in the error.
    for (managed_by, name, instance, refusals) in [
        ("k8s", "recorded-unreachable", true, vec![]),
        ("k8s", "unrecorded-unreachable", false, vec![NO_INSTANCE]),
        (
            "space",
            "127.0.0.1:9803",
            false,
            vec![KUBERNETES_ONLY, NO_INSTANCE],
        ),
    ] {
        let node = insert_node_with(pool, "worker", managed_by, name, instance).await?;
        let (status, body) = harness
            .as_operator(
                Method::GET,
                &format!("/nodes/{node}/drain-v2-preview"),
                &[],
                None,
            )
            .await;
        assert_eq!(
            (status, body["reason"].as_str()),
            (StatusCode::BAD_GATEWAY, Some("node_unreachable")),
            "{name}: {body}"
        );
        assert_eq!(body["refusals"], serde_json::json!(refusals), "{name}");
    }
    Ok(())
}

/// Answers every preview with `response` after `delay`.
struct SlowPreview {
    delay: std::time::Duration,
    response: PreviewDrainNodeResponse,
}

impl V2DrainProbe for SlowPreview {
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

/// A node with a v2 drain recorded, a cancel pending included, is sealed or
/// about to be: the preview answers 409 without asking it. One no drain
/// holds is asked.
#[tokio::test(flavor = "current_thread")]
async fn a_node_with_a_v2_drain_recorded_is_not_previewed() -> TestResult {
    let Some(harness) = admin_router_harness_with(|state| {
        state.v2_drain_probe = std::sync::Arc::new(ScriptedPreviews(BTreeMap::from([
            ("drained-v2".to_owned(), Ok(report())),
            ("cancelling-v2".to_owned(), Ok(report())),
            ("idle".to_owned(), Ok(report())),
        ])));
    })
    .await?
    else {
        return Ok(());
    };
    let pool = &harness.db.pool;
    for (name, phase, previews) in [
        ("drained-v2", Some("drained"), false),
        ("cancelling-v2", None, false),
        ("idle", None, true),
    ] {
        let node = insert_node(pool, "worker", "k8s", name).await?;
        if name != "idle" {
            sqlx::query(sql!(
                "UPDATE nodes SET drain_phase = $2, drain_engine = 'v2',
                                  drain_status = '{\"generation\": 1, \"deadline_at\": 0}'::JSONB
                  WHERE id = $1"
            ))
            .bind(node)
            .bind(phase)
            .execute(pool)
            .await?;
        }
        let (status, body) = answer_of(
            preview_within(&harness.state, node, std::time::Duration::from_secs(30)).await,
        )
        .await;
        if previews {
            assert_eq!(status, StatusCode::OK, "{name}: {body}");
        } else {
            assert_eq!(
                (status, body["reason"].as_str()),
                (StatusCode::CONFLICT, Some(DRAIN_IN_PROGRESS_REASON)),
                "{name}: {body}"
            );
        }
    }
    Ok(())
}

/// A node that refuses the preview as sealed while no v2 drain of it is
/// recorded has an orphaned seal: 409 `orphaned_seal`, saying how to heal
/// it, counted on `node_drain.v2.orphaned_seal_refusals`. A node busy with
/// another preview is retryable: 503 `node_busy` with `Retry-After`.
#[tokio::test(flavor = "current_thread")]
async fn a_sealed_or_busy_node_answers_its_own_refusal() -> TestResult {
    let Some(harness) = admin_router_harness_with(|state| {
        state.v2_drain_probe = std::sync::Arc::new(ScriptedPreviews(BTreeMap::from([
            (
                "sealed".to_owned(),
                Err(DrainPreviewError::from_status(
                    tonic::Status::failed_precondition("sealed under generation 7"),
                )),
            ),
            (
                "busy".to_owned(),
                Err(DrainPreviewError::from_status(tonic::Status::aborted(
                    "another preview is in progress",
                ))),
            ),
        ])));
    })
    .await?
    else {
        return Ok(());
    };
    let pool = &harness.db.pool;
    let sealed = insert_node(pool, "worker", "k8s", "sealed").await?;
    let counters = pplx_space_metrics_emit::test_capture::capture_counters_guard();
    let answered = preview_within(&harness.state, sealed, std::time::Duration::from_secs(30)).await;
    let counted = counters.finish();
    let (status, body) = answer_of(answered).await;
    assert_eq!(
        (status, body["reason"].as_str()),
        (StatusCode::CONFLICT, Some(ORPHANED_SEAL_REASON)),
        "{body}"
    );
    assert!(
        body["error"].as_str().is_some_and(
            |error| error.contains(":undrain") && error.contains("sealed under generation 7")
        ),
        "{body}"
    );
    assert!(
        counted.iter().any(
            |counter| counter.name == "node_drain.v2.orphaned_seal_refusals"
                && counter.tags.contains(&format!("node_id:{sealed}"))
        ),
        "{counted:?}"
    );
    let busy = insert_node(pool, "worker", "k8s", "busy").await?;
    let answered = preview_within(&harness.state, busy, std::time::Duration::from_secs(30)).await;
    let retry_after = answered
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (status, body) = answer_of(answered).await;
    assert_eq!(
        (status, body["reason"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("node_busy")),
        "{body}"
    );
    assert_eq!(retry_after.as_deref(), Some("5"));
    Ok(())
}

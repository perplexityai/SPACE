//! Fixtures shared by the `lifecycle_writer_tests` modules, which characterize
//! the cluster-controller's writes to Postgres `sandboxes.lifecycle` against
//! the real Postgres fixture. Everything here writes Postgres control-plane
//! tables only, apart from [`kubernetes_substrate`], which also stands in for
//! the Kubernetes apiserver, in process. The stored values a sweep seeds come
//! from `StoredSandboxLifecycle::all_as_str`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::response::IntoResponse;
use k8s_openapi::api::core::v1::Pod;
use pplx_space_api::controllers::substrate::kubernetes::{
    KubernetesSubstrate, TEST_SANDBOX_NAMESPACE,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_db_test_fixtures::TestDb;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Lifecycles the `BEFORE UPDATE` trigger lets an evacuation marker survive a
/// lifecycle change into.
pub(crate) fn keeps_marker(lifecycle: &str) -> bool {
    matches!(lifecycle, "suspending" | "suspended" | "restoring")
}

/// A Postgres `sandboxes` row to insert. Inserts fire no `BEFORE UPDATE`
/// trigger, so the marker is stored as given whatever the lifecycle.
#[derive(Debug, Clone)]
pub(crate) struct SandboxSeed {
    pub(crate) lifecycle: String,
    pub(crate) managed_by: &'static str,
    pub(crate) active_node_id: Option<NodeId>,
    pub(crate) target_node_id: Option<NodeId>,
    pub(crate) marker: Option<&'static str>,
    /// Seconds since the last lifecycle change. `placement_changed_at` is an
    /// hour earlier still, the shape a row has once a suspend unplaced it.
    pub(crate) lifecycle_age_secs: i64,
}

impl SandboxSeed {
    /// An hour-old row of `managed_by`, unplaced and unmarked.
    pub(crate) fn new(lifecycle: &str, managed_by: &'static str) -> Self {
        Self {
            lifecycle: lifecycle.to_owned(),
            managed_by,
            active_node_id: None,
            target_node_id: None,
            marker: None,
            lifecycle_age_secs: 3600,
        }
    }

    /// Resident on `node_id`: active and targeted there, placed `on_node`.
    pub(crate) fn on_node(mut self, node_id: NodeId) -> Self {
        self.active_node_id = Some(node_id);
        self.target_node_id = Some(node_id);
        self
    }

    pub(crate) fn marked(mut self, target: &'static str) -> Self {
        self.marker = Some(target);
        self
    }
}

pub(crate) async fn insert_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    seed: &SandboxSeed,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, evacuation_restore_target,
             network_policy_generation, template_id, content_hash,
             lifecycle_changed_at, placement_changed_at
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, $3, CASE WHEN $4::uuid IS NULL THEN 'unplaced' ELSE 'on_node' END,
             $4, $5, $6, 1, 'app-runtime', 'sha256:synthetic',
             NOW() - make_interval(secs => $7::BIGINT),
             NOW() - make_interval(secs => $7::BIGINT + 3600)
         )"
    ))
    .bind(sandbox_id)
    .bind(seed.managed_by)
    .bind(&seed.lifecycle)
    .bind(seed.active_node_id)
    .bind(seed.target_node_id)
    .bind(seed.marker)
    .bind(seed.lifecycle_age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// A `k8s` worker `nodes` row with the given drain columns.
pub(crate) async fn insert_node(
    pool: &PgPool,
    drain_phase: Option<&str>,
    drain_mode: Option<&str>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode)
         VALUES ($1, '10.0.0.99'::inet, 'worker', 'k8s', 'registered', $2, $3)"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(drain_mode)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// One `sandbox_k8s_executions` row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExecutionSeed {
    pub(crate) generation: i64,
    pub(crate) state: &'static str,
    pub(crate) active: bool,
}

impl ExecutionSeed {
    pub(crate) const RUNNING: Self = Self {
        generation: 1,
        state: "running",
        active: true,
    };
}

pub(crate) fn pod_name(sandbox_id: SandboxId, generation: i64) -> String {
    format!("space-sbx-{}-g{generation}", sandbox_id.as_uuid().simple())
}

pub(crate) fn pod_uid(sandbox_id: SandboxId, generation: i64) -> String {
    format!("{}-uid-g{generation}", sandbox_id.as_uuid().simple())
}

/// Insert an execution an hour old, with the pod name and UID
/// [`pod_name`] and [`pod_uid`] derive for its generation.
pub(crate) async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    execution: ExecutionSeed,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             state, active, created_at, updated_at)
         VALUES ($1, $2, $3, 'sandboxes', $4, $5, $6,
                 NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour')"
    ))
    .bind(sandbox_id)
    .bind(execution.generation)
    .bind(pod_name(sandbox_id, execution.generation))
    .bind(pod_uid(sandbox_id, execution.generation))
    .bind(execution.state)
    .bind(execution.active)
    .execute(pool)
    .await?;
    Ok(())
}

/// Register a full snapshot in `state`, taken `offset_secs` after the row's
/// last lifecycle change (negative: before it); a `ready` snapshot gets a
/// durable storage location.
pub(crate) async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
    offset_secs: i64,
) -> TestResult {
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES (
             $1, $2, 'full', $3, $4,
             (SELECT lifecycle_changed_at + make_interval(secs => $5::BIGINT)
                FROM sandboxes WHERE id = $2)
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(state)
    .bind(NodeId::new())
    .bind(offset_secs)
    .execute(pool)
    .await?;
    if state == "ready" {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(snapshot_id)
        .bind(format!("s3://synthetic-bucket/{snapshot_id}"))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Record a published local suspend artifact on the row.
pub(crate) async fn pin_local_artifact(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

/// Store `lifecycle` on a row and put `lifecycle_changed_at` back, so a writer
/// whose other guards age the row sees the stored lifecycle as the only
/// change. The trigger stamps the column only when the lifecycle moves, so
/// the second write sticks.
pub(crate) async fn relabel_keeping_lifecycle_age(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
) -> TestResult {
    let mut tx = pool.begin().await?;
    let changed_at: String = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at::text FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = $2::timestamptz WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(changed_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// A one-connection pool on the test database and that connection's backend
/// pid. A writer handed this pool runs every statement on that backend, so a
/// test can wait for exactly that backend to block.
pub(crate) async fn single_backend_pool(db: &TestDb) -> TestResult<(PgPool, i32)> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .connect(&db.database_url)
        .await?;
    let pid = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&pool)
        .await?;
    Ok((pool, pid))
}

/// Bound for a whole interleaving step: waiting for the lock, and joining the
/// writer that waited.
pub(crate) const INTERLEAVING_BOUND: Duration = Duration::from_secs(10);

/// Run `step` within [`INTERLEAVING_BOUND`], failing the test instead of
/// hanging it.
pub(crate) async fn bounded<T>(step: impl Future<Output = TestResult<T>>) -> TestResult<T> {
    tokio::time::timeout(INTERLEAVING_BOUND, step)
        .await
        .map_err(|_| format!("interleaving step exceeded {INTERLEAVING_BOUND:?}"))?
}

/// Poll `pg_blocking_pids` every 10 ms until backend `waiter_pid` is blocked
/// by backend `blocker_pid`. Callers bound it with [`bounded`].
pub(crate) async fn wait_until_blocked(
    pool: &PgPool,
    waiter_pid: i32,
    blocker_pid: i32,
) -> TestResult {
    loop {
        let blocked: bool = sqlx::query_scalar(sql!("SELECT $2 = ANY(pg_blocking_pids($1))"))
            .bind(waiter_pid)
            .bind(blocker_pid)
            .fetch_one(pool)
            .await?;
        if blocked {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// A Kubernetes substrate, for the writers that call one.
// ---------------------------------------------------------------------------

/// The namespace the substrate's sandbox pods and their executions live in.
pub(crate) const KUBERNETES_NAMESPACE: &str = TEST_SANDBOX_NAMESPACE;

/// A port no host service listens on, for a substrate that must not dial.
const CLOSED_PORT: u16 = 1;

/// A `KubernetesSubstrate` over `pool` whose apiserver is an in-process
/// double holding no pods and no nodes; see [`kubernetes_substrate_with_pods`].
///
/// Its callers seed no pod and no execution with a `pod_ip`, so the substrate
/// resolves no address to dial. Should one resolve, [`CLOSED_PORT`] keeps the
/// dial off any host service: it fails rather than sending an RPC, so no
/// bearer token is read.
pub(crate) fn kubernetes_substrate(pool: &PgPool) -> TestResult<KubernetesSubstrate> {
    kubernetes_substrate_with_pods(pool, Vec::new(), CLOSED_PORT)
}

/// A `KubernetesSubstrate` over `pool` whose caches start warm holding `pods`
/// (Pod objects in the [`KUBERNETES_NAMESPACE`]) and no nodes, and which
/// dials a pod's sandbox proxy on `sandbox_proxy_port`; a node's node-api is
/// dialed there too only when its node-api pod advertises no port. Its
/// budgets are the production defaults.
///
/// Its apiserver is a double of its own, answering a read of a held pod by
/// name with that pod. Anything else, including any write, is a 404.
pub(crate) fn kubernetes_substrate_with_pods(
    pool: &PgPool,
    pods: Vec<serde_json::Value>,
    sandbox_proxy_port: u16,
) -> TestResult<KubernetesSubstrate> {
    let seeded = pods
        .iter()
        .map(|pod| serde_json::from_value::<Pod>(pod.clone()))
        .collect::<Result<_, _>>()?;
    let held = Arc::new(pods);
    let apiserver = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let held = Arc::clone(&held);
        async move { apiserver_double(&held, &request) }
    });
    Ok(KubernetesSubstrate::with_seeded_caches(
        pool.clone(),
        kube::Client::new(apiserver, KUBERNETES_NAMESPACE),
        sandbox_proxy_port,
        seeded,
    ))
}

fn apiserver_double(
    pods: &[serde_json::Value],
    request: &axum::extract::Request,
) -> axum::response::Response {
    use axum::http::{Method, StatusCode};

    let held = request
        .uri()
        .path()
        .strip_prefix(&format!("/api/v1/namespaces/{KUBERNETES_NAMESPACE}/pods/"))
        .filter(|name| !name.contains('/'))
        .and_then(|name| {
            pods.iter()
                .find(|pod| pod["metadata"]["name"].as_str() == Some(name))
        });
    match (request.method(), held) {
        (&Method::GET, Some(pod)) => axum::Json(pod.clone()).into_response(),
        _ => (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "NotFound",
                "code": 404,
            })),
        )
            .into_response(),
    }
}

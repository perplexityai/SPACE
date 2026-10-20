//! Fixtures shared by the `lifecycle_writer_tests` modules, which characterize
//! the cluster-controller's writes to Postgres `sandboxes.lifecycle` against
//! the real Postgres fixture. Everything here writes Postgres control-plane
//! tables only, apart from [`kubernetes_substrate`], which also stands in for
//! the Kubernetes apiserver.

use std::ffi::OsStr;
use std::future::{Future, IntoFuture};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::response::IntoResponse;
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxLifecycle};
use pplx_space_common_svc::bearer_token::{
    SPACE_INTERNAL_CONTROL_TOKEN_ENV, SPACE_INTERNAL_TOKEN_ENV,
    SPACE_NODE_API_INTERNAL_TOKEN_PATH_ENV, SPACE_NODE_API_TOKEN_PATH_ENV,
};
use pplx_space_db_test_fixtures::TestDb;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

/// Values Postgres `sandboxes.lifecycle` can hold that are not
/// [`SandboxLifecycle`] variants (see `SandboxLifecycle::all`).
pub(crate) const NON_VARIANT_LIFECYCLES: [&str; 2] = ["deleted", "cleanup_required"];

/// Every value a sweep stores in Postgres `sandboxes.lifecycle`.
pub(crate) fn stored_lifecycles() -> impl Iterator<Item = &'static str> {
    SandboxLifecycle::all()
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .chain(NON_VARIANT_LIFECYCLES)
}

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

/// Re-run the calling test in a child process whose environment lets
/// [`kubernetes_substrate`] build a substrate. Call it first, before any
/// fixture, and return when it returns `false`: the child has then run the
/// test.
///
/// `pplx-space-api` builds its Kubernetes test substrates in `#[cfg(test)]`
/// code this crate cannot reach, and the substrate's only public constructor
/// is the production one, `KubernetesSubstrate::try_from_env`, which reads the
/// process environment. So the child gets the variables it reads. This parent
/// keeps a directory until the child exits: [`KUBECONFIG_DOUBLE_ENV`] names
/// it, `KUBECONFIG` names a file in it that does not exist yet, and the
/// node-api token paths name files in it holding the internal tokens, so the
/// token reader never falls back to the host's token files. The child's own
/// copy of the directory is unused. The
/// proxy variables kube reads are removed so the client dials the loopback
/// apiserver directly, and the internal-RPC variables let the substrate dial a
/// node-api double in plaintext.
pub(crate) fn enter_kubernetes_process() -> TestResult<bool> {
    let owned = tempfile::tempdir()?;
    let kubeconfig = owned.path().join("kubeconfig");
    let node_api_token = owned.path().join("node-api.token");
    let node_api_internal_token = owned.path().join("node-api-internal.token");
    std::fs::write(&node_api_token, INTERNAL_TOKEN)?;
    std::fs::write(&node_api_internal_token, INTERNAL_CONTROL_TOKEN)?;
    Ok(pplx_space_test_env::enter_test_process(&[
        ("SPACE_K8S_CONTROL_PLANE", Some(OsStr::new("true"))),
        ("KUBECONFIG", Some(kubeconfig.as_os_str())),
        (KUBECONFIG_DOUBLE_ENV, Some(owned.path().as_os_str())),
        (
            "SPACE_SANDBOX_NAMESPACE",
            Some(OsStr::new(KUBERNETES_NAMESPACE)),
        ),
        (
            "SPACE_NODE_API_NAMESPACE",
            Some(OsStr::new(KUBERNETES_NAMESPACE)),
        ),
        ("HTTPS_PROXY", None),
        ("https_proxy", None),
        ("KUBE_RS_DEBUG_OVERRIDE_URL", None),
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
        (SPACE_INTERNAL_TOKEN_ENV, Some(OsStr::new(INTERNAL_TOKEN))),
        (
            SPACE_INTERNAL_CONTROL_TOKEN_ENV,
            Some(OsStr::new(INTERNAL_CONTROL_TOKEN)),
        ),
        (
            SPACE_NODE_API_TOKEN_PATH_ENV,
            Some(node_api_token.as_os_str()),
        ),
        (
            SPACE_NODE_API_INTERNAL_TOKEN_PATH_ENV,
            Some(node_api_internal_token.as_os_str()),
        ),
    ]))
}

const INTERNAL_TOKEN: &str = "synthetic-internal-token";
const INTERNAL_CONTROL_TOKEN: &str = "synthetic-internal-control-token";

/// Names the directory [`enter_kubernetes_process`] created for its child.
/// The fixture writes a kubeconfig only as a new file in that directory (see
/// [`write_owned_kubeconfig`]), so a test that skips the child process cannot
/// overwrite a developer's kubeconfig.
const KUBECONFIG_DOUBLE_ENV: &str = "SPACE_TEST_KUBECONFIG_DOUBLE";

/// Create `kubeconfig` holding `contents`, only when its directory, with
/// symlinks and `..` resolved, is the directory `owned_dir` names, and only
/// as a new file: an existing file, or a symlink in its place, is refused
/// rather than written through.
fn write_owned_kubeconfig(
    kubeconfig: Option<&OsStr>,
    owned_dir: Option<&OsStr>,
    contents: &str,
) -> TestResult {
    let refuse = |why: &str| -> TestResult {
        Err(format!(
            "refusing to write a kubeconfig: {why}; call enter_kubernetes_process first \
             and run inside its child"
        )
        .into())
    };
    let (Some(kubeconfig), Some(owned_dir)) = (kubeconfig, owned_dir) else {
        return refuse(&format!(
            "KUBECONFIG and {KUBECONFIG_DOUBLE_ENV} must both be set"
        ));
    };
    let kubeconfig = Path::new(kubeconfig);
    let Some(directory) = kubeconfig.parent() else {
        return refuse("KUBECONFIG names no directory");
    };
    if directory.canonicalize()? != Path::new(owned_dir).canonicalize()? {
        return refuse(&format!(
            "KUBECONFIG is not in the directory {KUBECONFIG_DOUBLE_ENV} names"
        ));
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(kubeconfig)?
        .write_all(contents.as_bytes())?;
    Ok(())
}

/// The namespace the substrate's sandbox pods and node-api pods live in.
pub(crate) const KUBERNETES_NAMESPACE: &str = "space";

/// A `KubernetesSubstrate` built by `try_from_env` over `pool`, its
/// apiserver an in-process double holding no pods and no nodes. Only inside
/// [`enter_kubernetes_process`]'s child.
pub(crate) async fn kubernetes_substrate(pool: &PgPool) -> TestResult<KubernetesSubstrate> {
    kubernetes_substrate_with_pods(pool, Vec::new()).await
}

/// [`kubernetes_substrate`] whose apiserver double holds `pods` (Pod objects
/// in the [`KUBERNETES_NAMESPACE`]) and no nodes.
///
/// The double answers every pod or node collection read with what it holds,
/// whatever the selector, and a read of a held pod by name with that pod. It
/// holds every watch open without an event, so the substrate's caches sync at
/// once with exactly those pods. Anything else, including any write, is a
/// 404.
///
/// The kubeconfig naming the double can only be created once, so the double
/// is one per process: the first call starts it, and every call replaces the
/// pods it holds. A substrate built earlier keeps the pods its caches synced
/// with, since no watch event arrives; only its direct API reads see the new
/// ones.
pub(crate) async fn kubernetes_substrate_with_pods(
    pool: &PgPool,
    pods: Vec<serde_json::Value>,
) -> TestResult<KubernetesSubstrate> {
    let held = APISERVER_DOUBLE_PODS
        .get_or_try_init(start_apiserver_double)
        .await?;
    *held.lock().map_err(|_| "apiserver double pods poisoned")? = pods;
    KubernetesSubstrate::try_from_env(pool, CancellationToken::new(), TaskTracker::new())
        .await?
        .ok_or_else(|| "enter_kubernetes_process sets SPACE_K8S_CONTROL_PLANE".into())
}

type HeldPods = Arc<Mutex<Vec<serde_json::Value>>>;

/// The pods this process's apiserver double holds, once it is started.
static APISERVER_DOUBLE_PODS: tokio::sync::OnceCell<HeldPods> = tokio::sync::OnceCell::const_new();

/// Start the apiserver double and create the kubeconfig that names it.
async fn start_apiserver_double() -> TestResult<HeldPods> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let server = format!("http://{}", listener.local_addr()?);
    write_owned_kubeconfig(
        std::env::var_os("KUBECONFIG").as_deref(),
        std::env::var_os(KUBECONFIG_DOUBLE_ENV).as_deref(),
        &format!(
            "apiVersion: v1
kind: Config
clusters:
- name: double
  cluster:
    server: {server}
users:
- name: double
  user: {{}}
contexts:
- name: double
  context:
    cluster: double
    user: double
    namespace: {KUBERNETES_NAMESPACE}
current-context: double
"
        ),
    )?;
    let pods = HeldPods::default();
    let held = Arc::clone(&pods);
    let apiserver = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let held = Arc::clone(&held);
        async move {
            match held.lock() {
                Ok(pods) => apiserver_double(&pods, &request),
                Err(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    });
    tokio::spawn(axum::serve(listener, apiserver).into_future());
    Ok(pods)
}

fn apiserver_double(
    pods: &[serde_json::Value],
    request: &axum::extract::Request,
) -> axum::response::Response {
    use axum::http::{Method, StatusCode, header};

    let path = request.uri().path();
    let watch = request
        .uri()
        .query()
        .is_some_and(|query| query.split('&').any(|pair| pair == "watch=true"));
    let list = |kind: &str, items: &[serde_json::Value]| {
        axum::Json(serde_json::json!({
            "kind": kind,
            "apiVersion": "v1",
            "metadata": { "resourceVersion": "1" },
            "items": items,
        }))
        .into_response()
    };
    let pod_name = path
        .strip_prefix(&format!("/api/v1/namespaces/{KUBERNETES_NAMESPACE}/pods/"))
        .filter(|name| !name.contains('/'));
    let held = pod_name.and_then(|name| {
        pods.iter()
            .find(|pod| pod["metadata"]["name"].as_str() == Some(name))
    });
    let collection = path.ends_with("/pods") || path.ends_with("/nodes");
    match (request.method(), held) {
        (&Method::GET, _) if collection && watch => (
            [(header::CONTENT_TYPE, "application/json")],
            axum::body::Body::from_stream(futures_util::stream::pending::<
                Result<bytes::Bytes, std::convert::Infallible>,
            >()),
        )
            .into_response(),
        (&Method::GET, _) if path.ends_with("/pods") => list("PodList", pods),
        (&Method::GET, _) if path.ends_with("/nodes") => list("NodeList", &[]),
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

#[cfg(test)]
mod tests;

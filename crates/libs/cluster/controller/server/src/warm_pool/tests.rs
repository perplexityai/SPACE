use pplx_space_common_core::types::SandboxId;
use pplx_space_common_core::types::{OrgId, UserId};

use pplx_space_common_core::sql;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use k8s_openapi::api::core::v1::{Node, PodCondition, PodSpec, PodStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    client::{Body as KubeBody, ClientBuilder},
    runtime::{reflector, watcher::Event},
};
use pplx_space_api::controllers::substrate::direct::DirectSubstrate;
use pplx_space_common_core::{
    kubernetes::{
        DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY, DEFAULT_SANDBOX_INTERNAL_SECRET_NAME,
        DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY, DEFAULT_SANDBOX_NODE_SELECTOR_KEY,
        DEFAULT_SANDBOX_NODE_SELECTOR_VALUE, DEFAULT_SANDBOX_POD_IMAGE,
        DEFAULT_SANDBOX_RUNTIME_CLASS, SANDBOX_INTERNAL_SECRET_NAME_ENV,
        SANDBOX_NODE_SELECTOR_LABELS_ENV, SANDBOX_NODE_SELECTOR_VALUE_ENV, SANDBOX_POD_IMAGE_ENV,
        SANDBOX_POOL_CONFIG_ID_KEY, SANDBOX_POOL_ROLE_KEY,
    },
    types::{ContentHash, Digest, MemorySize, TemplateName, VcpuCount},
};
use pplx_space_db_test_fixtures::pg_test_db;

use time::OffsetDateTime;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    body::Body as GrpcBody,
    codegen::http::{Method, Request, Response, StatusCode},
    transport::Server,
};
use tower::Service;

use super::*;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
type TestResult<T = ()> = std::result::Result<T, BoxError>;

const POD_CONFIG_ENV_VARS: &[&str] = &[
    SANDBOX_POD_IMAGE_ENV,
    SANDBOX_NODE_SELECTOR_VALUE_ENV,
    SANDBOX_NODE_SELECTOR_LABELS_ENV,
    SANDBOX_INTERNAL_SECRET_NAME_ENV,
];

const SPACED_WAIT_READY: &str = "/space.node.api.v1.WorkerApi/SpacedWaitReady";

struct TestWorkerApi {
    port: u16,
    state: Arc<Mutex<FaultState>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestWorkerApi {
    async fn spawn(faults: Vec<Fault>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(FaultState {
            calls: 0,
            faults: VecDeque::from(faults),
        }));
        let service = FaultyGrpcService {
            state: Arc::clone(&state),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            Server::builder()
                .serve_with_incoming_shutdown(
                    service,
                    TcpListenerStream::new(listener),
                    async move {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .unwrap();
        });

        Self {
            port,
            state,
            shutdown: Some(shutdown_tx),
        }
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn calls(&self, path: &str) -> usize {
        assert_eq!(path, SPACED_WAIT_READY);
        self.state.lock().unwrap().calls
    }
}

impl Drop for TestWorkerApi {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// A resolver with the gate closed, which is what these tests exercise: they
/// assert warm-slot lifecycle, not placement, and a closed gate issues no
/// query — so the lazily-connected pool is never dialed. Placement rendering is
/// covered by the pod-builder snapshots and by `build_pool_pod`'s own tests.
fn disabled_node_group_placement() -> NodeGroupPlacementResolver {
    NodeGroupPlacementResolver::new(
        sqlx::PgPool::connect_lazy("postgresql://localhost/unused").unwrap(),
        false,
    )
}

fn reconcile_stage_test_reconciler(runtime: &tokio::runtime::Runtime) -> Reconciler {
    let _guard = runtime.enter();
    reconcile_stage_test_reconciler_inner()
}

fn reconcile_stage_test_reconciler_inner() -> Reconciler {
    let kube = ClientBuilder::new(
        tower::service_fn(|_: Request<KubeBody>| async {
            Err::<Response<KubeBody>, BoxError>(
                "reconcile_config_stage unexpectedly called Kubernetes".into(),
            )
        }),
        "space",
    )
    .build();
    let (pod_store, _) = reflector::store();
    let (node_store, _) = reflector::store();
    let postgres = sqlx::PgPool::connect_lazy("postgresql://localhost/unused").unwrap();

    Reconciler {
        kube,
        pod_cache: PodCache {
            store: pod_store,
            ready: ReflectorReady::always_ready(),
        },
        node_store,
        store: WarmPoolStore::new(postgres.clone()),
        substrate: Arc::new(Substrate::Direct(Box::new(DirectSubstrate::new(postgres)))),
        namespace: "space".to_string(),
        pod_config: pool_pod_config(),
        warming_slot_timeout: DEFAULT_WARMING_SLOT_TIMEOUT,
        claimed_slot_timeout: DEFAULT_CLAIMED_SLOT_TIMEOUT,
        max_concurrent_configs: DEFAULT_MAX_CONCURRENT_CONFIGS,
        create_permits: Arc::new(Semaphore::new(MAX_WARM_SLOT_CREATE_CONCURRENCY)),
        io_timeouts: ReconcileIoTimeouts {
            postgres: DEFAULT_RECONCILE_POSTGRES_IO_TIMEOUT,
            k8s: DEFAULT_RECONCILE_K8S_IO_TIMEOUT,
            grpc_substrate: DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT,
        },
        ready_probe: SbxManagerWarmSlotReadyProbe::new(DEFAULT_SANDBOX_PROXY_PORT),
        inventory_publications: Mutex::new(HashMap::new()),
        orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig::default(),
        orphan_sweep_cursor: AtomicUsize::new(0),
        receipt_backstops: Mutex::new(ReceiptBackstopState::default()),
        node_group_placement: disabled_node_group_placement(),
    }
}

/// [`reconcile_test_reconciler`] with the node-group placement gate **open**,
/// resolving against the fixture's own database.
fn reconcile_test_reconciler_with_placement(
    kube: Client,
    store: WarmPoolStore,
    postgres: sqlx::PgPool,
) -> Reconciler {
    let resolver = NodeGroupPlacementResolver::new(postgres.clone(), true);
    Reconciler {
        node_group_placement: resolver,
        ..reconcile_test_reconciler(kube, store, postgres)
    }
}

fn reconcile_test_reconciler(
    kube: Client,
    store: WarmPoolStore,
    postgres: sqlx::PgPool,
) -> Reconciler {
    let (pod_store, _) = reflector::store();
    let (node_store, _) = reflector::store();
    Reconciler {
        kube,
        pod_cache: PodCache {
            store: pod_store,
            ready: ReflectorReady::always_ready(),
        },
        node_store,
        store,
        substrate: Arc::new(Substrate::Direct(Box::new(DirectSubstrate::new(postgres)))),
        namespace: "space".to_string(),
        pod_config: pool_pod_config(),
        warming_slot_timeout: DEFAULT_WARMING_SLOT_TIMEOUT,
        claimed_slot_timeout: DEFAULT_CLAIMED_SLOT_TIMEOUT,
        max_concurrent_configs: DEFAULT_MAX_CONCURRENT_CONFIGS,
        create_permits: Arc::new(Semaphore::new(MAX_WARM_SLOT_CREATE_CONCURRENCY)),
        io_timeouts: ReconcileIoTimeouts {
            postgres: DEFAULT_RECONCILE_POSTGRES_IO_TIMEOUT,
            k8s: DEFAULT_RECONCILE_K8S_IO_TIMEOUT,
            grpc_substrate: DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT,
        },
        ready_probe: SbxManagerWarmSlotReadyProbe::new(DEFAULT_SANDBOX_PROXY_PORT),
        inventory_publications: Mutex::new(HashMap::new()),
        orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig::default(),
        orphan_sweep_cursor: AtomicUsize::new(0),
        receipt_backstops: Mutex::new(ReceiptBackstopState::default()),
        node_group_placement: disabled_node_group_placement(),
    }
}

async fn seeded_pool_config(store: &WarmPoolStore, config_id: Uuid) -> PoolConfig {
    store
        .reconcilable_configs()
        .await
        .expect("pool config query should succeed")
        .into_iter()
        .find(|config| config.id == config_id)
        .expect("seeded pool config is reconcilable")
}

struct FaultState {
    calls: usize,
    faults: VecDeque<Fault>,
}

struct Fault {
    code: Code,
    message: &'static str,
}

impl Fault {
    fn new(code: Code, message: &'static str) -> Self {
        Self { code, message }
    }
}

#[derive(Clone)]
struct FaultyGrpcService {
    state: Arc<Mutex<FaultState>>,
}

impl Service<Request<GrpcBody>> for FaultyGrpcService {
    type Response = Response<GrpcBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<GrpcBody>) -> Self::Future {
        let path = request.uri().path().to_string();
        let response = {
            let mut state = self.state.lock().unwrap();
            assert_eq!(path, SPACED_WAIT_READY);
            state.calls += 1;
            match state.faults.pop_front() {
                Some(fault) => grpc_status_response(fault.code, fault.message),
                None => grpc_ok_response(),
            }
        };
        Box::pin(async move { Ok(response) })
    }
}

fn grpc_status_response(code: Code, message: &str) -> Response<GrpcBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("grpc-status", (code as i32).to_string())
        .header("grpc-message", message)
        .body(GrpcBody::empty())
        .unwrap()
}

fn grpc_ok_response() -> Response<GrpcBody> {
    static EMPTY_GRPC_MESSAGE: &[u8] = &[0, 0, 0, 0, 0];
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("grpc-status", "0")
        .body(GrpcBody::new(Full::new(Bytes::from_static(
            EMPTY_GRPC_MESSAGE,
        ))))
        .unwrap()
}

async fn seed_pool_config(
    pool: &sqlx::PgPool,
    template_name: &TemplateName,
    template_release_id: Uuid,
    config_id: Uuid,
    content_hash: ContentHash,
) -> TestResult {
    let user_id = UserId::new();
    let org_id = OrgId::new();

    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Warm Pool Probe Test', $2)"
    ))
    .bind(org_id)
    .bind(format!("warm-pool-probe-test-{org_id}"))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Warm Pool Probe Test', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("warm-pool-probe-test-{user_id}@example.com"))
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, $2, $3)"
    ))
    .bind(Uuid::now_v7())
    .bind(template_name.as_str())
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, $2, 'fc', $3)"
    ))
    .bind(template_release_id)
    .bind(template_name.as_str())
    .bind(content_hash.to_string())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 2, 1024, 1, 1, 1, 'space', true)"
    ))
    .bind(config_id)
    .bind(template_release_id)
    .execute(pool)
    .await?;

    Ok(())
}

fn kubernetes_client_rejecting_requests(message: &'static str) -> Client {
    ClientBuilder::new(
        tower::service_fn(move |_: Request<KubeBody>| async move {
            Err::<Response<KubeBody>, BoxError>(message.into())
        }),
        "space",
    )
    .build()
}

/// [`reconcile_kubernetes_client`] whose node LIST returns **one** worker that
/// `needs_template_write_annotation` accepts, and which acks the annotation
/// patch.
///
/// Needed because the plain fake answers the LIST with an empty `items`, which
/// makes `template_write_annotations == 0` hold on every path — including the
/// one where the annotation stage ran. An assertion that the pull did not
/// happen is only evidence if the fixture makes the pull possible.
fn reconcile_kubernetes_client_with_annotatable_worker(patched: Arc<AtomicUsize>) -> Client {
    let config = pool_pod_config();
    let node = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "worker-annotatable",
            "labels": { config.sandbox_node_selector_key.clone(): config.sandbox_node_selector_value.clone() },
        },
    });
    ClientBuilder::new(
        tower::service_fn(move |request: Request<KubeBody>| {
            let patched = Arc::clone(&patched);
            let node = node.clone();
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::GET, "/api/v1/nodes") => kubernetes_json_response(
                        StatusCode::OK,
                        &serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "NodeList",
                            "metadata": {},
                            "items": [node],
                        }),
                    ),
                    (Method::PATCH, "/api/v1/nodes/worker-annotatable") => {
                        patched.fetch_add(1, Ordering::SeqCst);
                        kubernetes_json_response(StatusCode::OK, &node)
                    }
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        let body = http_body_util::BodyExt::collect(request.into_body())
                            .await?
                            .to_bytes();
                        let pod: serde_json::Value = serde_json::from_slice(&body)?;
                        kubernetes_json_response(StatusCode::CREATED, &pod)
                    }
                    _ => kubernetes_json_response(
                        StatusCode::OK,
                        &serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "PodList",
                            "metadata": {},
                            "items": []
                        }),
                    ),
                }
            }
        }),
        "space",
    )
    .build()
}

fn reconcile_kubernetes_client(observation_failure: Arc<AtomicBool>) -> Client {
    ClientBuilder::new(
        tower::service_fn(move |request: Request<KubeBody>| {
            let observation_failure = Arc::clone(&observation_failure);
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::GET, "/api/v1/nodes") => kubernetes_json_response(
                        StatusCode::OK,
                        &serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "NodeList",
                            "metadata": {},
                            "items": []
                        }),
                    ),
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        let body = http_body_util::BodyExt::collect(request.into_body())
                            .await?
                            .to_bytes();
                        let pod: serde_json::Value = serde_json::from_slice(&body)?;
                        kubernetes_json_response(StatusCode::CREATED, &pod)
                    }
                    _ if observation_failure.load(Ordering::SeqCst) => {
                        observation_failure.store(false, Ordering::SeqCst);
                        kubernetes_status_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "injected Kubernetes observation failure",
                        )
                    }
                    (method, path) => kubernetes_status_response(
                        StatusCode::BAD_REQUEST,
                        &format!("unexpected Kubernetes request: {method} {path}"),
                    ),
                }
            }
        }),
        "space",
    )
    .build()
}

fn mock_kubernetes_client_for_pool_creation() -> Client {
    let service = tower::service_fn(move |request: Request<KubeBody>| async move {
        match (request.method().clone(), request.uri().path()) {
            (Method::GET, "/api/v1/nodes") => Ok::<_, BoxError>(kubernetes_json_response(
                StatusCode::OK,
                &serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "NodeList",
                    "items": []
                }),
            )?),
            (Method::POST, "/api/v1/namespaces/space/pods") => Ok::<_, BoxError>(
                kubernetes_json_response(StatusCode::CREATED, &Pod::default())?,
            ),
            (method, path) => {
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                    "reason": "BadRequest",
                    "code": 400
                });
                Ok::<_, BoxError>(kubernetes_json_response(StatusCode::BAD_REQUEST, &body)?)
            }
        }
    });
    ClientBuilder::new(service, "space").build()
}

fn kubernetes_json_response<T: serde::Serialize>(
    status: StatusCode,
    value: &T,
) -> Result<Response<KubeBody>, BoxError> {
    let mut body = serde_json::to_value(value)?;
    if let Some(object) = body.as_object_mut() {
        object
            .entry("apiVersion".to_string())
            .or_insert_with(|| serde_json::Value::String("v1".to_string()));
        object
            .entry("kind".to_string())
            .or_insert_with(|| serde_json::Value::String("Pod".to_string()));
    }
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(KubeBody::from(serde_json::to_vec(&body)?))?)
}

fn kubernetes_status_response(
    status: StatusCode,
    message: &str,
) -> Result<Response<KubeBody>, BoxError> {
    kubernetes_json_response(
        status,
        &serde_json::json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": message,
            "reason": "InternalError",
            "code": status.as_u16()
        }),
    )
}

#[tokio::test(start_paused = true)]
async fn timed_io_times_out_slow_future() {
    let result = super::timed_io("test-slow-io", Duration::from_millis(1), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<(), std::io::Error>(())
    })
    .await;

    let err = result.expect_err("slow I/O should time out");

    assert!(matches!(&err, TimedIoError::Timeout(_)));
    assert!(err.to_string().contains("operation=test-slow-io"));
    assert!(err.to_string().contains("timed out after 1ms"));
}

#[tokio::test(start_paused = true)]
async fn pod_create_timeout_keeps_warming_slot_row_for_later_observation() {
    let result = super::timed_io("test-create-timeout", Duration::from_millis(1), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<(), KubeError>(())
    })
    .await;
    let err = result.expect_err("pod create should time out");

    assert!(!super::should_cleanup_warming_slot_after_create_error(&err));
}

#[test]
fn warm_slot_create_concurrency_follows_refill_per_tick() {
    // Below the ceiling the concurrency tracks `refill_per_tick` exactly,
    // so the whole tick's batch can be created concurrently.
    let config = pool_config(20, 5, 30);

    assert_eq!(super::warm_slot_create_concurrency(&config), 5);
}

#[test]
fn warm_slot_create_concurrency_clamps_to_minimum_one() {
    // A zero (or negative) `refill_per_tick` floors at 1 so a buffer of 0
    // never stalls the fan-out entirely.
    let config = pool_config(20, 0, 30);

    assert_eq!(super::warm_slot_create_concurrency(&config), 1);
}

#[test]
fn warm_slot_create_concurrency_clamps_to_ceiling() {
    // A `refill_per_tick` set very high in the DB is capped at the hard
    // ceiling so the fan-out cannot spike the K8s apiserver.
    let config = pool_config(500, 100, 500);

    assert_eq!(
        super::warm_slot_create_concurrency(&config),
        super::MAX_WARM_SLOT_CREATE_CONCURRENCY
    );
}

#[test]
fn sandbox_namespace_prefers_space_sandbox_namespace() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(super::SANDBOX_NAMESPACE_ENV, "sandboxes");
    env.set(super::POD_NAMESPACE_ENV, "space");

    let namespace = super::sandbox_namespace_from_environment(&env);

    assert_eq!(namespace, "sandboxes");
}

#[test]
fn sandbox_namespace_falls_back_to_pod_namespace() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(super::POD_NAMESPACE_ENV, "space");

    let namespace = super::sandbox_namespace_from_environment(&env);

    assert_eq!(namespace, "space");
}

#[test]
fn pod_config_uses_api_compatible_env_defaults() {
    {
        let vars: Vec<(&str, Option<&std::ffi::OsStr>)> =
            POD_CONFIG_ENV_VARS.iter().map(|key| (*key, None)).collect();

        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        let config = super::pool_pod_config_from_env().unwrap();

        assert_eq!(config.sandbox_image, DEFAULT_SANDBOX_POD_IMAGE);
        assert_eq!(config.runtime_class_name, DEFAULT_SANDBOX_RUNTIME_CLASS);
        assert_eq!(
            config.sandbox_node_selector_key,
            DEFAULT_SANDBOX_NODE_SELECTOR_KEY
        );
        assert_eq!(
            config.sandbox_node_selector_value,
            DEFAULT_SANDBOX_NODE_SELECTOR_VALUE
        );
        assert!(config.extra_node_selector_labels.is_empty());
        assert_eq!(
            config.internal_secret_name,
            DEFAULT_SANDBOX_INTERNAL_SECRET_NAME
        );
        assert_eq!(
            config.internal_token_key,
            DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY
        );
        assert_eq!(
            config.internal_control_token_key,
            DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY
        );
    };
}

#[test]
fn slots_to_create_respects_target_refill_and_warming_budget() {
    let config = pool_config(10, 3, 5);

    let create_count = slots_to_create(&config, 4, 6);

    assert_eq!(create_count, 1);
}

#[test]
fn slots_to_create_stops_when_open_slots_meet_target() {
    let config = pool_config(2, 3, 5);

    let create_count = slots_to_create(&config, 0, 2);

    assert_eq!(create_count, 0);
}

#[test]
fn slots_to_create_stops_when_pool_is_disabled() {
    let mut config = pool_config(10, 3, 5);
    config.enabled = false;

    let create_count = slots_to_create(&config, 0, 0);

    assert_eq!(create_count, 0);
}

#[test]
fn target_open_slots_is_zero_when_pool_is_disabled() {
    let mut config = pool_config(10, 3, 5);
    config.enabled = false;

    let target = target_open_slots(&config);

    assert_eq!(target, 0);
}

#[test]
fn slot_matches_config_requires_cached_policy_and_namespace() {
    let config = pool_config(10, 3, 5);
    let mut slot = pool_slot_created_at(OffsetDateTime::now_utc());
    slot.template_release_id = config.template_release_id;

    let matches = slot_matches_config(&slot, &config, "space");

    assert!(matches);

    slot.disk_io_limits = pplx_space_common_core::types::DiskIoLimits::try_new(
        5_000,
        slot.disk_io_limits.max_bandwidth_mib_per_sec(),
    )
    .unwrap();
    assert!(!slot_matches_config(&slot, &config, "space"));
    slot.disk_io_limits = config.disk_io_limits;

    slot.disk_mib = Some(32768);
    assert!(!slot_matches_config(&slot, &config, "space"));
    slot.disk_mib = config.disk_mib;

    slot.pod_namespace = "old-space".to_string();
    let matches = slot_matches_config(&slot, &config, "space");

    assert!(!matches);
}

#[test]
fn ready_slot_requires_matching_pod_identity() {
    let mut slot = pool_slot_created_at(OffsetDateTime::now_utc());
    slot.pod_uid = Some("pod-uid".to_string());
    slot.pod_ip = Some("10.90.0.9".to_string());
    slot.k8s_node_name = Some("worker-1".to_string());
    let mut pod = Pod {
        metadata: ObjectMeta {
            uid: Some("pod-uid".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some("worker-1".to_string()),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some("10.90.0.9".to_string()),
            conditions: Some(vec![ready_condition("True")]),
            ..Default::default()
        }),
    };

    assert!(ready_slot_matches_pod(
        &slot,
        &SandboxPodView::from(pod.clone())
    ));

    pod.metadata.uid = Some("replacement-pod-uid".to_string());
    assert!(!ready_slot_matches_pod(
        &slot,
        &SandboxPodView::from(pod.clone())
    ));
}

#[tokio::test]
async fn claimable_slot_metadata_rejects_running_pod_until_sandbox_started() {
    let slot = pool_slot_created_at(OffsetDateTime::now_utc());
    let pod = running_ready_pod("pod-uid", "10.90.0.9", "worker-1");

    let metadata = claimable_slot_metadata(
        &slot,
        &SandboxPodView::from(pod.clone()),
        &FakeReadyProbe { started: false },
        "sha256:test",
    )
    .await;

    assert!(matches!(metadata, ClaimableSlotMetadata::ProbeFailed(_)));
}

#[tokio::test]
async fn claimable_slot_metadata_accepts_started_sandbox() {
    let slot = pool_slot_created_at(OffsetDateTime::now_utc());
    let pod = running_ready_pod("pod-uid", "10.90.0.9", "worker-1");

    let metadata = claimable_slot_metadata(
        &slot,
        &SandboxPodView::from(pod.clone()),
        &FakeReadyProbe { started: true },
        "sha256:test",
    )
    .await;

    let ClaimableSlotMetadata::Ready(metadata) = metadata else {
        panic!("started sandbox should be claimable");
    };

    assert_eq!(metadata.uid, "pod-uid");
    assert_eq!(metadata.pod_ip, "10.90.0.9");
    assert_eq!(metadata.node_name, "worker-1");
}

#[tokio::test]
async fn observe_warming_slots_retries_not_started_probe_and_promotes_slot() -> TestResult {
    // arrange

    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("warm-pool-probe-test-token")),
        ),
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
    ]) {
        return Ok(());
    }

    let db = pg_test_db().await?;
    let store = WarmPoolStore::new(db.pool.clone());
    let template_name = TemplateName::parse("app-runtime")?;
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x42; 32]));
    seed_pool_config(
        &db.pool,
        &template_name,
        template_release_id,
        config_id,
        content_hash,
    )
    .await?;
    let pod_name = format!("space-sbx-{}-g1", Uuid::now_v7().simple());
    let slot = store
        .insert_warming_slot(NewPoolSlot {
            config_id,
            template_release_id,
            reserved_sandbox_id: SandboxId::new(),
            template_name: &template_name,
            content_hash,
            vcpu: VcpuCount::try_from_u32(2)?,
            memory: MemorySize::mib(1024)?,
            pod_name: &pod_name,
            pod_namespace: "space",
        })
        .await?;
    let worker = TestWorkerApi::spawn(vec![Fault::new(
        Code::FailedPrecondition,
        "sandbox is starting",
    )])
    .await;
    let mut pod = running_ready_pod("pod-uid", "127.0.0.1", "worker-1");
    pod.metadata.name = Some(pod_name.clone());
    pod.metadata.namespace = Some("space".to_string());
    pod.metadata.labels = Some(BTreeMap::from([
        (
            SANDBOX_POOL_ROLE_KEY.to_string(),
            SANDBOX_POOL_ROLE_WARM.to_string(),
        ),
        (
            SANDBOX_POOL_CONFIG_ID_KEY.to_string(),
            config_id.to_string(),
        ),
    ]));
    let kube = kubernetes_client_rejecting_requests(
        "cache-based warming observation made a Kubernetes request",
    );
    let (pod_store, mut pod_writer) = reflector::store();
    pod_writer.apply_watcher_event(&Event::Init);
    pod_writer.apply_watcher_event(&Event::InitApply(SandboxPodView::from(pod)));
    pod_writer.apply_watcher_event(&Event::InitDone);
    let (node_store, mut node_writer) = reflector::store();
    let reconciler = Reconciler {
        kube,
        pod_cache: PodCache {
            store: pod_store,
            ready: ReflectorReady::always_ready(),
        },
        node_store,
        store: store.clone(),
        substrate: Arc::new(Substrate::Direct(Box::new(DirectSubstrate::new(
            db.pool.clone(),
        )))),
        namespace: "space".to_string(),
        pod_config: pool_pod_config(),
        warming_slot_timeout: DEFAULT_WARMING_SLOT_TIMEOUT,
        claimed_slot_timeout: DEFAULT_CLAIMED_SLOT_TIMEOUT,
        max_concurrent_configs: DEFAULT_MAX_CONCURRENT_CONFIGS,
        create_permits: Arc::new(Semaphore::new(MAX_WARM_SLOT_CREATE_CONCURRENCY)),
        io_timeouts: ReconcileIoTimeouts {
            postgres: DEFAULT_RECONCILE_POSTGRES_IO_TIMEOUT,
            k8s: DEFAULT_RECONCILE_K8S_IO_TIMEOUT,
            grpc_substrate: DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT,
        },
        ready_probe: SbxManagerWarmSlotReadyProbe {
            sandbox_proxy_port: worker.port(),
            probe_timeout: WARM_SLOT_STARTED_PROBE_TIMEOUT,
        },
        inventory_publications: Mutex::new(HashMap::new()),
        orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig::default(),
        orphan_sweep_cursor: AtomicUsize::new(0),
        receipt_backstops: Mutex::new(ReceiptBackstopState::default()),
        node_group_placement: disabled_node_group_placement(),
    };
    let config = store
        .reconcilable_configs()
        .await?
        .into_iter()
        .find(|config| config.id == config_id)
        .expect("seeded pool config is reconcilable");

    // A ready pod alone cannot bind its node incarnation.
    let pending = reconciler.observe_warming_slots(&config).await?;
    assert_eq!(pending.marked_ready_slots, 0);
    assert_eq!(worker.calls(SPACED_WAIT_READY), 0);
    node_writer.apply_watcher_event(&Event::Apply(Node {
        metadata: ObjectMeta {
            name: Some("worker-1".to_string()),
            uid: Some("original-worker-uid".to_string()),
            ..Default::default()
        },
        ..Default::default()
    }));
    let summary = reconciler.observe_warming_slots(&config).await?;

    // assert
    assert_eq!(summary.marked_ready_slots, 1);
    let recorded: Option<String> = sqlx::query_scalar(sql!(
        "SELECT k8s_node_uid FROM kubernetes_pool_slots WHERE id=$1"
    ))
    .bind(slot.id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(recorded.as_deref(), Some("original-worker-uid"));
    assert_eq!(worker.calls(SPACED_WAIT_READY), 2);
    let counts = store.slot_counts(config_id).await?;
    assert_eq!(counts.warming, 0);
    assert_eq!(counts.ready, 1);
    let open_slots = store.open_slots(config_id).await?;
    assert_eq!(open_slots.len(), 1);
    assert_eq!(open_slots[0].id, slot.id);
    assert_eq!(open_slots[0].state, PoolSlotState::Ready);
    assert_eq!(open_slots[0].pod_uid.as_deref(), Some("pod-uid"));
    assert_eq!(open_slots[0].pod_ip.as_deref(), Some("127.0.0.1"));
    assert_eq!(open_slots[0].k8s_node_name.as_deref(), Some("worker-1"));
    Ok(())
}

#[tokio::test]
async fn observe_warming_slots_keeps_young_slot_missing_from_cache() -> TestResult {
    let db = pg_test_db().await?;
    let store = WarmPoolStore::new(db.pool.clone());
    let template_name = TemplateName::parse("app-runtime")?;
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x43; 32]));
    seed_pool_config(
        &db.pool,
        &template_name,
        template_release_id,
        config_id,
        content_hash,
    )
    .await?;
    store
        .insert_warming_slot(NewPoolSlot {
            config_id,
            template_release_id,
            reserved_sandbox_id: SandboxId::new(),
            template_name: &template_name,
            content_hash,
            vcpu: VcpuCount::try_from_u32(2)?,
            memory: MemorySize::mib(1024)?,
            pod_name: "space-sbx-not-yet-observed-g1",
            pod_namespace: "space",
        })
        .await?;
    let kube = kubernetes_client_rejecting_requests(
        "cache-based warming observation made a Kubernetes request",
    );
    let (pod_store, _) = reflector::store();
    let (node_store, _) = reflector::store();
    let reconciler = Reconciler {
        kube,
        pod_cache: PodCache {
            store: pod_store,
            ready: ReflectorReady::always_ready(),
        },
        node_store,
        store: store.clone(),
        substrate: Arc::new(Substrate::Direct(Box::new(DirectSubstrate::new(
            db.pool.clone(),
        )))),
        namespace: "space".to_string(),
        pod_config: pool_pod_config(),
        warming_slot_timeout: DEFAULT_WARMING_SLOT_TIMEOUT,
        claimed_slot_timeout: DEFAULT_CLAIMED_SLOT_TIMEOUT,
        max_concurrent_configs: DEFAULT_MAX_CONCURRENT_CONFIGS,
        create_permits: Arc::new(Semaphore::new(MAX_WARM_SLOT_CREATE_CONCURRENCY)),
        io_timeouts: ReconcileIoTimeouts {
            postgres: DEFAULT_RECONCILE_POSTGRES_IO_TIMEOUT,
            k8s: DEFAULT_RECONCILE_K8S_IO_TIMEOUT,
            grpc_substrate: DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT,
        },
        ready_probe: SbxManagerWarmSlotReadyProbe::new(DEFAULT_SANDBOX_PROXY_PORT),
        inventory_publications: Mutex::new(HashMap::new()),
        orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig::default(),
        orphan_sweep_cursor: AtomicUsize::new(0),
        receipt_backstops: Mutex::new(ReceiptBackstopState::default()),
        node_group_placement: disabled_node_group_placement(),
    };
    let config = store
        .reconcilable_configs()
        .await?
        .into_iter()
        .find(|config| config.id == config_id)
        .expect("seeded pool config is reconcilable");

    let summary = reconciler.observe_warming_slots(&config).await?;

    assert_eq!(summary.deleted_missing_pods, 0);
    assert_eq!(store.slot_counts(config_id).await?.warming, 1);
    Ok(())
}

#[tokio::test]
async fn reconcile_config_refills_after_observation_failure() -> TestResult {
    let db = pg_test_db().await?;
    let store = WarmPoolStore::new(db.pool.clone());
    let template_name = TemplateName::parse("app-runtime")?;
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x44; 32]));
    seed_pool_config(
        &db.pool,
        &template_name,
        template_release_id,
        config_id,
        content_hash,
    )
    .await?;
    let ready_slot = store
        .insert_warming_slot(NewPoolSlot {
            config_id,
            template_release_id,
            reserved_sandbox_id: SandboxId::new(),
            template_name: &template_name,
            content_hash,
            vcpu: VcpuCount::try_from_u32(2)?,
            memory: MemorySize::mib(1024)?,
            pod_name: "space-sbx-ready-but-cache-missing-g1",
            pod_namespace: "space",
        })
        .await?;
    store
        .mark_slot_ready(
            ready_slot.id,
            "pod-uid",
            "127.0.0.1",
            "worker-1",
            &pplx_space_common_core::types::K8sNodeUid::parse("original-worker-uid")?,
        )
        .await?;
    let observation_failure = Arc::new(AtomicBool::new(true));
    let reconciler = reconcile_test_reconciler(
        reconcile_kubernetes_client(Arc::clone(&observation_failure)),
        store.clone(),
        db.pool.clone(),
    );
    let mut config = seeded_pool_config(&store, config_id).await;
    config.target_ready_slots = 2;

    let summary = reconciler
        .reconcile_config(&config, &HashSet::new())
        .await?;

    assert!(
        !observation_failure.load(Ordering::SeqCst),
        "the injected ready-slot observation failure must be exercised"
    );
    assert_eq!(summary.created_slots, 1);
    assert_eq!(summary.failed_required_operations, 1);
    assert!(!summary.completed_required_operations());
    let counts = store.slot_counts(config_id).await?;
    assert_eq!(counts.ready, 1);
    assert_eq!(counts.warming, 1);
    let warming_slots = store.warming_slots(config_id).await?;
    assert_eq!(warming_slots[0].template_release_id, template_release_id);
    assert_eq!(warming_slots[0].content_hash, content_hash);
    Ok(())
}

/// A config whose candidate set cannot be resolved must stop **growing** and
/// keep **shrinking**.
///
/// The property, in operator terms: a pool config naming a template no pooled
/// pod may host is a misconfiguration an operator has to fix, and while it
/// stands the config must not pull that template onto shared workers or fill
/// new slots. But it must still be drainable — the same tick evicts slots on
/// cordoned nodes (which node drain waits on) and deletes excess slots (which
/// is how disabling a config drains it to zero). Aborting the tick on the
/// refusal would take those away, and because an unhostable template is a
/// standing condition, it would take them away permanently.
///
/// `reconciled_slot_counts` is the discriminator: it is populated by a stage
/// that runs *after* the resolve, so its presence proves execution continued
/// past the refusal rather than merely not erroring.
#[tokio::test]
async fn reconcile_config_refused_by_placement_still_runs_teardown() -> TestResult {
    let db = pg_test_db().await?;
    let store = WarmPoolStore::new(db.pool.clone());
    let template_name = TemplateName::parse("acme-private-tools")?;
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x51; 32]));
    seed_pool_config(
        &db.pool,
        &template_name,
        template_release_id,
        config_id,
        content_hash,
    )
    .await?;
    // Flip the template org-specific: no org-less pooled pod may host it, so
    // the warm-pool subject's candidate set is empty and the resolver fails
    // closed. Phase 2 below seeds a generic group to show the refusal is by
    // *visibility* and not merely by an empty `node_groups` table.
    sqlx::query(sql!(
        "UPDATE templates SET visibility = 'org_specific' WHERE name = $1"
    ))
    .bind(template_name.as_str())
    .execute(&db.pool)
    .await?;
    let config = seeded_pool_config(&store, config_id).await;
    // One annotatable worker, so "no worker was told to pull this template" is
    // a claim the fixture can falsify. The `patched` counter is the direct
    // observation: the summary's own count would read 0 either way if the LIST
    // returned nothing.
    let patched = Arc::new(AtomicUsize::new(0));
    let reconciler = reconcile_test_reconciler_with_placement(
        reconcile_kubernetes_client_with_annotatable_worker(Arc::clone(&patched)),
        store.clone(),
        db.pool.clone(),
    );

    let summary = reconciler
        .reconcile_config(&config, &HashSet::new())
        .await
        .expect("a refused candidate set must not abort the config's tick");

    assert_eq!(
        summary.created_slots, 0,
        "a config that cannot be placed must not grow"
    );
    assert_eq!(
        summary.failed_required_operations, 1,
        "the refusal must be booked, not swallowed"
    );
    assert!(
        summary.reconciled_slot_counts.is_some(),
        "the stages after the resolve must still run, or a standing \
         misconfiguration would freeze eviction and drain forever"
    );
    assert_eq!(
        patched.load(Ordering::SeqCst),
        0,
        "an unhostable template must not be pulled onto any worker"
    );
    assert_eq!(summary.template_write_annotations, 0);
    let slot_rows =
        sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*) FROM kubernetes_pool_slots"))
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(slot_rows, 0);

    // Phase 2 — a generic group now exists, and the template is still
    // org-specific. Nothing changes: the refusal is a property of the
    // template's visibility, not of an empty node-group table, so the
    // mechanism this test names is the one operating.
    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, 'workers', NULL, 'default', TRUE)"
    ))
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;

    let still_refused = reconciler
        .reconcile_config(&config, &HashSet::new())
        .await
        .expect("a refused candidate set must not abort the config's tick");

    assert_eq!(still_refused.failed_required_operations, 1);
    assert_eq!(
        patched.load(Ordering::SeqCst),
        0,
        "generic capacity existing does not make an org-specific template \
         hostable by an org-less pooled pod"
    );

    // Phase 3 — the control. Same fixture, same worker, same generic group,
    // template flipped back to generic. The pull happens and the slot fills,
    // which is what makes the two zeros above evidence rather than an artifact
    // of a fixture that could never have produced a one.
    sqlx::query(sql!(
        "UPDATE templates SET visibility = 'generic' WHERE name = $1"
    ))
    .bind(template_name.as_str())
    .execute(&db.pool)
    .await?;

    let resolvable = reconciler
        .reconcile_config(&config, &HashSet::new())
        .await
        .expect("a resolvable config reconciles");

    assert_eq!(
        patched.load(Ordering::SeqCst),
        1,
        "a resolvable config does pull its template onto the worker"
    );
    assert_eq!(
        resolvable.created_slots, 1,
        "and it does fill, so the blocked growth above was the gate's doing"
    );
    assert_eq!(resolvable.failed_required_operations, 0);
    Ok(())
}

#[tokio::test]
async fn reconcile_config_does_not_refill_when_slot_counts_fail() -> TestResult {
    let db = pg_test_db().await?;
    let store = WarmPoolStore::new(db.pool.clone());
    let template_name = TemplateName::parse("app-runtime")?;
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x45; 32]));
    seed_pool_config(
        &db.pool,
        &template_name,
        template_release_id,
        config_id,
        content_hash,
    )
    .await?;
    let config = seeded_pool_config(&store, config_id).await;
    sqlx::query(sql!("ALTER TABLE kubernetes_pool_slots DROP COLUMN state"))
        .execute(&db.pool)
        .await?;
    let reconciler = reconcile_test_reconciler(
        reconcile_kubernetes_client(Arc::new(AtomicBool::new(false))),
        store.clone(),
        db.pool.clone(),
    );

    let err = reconciler
        .reconcile_config(&config, &HashSet::new())
        .await
        .expect_err("slot_counts failure must abort refill");

    assert!(err.to_string().contains("column \"state\" does not exist"));
    let slot_rows =
        sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*) FROM kubernetes_pool_slots"))
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(slot_rows, 0);
    Ok(())
}

#[tokio::test]
async fn reconcile_once_starts_preparing_republished_exact_release() -> TestResult {
    let db = pg_test_db().await?;
    let store = WarmPoolStore::new(db.pool.clone());
    let template_name = TemplateName::parse("ask-project-files")?;
    let republished_release_id = Uuid::now_v7();
    let current_release_id = Uuid::now_v7();
    let republished_hash = ContentHash::new(Digest::sha256([0x63; 32]));
    let current_hash = ContentHash::new(Digest::sha256([0x64; 32]));
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Warm Pool Republish Test', $2)"
    ))
    .bind(org_id)
    .bind(format!("warm-pool-republish-test-{org_id}"))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, $2, $3)"
    ))
    .bind(Uuid::now_v7())
    .bind(template_name.as_str())
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (
             id, template_name, type, hash, created_at
         ) VALUES
             ($1, $3, 'fc', $4, NOW() - INTERVAL '2 days'),
             ($2, $3, 'fc', $5, NOW() - INTERVAL '1 day')"
    ))
    .bind(republished_release_id)
    .bind(current_release_id)
    .bind(template_name.as_str())
    .bind(republished_hash.to_string())
    .bind(current_hash.to_string())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (
             template_release_id, template_name, tag, created_at, modified_at
         ) VALUES
             ($1, $3, $4, NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days'),
             ($2, $3, $5, NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day')"
    ))
    .bind(republished_release_id)
    .bind(current_release_id)
    .bind(template_name.as_str())
    .bind(republished_hash.to_string())
    .bind(current_hash.to_string())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, refill_per_tick, max_warming_slots, retain_releases
         ) VALUES ($1, 'fc', 2, 1024, 1, 10, 1, 1, 1)"
    ))
    .bind(template_name.as_str())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE template_release_tags
         SET modified_at = NOW()
         WHERE template_release_id = $1
           AND tag = $2"
    ))
    .bind(republished_release_id)
    .bind(republished_hash.to_string())
    .execute(&db.pool)
    .await?;

    let (pod_store, _) = reflector::store();
    let (node_store, _) = reflector::store();
    let reconciler = Reconciler {
        kube: mock_kubernetes_client_for_pool_creation(),
        pod_cache: PodCache {
            store: pod_store,
            ready: ReflectorReady::always_ready(),
        },
        node_store,
        store: store.clone(),
        substrate: Arc::new(Substrate::Direct(Box::new(DirectSubstrate::new(
            db.pool.clone(),
        )))),
        namespace: "space".to_string(),
        pod_config: pool_pod_config(),
        warming_slot_timeout: DEFAULT_WARMING_SLOT_TIMEOUT,
        claimed_slot_timeout: DEFAULT_CLAIMED_SLOT_TIMEOUT,
        max_concurrent_configs: DEFAULT_MAX_CONCURRENT_CONFIGS,
        create_permits: Arc::new(Semaphore::new(MAX_WARM_SLOT_CREATE_CONCURRENCY)),
        io_timeouts: ReconcileIoTimeouts {
            postgres: DEFAULT_RECONCILE_POSTGRES_IO_TIMEOUT,
            k8s: DEFAULT_RECONCILE_K8S_IO_TIMEOUT,
            grpc_substrate: DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT,
        },
        ready_probe: SbxManagerWarmSlotReadyProbe::new(DEFAULT_SANDBOX_PROXY_PORT),
        inventory_publications: Mutex::new(HashMap::new()),
        orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig::default(),
        orphan_sweep_cursor: AtomicUsize::new(0),
        receipt_backstops: Mutex::new(ReceiptBackstopState::default()),
        node_group_placement: disabled_node_group_placement(),
    };

    let summary = reconciler.reconcile_once().await?;

    assert_eq!(summary.created_slots, 1);
    let (template_release_id, target_ready_slots, enabled, managed_by) =
        sqlx::query_as::<_, (Uuid, i32, bool, String)>(sql!(
            "SELECT template_release_id, target_ready_slots, enabled, managed_by
             FROM kubernetes_pool_configs"
        ))
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(template_release_id, republished_release_id);
    assert_eq!(target_ready_slots, 1);
    assert!(enabled);
    assert_eq!(managed_by, "autoscaler");
    let slot = sqlx::query_as::<_, (Uuid, String, String, String)>(sql!(
        "SELECT template_release_id, template_name, content_hash, state
         FROM kubernetes_pool_slots"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        slot,
        (
            republished_release_id,
            template_name.to_string(),
            republished_hash.to_string(),
            "warming".to_string(),
        )
    );

    Ok(())
}

#[test]
fn warm_slot_probe_retry_classification_treats_not_started_as_transient() {
    let timeout = WarmSlotProbeAttemptError::Timeout(ReconcileIoTimeout {
        operation: "warm-slot-started-probe",
        timeout_ms: 300,
    });
    assert_eq!(warm_slot_probe_outcome(&timeout), ProbeOutcome::Timeout);
    assert!(is_retryable_warm_slot_probe_error(&timeout));
    assert_eq!(warm_slot_probe_failure_reason(&timeout), "probe_timeout");

    let unavailable = WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::WaitReadyStatus(
        tonic::Status::unavailable("deploy blip"),
    ));
    assert_eq!(warm_slot_probe_outcome(&unavailable), ProbeOutcome::Error);
    assert!(is_retryable_warm_slot_probe_error(&unavailable));
    assert_eq!(
        warm_slot_probe_failure_reason(&unavailable),
        "wait_ready_unavailable"
    );

    let deadline_exceeded = WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::WaitReadyStatus(
        tonic::Status::deadline_exceeded("deadline exceeded"),
    ));
    assert!(is_retryable_warm_slot_probe_error(&deadline_exceeded));
    assert_eq!(
        warm_slot_probe_failure_reason(&deadline_exceeded),
        "wait_ready_deadline_exceeded"
    );

    let not_started = WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::WaitReadyStatus(
        tonic::Status::failed_precondition("sandbox is starting"),
    ));
    assert_eq!(
        warm_slot_probe_outcome(&not_started),
        ProbeOutcome::NotStarted
    );
    assert!(is_retryable_warm_slot_probe_error(&not_started));
    assert_eq!(
        warm_slot_probe_failure_reason(&not_started),
        "sandbox_not_started"
    );

    let invalid_endpoint = WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::InvalidEndpoint(
        WarmSlotRuntimeEndpointError::InvalidIp {
            pod_ip: "bad pod IP".to_string(),
            source: "bad pod IP".parse::<IpAddr>().unwrap_err(),
        },
    ));
    assert_eq!(
        warm_slot_probe_outcome(&invalid_endpoint),
        ProbeOutcome::Error
    );
    assert!(!is_retryable_warm_slot_probe_error(&invalid_endpoint));
}

#[test]
fn warm_slot_probe_retry_delay_desynchronizes_slots() {
    let slot_without_jitter = Uuid::from_u128(0);
    let slot_with_jitter = Uuid::from_u128(1);

    assert_eq!(
        warm_slot_probe_retry_delay(slot_without_jitter, 0),
        Duration::from_millis(40)
    );
    assert_eq!(
        warm_slot_probe_retry_delay(slot_with_jitter, 0),
        Duration::from_millis(41)
    );
    assert_ne!(
        warm_slot_probe_retry_delay(slot_without_jitter, 0),
        warm_slot_probe_retry_delay(slot_with_jitter, 0)
    );
    assert_eq!(
        warm_slot_probe_retry_delay(slot_without_jitter, 1),
        Duration::from_millis(80)
    );
}

#[test]
fn summary_add_assign_accumulates_deleted_excess_slots() {
    let mut summary = ReconcileSummary {
        template_write_annotations: 1,
        deleted_unready_pods: 5,
        deleted_excess_slots: 1,
        deleted_mismatched_slots: 3,
        deleted_cleanup_pending_slots: 9,
        ..Default::default()
    };

    summary += ReconcileSummary {
        template_write_annotations: 2,
        deleted_unready_pods: 7,
        deleted_excess_slots: 2,
        deleted_mismatched_slots: 4,
        deleted_cleanup_pending_slots: 11,
        ..Default::default()
    };

    assert_eq!(summary.template_write_annotations, 3);
    assert_eq!(summary.deleted_unready_pods, 12);
    assert_eq!(summary.deleted_excess_slots, 3);
    assert_eq!(summary.deleted_mismatched_slots, 7);
    assert_eq!(summary.deleted_cleanup_pending_slots, 20);
}

#[test]
fn summary_delete_counts_maps_every_delete_field_to_its_reason() {
    // Distinct primes per field so a swapped or dropped mapping in
    // delete_counts() can't cancel out in the assertions below.
    let summary = ReconcileSummary {
        deleted_missing_pods: 2,
        deleted_unready_pods: 3,
        deleted_excess_slots: 5,
        deleted_mismatched_slots: 7,
        deleted_stale_claimed_slots: 11,
        deleted_dead_claimed_slots: 13,
        deleted_orphaned_receipts: 23,
        deleted_cleanup_pending_slots: 17,
        deleted_cordoned_slots: 19,
        ..Default::default()
    };

    assert_eq!(
        summary.delete_counts(),
        [
            (SlotDeleteReason::MissingPod, 2),
            (SlotDeleteReason::UnreadyPod, 3),
            (SlotDeleteReason::Excess, 5),
            (SlotDeleteReason::Mismatched, 7),
            (SlotDeleteReason::StaleClaimed, 11),
            (SlotDeleteReason::DeadClaimed, 13),
            (SlotDeleteReason::OrphanedReceipt, 23),
            (SlotDeleteReason::CleanupPending, 17),
            (SlotDeleteReason::Cordoned, 19),
        ]
    );
    assert_eq!(
        summary.deleted_slots(),
        2 + 3 + 5 + 7 + 11 + 13 + 17 + 19 + 23
    );
}

#[test]
fn summary_add_assign_accumulates_required_operation_failures() {
    let mut summary = ReconcileSummary {
        failed_required_operations: 1,
        ..Default::default()
    };

    summary += ReconcileSummary {
        failed_required_operations: 2,
        ..Default::default()
    };

    assert_eq!(summary.failed_required_operations, 3);
    assert!(!summary.completed_required_operations());
}

#[test]
fn cleanup_pending_slot_delete_result_keeps_sweep_best_effort() {
    let slot = pool_slot_created_at(OffsetDateTime::now_utc());
    let mut summary = ReconcileSummary::default();

    Reconciler::record_cleanup_pending_slot_delete_result(
        &mut summary,
        &slot,
        Err(super::WarmPoolError::io(std::io::Error::other(
            "node-api unavailable",
        ))),
    );
    Reconciler::record_cleanup_pending_slot_delete_result(&mut summary, &slot, Ok(false));
    Reconciler::record_cleanup_pending_slot_delete_result(&mut summary, &slot, Ok(true));

    assert_eq!(summary.deleted_cleanup_pending_slots, 1);
}

#[tokio::test]
async fn cleanup_pending_slot_batch_attempts_remaining_slots_after_delete_failure() {
    let failed = cleanup_pending_slot(
        "019e6040-4a23-7472-bf54-d1feb6f0d201",
        "019e6040-5356-72a0-a1da-c3360aa8d43b",
        "space-sbx-failed-g1",
    );
    let skipped = cleanup_pending_slot(
        "019e6040-59c0-7551-866f-f5f39196c79d",
        "019e6040-601a-7196-bef6-d2762c6a9a60",
        "space-sbx-skipped-g1",
    );
    let deleted = cleanup_pending_slot(
        "019e6040-68c4-75b5-9817-6a76d6922058",
        "019e6040-6e6f-72fc-8420-2f71aef7c7a7",
        "space-sbx-deleted-g1",
    );
    let mut outcomes: VecDeque<Result<bool, super::WarmPoolError>> = VecDeque::from([
        Err(super::WarmPoolError::io(std::io::Error::other(
            "node-api unavailable",
        ))),
        Ok(false),
        Ok(true),
    ]);
    let mut attempted = Vec::new();

    let summary = Reconciler::delete_cleanup_pending_slot_batch(
        vec![failed.clone(), skipped.clone(), deleted.clone()],
        |slot| {
            attempted.push(slot.id);
            let result = outcomes.pop_front().expect("one outcome per slot");
            async move { result }
        },
    )
    .await;

    assert!(outcomes.is_empty());
    assert_eq!(attempted, vec![failed.id, skipped.id, deleted.id]);
    assert_eq!(summary.deleted_cleanup_pending_slots, 1);
}

#[tokio::test(start_paused = true)]
async fn cleanup_pending_slot_batch_attempts_remaining_slots_after_timeout() {
    let timed_out = cleanup_pending_slot(
        "019e6040-4a23-7472-bf54-d1feb6f0d201",
        "019e6040-5356-72a0-a1da-c3360aa8d43b",
        "space-sbx-timed-out-g1",
    );
    let deleted = cleanup_pending_slot(
        "019e6040-68c4-75b5-9817-6a76d6922058",
        "019e6040-6e6f-72fc-8420-2f71aef7c7a7",
        "space-sbx-deleted-g1",
    );
    let mut attempted = Vec::new();

    let summary = Reconciler::delete_cleanup_pending_slot_batch(
        vec![timed_out.clone(), deleted.clone()],
        |slot| {
            attempted.push(slot.id);
            async move {
                if slot.id == timed_out.id {
                    return super::timed_io(
                        "test-cleanup-timeout",
                        Duration::from_millis(1),
                        async {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            Ok::<bool, std::io::Error>(true)
                        },
                    )
                    .await
                    .map_err(Into::into);
                }

                Ok(true)
            }
        },
    )
    .await;

    assert_eq!(attempted, vec![timed_out.id, deleted.id]);
    assert_eq!(summary.deleted_cleanup_pending_slots, 1);
}

#[tokio::test]
async fn stale_claimed_slot_batch_attempts_remaining_slots_after_delete_failure() {
    let failed = claimed_slot(
        "019e6040-4a23-7472-bf54-d1feb6f0d201",
        "019e6040-5356-72a0-a1da-c3360aa8d43b",
        "space-sbx-failed-g1",
    );
    let skipped = claimed_slot(
        "019e6040-59c0-7551-866f-f5f39196c79d",
        "019e6040-601a-7196-bef6-d2762c6a9a60",
        "space-sbx-skipped-g1",
    );
    let deleted = claimed_slot(
        "019e6040-68c4-75b5-9817-6a76d6922058",
        "019e6040-6e6f-72fc-8420-2f71aef7c7a7",
        "space-sbx-deleted-g1",
    );
    let mut outcomes: VecDeque<Result<bool, super::WarmPoolError>> = VecDeque::from([
        Err(super::WarmPoolError::io(std::io::Error::other(
            "k8s delete timed out",
        ))),
        Ok(false),
        Ok(true),
    ]);
    let mut attempted = Vec::new();

    let summary = Reconciler::delete_stale_claimed_slot_batch(
        vec![failed.clone(), skipped.clone(), deleted.clone()],
        |slot| {
            attempted.push(slot.id);
            let result = outcomes.pop_front().expect("one outcome per slot");
            async move { result }
        },
    )
    .await;

    assert!(outcomes.is_empty());
    assert_eq!(attempted, vec![failed.id, skipped.id, deleted.id]);
    assert_eq!(summary.deleted_stale_claimed_slots, 1);
}

#[tokio::test]
async fn reap_stale_claimed_slot_runs_recheck_then_pod_delete_then_row_finalize() {
    let events = std::cell::RefCell::new(Vec::new());

    let deleted = Reconciler::reap_stale_claimed_slot(
        || async {
            events.borrow_mut().push("recheck");
            Ok(true)
        },
        || async {
            events.borrow_mut().push("delete-pod");
            Ok(())
        },
        || async {
            events.borrow_mut().push("finalize-row");
            Ok(true)
        },
    )
    .await
    .expect("reap succeeds");

    assert!(deleted);
    assert_eq!(
        *events.borrow(),
        vec!["recheck", "delete-pod", "finalize-row"],
    );
}

#[tokio::test]
async fn reap_stale_claimed_slot_skips_pod_delete_when_recheck_fails() {
    let deleted = Reconciler::reap_stale_claimed_slot(
        || async { Ok(false) },
        || async { panic!("pod delete must not run when the recheck fails") },
        || async { panic!("row finalize must not run when the recheck fails") },
    )
    .await
    .expect("skipped reap is not an error");

    assert!(!deleted);
}

#[tokio::test]
async fn reap_stale_claimed_slot_keeps_row_when_pod_delete_fails() {
    let error = Reconciler::reap_stale_claimed_slot(
        || async { Ok(true) },
        || async {
            Err(super::WarmPoolError::io(std::io::Error::other(
                "k8s api unavailable",
            )))
        },
        || async { panic!("row finalize must not run when the pod delete fails") },
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("k8s api unavailable"));
}

#[tokio::test]
async fn reap_stale_claimed_slot_reports_skip_when_locked_verify_fails() {
    let deleted = Reconciler::reap_stale_claimed_slot(
        || async { Ok(true) },
        || async { Ok(()) },
        || async { Ok(false) },
    )
    .await
    .expect("skipped finalize is not an error");

    assert!(!deleted);
}

#[test]
fn template_write_request_targets_exact_pool_release_digest() {
    let config = pool_config(10, 3, 5);

    let request = template_write_request(&config);

    assert_eq!(request.to_string(), format!("seed@{}", config.content_hash));
}

#[test]
fn template_write_node_list_params_filters_to_sandbox_nodes() {
    let pod_config = pool_pod_config();

    let params = template_write_node_list_params(&pod_config);

    assert_eq!(
        params.label_selector.as_deref(),
        Some("space.pplx.net/node-pool=workers")
    );
}

#[test]
fn cached_warm_pods_by_name_filters_to_pool_config() {
    let config = pool_config(10, 3, 5);
    let mut other_config = pool_config(10, 3, 5);
    other_config.id = Uuid::from_str("019e6040-0f0f-7296-b521-a1043e0ff49b").unwrap();
    let config_id = config.id.to_string();
    let other_config_id = other_config.id.to_string();
    let (pod_store, mut pod_writer) = reflector::store::<SandboxPodView>();
    pod_writer.apply_watcher_event(&Event::Init);
    pod_writer.apply_watcher_event(&Event::InitApply(SandboxPodView::from(pod_with_labels(
        "matching-warm",
        [
            (SANDBOX_POOL_ROLE_KEY, SANDBOX_POOL_ROLE_WARM),
            (SANDBOX_POOL_CONFIG_ID_KEY, config_id.as_str()),
        ],
    ))));
    pod_writer.apply_watcher_event(&Event::InitApply(SandboxPodView::from(pod_with_labels(
        "other-config-warm",
        [
            (SANDBOX_POOL_ROLE_KEY, SANDBOX_POOL_ROLE_WARM),
            (SANDBOX_POOL_CONFIG_ID_KEY, other_config_id.as_str()),
        ],
    ))));
    pod_writer.apply_watcher_event(&Event::InitApply(SandboxPodView::from(pod_with_labels(
        "matching-config-non-warm",
        [
            (SANDBOX_POOL_ROLE_KEY, "claimed"),
            (SANDBOX_POOL_CONFIG_ID_KEY, config_id.as_str()),
        ],
    ))));
    pod_writer.apply_watcher_event(&Event::InitDone);

    let pods = cached_warm_pods_by_name(&pod_store, &config);

    assert!(pods.contains_key("matching-warm"));
    assert!(!pods.contains_key("other-config-warm"));
    assert!(!pods.contains_key("matching-config-non-warm"));
}

#[test]
fn cached_cordoned_node_names_filters_reflector_state() {
    let (node_store, mut node_writer) = reflector::store::<Node>();
    node_writer.apply_watcher_event(&Event::Init);
    node_writer.apply_watcher_event(&Event::InitApply(node_with_name_and_unschedulable(
        "cordoned-worker",
        Some(true),
    )));
    node_writer.apply_watcher_event(&Event::InitApply(node_with_name_and_unschedulable(
        "schedulable-worker",
        Some(false),
    )));
    node_writer.apply_watcher_event(&Event::InitApply(node_with_name_and_unschedulable(
        "unset-worker",
        None,
    )));
    node_writer.apply_watcher_event(&Event::InitDone);

    let names = cached_cordoned_node_names(&node_store);

    assert_eq!(names, HashSet::from(["cordoned-worker".to_string()]));
}

#[test]
fn matching_node_without_template_presence_needs_template_write_annotation() {
    let config = pool_config(10, 3, 5);
    let pod_config = pool_pod_config();
    let request = template_write_request(&config);
    let annotation_key = template_write_annotation_key_for(&request);
    let annotation_value = request.to_string();
    let presence_label = template_build_presence_label_key(
        config.template_name.as_str(),
        &config.content_hash.to_string(),
    );
    let node = node_with_metadata(
        [(
            pod_config.sandbox_node_selector_key.as_str(),
            pod_config.sandbox_node_selector_value.as_str(),
        )],
        [],
    );

    assert!(needs_template_write_annotation(
        &node,
        &pod_config,
        &presence_label,
        &annotation_key,
        &annotation_value,
    ));
}

#[test]
fn node_with_template_presence_does_not_need_template_write_annotation() {
    let config = pool_config(10, 3, 5);
    let pod_config = pool_pod_config();
    let request = template_write_request(&config);
    let annotation_key = template_write_annotation_key_for(&request);
    let annotation_value = request.to_string();
    let presence_label = template_build_presence_label_key(
        config.template_name.as_str(),
        &config.content_hash.to_string(),
    );
    let node = node_with_metadata(
        [
            (
                pod_config.sandbox_node_selector_key.as_str(),
                pod_config.sandbox_node_selector_value.as_str(),
            ),
            (&presence_label, TEMPLATE_PRESENT_LABEL_VALUE),
        ],
        [],
    );

    assert!(!needs_template_write_annotation(
        &node,
        &pod_config,
        &presence_label,
        &annotation_key,
        &annotation_value,
    ));
}

#[test]
fn node_with_existing_matching_template_write_annotation_is_current() {
    let config = pool_config(10, 3, 5);
    let pod_config = pool_pod_config();
    let request = template_write_request(&config);
    let annotation_key = template_write_annotation_key_for(&request);
    let annotation_value = request.to_string();
    let presence_label = template_build_presence_label_key(
        config.template_name.as_str(),
        &config.content_hash.to_string(),
    );
    let node = node_with_metadata(
        [(
            pod_config.sandbox_node_selector_key.as_str(),
            pod_config.sandbox_node_selector_value.as_str(),
        )],
        [(annotation_key.as_str(), annotation_value.as_str())],
    );

    assert!(!needs_template_write_annotation(
        &node,
        &pod_config,
        &presence_label,
        &annotation_key,
        &annotation_value,
    ));
}

#[test]
fn unready_warming_pod_deletion_keeps_young_pending_pods() {
    let slot = pool_slot_created_at(OffsetDateTime::now_utc());
    let pod = pod_with_phase("Pending");

    let should_delete = should_delete_unready_warming_pod(
        &SandboxPodView::from(pod.clone()),
        &slot,
        super::DEFAULT_WARMING_SLOT_TIMEOUT,
    );

    assert!(!should_delete);
}

#[test]
fn unready_warming_pod_deletion_removes_old_pending_pods() {
    let slot = pool_slot_created_at(OffsetDateTime::now_utc() - TimeDuration::minutes(11));
    let pod = pod_with_phase("Pending");

    let should_delete = should_delete_unready_warming_pod(
        &SandboxPodView::from(pod.clone()),
        &slot,
        super::DEFAULT_WARMING_SLOT_TIMEOUT,
    );

    assert!(should_delete);
}

#[test]
fn unready_warming_pod_deletion_removes_failed_pods() {
    let slot = pool_slot_created_at(OffsetDateTime::now_utc());
    let pod = pod_with_phase("Failed");

    let should_delete = should_delete_unready_warming_pod(
        &SandboxPodView::from(pod.clone()),
        &slot,
        super::DEFAULT_WARMING_SLOT_TIMEOUT,
    );

    assert!(should_delete);
}

#[test]
fn reconcile_config_stage_or_continue_marks_failure_and_returns_empty_work() {
    let config = pool_config(10, 3, 5);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let reconciler = reconcile_stage_test_reconciler(&runtime);

    let summary = runtime.block_on(reconciler.reconcile_config_stage_or_continue(
        &config,
        "observe_warming_slots",
        async {
            Err(super::WarmPoolError::io(std::io::Error::other(
                "transient stage failure",
            )))
        },
    ));

    assert_eq!(
        summary,
        ReconcileSummary {
            failed_required_operations: 1,
            ..Default::default()
        }
    );
    assert!(!summary.completed_required_operations());
}

fn pool_config(
    target_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
) -> PoolConfig {
    pool_config_with_id_and_limits(
        "019e5f3d-9f9f-7e90-9fdb-c944972bbff5",
        "seed",
        target_ready_slots,
        refill_per_tick,
        max_warming_slots,
    )
}

fn pool_config_with_id(id: &str, template_name: &str) -> PoolConfig {
    pool_config_with_id_and_limits(id, template_name, 10, 3, 5)
}

fn pool_config_with_id_and_limits(
    id: &str,
    template_name: &str,
    target_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
) -> PoolConfig {
    let now = OffsetDateTime::now_utc();
    PoolConfig {
        id: Uuid::from_str(id).unwrap(),
        template_release_id: Uuid::from_str("019e5f3d-a31d-7442-9075-85f654c148f7").unwrap(),
        template_name: TemplateName::parse(template_name).unwrap(),
        content_hash: ContentHash::new(Digest::sha256([0x11; 32])),
        vcpu: VcpuCount::try_from_u32(2).unwrap(),
        memory: MemorySize::mib(1024).unwrap(),
        disk_mib: None,
        disk_io_limits: Default::default(),
        target_ready_slots,
        refill_per_tick,
        max_warming_slots,
        namespace: "space".to_string(),
        enabled: true,
        encryptable: false,
        managed_by: pplx_space_cluster_pool::PoolConfigOwner::Manual,
        created_at: now,
        updated_at: now,
    }
}

/// A rogue-enabled encryptable config (only reachable out of band today) must
/// drain, not fill: members would boot plaintext and be claimable only by
/// encrypted creates.
#[test]
fn target_open_slots_is_zero_for_encryptable_configs() {
    let mut config = pool_config(5, 1, 2);
    assert_eq!(target_open_slots(&config), 5);

    config.encryptable = true;
    assert_eq!(target_open_slots(&config), 0);
}

fn pool_pod_config() -> SandboxPodConfigValues {
    SandboxPodConfigValues {
        sandbox_image: DEFAULT_SANDBOX_POD_IMAGE.to_string(),
        runtime_class_name: DEFAULT_SANDBOX_RUNTIME_CLASS.to_string(),
        sandbox_node_selector_key: "space.pplx.net/node-pool".to_string(),
        sandbox_node_selector_value: "workers".to_string(),
        extra_node_selector_labels: BTreeMap::new(),
        internal_secret_name: DEFAULT_SANDBOX_INTERNAL_SECRET_NAME.to_string(),
        internal_token_key: DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY.to_string(),
        internal_control_token_key: DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY.to_string(),
        ..SandboxPodConfigValues::default()
    }
}

fn node_with_metadata<'a>(
    labels: impl IntoIterator<Item = (&'a str, &'a str)>,
    annotations: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Node {
    Node {
        metadata: ObjectMeta {
            name: Some("worker-1".to_string()),
            labels: Some(
                labels
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<BTreeMap<_, _>>(),
            ),
            annotations: Some(
                annotations
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<BTreeMap<_, _>>(),
            ),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn pod_with_labels<'a>(name: &str, labels: impl IntoIterator<Item = (&'a str, &'a str)>) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(
                labels
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<BTreeMap<_, _>>(),
            ),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn pool_slot_created_at(created_at: OffsetDateTime) -> PoolSlot {
    PoolSlot {
        id: Uuid::from_str("019e5f46-5374-7e42-b1e3-a604b80a4a62").unwrap(),
        config_id: Uuid::from_str("019e5f46-5889-76d1-af26-815258e35f40").unwrap(),
        template_release_id: Uuid::from_str("019e5f46-5ae8-7123-82e3-4273cc150730").unwrap(),
        reserved_sandbox_id: SandboxId::from_uuid(
            Uuid::from_str("019e5f46-5d30-7700-baf8-5a774c185a67").unwrap(),
        ),
        template_name: TemplateName::parse("seed").unwrap(),
        content_hash: ContentHash::new(Digest::sha256([0x11; 32])),
        vcpu: VcpuCount::try_from_u32(2).unwrap(),
        memory: MemorySize::mib(1024).unwrap(),
        disk_mib: None,
        disk_io_limits: Default::default(),
        pod_name: "space-sbx-test-g1".to_string(),
        pod_namespace: "space".to_string(),
        pod_uid: None,
        pod_ip: None,
        k8s_node_name: None,
        state: pplx_space_cluster_pool::PoolSlotState::Warming,
        claim_id: None,
        ready_at: None,
        claimed_at: None,
        created_at,
        updated_at: created_at,
    }
}

fn cleanup_pending_slot(slot_id: &str, sandbox_id: &str, pod_name: &str) -> PoolSlot {
    let mut slot = pool_slot_created_at(OffsetDateTime::now_utc());
    slot.id = Uuid::from_str(slot_id).unwrap();
    slot.reserved_sandbox_id = SandboxId::from_uuid(Uuid::from_str(sandbox_id).unwrap());
    slot.pod_name = pod_name.to_string();
    slot.state = pplx_space_cluster_pool::PoolSlotState::CleanupPending;
    slot
}

fn claimed_slot(slot_id: &str, sandbox_id: &str, pod_name: &str) -> PoolSlot {
    let mut slot = pool_slot_created_at(OffsetDateTime::now_utc());
    slot.id = Uuid::from_str(slot_id).unwrap();
    slot.reserved_sandbox_id = SandboxId::from_uuid(Uuid::from_str(sandbox_id).unwrap());
    slot.pod_name = pod_name.to_string();
    slot.state = pplx_space_cluster_pool::PoolSlotState::Claimed;
    slot
}

fn pod_with_phase(phase: &str) -> Pod {
    Pod {
        metadata: ObjectMeta::default(),
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn running_ready_pod(uid: &str, pod_ip: &str, node_name: &str) -> Pod {
    Pod {
        metadata: ObjectMeta {
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some(node_name.to_string()),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some(pod_ip.to_string()),
            conditions: Some(vec![ready_condition("True")]),
            ..Default::default()
        }),
    }
}

fn ready_condition(status: &str) -> PodCondition {
    PodCondition {
        type_: "Ready".to_string(),
        status: status.to_string(),
        ..Default::default()
    }
}

struct FakeReadyProbe {
    started: bool,
}

impl WarmSlotReadyProbe for FakeReadyProbe {
    async fn slot_started(
        &self,
        _slot: &PoolSlot,
        _metadata: &crate::pod::ReadyPodMetadata,
        _content_hash: &str,
    ) -> bool {
        self.started
    }
}

fn node_with_unschedulable(unschedulable: Option<bool>) -> Node {
    Node {
        spec: Some(k8s_openapi::api::core::v1::NodeSpec {
            unschedulable,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn node_with_name_and_unschedulable(name: &str, unschedulable: Option<bool>) -> Node {
    let mut node = node_with_unschedulable(unschedulable);
    node.metadata.name = Some(name.to_string());
    node
}

#[test]
fn node_is_cordoned_true_only_when_unschedulable_set() {
    assert!(super::node_is_cordoned(&node_with_unschedulable(Some(
        true
    ))));
}

#[test]
fn node_is_cordoned_false_when_schedulable_unset_or_no_spec() {
    // Explicit `false`, absent field, and a missing spec entirely all
    // mean schedulable — warm capacity must NOT be evicted off them.
    assert!(!super::node_is_cordoned(&node_with_unschedulable(Some(
        false
    ))));
    assert!(!super::node_is_cordoned(&node_with_unschedulable(None)));
    assert!(!super::node_is_cordoned(&Node::default()));
}

#[tokio::test]
async fn create_warming_slots_bounds_in_flight_concurrency() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const CONCURRENCY: usize = 4;
    const CREATE_COUNT: usize = 20;
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak_in_flight = Arc::new(AtomicUsize::new(0));

    let results = super::create_warming_slots(CREATE_COUNT, CONCURRENCY, || {
        let in_flight = Arc::clone(&in_flight);
        let peak_in_flight = Arc::clone(&peak_in_flight);
        async move {
            let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            peak_in_flight.fetch_max(current, Ordering::SeqCst);
            // Yield so the scheduler fills the buffer before any create
            // completes, exposing the true peak concurrency.
            tokio::task::yield_now().await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok::<(), super::WarmPoolError>(())
        }
    })
    .await;

    assert_eq!(results.len(), CREATE_COUNT);
    assert!(results.iter().all(Result::is_ok));
    // More work than the bound, so concurrency saturates at exactly the
    // bound and never exceeds it.
    assert_eq!(peak_in_flight.load(Ordering::SeqCst), CONCURRENCY);
}

#[tokio::test]
async fn create_permits_bound_in_flight_creates_across_config_batches() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    const CREATE_COUNT: usize = 10;
    const PER_CONFIG_CONCURRENCY: usize = 10;
    const GLOBAL_CONCURRENCY: usize = 3;
    let permits = Arc::new(Semaphore::new(GLOBAL_CONCURRENCY));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak_in_flight = Arc::new(AtomicUsize::new(0));

    let run_batch = || {
        let permits = Arc::clone(&permits);
        let in_flight = Arc::clone(&in_flight);
        let peak_in_flight = Arc::clone(&peak_in_flight);
        async move {
            super::create_warming_slots(CREATE_COUNT, PER_CONFIG_CONCURRENCY, || {
                let permits = Arc::clone(&permits);
                let in_flight = Arc::clone(&in_flight);
                let peak_in_flight = Arc::clone(&peak_in_flight);
                async move {
                    super::with_create_permit(permits.as_ref(), async {
                        let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        peak_in_flight.fetch_max(current, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        Ok::<(), super::WarmPoolError>(())
                    })
                    .await
                }
            })
            .await
        }
    };

    let (left, right) = tokio::join!(run_batch(), run_batch());

    assert_eq!(left.len() + right.len(), CREATE_COUNT * 2);
    assert!(left.iter().chain(right.iter()).all(Result::is_ok));
    assert_eq!(peak_in_flight.load(Ordering::SeqCst), GLOBAL_CONCURRENCY);
}

#[tokio::test]
async fn create_warming_slots_returns_every_result_on_partial_failure() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let attempts = Arc::new(AtomicUsize::new(0));
    let results = super::create_warming_slots(6, 3, || {
        let attempts = Arc::clone(&attempts);
        async move {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            if n.is_multiple_of(2) {
                return Err(super::WarmPoolError::io(std::io::Error::other(format!(
                    "slot {n} failed"
                ))));
            }
            Ok(n)
        }
    })
    .await;

    // A failing slot is surfaced as an Err, not propagated: the whole
    // batch still runs and every outcome comes back for the caller to
    // aggregate.
    assert_eq!(results.len(), 6);
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 3);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 3);
}

#[tokio::test]
async fn create_warming_slots_does_nothing_when_count_is_zero() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let results = super::create_warming_slots(0, 8, || {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), super::WarmPoolError>(())
        }
    })
    .await;

    assert!(results.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reconcile_configs_runs_configs_concurrently_and_isolates_failures() {
    use std::sync::Arc;
    use tokio::sync::{Notify, mpsc};

    let slow = pool_config_with_id("019e6040-1111-7111-8111-111111111111", "slow-template");
    let failed = pool_config_with_id("019e6040-2222-7222-8222-222222222222", "failed-template");
    let fast = pool_config_with_id("019e6040-3333-7333-8333-333333333333", "fast-template");
    let slow_id = slow.id;
    let failed_id = failed.id;
    let fast_id = fast.id;
    let slow_started = Arc::new(Notify::new());
    let release_slow = Arc::new(Notify::new());
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();

    let handle = tokio::spawn({
        let slow_started = Arc::clone(&slow_started);
        let release_slow = Arc::clone(&release_slow);
        async move {
            super::reconcile_configs(vec![slow, failed, fast], 2, move |config| {
                let slow_started = Arc::clone(&slow_started);
                let release_slow = Arc::clone(&release_slow);
                let completed_tx = completed_tx.clone();
                async move {
                    if config.id == slow_id {
                        slow_started.notify_one();
                        release_slow.notified().await;
                        completed_tx.send(config.id).unwrap();
                        return Ok(ReconcileSummary {
                            created_slots: 1,
                            ..Default::default()
                        });
                    }

                    completed_tx.send(config.id).unwrap();
                    if config.id == failed_id {
                        return Err(super::WarmPoolError::io(std::io::Error::other(
                            "config-local failure",
                        )));
                    }

                    Ok(ReconcileSummary {
                        marked_ready_slots: 1,
                        ..Default::default()
                    })
                }
            })
            .await
        }
    });

    slow_started.notified().await;
    let completed_before_slow = [completed_rx.recv().await, completed_rx.recv().await];
    assert_eq!(
        completed_before_slow
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .unwrap(),
        vec![failed_id, fast_id]
    );

    release_slow.notify_one();
    let results = handle.await.unwrap();
    let mut summary = ReconcileSummary::default();
    let mut failed_configs = Vec::new();
    for config_result in results {
        let (config, result) = config_result.into_parts();
        match result {
            Ok(config_summary) => summary += config_summary,
            Err(_) => failed_configs.push(config.id),
        }
    }

    assert_eq!(completed_rx.recv().await, Some(slow_id));
    assert_eq!(failed_configs, vec![failed_id]);
    assert_eq!(
        summary,
        ReconcileSummary {
            created_slots: 1,
            marked_ready_slots: 1,
            ..Default::default()
        }
    );
}

/// Inside the cadence interval the receipt backstops must be a pure no-op —
/// the fixture's lazy pool never connects, so any Postgres round trip here
/// fails the test.
#[tokio::test]
async fn receipt_backstops_are_noops_inside_the_cadence_interval() {
    let reconciler = reconcile_stage_test_reconciler_inner();
    {
        let mut state = reconciler.receipt_backstops.lock().unwrap();
        state.last_run = Some(Instant::now());
        state.dead_claimed_lap = Some(BackstopLap {
            cursor: Uuid::now_v7(),
            high_water: Uuid::now_v7(),
        });
    }

    let summary = reconciler
        .sweep_receipt_backstops()
        .await
        .expect("gated sweep must not touch the database");

    assert_eq!(summary, ReconcileSummary::default());
    let state = reconciler.receipt_backstops.lock().unwrap();
    assert!(
        state.dead_claimed_lap.is_some(),
        "a gated tick must not move the sweep lap"
    );
}

#[tokio::test]
async fn warming_slots_without_node_identity_still_timeout() -> TestResult {
    let db = pg_test_db().await?;
    let store = WarmPoolStore::new(db.pool.clone());
    let template = TemplateName::parse("warming-timeout")?;
    let release = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x51; 32]));
    seed_pool_config(&db.pool, &template, release, config_id, content_hash).await?;
    let deletes = Arc::new(AtomicUsize::new(0));
    let count = deletes.clone();
    let kube = ClientBuilder::new(
        tower::service_fn(move |request: Request<KubeBody>| {
            let count = count.clone();
            async move {
                assert_eq!(request.method(), Method::DELETE);
                count.fetch_add(1, Ordering::SeqCst);
                kubernetes_json_response(
                    StatusCode::OK,
                    &serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Success"}),
                )
            }
        }),
        "space",
    )
    .build();
    let mut reconciler = reconcile_test_reconciler(kube, store.clone(), db.pool.clone());
    let (pods, mut writer) = reflector::store();
    for (name, node_name) in [
        ("warming-unscheduled", None),
        ("warming-cache-miss", Some("missing-node")),
    ] {
        let slot = store
            .insert_warming_slot(NewPoolSlot {
                config_id,
                template_release_id: release,
                reserved_sandbox_id: SandboxId::new(),
                template_name: &template,
                content_hash,
                vcpu: VcpuCount::try_from_u32(2)?,
                memory: MemorySize::mib(1024)?,
                pod_name: name,
                pod_namespace: "space",
            })
            .await?;
        sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots SET created_at=NOW()-INTERVAL '1 hour' WHERE id=$1"
        ))
        .bind(slot.id)
        .execute(&db.pool)
        .await?;
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "metadata": {"name":name,"namespace":"space","uid":format!("uid-{name}"),"labels": {
                SANDBOX_POOL_ROLE_KEY: SANDBOX_POOL_ROLE_WARM, SANDBOX_POOL_CONFIG_ID_KEY:config_id.to_string()
            }}, "spec":{"nodeName":node_name,"containers":[]}, "status":{"phase":"Pending"}
        }))?;
        writer.apply_watcher_event(&Event::Apply(SandboxPodView::from(pod)));
    }
    reconciler.pod_cache.store = pods;
    let config = store
        .reconcilable_configs()
        .await?
        .into_iter()
        .find(|c| c.id == config_id)
        .expect("config");
    let summary = reconciler.observe_warming_slots(&config).await?;
    assert_eq!(summary.deleted_unready_pods, 2);
    assert_eq!(summary.marked_ready_slots, 0);
    assert_eq!(deletes.load(Ordering::SeqCst), 2);
    assert_eq!(store.slot_counts(config_id).await?.warming, 0);
    Ok(())
}

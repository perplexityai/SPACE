//! A `failed` sandbox whose node reports its VM stopped, while its pod still
//! looks Running with an IP that nothing answers on: the forwarding in the
//! pod's network namespace that reaches the node-api is gone with the VM. The
//! failed-cleanup pass sends the retire's stop back to the node-api the probe
//! reached by node name, so the retire completes instead of failing on every
//! tick.
//!
//! Runs the production sink over the production Kubernetes substrate. The
//! node-api is a double serving the node-api simulator's worker API on its
//! own `127.0.0.1` port, advertised by the node's node-api pod; the sandbox
//! pod's proxy port is one nothing listens on.
//!
//! An execution that names no node is `missing_on_node` without a probe, and
//! there is no node-api to send its stop back to; its retire keeps the pod
//! route, which answers there.

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_node_api_client::proto::worker_api_server::WorkerApiServer;
use pplx_space_node_api_sim::state::status;
use pplx_space_node_api_sim::{SimState, SimWorkerApiService};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

use super::*;
use crate::lifecycle_writer_test_support::{
    CLOSED_PORT, KUBERNETES_NAMESPACE, SandboxSeed, enter_node_api_client_process, insert_sandbox,
    kubernetes_substrate_recording_requests,
};

/// The Kubernetes node the sandbox's pod runs on.
const WORKER_NODE: &str = "node-a";

/// The node-api double, stopped on drop, and the gRPC paths it was called on.
struct NodeApi {
    port: u16,
    state: SimState,
    calls: Arc<Mutex<Vec<String>>>,
    shutdown: CancellationToken,
    _state_dir: tempfile::TempDir,
}

impl Drop for NodeApi {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl NodeApi {
    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .expect("node-api call log poisoned")
            .clone()
    }
}

async fn start_node_api() -> TestResult<NodeApi> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let state_dir = tempfile::tempdir()?;
    let state = SimState::new(
        state_dir.path().join("snapshots"),
        state_dir.path().join("sandboxes"),
        WORKER_NODE.to_owned(),
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&calls);
    let shutdown = CancellationToken::new();
    let stopped = shutdown.clone().cancelled_owned();
    let service = WorkerApiServer::new(SimWorkerApiService::new(state.clone()));
    tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .trace_fn(move |request| {
                recorded
                    .lock()
                    .expect("node-api call log poisoned")
                    .push(request.uri().path().to_owned());
                tracing::Span::none()
            })
            .add_service(service)
            .serve_with_incoming_shutdown(
                TcpIncoming::from(listener).with_nodelay(Some(true)),
                stopped,
            )
            .await
        {
            tracing::warn!(%error, "node-api double exited with an error");
        }
    });
    Ok(NodeApi {
        port,
        state,
        calls,
        shutdown,
        _state_dir: state_dir,
    })
}

/// The node's ready node-api pod, advertising the double's port.
fn node_api_pod(port: u16) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": format!("space-worker-{WORKER_NODE}"),
            "namespace": KUBERNETES_NAMESPACE,
            "uid": "node-api-pod-uid",
            "labels": { "app.kubernetes.io/name": "space-worker" },
            "annotations": { "space.pplx.net/node-api-port": port.to_string() },
        },
        "spec": { "nodeName": WORKER_NODE, "containers": [] },
        "status": {
            "phase": "Running",
            "podIP": Ipv4Addr::LOCALHOST.to_string(),
            "conditions": [{ "type": "Ready", "status": "True" }],
        },
    })
}

/// A `failed` row placed on the worker node, its running generation-1
/// execution at `127.0.0.1` naming `execution_node`, and the Running pod on
/// the worker node that backs it. Returns the pod's name and object.
async fn insert_failed_resident(
    pool: &PgPool,
    sandbox_id: SandboxId,
    execution_node: Option<&str>,
) -> TestResult<(String, serde_json::Value)> {
    let node_id = pplx_space_common_svc::types::k8s::space_k8s_node_id(WORKER_NODE, "worker");
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1'::INET, 'worker', 'k8s', 'ready')
         ON CONFLICT (id) DO NOTHING"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new("failed", "k8s").on_node(node_id),
    )
    .await?;
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let pod_uid = format!("{pod_name}-uid");
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
              k8s_node_name, state, active)
         VALUES ($1, 1, $2, $3, $4, '127.0.0.1'::INET, $5, 'running', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .bind(KUBERNETES_NAMESPACE)
    .bind(&pod_uid)
    .bind(execution_node)
    .execute(pool)
    .await?;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": KUBERNETES_NAMESPACE,
            "uid": pod_uid,
            "resourceVersion": "1",
        },
        "spec": { "nodeName": WORKER_NODE, "containers": [] },
        "status": {
            "phase": "Running",
            "podIP": Ipv4Addr::LOCALHOST.to_string(),
            "podIPs": [{ "ip": Ipv4Addr::LOCALHOST.to_string() }],
        },
    });
    Ok((pod_name, pod))
}

async fn execution_active(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT active FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The retire stops the sandbox and enqueues its stopped-disk backup through
/// the node's node-api, deletes the pod and releases the execution, all on
/// the first tick, although the pod's own IP refuses every connection.
#[tokio::test(flavor = "current_thread")]
async fn a_retire_the_node_permits_completes_although_the_pod_ip_is_dead() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    let (pod_name, pod) = insert_failed_resident(pool, sandbox_id, Some(WORKER_NODE)).await?;
    let node_api = start_node_api().await?;
    // Known to the node and not running: the probe reads `dead_on_node`.
    node_api
        .state
        .ensure_default_running(&sandbox_id.to_string())
        .await?;
    node_api
        .state
        .set_status(&sandbox_id.to_string(), status::STOPPED)
        .await?;
    let (substrate, apiserver_requests) = kubernetes_substrate_recording_requests(
        pool,
        vec![pod, node_api_pod(node_api.port)],
        CLOSED_PORT,
    )?;
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            enabled: false,
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        SubstrateSweepSink::new(
            pool.clone(),
            Arc::new(Substrate::from_kubernetes(substrate)),
        ),
    );

    let report = Box::pin(reconciler.tick()).await;

    assert_eq!(
        (
            report.failed_cleanup_attempts,
            report.failed_cleanup_failures
        ),
        (1, 0),
        "{report:?}"
    );
    assert!(
        !execution_active(pool, sandbox_id).await?,
        "the retire must release the execution"
    );
    let calls = node_api.calls();
    for rpc in [
        "/space.node.api.v1.WorkerApi/StopSandbox",
        "/space.node.api.v1.WorkerApi/EnqueueSandboxArtifactBackup",
    ] {
        assert!(
            calls.iter().any(|call| call == rpc),
            "the node-api must receive {rpc}: {calls:?}"
        );
    }
    let pod_delete = format!("DELETE /api/v1/namespaces/{KUBERNETES_NAMESPACE}/pods/{pod_name}");
    assert!(
        apiserver_requests
            .lock()
            .expect("apiserver request log poisoned")
            .contains(&pod_delete),
        "the retire must delete the pod"
    );
    Ok(())
}

/// An execution that names no node is retired through its pod's IP, which
/// reaches the node-api here: it is never probed, so no node answered, and a
/// stop asked of the node route would have no node-api to go to.
#[tokio::test(flavor = "current_thread")]
async fn a_retire_of_an_execution_that_names_no_node_stops_through_the_pod() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    let (pod_name, pod) = insert_failed_resident(pool, sandbox_id, None).await?;
    let node_api = start_node_api().await?;
    node_api
        .state
        .ensure_default_running(&sandbox_id.to_string())
        .await?;
    // The pod's IP and proxy port reach the node-api, as the forwarding in a
    // live pod's network namespace does.
    let (substrate, apiserver_requests) = kubernetes_substrate_recording_requests(
        pool,
        vec![pod, node_api_pod(node_api.port)],
        node_api.port,
    )?;
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            enabled: false,
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        SubstrateSweepSink::new(
            pool.clone(),
            Arc::new(Substrate::from_kubernetes(substrate)),
        ),
    );

    let report = Box::pin(reconciler.tick()).await;

    assert_eq!(
        (
            report.failed_cleanup_attempts,
            report.failed_cleanup_failures
        ),
        (1, 0),
        "{report:?}"
    );
    assert!(
        !execution_active(pool, sandbox_id).await?,
        "the retire must release the execution"
    );
    // Nothing else, the probe included, reached the node-api.
    assert_eq!(
        node_api.calls(),
        [
            "/space.node.api.v1.WorkerApi/StopSandbox",
            "/space.node.api.v1.WorkerApi/EnqueueSandboxArtifactBackup",
        ],
        "the pod route must deliver the stop and the backup"
    );
    let pod_delete = format!("DELETE /api/v1/namespaces/{KUBERNETES_NAMESPACE}/pods/{pod_name}");
    assert!(
        apiserver_requests
            .lock()
            .expect("apiserver request log poisoned")
            .contains(&pod_delete),
        "the retire must delete the pod"
    );
    Ok(())
}

/// A node-route retire of an execution that names no node fails before the
/// pod delete and the tracking release, and says that is why.
#[tokio::test(flavor = "current_thread")]
async fn a_node_route_retire_without_a_node_says_the_execution_names_none() -> TestResult {
    use pplx_space_api::controllers::substrate::{
        RetireExecutionRequest, SubstrateAdapter, SubstrateError,
    };

    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    let (pod_name, pod) = insert_failed_resident(pool, sandbox_id, None).await?;
    let pod_uid = pod["metadata"]["uid"]
        .as_str()
        .expect("the pod has a uid")
        .to_owned();
    let node_api = start_node_api().await?;
    let (substrate, apiserver_requests) = kubernetes_substrate_recording_requests(
        pool,
        vec![pod, node_api_pod(node_api.port)],
        node_api.port,
    )?;
    let fence = PodIdentityFence::new(PodName::parse(&pod_name)?)
        .with_pod_uid(PodUid::parse(&pod_uid)?)
        .with_generation(1);

    let error = Box::pin(substrate.retire_execution(RetireExecutionRequest {
        sandbox_id,
        fence: RetireFence::Pod(fence),
        stop_route: RetireStopRoute::Node,
    }))
    .await
    .expect_err("there is no node to stop the execution through");

    let SubstrateError::Failed { message } = &error else {
        panic!("unexpected retire error {error:?}");
    };
    assert!(
        message.contains("the execution names no node"),
        "the error must name the cause: {message}"
    );
    assert!(
        execution_active(pool, sandbox_id).await?,
        "the execution must stay tracked for retry"
    );
    assert!(
        !apiserver_requests
            .lock()
            .expect("apiserver request log poisoned")
            .iter()
            .any(|request| request.starts_with("DELETE ")),
        "the pod must not be deleted"
    );
    assert!(node_api.calls().is_empty(), "{:?}", node_api.calls());
    Ok(())
}

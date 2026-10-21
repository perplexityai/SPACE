//! Characterization of the evacuation restore's finalize, node drain's
//! `restoring` → target write to Postgres `sandboxes.lifecycle`, through
//! [`NodeDrainSink::restore_evacuated_sandbox`] on a sink over the production
//! Kubernetes substrate.
//!
//! The restore is a resident one: the marked `suspended` row still has its
//! running execution and pod, so the substrate claims the row `restoring` and
//! wakes the pod through its node-api, and no pod is created. The node-api is
//! a double serving the node-api simulator's worker API and a Wake that
//! succeeds. The Wake unplaces the row, so the finalize's own placement write
//! shows, and optionally lands another writer's lifecycle on it, which is the
//! window the finalize's guard closes.
//!
//! Each case's double listens on a port of its own on `127.0.0.1`, and its
//! substrate dials the pod's sandbox proxy on that port, where the double
//! serves the worker APIs.

use std::ffi::OsStr;
use std::net::Ipv4Addr;

use pplx_space_common_grpc::internal_transport::INSECURE_DEV_ENV;
use pplx_space_common_svc::bearer_token::{
    SPACE_NODE_API_INTERNAL_TOKEN_PATH_ENV, SPACE_NODE_API_TOKEN_PATH_ENV,
};
use pplx_space_node_api_client::proto::worker_api_internal_server::{
    WorkerApiInternal, WorkerApiInternalServer,
};
use pplx_space_node_api_client::proto::worker_api_server::WorkerApiServer;
use pplx_space_node_api_sim::{SimState, SimWorkerApiService};
use pplx_space_sbx_mgr_client::proto::{
    AdoptPoolSandboxRequest, AdoptPoolSandboxResponse, WakeRequest, WakeResponse, WakeTransition,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use uuid::Uuid;

use super::*;
use crate::lifecycle_writer_test_support::{KUBERNETES_NAMESPACE, kubernetes_substrate_with_pods};

/// The Kubernetes node the resident's pod runs on.
const WORKER_NODE: &str = "node-a";

/// A Wake that succeeds, after unplacing the row and landing
/// `concurrent_lifecycle` on it when set, and reports the route identity it
/// was asked to publish as current on `node_id`.
struct SucceedingWake {
    pool: PgPool,
    node_id: NodeId,
    concurrent_lifecycle: Option<&'static str>,
}

#[tonic::async_trait]
impl WorkerApiInternal for SucceedingWake {
    async fn adopt_pool_sandbox(
        &self,
        _request: tonic::Request<AdoptPoolSandboxRequest>,
    ) -> Result<tonic::Response<AdoptPoolSandboxResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "a restore adopts no pool sandbox",
        ))
    }

    async fn wake_sandbox(
        &self,
        request: tonic::Request<WakeRequest>,
    ) -> Result<tonic::Response<WakeResponse>, tonic::Status> {
        let request = request.into_inner();
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET placement = 'unplaced',
                    lifecycle = COALESCE($2, lifecycle)
              WHERE id = $1::uuid"
        ))
        .bind(&request.sandbox_id)
        .bind(self.concurrent_lifecycle)
        .execute(&self.pool)
        .await
        .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let route_identity = request
            .start_context
            .and_then(|context| context.route_identity)
            .map(|mut identity| {
                identity.current_node_id = Some(self.node_id.to_string());
                identity
            });
        Ok(tonic::Response::new(WakeResponse {
            sandbox_id: request.sandbox_id,
            transition: WakeTransition::Noop as i32,
            route_identity,
        }))
    }
}

/// The node-api double, stopped on drop.
struct NodeApi {
    port: u16,
    shutdown: CancellationToken,
    _state_dir: tempfile::TempDir,
}

impl Drop for NodeApi {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn start_node_api(wake: SucceedingWake) -> TestResult<NodeApi> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let state_dir = tempfile::tempdir()?;
    let state = SimState::new(
        state_dir.path().join("snapshots"),
        state_dir.path().join("sandboxes"),
        WORKER_NODE.to_owned(),
    );
    let (health, health_service) = pplx_space_common_svc::grpc_health_service();
    for service in [
        pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE,
        pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
    ] {
        health
            .set_service_status(service, tonic_health::ServingStatus::Serving)
            .await;
    }
    let shutdown = CancellationToken::new();
    let stopped = shutdown.clone().cancelled_owned();
    tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .add_service(health_service)
            .add_service(WorkerApiServer::new(SimWorkerApiService::new(state)))
            .add_service(WorkerApiInternalServer::new(wake))
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
        shutdown,
        _state_dir: state_dir,
    })
}

/// Re-run the calling test in a child process whose environment lets the
/// substrate's node-api client reach the double: plaintext internal RPC, and
/// node-api token paths naming files this parent writes and keeps until the
/// child exits, so the token reader never falls back to the host's token
/// files. That client reads both from the process environment, as in
/// production. Call it first, and return when it returns `false`: the child
/// has then run the test.
fn enter_node_api_client_process() -> TestResult<bool> {
    let tokens = tempfile::tempdir()?;
    let node_api_token = tokens.path().join("node-api.token");
    let node_api_internal_token = tokens.path().join("node-api-internal.token");
    std::fs::write(&node_api_token, "synthetic-internal-token")?;
    std::fs::write(&node_api_internal_token, "synthetic-internal-control-token")?;
    Ok(pplx_space_test_env::enter_test_process(&[
        (INSECURE_DEV_ENV, Some(OsStr::new("true"))),
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

/// A marked `suspended` row resident on the worker node at `pod_ip`, with a
/// ready full snapshot, its running generation-1 execution and the Running
/// pod that backs it. Returns the pod object.
async fn insert_resident_suspended(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    pod_ip: Ipv4Addr,
) -> TestResult<serde_json::Value> {
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new("suspended", "k8s")
            .on_node(node_id)
            .marked("paused"),
    )
    .await?;
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3,
                 (SELECT lifecycle_changed_at + INTERVAL '1 second'
                    FROM sandboxes WHERE id = $2))"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!(
        "s3://synthetic-bucket/{snapshot_id}?region=us-east-1"
    ))
    .execute(pool)
    .await?;
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    let pod_uid = format!("{pod_name}-uid");
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active,
              route_activation_id)
         VALUES ($1, 1, $2, $3, $4, $5::INET, 'running', TRUE, $6)"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .bind(KUBERNETES_NAMESPACE)
    .bind(&pod_uid)
    .bind(pod_ip.to_string())
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": KUBERNETES_NAMESPACE,
            "uid": pod_uid,
            "resourceVersion": "1",
        },
        "status": {
            "phase": "Running",
            "podIP": pod_ip.to_string(),
            "podIPs": [{ "ip": pod_ip.to_string() }],
        },
    }))
}

/// The finalize moves the restored row to its target, `on_node` and
/// unmarked, only while the row is still `restoring`: a lifecycle another
/// writer lands during the Wake, from every stored value, stands, unplaced as
/// the Wake left it, and the restore reports the lost finalize CAS.
#[tokio::test(flavor = "current_thread")]
async fn restore_finalize_moves_only_a_row_still_restoring() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = pplx_space_common_svc::types::k8s::space_k8s_node_id(WORKER_NODE, "worker");
    let timeout = PositiveDuration::new(Duration::from_secs(10)).expect("non-zero");

    let mut mismatches = Vec::new();
    for concurrent_lifecycle in [None]
        .into_iter()
        .chain(StoredSandboxLifecycle::all_as_str().map(Some))
    {
        let pod_ip = Ipv4Addr::LOCALHOST;
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, $2::INET, 'worker', 'k8s', 'ready')
             ON CONFLICT (id) DO UPDATE SET address = EXCLUDED.address"
        ))
        .bind(node_id)
        .bind(pod_ip.to_string())
        .execute(pool)
        .await?;
        let sandbox_id = SandboxId::new();
        let pod = insert_resident_suspended(pool, sandbox_id, node_id, pod_ip).await?;
        let node_api = start_node_api(SucceedingWake {
            pool: pool.clone(),
            node_id,
            concurrent_lifecycle,
        })
        .await?;
        let substrate = kubernetes_substrate_with_pods(pool, vec![pod], node_api.port)?;
        let sink = SubstrateSink::new(
            pool.clone(),
            Arc::new(substrate),
            ResidencyEvidence::NodeCensusAware,
            true,
            true,
        );

        let result = Box::pin(sink.restore_evacuated_sandbox(
            sandbox_id,
            SandboxLifecycle::Paused,
            timeout,
            None,
        ))
        .await;

        let during = concurrent_lifecycle.unwrap_or("restoring");
        let finalizes = during == "restoring";
        let expected = if finalizes {
            Row::new("paused", None, Some(node_id))
        } else {
            Row {
                placement: "unplaced".to_owned(),
                ..Row::new(
                    during,
                    keeps_marker(during).then_some("paused"),
                    Some(node_id),
                )
            }
        };
        let outcome_matches = match &result {
            Ok(RestoreOutcome::Restored) => finalizes,
            Err(EvacuateError::Restore(message)) => {
                !finalizes && message.starts_with("post-restore finalize CAS missed")
            }
            _ => false,
        };
        let actual = row(pool, sandbox_id).await?;
        if !outcome_matches || actual != expected {
            let outcome = match &result {
                Ok(RestoreOutcome::Restored) => "restored".to_owned(),
                Ok(_) => "deferred or converging".to_owned(),
                Err(error) => error.to_string(),
            };
            mismatches.push(format!(
                "landed {concurrent_lifecycle:?}: {outcome}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

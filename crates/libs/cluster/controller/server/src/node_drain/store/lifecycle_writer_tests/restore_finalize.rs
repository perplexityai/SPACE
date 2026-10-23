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

use pplx_space_common_core::kubernetes::{
    SANDBOX_CHECKPOINT_ANNOTATION, SANDBOX_STOPPED_MATERIALIZATION_ARTIFACT_ID_ANNOTATION,
    SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION, STOPPED_MATERIALIZATION_CHECKPOINT,
};

use super::*;
use crate::lifecycle_writer_test_support::{
    KUBERNETES_NAMESPACE, kubernetes_substrate_creating_pods, kubernetes_substrate_with_pods,
};
use crate::node_drain::{InstanceTerminator, Reconciler, ReconcilerConfig, TerminateError};

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

/// A Wake answered the way sandbox-manager answers a stopped target it has
/// materialized: success, with no route identity, because nothing was
/// launched. Every request is
/// kept for the test to inspect. A Wake for a sandbox in `stalled` is never
/// answered, the way a destination that can't materialize it never confirms.
#[derive(Default)]
struct StoppedWake {
    requests: Arc<std::sync::Mutex<Vec<WakeRequest>>>,
    stalled: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

#[tonic::async_trait]
impl WorkerApiInternal for StoppedWake {
    async fn adopt_pool_sandbox(
        &self,
        _request: tonic::Request<AdoptPoolSandboxRequest>,
    ) -> Result<tonic::Response<AdoptPoolSandboxResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "a re-placement adopts no pool sandbox",
        ))
    }

    async fn wake_sandbox(
        &self,
        request: tonic::Request<WakeRequest>,
    ) -> Result<tonic::Response<WakeResponse>, tonic::Status> {
        let request = request.into_inner();
        let sandbox_id = request.sandbox_id.clone();
        self.requests.lock().expect("wake requests").push(request);
        if self
            .stalled
            .lock()
            .expect("stalled wakes")
            .contains(&sandbox_id)
        {
            std::future::pending::<()>().await;
        }
        Ok(tonic::Response::new(WakeResponse {
            sandbox_id,
            transition: WakeTransition::Restore as i32,
            route_identity: None,
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

async fn start_node_api(wake: impl WorkerApiInternal) -> TestResult<NodeApi> {
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

/// A forced drain's released stopped evacuation is placed `stopped` on another
/// node by the restore sweep, through the production Kubernetes substrate:
/// the claim takes the row, a pod is created for it whose shim materializes
/// the drain's pinned disk artifact without a VM, the node confirms it through
/// a stopped Wake, and the finalize settles the row `stopped`, `on_node` and
/// unmarked.
///
/// What the pod and the Wake carry is the property the shim and
/// sandbox-manager depend on: the stopped-materialization checkpoint, the
/// stopped target, the artifact by URI and by id, no source node, and no
/// network namespace. And the node answering with no route identity, as a real
/// stopped materialization does, must still count as success. The shim's half
/// runs in neither double; its own tests and the kind scenario
/// `native.forced_drain_re_places_stopped_sandbox` cover it.
#[tokio::test(flavor = "current_thread")]
async fn a_released_stopped_evacuation_is_re_placed_stopped() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let StoppedReplacement {
        sandbox_id,
        pin,
        storage_uri,
        sink,
        created_pods,
        requests,
        _node_api,
        ..
    } = released_stopped_evacuation(pool, &[]).await?;

    let outcome = Box::pin(sink.restore_evacuated_sandbox(
        sandbox_id,
        SandboxLifecycle::Stopped,
        PositiveDuration::new(Duration::from_secs(10)).expect("non-zero"),
        None,
    ))
    .await?;

    assert!(
        matches!(outcome, RestoreOutcome::Restored),
        "the re-placement must report a completed restore"
    );
    let row: (String, String, Option<String>) = sqlx::query_as(sql!(
        "SELECT lifecycle, placement, evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        row,
        ("stopped".to_owned(), "on_node".to_owned(), None),
        "the re-placed sandbox must settle stopped, placed and unmarked"
    );
    // Nothing was activated, so no node receipt will release the attempt the
    // re-placement claimed; left set, it refuses the owner's next `start`.
    let attempts: Vec<Option<Uuid>> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        attempts,
        vec![None],
        "the settled re-placement must leave its execution with no activation attempt"
    );
    let created_pods = created_pods.lock().expect("created pods").clone();
    let [pod] = created_pods.as_slice() else {
        panic!(
            "expected exactly one created pod, got {}",
            created_pods.len()
        );
    };
    // What the pod's shim acts on: materialize exactly this artifact, and boot
    // nothing. Any other checkpoint would have the shim start a restore.
    let annotations = &pod["metadata"]["annotations"];
    assert_eq!(
        (
            annotations[SANDBOX_CHECKPOINT_ANNOTATION].as_str(),
            annotations[SANDBOX_STOPPED_MATERIALIZATION_ARTIFACT_ID_ANNOTATION].as_str(),
            annotations[SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION].as_str(),
        ),
        (
            Some(STOPPED_MATERIALIZATION_CHECKPOINT),
            Some(pin.to_string().as_str()),
            Some(storage_uri.as_str()),
        ),
        "the re-placement pod must ask its shim to materialize the pinned artifact"
    );
    // A sandbox with no earlier execution has no node to keep the pod off.
    assert!(
        excluded_hosts(pod).iter().all(Vec::is_empty),
        "a re-placement with no prior host must not exclude any node: {:?}",
        excluded_hosts(pod)
    );
    let requests = requests.lock().expect("wake requests").clone();
    let [request] = requests.as_slice() else {
        panic!("expected exactly one Wake, got {}", requests.len());
    };
    assert_eq!(
        request.target_state,
        pplx_space_sbx_mgr_client::proto::WakeTargetState::Stopped as i32,
        "the node must be asked to leave the sandbox stopped"
    );
    let context = request
        .start_context
        .as_ref()
        .expect("a stopped Wake must carry its start context");
    assert_eq!(
        (
            context.suspend_restore_storage_uri.as_deref(),
            context.expected_local_artifact_id.clone(),
            context.suspend_restore_source_node.as_deref(),
            context.netns_path.as_deref(),
        ),
        (
            Some(storage_uri.as_str()),
            Some(pin.to_string()),
            None,
            None
        ),
        "the node must be sent the pinned artifact by URI and id, and no source node"
    );
    Ok(())
}

/// A stopped re-placement is kept off every node an earlier execution of the
/// sandbox ran on, because sandbox-manager there may hold a `stopped` row
/// pinned to another artifact, which it refuses to materialize over. The
/// excluded nodes are exactly the recorded hosts of retired executions; one
/// whose pod never bound records none.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_re_placement_is_kept_off_every_prior_host() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let StoppedReplacement {
        sandbox_id,
        sink,
        created_pods,
        _node_api,
        ..
    } = released_stopped_evacuation(pool, &[Some("node-z"), None, Some("node-y")]).await?;

    let outcome = Box::pin(sink.restore_evacuated_sandbox(
        sandbox_id,
        SandboxLifecycle::Stopped,
        PositiveDuration::new(Duration::from_secs(10)).expect("non-zero"),
        None,
    ))
    .await?;

    assert!(
        matches!(outcome, RestoreOutcome::Restored),
        "a worker that isn't a prior host must take the re-placement"
    );
    let created_pods = created_pods.lock().expect("created pods").clone();
    let [pod] = created_pods.as_slice() else {
        panic!(
            "expected exactly one created pod, got {}",
            created_pods.len()
        );
    };
    let excluded = excluded_hosts(pod);
    assert!(!excluded.is_empty(), "the pod must carry required terms");
    for term in excluded {
        assert_eq!(
            term,
            vec!["node-y".to_owned(), "node-z".to_owned()],
            "every required term must exclude each prior host, and only those"
        );
    }
    Ok(())
}

/// With every worker a prior host, nothing can take the re-placement. The pod
/// is `Unschedulable`, which the restore reports as a failure: the sweep backs
/// it off and, after repeated failures, names it at ERROR, instead of
/// re-driving it every tick as a converging restore. The row stays a claimed
/// re-placement, so the pending gauge counts it while the backoff keeps it out
/// of the sweep, and a retry reads the same pod rather than creating another.
/// Nothing reaches the node: no Wake is sent.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placement_every_worker_excludes_waits_unschedulable_and_backs_off() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let StoppedReplacement {
        sandbox_id,
        sink,
        created_pods,
        requests,
        _node_api,
        ..
    } = released_stopped_evacuation(pool, &[Some(WORKER_NODE)]).await?;
    let restore = || {
        Box::pin(sink.restore_evacuated_sandbox(
            sandbox_id,
            SandboxLifecycle::Stopped,
            PositiveDuration::new(Duration::from_secs(10)).expect("non-zero"),
            None,
        ))
    };

    let first = restore().await;
    assert!(
        first
            .as_ref()
            .is_err_and(|error| error.to_string().contains("unschedulable")),
        "an unschedulable re-placement must fail, not converge: {}",
        describe(&first)
    );
    let row: (String, Option<String>) = sqlx::query_as(sql!(
        "SELECT lifecycle, evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        row,
        ("restoring".to_owned(), Some("stopped".to_owned())),
        "the claim must stand, so the re-placement is still owned and counted"
    );

    let attempts = sink
        .schedule_restore_retry(
            sandbox_id,
            PositiveDuration::new(Duration::from_secs(60)).expect("non-zero"),
            PositiveDuration::new(Duration::from_secs(1800)).expect("non-zero"),
        )
        .await?;
    assert_eq!(attempts, Some(1), "the failure must arm the backoff");
    let sweep = restore_pending_evacuations_query(true);
    let swept: Vec<(SandboxId, String)> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(sweep.as_str())))
        .fetch_all(pool)
        .await?;
    assert!(
        !swept.iter().any(|(id, _)| *id == sandbox_id),
        "the backoff must keep the re-placement out of the next sweep: {swept:?}"
    );
    let pending: i64 = sqlx::query_scalar(stopped_replacements_pending_query())
        .fetch_one(pool)
        .await?;
    assert_eq!(
        pending, 1,
        "the pending gauge must count the stuck re-placement"
    );

    let retried = restore().await;
    assert!(
        retried.is_err(),
        "still unschedulable: {}",
        describe(&retried)
    );
    assert_eq!(
        created_pods.lock().expect("created pods").len(),
        1,
        "a retry must read the unschedulable pod, not create another"
    );
    assert!(
        requests.lock().expect("wake requests").is_empty(),
        "an unscheduled pod has no node to send a Wake to"
    );
    Ok(())
}

/// A re-placement every worker excludes waits `Unschedulable`, and its retry
/// comes after the activation lease its first attempt took has run out, so
/// the retry owns the execution again rather than joining that attempt. It
/// must still be a stopped re-placement: the retry doesn't delete and replace
/// the pod the first attempt created, which asks the shim to materialize the
/// pinned artifact and keeps off every prior host. The replacement a re-drive
/// would build is an ordinary restore pod carrying neither.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placement_retried_after_its_lease_expires_stays_a_stopped_re_placement() -> TestResult
{
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let StoppedReplacement {
        sandbox_id,
        pin,
        storage_uri,
        sink,
        created_pods,
        _node_api,
        ..
    } = released_stopped_evacuation(pool, &[Some(WORKER_NODE)]).await?;
    let restore = || {
        Box::pin(sink.restore_evacuated_sandbox(
            sandbox_id,
            SandboxLifecycle::Stopped,
            PositiveDuration::new(Duration::from_secs(10)).expect("non-zero"),
            None,
        ))
    };

    let first = restore().await;
    assert!(
        first
            .as_ref()
            .is_err_and(|error| error.to_string().contains("sandbox pod is unschedulable")),
        "fixture precondition: with every worker excluded the first attempt waits \
         unschedulable: {}",
        describe(&first)
    );
    let expired = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active AND activation_attempt_id IS NOT NULL"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    assert_eq!(
        expired.rows_affected(),
        1,
        "fixture precondition: the first attempt left its activation attempt to expire"
    );

    let attempt_before: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;

    let retried = restore().await;
    let attempt_after: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert!(
        attempt_after.is_some() && attempt_after != attempt_before,
        "the retry must take an activation attempt of its own rather than join the expired \
         one: {attempt_before:?} -> {attempt_after:?}"
    );
    let pods = created_pods.lock().expect("created pods").clone();
    assert_eq!(
        pods.len(),
        1,
        "the retry must keep the re-placement's pod, not replace it with another: {:?}",
        pods.iter()
            .map(|pod| &pod["metadata"]["annotations"])
            .collect::<Vec<_>>()
    );
    for pod in &pods {
        let annotations = &pod["metadata"]["annotations"];
        assert_eq!(
            (
                annotations[SANDBOX_CHECKPOINT_ANNOTATION].as_str(),
                annotations[SANDBOX_STOPPED_MATERIALIZATION_ARTIFACT_ID_ANNOTATION].as_str(),
                annotations[SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION].as_str(),
            ),
            (
                Some(STOPPED_MATERIALIZATION_CHECKPOINT),
                Some(pin.to_string().as_str()),
                Some(storage_uri.as_str()),
            ),
            "every pod the re-placement creates must stay a stopped materialization of the \
             pinned artifact: {}",
            pod["metadata"]["name"]
        );
        let excluded = excluded_hosts(pod);
        assert!(
            !excluded.is_empty()
                && excluded
                    .iter()
                    .all(|term| term == &vec![WORKER_NODE.to_owned()]),
            "every pod must keep off the prior host in every required term: {excluded:?}"
        );
    }
    assert!(
        retried
            .as_ref()
            .is_err_and(|error| error.to_string().contains("sandbox pod is unschedulable")),
        "the retry still waits unschedulable rather than becoming something else: {}",
        describe(&retried)
    );
    Ok(())
}

/// A stopped re-placement whose destination never confirms it (a shim or
/// sandbox-manager that can't materialize it) must not keep the cluster's
/// restore budget from every other evacuation. With one slot, it takes the slot
/// for one wait; the failure backs it off, and on the next sweep the slot goes
/// to the started evacuation queued behind it.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_re_placement_that_never_confirms_backs_off_out_of_the_only_slot() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let StoppedReplacement {
        sandbox_id: stuck,
        sink,
        created_pods,
        requests,
        stalled_wakes,
        _node_api,
        ..
    } = released_stopped_evacuation(pool, &[]).await?;
    stalled_wakes
        .lock()
        .expect("stalled wakes")
        .insert(stuck.to_string());
    let started = SandboxId::new();
    insert_released_suspended_evacuation(pool, started, "started").await?;
    // The stuck re-placement became the sweep's responsibility most recently,
    // so it leads the never-failed tier and takes the only slot first.
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() + INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(stuck)
    .execute(pool)
    .await?;
    let listed = sink.list_restore_pending_evacuations(true).await?;
    assert_eq!(
        listed,
        vec![
            (stuck, SandboxLifecycle::Stopped),
            (started, SandboxLifecycle::Started)
        ],
        "fixture precondition: both are offered, the stuck re-placement first"
    );

    let reconciler = Reconciler::new(
        ReconcilerConfig {
            max_per_cluster: 1,
            evacuate_timeout: PositiveDuration::new(Duration::from_secs(1)).expect("non-zero"),
            ..ReconcilerConfig::default()
        },
        sink,
        UnusedTerminator,
    );
    let dispatched = |pods: &[serde_json::Value], sandbox_id: SandboxId| {
        let prefix = format!("space-sbx-{}-", sandbox_id.as_uuid().simple());
        pods.iter()
            .filter(|pod| {
                pod["metadata"]["name"]
                    .as_str()
                    .is_some_and(|name| name.starts_with(&prefix))
            })
            .count()
    };

    assert_eq!(reconciler.run_restore_sweep(false).await, 1);
    reconciler.drain_evacuations(Duration::from_secs(30)).await;
    let pods = created_pods.lock().expect("created pods").clone();
    assert_eq!(
        (dispatched(&pods, stuck), dispatched(&pods, started)),
        (1, 0),
        "the first sweep's only slot goes to the stuck re-placement"
    );
    let backoff: (i32, bool) = sqlx::query_as(sql!(
        "SELECT restore_attempt_count, COALESCE(next_restore_attempt_at > NOW(), FALSE)
           FROM sandboxes WHERE id = $1"
    ))
    .bind(stuck)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        backoff,
        (1, true),
        "a wait the destination never confirms is a failure, so it arms the backoff"
    );

    assert_eq!(reconciler.run_restore_sweep(false).await, 1);
    reconciler.drain_evacuations(Duration::from_secs(30)).await;
    let pods = created_pods.lock().expect("created pods").clone();
    assert_eq!(
        dispatched(&pods, started),
        1,
        "the next sweep's slot goes to the started evacuation behind it"
    );
    let stuck_wakes = requests
        .lock()
        .expect("wake requests")
        .iter()
        .filter(|request| request.sandbox_id == stuck.to_string())
        .count();
    assert_eq!(
        stuck_wakes, 1,
        "and the stuck re-placement isn't re-driven inside its backoff window"
    );
    Ok(())
}

/// A re-placement slow enough to outlast one wait is retried, and the retry
/// joins the activation attempt the timed-out wait left inside its lease. Once
/// the node confirms it, the sandbox must be left as an owned success leaves
/// it: no attempt on its execution, so the materialization pod can be released
/// and the owner's next `start` isn't refused until the lease runs out.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placement_that_succeeds_on_a_retry_releases_the_attempt_it_joined() -> TestResult {
    if !enter_node_api_client_process()? {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let StoppedReplacement {
        sandbox_id,
        sink,
        requests,
        stalled_wakes,
        _node_api,
        ..
    } = released_stopped_evacuation(pool, &[]).await?;
    let restore = || {
        Box::pin(sink.restore_evacuated_sandbox(
            sandbox_id,
            SandboxLifecycle::Stopped,
            PositiveDuration::new(Duration::from_secs(1)).expect("non-zero"),
            None,
        ))
    };
    let live_attempts = || async {
        sqlx::query_scalar::<_, bool>(sql!(
            "SELECT activation_attempt_id IS NOT NULL
                    AND COALESCE(activation_attempt_expires_at > NOW(), TRUE)
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_all(pool)
        .await
    };

    stalled_wakes
        .lock()
        .expect("stalled wakes")
        .insert(sandbox_id.to_string());
    let first = restore().await;
    assert!(
        first.is_err(),
        "a wait the node doesn't answer in time fails: {}",
        describe(&first)
    );
    assert_eq!(
        live_attempts().await?,
        vec![true],
        "fixture precondition: the timed-out wait leaves its attempt inside its lease, so the \
         retry joins it rather than taking its own"
    );
    let left_behind: Uuid = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;

    stalled_wakes.lock().expect("stalled wakes").clear();
    let retried = restore().await;
    assert!(
        matches!(retried, Ok(RestoreOutcome::Restored)),
        "the retry must complete the re-placement: {}",
        describe(&retried)
    );
    let wake_attempts: Vec<Vec<String>> = requests
        .lock()
        .expect("wake requests")
        .iter()
        .map(|request| {
            let context = request.start_context.as_ref();
            context
                .and_then(|context| context.route_identity.as_ref())
                .and_then(|identity| identity.activation_attempt_id.clone())
                .into_iter()
                .chain(context.and_then(|context| context.operation_activation_attempt_id.clone()))
                .collect()
        })
        .collect();
    assert_eq!(
        wake_attempts.len(),
        2,
        "fixture precondition: the retry sent its own Wake"
    );
    assert!(
        wake_attempts
            .iter()
            .all(|ids| !ids.is_empty() && ids.iter().all(|id| *id == left_behind.to_string())),
        "fixture precondition: both Wakes ran under the attempt the timed-out wait left, so \
         the retry joined it: {wake_attempts:?} vs {left_behind}"
    );
    assert_eq!(
        live_attempts().await?,
        vec![false],
        "the settled re-placement must leave no attempt on its execution, joined or owned"
    );
    Ok(())
}

/// Never called: the restore sweep terminates no instance.
struct UnusedTerminator;

impl InstanceTerminator for UnusedTerminator {
    async fn terminate(&self, node_id: NodeId) -> Result<(), TerminateError> {
        panic!("the restore sweep terminated node {node_id}")
    }
}

/// A suspend evacuation whose source node is gone: `suspended`, marked for
/// `target`, on no node and with no execution, with a ready full snapshot of
/// the current suspend in S3.
async fn insert_released_suspended_evacuation(
    pool: &PgPool,
    sandbox_id: SandboxId,
    target: &'static str,
) -> TestResult {
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new("suspended", "k8s").marked(target),
    )
    .await?;
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp)
         VALUES ($1, $2, 'full', 'ready',
                 (SELECT lifecycle_changed_at + INTERVAL '1 second'
                    FROM sandboxes WHERE id = $2))"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
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
    Ok(())
}

/// A released stopped evacuation and a sink to re-place it with, through the
/// production Kubernetes substrate against an apiserver double with one
/// worker, [`WORKER_NODE`].
struct StoppedReplacement {
    sandbox_id: SandboxId,
    pin: LocalArtifactId,
    storage_uri: String,
    sink: SubstrateSink,
    created_pods: crate::lifecycle_writer_test_support::CreatedPods,
    requests: Arc<std::sync::Mutex<Vec<WakeRequest>>>,
    /// Sandbox ids whose Wake the node-api double never answers.
    stalled_wakes: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    _node_api: NodeApi,
}

/// What a stopped evacuation's teardown leaves: `stopped`, marked, on no node,
/// pinning the disk artifact whose ready copy is in S3. `prior_hosts` seeds
/// one retired execution per entry, in generation order, recording the node
/// its pod ran on (`None` for one that never bound).
async fn released_stopped_evacuation(
    pool: &PgPool,
    prior_hosts: &[Option<&str>],
) -> TestResult<StoppedReplacement> {
    let pod_ip = Ipv4Addr::LOCALHOST;
    let node_id = pplx_space_common_svc::types::k8s::space_k8s_node_id(WORKER_NODE, "worker");
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .bind(pod_ip.to_string())
    .execute(pool)
    .await?;

    let sandbox_id = SandboxId::new();
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new("stopped", "k8s").marked("stopped"),
    )
    .await?;
    for (generation, host) in (1_i64..).zip(prior_hosts) {
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, k8s_node_name,
                  state, active, terminated_at)
             VALUES ($1, $2, $3, $4, $5, $6, 'terminated', FALSE, NOW())"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(format!(
            "space-sbx-{}-g{generation}",
            sandbox_id.as_uuid().simple()
        ))
        .bind(KUBERNETES_NAMESPACE)
        .bind(format!("uid-prior-{generation}"))
        .bind(*host)
        .execute(pool)
        .await?;
    }
    let pin = LocalArtifactId::new();
    let storage_uri = format!("s3://synthetic-bucket/{}?region=us-east-1", Uuid::now_v7());
    pplx_space_db_test_fixtures::publish_ready_disk_artifact(pool, sandbox_id, pin, &storage_uri)
        .await?;

    let wake = StoppedWake::default();
    let requests = Arc::clone(&wake.requests);
    let stalled_wakes = Arc::clone(&wake.stalled);
    let node_api = start_node_api(wake).await?;
    let (substrate, created_pods) =
        kubernetes_substrate_creating_pods(pool, node_api.port, pod_ip)?;
    let sink = SubstrateSink::new(
        pool.clone(),
        Arc::new(substrate),
        ResidencyEvidence::NodeCensusAware,
        true,
        true,
        true,
    );
    Ok(StoppedReplacement {
        sandbox_id,
        pin,
        storage_uri,
        sink,
        created_pods,
        requests,
        stalled_wakes,
        _node_api: node_api,
    })
}

/// The node names each required node-affinity term of `pod` excludes by a
/// `metadata.name NotIn`, sorted, one list per term (empty for a term that
/// excludes nothing); no lists when the pod has no required terms.
fn excluded_hosts(pod: &serde_json::Value) -> Vec<Vec<String>> {
    pod["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
        ["nodeSelectorTerms"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|term| {
            let mut names: Vec<String> = term["matchFields"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|field| field["key"] == "metadata.name" && field["operator"] == "NotIn")
                .flat_map(|field| field["values"].as_array().cloned().unwrap_or_default())
                .filter_map(|value| value.as_str().map(str::to_owned))
                .collect();
            names.sort();
            names
        })
        .collect()
}

fn describe(result: &Result<RestoreOutcome, EvacuateError>) -> String {
    match result {
        Ok(RestoreOutcome::Restored) => "restored".to_owned(),
        Ok(RestoreOutcome::Deferred) => "deferred".to_owned(),
        Ok(RestoreOutcome::Converging { detail, .. }) => format!("converging: {detail}"),
        Err(error) => error.to_string(),
    }
}

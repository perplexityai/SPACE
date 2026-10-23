//! The SPACE node controller: the per-node reconciler that keeps a node's
//! facets (network policies, sandbox and template desired state, outbound
//! inventory and status) converged with the cluster, and the Unix-socket gRPC
//! surface that lets local services trigger and inspect it.
//!
//! This crate is the whole service. The `space-nodectl-svc` binary is a thin
//! `main` that parses flags, initializes telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the service.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use pplx_space_common_svc::core::host::linux::identities::{group_names, user_names, users};
use pplx_space_common_svc::{
    AcquireOutcome, DEFAULT_DRAIN_TIMEOUT, LocalHealthServer, LocalRole, ServiceMutex,
    UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal,
};
use pplx_space_node_api_client::proto::cluster_api_client::ClusterApiClient;
use pplx_space_node_api_client::proto::node_ctl_server::{NodeCtl, NodeCtlServer};
use pplx_space_node_api_client::proto::{
    ClaimNodeReconcileRefsRequest, CompleteNodeReconcileRefsRequest, FacetControllerState,
    FacetDirection, FacetRef as ProtoFacetRef, FacetSyncOverride, FailedNodeReconcileRef,
    GetControllerStateRequest, GetControllerStateResponse, GetReconciliationStatusRequest,
    GetReconciliationStatusResponse, MarkFacetDirtyRequest, MarkFacetDirtyResponse, NodeCtlStatus,
    NodeRole, NodeSchedulingStatus, PublishedNodeStatus, ReconciliationStatus,
    RefreshFacetStateRequest, RefreshFacetStateResponse, SetFacetSyncDisabledRequest,
    SetFacetSyncDisabledResponse, TriggerStateReconciliationRequest,
    TriggerStateReconciliationResponse,
};
use pplx_space_node_mgr_client::{
    CacheDiskSchedulingEffect, NetworkPoolStatusReader, NodeResourceStatusReader,
};
use rusqlite::{Connection, params};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::Instrument as _;

mod cluster_reconcile_trigger;
mod dirty_state;
mod facets;
#[cfg(feature = "kubernetes")]
mod kubernetes_controller;
mod orphan_inventory;
mod util;

use cluster_reconcile_trigger::{ClusterReconcileTrigger, trigger_request_reason};

/// The group node-ctl's clients reach its production socket through.
pub const DEFAULT_NODE_CTL_SOCKET_GROUP: &str = group_names::SPACE_NODECTLCLIENTS;
const SERVICE_MUTEX_FILE_NAME: &str = "service.mutex";
const NETWORK_POLICY_RECONCILER: &str = "network-policies";
const SANDBOX_DESIRED_STATE_RECONCILER: &str = "sandbox-desired-state";
const TEMPLATE_BASE_SNAPSHOT_POLICY_RECONCILER: &str = "template-base-snapshot-policy";
const TEMPLATE_DESIRED_STATE_RECONCILER: &str = "template-desired-state";
const RUNTIME_PROVISIONING_RECONCILER: &str = "runtime-provisioning";
const NODE_STATE_OUTBOUND_RECONCILER: &str = "node-state-outbound";
const NETWORK_POLICY_OUTBOUND_RECONCILER: &str = "network-policy-outbound";
const SANDBOX_INVENTORY_OUTBOUND_RECONCILER: &str = "sandbox-inventory-outbound";
const TEMPLATE_INVENTORY_OUTBOUND_RECONCILER: &str = "template-inventory-outbound";
const NODE_STATUS_OUTBOUND_RECONCILER: &str = "node-status-outbound";
/// Where the binary looks for the node-ctl TOML config by default.
pub const DEFAULT_NODE_CTL_CONFIG_PATH: &str = "/space/config/nodectl.toml";
const DEFAULT_DISABLE_TTL_SECS: u64 = 10 * 60;
pub(crate) const DEFAULT_POLL_INTERVAL_SECS: u64 = 60;
const CLUSTER_RECONCILE_DRAIN_MAX_BATCHES: usize = 16;
const WORK_QUEUE_CAPACITY: usize = 1024;
const CLUSTER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
// A facet run wraps more than one cluster RPC's worth of work (scope
// resolution, the RPC, and per-sandbox local applies), so its watchdog needs
// headroom above the per-RPC deadline: a single in-deadline RPC must never be
// able to exhaust the whole facet budget on its own.
const FACET_RUN_TIMEOUT_MULTIPLIER: u32 = 3;

/// Per-RPC deadline for calls to cluster-api, applied at the client channel.
const CLUSTER_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Watchdog deadline for one whole facet reconcile job (inbound or outbound):
/// a multiple of the cluster RPC timeout so the two budgets stay consistent.
const FACET_RUN_TIMEOUT: Duration =
    Duration::from_secs(CLUSTER_RPC_TIMEOUT.as_secs() * FACET_RUN_TIMEOUT_MULTIPLIER as u64);

/// Upper bound on the number of per-item entries (currently sandbox ids in
/// network-policy state fetches) carried by a single cluster-api RPC. Bounding
/// the batch bounds both the response payload and the unit of work lost when
/// one RPC times out.
const CLUSTER_RPC_MAX_BATCH_SIZE: usize = 200;

/// Progress marker for one facet run. The facet touches it whenever it
/// completes a unit of work (e.g. one fetched-and-applied batch); the watchdog
/// treats time-since-last-touch, not total runtime, as the stall signal. A
/// multi-batch run on a large node therefore gets a budget that scales with
/// the work it is actually completing, while a wedged run (hung RPC or local
/// apply) is still detected within one quiet period.
#[derive(Clone)]
pub(crate) struct FacetProgress(Arc<Mutex<tokio::time::Instant>>);

impl FacetProgress {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(tokio::time::Instant::now())))
    }

    pub(crate) fn touch(&self) {
        *self.0.lock().expect("facet progress lock poisoned") = tokio::time::Instant::now();
    }

    fn last(&self) -> tokio::time::Instant {
        *self.0.lock().expect("facet progress lock poisoned")
    }
}

/// Resolves when `progress` has gone `quiet_period` without a touch.
async fn facet_stalled(progress: FacetProgress, quiet_period: Duration) {
    loop {
        let deadline = progress.last() + quiet_period;
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep_until(deadline).await;
    }
}

// Client-side max-age for the cluster-api channel. A cached gRPC channel is a
// single HTTP/2 connection pinned to one space-cluster-api pod for its whole
// life, so scaling out replicas never rebalances established clients —
// recovering a saturated pod used to require manually recycling it. Expiring
// the cached client makes new RPCs pick up a fresh connection (and a fresh
// load-balancing decision) within one max-age window. Jitter spreads
// reconnects so the node fleet doesn't stampede the cluster API in lockstep.
//
// Rotation only happens when a caller asks for the client, and the callers are
// sync loops polling every DEFAULT_POLL_INTERVAL_SECS — so the effective
// rotation time quantizes up to the next poll tick. The jitter must therefore
// span multiple poll intervals: a narrower jitter would round every node to
// the same tick and the whole fleet would rotate simultaneously. Spanning two
// poll intervals makes the rotation tick itself random, so client phases
// decohere further on every rotation.
const CLUSTER_CLIENT_MAX_AGE: Duration = Duration::from_secs(60);
const CLUSTER_CLIENT_MAX_AGE_JITTER: Duration = Duration::from_secs(2 * DEFAULT_POLL_INTERVAL_SECS);
const SPACE_INTERNAL_API_ENDPOINT_ENV: &str = "SPACE_INTERNAL_API_ENDPOINT";
const SPACE_NODE_ID_ENV: &str = "SPACE_NODE_ID";
const SPACE_NODE_ROLE_ENV: &str = "SPACE_NODE_ROLE";
const NODE_ID_PATH: &str = "/space/etc/node-id";
const SKIP_CLUSTER_SYNC_METADATA: &str = "x-space-skip-cluster-sync";
static NODE_CTL_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../../../db/node-ctl");
static CLUSTER_NODE_API_ENDPOINT: OnceLock<Option<String>> = OnceLock::new();
const ALLOW_NODE_CTL_OBSERVER: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
    UdsPeerPrincipal::Role(LocalRole::Observer),
];
const ALLOW_NODE_CTL_REFRESH: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Role(LocalRole::Operator),
];
const ALLOW_NODE_CTL_MARK_DIRTY: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
    UdsPeerPrincipal::Role(LocalRole::Operator),
];
const ALLOW_NODE_CTL_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
];

fn facet_scope_span_fields(scope: &facets::FacetScope) -> (&'static str, usize) {
    match scope {
        facets::FacetScope::Full => ("full", 0),
        facets::FacetScope::Items(items) => ("items", items.len()),
    }
}

/// The service name node-ctl reports to telemetry, names its health and
/// rate-limit surfaces with, and authorizes its Unix-socket peers under.
pub const SERVICE_NAME: &str = "space-node-ctl";

/// Everything [`run`] needs from the process that hosts it: the binary's flags,
/// already parsed. Environment-driven tuning (cluster endpoints, RPC timeouts,
/// batch sizes) stays with the code that reads it.
#[derive(Debug, Clone)]
pub struct NodeCtlRunConfig {
    /// Node-ctl TOML config path. A missing file is the default config.
    pub config_path: PathBuf,
    /// Production node-ctl Unix socket, bound only after the service mutex is
    /// acquired.
    pub socket_path: PathBuf,
    /// Instance-local health Unix socket, bound from startup so readiness can
    /// pass while this instance is still passive.
    pub health_socket_path: Option<PathBuf>,
    /// How long the passive instance waits for the predecessor to release the
    /// service mutex before exiting non-zero.
    pub service_mutex_timeout: Duration,
    /// The group the production socket must be given. Production uses
    /// [`DEFAULT_NODE_CTL_SOCKET_GROUP`]; tests name a group they belong to.
    pub socket_group: &'static str,
    /// Node-ctl's SQLite database.
    pub db_path: PathBuf,
    /// Reconciliation poll interval; `None` defers to the config file, then
    /// the default.
    pub poll_interval_secs: Option<u64>,
    /// SPACE node role when the node id is a substrate-local name; `None`
    /// defers to the environment, then the config file.
    pub role: Option<String>,
}

/// Why [`run`] returned without a clean shutdown.
#[derive(Debug, thiserror::Error)]
pub enum RunNodeCtlError {
    #[error("failed to read node-ctl config {path}: {source}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse node-ctl config {path}: {source}")]
    ParseConfig {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to read node id file {path}: {source}")]
    ReadNodeId {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("node id file {path} is empty")]
    EmptyNodeId { path: PathBuf },
    #[error("unsupported node role {role:?}; expected cluster, worker, or builder")]
    UnsupportedNodeRole { role: String },
    #[error("failed to bind the node-ctl health socket: {0}")]
    HealthSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to acquire the node-ctl service mutex: {0}")]
    ServiceMutex(#[source] pplx_space_common_svc::service_mutex::AcquireError),
    #[error("failed to migrate node-ctl's SQLite database: {0}")]
    Migrate(#[source] pplx_space_sqlite::SqliteError),
    #[error("failed to open node-ctl's SQLite database: {0}")]
    OpenDatabase(#[source] pplx_space_sqlite::SqliteError),
    #[error("failed to seed reconciliation status rows: {0}")]
    InitReconciliationStatus(#[source] rusqlite::Error),
    #[error("failed to bind the node-ctl socket: {0}")]
    BindSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    /// The worker returned before shutdown was requested: a dead per-node loop.
    #[error("node-ctl worker exited unexpectedly")]
    WorkerExited,
    #[error("node-ctl worker task failed: {0}")]
    WorkerTaskFailed(#[source] tokio::task::JoinError),
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct NodeCtlConfig {
    role: Option<String>,
    cluster: NodeCtlClusterConfig,
    reconciliation: NodeCtlReconciliationConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct NodeCtlClusterConfig {
    #[serde(alias = "endpoint", alias = "control_api_endpoint")]
    node_api_endpoint: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct NodeCtlReconciliationConfig {
    poll_interval_secs: Option<u64>,
}

#[derive(Clone)]
struct NodeCtlService {
    tx: mpsc::Sender<WorkItem>,
    cluster_reconcile_trigger: cluster_reconcile_trigger::ClusterReconcileTrigger,
    db: Arc<Mutex<Connection>>,
    peer_authorizer: UdsPeerAuthorizer,
}

#[derive(Debug, Clone)]
pub(crate) enum WorkItem {
    TimerTick { next_due_at: i64 },
    ManualRefresh { refs: Vec<dirty_state::FacetRef> },
    DrainClusterQueue,
    SyncDirty,
}

struct ClusterReconcileClaim {
    claim_token: String,
    refs: Vec<dirty_state::FacetRef>,
    /// The saved trace context of the writers that queued the refs, in the
    /// order the claim picked the refs.
    origins: Vec<String>,
    /// The canonical ids of the sandboxes the refs name.
    sandbox_ids: BTreeSet<String>,
}

/// The most sandbox ids a pass root lists.
const PASS_SANDBOX_IDS_MAX: usize = 16;

/// A count as a span attribute. tracing-opentelemetry records an unsigned
/// value as its debug string, so counts are recorded signed to stay numeric.
fn span_count(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// The root span of a node-ctl pass over one claim of cluster reconcile refs.
///
/// A claim whose refs saved the trace context of the writers that queued
/// them, a request's context included, runs under a root that links to those
/// writers, is sampled as they were, and parents the claim's facet
/// `node_ctl.reconcile` spans and its completion RPC. A claim of refs that no
/// traced writer queued is background work and runs under no root unless it
/// fails: whether it fails is known only once it has run, so its failure is
/// reported by a childless root that records the outcome. An idle trigger or
/// timer tick claims nothing and emits nothing.
struct ClusterPass;

impl ClusterPass {
    fn span(
        node_id: &str,
        cause: &'static str,
        refs: usize,
        sandbox_ids: &BTreeSet<String>,
    ) -> tracing::Span {
        let span = tracing::info_span!(
            parent: None,
            "node_ctl.pass",
            otel.kind = "internal",
            "operation.name" = "node_ctl.pass",
            cause,
            node_id = %node_id,
            refs = span_count(refs),
            failed = tracing::field::Empty,
            sandbox_id = tracing::field::Empty,
            sandbox_ids = tracing::field::Empty,
            sandbox_ids.dropped = tracing::field::Empty,
            origin_trace_id = tracing::field::Empty,
            origin_sampled = tracing::field::Empty,
            links.dropped = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
        );
        // One sandbox is the common case, and `sandbox_id` is the field every
        // sandbox's telemetry query uses; the capped list serves a pass over
        // several.
        if sandbox_ids.len() == 1
            && let Some(sandbox_id) = sandbox_ids.first()
        {
            span.record("sandbox_id", sandbox_id.as_str());
        }
        if !sandbox_ids.is_empty() {
            let listed = sandbox_ids
                .iter()
                .take(PASS_SANDBOX_IDS_MAX)
                .map(String::as_str)
                .collect::<Vec<_>>();
            span.record("sandbox_ids", listed.join(",").as_str());
        }
        if sandbox_ids.len() > PASS_SANDBOX_IDS_MAX {
            span.record(
                "sandbox_ids.dropped",
                span_count(sandbox_ids.len() - PASS_SANDBOX_IDS_MAX),
            );
        }
        span
    }

    fn record_outcome(span: &tracing::Span, refs: usize, failed: usize) {
        span.record("failed", span_count(failed));
        if failed > 0 {
            span.record("otel.status_code", "ERROR");
            span.record(
                "otel.status_description",
                format!("{failed} of {refs} claimed reconcile refs did not complete").as_str(),
            );
        }
    }
}

struct SchedulingSnapshot {
    status: NodeSchedulingStatus,
    reason: String,
    ready: u32,
    min_ready: u32,
    degraded: u32,
    quarantined: u32,
    total: u32,
    leased: u32,
    max_slots: u32,
}

struct NodeCtlWorker {
    db: Arc<Mutex<Connection>>,
    node_id: String,
    node_role: Option<NodeRole>,
    /// The resolved reconciliation poll, as `spawn_poll_loop` runs it.
    poll_interval: Duration,
    cluster_reconcile_trigger: ClusterReconcileTrigger,
    resource_status_reader: NodeResourceStatusReader,
    network_pool_status_reader: NetworkPoolStatusReader,
    orphan_inventory: orphan_inventory::Observer,
    runtime_provisioning: Arc<facets::Provisioning>,
    /// The Kubernetes node-local controller when this node runs one.
    node_objects: Option<Box<dyn NodeObjectReconciles>>,
    rx: mpsc::Receiver<WorkItem>,
}

/// The node's Kubernetes object reconciles: template-presence node metadata,
/// network-slot status capacity, the SSD placement labels and the host-CPU
/// label. They ride the template-inventory publication (see
/// `NodeCtlWorker::run_outbound_sync`). A trait so a test can stand in for the
/// Kubernetes controller.
#[tonic::async_trait]
trait NodeObjectReconciles: Send + Sync {
    /// Run each reconcile once; a failure is logged and the rest still run.
    async fn reconcile_node_objects(&mut self);
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl NodeObjectReconciles for kubernetes_controller::Controller {
    async fn reconcile_node_objects(&mut self) {
        if let Err(error) = self.reconcile_template_inventory().await {
            tracing::warn!(%error, "local Kubernetes template-inventory facet failed");
        }
        if let Err(error) = self.reconcile_network_slot_capacity().await {
            tracing::warn!(%error, "local Kubernetes network-slot capacity reconcile failed");
        }
        if let Err(error) = self.reconcile_ssd_headroom_tier().await {
            tracing::warn!(%error, "local Kubernetes ssd-headroom-tier reconcile failed");
        }
        if let Err(error) = self.reconcile_host_cpu().await {
            tracing::warn!(%error, "local Kubernetes host-cpu reconcile failed");
        }
    }
}

#[derive(Debug, Default)]
struct ReconcileStats {
    templates_seen: u64,
    sandboxes_seen: u64,
    templates_applied: u64,
    sandboxes_applied: u64,
}

#[derive(Debug, Default)]
struct InboundFacetResult {
    stats: ReconcileStats,
    element_errors: BTreeMap<String, String>,
}

impl InboundFacetResult {
    fn from_stats(stats: ReconcileStats) -> Self {
        Self {
            stats,
            element_errors: BTreeMap::new(),
        }
    }
}

/// Run node-ctl until `shutdown_token` fires or a fatal condition ends it.
/// Telemetry must already be initialized by the caller.
pub async fn run(
    cli: NodeCtlRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunNodeCtlError> {
    let config = read_node_ctl_config(&cli.config_path)?;
    let poll_interval_secs = resolve_poll_interval(&cli, &config);
    let cluster_endpoint = resolve_cluster_node_api_endpoint(&config);
    CLUSTER_NODE_API_ENDPOINT
        .set(cluster_endpoint.clone())
        .expect("cluster endpoint configured once at startup");
    let node_id = resolve_node_id(Path::new(NODE_ID_PATH))?;
    let node_role = resolve_node_role(&cli, &config)?;

    let health_server = LocalHealthServer::spawn_optional(
        cli.health_socket_path.as_deref(),
        pplx_space_common_svc::UnixSocketOptions::new(0o660),
        SERVICE_NAME,
        shutdown_token.clone(),
    )
    .map_err(RunNodeCtlError::HealthSocket)?;
    let service_mutex_path = service_mutex_path(&cli.socket_path);
    let health_socket = cli
        .health_socket_path
        .as_ref()
        .map_or_else(|| "disabled".to_string(), |path| path.display().to_string());

    tracing::info!(
        health_socket = %health_socket,
        service_mutex = %service_mutex_path.display(),
        "space-node-ctl passive; waiting for service mutex"
    );

    let service_mutex = match ServiceMutex::acquire_with_timeout(
        &service_mutex_path,
        &shutdown_token,
        cli.service_mutex_timeout,
    )
    .await
    .map_err(RunNodeCtlError::ServiceMutex)?
    {
        AcquireOutcome::Acquired(mutex) => mutex,
        AcquireOutcome::PassiveExit => {
            tracing::warn!("terminated while passive; exiting cleanly");
            health_server.join().await;
            return Ok(());
        }
    };

    tracing::info!(
        service_mutex = %service_mutex.path().display(),
        "acquired service mutex; transitioning to active"
    );
    health_server.mark_starting();

    pplx_space_sqlite::migrate("node-ctl", &cli.db_path, &NODE_CTL_MIGRATOR)
        .await
        .map_err(RunNodeCtlError::Migrate)?;
    let db = pplx_space_sqlite::open_rusqlite(
        &cli.db_path,
        pplx_space_sqlite::RusqliteOpenOptions::default(),
    )
    .map_err(RunNodeCtlError::OpenDatabase)?;
    init_reconciliation_status(&db, poll_interval_secs)
        .map_err(RunNodeCtlError::InitReconciliationStatus)?;
    let db = Arc::new(Mutex::new(db));

    let (tx, rx) = mpsc::channel(WORK_QUEUE_CAPACITY);
    let cluster_reconcile_trigger = ClusterReconcileTrigger::new(tx.clone(), &node_id);
    let worker_shutdown_token = shutdown_token.child_token();
    let resource_status_reader = NodeResourceStatusReader::new();
    let network_pool_status_reader = NetworkPoolStatusReader::new();
    #[cfg(feature = "kubernetes")]
    let kubernetes_controller = match kubernetes_controller::Controller::from_env(
        shutdown_token.child_token(),
        resource_status_reader.clone(),
    )
    .await
    {
        Ok(controller) => controller,
        Err(error) => {
            tracing::warn!(%error, "failed to initialize Kubernetes node controller; continuing without it");
            None
        }
    };
    let worker = NodeCtlWorker {
        db: db.clone(),
        node_id: node_id.clone(),
        node_role,
        poll_interval: poll_interval(poll_interval_secs),
        cluster_reconcile_trigger: cluster_reconcile_trigger.clone(),
        resource_status_reader,
        network_pool_status_reader,
        orphan_inventory: orphan_inventory::Observer::default(),
        runtime_provisioning: Arc::default(),
        #[cfg(feature = "kubernetes")]
        node_objects: kubernetes_controller
            .map(|controller| Box::new(controller) as Box<dyn NodeObjectReconciles>),
        #[cfg(not(feature = "kubernetes"))]
        node_objects: None,
        rx,
    };
    let mut worker_task = tokio::spawn(worker.run(worker_shutdown_token));

    spawn_poll_loop(tx.clone(), poll_interval_secs, shutdown_token.child_token());

    let listener = pplx_space_common_svc::bind_unix_socket(
        &cli.socket_path,
        pplx_space_common_svc::UnixSocketOptions::new(0o660).with_group(
            pplx_space_common_svc::SocketGroup::Required(cli.socket_group),
        ),
    )
    .map_err(RunNodeCtlError::BindSocket)?;
    tracing::info!(
        socket_path = %cli.socket_path.display(),
        db_path = %cli.db_path.display(),
        config_path = %cli.config_path.display(),
        poll_interval_secs,
        cluster_node_api_endpoint = cluster_endpoint.as_deref().unwrap_or("-"),
        node_id = %node_id,
        node_role = node_role_label(node_role),
        "space-nodectl listening"
    );
    health_server.activate(cli.socket_path.clone());

    let server_shutdown_token = shutdown_token.clone();
    let shutdown = async move {
        server_shutdown_token.cancelled().await;
    };

    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    let server = pplx_space_common_svc::grpc_server_builder()
        .layer(pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(SERVICE_NAME))
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .add_service(health_service)
        .add_service(NodeCtlServer::new(NodeCtlService {
            tx,
            cluster_reconcile_trigger,
            db,
            peer_authorizer: UdsPeerAuthorizer::new_with_default_local_roles(
                SERVICE_NAME,
                [
                    UdsPeerAccount::with_linux_user(users::SPACE_NODEAPI),
                    UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR),
                    UdsPeerAccount::with_linux_user(users::SPACE_TMPLMGR),
                    UdsPeerAccount::with_linux_user(users::SPACE_NODECTL),
                ],
            ),
        }))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown);

    let mut service_mutex = Some(service_mutex);
    tokio::select! {
        result = server => {
            shutdown_token.cancel();
            if let Err(error) = result {
                // The worker may still be mutating node-ctl's SQLite; a
                // normal unwind would release the flock before it stops.
                tracing::error!(%error, "node-ctl gRPC server failed");
                service_mutex
                    .take()
                    .expect("service mutex held until shutdown")
                    .hard_exit(1, "node-ctl gRPC server failed unexpectedly");
            }
            // Facet runs mutate node-ctl's SQLite from the worker task, so
            // ownership is released only after the worker provably stopped —
            // or at a hard process boundary when it exceeds the drain budget.
            match tokio::time::timeout(DEFAULT_DRAIN_TIMEOUT, &mut worker_task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "node-ctl worker task ended unexpectedly during shutdown");
                }
                Err(_) => {
                    tracing::error!(
                        drain_timeout_secs = DEFAULT_DRAIN_TIMEOUT.as_secs(),
                        "node-ctl worker exceeded the drain budget; exiting while holding ownership"
                    );
                    service_mutex
                        .take()
                        .expect("service mutex held until worker stops")
                        .hard_exit(1, "node-ctl worker did not stop within the drain budget");
                }
            }
        }
        result = &mut worker_task => {
            let shutting_down = shutdown_token.is_cancelled();
            shutdown_token.cancel();
            match result {
                Ok(()) if shutting_down => {}
                Ok(()) => return Err(RunNodeCtlError::WorkerExited),
                Err(error) => return Err(RunNodeCtlError::WorkerTaskFailed(error)),
            }
        }
    }

    health_server.join().await;
    drop(service_mutex.take());
    Ok(())
}

/// The service mutex of the instances that publish `socket_path`: a file
/// beside the socket. Instances that would publish the same socket therefore
/// contend on the same mutex, so at most one of them serves it.
///
/// Every release must derive the same mutex path for the production socket,
/// `/space/run/node-ctl/service.mutex`. A release that derived another would
/// not contend with an older node-ctl during a rolling upgrade, and both
/// would be active.
fn service_mutex_path(socket_path: &Path) -> PathBuf {
    socket_path.with_file_name(SERVICE_MUTEX_FILE_NAME)
}

fn read_node_ctl_config(path: &Path) -> Result<NodeCtlConfig, RunNodeCtlError> {
    if !path.exists() {
        return Ok(NodeCtlConfig::default());
    }

    let contents = std::fs::read_to_string(path).map_err(|source| RunNodeCtlError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&contents).map_err(|source| RunNodeCtlError::ParseConfig {
        path: path.to_path_buf(),
        source,
    })
}

fn resolve_poll_interval(cli: &NodeCtlRunConfig, config: &NodeCtlConfig) -> u64 {
    cli.poll_interval_secs
        .or(config.reconciliation.poll_interval_secs)
        .unwrap_or(DEFAULT_POLL_INTERVAL_SECS)
        .max(1)
}

fn resolve_cluster_node_api_endpoint(config: &NodeCtlConfig) -> Option<String> {
    std::env::var(SPACE_INTERNAL_API_ENDPOINT_ENV)
        .ok()
        .and_then(|value| non_empty(value.as_str()))
        .or_else(|| {
            config
                .cluster
                .node_api_endpoint
                .as_deref()
                .and_then(non_empty)
        })
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn init_reconciliation_status(conn: &Connection, poll_interval_secs: u64) -> rusqlite::Result<()> {
    let now = now_secs();
    for name in [
        NETWORK_POLICY_RECONCILER,
        SANDBOX_DESIRED_STATE_RECONCILER,
        TEMPLATE_BASE_SNAPSHOT_POLICY_RECONCILER,
        TEMPLATE_DESIRED_STATE_RECONCILER,
        RUNTIME_PROVISIONING_RECONCILER,
        NODE_STATE_OUTBOUND_RECONCILER,
        NETWORK_POLICY_OUTBOUND_RECONCILER,
        SANDBOX_INVENTORY_OUTBOUND_RECONCILER,
        TEMPLATE_INVENTORY_OUTBOUND_RECONCILER,
        NODE_STATUS_OUTBOUND_RECONCILER,
    ] {
        conn.execute(
            "INSERT INTO reconciliation_status
                 (name, poll_interval_secs, next_due_at, running, updated_at)
             VALUES (?1, ?2, ?3, 0, ?3)
             ON CONFLICT(name) DO UPDATE
             SET poll_interval_secs = excluded.poll_interval_secs,
                 next_due_at = CASE
                     WHEN reconciliation_status.running != 0
                     THEN ?3
                     ELSE reconciliation_status.next_due_at
                 END,
                 running = 0,
                 updated_at = ?3",
            params![name, poll_interval_secs.cast_signed(), now],
        )?;
    }
    Ok(())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed()
}

fn resolve_node_id(path: &Path) -> Result<String, RunNodeCtlError> {
    if let Ok(value) = std::env::var(SPACE_NODE_ID_ENV)
        && !value.trim().is_empty()
    {
        return Ok(value.trim().to_string());
    }
    let content = std::fs::read_to_string(path).map_err(|source| RunNodeCtlError::ReadNodeId {
        path: path.to_path_buf(),
        source,
    })?;
    let content = content.trim();
    if content.is_empty() {
        return Err(RunNodeCtlError::EmptyNodeId {
            path: path.to_path_buf(),
        });
    }
    Ok(content.to_string())
}

fn resolve_node_role(
    cli: &NodeCtlRunConfig,
    config: &NodeCtlConfig,
) -> Result<Option<NodeRole>, RunNodeCtlError> {
    if let Some(role) = cli.role.as_deref().and_then(non_empty) {
        return parse_node_role(&role).map(Some);
    }
    if let Ok(role) = std::env::var(SPACE_NODE_ROLE_ENV)
        && let Some(role) = non_empty(&role)
    {
        return parse_node_role(&role).map(Some);
    }
    if let Some(role) = config.role.as_deref().and_then(non_empty) {
        return parse_node_role(&role).map(Some);
    }
    Ok(None)
}

fn parse_node_role(value: &str) -> Result<NodeRole, RunNodeCtlError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "cluster" => Ok(NodeRole::Cluster),
        "worker" => Ok(NodeRole::Worker),
        "builder" => Ok(NodeRole::Builder),
        other => Err(RunNodeCtlError::UnsupportedNodeRole {
            role: other.to_string(),
        }),
    }
}

fn proto_node_role(role: Option<NodeRole>) -> Option<i32> {
    role.map(|role| role as i32)
}

fn node_role_label(role: Option<NodeRole>) -> &'static str {
    match role {
        Some(NodeRole::Cluster) => "cluster",
        Some(NodeRole::Worker) => "worker",
        Some(NodeRole::Builder) => "builder",
        None => "-",
    }
}

/// The poll period for a configured interval; a zero interval polls every
/// second rather than spinning.
fn poll_interval(interval_secs: u64) -> Duration {
    Duration::from_secs(interval_secs.max(1))
}

fn spawn_poll_loop(
    tx: mpsc::Sender<WorkItem>,
    interval_secs: u64,
    shutdown_token: CancellationToken,
) {
    tokio::spawn(async move {
        let period = poll_interval(interval_secs);
        let interval_secs = period.as_secs();
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown_token.cancelled() => break,
                _ = interval.tick() => {}
            }
            let next_due_at = now_secs().saturating_add(interval_secs.cast_signed());
            if tx.send(WorkItem::TimerTick { next_due_at }).await.is_err() {
                break;
            }
        }
    });
}

#[tonic::async_trait]
impl NodeCtl for NodeCtlService {
    async fn get_reconciliation_status(
        &self,
        request: Request<GetReconciliationStatusRequest>,
    ) -> Result<Response<GetReconciliationStatusResponse>, Status> {
        self.peer_authorizer.authorize_request(
            &request,
            "GetReconciliationStatus",
            ALLOW_NODE_CTL_OBSERVER,
        )?;
        let reconciliations = read_reconciliation_statuses(&self.db)
            .map_err(|error| Status::internal(format!("read reconciliation status: {error}")))?;
        let sync_overrides = dirty_state::list_overrides(&self.db)
            .map_err(|error| Status::internal(format!("read facet sync overrides: {error}")))?;
        Ok(Response::new(GetReconciliationStatusResponse {
            reconciliations,
            sync_overrides,
        }))
    }

    async fn get_controller_state(
        &self,
        request: Request<GetControllerStateRequest>,
    ) -> Result<Response<GetControllerStateResponse>, Status> {
        self.peer_authorizer.authorize_request(
            &request,
            "GetControllerState",
            ALLOW_NODE_CTL_OBSERVER,
        )?;
        let request = request.into_inner();
        let facet_filter = request
            .facet
            .as_deref()
            .map(str::trim)
            .filter(|facet| !facet.is_empty());
        if let Some(facet) = facet_filter
            && facets::FacetKind::parse(facet).is_none()
        {
            return Err(Status::invalid_argument(format!("unknown facet {facet:?}")));
        }

        let reconciliations = read_reconciliation_statuses(&self.db)
            .map_err(|error| Status::internal(format!("read reconciliation status: {error}")))?;
        let sync_overrides = dirty_state::list_overrides(&self.db)
            .map_err(|error| Status::internal(format!("read facet sync overrides: {error}")))?;
        let outbound_queue = if request.include_queue {
            dirty_state::list_queue(&self.db, facet_filter)
                .map_err(|error| Status::internal(format!("read outbound queue: {error}")))?
        } else {
            Vec::new()
        };
        let facets = build_controller_facets(&reconciliations, &sync_overrides, facet_filter);

        Ok(Response::new(GetControllerStateResponse {
            facets,
            outbound_queue,
            sync_overrides,
            cluster_sync_configured: cluster_sync_configured(),
        }))
    }

    async fn refresh_facet_state(
        &self,
        request: Request<RefreshFacetStateRequest>,
    ) -> Result<Response<RefreshFacetStateResponse>, Status> {
        self.peer_authorizer.authorize_request(
            &request,
            "RefreshFacetState",
            ALLOW_NODE_CTL_REFRESH,
        )?;
        let refs = request
            .into_inner()
            .refs
            .into_iter()
            .map(dirty_state::FacetRef::from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::invalid_argument)?;
        let refs = if refs.is_empty() {
            facets::default_inbound_refs()
        } else {
            refs
        };
        facets::validate_refs(&refs).map_err(Status::invalid_argument)?;
        for reference in &refs {
            let facet = facets::FacetKind::parse(&reference.facet)
                .ok_or_else(|| Status::invalid_argument("unknown facet"))?;
            if !facet.supports_inbound() {
                return Err(Status::invalid_argument(format!(
                    "facet {} does not support inbound reconciliation",
                    reference.facet
                )));
            }
        }
        self.tx
            .send(WorkItem::ManualRefresh { refs })
            .await
            .map_err(|_closed| Status::unavailable("node-ctl worker stopped"))?;
        Ok(Response::new(RefreshFacetStateResponse {}))
    }

    async fn trigger_state_reconciliation(
        &self,
        request: Request<TriggerStateReconciliationRequest>,
    ) -> Result<Response<TriggerStateReconciliationResponse>, Status> {
        self.peer_authorizer.authorize_request(
            &request,
            "TriggerStateReconciliation",
            ALLOW_NODE_CTL_REFRESH,
        )?;
        let reason = trigger_request_reason(request.into_inner());
        self.cluster_reconcile_trigger.trigger(reason).await?;
        Ok(Response::new(TriggerStateReconciliationResponse {}))
    }

    async fn mark_facet_dirty(
        &self,
        request: Request<MarkFacetDirtyRequest>,
    ) -> Result<Response<MarkFacetDirtyResponse>, Status> {
        self.peer_authorizer.authorize_request(
            &request,
            "MarkFacetDirty",
            ALLOW_NODE_CTL_MARK_DIRTY,
        )?;
        let refs = request
            .into_inner()
            .refs
            .into_iter()
            .map(dirty_state::FacetRef::from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::invalid_argument)?;
        if refs.is_empty() {
            return Ok(Response::new(MarkFacetDirtyResponse {}));
        }
        facets::validate_refs(&refs).map_err(Status::invalid_argument)?;
        {
            let mut db = self
                .db
                .lock()
                .map_err(|_poisoned| Status::internal("node-ctl database lock poisoned"))?;
            dirty_state::mark_refs(&mut db, &refs)
                .map_err(|error| Status::internal(format!("mark facet dirty: {error}")))?;
        }
        match self.tx.try_send(WorkItem::SyncDirty) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(item)) => {
                tracing::warn!("node-ctl work queue full while scheduling dirty sync; waiting");
                self.tx
                    .send(item)
                    .await
                    .map_err(|_closed| Status::unavailable("node-ctl worker stopped"))?;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(Status::unavailable("node-ctl worker stopped"));
            }
        }
        Ok(Response::new(MarkFacetDirtyResponse {}))
    }

    async fn set_facet_sync_disabled(
        &self,
        request: Request<SetFacetSyncDisabledRequest>,
    ) -> Result<Response<SetFacetSyncDisabledResponse>, Status> {
        self.peer_authorizer.authorize_request(
            &request,
            "SetFacetSyncDisabled",
            ALLOW_NODE_CTL_OPERATOR,
        )?;
        let request = request.into_inner();
        let facet = facets::FacetKind::parse(&request.facet).ok_or_else(|| {
            Status::invalid_argument(format!("unknown facet {:?}", request.facet))
        })?;
        let direction = dirty_state::SyncDirection::try_from(
            FacetDirection::try_from(request.direction).unwrap_or(FacetDirection::Unspecified),
        )
        .map_err(Status::invalid_argument)?;
        if direction == dirty_state::SyncDirection::Inbound && !facet.supports_inbound() {
            return Err(Status::invalid_argument(format!(
                "facet {} does not support inbound reconciliation",
                facet.name()
            )));
        }
        if direction == dirty_state::SyncDirection::Outbound && !facet.supports_outbound() {
            return Err(Status::invalid_argument(format!(
                "facet {} does not support outbound reconciliation",
                facet.name()
            )));
        }
        if request.disabled {
            let disabled_until = if request.no_expiry {
                None
            } else {
                let ttl = request.ttl_secs.unwrap_or(DEFAULT_DISABLE_TTL_SECS);
                if ttl == 0 {
                    return Err(Status::invalid_argument("ttl_secs must be greater than 0"));
                }
                Some(now_secs().saturating_add(i64::try_from(ttl).unwrap_or(i64::MAX)))
            };
            dirty_state::set_disabled(
                &self.db,
                facet.name(),
                direction,
                disabled_until,
                request.reason.trim(),
            )
            .map_err(|error| Status::internal(format!("set facet sync override: {error}")))?;
        } else {
            dirty_state::clear_disabled(&self.db, facet.name(), direction)
                .map_err(|error| Status::internal(format!("clear facet sync override: {error}")))?;
        }
        Ok(Response::new(SetFacetSyncDisabledResponse {}))
    }
}

impl NodeCtlWorker {
    async fn run(mut self, shutdown_token: CancellationToken) {
        loop {
            let work = tokio::select! {
                () = shutdown_token.cancelled() => break,
                work = self.rx.recv() => work,
            };
            let Some(work) = work else {
                break;
            };
            match work {
                WorkItem::TimerTick { next_due_at } => {
                    if let Err(error) = self.mark_next_due(next_due_at) {
                        tracing::warn!(%error, "failed to persist next node-ctl reconciliation time");
                    }
                    self.run_inbound_reconcile(
                        facets::periodic_inbound_refs(),
                        shutdown_token.child_token(),
                    )
                    .await;
                    // The timer is the backstop for refs no trigger delivered.
                    if let Some(claim) = self.claim_cluster_reconcile_refs().await {
                        self.apply_cluster_claim("timer", claim, shutdown_token.child_token())
                            .await;
                    }
                    self.run_outbound_sync(
                        facets::periodic_outbound_refs(),
                        shutdown_token.child_token(),
                    )
                    .await;
                    tokio::select! {
                        () = shutdown_token.cancelled() => break,
                        () = self.orphan_inventory.observe() => {}
                    }
                }
                WorkItem::ManualRefresh { refs } => {
                    self.run_inbound_reconcile(refs, shutdown_token.child_token())
                        .await;
                    self.run_outbound_sync(Vec::new(), shutdown_token.child_token())
                        .await;
                }
                WorkItem::DrainClusterQueue => {
                    self.run_cluster_queue_drain(shutdown_token.child_token())
                        .await;
                }
                WorkItem::SyncDirty => {
                    self.run_outbound_sync(Vec::new(), shutdown_token.child_token())
                        .await;
                }
            }
        }
    }

    async fn run_cluster_queue_drain(&self, shutdown_token: CancellationToken) {
        let start = self.cluster_reconcile_trigger.mark_started().await;
        tracing::debug!(
            reason = %start.reason,
            "starting triggered cluster reconcile queue drain"
        );
        let mut force_follow_up = false;
        for batch_index in 0..CLUSTER_RECONCILE_DRAIN_MAX_BATCHES {
            if shutdown_token.is_cancelled() {
                break;
            }
            let Some(claim) = self.claim_cluster_reconcile_refs().await else {
                break;
            };
            self.apply_cluster_claim("trigger", claim, shutdown_token.child_token())
                .await;
            if batch_index + 1 == CLUSTER_RECONCILE_DRAIN_MAX_BATCHES {
                force_follow_up = true;
            }
        }
        self.cluster_reconcile_trigger
            .mark_finished(start.epoch, force_follow_up)
            .await;
    }

    async fn run_inbound_reconcile(
        &self,
        refs: Vec<dirty_state::FacetRef>,
        shutdown_token: CancellationToken,
    ) -> BTreeMap<facets::FacetKind, Result<InboundFacetResult, String>> {
        let mut report = BTreeMap::new();
        let scopes = group_refs(refs);
        if !cluster_sync_configured() {
            for facet in scopes.keys().copied() {
                if !facet.supports_inbound() {
                    report.insert(facet, Ok(InboundFacetResult::default()));
                    continue;
                }
                let status_name = inbound_reconciler_name(facet);
                if let Err(error) = self.mark_reconcile_started_for(status_name) {
                    tracing::warn!(%error, "failed to persist skipped inbound reconciliation start");
                }
                if let Err(error) =
                    self.mark_reconcile_finished_for(status_name, &ReconcileStats::default(), None)
                {
                    tracing::warn!(%error, "failed to persist skipped inbound reconciliation result");
                }
                report.insert(facet, Ok(InboundFacetResult::default()));
            }
            return report;
        }

        let mut jobs = Vec::new();
        for (facet, scope) in scopes {
            if !facet.supports_inbound() {
                report.insert(facet, Ok(InboundFacetResult::default()));
                continue;
            }

            let status_name = inbound_reconciler_name(facet);
            if let Err(error) = self.mark_reconcile_started_for(status_name) {
                tracing::warn!(%error, "failed to persist inbound reconciliation start");
            }

            match dirty_state::is_disabled(
                &self.db,
                facet.name(),
                dirty_state::SyncDirection::Inbound,
            ) {
                Ok(true) => {
                    if let Err(error) = self.mark_reconcile_finished_for(
                        status_name,
                        &ReconcileStats::default(),
                        None,
                    ) {
                        tracing::warn!(%error, "failed to persist disabled inbound reconciliation result");
                    }
                    report.insert(facet, Ok(InboundFacetResult::default()));
                }
                Ok(false) => {
                    let node_id = self.node_id.clone();
                    let node_role = self.node_role;
                    let poll_interval = self.poll_interval;
                    let provisioning = self.runtime_provisioning.clone();
                    let facet_token = shutdown_token.child_token();
                    let (scope_kind, scope_items) = facet_scope_span_fields(&scope);
                    let span = tracing::info_span!(
                        "node_ctl.reconcile",
                        direction = "inbound",
                        facet = facet.name(),
                        node_id = %node_id,
                        scope = scope_kind,
                        scope_items,
                    );
                    jobs.push(
                        async move {
                            let started_at = Instant::now();
                            let progress = FacetProgress::new();
                            let result = tokio::select! {
                                () = facet_token.cancelled() => Err(format!(
                                    "inbound facet {} cancelled during shutdown",
                                    facet.name(),
                                )),
                                () = facet_stalled(progress.clone(), FACET_RUN_TIMEOUT) => Err(format!(
                                    "inbound facet {} stalled: no progress for {}s",
                                    facet.name(),
                                    FACET_RUN_TIMEOUT.as_secs()
                                )),
                                result = facet.run_inbound(&node_id, node_role, scope, progress.clone(), poll_interval, &provisioning) => result,
                            };
                            (facet, status_name, started_at, result)
                        }
                        .instrument(span),
                    );
                }
                Err(error) => {
                    let error = format!("read inbound sync override: {error}");
                    if let Err(db_error) = self.mark_reconcile_finished_for(
                        status_name,
                        &ReconcileStats::default(),
                        Some(&error),
                    ) {
                        tracing::warn!(%db_error, "failed to persist inbound reconciliation result");
                    }
                    report.insert(facet, Err(error));
                }
            }
        }

        for (facet, status_name, started_at, result) in join_all(jobs).await {
            let empty_stats = ReconcileStats::default();
            let (stats, error) = match &result {
                Ok(result) if result.element_errors.is_empty() => (&result.stats, None),
                Ok(result) => (
                    &result.stats,
                    Some(format!(
                        "failed to reconcile {} {} item(s)",
                        result.element_errors.len(),
                        facet.name()
                    )),
                ),
                Err(error) => {
                    tracing::warn!(facet = facet.name(), %error, "inbound facet reconciliation failed");
                    (&empty_stats, Some(error.clone()))
                }
            };
            let result_label = if error.is_some() { "error" } else { "ok" };
            pplx_space_metrics::reconcile::run_duration(
                facet.name(),
                "inbound",
                result_label,
                "node_ctl",
                started_at,
            );
            pplx_space_metrics::reconcile::run_items(
                facet.name(),
                "inbound",
                result_label,
                stats
                    .templates_applied
                    .saturating_add(stats.sandboxes_applied),
            );
            if let Err(error) =
                self.mark_reconcile_finished_for(status_name, stats, error.as_deref())
            {
                tracing::warn!(%error, "failed to persist inbound reconciliation result");
            }
            report.insert(facet, result);
        }
        report
    }

    async fn run_outbound_sync(
        &mut self,
        periodic_refs: Vec<dirty_state::FacetRef>,
        shutdown_token: CancellationToken,
    ) {
        let mut batch = match dirty_state::read(&self.db) {
            Ok(batch) => batch,
            Err(error) => {
                tracing::warn!(%error, "failed to read dirty node state");
                return;
            }
        };
        for reference in periodic_refs {
            batch.push_ref(reference);
        }
        if batch.is_empty() {
            return;
        }
        // The node-local Kubernetes object reconciles (template-presence node
        // metadata, network-slot status capacity, the SSD placement labels, and
        // the host-CPU label) share this cadence: the template-inventory facet
        // is pushed on every periodic outbound tick, so gating on it runs each
        // of these reconciles every tick and self-heals drift.
        if batch.facets().any(|(facet_name, _)| {
            facets::FacetKind::parse(facet_name) == Some(facets::FacetKind::TemplateInventory)
        }) && let Some(node_objects) = &mut self.node_objects
        {
            node_objects.reconcile_node_objects().await;
        }

        if !cluster_sync_configured() {
            for (facet_name, _) in batch.facets() {
                let Some(facet) = facets::FacetKind::parse(facet_name) else {
                    continue;
                };
                if !facet.supports_outbound() {
                    continue;
                }
                let status_name = outbound_reconciler_name(facet);
                if let Err(error) = self.mark_reconcile_started_for(status_name) {
                    tracing::warn!(%error, "failed to persist skipped outbound sync start");
                }
                if let Err(error) =
                    self.mark_reconcile_finished_for(status_name, &ReconcileStats::default(), None)
                {
                    tracing::warn!(%error, "failed to persist skipped outbound sync result");
                }
            }
            return;
        }

        let mut succeeded = batch.empty_with_claim();
        let scopes = batch
            .facets()
            .map(|(facet_name, items)| (facet_name.to_string(), items))
            .collect::<Vec<_>>();
        let mut jobs = Vec::new();
        let mut node_status_for_batch: Option<Option<PublishedNodeStatus>> = None;
        for (facet_name, items) in scopes {
            let Some(facet) = facets::FacetKind::parse(&facet_name) else {
                succeeded.copy_scope_from(&batch, &facet_name);
                continue;
            };
            if !facet.supports_outbound() {
                succeeded.copy_scope_from(&batch, facet.name());
                continue;
            }
            let status_name = outbound_reconciler_name(facet);
            if let Err(error) = self.mark_reconcile_started_for(status_name) {
                tracing::warn!(%error, "failed to persist outbound sync start");
            }
            match dirty_state::is_disabled(
                &self.db,
                facet.name(),
                dirty_state::SyncDirection::Outbound,
            ) {
                Ok(true) => {
                    if let Err(db_error) = self.mark_reconcile_finished_for(
                        status_name,
                        &ReconcileStats::default(),
                        None,
                    ) {
                        tracing::warn!(%db_error, "failed to persist disabled outbound sync result");
                    }
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    let error = format!("read outbound sync override: {error}");
                    let mut failed = batch.empty_with_claim();
                    failed.copy_scope_from(&batch, facet.name());
                    if let Err(db_error) = dirty_state::mark_error(&self.db, &failed, &error) {
                        tracing::warn!(%db_error, "failed to persist dirty node state sync error");
                    }
                    if let Err(db_error) = self.mark_reconcile_finished_for(
                        status_name,
                        &ReconcileStats::default(),
                        Some(&error),
                    ) {
                        tracing::warn!(%db_error, "failed to persist outbound sync result");
                    }
                    continue;
                }
            }

            let node_id = self.node_id.clone();
            let node_role = self.node_role;
            let node_status = if let Some(node_status) = &node_status_for_batch {
                node_status.clone()
            } else {
                let node_status = self.node_status_for_publish().await;
                node_status_for_batch = Some(node_status.clone());
                node_status
            };
            let scope = facets::FacetScope::from_optional_items(items);
            let facet_token = shutdown_token.child_token();
            let (scope_kind, scope_items) = facet_scope_span_fields(&scope);
            let span = tracing::info_span!(
                "node_ctl.reconcile",
                direction = "outbound",
                facet = facet.name(),
                node_id = %node_id,
                scope = scope_kind,
                scope_items,
            );
            jobs.push(
                async move {
                    let started_at = Instant::now();
                    let timeout = tokio::time::sleep(FACET_RUN_TIMEOUT);
                    tokio::pin!(timeout);
                    let result = tokio::select! {
                        () = facet_token.cancelled() => Err(format!(
                            "outbound facet {} cancelled during shutdown",
                            facet.name(),
                        )),
                        () = &mut timeout => Err(format!(
                            "outbound facet {} timed out after {}s",
                            facet.name(),
                            FACET_RUN_TIMEOUT.as_secs()
                        )),
                        result = facet.run_outbound(&node_id, node_role, scope, node_status) => result,
                    };
                    (facet, status_name, started_at, result)
                }
                .instrument(span),
            );
        }

        for (facet, status_name, started_at, result) in join_all(jobs).await {
            match result {
                Err(error) => {
                    pplx_space_metrics::reconcile::run_duration(
                        facet.name(),
                        "outbound",
                        "error",
                        "node_ctl",
                        started_at,
                    );
                    tracing::warn!(facet = facet.name(), %error, "outbound facet sync failed");
                    let mut failed = batch.empty_with_claim();
                    failed.copy_scope_from(&batch, facet.name());
                    if let Err(db_error) = dirty_state::mark_error(&self.db, &failed, &error) {
                        tracing::warn!(%db_error, "failed to persist dirty node state sync error");
                    }
                    if let Err(db_error) = self.mark_reconcile_finished_for(
                        status_name,
                        &ReconcileStats::default(),
                        Some(&error),
                    ) {
                        tracing::warn!(%db_error, "failed to persist outbound sync result");
                    }
                }
                Ok(stats) => {
                    pplx_space_metrics::reconcile::run_duration(
                        facet.name(),
                        "outbound",
                        "ok",
                        "node_ctl",
                        started_at,
                    );
                    pplx_space_metrics::reconcile::run_items(
                        facet.name(),
                        "outbound",
                        "ok",
                        stats
                            .templates_applied
                            .saturating_add(stats.sandboxes_applied),
                    );
                    if let Err(db_error) =
                        self.mark_reconcile_finished_for(status_name, &stats, None)
                    {
                        tracing::warn!(%db_error, "failed to persist outbound sync result");
                    }
                    succeeded.copy_scope_from(&batch, facet.name());
                }
            }
        }

        if !succeeded.is_empty()
            && let Err(db_error) = dirty_state::clear(&self.db, &succeeded)
        {
            tracing::warn!(%db_error, "failed to clear dirty node state");
        }
    }

    fn mark_next_due(&self, next_due_at: i64) -> rusqlite::Result<()> {
        let mut db = self
            .db
            .lock()
            .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
        let tx = db.transaction()?;
        for name in [
            NETWORK_POLICY_RECONCILER,
            SANDBOX_DESIRED_STATE_RECONCILER,
            TEMPLATE_BASE_SNAPSHOT_POLICY_RECONCILER,
            TEMPLATE_DESIRED_STATE_RECONCILER,
            RUNTIME_PROVISIONING_RECONCILER,
            NODE_STATE_OUTBOUND_RECONCILER,
            NETWORK_POLICY_OUTBOUND_RECONCILER,
            SANDBOX_INVENTORY_OUTBOUND_RECONCILER,
            TEMPLATE_INVENTORY_OUTBOUND_RECONCILER,
            NODE_STATUS_OUTBOUND_RECONCILER,
        ] {
            tx.execute(
                "UPDATE reconciliation_status
                 SET next_due_at = ?1,
                     updated_at = ?2
                 WHERE name = ?3",
                params![next_due_at, now_secs(), name],
            )?;
        }
        tx.commit()
    }

    async fn node_status_for_publish(&self) -> Option<PublishedNodeStatus> {
        let scheduling = self.scheduling_status_for_publish().await;
        Some(PublishedNodeStatus {
            scheduling_status: scheduling.status as i32,
            scheduling_reason: scheduling.reason,
            network_slots_ready: scheduling.ready,
            network_pool_min_ready: scheduling.min_ready,
            network_slots_degraded: scheduling.degraded,
            network_slots_quarantined: scheduling.quarantined,
            network_slots_total: scheduling.total,
            network_slots_leased: scheduling.leased,
            network_pool_max_slots: scheduling.max_slots,
            node_ctl: Some(self.node_ctl_status_for_publish()),
        })
    }

    async fn scheduling_status_for_publish(&self) -> SchedulingSnapshot {
        let response = match self
            .network_pool_status_reader
            .read(
                pplx_space_node_mgr_client::lazy(),
                local_manager_request(
                    pplx_space_node_mgr_client::proto::NetworkPoolStatusRequest {},
                    false,
                ),
            )
            .await
        {
            Ok(status) => {
                if let Some(error) = status.fallback_error() {
                    tracing::warn!(
                        %error,
                        "node-manager network pool status unavailable; reusing recent status for node scheduling publish"
                    );
                }
                status.response
            }
            Err(error) => {
                return SchedulingSnapshot {
                    status: NodeSchedulingStatus::Unavailable,
                    reason: format!("node-manager status failed: {error}"),
                    ready: 0,
                    min_ready: 0,
                    degraded: 0,
                    quarantined: 0,
                    total: 0,
                    leased: 0,
                    max_slots: 0,
                };
            }
        };

        use pplx_space_node_mgr_client::proto::NetworkPoolStatus;
        let pool_status =
            NetworkPoolStatus::try_from(response.network_pool_status).unwrap_or_default();
        let status = match pool_status {
            NetworkPoolStatus::Healthy => {
                if response.network_pool_max_slots > 0
                    && response.network_slots_leased >= response.network_pool_max_slots
                {
                    NodeSchedulingStatus::Full
                } else if response.network_slots_ready >= response.network_pool_min_ready {
                    NodeSchedulingStatus::Ready
                } else {
                    NodeSchedulingStatus::Warming
                }
            }
            NetworkPoolStatus::Warming => NodeSchedulingStatus::Warming,
            NetworkPoolStatus::Draining => NodeSchedulingStatus::Draining,
            NetworkPoolStatus::Degraded => NodeSchedulingStatus::Degraded,
            NetworkPoolStatus::Empty => NodeSchedulingStatus::Empty,
            NetworkPoolStatus::Unspecified => NodeSchedulingStatus::Unavailable,
        };
        let reason = match status {
            NodeSchedulingStatus::Ready => format!(
                "ready slots {} >= min_ready {}",
                response.network_slots_ready, response.network_pool_min_ready
            ),
            NodeSchedulingStatus::Warming => format!(
                "warming network pool: ready slots {} < min_ready {}",
                response.network_slots_ready, response.network_pool_min_ready
            ),
            NodeSchedulingStatus::Full => format!(
                "network pool full: leased slots {} >= max_slots {}",
                response.network_slots_leased, response.network_pool_max_slots
            ),
            NodeSchedulingStatus::Draining => "network pool is draining".to_string(),
            NodeSchedulingStatus::Degraded => format!(
                "network pool degraded: degraded={} quarantined={}",
                response.network_slots_degraded, response.network_slots_quarantined
            ),
            NodeSchedulingStatus::Empty => "network pool has no ready slots".to_string(),
            NodeSchedulingStatus::Unavailable | NodeSchedulingStatus::Unspecified => {
                "network pool status unavailable".to_string()
            }
        };

        let mut snapshot = SchedulingSnapshot {
            status,
            reason,
            ready: response.network_slots_ready,
            min_ready: response.network_pool_min_ready,
            degraded: response.network_slots_degraded,
            quarantined: response.network_slots_quarantined,
            total: response.network_slots_total,
            leased: response.network_slots_leased,
            max_slots: response.network_pool_max_slots,
        };
        self.apply_node_resource_status(&mut snapshot).await;
        snapshot
    }

    async fn apply_node_resource_status(&self, snapshot: &mut SchedulingSnapshot) {
        let status = self
            .resource_status_reader
            .read(
                pplx_space_node_mgr_client::lazy(),
                local_manager_request(
                    pplx_space_node_mgr_client::proto::NodeResourceStatusRequest {},
                    false,
                ),
            )
            .await;
        let status = match status {
            Ok(status) => status,
            Err(error) => {
                snapshot.status = NodeSchedulingStatus::Unavailable;
                snapshot.reason = format!("node-manager resource status failed: {error}");
                return;
            }
        };
        if let Some(error) = status.fallback_error() {
            tracing::warn!(
                %error,
                "node-manager resource status unavailable; reusing recent status for node scheduling publish"
            );
        }
        let cache_disk = match pplx_space_node_mgr_client::cache_disk_status(&status.response) {
            Ok(cache_disk) => cache_disk,
            Err(error) => {
                snapshot.status = NodeSchedulingStatus::Unavailable;
                snapshot.reason = format!("node-manager resource status failed: {error}");
                return;
            }
        };

        Self::apply_cache_disk_scheduling_effect(snapshot, cache_disk);
    }

    fn apply_cache_disk_scheduling_effect(
        snapshot: &mut SchedulingSnapshot,
        cache_disk: &pplx_space_node_mgr_client::proto::CacheDiskStatus,
    ) {
        match cache_disk.scheduling_effect() {
            CacheDiskSchedulingEffect::Open => {}
            CacheDiskSchedulingEffect::WarnOnly => {
                tracing::warn!(
                    reason = %cache_disk.admission_reason,
                    detail = %cache_disk.admission_detail,
                    "node disk admission warning threshold crossed"
                );
                snapshot.status = NodeSchedulingStatus::Full;
                snapshot.reason = format!(
                    "{}: {}",
                    cache_disk.admission_reason, cache_disk.admission_detail
                );
            }
            CacheDiskSchedulingEffect::Full => {
                snapshot.status = NodeSchedulingStatus::Full;
                snapshot.reason = format!(
                    "{}: {}",
                    cache_disk.admission_reason, cache_disk.admission_detail
                );
            }
            CacheDiskSchedulingEffect::Unavailable => {
                snapshot.status = NodeSchedulingStatus::Unavailable;
                snapshot.reason = format!(
                    "{}: {}",
                    cache_disk.admission_reason, cache_disk.admission_detail
                );
            }
        }
    }

    fn node_ctl_status_for_publish(&self) -> NodeCtlStatus {
        let reconciliations = match read_reconciliation_statuses(&self.db) {
            Ok(reconciliations) => reconciliations,
            Err(error) => {
                tracing::warn!(%error, "failed to read reconciliation status for node state publish");
                return NodeCtlStatus {
                    reachable: false,
                    error: Some(format!("read reconciliation status failed: {error}")),
                    reconciliations: Vec::new(),
                    sync_overrides: Vec::new(),
                };
            }
        };
        let sync_overrides = match dirty_state::list_overrides(&self.db) {
            Ok(sync_overrides) => sync_overrides,
            Err(error) => {
                tracing::warn!(%error, "failed to read sync overrides for node state publish");
                return NodeCtlStatus {
                    reachable: false,
                    error: Some(format!("read sync overrides failed: {error}")),
                    reconciliations: Vec::new(),
                    sync_overrides: Vec::new(),
                };
            }
        };
        NodeCtlStatus {
            reachable: true,
            error: None,
            reconciliations,
            sync_overrides,
        }
    }

    fn mark_reconcile_started_for(&self, name: &str) -> rusqlite::Result<()> {
        let now = now_secs();
        let db = self
            .db
            .lock()
            .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
        db.execute(
            "UPDATE reconciliation_status
             SET last_started_at = ?1,
                 running = 1,
                 updated_at = ?1
             WHERE name = ?2",
            params![now, name],
        )?;
        Ok(())
    }

    async fn claim_cluster_reconcile_refs(&self) -> Option<ClusterReconcileClaim> {
        if !cluster_sync_configured() {
            return None;
        }
        let mut client = match connect_cluster_api().await {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(%error, "failed to connect to cluster API for queued reconcile claim");
                return None;
            }
        };
        match client
            .claim_node_reconcile_refs(ClaimNodeReconcileRefsRequest {
                node_id: self.node_id.clone(),
                limit: 256,
                role: proto_node_role(self.node_role),
            })
            .await
        {
            Ok(response) => {
                let response = response.into_inner();
                let sandbox_ids = response
                    .refs
                    .iter()
                    .filter_map(claimed_ref_sandbox_id)
                    .collect::<BTreeSet<_>>();
                let refs = response
                    .refs
                    .into_iter()
                    .map(dirty_state::FacetRef::from_proto)
                    .collect::<Result<Vec<_>, _>>();
                let refs = match refs {
                    Ok(refs) => refs,
                    Err(error) => {
                        tracing::warn!(%error, "cluster returned invalid queued reconcile ref");
                        return None;
                    }
                };
                pplx_space_metrics::reconcile::claimed(refs.len() as u64);
                if refs.is_empty() {
                    None
                } else {
                    Some(ClusterReconcileClaim {
                        claim_token: response.claim_token,
                        refs,
                        origins: response.origin_traceparents,
                        sandbox_ids,
                    })
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to claim queued reconcile refs");
                None
            }
        }
    }

    /// Apply one claim of cluster refs and complete it, under the pass root
    /// [`ClusterPass`] describes.
    async fn apply_cluster_claim(
        &self,
        cause: &'static str,
        mut claim: ClusterReconcileClaim,
        shutdown_token: CancellationToken,
    ) {
        let refs = claim.refs.len();
        let sandbox_ids = std::mem::take(&mut claim.sandbox_ids);
        let origins = std::mem::take(&mut claim.origins);
        let span = (!origins.is_empty()).then(|| {
            let span = ClusterPass::span(&self.node_id, cause, refs, &sandbox_ids);
            pplx_space_common_svc::observability::trace_propagation::link_root_to_origins(
                &span,
                origins.iter().map(String::as_str),
            );
            span
        });
        match span {
            Some(span) => {
                let failed = self
                    .run_and_complete_cluster_claim(claim, shutdown_token)
                    .instrument(span.clone())
                    .await;
                ClusterPass::record_outcome(&span, refs, failed);
            }
            None => {
                let failed = self
                    .run_and_complete_cluster_claim(claim, shutdown_token)
                    .await;
                if failed > 0 {
                    let span = ClusterPass::span(&self.node_id, cause, refs, &sandbox_ids);
                    ClusterPass::record_outcome(&span, refs, failed);
                }
            }
        }
    }

    async fn run_and_complete_cluster_claim(
        &self,
        claim: ClusterReconcileClaim,
        shutdown_token: CancellationToken,
    ) -> usize {
        let report = self
            .run_inbound_reconcile(claim.refs.clone(), shutdown_token)
            .await;
        self.complete_cluster_reconcile_refs(claim, report).await
    }

    /// Report each claimed ref's outcome. Returns how many refs did not
    /// complete: those that failed, or every ref when the completion could
    /// not be sent and the claim is left to expire.
    async fn complete_cluster_reconcile_refs(
        &self,
        claim: ClusterReconcileClaim,
        report: BTreeMap<facets::FacetKind, Result<InboundFacetResult, String>>,
    ) -> usize {
        if claim.claim_token.is_empty() || claim.refs.is_empty() {
            return 0;
        }

        let refs = claim.refs.len();
        let (succeeded, failed) = partition_completed_refs(claim.refs, &report);
        let failed_refs = refs.saturating_sub(succeeded.len());

        let mut client = match connect_cluster_api().await {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(%error, "failed to connect to cluster API for queued reconcile completion");
                return refs;
            }
        };
        if let Err(error) = client
            .complete_node_reconcile_refs(CompleteNodeReconcileRefsRequest {
                node_id: self.node_id.clone(),
                claim_token: claim.claim_token,
                succeeded,
                failed,
                role: proto_node_role(self.node_role),
            })
            .await
        {
            tracing::warn!(%error, "failed to complete queued reconcile refs");
            return refs;
        }
        failed_refs
    }

    fn mark_reconcile_finished_for(
        &self,
        name: &str,
        stats: &ReconcileStats,
        error: Option<&str>,
    ) -> rusqlite::Result<()> {
        let now = now_secs();
        let success = error.is_none();
        let db = self
            .db
            .lock()
            .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
        db.execute(
            "UPDATE reconciliation_status
             SET last_completed_at = ?1,
                 last_success_at = CASE WHEN ?2 != 0 THEN ?1 ELSE last_success_at END,
                 running = 0,
                 templates_seen = ?3,
                 sandboxes_seen = ?4,
                 templates_applied = ?5,
                 sandboxes_applied = ?6,
                 last_error = ?7,
                 updated_at = ?1
             WHERE name = ?8",
            params![
                now,
                i64::from(success),
                stats.templates_seen.cast_signed(),
                stats.sandboxes_seen.cast_signed(),
                stats.templates_applied.cast_signed(),
                stats.sandboxes_applied.cast_signed(),
                error.unwrap_or(""),
                name,
            ],
        )?;
        Ok(())
    }
}

fn read_reconciliation_statuses(
    db: &Arc<Mutex<Connection>>,
) -> rusqlite::Result<Vec<ReconciliationStatus>> {
    let db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    let mut stmt = db.prepare(
        "SELECT name,
                poll_interval_secs,
                last_started_at,
                last_completed_at,
                last_success_at,
                next_due_at,
                running,
                templates_seen,
                sandboxes_seen,
                templates_applied,
                sandboxes_applied,
                last_error
         FROM reconciliation_status
         ORDER BY name ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ReconciliationStatus {
            name: row.get(0)?,
            poll_interval_secs: i64_to_u64(row.get(1)?),
            last_started_at_unix_secs: i64_to_u64(row.get(2)?),
            last_completed_at_unix_secs: i64_to_u64(row.get(3)?),
            last_success_at_unix_secs: i64_to_u64(row.get(4)?),
            next_due_at_unix_secs: i64_to_u64(row.get(5)?),
            running: row.get::<_, i64>(6)? != 0,
            templates_seen: i64_to_u64(row.get(7)?),
            sandboxes_seen: i64_to_u64(row.get(8)?),
            templates_applied: i64_to_u64(row.get(9)?),
            sandboxes_applied: i64_to_u64(row.get(10)?),
            last_error: row.get(11)?,
        })
    })?;
    rows.collect()
}

fn group_refs(refs: Vec<dirty_state::FacetRef>) -> BTreeMap<facets::FacetKind, facets::FacetScope> {
    let mut grouped: BTreeMap<facets::FacetKind, Option<BTreeSet<String>>> = BTreeMap::new();
    for reference in refs {
        let Some(facet) = facets::FacetKind::parse(&reference.facet) else {
            continue;
        };
        match reference.element {
            Some(element) => {
                if let Some(Some(elements)) = grouped.get_mut(&facet) {
                    elements.insert(element);
                } else {
                    grouped.entry(facet).or_insert_with(|| {
                        let mut elements = BTreeSet::new();
                        elements.insert(element);
                        Some(elements)
                    });
                }
            }
            None => {
                grouped.insert(facet, None);
            }
        }
    }
    grouped
        .into_iter()
        .map(|(facet, items)| {
            (
                facet,
                items.map_or(facets::FacetScope::Full, |items| {
                    facets::FacetScope::Items(items.into_iter().collect::<Vec<_>>())
                }),
            )
        })
        .collect()
}

fn proto_facet_ref(reference: &dirty_state::FacetRef) -> Result<ProtoFacetRef, String> {
    reference.to_proto()
}

/// The canonical id of the sandbox a claimed ref names, if it names one.
fn claimed_ref_sandbox_id(reference: &ProtoFacetRef) -> Option<String> {
    use pplx_space_node_api_client::proto::{facet_ref, network_policy_target_ref};
    let sandbox_id = match reference.element.as_ref()? {
        facet_ref::Element::SandboxId(sandbox_id) => sandbox_id,
        facet_ref::Element::NetworkPolicyTarget(target) => match target.target.as_ref()? {
            network_policy_target_ref::Target::SandboxId(sandbox_id) => sandbox_id,
            network_policy_target_ref::Target::Template(_) => return None,
        },
        facet_ref::Element::Template(_) => return None,
    };
    sandbox_id
        .parse::<pplx_space_common_core::types::SandboxId>()
        .ok()
        .map(|sandbox_id| sandbox_id.to_string())
}

fn partition_completed_refs(
    refs: Vec<dirty_state::FacetRef>,
    report: &BTreeMap<facets::FacetKind, Result<InboundFacetResult, String>>,
) -> (Vec<ProtoFacetRef>, Vec<FailedNodeReconcileRef>) {
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    for reference in refs {
        let proto_ref = match proto_facet_ref(&reference) {
            Ok(proto_ref) => proto_ref,
            Err(error) => {
                tracing::warn!(%error, "failed to encode claimed reconcile ref");
                continue;
            }
        };
        let error =
            match facets::FacetKind::parse(&reference.facet).and_then(|facet| report.get(&facet)) {
                Some(Err(error)) => Some(error.clone()),
                Some(Ok(result)) => match reference.element.as_deref() {
                    Some(element) => result.element_errors.get(element).cloned(),
                    None if result.element_errors.is_empty() => None,
                    None => Some(format!(
                        "failed to reconcile {} {} item(s)",
                        result.element_errors.len(),
                        reference.facet
                    )),
                },
                None => Some("node-ctl did not run claimed reconcile ref".to_string()),
            };
        if let Some(error) = error {
            failed.push(FailedNodeReconcileRef {
                r#ref: Some(proto_ref),
                error,
            });
        } else {
            succeeded.push(proto_ref);
        }
    }
    (succeeded, failed)
}

fn inbound_reconciler_name(facet: facets::FacetKind) -> &'static str {
    match facet {
        facets::FacetKind::NetworkPolicy => NETWORK_POLICY_RECONCILER,
        facets::FacetKind::SandboxDesiredState => SANDBOX_DESIRED_STATE_RECONCILER,
        facets::FacetKind::TemplateBaseSnapshotPolicy => TEMPLATE_BASE_SNAPSHOT_POLICY_RECONCILER,
        facets::FacetKind::TemplateDesiredState => TEMPLATE_DESIRED_STATE_RECONCILER,
        facets::FacetKind::RuntimeProvisioning => RUNTIME_PROVISIONING_RECONCILER,
        facets::FacetKind::SandboxInventory
        | facets::FacetKind::TemplateInventory
        | facets::FacetKind::NodeStatus => NODE_STATE_OUTBOUND_RECONCILER,
    }
}

fn outbound_reconciler_name(facet: facets::FacetKind) -> &'static str {
    match facet {
        facets::FacetKind::NetworkPolicy => NETWORK_POLICY_OUTBOUND_RECONCILER,
        facets::FacetKind::SandboxInventory => SANDBOX_INVENTORY_OUTBOUND_RECONCILER,
        facets::FacetKind::TemplateInventory => TEMPLATE_INVENTORY_OUTBOUND_RECONCILER,
        facets::FacetKind::NodeStatus => NODE_STATUS_OUTBOUND_RECONCILER,
        facets::FacetKind::SandboxDesiredState
        | facets::FacetKind::TemplateBaseSnapshotPolicy
        | facets::FacetKind::TemplateDesiredState
        | facets::FacetKind::RuntimeProvisioning => NODE_STATE_OUTBOUND_RECONCILER,
    }
}

fn build_controller_facets(
    reconciliations: &[ReconciliationStatus],
    sync_overrides: &[FacetSyncOverride],
    facet_filter: Option<&str>,
) -> Vec<FacetControllerState> {
    let status_by_name = reconciliations
        .iter()
        .map(|status| (status.name.as_str(), status))
        .collect::<BTreeMap<_, _>>();

    facets::FACETS
        .iter()
        .copied()
        .filter(|facet| facet_filter.is_none_or(|filter| filter == facet.name()))
        .map(|facet| {
            let inbound = facet
                .supports_inbound()
                .then(|| {
                    status_by_name
                        .get(inbound_reconciler_name(facet))
                        .copied()
                        .cloned()
                })
                .flatten();
            let outbound = facet
                .supports_outbound()
                .then(|| {
                    status_by_name
                        .get(outbound_reconciler_name(facet))
                        .copied()
                        .cloned()
                })
                .flatten();
            FacetControllerState {
                facet: facet.name().to_string(),
                supports_inbound: facet.supports_inbound(),
                supports_outbound: facet.supports_outbound(),
                inbound,
                outbound,
                inbound_pause: find_pause(
                    sync_overrides,
                    facet.name(),
                    dirty_state::SyncDirection::Inbound,
                ),
                outbound_pause: find_pause(
                    sync_overrides,
                    facet.name(),
                    dirty_state::SyncDirection::Outbound,
                ),
            }
        })
        .collect()
}

fn find_pause(
    sync_overrides: &[FacetSyncOverride],
    facet: &str,
    direction: dirty_state::SyncDirection,
) -> Option<FacetSyncOverride> {
    let direction = FacetDirection::from(direction) as i32;
    sync_overrides
        .iter()
        .find(|pause| pause.facet == facet && pause.direction == direction)
        .cloned()
}

fn cluster_sync_configured() -> bool {
    CLUSTER_NODE_API_ENDPOINT
        .get()
        .and_then(|endpoint| endpoint.as_deref())
        .is_some()
}

fn i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

type TracedClusterApiClient = ClusterApiClient<
    pplx_space_common_svc::bootstrap::GrpcClientTrace<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            pplx_space_common_svc::auth::SpaceBearerInterceptor,
        >,
    >,
>;

/// A cached cluster-api client plus the instant after which `get` stops
/// handing it out. Expiry only affects *new* requests: RPCs already running on
/// a clone of the expired client keep their channel alive until they finish.
struct ExpiringClient<T> {
    client: T,
    expires_at: Instant,
}

impl<T: Clone> ExpiringClient<T> {
    fn new(client: T, now: Instant, max_age: Duration) -> Self {
        Self {
            client,
            expires_at: now + max_age,
        }
    }

    fn get(&self, now: Instant) -> Option<T> {
        (now < self.expires_at).then(|| self.client.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientCacheOutcome {
    Hit,
    Rotated,
}

impl ClientCacheOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Rotated => "rotated",
        }
    }
}

impl std::fmt::Display for ClientCacheOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

struct ExpiringClientCache<T> {
    cached: tokio::sync::Mutex<Option<ExpiringClient<T>>>,
}

impl<T: Clone> ExpiringClientCache<T> {
    const fn new() -> Self {
        Self {
            cached: tokio::sync::Mutex::const_new(None),
        }
    }

    /// Single-flight get-or-rotate: the lock is held across the expiry check,
    /// `build`, and the store, so concurrent callers that observe an expired
    /// client build exactly one replacement instead of one channel each.
    /// TLS trust is read asynchronously once per replacement. Holding an async
    /// lock keeps concurrent rotations single-flight without blocking an executor.
    async fn get_or_rotate<E, F: std::future::Future<Output = Result<T, E>>>(
        &self,
        now: Instant,
        max_age: impl FnOnce() -> Duration,
        build: impl FnOnce() -> F,
    ) -> Result<(T, ClientCacheOutcome), E> {
        let mut cached = self.cached.lock().await;
        if let Some(client) = cached.as_ref().and_then(|cached| cached.get(now)) {
            return Ok((client, ClientCacheOutcome::Hit));
        }
        let client = build().await?;
        *cached = Some(ExpiringClient::new(client.clone(), now, max_age()));
        Ok((client, ClientCacheOutcome::Rotated))
    }
}

/// Uniformly jittered client max-age in `[max_age, max_age + jitter]`.
fn jittered_max_age(max_age: Duration, jitter: Duration) -> Duration {
    max_age + jitter.mul_f64(rand::random::<f64>())
}

#[derive(Debug, thiserror::Error)]
enum ClusterApiConnectError {
    #[error("{SPACE_INTERNAL_API_ENDPOINT_ENV} or /space/config/nodectl.toml is not configured")]
    MissingEndpoint,
    #[error("invalid cluster API endpoint: {0}")]
    InvalidEndpoint(#[source] tonic::transport::Error),
    #[error("cluster API transport configuration: {0}")]
    Transport(#[source] pplx_space_common_grpc::internal_transport::TransportError),
}

async fn connect_cluster_api() -> Result<TracedClusterApiClient, ClusterApiConnectError> {
    let endpoint = CLUSTER_NODE_API_ENDPOINT
        .get()
        .and_then(|endpoint| endpoint.as_deref())
        .ok_or(ClusterApiConnectError::MissingEndpoint)?;
    static CLIENT_CACHE: ExpiringClientCache<TracedClusterApiClient> = ExpiringClientCache::new();
    let (client, outcome) = CLIENT_CACHE
        .get_or_rotate(
            Instant::now(),
            || jittered_max_age(CLUSTER_CLIENT_MAX_AGE, CLUSTER_CLIENT_MAX_AGE_JITTER),
            || build_cluster_api_client(endpoint),
        )
        .await?;
    pplx_space_metrics::grpc::client_channel_cache("cluster-api", outcome.as_str());
    if outcome == ClientCacheOutcome::Rotated {
        tracing::debug!("rotated cluster API client channel");
    }
    Ok(client)
}

async fn build_cluster_api_client(
    endpoint: &str,
) -> Result<TracedClusterApiClient, ClusterApiConnectError> {
    let client = {
        let endpoint = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(ClusterApiConnectError::InvalidEndpoint)?
            .tcp_nodelay(true)
            .connect_timeout(CLUSTER_CONNECT_TIMEOUT)
            .timeout(CLUSTER_RPC_TIMEOUT);
        let channel = pplx_space_common_grpc::internal_transport::client_connect_endpoint(
            pplx_space_common_grpc::internal_transport::Service::ClusterApi,
            endpoint,
        )
        .await
        .map_err(ClusterApiConnectError::Transport)?
        .connect_lazy();
        // The cluster-api bearer is attached by the channel interceptor, so a
        // local token read/mount failure fails the RPC with `Status::unavailable`
        // instead of a header-less request the server would reject as a 401.
        let channel = tonic::service::interceptor::InterceptedService::new(
            channel,
            pplx_space_common_svc::auth::SpaceBearerInterceptor::new(
                pplx_space_common_svc::bearer_token::SpaceBearerToken::ClusterApi,
            ),
        );
        ClusterApiClient::new(pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(
            channel,
        ))
    };
    Ok(client)
}

fn local_manager_request<T>(message: T, sync_cluster: bool) -> Request<T> {
    let mut request = Request::new(message);
    if !sync_cluster {
        request.metadata_mut().insert(
            SKIP_CLUSTER_SYNC_METADATA,
            "true".parse().expect("static metadata value must parse"),
        );
    }
    request
}

#[cfg(test)]
mod tests;

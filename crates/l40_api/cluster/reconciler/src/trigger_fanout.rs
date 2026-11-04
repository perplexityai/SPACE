//! Cluster-side fanout for immediate node reconciliation triggers.
//!
//! API handlers enqueue durable trigger intents in Postgres after they enqueue
//! node reconcile refs. This cluster-singleton reconciler turns those intents
//! into best-effort node-api pokes without putting node-api latency on REST
//! request paths.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::{NodeManagedBy, NodeRole as SpaceNodeRole};
use pplx_space_common_svc::types::ReconcileReason;
use pplx_space_node_api_client::proto::{
    StateReconciliationReason as ProtoReconcileReason, TriggerStateReconciliationRequest,
};
use pplx_space_node_api_client::{BuilderApiClient, NodeApiClient, WorkerApiClient};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

use pplx_space_cluster_trigger_queue::{
    ClaimedStateReconciliationTrigger, StateReconciliationTriggerClaim, TriggerTargetRow,
    claim_triggers, complete_claim as complete_claim_rows,
    dead_letter_claim as dead_letter_claim_rows, fetch_trigger_targets,
    sweep_dead_triggers as sweep_dead_trigger_rows,
};

const DEFAULT_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_BATCH_SIZE: u32 = 128;
const DEFAULT_MAX_BATCHES_PER_TICK: u32 = 16;
const DEFAULT_CONCURRENCY: usize = 64;
const DEFAULT_DEAD_TRIGGER_REAP_INTERVAL: Duration = Duration::from_secs(300);
const DEFAULT_DEAD_TRIGGER_REAP_AFTER: Duration = Duration::from_secs(3600);
const DEFAULT_DEAD_TRIGGER_REAP_LIMIT: i64 = 512;
const NODE_API_TRIGGER_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const NODE_API_TRIGGER_RPC_TIMEOUT: Duration = Duration::from_secs(2);
const TRIGGER_CLIENT_CACHE_MAX_ENTRIES: usize = 1024;
const TRIGGER_CLIENT_CACHE_IDLE_TTL: Duration = Duration::from_secs(600);

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub interval: Duration,
    pub batch_size: u32,
    pub max_batches_per_tick: u32,
    pub concurrency: usize,
    pub dead_trigger_reap_interval: Duration,
    pub dead_trigger_reap_after: Duration,
    pub dead_trigger_reap_limit: i64,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
            max_batches_per_tick: DEFAULT_MAX_BATCHES_PER_TICK,
            concurrency: DEFAULT_CONCURRENCY,
            dead_trigger_reap_interval: DEFAULT_DEAD_TRIGGER_REAP_INTERVAL,
            dead_trigger_reap_after: DEFAULT_DEAD_TRIGGER_REAP_AFTER,
            dead_trigger_reap_limit: DEFAULT_DEAD_TRIGGER_REAP_LIMIT,
        }
    }
}

pub struct TriggerNodeApiTarget<'a> {
    pub managed_by: NodeManagedBy,
    pub role: SpaceNodeRole,
    pub direct_endpoint: Option<&'a str>,
    pub node_name: Option<&'a str>,
}

#[allow(async_fn_in_trait)]
pub trait TriggerEndpointResolver: Send + Sync {
    fn live_node_names(&self) -> Option<HashSet<String>>;

    async fn try_resolve_node_api_endpoint(
        &self,
        target: TriggerNodeApiTarget<'_>,
    ) -> Result<String, TriggerResolveError>;
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct TriggerResolveError {
    message: String,
}

impl TriggerResolveError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TriggerError {
    #[error("unsupported node role {0:?}")]
    UnknownRole(SpaceNodeRole),
    #[error("resolve node-api endpoint: {0}")]
    Resolve(#[source] TriggerResolveError),
    #[error("invalid node-api endpoint {endpoint}: {error}")]
    InvalidEndpoint {
        endpoint: String,
        #[source]
        error: tonic::transport::Error,
    },
    #[error("trigger node-api endpoint {endpoint}: {status}")]
    Rpc {
        endpoint: String,
        #[source]
        status: tonic::Status,
    },
}

impl TriggerError {
    fn is_permanent(&self) -> bool {
        matches!(self, Self::UnknownRole(_) | Self::InvalidEndpoint { .. })
    }

    fn class(&self) -> &'static str {
        match self {
            Self::UnknownRole(_) => "unknown_role",
            Self::Resolve(_) => "resolve",
            Self::InvalidEndpoint { .. } => "invalid_endpoint",
            Self::Rpc { status, .. } if is_retrying_client_connect_failure(status) => "connect",
            Self::Rpc { .. } => "rpc",
        }
    }
}

fn is_retrying_client_connect_failure(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable && status.message().contains("connect failed:")
}

#[derive(Clone)]
struct CachedTriggerNodeApiClient {
    client: Arc<Mutex<NodeApiClient>>,
    last_used: Instant,
}

#[derive(Default)]
struct TriggerNodeApiClientCache {
    clients: Mutex<BTreeMap<String, CachedTriggerNodeApiClient>>,
}

#[derive(Clone, Debug)]
struct TriggerReasonPayload {
    reasons: Vec<ReconcileReason>,
    diagnostic_reason: String,
}

impl TriggerReasonPayload {
    fn diagnostic_reason(&self) -> &str {
        if self.diagnostic_reason.is_empty() {
            "unspecified"
        } else {
            &self.diagnostic_reason
        }
    }
}

impl From<&ClaimedStateReconciliationTrigger> for TriggerReasonPayload {
    fn from(trigger: &ClaimedStateReconciliationTrigger) -> Self {
        Self {
            reasons: trigger.reasons.clone(),
            diagnostic_reason: trigger.diagnostic_reason.clone(),
        }
    }
}

impl TriggerNodeApiClientCache {
    async fn client(&self, endpoint: &str) -> Result<Arc<Mutex<NodeApiClient>>, TriggerError> {
        let now = Instant::now();
        {
            let mut clients = self.clients.lock().await;
            clients.retain(|_, cached| {
                now.duration_since(cached.last_used) <= TRIGGER_CLIENT_CACHE_IDLE_TTL
            });
            if let Some(cached) = clients.get_mut(endpoint) {
                cached.last_used = now;
                return Ok(cached.client.clone());
            }
        }

        let client = Arc::new(Mutex::new(NodeApiClient::new(node_endpoint(endpoint)?)));
        let mut clients = self.clients.lock().await;
        if clients.len() >= TRIGGER_CLIENT_CACHE_MAX_ENTRIES
            && let Some(oldest) = clients
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(endpoint, _)| endpoint.clone())
        {
            clients.remove(&oldest);
        }
        clients.insert(
            endpoint.to_string(),
            CachedTriggerNodeApiClient {
                client: client.clone(),
                last_used: now,
            },
        );
        Ok(client)
    }
}

pub async fn run<R>(
    pool: PgPool,
    resolver: Arc<R>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) where
    R: TriggerEndpointResolver + 'static,
{
    tracing::info!(
        interval_ms = config.interval.as_millis(),
        batch_size = config.batch_size,
        max_batches_per_tick = config.max_batches_per_tick,
        concurrency = config.concurrency,
        dead_trigger_reap_interval_secs = config.dead_trigger_reap_interval.as_secs(),
        dead_trigger_reap_after_secs = config.dead_trigger_reap_after.as_secs(),
        dead_trigger_reap_limit = config.dead_trigger_reap_limit,
        "starting state-reconciliation trigger fanout"
    );

    let client_cache = Arc::new(TriggerNodeApiClientCache::default());
    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut dead_reap_interval = tokio::time::interval(config.dead_trigger_reap_interval);
    dead_reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                drain_ready_triggers(&pool, resolver.clone(), client_cache.clone(), &config).await;
                tracing::debug!(
                    elapsed_ms = started.elapsed().as_millis(),
                    "state-reconciliation trigger fanout tick completed"
                );
            }
            _ = dead_reap_interval.tick() => {
                sweep_dead_trigger_rows_for_config(&pool, &config).await;
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("state-reconciliation trigger fanout stopping");
                return;
            }
        }
    }
}

async fn drain_ready_triggers<R>(
    pool: &PgPool,
    resolver: Arc<R>,
    client_cache: Arc<TriggerNodeApiClientCache>,
    config: &ReconcilerConfig,
) where
    R: TriggerEndpointResolver + 'static,
{
    for _ in 0..config.max_batches_per_tick {
        let claim = match claim_triggers(pool, config.batch_size).await {
            Ok(claim) => claim,
            Err(error) => {
                tracing::warn!(%error, "failed to claim state-reconciliation triggers");
                return;
            }
        };
        if claim.triggers.is_empty() {
            return;
        }
        process_claim(
            pool,
            resolver.clone(),
            client_cache.clone(),
            claim,
            config.concurrency,
        )
        .await;
    }
    tracing::debug!(
        max_batches_per_tick = config.max_batches_per_tick,
        "state-reconciliation trigger fanout left ready rows for the next tick"
    );
}

async fn process_claim<R>(
    pool: &PgPool,
    resolver: Arc<R>,
    client_cache: Arc<TriggerNodeApiClientCache>,
    claim: StateReconciliationTriggerClaim,
    concurrency: usize,
) where
    R: TriggerEndpointResolver + 'static,
{
    let started = Instant::now();
    let reason_by_node = claim
        .triggers
        .iter()
        .map(|trigger| (trigger.node_id, TriggerReasonPayload::from(trigger)))
        .collect::<BTreeMap<_, _>>();
    let claimed = reason_by_node.len() as u64;
    let node_ids = reason_by_node.keys().copied().collect::<Vec<_>>();
    let live_node_names = resolver.live_node_names();
    let targets = match fetch_trigger_targets(pool, &node_ids, live_node_names.as_ref()).await {
        Ok(targets) => targets,
        Err(error) => {
            let failed = node_ids
                .into_iter()
                .map(|node_id| (node_id, format!("load trigger target: {error}")))
                .collect::<Vec<_>>();
            complete_claim_best_effort(pool, &claim.claim_token, &[], &failed).await;
            pplx_space_metrics::reconcile::trigger_fanout_batch(claimed, started);
            return;
        }
    };

    let permanently_unavailable_node_ids = targets
        .permanently_unavailable
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let target_node_ids = targets
        .triggerable
        .iter()
        .map(|target| target.id)
        .collect::<BTreeSet<_>>();
    let succeeded_without_poke = node_ids
        .iter()
        .copied()
        .filter(|node_id| {
            !target_node_ids.contains(node_id)
                && !permanently_unavailable_node_ids.contains(node_id)
        })
        .collect::<Vec<_>>();
    for node_id in &succeeded_without_poke {
        let reason = reason_by_node
            .get(node_id)
            .map(TriggerReasonPayload::diagnostic_reason)
            .unwrap_or("unspecified");
        tracing::warn!(
            %node_id,
            reason,
            "dropping state-reconciliation trigger for non-triggerable node"
        );
    }
    let mut permanent_failed = targets
        .permanently_unavailable
        .into_iter()
        .map(|node_id| {
            (
                node_id,
                "k8s node is absent from the live Kubernetes node cache".to_string(),
            )
        })
        .collect::<Vec<_>>();
    for (node_id, error) in &permanent_failed {
        let reason = reason_by_node
            .get(node_id)
            .map(TriggerReasonPayload::diagnostic_reason)
            .unwrap_or("unspecified");
        pplx_space_metrics::reconcile::trigger_fanout("permanent_error", "target_absent");
        tracing::warn!(
            %node_id,
            reason,
            %error,
            "state-reconciliation trigger target permanently unavailable"
        );
    }

    let results =
        stream::iter(targets.triggerable)
            .map(|target| {
                let resolver = resolver.clone();
                let client_cache = client_cache.clone();
                let reason = reason_by_node.get(&target.id).cloned().unwrap_or_else(|| {
                    TriggerReasonPayload {
                        reasons: Vec::new(),
                        diagnostic_reason: "unspecified".to_string(),
                    }
                });
                async move {
                    let node_id = target.id;
                    trigger_target(resolver.as_ref(), client_cache.as_ref(), target, &reason)
                        .await
                        .map(|_| node_id)
                        .map_err(|error| (node_id, error))
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

    let mut succeeded = succeeded_without_poke;
    let mut failed = Vec::new();
    for result in results {
        match result {
            Ok(node_id) => {
                pplx_space_metrics::reconcile::trigger_fanout("ok", "none");
                succeeded.push(node_id);
            }
            Err((node_id, error)) if error.is_permanent() => {
                pplx_space_metrics::reconcile::trigger_fanout("permanent_error", error.class());
                permanent_failed.push((node_id, error.to_string()));
            }
            Err((node_id, error)) => {
                pplx_space_metrics::reconcile::trigger_fanout("error", error.class());
                failed.push((node_id, error.to_string()));
            }
        }
    }
    dead_letter_permanent_claim_failures(pool, &claim.claim_token, &permanent_failed).await;
    complete_claim_best_effort(pool, &claim.claim_token, &succeeded, &failed).await;

    if !failed.is_empty() {
        for (node_id, error) in &failed {
            tracing::warn!(%node_id, %error, "state-reconciliation trigger failed");
        }
    }
    if !permanent_failed.is_empty() {
        for (node_id, error) in &permanent_failed {
            tracing::warn!(%node_id, %error, "state-reconciliation trigger permanently failed");
        }
    }
    pplx_space_metrics::reconcile::trigger_fanout_batch(claimed, started);
    tracing::info!(
        claimed = reason_by_node.len(),
        triggered = succeeded.len().saturating_sub(
            succeeded
                .iter()
                .filter(|node_id| !target_node_ids.contains(node_id))
                .count()
        ),
        skipped = succeeded
            .iter()
            .filter(|node_id| !target_node_ids.contains(node_id))
            .count(),
        failed = failed.len(),
        permanently_failed = permanent_failed.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "state-reconciliation trigger fanout completed"
    );
}

async fn trigger_target<R>(
    resolver: &R,
    client_cache: &TriggerNodeApiClientCache,
    target: TriggerTargetRow,
    reason: &TriggerReasonPayload,
) -> Result<(), TriggerError>
where
    R: TriggerEndpointResolver,
{
    let endpoint = resolver
        .try_resolve_node_api_endpoint(TriggerNodeApiTarget {
            managed_by: target.managed_by,
            role: target.role,
            direct_endpoint: target.direct_endpoint.as_deref(),
            node_name: target.node_name.as_deref(),
        })
        .await
        .map_err(TriggerError::Resolve)?;
    let request = TriggerStateReconciliationRequest {
        reasons: reason
            .reasons
            .iter()
            .copied()
            .map(proto_reconcile_reason)
            .map(i32::from)
            .collect(),
        diagnostic_reason: reason.diagnostic_reason().to_string(),
    };
    if target.role == SpaceNodeRole::Cluster {
        return Err(TriggerError::UnknownRole(target.role));
    }

    let client = client_cache.client(&endpoint).await?;
    let mut node_api = client.lock().await;
    match target.role {
        SpaceNodeRole::Worker => {
            let mut client = WorkerApiClient::new(&mut node_api);
            client
                .trigger_state_reconciliation(request, NODE_API_TRIGGER_RPC_TIMEOUT)
                .await
        }
        SpaceNodeRole::Builder => {
            let mut client = BuilderApiClient::new(&mut node_api);
            client
                .trigger_state_reconciliation(request, NODE_API_TRIGGER_RPC_TIMEOUT)
                .await
        }
        SpaceNodeRole::Cluster => unreachable!("cluster nodes are rejected above"),
    }
    .map(|_| ())
    .map_err(|status| TriggerError::Rpc { endpoint, status })
}

async fn complete_claim_best_effort(
    pool: &PgPool,
    claim_token: &str,
    succeeded: &[NodeId],
    failed: &[(NodeId, String)],
) {
    if let Err(error) = complete_claim_rows(pool, claim_token, succeeded, failed).await {
        tracing::warn!(%error, "failed to complete state-reconciliation trigger claim");
    }
}

async fn dead_letter_permanent_claim_failures(
    pool: &PgPool,
    claim_token: &str,
    failed: &[(NodeId, String)],
) {
    if failed.is_empty() {
        return;
    }
    if let Err(error) = dead_letter_claim_rows(pool, claim_token, failed).await {
        tracing::warn!(
            claim_token,
            %error,
            "failed to dead-letter permanent state-reconciliation trigger failures"
        );
    }
}

async fn sweep_dead_trigger_rows_for_config(pool: &PgPool, config: &ReconcilerConfig) {
    match sweep_dead_trigger_rows(
        pool,
        i64::try_from(config.dead_trigger_reap_after.as_secs()).unwrap_or(i64::MAX),
        config.dead_trigger_reap_limit,
    )
    .await
    {
        Ok(0) => {}
        Ok(deleted) => {
            tracing::info!(
                deleted,
                "deleted stale dead-lettered state-reconciliation triggers"
            );
        }
        Err(error) => {
            tracing::warn!(%error, "failed to sweep dead-lettered state-reconciliation triggers");
        }
    }
}

fn node_endpoint(endpoint: &str) -> Result<Endpoint, TriggerError> {
    let endpoint = pplx_space_common_grpc::internal_transport::qualify_endpoint(endpoint);
    Endpoint::from_shared(endpoint.clone())
        .map_err(|error| TriggerError::InvalidEndpoint { endpoint, error })
        .map(|endpoint| {
            endpoint
                .tcp_nodelay(true)
                .connect_timeout(NODE_API_TRIGGER_CONNECT_TIMEOUT)
                .timeout(NODE_API_TRIGGER_RPC_TIMEOUT)
        })
}

fn proto_reconcile_reason(reason: ReconcileReason) -> ProtoReconcileReason {
    match reason {
        ReconcileReason::NodeRegistered => ProtoReconcileReason::NodeRegistered,
        ReconcileReason::NodeUpdated => ProtoReconcileReason::NodeUpdated,
        ReconcileReason::ManualNodeStatusRefresh => ProtoReconcileReason::ManualNodeStatusRefresh,
        ReconcileReason::ManualNodeReconcile => ProtoReconcileReason::ManualNodeReconcile,
        ReconcileReason::ManualNodeNetworkPolicyReconcile => {
            ProtoReconcileReason::ManualNodeNetworkPolicyReconcile
        }
        ReconcileReason::NodeTemplatePulled => ProtoReconcileReason::NodeTemplatePulled,
        ReconcileReason::SandboxRegistered => ProtoReconcileReason::SandboxRegistered,
        ReconcileReason::SandboxPortExposureUpdated => {
            ProtoReconcileReason::SandboxPortExposureUpdated
        }
        ReconcileReason::SandboxNetworkPolicyUpdated => {
            ProtoReconcileReason::SandboxNetworkPolicyUpdated
        }
        ReconcileReason::SandboxDesiredStateUpdated => {
            ProtoReconcileReason::SandboxDesiredStateUpdated
        }
        ReconcileReason::TemplateNetworkPolicyUpdated => {
            ProtoReconcileReason::TemplateNetworkPolicyUpdated
        }
        ReconcileReason::TemplateDesiredStateUpdated => {
            ProtoReconcileReason::TemplateDesiredStateUpdated
        }
        ReconcileReason::TemplateBaseSnapshotPolicyUpdated => {
            ProtoReconcileReason::TemplateBaseSnapshotPolicyUpdated
        }
        ReconcileReason::TemplatePoolPolicyUpdated => {
            ProtoReconcileReason::TemplatePoolPolicyUpdated
        }
        ReconcileReason::TemplatePoolPolicyDeleted => {
            ProtoReconcileReason::TemplatePoolPolicyDeleted
        }
        ReconcileReason::NodeSandboxInventoryPublished => {
            ProtoReconcileReason::NodeSandboxInventoryPublished
        }
        ReconcileReason::NodeTemplateInventoryPublished => {
            ProtoReconcileReason::NodeTemplateInventoryPublished
        }
    }
}

#[cfg(test)]
mod tests;

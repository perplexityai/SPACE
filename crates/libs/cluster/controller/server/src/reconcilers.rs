//! Cluster-singleton reconciler chassis.
//!
//! Each reconciler runs as its own `tokio::spawn` task driven by a
//! `tokio::time::interval` (with `MissedTickBehavior::Delay`) and listens on a
//! child `CancellationToken` for graceful shutdown. To add a reconciler:
//!
//! 1. Implement the loop (e.g. in `crates/libs/...`) with a `Config::from_env()`
//!    helper for deploy-time knobs and a `run(pool, substrate, shutdown)` entry.
//! 2. Spawn it from `run_reconcilers` below as a `tokio::spawn(...)` line.
//! 3. Wire its env vars into the owning deployment configuration.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::{NodeApiTarget, Substrate, SubstrateError};
use pplx_space_api::egress_profiles::EgressProfiles;
use pplx_space_api::tls_interception_exemptions::TlsInterceptionExemptions;
use pplx_space_cluster_pool::WarmPoolStore;
use pplx_space_cluster_reconciler::{
    TriggerEndpointResolver, TriggerFanoutConfig, TriggerNodeApiTarget, metrics, trigger_fanout,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::admin_audit_retention;
use crate::data_plane_session_retention;
use crate::drained_node_terminator;
use crate::execution_materialization_backfill;
use crate::failed_restorable_repair;
use crate::kube_node_sync;
use crate::lost_started_sandboxes;
use crate::node_drain;
use crate::node_group_labeler;
use crate::node_group_mirror;
use crate::node_service_readiness;
use crate::observed_projection_gc;
use crate::orphaned_nodes;
use crate::orphaned_restore_pod_gc;
use crate::orphaned_sandboxes;
use crate::public_operation_dispatcher;
use crate::public_operation_retention;
use crate::reconcile_queue_gc;
use crate::snapshot_retention_sweeper;
use crate::snapshot_storage_sweeper;
use crate::started_sandbox_repair;
use crate::stuck_container_creating_gc;
use crate::stuck_snapshot_reaper;
use crate::suspended_execution_reaper;
use crate::tombstone_retention;
use crate::transient_lifecycle_sweeper;
use pplx_space_common_svc::sql::sandbox_residency::{NODE_CENSUS_RESIDENCY_ENV, ResidencyEvidence};

/// Bounded grace period for in-flight reconciler ticks to drain after the
/// shutdown token fires. Keeps the cluster-controller pod from getting stuck
/// past Kubernetes' termination grace period if a reconciler is wedged on a
/// hung kube/DB call — runtime drop will abort outstanding tasks once we
/// return.
pub(crate) const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// A fatal runtime condition that ends [`run_reconcilers`] before a requested
/// shutdown, so the controller process exits non-zero and Kubernetes restarts
/// it.
#[derive(Debug, thiserror::Error)]
pub enum ReconcilerRuntimeError {
    #[error("federated identity configuration failed: {0}")]
    FederatedIdentity(
        #[from] pplx_space_common_svc::federated_identity::FederatedIdentityPolicyError,
    ),
    /// A spawned reconciler task exited (returned or panicked) before the
    /// shutdown token fired — a dead cluster-singleton loop.
    #[error("a reconciler task exited before shutdown was requested")]
    TaskExited,
    #[error(
        "unrecognized SPACE_NODE_DRAIN_TERMINATOR_KIND {0:?} \
         (expected one of: aws, kube-delete, noop)"
    )]
    InvalidTerminatorKind(String),
    #[error("invalid or missing SPACE API route signing configuration: {0}")]
    RouteSigningConfiguration(
        #[from] pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error,
    ),
}

/// How [`run_reconcilers`] wound down on a requested shutdown — whether every
/// reconciler tick actually finished. The leader-election caller uses this to
/// decide whether it is safe to *release* the lease (only after a clean drain)
/// or must let it expire at its TTL (a wedged tick may still be detached and
/// running).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// Every reconciler task stopped within the drain grace period.
    Drained,
    /// The drain grace period elapsed with tasks still running; they are now
    /// detached and will be aborted at process exit.
    DrainTimedOut,
}

/// Selects the [`node_drain::InstanceTerminator`] implementation for
/// the `terminating -> deleted` transition. Read from
/// `SPACE_NODE_DRAIN_TERMINATOR_KIND`; defaults to [`TerminatorKind::Aws`]
/// for backwards compatibility with the original EKS-only deploy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminatorKind {
    Aws,
    KubeDelete,
    Noop,
}

impl TerminatorKind {
    /// Canonical names only — no aliases. The Helm RBAC templates
    /// compare against `kube-delete` exactly; accepting an alias here
    /// while the chart omitted it would leave the kube-delete backend
    /// 403-blocked at runtime. If the set grows, keep this match and
    /// the chart's `eq ... "kube-delete"` checks in lockstep.
    ///
    /// An unrecognized value is a startup error: falling back to `aws`
    /// would let a typo of `kube-delete` or `noop` select real instance
    /// termination.
    fn from_env() -> Result<Self, ReconcilerRuntimeError> {
        Self::parse(
            std::env::var("SPACE_NODE_DRAIN_TERMINATOR_KIND")
                .ok()
                .as_deref(),
        )
    }

    fn parse(raw: Option<&str>) -> Result<Self, ReconcilerRuntimeError> {
        match raw.map(str::trim) {
            None | Some("aws" | "") => Ok(Self::Aws),
            Some("kube-delete") => Ok(Self::KubeDelete),
            Some("noop") => Ok(Self::Noop),
            Some(other) => Err(ReconcilerRuntimeError::InvalidTerminatorKind(
                other.to_owned(),
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::KubeDelete => "kube-delete",
            Self::Noop => "noop",
        }
    }
}

#[derive(Clone)]
struct TriggerSubstrate {
    substrate: Arc<Substrate>,
}

impl TriggerSubstrate {
    fn new(substrate: Arc<Substrate>) -> Self {
        Self { substrate }
    }
}

impl TriggerEndpointResolver for TriggerSubstrate {
    fn live_node_names(&self) -> Option<std::collections::HashSet<String>> {
        self.substrate.live_node_names()
    }

    async fn try_resolve_node_api_endpoint(
        &self,
        target: TriggerNodeApiTarget<'_>,
    ) -> Result<String, pplx_space_cluster_reconciler::trigger_fanout::TriggerResolveError> {
        self.substrate
            .try_resolve_node_api_endpoint(&NodeApiTarget {
                managed_by: target.managed_by,
                role: target.role,
                direct_endpoint: target.direct_endpoint,
                substrate_node_id: target.node_name,
            })
            .await
            .map_err(resolve_error)
    }
}

fn resolve_error(
    error: SubstrateError,
) -> pplx_space_cluster_reconciler::trigger_fanout::TriggerResolveError {
    pplx_space_cluster_reconciler::trigger_fanout::TriggerResolveError::new(error.to_string())
}

fn build_if_enabled<T, E>(
    enabled: bool,
    build: impl FnOnce() -> Result<T, E>,
) -> Result<Option<T>, E> {
    if enabled { build().map(Some) } else { Ok(None) }
}

pub async fn run_reconcilers(
    pool: PgPool,
    credentials_pool: pplx_space_common_svc::sql::CredentialsPool,
    substrate: Substrate,
    tls_interception_exemptions: Option<TlsInterceptionExemptions>,
    egress_profiles: EgressProfiles,
    shutdown_token: CancellationToken,
) -> Result<ShutdownOutcome, ReconcilerRuntimeError> {
    // Share substrate clients/caches across all reconcilers in this binary so
    // they don't each open their own watchers or reconnect hot paths.
    let substrate = Arc::new(substrate);
    let trigger_substrate = Arc::new(TriggerSubstrate::new(substrate.clone()));
    let public_operation_dispatcher_config =
        public_operation_dispatcher::ReconcilerConfig::from_env();
    let retention_config = snapshot_retention_sweeper::ReconcilerConfig::from_env();
    // One SDK cache/poller for both consumers; disabled loops need neither.
    let feature_flags = if public_operation_dispatcher_config.enabled || retention_config.enabled {
        pplx_space_feature_flags::FeatureFlagClient::from_env()
    } else {
        pplx_space_feature_flags::FeatureFlagClient::disabled()
    };
    let native_public_operation_executor =
        build_if_enabled(public_operation_dispatcher_config.enabled, || {
            public_operation_dispatcher::NativePublicOperationExecutor::new(
                pool.clone(),
                substrate.as_ref().clone(),
                feature_flags.clone(),
                pplx_space_common_svc::federated_identity::FederatedIdentityPolicy::from_env()?
                    .grant_audience,
                tls_interception_exemptions,
                egress_profiles,
            )
            .map_err(ReconcilerRuntimeError::from)
        })?;
    let mut handles = vec![
        tokio::spawn(trigger_fanout::run(
            pool.clone(),
            trigger_substrate,
            TriggerFanoutConfig::default(),
            shutdown_token.child_token(),
        )),
        tokio::spawn(metrics::run_queue_metrics(
            pool.clone(),
            metrics::QueueMetricsConfig::default(),
            shutdown_token.child_token(),
        )),
        tokio::spawn(public_operation_retention::run_with_pool(
            public_operation_retention::ReconcilerConfig::from_env(),
            pool.clone(),
            shutdown_token.child_token(),
        )),
        tokio::spawn(data_plane_session_retention::run_with_pool(
            data_plane_session_retention::ReconcilerConfig::from_env(),
            pool.clone(),
            shutdown_token.child_token(),
        )),
        tokio::spawn(admin_audit_retention::run_with_pool(
            admin_audit_retention::ReconcilerConfig::from_env(),
            pool.clone(),
            shutdown_token.child_token(),
        )),
        tokio::spawn(observed_projection_gc::run_with_pool(
            observed_projection_gc::ReconcilerConfig::from_env(),
            pool.clone(),
            shutdown_token.child_token(),
        )),
        tokio::spawn(tombstone_retention::run_with_pool(
            tombstone_retention::ReconcilerConfig::from_env(),
            pool.clone(),
            shutdown_token.child_token(),
        )),
    ];
    if let Some(native_public_operation_executor) = native_public_operation_executor {
        // The cluster activation limiter exists in this process only while the
        // dispatcher does, so its heartbeat takes a sibling of the same token
        // the dispatcher runs on. Under leader election that token is the
        // lease's, so a replica that loses the lease stops vouching for a
        // limiter it no longer runs; with election disabled (a Direct process
        // outside a pod, or the kill-switch, which assume a single replica) it
        // is the process token. Either way the signal is scoped to a replica that
        // actually runs the limiter.
        //
        // This is the owning statement of what a *silent* cluster-controller
        // heartbeat means. Four independent causes, so absence alone never
        // identifies one: (1) no replica is running; (2) the dispatcher is
        // disabled, and the limiter with it; (3) under leader election only, no
        // replica currently holds the lease; or (4) the metric path itself is
        // broken. Cause 4 is the one this signal exists to expose, which is why
        // 1-3 have to be ruled out before absence can be read that way.
        //
        // space-api runs the other copy of this limiter and heartbeats it from
        // its own `AppState`, so its own service group is unaffected by causes
        // 1-3.
        handles.push(tokio::spawn(
            native_public_operation_executor
                .cluster_admission()
                .liveness_heartbeat(shutdown_token.child_token()),
        ));
        handles.push(tokio::spawn(public_operation_dispatcher::run(
            public_operation_dispatcher_config,
            pplx_space_api::public_operations::PublicOperationStore::new(pool.clone()),
            native_public_operation_executor,
            shutdown_token.child_token(),
        )));
    }

    // Snapshot-storage S3 sweeper. Substrate-agnostic — needs only
    // Postgres + S3, so it runs in both Direct and Kubernetes modes
    // by default. `SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED=false` disables it.
    // A missing bucket scope also refuses startup before AWS SDK init so an
    // OSS install without object storage remains inert.
    let sweeper_config = snapshot_storage_sweeper::ReconcilerConfig::from_env();
    if !sweeper_config.enabled {
        tracing::info!(
            "snapshot-storage-sweeper reconciler is disabled \
             (SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED=false); skipping AWS SDK init"
        );
    } else if sweeper_config.scoped_bucket.is_none() {
        tracing::error!(
            "snapshot-storage-sweeper is enabled but SPACE_SNAPSHOT_BUCKET is not set; \
             refusing to run without a bucket scope and skipping AWS SDK init"
        );
    } else {
        let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let backend =
            pplx_space_snapshot_storage::s3::S3StorageBackend::from_sdk_config(&aws_config);
        // The sweeper's candidate scan joins the credential-fenced
        // snapshot_storage_location_encrypted_fields sidecar, so its store
        // runs on the credentials lane rather than the rollout-default pool.
        let sink =
            snapshot_storage_sweeper::SnapshotStorageDbStore::new(credentials_pool.pool().clone());
        handles.push(tokio::spawn(snapshot_storage_sweeper::run(
            sweeper_config,
            sink,
            backend,
            shutdown_token.child_token(),
        )));
    }

    // Snapshot-retention sweeper: cluster-singleton that enforces a
    // keep-newest-N retention policy over still-live sandboxes' durable
    // snapshots, pruning the older ones (storage prefix + DB rows). The
    // reconciler is backend-neutral: it routes each prune through a
    // `StorageBackendRegistry` keyed by URI scheme. Today only the S3
    // backend is registered (when `SPACE_SNAPSHOT_BUCKET` is set);
    // additional backends (gs, ...) register here as they land. Off by
    // default; opt in per environment via
    // `SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED=true`. Skips AWS SDK init
    // in the disabled path.
    if retention_config.enabled {
        let mut registry = snapshot_retention_sweeper::backend::StorageBackendRegistry::new();
        if let Some(bucket) = retention_config.scoped_bucket.clone() {
            let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let s3 =
                pplx_space_snapshot_storage::s3::S3StorageBackend::from_sdk_config(&aws_config);
            registry.register(Arc::new(
                snapshot_retention_sweeper::backend::S3Backend::new(s3, Some(bucket)),
            ));
        } else {
            tracing::warn!(
                "snapshot-retention-sweeper enabled but SPACE_SNAPSHOT_BUCKET is unset; \
                 no s3 backend registered (the reconciler will refuse to run)"
            );
        }
        // Joins the credential-fenced encrypted-fields sidecar; same lane
        // rule as the storage sweeper above.
        let sink = snapshot_retention_sweeper::SnapshotRetentionDbStore::new(
            credentials_pool.pool().clone(),
        );
        handles.push(tokio::spawn(snapshot_retention_sweeper::run(
            retention_config,
            sink,
            registry,
            feature_flags.clone(),
            shutdown_token.child_token(),
        )));
    } else {
        tracing::info!(
            "snapshot-retention-sweeper reconciler is disabled \
             (SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED unset); skipping AWS SDK init"
        );
    }

    // Transient-lifecycle sweeper: cluster-wide backstop that bounds the
    // time any sandbox row can sit in a transient lifecycle (creating /
    // starting / pausing / resuming / suspending / restoring / stopping /
    // checkpointing). The same loop can also retry cleanup for k8s sandboxes
    // already marked failed but still carrying active execution rows. Runs for
    // both substrates because a stuck row outside any drain context still
    // wedges future operations on the row, and the sink only needs DB access +
    // `SubstrateAdapter` methods, which `Substrate` exposes
    // uniformly. Transient lifecycle aging remains default-disabled,
    // while failed-sandbox cleanup runs unless
    // `SPACE_FAILED_SANDBOX_CLEANUP_ENABLED=false`.
    handles.push(tokio::spawn(
        transient_lifecycle_sweeper::run_with_substrate(
            transient_lifecycle_sweeper::ReconcilerConfig::from_env(),
            pool.clone(),
            substrate.clone(),
            shutdown_token.child_token(),
        ),
    ));

    match substrate.as_kubernetes() {
        None => {
            tracing::info!("skipping Kubernetes-only reconcilers on direct substrate");
        }
        Some(kube) => {
            // Kubernetes executions are the only rows that record a pod.
            handles.push(tokio::spawn(
                execution_materialization_backfill::run_with_pool(
                    execution_materialization_backfill::ReconcilerConfig::default(),
                    pool.clone(),
                    shutdown_token.child_token(),
                ),
            ));
            let kube = Arc::new(kube.clone());
            let warm_pool_token = shutdown_token.child_token();
            let warm_pool_store = WarmPoolStore::new(pool.clone());
            let warm_pool_kube = kube.k8s.client.clone();
            let warm_pool_cache = crate::warm_pool::WarmPoolKubeCache {
                pods: crate::warm_pool::PodCache {
                    store: kube.sandbox_pod_store(),
                    ready: kube.readiness(),
                },
                nodes: kube.k8s.caches.nodes.clone(),
            };
            handles.push(tokio::spawn(async move {
                if let Err(err) = crate::warm_pool::run(
                    warm_pool_kube,
                    warm_pool_cache,
                    warm_pool_store,
                    substrate.clone(),
                    warm_pool_token,
                )
                .await
                {
                    tracing::warn!(error = %err, "warm pool reconciler exited");
                }
            }));

            handles.push(tokio::spawn(orphaned_sandboxes::run(
                pool.clone(),
                kube.clone(),
                orphaned_sandboxes::ReconcilerConfig::from_env(),
                shutdown_token.child_token(),
            )));

            // Suspended-execution reaper: retires stale `active = TRUE`
            // execution rows on `lifecycle = 'suspended'` sandboxes whose
            // pod is confirmed gone (the node-side auto-suspend path
            // converges only the lifecycle, leaving the execution row and
            // `active_node_id` behind), and clears the stale placement
            // bookkeeping. Default-disabled per environment via
            // `SPACE_SUSPENDED_EXECUTION_REAPER_ENABLED=true`; when
            // enabled, defaults to dry-run.
            handles.push(tokio::spawn(suspended_execution_reaper::run(
                pool.clone(),
                kube.clone(),
                suspended_execution_reaper::ReconcilerConfig::from_env(),
                shutdown_token.child_token(),
            )));

            // Started-sandbox repair: moves `lifecycle = 'started'`
            // Kubernetes sandboxes back to `suspended` only when there is no
            // active execution/pod IP, any tracked pod is confirmed missing,
            // and a current-cycle suspend snapshot is ready. Enabled by
            // default and applies its fenced write directly;
            // `SPACE_STARTED_SANDBOX_REPAIR_ENABLED=false` disables it, and any
            // other non-truthy value disables it too.
            handles.push(tokio::spawn(started_sandbox_repair::run(
                pool.clone(),
                kube.clone(),
                started_sandbox_repair::ReconcilerConfig::from_env(),
                shutdown_token.child_token(),
            )));

            // Failed-restorable repair: moves `lifecycle = 'failed'`
            // Kubernetes sandboxes back to `suspended` only when there is no
            // execution/pod-claim/observed-failed evidence left and a durable
            // suspend snapshot is provably safe to restore from
            // (activity-anchored — the failed transition poisons the
            // lifecycle/placement clocks). Recovers sandboxes wedged by
            // interrupted-restore debris materialized into failed rows.
            // Enabled by default and applies its fenced write directly;
            // `SPACE_FAILED_RESTORABLE_REPAIR_ENABLED=false` disables it, and any
            // other non-truthy value disables it too.
            handles.push(tokio::spawn(failed_restorable_repair::run(
                pool.clone(),
                failed_restorable_repair::ReconcilerConfig::from_env(),
                shutdown_token.child_token(),
            )));

            // Stuck-ContainerCreating pod GC: deletes sandbox pods wedged in
            // `ContainerCreating` past a threshold on an otherwise-healthy
            // node, breaking the kubelet/control-plane/volume-manager retry
            // storm a large backlog of stuck creates sustains. Pure
            // reflector-cache + apiserver work, no Postgres. Default-disabled;
            // starts in dry-run.
            handles.push(tokio::spawn(stuck_container_creating_gc::run(
                kube.clone(),
                stuck_container_creating_gc::ReconcilerConfig::from_env(),
                shutdown_token.child_token(),
            )));

            // Node-local service readiness: publishes whether each worker's
            // node-ctl/volume-manager DaemonSet pods are Ready onto the kube
            // Node, so placement drains off nodes whose daemons are restarting
            // during a worker DaemonSet rollout instead of stalling creates on
            // them. Soft signal only — nothing here makes a node
            // unschedulable. Default-disabled; starts in dry-run.
            handles.push(tokio::spawn(node_service_readiness::run(
                kube.clone(),
                node_service_readiness::ReconcilerConfig::from_env(),
                shutdown_token.child_token(),
            )));

            // Orphaned-restore-pod GC: reaps restore pods that are provably
            // superseded by their owning `sandboxes` row (row deleted, no
            // active execution, or active execution at a strictly-higher
            // generation) — the unrouted VM an interrupted source-pinned
            // restore leaves behind when its pinned pod binds after the row
            // moved on. Pod-cache-driven with a batched Postgres
            // classification; needs no DB-convergence companion (the row
            // already reflects the desired state). Default-disabled; starts
            // in dry-run.
            handles.push(tokio::spawn(orphaned_restore_pod_gc::run(
                pool.clone(),
                kube.clone(),
                orphaned_restore_pod_gc::ReconcilerConfig::from_env(),
                shutdown_token.child_token(),
            )));

            // Kube node sync: reconciles labeled kube Nodes into the SPACE DB.
            // It repairs missing registrations and demotes stale ready rows
            // after their kube Node disappears, while preserving the node row
            // and sandbox placements for normal recovery paths.
            handles.push(tokio::spawn(kube_node_sync::run(
                kube_node_sync::ReconcilerConfig::from_env(),
                kube.clone(),
                shutdown_token.child_token(),
            )));

            // Node-group label mirror: copies each kube Node's
            // `node-restriction.kubernetes.io/space-node-group` label into
            // `nodes.node_group_id` (the reporting mirror behind the
            // /admin/v1/node-groups surface). The label is the scheduling
            // source of truth; the column is never set by hand.
            // Default-disabled until the org-scoped node-group rollout
            // labels the shared worker pool.
            handles.push(tokio::spawn(node_group_mirror::run(
                node_group_mirror::ReconcilerConfig::from_env(),
                pool.clone(),
                kube.clone(),
                shutdown_token.child_token(),
            )));

            // Node-group labeler: the migration-era rollout of org-scoped
            // node groups — not open-ended self-healing. It labels only while
            // the default is the only group and never overwrites a label, so
            // it does not reconcile divergence. Seeds the singleton default
            // generic `node_groups` row and applies the
            // NodeRestriction-protected boundary label
            // (`node-restriction.kubernetes.io/space-node-group`) to shared
            // worker Nodes control-plane-side (a kubelet cannot self-set
            // labels under that prefix, so launch-template `--node-labels`
            // can never do this). It does not write `nodes.node_group_id` —
            // the `node_group_mirror` reconciler above is that column's only
            // writer, and it is default-disabled too, so both flags have to be
            // on for the column to populate.
            // Worker discovery scans the shared Node reflector store rather
            // than issuing a per-tick LIST of its own, and reports a cold
            // cache as an error rather than an empty fleet; the patches still go straight to the
            // apiserver.
            // Default-disabled via `SPACE_NODE_GROUP_LABELER_ENABLED`;
            // requires the node-groups schema — until that migration is
            // applied, each tick fails loudly and retries.
            handles.push(tokio::spawn(node_group_labeler::run(
                node_group_labeler::ReconcilerConfig::from_env(),
                node_group_labeler::NodeGroupDbStore::new(pool.clone()),
                node_group_labeler::KubeWorkerNodes::new(
                    kube.k8s.client.clone(),
                    kube.k8s.caches.clone(),
                    &kube.k8s.sandbox_pod_config,
                ),
                shutdown_token.child_token(),
            )));

            // Node-drain reconciler. Default-disabled; flips on via
            // `SPACE_NODE_DRAIN_ENABLED=true` per environment.
            //
            // Skip all AWS SDK init in the default-disabled path so a
            // broken IMDS / missing AWS_REGION environment can't slow
            // startup or surface confusing errors in clusters that
            // never intend to use node-drain. The terminator backend
            // is selected via `SPACE_NODE_DRAIN_TERMINATOR_KIND`:
            //
            // - `aws` (default): EC2 ASG TerminateInstanceInAutoScalingGroup.
            // - `kube-delete`: delete the kube Node object. Provider-
            //   agnostic; for kind/Tilt/self-managed clusters.
            // - `noop`: return Ok without touching infra. The
            //   reconciler still DELETEs the SPACE row, but the
            //   underlying compute is untouched. For e2e smoke tests
            //   that exercise the state machine without affecting
            //   capacity (the kube Node remains cordoned after — a
            //   manual uncordon is needed for re-use).
            // Residency evidence is read ONCE here and handed to every store
            // that answers "does this row still occupy the node?". The
            // node-drain gate and the drained-node terminator evaluate it at
            // consecutive gates, so a per-reconciler read would let one be
            // disabled and leave a node that converges to `drained` and then
            // parks there with nothing to advance it.
            let residency_evidence = ResidencyEvidence::from_env_value(
                std::env::var(NODE_CENSUS_RESIDENCY_ENV).ok().as_deref(),
            );
            tracing::info!(
                residency_evidence = %residency_evidence,
                env_var = NODE_CENSUS_RESIDENCY_ENV,
                "resolved node-census residency evidence for drain + terminate gates"
            );
            let node_drain_config = node_drain::ReconcilerConfig::from_env();
            // Read `enabled` once: `node_drain_config` is consumed by
            // the spawn arms below and re-read by the drained-node
            // terminator gate that follows the match.
            let node_drain_enabled = node_drain_config.enabled;
            // Resolved once, here, because the sink and the reconciler have to
            // agree on it — except that only one of them ends up reading it.
            // The sink decides whether `stopped` rows are listed at all. The
            // reconciler does **not** gate dispatch on the flag: it routes on
            // `pre_lifecycle == SandboxLifecycle::Stopped` alone, deliberately,
            // so an evacuation opened before the flag was turned off still
            // finishes (see the dispatch site). Resolving here anyway keeps the
            // WARN below and the sink's value derived from one expression; the
            // WARN is the one place that also reads the raw flag, to tell the
            // operator which half of the pair is missing.
            let evacuates_stopped_residents = node_drain_config.evacuates_stopped_residents();
            if node_drain_config.evacuate_stopped_residents && !evacuates_stopped_residents {
                // Worth a WARN rather than silence: the operator asked for a
                // behaviour they are not getting, and the reason is a second
                // flag rather than anything they can see in this one. Left
                // unstated, the symptom is a fleet where nothing changes and
                // no log says why.
                tracing::warn!(
                    "SPACE_NODE_DRAIN_EVACUATE_STOPPED_RESIDENTS is set but \
                     SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE is not; stopped-resident \
                     evacuation stays off. It only takes effect in a cluster that already holds \
                     draining nodes for stopped residents, so enable the termination-blocking \
                     gate first"
                );
            }
            tracing::info!(
                evacuate_stopped_residents = evacuates_stopped_residents,
                "resolved stopped-resident evacuation for the node-drain reconciler"
            );
            if node_drain_enabled {
                let kind = TerminatorKind::from_env()?;
                tracing::info!(
                    terminator = kind.as_str(),
                    "starting node-drain reconciler with selected terminator backend"
                );
                let node_drain_token = shutdown_token.child_token();
                // The sink needs the gate too: the completion-edge write's
                // residency guard is scoped by it, so a sink built without it
                // would enforce the term the flag withholds.
                let node_drain_sink = node_drain::SubstrateSink::new(
                    pool.clone(),
                    kube.clone(),
                    residency_evidence,
                    node_drain_config.termination_blocking_gate,
                    evacuates_stopped_residents,
                );
                match kind {
                    TerminatorKind::Aws => {
                        let aws_config =
                            aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                        let autoscaling_client =
                            pplx_space_aws_client::autoscaling::Client::from_config(&aws_config);
                        let terminator = node_drain::Ec2InstanceTerminator::new(
                            pool.clone(),
                            kube.k8s.client.clone(),
                            autoscaling_client,
                        );
                        // EC2 cross-check for the KubeNodeGone recovery on
                        // `draining` rows. Shares the production impl with
                        // `drained_node_terminator`; only the real-EC2
                        // terminator deploy can meaningfully query EC2, so the
                        // `kube-delete` / `noop` arms below pass `None` and
                        // keep the fail-safe parked behavior on a missing
                        // kube Node.
                        let ec2_client =
                            pplx_space_aws_client::ec2::Client::from_config(&aws_config);
                        let instance_lookup: Arc<dyn node_drain::InstanceStateLookup> =
                            Arc::new(drained_node_terminator::Ec2InstanceStateLookup::new(
                                pool.clone(),
                                Arc::new(ec2_client),
                            ));
                        handles.push(tokio::spawn(node_drain::run(
                            node_drain_config,
                            node_drain_sink,
                            terminator,
                            Some(instance_lookup),
                            node_drain_token,
                        )));
                    }
                    TerminatorKind::KubeDelete => {
                        let terminator = node_drain::KubeNodeDeleteTerminator::new(
                            pool.clone(),
                            kube.k8s.client.clone(),
                        );
                        handles.push(tokio::spawn(node_drain::run(
                            node_drain_config,
                            node_drain_sink,
                            terminator,
                            None,
                            node_drain_token,
                        )));
                    }
                    TerminatorKind::Noop => {
                        let terminator = node_drain::NoopInstanceTerminator::new();
                        handles.push(tokio::spawn(node_drain::run(
                            node_drain_config,
                            node_drain_sink,
                            terminator,
                            None,
                            node_drain_token,
                        )));
                    }
                }
            } else {
                tracing::info!(
                    "node-drain reconciler is disabled \
                     (SPACE_NODE_DRAIN_ENABLED unset); skipping terminator backend init"
                );
            }

            // Drained-node terminator. Companion to node-drain: owns
            // the `drained -> terminating` policy transition, honoring
            // the `space.pplx.net/drain-keep` operator override
            // annotation on the k8s Node.
            //
            // **Coupling to node-drain.** Although the env flags are
            // independent, this reconciler is useless without
            // `node_drain` running: nothing else picks up
            // `terminating` rows. If node-drain is disabled we
            // force-disable this loop and log a warning rather than
            // silently advancing rows into a state no one drains.
            let mut drained_terminator_config =
                drained_node_terminator::ReconcilerConfig::from_env();
            if drained_terminator_config.enabled && !node_drain_enabled {
                tracing::warn!(
                    "drained-node-terminator requested (SPACE_DRAINED_NODE_TERMINATOR_ENABLED=true) \
                     but node-drain is disabled (SPACE_NODE_DRAIN_ENABLED=false). \
                     Force-disabling drained-node-terminator: without node-drain, \
                     `terminating` rows would have no consumer and would stay stuck. \
                     Enable SPACE_NODE_DRAIN_ENABLED=true to use this reconciler."
                );
                drained_terminator_config.enabled = false;
            }
            // Orphaned-nodes reconciler. Sibling to `orphaned_sandboxes`:
            // hard-deletes `nodes` rows whose backing EC2 instance was
            // force-terminated outside the drain path (manual `terminate-
            // instances`, `terminate-instance-in-auto-scaling-group` from
            // a rollout, ASG-driven AMI rollouts pre-`SPACE_NODE_DRAIN_ENABLED`).
            //
            // Default-disabled per environment via
            // `SPACE_ORPHANED_NODES_ENABLED=true`. We skip AWS SDK init when
            // disabled so a broken IMDS / missing AWS_REGION can't slow
            // startup or surface confusing errors in environments that
            // don't intend to use this path.
            let orphaned_nodes_config = orphaned_nodes::ReconcilerConfig::from_env();
            if orphaned_nodes_config.enabled {
                let aws_config =
                    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                let ec2_client = pplx_space_aws_client::ec2::Client::from_config(&aws_config);
                let lookup = orphaned_nodes::Ec2InstanceLookup::new(Arc::new(ec2_client));
                let sink = orphaned_nodes::NodeDbStore::new(pool.clone());
                handles.push(tokio::spawn(orphaned_nodes::run(
                    orphaned_nodes_config,
                    sink,
                    lookup,
                    shutdown_token.child_token(),
                )));
            } else {
                tracing::info!(
                    "orphaned-nodes reconciler is disabled \
                     (SPACE_ORPHANED_NODES_ENABLED unset); skipping AWS SDK init"
                );
            }

            // Lost-started-sandbox give-up: flips `started` sandboxes whose
            // started recovery failed with no restore source to `failed` once
            // their source node is gone (row deleted, or absent from
            // Kubernetes with no live EC2 instance). Opt-in; the AWS SDK is
            // only initialized when it's enabled, like `orphaned_nodes`.
            let lost_started_config = lost_started_sandboxes::ReconcilerConfig::from_env();
            if lost_started_config.enabled {
                let aws_config =
                    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                let ec2_client = pplx_space_aws_client::ec2::Client::from_config(&aws_config);
                let lookup = orphaned_nodes::Ec2InstanceLookup::new(Arc::new(ec2_client));
                let sink = lost_started_sandboxes::LostStartedDbStore::new(pool.clone());
                let presence = reconcile_queue_gc::SubstrateKubeNodePresence::new(kube.clone());
                handles.push(tokio::spawn(lost_started_sandboxes::run(
                    lost_started_config,
                    sink,
                    lookup,
                    presence,
                    shutdown_token.child_token(),
                )));
            } else {
                tracing::info!(
                    "lost_started_sandboxes is disabled \
                     (SPACE_LOST_STARTED_SANDBOXES_ENABLED unset); skipping AWS SDK init"
                );
            }

            // Reconcile-queue GC — one reconciler, two detector instances.
            //
            // EC2 instance (`SPACE_RECONCILE_QUEUE_GC_*`): owns the dead-letter
            // pass and prunes live refs for nodes whose backing EC2 instance is
            // confirmed gone. Needs the AWS SDK for `DescribeInstances`.
            let ec2_gc_config = reconcile_queue_gc::ReconcilerConfig::from_env_ec2();
            if ec2_gc_config.enabled {
                let aws_config =
                    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                let ec2_client = pplx_space_aws_client::ec2::Client::from_config(&aws_config);
                let lookup = orphaned_nodes::Ec2InstanceLookup::new(Arc::new(ec2_client));
                let detector = reconcile_queue_gc::Ec2Detector::new(lookup);
                let sink = reconcile_queue_gc::QueueDbStore::new(pool.clone());
                handles.push(tokio::spawn(reconcile_queue_gc::run(
                    ec2_gc_config,
                    sink,
                    detector,
                    shutdown_token.child_token(),
                )));
            } else {
                tracing::info!(
                    "reconcile-queue GC (ec2 detector) is disabled; skipping its AWS SDK init"
                );
            }

            // Kubernetes-presence instance (`SPACE_GONE_NODE_QUEUE_GC_*`):
            // prunes live refs for `drain_phase IS NULL` k8s nodes whose Node
            // object is sustainably absent from the reflector and a direct
            // Kubernetes GET confirms it missing — the debt the EC2 detector
            // misses (its `nodes` row survives because non-terminal sandboxes
            // stay pinned to it, so it can't be deleted outright). Uses
            // Postgres + Kubernetes reads, with no AWS SDK.
            let kube_gc_config = reconcile_queue_gc::ReconcilerConfig::from_env_kube();
            if kube_gc_config.enabled {
                let presence = reconcile_queue_gc::SubstrateKubeNodePresence::new(kube.clone());
                let detector = reconcile_queue_gc::KubePresenceDetector::new(presence);
                let sink = reconcile_queue_gc::QueueDbStore::new(pool.clone());
                handles.push(tokio::spawn(reconcile_queue_gc::run(
                    kube_gc_config,
                    sink,
                    detector,
                    shutdown_token.child_token(),
                )));
            } else {
                tracing::info!(
                    "reconcile-queue GC (kube-presence detector) is disabled \
                     (the fixed-enabled gone-node queue GC=false)"
                );
            }

            let drained_terminator_sink =
                drained_node_terminator::NodeDbStore::new(pool.clone(), residency_evidence);
            let drained_annotation_lookup = drained_node_terminator::KubeAnnotationLookup::new(
                pool.clone(),
                kube.k8s.client.clone(),
            );
            // EC2 cross-check for the KubeNodeGone recovery path. Only the
            // real-EC2 (`aws`) terminator deploy can meaningfully query EC2;
            // on `kube-delete` / `noop` substrates (kind / Tilt / smokes)
            // there is no EC2, so we leave the cross-check unconfigured and
            // the reconciler preserves its fail-safe "park for manual review"
            // behavior on a missing k8s Node. Gated on the terminator already
            // being enabled so we skip AWS SDK init when the reconciler is
            // dormant (force-disabled above when node-drain is off).
            let drained_instance_lookup: Option<
                Arc<dyn drained_node_terminator::InstanceStateLookup>,
            > = if drained_terminator_config.enabled
                && TerminatorKind::from_env()? == TerminatorKind::Aws
            {
                let aws_config =
                    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                let ec2_client = pplx_space_aws_client::ec2::Client::from_config(&aws_config);
                Some(Arc::new(
                    drained_node_terminator::Ec2InstanceStateLookup::new(
                        pool.clone(),
                        Arc::new(ec2_client),
                    ),
                ))
            } else {
                None
            };
            handles.push(tokio::spawn(drained_node_terminator::run(
                drained_terminator_config,
                drained_terminator_sink,
                drained_annotation_lookup,
                drained_instance_lookup,
                shutdown_token.child_token(),
            )));
        }
    }

    // Stuck-snapshot reaper: cluster-wide backstop that terminalizes
    // suspend-snapshot rows wedged in `state = 'creating'` past the on-node
    // upload worker's full retry window (≈10h). Such a row blocks the source
    // node's drain indefinitely because the state-based gate keys "upload in
    // flight?" on `state = 'creating'`; flipping it to `failed` unblocks the
    // drain and re-enables the give-up / reupload path. Substrate-agnostic —
    // a pure Postgres CAS plus telemetry, no node interaction — so it runs in
    // both Direct and Kubernetes modes. Default-disabled per environment via
    // `SPACE_STUCK_SNAPSHOT_REAPER_ENABLED=true`.
    handles.push(tokio::spawn(stuck_snapshot_reaper::run_with_pool(
        stuck_snapshot_reaper::ReconcilerConfig::from_env(),
        pool.clone(),
        shutdown_token.child_token(),
    )));

    // Add future reconcilers above this line (snapshot retention, ...).

    // Supervise the spawned tasks. A reconciler loop runs until its shutdown
    // token fires; a *disabled* reconciler returns cleanly at startup (expected
    // — Helm defaults disable several of them). A task that *panics* while the
    // controller is still running is a dead cluster-singleton loop: fail fast so
    // Kubernetes restarts the pod and the startup gates re-evaluate (e.g. the
    // stuck-ContainerCreating GC's live-mode prerequisite, which trusts the
    // orphaned-sandbox / transient-lifecycle reconcilers to be running) rather
    // than limping with a crashed loop. A clean early return is NOT fatal — it's
    // how a disabled reconciler exits.
    //
    // Move each handle into a JoinSet preserving its join result, so a panic
    // (a `JoinError`) is distinguishable from a clean return, and a completion
    // can be awaited alongside the shutdown token while draining the rest.
    // Aborting a wrapper doesn't hard-abort its reconciler, but on exit we cancel
    // the shutdown token first, which stops every loop cooperatively.
    // A `JoinHandle` is itself a future resolving to the task's join result, so
    // spawning it into the set makes that result observable.
    let mut supervised: tokio::task::JoinSet<Result<(), tokio::task::JoinError>> =
        tokio::task::JoinSet::new();
    for handle in handles {
        supervised.spawn(handle);
    }

    let outcome = supervise_reconcilers(&mut supervised, &shutdown_token).await;

    // Cancel first so remaining loops stop cooperatively, then drain within the
    // grace budget (awaiting the wrappers, not aborting, so an in-flight tick
    // finishes before the process exits).
    shutdown_token.cancel();
    let drain = async { while supervised.join_next().await.is_some() {} };
    let drained_cleanly = tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, drain)
        .await
        .is_ok();
    if !drained_cleanly {
        tracing::warn!(
            timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
            "reconcilers did not drain within timeout; exiting"
        );
    }

    match outcome {
        Supervision::Shutdown => Ok(if drained_cleanly {
            ShutdownOutcome::Drained
        } else {
            ShutdownOutcome::DrainTimedOut
        }),
        Supervision::TaskExited => {
            tracing::error!(
                "a reconciler task exited before shutdown was requested; \
                 failing the controller so it restarts with a fresh reconciler set"
            );
            Err(ReconcilerRuntimeError::TaskExited)
        }
    }
}

/// Outcome of [`supervise_reconcilers`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Supervision {
    /// The shutdown token fired (or every supervised task cleanly exited) — a
    /// normal stop.
    Shutdown,
    /// A supervised task *panicked* before shutdown — a dead loop.
    TaskExited,
}

/// Wait until either the shutdown token fires or a supervised task panics.
///
/// A reconciler loop runs until its token fires; a *disabled* one returns
/// cleanly at startup, which is expected and NOT fatal — such completions are
/// drained and supervision continues watching the rest. Only a panic (a crashed
/// loop) is fatal. If every task cleanly exits (e.g. all disabled), the
/// controller waits for real shutdown rather than exiting.
async fn supervise_reconcilers(
    set: &mut tokio::task::JoinSet<Result<(), tokio::task::JoinError>>,
    shutdown_token: &CancellationToken,
) -> Supervision {
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => return Supervision::Shutdown,
            joined = set.join_next() => match joined {
                // Nothing left to supervise (all cleanly exited) — hold the
                // controller up until real shutdown rather than exiting.
                None => {
                    shutdown_token.cancelled().await;
                    return Supervision::Shutdown;
                }
                // A reconciler panicked — a dead loop. Fail fast. (Outer `Ok` is
                // the wrapper's own join; inner `Err` is the reconciler's.) Log
                // the `JoinError` — it carries the panicked task's id — to pair
                // with the panic the runtime's hook already logged, so operators
                // can identify which reconciler died.
                Some(Ok(Err(join_err))) if join_err.is_panic() => {
                    tracing::error!(error = %join_err, "a reconciler task panicked");
                    return Supervision::TaskExited;
                }
                // A clean return (e.g. a disabled reconciler) or a benign
                // cancellation — not fatal; keep watching the others.
                Some(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests;

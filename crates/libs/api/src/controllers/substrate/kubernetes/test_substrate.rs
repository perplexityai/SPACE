//! A [`KubernetesSubstrate`] built in process, for tests. This crate's tests
//! get it under `cfg(test)`; another crate's tests enable the `test-hooks`
//! feature through a dev-dependency.
//!
//! [`KubernetesSubstrate::try_from_env`] reads the process environment and
//! starts reflectors against a live apiserver. A substrate built here takes
//! its apiserver client from the caller and starts no reflector: its caches
//! are seeded once, warm, and are never fed afterwards.

use kube::runtime::{reflector, watcher::Event};

use super::execution::RecoveryJoinBackoffPolicy;
use super::*;
use crate::controllers::substrate::AbandonedRestoreRetirement;
use crate::controllers::substrate::runtime_upgrade::{
    RuntimePolicyCache, RuntimeUpgradeAtActivation,
};

/// The namespace a substrate built here keeps its sandbox pods and their
/// `sandbox_k8s_executions` rows in.
pub const TEST_SANDBOX_NAMESPACE: &str = "space";

impl KubernetesSubstrate {
    /// A substrate over `pool` whose apiserver is `client`, with every cache
    /// warm: the sandbox-pod cache and the node-api-pod cache each hold every
    /// pod in `pods`, whatever its labels, and the node cache holds no nodes.
    /// Pods `client` serves later reach the substrate only through its
    /// uncached reads.
    ///
    /// It dials a pod's sandbox proxy on `sandbox_proxy_port`, and a node's
    /// node-api on the port its node-api pod advertises, or on
    /// `sandbox_proxy_port` when that pod advertises none. Its create,
    /// data-plane, delete and cancel-rate budgets, and its abandoned-restore
    /// retirement switch (off), are the ones [`Self::try_from_env`] computes
    /// with none of their variables set. The
    /// node-api client still reads its transport and bearer tokens from the
    /// process environment, as in production.
    pub fn with_seeded_caches(
        pool: PgPool,
        client: Client,
        sandbox_proxy_port: u16,
        pods: Vec<Pod>,
    ) -> Self {
        let mut substrate = seeded_substrate(pool, client, sandbox_proxy_port, pods, Vec::new());
        substrate.k8s.create_command_ready_timeout = parse_create_command_ready_timeout(None);
        substrate.k8s.create_data_plane_ready_timeout = parse_create_data_plane_ready_timeout(None);
        substrate.k8s.k8s_blocking_delete_timeout = DEFAULT_K8S_BLOCKING_DELETE_TIMEOUT;
        substrate.k8s.abandoned_restore_retirement = AbandonedRestoreRetirement::Disabled;
        substrate.cancel_tracker = CancelRateTracker::with_default_budget();
        substrate
    }
}

/// A substrate over `pool` and `client` whose caches hold every pod in `pods`
/// and every node in `nodes`, warm, with [`assemble_substrate`]'s budgets.
pub(super) fn seeded_substrate(
    pool: PgPool,
    client: Client,
    sandbox_proxy_port: u16,
    pods: Vec<Pod>,
    nodes: Vec<Node>,
) -> KubernetesSubstrate {
    let (node_api_pod_store, mut node_api_pod_writer) = reflector::store::<Pod>();
    node_api_pod_writer.apply_watcher_event(&Event::Init);
    for pod in &pods {
        node_api_pod_writer.apply_watcher_event(&Event::InitApply(pod.clone()));
    }
    node_api_pod_writer.apply_watcher_event(&Event::InitDone);
    let (pod_store, pod_network_slots, _) = sandbox_pod_view::seeded_sandbox_pod_cache(pods);

    let (node_store, mut node_writer) = reflector::store::<Node>();
    node_writer.apply_watcher_event(&Event::Init);
    for node in nodes {
        node_writer.apply_watcher_event(&Event::InitApply(node));
    }
    node_writer.apply_watcher_event(&Event::InitDone);

    let caches = Arc::new(K8sCaches::for_test(node_api_pod_store, node_store, true));
    assemble_substrate(
        pool,
        client,
        sandbox_proxy_port,
        SandboxPodConfigValues::default(),
        caches,
        (pod_store, pod_network_slots),
    )
}

/// A substrate over `pool` and `client` with the given caches, one-second
/// create and delete budgets, a cancel-rate budget of 64 a second,
/// abandoned-restore retirement enabled, and the node-group placement gate
/// closed.
pub(super) fn assemble_substrate(
    pool: PgPool,
    client: Client,
    sandbox_proxy_port: u16,
    sandbox_pod_config: SandboxPodConfigValues,
    caches: Arc<K8sCaches>,
    (pod_store, pod_network_slots): (Store<SandboxPodView>, NodeNetworkSlots),
) -> KubernetesSubstrate {
    let shutdown_token = CancellationToken::new();
    let task_tracker = TaskTracker::new();
    KubernetesSubstrate {
        k8s: KubernetesState {
            client,
            sandbox_namespace: TEST_SANDBOX_NAMESPACE.to_string(),
            sandbox_proxy_port,
            node_api_port: sandbox_proxy_port,
            sandbox_pod_config,
            create_command_ready_timeout: Duration::from_secs(1),
            create_data_plane_ready_timeout: Duration::from_secs(1),
            k8s_blocking_delete_timeout: Duration::from_secs(1),
            abandoned_restore_retirement: AbandonedRestoreRetirement::Enabled,
            runtime_upgrade_at_activation: RuntimeUpgradeAtActivation::Disabled,
            caches,
        },
        restore_waiters: RestoreWaiters::new(
            K8sStore::new(pool.clone(), TEST_SANDBOX_NAMESPACE.to_string()),
            shutdown_token.clone(),
            task_tracker.clone(),
        ),
        pod_worker_channels: Arc::default(),
        wake_coalescer: Arc::new(
            pplx_space_common_work_coalescer::WorkCoalescer::with_tracker(task_tracker.clone()),
        ),
        store: K8sStore::new(pool.clone(), TEST_SANDBOX_NAMESPACE.to_string()),
        pod_store,
        pod_network_slots,
        pod_events: pod_reflector::PodEvents::for_test(),
        pod_reflector_ready: ReflectorReady::always_ready(),
        cancel_tracker: Arc::new(CancelRateTracker::with_budget(64)),
        // Gate closed: a closed gate issues no query, so a lazily-connected
        // pool stays undialed. Placement rendering is covered by the
        // pod-builder snapshots; resolution by `node_group_placement`'s own
        // tests.
        node_group_placement: NodeGroupPlacementResolver::new(pool, false),
        recovery_join_backoff: RecoveryJoinBackoffPolicy::default(),
        runtime_policy: RuntimePolicyCache::default(),
        budgets: SubstrateBudgets::default(),
        shutdown_token,
        task_tracker,
    }
}

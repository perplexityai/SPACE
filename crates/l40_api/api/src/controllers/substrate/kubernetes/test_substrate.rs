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
use crate::controllers::substrate::runtime_upgrade::{
    RuntimePolicyCache, RuntimeUpgradeAtActivation,
};
use crate::controllers::substrate::{AbandonedRestoreRetirement, ActivationClientAttribution};

/// The namespace a substrate built here keeps its sandbox pods and their
/// `sandbox_k8s_executions` rows in.
pub const TEST_SANDBOX_NAMESPACE: &str = "space";

/// [`assemble_substrate`]'s reflector-event wait. Its reflectors are seeded
/// once and never fed, so a pod read's wait can only time out before the
/// apiserver GET that answers it; a shorter wait changes no outcome.
const TEST_REFLECTOR_WAIT: Duration = Duration::from_millis(20);

/// [`assemble_substrate`]'s blocking-delete budget. Its reflector never sees
/// a delete, so the absence wait always runs to the deadline, and the GET
/// there decides the outcome whatever the budget.
const TEST_BLOCKING_DELETE_TIMEOUT: Duration = Duration::from_millis(20);

/// How often [`assemble_substrate`]'s restore followers re-observe a restore
/// whose owner holds a long lease. A follower sees the owner's change on its
/// next poll, so this only bounds how late it sees it.
const TEST_RESTORE_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    /// with none of their variables set, and its other waits are
    /// production's. The node-api client still reads its transport and bearer tokens from the
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
        substrate.budgets = SubstrateBudgets::default();
        substrate.restore_waiters = RestoreWaiters::new(
            substrate.store.clone(),
            substrate.shutdown_token.clone(),
            substrate.task_tracker.clone(),
        );
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
    let (pod_store, pod_indexes, _) = sandbox_pod_view::seeded_sandbox_pod_cache(pods);

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
        (pod_store, pod_indexes),
    )
}

/// A substrate over `pool` and `client` with the given caches, one-second
/// create budgets, a cancel-rate budget of 64 a second, abandoned-restore
/// retirement enabled, and the node-group placement gate closed. Its
/// blocking-delete budget, reflector-event wait and restore-follower poll
/// interval are shortened, as their constants say.
pub(super) fn assemble_substrate(
    pool: PgPool,
    client: Client,
    sandbox_proxy_port: u16,
    sandbox_pod_config: SandboxPodConfigValues,
    caches: Arc<K8sCaches>,
    (pod_store, pod_indexes): (Store<SandboxPodView>, SandboxPodIndexes),
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
            k8s_blocking_delete_timeout: TEST_BLOCKING_DELETE_TIMEOUT,
            abandoned_restore_retirement: AbandonedRestoreRetirement::Enabled,
            runtime_upgrade_at_activation: RuntimeUpgradeAtActivation::Disabled,
            environment_placement:
                crate::controllers::substrate::environment_placement::EnvironmentPlacementGate::Off,
            activation_client_attribution: ActivationClientAttribution::Enabled,
            restore_dispatch_fence: super::RestoreDispatchFence::Disabled,
            caches,
        },
        restore_waiters: RestoreWaiters::with_max_poll_interval(
            K8sStore::new(
                pool.clone(),
                TEST_SANDBOX_NAMESPACE.to_string(),
                ActivationClientAttribution::Enabled,
            ),
            shutdown_token.clone(),
            task_tracker.clone(),
            TEST_RESTORE_POLL_INTERVAL,
        ),
        pod_worker_channels: Arc::default(),
        wake_coalescer: Arc::new(pplx_space_work_coalescer::WorkCoalescer::with_tracker(
            task_tracker.clone(),
        )),
        store: K8sStore::new(
            pool.clone(),
            TEST_SANDBOX_NAMESPACE.to_string(),
            ActivationClientAttribution::Enabled,
        ),
        pod_store,
        pod_network_slots: pod_indexes.network_slots,
        pod_client_index: pod_indexes.client_pods,
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
        budgets: SubstrateBudgets {
            reflector_wait: TEST_REFLECTOR_WAIT,
            ..SubstrateBudgets::DEFAULT
        },
        shutdown_token,
        task_tracker,
    }
}

/// Live writer handle into a test [`KubernetesSubstrate`]'s sandbox-pod
/// reflector cache, indexes, event broadcast, and readiness state.
#[cfg(test)]
pub(super) struct PodCacheFeed {
    writer: sandbox_pod_view::SandboxPodWriter,
    events_tx: tokio::sync::broadcast::Sender<String>,
    subscribed: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl PodCacheFeed {
    pub(super) fn apply(&mut self, pod: Pod) {
        let view = SandboxPodView::from(pod);
        let name = view.name().to_string();
        self.writer.apply_watcher_event(&Event::Apply(view));
        let _ = self.events_tx.send(name);
    }

    pub(super) fn start_relist(&mut self) {
        self.writer.apply_watcher_event(&Event::Init);
        let _ = self.events_tx.send(String::new());
    }

    pub(super) fn finish_relist(&mut self, pods: impl IntoIterator<Item = Pod>) {
        for pod in pods {
            self.writer
                .apply_watcher_event(&Event::InitApply(SandboxPodView::from(pod)));
        }
        self.writer.apply_watcher_event(&Event::InitDone);
        let _ = self.events_tx.send(String::new());
    }

    pub(super) fn mark_watch_error(&self) {
        self.writer.mark_watch_error();
        let _ = self.events_tx.send(String::new());
    }

    /// Wait until a reader has completed its initial cache probe and subscribed
    /// to `pod_events` inside `await_cached`, then yield once so it parks on
    /// `rx.recv()` before the caller mutates the cache or watch health.
    pub(super) async fn wait_for_subscriber(&self) {
        let mut notified = std::pin::pin!(self.subscribed.notified());
        notified.as_mut().enable();
        if self.events_tx.receiver_count() == 0 {
            tokio::time::timeout(Duration::from_secs(1), notified)
                .await
                .expect("reader never entered reflector event wait after initial cache probe");
        }
        tokio::task::yield_now().await;
    }
}

/// Build a test [`KubernetesSubstrate`] paired with a [`PodCacheFeed`] so unit
/// tests can drive live reflector updates, re-lists, and watch errors without a
/// real apiserver watch. Passing `Some(pods)` completes the initial `LIST` sync
/// and marks the reflector ready; passing `None` leaves the reflector unready.
#[cfg(test)]
pub(super) fn substrate_with_pod_cache_feed(
    pool: PgPool,
    client: Client,
    sandbox_proxy_port: u16,
    initial_pods: Option<Vec<Pod>>,
) -> (KubernetesSubstrate, PodCacheFeed) {
    let (node_api_pod_store, mut node_api_pod_writer) = reflector::store::<Pod>();
    node_api_pod_writer.apply_watcher_event(&Event::Init);
    node_api_pod_writer.apply_watcher_event(&Event::InitDone);

    let (node_store, mut node_writer) = reflector::store::<Node>();
    node_writer.apply_watcher_event(&Event::Init);
    node_writer.apply_watcher_event(&Event::InitApply(Node {
        metadata: kube::api::ObjectMeta {
            name: Some(super::scripted_test_doubles::WORKER_NODE.to_string()),
            uid: Some(format!("{}-uid", super::scripted_test_doubles::WORKER_NODE)),
            ..Default::default()
        },
        status: Some(k8s_openapi::api::core::v1::NodeStatus {
            addresses: Some(vec![k8s_openapi::api::core::v1::NodeAddress {
                type_: "InternalIP".to_string(),
                address: "127.0.0.1".to_string(),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }));
    node_writer.apply_watcher_event(&Event::InitDone);

    let (pod_store, pod_indexes, mut writer) = sandbox_pod_view::sandbox_pod_cache();
    let (pod_events, events_tx, subscribed) = pod_reflector::PodEvents::for_test_with_sender();
    let ready = ReflectorReady::new();
    if let Some(pods) = initial_pods {
        writer.apply_watcher_event(&Event::Init);
        for pod in pods {
            writer.apply_watcher_event(&Event::InitApply(SandboxPodView::from(pod)));
        }
        writer.apply_watcher_event(&Event::InitDone);
        ready.mark_ready();
    }

    let caches = Arc::new(K8sCaches::for_test(node_api_pod_store, node_store, true));
    let mut substrate = assemble_substrate(
        pool,
        client,
        sandbox_proxy_port,
        SandboxPodConfigValues::default(),
        caches,
        (pod_store, pod_indexes),
    );
    substrate.pod_events = pod_events;
    substrate.pod_reflector_ready = ready;
    (
        substrate,
        PodCacheFeed {
            writer,
            events_tx,
            subscribed,
        },
    )
}

/// The database half of
/// [`KubernetesSubstrate::delete_execution_retired_by_current_stop`]: retire
/// the execution a `stopped` sandbox's stop left active, unless the teardown
/// fence refuses. It runs the production statement and transaction
/// ([`K8sStore::retire_executions_predating_current_stop`]), so another
/// crate's test that fakes only the pod delete still exercises the real fence.
/// `Ok(false)` when the fence refused and nothing changed.
pub async fn retire_executions_predating_current_stop(
    pool: PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, SubstrateError> {
    K8sStore::new(
        pool,
        TEST_SANDBOX_NAMESPACE.to_string(),
        ActivationClientAttribution::Enabled,
    )
    .retire_executions_predating_current_stop(sandbox_id)
    .await
}

/// The node release that ends
/// [`KubernetesSubstrate::delete_execution_with_no_active_execution`]: release
/// `source_node_id` for a `stopped` sandbox the drain has evacuated. It runs the
/// production statement and transaction
/// ([`K8sStore::release_stopped_evacuation_placement`]), so another crate's test
/// that fakes only the pod delete still exercises the real release guard.
/// `Ok(false)` when the guard refused and nothing changed.
pub async fn release_stopped_evacuation_placement(
    pool: PgPool,
    sandbox_id: SandboxId,
    source_node_id: pplx_space_accounts::NodeId,
) -> Result<bool, SubstrateError> {
    K8sStore::new(
        pool,
        TEST_SANDBOX_NAMESPACE.to_string(),
        ActivationClientAttribution::Enabled,
    )
    .release_stopped_evacuation_placement(sandbox_id, source_node_id)
    .await
}

use kube::Client;
use pplx_space_common_core::kubernetes::SPACE_K8S_NODE_NAME_ENV;
use pplx_space_node_mgr_client::NodeResourceStatusReader;
use tokio_util::sync::CancellationToken;

mod host_cpu;
mod network_slots;
mod node_watch;
mod ssd_headroom_tier;
mod template_manager;
mod template_presence;
mod template_pull_requests;

const TEMPLATE_MANAGER_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const TEMPLATE_PULL_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
/// Env flag clamping the advertised network-slot capacity to what node memory
/// admission would accept (default off: capacity is `network_pool_max_slots`).
const NETWORK_SLOTS_MEMORY_CLAMP_ENV: &str = "SPACE_NETWORK_SLOTS_MEMORY_CLAMP";
/// Cadence of the memory-clamped network-slot capacity reconcile. Memory headroom
/// moves within seconds under a create ramp, faster than the shared
/// node-object pass on the node-status tick, and a stale capacity keeps the
/// scheduler binding pods to nodes memory admission has already filled. The
/// patch is skipped while the advertised value is current, so a quiet node
/// costs one local node-mgr status read per tick.
const NETWORK_SLOT_CAPACITY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

pub struct Controller {
    kube: Client,
    node_name: String,
    node_cache: node_watch::NodeMetadataCache,
    tmpl: template_manager::Client,
    template_writes: template_pull_requests::TemplatePullRequestReconciler,
    resource_status_reader: NodeResourceStatusReader,
    network_slots_memory_clamp: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateInventoryReconcileError {
    #[error("template write annotation reconcile failed: {0}")]
    PullRequests(#[source] template_pull_requests::TemplatePullReconcileError),
    #[error("template node metadata reconcile failed: {0}")]
    Presence(#[source] template_presence::TemplatePresenceReconcileError),
}

#[derive(Debug, thiserror::Error)]
#[error("failed to initialize Kubernetes client: {0}")]
pub struct ControllerFromEnvError(#[from] kube::Error);

/// Reconcile the network-slot capacity every [`NETWORK_SLOT_CAPACITY_INTERVAL`]
/// so the memory-clamped capacity tracks node memory between the 60s
/// node-object passes (which still run it, as a self-heal).
fn spawn_network_slot_capacity_loop(
    kube: Client,
    node_cache: node_watch::NodeMetadataCache,
    node_name: String,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(NETWORK_SLOT_CAPACITY_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = interval.tick() => {}
            }
            if node_cache.metadata().is_none() {
                continue;
            }
            if let Err(error) =
                network_slots::reconcile_network_slot_capacity(&kube, &node_cache, &node_name, true)
                    .await
            {
                tracing::debug!(%error, "fast network-slot capacity reconcile failed");
            }
        }
    });
}

impl Controller {
    pub async fn from_env(
        shutdown: CancellationToken,
        resource_status_reader: NodeResourceStatusReader,
    ) -> Result<Option<Self>, ControllerFromEnvError> {
        let Some(node_name) = std::env::var(SPACE_K8S_NODE_NAME_ENV)
            .ok()
            .map(|node_name| node_name.trim().to_string())
            .filter(|node_name| !node_name.is_empty())
        else {
            return Ok(None);
        };

        let kube = pplx_space_common_svc::kube_client::traced_kube_client_from_env().await?;
        let node_cache = node_watch::spawn(kube.clone(), node_name.clone(), shutdown.child_token());

        let network_slots_memory_clamp =
            pplx_space_common_core::env::env_flag_var(NETWORK_SLOTS_MEMORY_CLAMP_ENV, false);
        tracing::info!(
            %node_name,
            network_slots_memory_clamp,
            "space Kubernetes node-local controller starting"
        );
        if network_slots_memory_clamp {
            spawn_network_slot_capacity_loop(
                kube.clone(),
                node_cache.clone(),
                node_name.clone(),
                shutdown.child_token(),
            );
        }

        Ok(Some(Self {
            kube,
            node_name,
            node_cache,
            tmpl: template_manager::Client::new(
                TEMPLATE_MANAGER_RPC_TIMEOUT,
                TEMPLATE_PULL_RPC_TIMEOUT,
            ),
            template_writes: template_pull_requests::TemplatePullRequestReconciler::new(
                TEMPLATE_PULL_RPC_TIMEOUT,
                shutdown.child_token(),
            ),
            resource_status_reader,
            network_slots_memory_clamp,
        }))
    }

    pub async fn reconcile_template_inventory(
        &mut self,
    ) -> Result<(), TemplateInventoryReconcileError> {
        if self.node_cache.metadata().is_none() {
            tracing::debug!(node_name = %self.node_name, "Kubernetes Node metadata cache is not ready; skipping local template inventory reconcile");
            return Ok(());
        }

        let mut result = Ok(());
        if let Err(error) = self
            .template_writes
            .reconcile(&self.kube, &self.node_cache, &self.node_name)
            .await
        {
            result = Err(TemplateInventoryReconcileError::PullRequests(error));
        }

        if let Err(error) = template_presence::reconcile_template_presence(
            &self.kube,
            &self.node_cache,
            &mut self.tmpl,
            &self.node_name,
        )
        .await
        {
            if result.is_ok() {
                result = Err(TemplateInventoryReconcileError::Presence(error));
            } else {
                tracing::warn!(%error);
            }
        }

        result
    }

    pub async fn reconcile_network_slot_capacity(
        &mut self,
    ) -> Result<(), network_slots::NetworkSlotCapacityReconcileError> {
        if self.node_cache.metadata().is_none() {
            tracing::debug!(node_name = %self.node_name, "Kubernetes Node metadata cache is not ready; skipping network-slot capacity reconcile");
            return Ok(());
        }

        network_slots::reconcile_network_slot_capacity(
            &self.kube,
            &self.node_cache,
            &self.node_name,
            self.network_slots_memory_clamp,
        )
        .await
    }

    pub async fn reconcile_host_cpu(&mut self) -> Result<(), host_cpu::HostCpuReconcileError> {
        if self.node_cache.metadata().is_none() {
            tracing::debug!(node_name = %self.node_name, "Kubernetes Node metadata cache is not ready; skipping host-cpu reconcile");
            return Ok(());
        }

        host_cpu::reconcile_host_cpu(&self.kube, &self.node_cache, &self.node_name).await
    }

    pub async fn reconcile_ssd_headroom_tier(
        &mut self,
    ) -> Result<(), ssd_headroom_tier::SsdHeadroomTierReconcileError> {
        if self.node_cache.metadata().is_none() {
            tracing::debug!(node_name = %self.node_name, "Kubernetes Node metadata cache is not ready; skipping ssd-headroom-tier reconcile");
            return Ok(());
        }

        ssd_headroom_tier::reconcile_ssd_headroom_tier(
            &self.kube,
            &self.node_cache,
            &self.node_name,
            &self.resource_status_reader,
        )
        .await
    }
}

//! Node eligibility: readiness, schedulability, placement-block and
//! network-slot predicates, the network-slot preflight, preservable-create
//! node resolution, and node-api endpoint resolution.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) enum NetworkSlotCapacityPreflight {
    Available,
    Full,
    Unknown,
}

fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|status| status.conditions.as_deref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "Ready")
        })
        .map(|condition| condition.status == "True")
        .unwrap_or(false)
}

pub(in crate::controllers::substrate::kubernetes) fn node_schedulable(node: &Node) -> bool {
    !node
        .spec
        .as_ref()
        .and_then(|spec| spec.unschedulable)
        .unwrap_or(false)
        && node_ready(node)
}

/// True when a node is not hard-blocked from new sandbox placement. Mirrors the
/// required node anti-affinity that `build_sandbox_pod` stamps on every sandbox
/// pod (`NotIn` the blocked value): a node labelled
/// `ssd-placement-blocked=true` is unschedulable for sandboxes, so it must not
/// count toward eligible start capacity. A missing label or any other value
/// leaves the node eligible — matching `NotIn`, which is satisfied by nodes
/// that lack the key.
pub(in crate::controllers::substrate::kubernetes) fn node_not_placement_blocked(
    node: &Node,
) -> bool {
    node.metadata
        .labels
        .as_ref()
        .and_then(|labels| {
            labels.get(pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_LABEL)
        })
        .map(|value| value != pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_VALUE)
        .unwrap_or(true)
}

/// True when a node's node-local SPACE service pods (node-ctl,
/// volume-manager, …) are known **not** to be `Ready` — the cluster-singleton
/// writer has labelled it `node-services-ready=false`.
///
/// The inverse of the soft `NotIn` preference `build_sandbox_pod` stamps: a
/// missing label or any other value reads as ready, so an unlabeled node (no
/// writer deployed, node just joined) is never treated as degraded.
pub(in crate::controllers::substrate::kubernetes) fn node_services_not_ready(node: &Node) -> bool {
    node.metadata
        .labels
        .as_ref()
        .and_then(|labels| {
            labels.get(pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL)
        })
        .is_some_and(|value| {
            value == pplx_space_common_core::kubernetes::NODE_SERVICES_NOT_READY_VALUE
        })
}

fn node_network_slot_capacity(node: &Node) -> Option<u64> {
    let status = node.status.as_ref()?;
    let advertised = status
        .capacity
        .as_ref()
        .and_then(|capacity| {
            capacity.get(pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE)
        })
        .and_then(quantity_as_u64)?;
    if advertised == 0 {
        return None;
    }

    status
        .allocatable
        .as_ref()
        .and_then(|allocatable| {
            allocatable.get(pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE)
        })
        .and_then(quantity_as_u64)
        .or(Some(advertised))
}

pub(in crate::controllers::substrate::kubernetes) fn node_network_slot_available(
    node: &Node,
    network_slots: &NodeNetworkSlots,
) -> bool {
    let Some(node_name) = node.metadata.name.as_deref() else {
        return false;
    };
    let Some(capacity) = node_network_slot_capacity(node) else {
        return false;
    };
    network_slots.requested_on(node_name) < capacity
}

pub(in crate::controllers::substrate::kubernetes) fn network_slot_capacity_preflight(
    nodes: &[std::sync::Arc<Node>],
    network_slots: &NodeNetworkSlots,
    config: &SandboxPodConfigValues,
) -> NetworkSlotCapacityPreflight {
    let selector = config.base_node_selector();
    let mut saw_capacity_node = false;
    for node in nodes {
        let Some(node_name) = node.metadata.name.as_deref() else {
            continue;
        };
        if !node_schedulable(node) || !selector.matches(node) {
            continue;
        }
        let Some(capacity) = node_network_slot_capacity(node) else {
            continue;
        };
        saw_capacity_node = true;

        if network_slots.requested_on(node_name) < capacity {
            return NetworkSlotCapacityPreflight::Available;
        }
    }

    if saw_capacity_node {
        NetworkSlotCapacityPreflight::Full
    } else {
        NetworkSlotCapacityPreflight::Unknown
    }
}

impl KubernetesSubstrate {
    /// Resolve the node a slow-create pod can be preserved on, or `None` if it
    /// isn't usefully preservable. Used by the slow-create preserve path to
    /// decide whether returning the sandbox as `started` would actually let a
    /// client retry converge.
    ///
    /// Preservable requires the execution to have advanced past `creating`,
    /// carry a persisted pod IP, be scheduled, be non-terminal/non-deleting,
    /// AND have a sandbox proxy that currently accepts TCP. The proxy check is
    /// the load-bearing one: the execution flips to `state = 'running'` the
    /// moment a pod IP is assigned — *before* the proxy ever accepts a
    /// connection — so "has an IP" alone does not mean the runtime is
    /// reachable. Without the live proxy probe, a create whose proxy never came
    /// up (guest wedged, node-mgr stuck) would be marked the non-sweepable
    /// `started` state and returned as Running forever, since there is no later
    /// readiness finalizer. Requiring a live proxy means a preserved create can
    /// actually be reached by the client's data-plane retry; anything else
    /// falls through to teardown, exactly as before this change.
    pub(super) async fn preservable_create_node(
        &self,
        sandbox_uuid: SandboxId,
        pod_name: &str,
    ) -> Option<NodeId> {
        let row = self
            .store
            .active_execution_status(sandbox_uuid)
            .await
            .ok()
            .flatten()?;
        if execution_pod_create_in_flight(row.state) || row.pod_ip.is_none() {
            return None;
        }
        let pod = match self.lookup_sandbox_pod(pod_name) {
            // Trust the cached pod only when it shows BOTH a node assignment
            // and a pod IP. The reflector can observe the scheduling update
            // (`nodeName`) before the later `podIP` assignment; an IP-lagged
            // cached pod would make `assigned_pod_ip` below return `None` and
            // wrongly drop a reachable slow-create to teardown even though the
            // per-pod watch already persisted the IP (`row.pod_ip`, checked
            // above). Fall through to the authoritative GET in that window.
            Some(pod)
                if assigned_pod_node_name(&pod).is_some()
                    && assigned_pod_ip(&pod).ok().flatten().is_some() =>
            {
                pod
            }
            _ => fetch_sandbox_pod_uncached(&self.k8s, pod_name)
                .await
                .ok()
                .flatten()?,
        };
        assigned_pod_node_name(&pod)?;
        if pod.is_deleting() || terminal_pod_phase(&pod).is_some() {
            return None;
        }
        // Live evidence the runtime is reachable — not merely that an IP was
        // assigned. Same probe + timeout the create proxy-reachable loop uses.
        let pod_ip = assigned_pod_ip(&pod).ok().flatten()?;
        if !Self::pod_proxy_accepts_tcp(
            pod_ip,
            self.k8s.sandbox_proxy_port,
            START_WAIT_CONNECT_TIMEOUT,
        )
        .await
        {
            return None;
        }
        self.remember_pod_node_placement(
            sandbox_uuid,
            &pod,
            PlacementRetryDisposition::RetryDuplicatesExecution,
        )
        .await
        .ok()
        .flatten()
    }

    pub(in crate::controllers::substrate) fn try_resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        let k8s_node_name = target
            .substrate_node_id
            .ok_or_else(|| SubstrateError::NotFound {
                message: "k8s node row missing substrate_node_id".to_string(),
            })?;

        let pod = self
            .k8s
            .caches
            .ready_node_api_pod_on(k8s_node_name, target.role.as_str())
            .ok_or_else(|| SubstrateError::Unavailable {
                message: format!(
                    "no ready node-api pod for role {} on k8s node {k8s_node_name}",
                    target.role
                ),
            })?;

        let raw_ip = caches::pod_ip(&pod).ok_or_else(|| SubstrateError::Unavailable {
            message: format!("node-api pod on k8s node {k8s_node_name} has no pod IP yet"),
        })?;

        let ip: IpAddr = raw_ip.parse().map_err(|err| SubstrateError::Failed {
            message: format!(
                "node-api pod on k8s node {k8s_node_name} has invalid pod IP {raw_ip}: {err}"
            ),
        })?;
        // Prefer the port advertised by the node-api pod itself so worker and
        // builder DaemonSets configured with different ports each get dialed
        // correctly. Fall back to the configured global default when a pod is
        // missing the annotation and legacy named port.
        let port = caches::pod_node_api_port(&pod).unwrap_or(self.k8s.node_api_port);
        Ok(SocketAddr::new(ip, port).to_string())
    }
}

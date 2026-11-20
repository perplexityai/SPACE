//! Assigns nodes from generic, Terraform-owned pools to tenants' dedicated
//! node groups.
//!
//! Capacity is Terraform's: a pool is an EKS managed node group declared at
//! size 0 with `desired_size` under `ignore_changes`. Assignment is SPACE's: a
//! `node_groups` row with `pool` and `desired_nodes` (written by
//! `POST /v1/tenants`). Each tick:
//!
//! 1. **Fence.** A pool node with no group label gets the
//!    `space.pplx.net/unassigned` NoSchedule taint, so nothing generic lands
//!    on it. Nodes join untainted, so there is a window of up to one tick;
//!    the clean fix is registering the pool with the taint (one-time
//!    Terraform).
//! 2. **Claim.** A group with fewer live nodes than `desired_nodes` claims
//!    unassigned nodes: the boundary label
//!    `node-restriction.kubernetes.io/space-node-group=<group>` and the taint
//!    `space.pplx.net/dedicated-org=<org>`, then the `unassigned` taint is
//!    removed. Both are control-plane patches; a kubelet cannot set the label.
//!    A node is never reassigned.
//! 3. **Grow.** If the pool has fewer nodes than its groups need, raise the
//!    managed node group's desired size with `UpdateNodegroupConfig`. Never
//!    lowers it: shrinking is per node, below.
//! 4. **Release.** A group with more live nodes than `desired_nodes` drains the
//!    excess: `drain_phase = 'draining'`, forced, with a capacity decrement.
//!    The existing node-drain reconciler evacuates and the drained-node
//!    terminator calls `TerminateInstanceInAutoScalingGroup`, which lowers the
//!    pool. An excess node SPACE never registered (no `nodes` row) is
//!    terminated directly.
//!
//! PoC scope: EKS only, one tick-driven loop, no leader fencing beyond the
//! controller's own, and no per-tenant metrics.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Node, Taint};
use kube::Api;
use kube::api::{Patch, PatchParams};
use pplx_space_accounts::OrgId;
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_sandbox_pod::{DEDICATED_NODE_TAINT_KEY, NODE_GROUP_LABEL};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::env_config;

/// Taint on a pool node no group has claimed yet.
pub(crate) const UNASSIGNED_TAINT_KEY: &str = "space.pplx.net/unassigned";
/// Label EKS sets on every node of a managed node group.
const EKS_NODEGROUP_LABEL: &str = "eks.amazonaws.com/nodegroup";
/// Set on a Node just before its instance is terminated; the node then
/// counts as leaving, so the terminate (and its decrement) is issued once.
const TERMINATING_ANNOTATION: &str = "space.pplx.net/terminating";
const DEFAULT_TICK: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub tick: Duration,
    /// EKS cluster that owns the pools.
    pub eks_cluster: Option<String>,
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self {
            enabled: env_config::truthy_default("SPACE_DEDICATED_CAPACITY_ENABLED", false),
            tick: env_config::duration_secs("SPACE_DEDICATED_CAPACITY_TICK_SECS", DEFAULT_TICK),
            eks_cluster: std::env::var("SPACE_DEDICATED_CAPACITY_EKS_CLUSTER")
                .ok()
                .filter(|value| !value.trim().is_empty()),
        }
    }
}

/// One dedicated group, as `node_groups` records it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct Group {
    pub(crate) name: String,
    pub(crate) org_id: OrgId,
    pub(crate) pool: String,
    pub(crate) desired_nodes: i32,
    pub(crate) created_at: time::OffsetDateTime,
}

/// One kube Node in a pool, reduced to what the plan reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PoolNode {
    pub(crate) name: String,
    pub(crate) pool: String,
    pub(crate) group: Option<String>,
    pub(crate) unassigned_taint: bool,
    /// The SPACE row is draining, drained or terminating, or the Node is
    /// being deleted: it no longer counts toward its group.
    pub(crate) leaving: bool,
    /// Whether SPACE has a `nodes` row for it (only those can be drained).
    pub(crate) registered: bool,
    pub(crate) instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Action {
    Fence {
        node: String,
    },
    Claim {
        node: String,
        group: String,
        org_id: OrgId,
    },
    Drain {
        node: String,
    },
    Terminate {
        node: String,
        instance_id: String,
    },
    /// Raise the pool to at least `nodes`.
    Grow {
        pool: String,
        nodes: i64,
    },
}

/// The whole decision, pure so it can be tested without a cluster.
pub(crate) fn plan(groups: &[Group], nodes: &[PoolNode]) -> Vec<Action> {
    let mut actions = Vec::new();
    let mut by_pool: BTreeMap<&str, Vec<&Group>> = BTreeMap::new();
    for group in groups {
        by_pool.entry(group.pool.as_str()).or_default().push(group);
    }
    for (pool, mut pool_groups) in by_pool {
        // Oldest group first, so claims are deterministic and fair.
        pool_groups.sort_by_key(|group| (group.created_at, group.name.clone()));
        let pool_nodes: Vec<&PoolNode> = nodes.iter().filter(|node| node.pool == pool).collect();
        let mut free: Vec<&PoolNode> = pool_nodes
            .iter()
            .copied()
            .filter(|node| node.group.is_none() && !node.leaving)
            .collect();
        free.sort_by(|a, b| a.name.cmp(&b.name));
        for node in &free {
            if !node.unassigned_taint {
                actions.push(Action::Fence {
                    node: node.name.clone(),
                });
            }
        }
        let mut free = free.into_iter();
        let mut needed: i64 = 0;
        let mut leaving: i64 = 0;
        for group in &pool_groups {
            let desired = i64::from(group.desired_nodes.max(0));
            needed += desired;
            let mut live: Vec<&PoolNode> = pool_nodes
                .iter()
                .copied()
                .filter(|node| node.group.as_deref() == Some(group.name.as_str()) && !node.leaving)
                .collect();
            live.sort_by(|a, b| a.name.cmp(&b.name));
            let have = live.len() as i64;
            if have < desired {
                for _ in have..desired {
                    let Some(node) = free.next() else { break };
                    actions.push(Action::Claim {
                        node: node.name.clone(),
                        group: group.name.clone(),
                        org_id: group.org_id,
                    });
                }
            } else {
                for node in live.into_iter().skip(usize::try_from(desired).unwrap_or(0)) {
                    leaving += 1;
                    actions.push(release(node));
                }
            }
        }
        // Free nodes no group is about to claim are excess capacity.
        for node in free {
            leaving += 1;
            actions.push(release(node));
        }
        // Nodes already leaving still occupy the pool until terminated.
        leaving += pool_nodes.iter().filter(|node| node.leaving).count() as i64;
        let present = pool_nodes.len() as i64;
        let target = needed + leaving;
        if present < target {
            actions.push(Action::Grow {
                pool: pool.to_string(),
                nodes: target,
            });
        }
    }
    actions
}

fn release(node: &PoolNode) -> Action {
    match (&node.instance_id, node.registered) {
        (Some(instance_id), false) => Action::Terminate {
            node: node.name.clone(),
            instance_id: instance_id.clone(),
        },
        _ => Action::Drain {
            node: node.name.clone(),
        },
    }
}

pub async fn run(
    config: ReconcilerConfig,
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "dedicated-capacity reconciler is disabled (SPACE_DEDICATED_CAPACITY_ENABLED unset)"
        );
        shutdown_token.cancelled().await;
        return;
    }
    let Some(eks_cluster) = config.eks_cluster.clone() else {
        tracing::error!(
            "dedicated-capacity reconciler is enabled but SPACE_DEDICATED_CAPACITY_EKS_CLUSTER \
             is unset; not starting"
        );
        shutdown_token.cancelled().await;
        return;
    };
    let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let backend = Backend {
        pool,
        kube,
        eks: pplx_space_aws_client::eks::Client::from_config(&aws),
        asg: pplx_space_aws_client::autoscaling::Client::from_config(&aws),
        eks_cluster,
    };
    tracing::info!(
        tick_secs = config.tick.as_secs(),
        "starting dedicated-capacity reconciler"
    );
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(err) = backend.tick().await {
                    tracing::warn!(error = %err, "dedicated_capacity: tick failed");
                }
            }
            _ = shutdown_token.cancelled() => break,
        }
    }
}

struct Backend {
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    eks: pplx_space_aws_client::eks::Client,
    asg: pplx_space_aws_client::autoscaling::Client,
    eks_cluster: String,
}

#[derive(sqlx::FromRow)]
struct NodeRow {
    k8s_node_name: String,
    drain_phase: Option<String>,
}

impl Backend {
    async fn tick(&self) -> Result<(), String> {
        let groups: Vec<Group> = sqlx::query_as(sql!(
            "SELECT name, org_id, pool, desired_nodes, created_at
               FROM node_groups
              WHERE pool IS NOT NULL AND org_id IS NOT NULL AND desired_nodes IS NOT NULL"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        if groups.is_empty() {
            return Ok(());
        }
        let caches = &self.kube.k8s.caches;
        if !caches.nodes_ready() {
            return Err("node cache not ready".to_string());
        }
        let pools: Vec<&str> = groups.iter().map(|group| group.pool.as_str()).collect();
        let kube_nodes: Vec<Arc<Node>> = caches
            .nodes
            .state()
            .into_iter()
            .filter(|node| {
                label(node, EKS_NODEGROUP_LABEL).is_some_and(|pool| pools.contains(&pool))
            })
            .collect();
        let names: Vec<String> = kube_nodes
            .iter()
            .filter_map(|node| node.metadata.name.clone())
            .collect();
        let rows: Vec<NodeRow> = sqlx::query_as(sql!(
            "SELECT k.k8s_node_name, n.drain_phase
               FROM node_k8s k JOIN nodes n ON n.id = k.node_id
              WHERE k.k8s_node_name = ANY($1) AND n.state <> 'deleted'"
        ))
        .bind(&names)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        let rows: HashMap<String, Option<String>> = rows
            .into_iter()
            .map(|row| (row.k8s_node_name, row.drain_phase))
            .collect();
        let nodes: Vec<PoolNode> = kube_nodes
            .iter()
            .filter_map(|node| {
                let name = node.metadata.name.clone()?;
                let row = rows.get(&name);
                Some(PoolNode {
                    pool: label(node, EKS_NODEGROUP_LABEL)?.to_string(),
                    group: label(node, NODE_GROUP_LABEL).map(str::to_string),
                    unassigned_taint: has_taint(node, UNASSIGNED_TAINT_KEY),
                    leaving: node.metadata.deletion_timestamp.is_some()
                        || annotation(node, TERMINATING_ANNOTATION).is_some()
                        || row.is_some_and(|phase| phase.is_some()),
                    registered: row.is_some(),
                    instance_id: instance_id(node),
                    name,
                })
            })
            .collect();
        let by_name: HashMap<&str, &Arc<Node>> = kube_nodes
            .iter()
            .filter_map(|node| Some((node.metadata.name.as_deref()?, node)))
            .collect();

        for action in plan(&groups, &nodes) {
            tracing::info!(?action, "dedicated_capacity: applying");
            let kube_node = |name: &str| {
                by_name
                    .get(name)
                    .copied()
                    .ok_or_else(|| format!("node {name} is no longer listed"))
            };
            let result = match &action {
                Action::Fence { node } => match kube_node(node) {
                    Err(err) => Err(err),
                    Ok(kube_node) => {
                        self.patch_node(kube_node, None, |taints| {
                            set_taint(taints, UNASSIGNED_TAINT_KEY, "true");
                        })
                        .await
                    }
                },
                Action::Claim {
                    node,
                    group,
                    org_id,
                } => match kube_node(node) {
                    Err(err) => Err(err),
                    Ok(kube_node) => {
                        self.patch_node(kube_node, Some(group), |taints| {
                            set_taint(taints, DEDICATED_NODE_TAINT_KEY, &org_id.to_string());
                            // An encrypted group would keep `unassigned` until the
                            // node's encrypted cache is ready (track B).
                            taints.retain(|taint| taint.key != UNASSIGNED_TAINT_KEY);
                        })
                        .await
                    }
                },
                Action::Drain { node } => self.drain(node).await,
                Action::Terminate { node, instance_id } => {
                    match self.mark_terminating(node).await {
                        Err(err) => Err(err),
                        Ok(()) => self
                            .asg
                            .terminate_instance_in_auto_scaling_group(instance_id, true)
                            .await
                            .map_err(|err| err.to_string()),
                    }
                }
                Action::Grow { pool, nodes } => self.grow(pool, *nodes).await,
            };
            if let Err(err) = result {
                tracing::warn!(?action, error = %err, "dedicated_capacity: action failed");
            }
        }
        Ok(())
    }

    /// Merge-patch the Node's taints (and optionally its group label), guarded
    /// by the observed `resourceVersion`: a concurrent change fails the patch
    /// with 409 and the next tick re-plans.
    async fn patch_node(
        &self,
        node: &Node,
        group: Option<&str>,
        edit: impl FnOnce(&mut Vec<Taint>),
    ) -> Result<(), String> {
        let name = node.metadata.name.as_deref().unwrap_or_default();
        let mut taints = node
            .spec
            .as_ref()
            .and_then(|spec| spec.taints.clone())
            .unwrap_or_default();
        edit(&mut taints);
        let metadata = match group {
            Some(group) => serde_json::json!({
                "resourceVersion": node.metadata.resource_version,
                "labels": { NODE_GROUP_LABEL: group },
            }),
            None => serde_json::json!({ "resourceVersion": node.metadata.resource_version }),
        };
        let patch = serde_json::json!({ "metadata": metadata, "spec": { "taints": taints } });
        Api::<Node>::all(self.kube.k8s.client.clone())
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    /// Record the terminate on the Node before issuing it, so later ticks
    /// (and a restarted controller) count the node as leaving instead of
    /// decrementing the pool again.
    async fn mark_terminating(&self, node: &str) -> Result<(), String> {
        let patch = serde_json::json!({
            "metadata": { "annotations": { TERMINATING_ANNOTATION: "true" } }
        });
        Api::<Node>::all(self.kube.k8s.client.clone())
            .patch(node, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn drain(&self, node: &str) -> Result<(), String> {
        sqlx::query(sql!(
            "UPDATE nodes n
                SET drain_phase = 'draining',
                    drain_started_at = NOW(),
                    drain_last_error = NULL,
                    drain_mode = 'forced',
                    terminate_decrement_capacity = true
               FROM node_k8s k
              WHERE k.node_id = n.id AND k.k8s_node_name = $1 AND n.drain_phase IS NULL"
        ))
        .bind(node)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|err| err.to_string())
    }

    async fn grow(&self, pool: &str, nodes: i64) -> Result<(), String> {
        let scaling = self
            .eks
            .describe_nodegroup_scaling(&self.eks_cluster, pool)
            .await
            .map_err(|err| err.to_string())?;
        // Instances already requested but not yet joined count as present.
        if scaling.desired_size >= nodes {
            return Ok(());
        }
        let target = nodes.min(scaling.max_size);
        self.eks
            .set_nodegroup_desired_size(&self.eks_cluster, pool, target)
            .await
            .map_err(|err| err.to_string())?;
        tracing::info!(
            pool,
            from = scaling.desired_size,
            to = target,
            "dedicated_capacity: raised pool desired size"
        );
        Ok(())
    }
}

fn label<'a>(node: &'a Node, key: &str) -> Option<&'a str> {
    node.metadata
        .labels
        .as_ref()?
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

fn annotation<'a>(node: &'a Node, key: &str) -> Option<&'a str> {
    node.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .map(String::as_str)
}

fn has_taint(node: &Node, key: &str) -> bool {
    node.spec
        .as_ref()
        .and_then(|spec| spec.taints.as_ref())
        .is_some_and(|taints| taints.iter().any(|taint| taint.key == key))
}

fn set_taint(taints: &mut Vec<Taint>, key: &str, value: &str) {
    taints.retain(|taint| taint.key != key);
    taints.push(Taint {
        key: key.to_string(),
        value: Some(value.to_string()),
        effect: "NoSchedule".to_string(),
        time_added: None,
    });
}

/// EC2 instance id from an AWS providerID (`aws:///us-east-1a/i-0123…`).
fn instance_id(node: &Node) -> Option<String> {
    let provider_id = node.spec.as_ref()?.provider_id.as_deref()?;
    provider_id
        .rsplit('/')
        .next()
        .filter(|id| id.starts_with("i-"))
        .map(str::to_string)
}

#[cfg(test)]
mod tests;

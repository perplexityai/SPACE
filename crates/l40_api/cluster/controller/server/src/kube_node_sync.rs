//! Periodic reconciler that proactively syncs labeled kube Nodes into the
//! SPACE `nodes` / `node_k8s` tables.
//!
//! A `space-node-api-svc` DaemonSet pod registers its node at startup. Without
//! that DaemonSet, nodes may appear only after their first sandbox pod lands.
//! This reconciler fills that gap so the admin UI shows all schedulable workers
//! immediately.
//!
//! The sync is idempotent; running it alongside the DaemonSet-based
//! registration path is safe. It also demotes `ready` rows that
//! have disappeared from the labeled Kubernetes inventory after both the
//! node heartbeat and Kubernetes metadata have remained stale for a grace
//! period. The row and its sandbox placements are preserved.
//!
//! Drain-owned rows are excluded from absence demotion. Successful node
//! termination leaves a tombstone that makes the inventory upsert refuse to
//! recreate the deleted row while a Kubernetes Node object is disappearing.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use tokio_util::sync::CancellationToken;

use crate::env_config;

const DEFAULT_TICK: Duration = Duration::from_secs(30);
const DEFAULT_MISSING_GRACE: Duration = Duration::from_secs(120);
const DEFAULT_MISSING_MAX_PER_TICK: u32 = 100;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub tick: Duration,
    pub missing_grace: Duration,
    pub missing_max_per_tick: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick: DEFAULT_TICK,
            missing_grace: DEFAULT_MISSING_GRACE,
            missing_max_per_tick: DEFAULT_MISSING_MAX_PER_TICK,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::destructive_gate("SPACE_KUBE_NODE_SYNC_ENABLED"),
            tick: default.tick,
            missing_grace: default.missing_grace,
            missing_max_per_tick: default.missing_max_per_tick,
        }
    }
}

pub async fn run(
    config: ReconcilerConfig,
    kube: Arc<KubernetesSubstrate>,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "kube-node-sync reconciler is disabled \
             (SPACE_KUBE_NODE_SYNC_ENABLED=false); skipping"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        missing_grace_secs = config.missing_grace.as_secs(),
        missing_max_per_tick = config.missing_max_per_tick,
        "starting kube-node-sync reconciler"
    );
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match kube
                    .sync_labeled_nodes_to_db(
                        config.missing_grace,
                        config.missing_max_per_tick,
                    )
                    .await
                {
                    Ok(names) if !names.is_empty() => {
                        tracing::info!(
                            changed = names.len(),
                            nodes = ?names,
                            "kube_node_sync: changed kube node inventory"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "kube_node_sync: sync failed");
                    }
                }
            }
            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }

    tracing::info!("kube-node-sync reconciler stopping");
}

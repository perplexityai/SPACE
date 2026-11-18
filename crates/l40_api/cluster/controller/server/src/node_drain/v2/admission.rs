//! Admission: which waiting drains take an evacuation slot.

use super::*;

impl V2Node {
    /// Holding an evacuation slot: admitted and not yet `drained`, or
    /// undrained after its admission with its cancel not yet finished, which
    /// lets the node's running suspends and uploads finish. The slot frees
    /// once the node leaves v2: its cancel finished, or its row deleted. An
    /// admitted gentle drain frees it while it only holds
    /// ([`Self::only_holding`]), and takes it again, whatever the other
    /// drains hold, whenever its report lists anything else and once it is
    /// due to turn forced or is escalated: until those finish the cluster
    /// may evacuate more nodes than the cap, and admission admits none while
    /// it does.
    fn evacuating(&self) -> bool {
        self.epoch.is_some()
            && !self.waiting
            && match self.phase.as_deref() {
                Some("draining") => !self.only_holding(),
                None => self.cancel,
                Some(_) => false,
            }
    }

    /// [`Self::evacuating`], unless EC2 has said the node's instance is
    /// going away for good: a stopped or unlisted instance, a stale
    /// heartbeat or no answer keeps the slot.
    fn holds_slot(&self, evidence: Option<&InstanceEvidence>) -> bool {
        self.evacuating() && !evidence.is_some_and(InstanceEvidence::irreversible)
    }

    fn awaiting_slot(&self) -> bool {
        self.epoch.is_some() && self.phase.as_deref() == Some("draining") && self.waiting
    }
}

/// How many v2 nodes may evacuate at once by default
/// (`SPACE_DRAIN_V2_MAX_EVACUATING_NODES`). Each node runs its own window of
/// suspends and uploads (sandbox-manager's `SPACE_DRAIN_NODE_WINDOW`, 8), so
/// 4 nodes put at most 32 sandboxes' suspends and uploads in flight across
/// the cluster, half the 64 the v1 drain shared with its restores.
pub const DEFAULT_MAX_EVACUATING_NODES: usize = 4;

/// Admit waiting drains whose heartbeat is fresh, oldest first (the epoch is
/// the drain's start), while fewer than `max_evacuating` nodes hold a slot
/// ([`V2Node::holds_slot`], by this tick's `evidence`). Returns the nodes
/// admitted; each is marked evacuating in `nodes`, so this tick seals it.
pub(super) async fn admit<S: DrainV2Sink>(
    sink: &S,
    nodes: &mut [V2Node],
    max_evacuating: usize,
    evidence: &HashMap<NodeId, InstanceEvidence>,
) -> Vec<NodeId> {
    let evacuating = nodes
        .iter()
        .filter(|node| node.holds_slot(evidence.get(&node.id)))
        .count();
    // Only a node that heartbeats is sealed: a dead one would hold the slot
    // until EC2 says it is gone.
    let mut waiting: Vec<&mut V2Node> = nodes
        .iter_mut()
        .filter(|node| node.awaiting_slot() && node.heartbeat_fresh)
        .collect();
    waiting.sort_by_key(|node| node.epoch);
    let mut admitted = Vec::new();
    for node in waiting
        .into_iter()
        .take(max_evacuating.saturating_sub(evacuating))
    {
        let Some(epoch) = node.epoch else { continue };
        match sink.admit_evacuation(node.id, epoch).await {
            Ok(true) => {
                node.waiting = false;
                admitted.push(node.id);
            }
            Ok(false) => {}
            Err(error) => {
                sink.record_error(node.id, &format!("admitting the drain failed: {error}"))
                    .await;
            }
        }
    }
    admitted
}

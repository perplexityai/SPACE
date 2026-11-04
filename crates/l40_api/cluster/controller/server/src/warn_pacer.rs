//! Per-node WARN pacing for reconciler conditions that have no give-up.
//!
//! Most reconciler blockers settle or are swept, so warning on every tick is
//! self-limiting. A few don't: a node held by a sandbox under investigation
//! stays held until a human acts, and at a 5-second tick an unpaced WARN emits
//! hundreds of lines an hour, per node, indefinitely — enough to bury every
//! other warning in the service.
//!
//! Several conditions need exactly this, across two reconcilers and on largely
//! the same nodes: node-drain's hold on a node the terminator would refuse, its
//! three KubeNodeGone verdicts, and the terminator's refusal of a node it was
//! handed anyway. The terminator's was written first and inline; this is that
//! mechanism, extracted so the others did not each grow their own.
//!
//! # One window per (node, reason), not per node
//!
//! Keying on the node alone lets one condition's window swallow another's *first*
//! report. A node whose EC2 cross-check errors for a few minutes and then
//! resolves to "instance terminated" would log the second — the operationally
//! interesting one — at debug, because the error already opened the window. So
//! `reason` is part of the key, and callers pass a distinct one per log line.
//!
//! # The `retain_live` half is the forgettable half
//!
//! `due` is the obvious API and pruning is the one that gets left out — and
//! leaving it out is unbounded, because the keys are node ids and a paced
//! condition is by definition one the node does not leave on its own. Fleet
//! churn then accumulates a slot per node for the process's lifetime. Calling
//! [`PerNodeWarnPacer::retain_live`] once per tick with the phase scan the
//! reconciler already performed bounds the map by live cardinality, so it is
//! part of this type's contract rather than each caller's habit.

use pplx_space_common_core::types::NodeId;

use std::time::Duration;

use dashmap::DashMap;

/// Tracks, per node, when its paced WARN last fired.
///
/// `tokio::time::Instant` rather than `std::time::Instant` so tests can drive
/// the window with a paused clock instead of sleeping through it.
pub(crate) struct PerNodeWarnPacer {
    interval: Duration,
    /// Keyed by `(node, reason)` — see the module docs on why the reason is part
    /// of the key. Cardinality is bounded by live nodes times the caller's fixed
    /// set of reasons.
    last_warned_at: DashMap<(NodeId, &'static str), tokio::time::Instant>,
}

impl PerNodeWarnPacer {
    /// `interval` is the minimum gap between WARNs for the same node. Held
    /// per-pacer rather than as a shared constant: the two reconcilers tick at
    /// different cadences and their pacing is an independent knob.
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_warned_at: DashMap::new(),
        }
    }

    /// True iff this node's WARN for `reason` is due — first observation of that
    /// pair, or `interval` elapsed since the last one — and records the firing
    /// when it is.
    ///
    /// Records on the same call that reports, so a caller cannot ask twice and
    /// warn twice. The cost is that this is not a pure predicate: calling it
    /// without emitting silently consumes the window.
    ///
    /// `reason` must be one of a small fixed set per caller, and must differ
    /// between log lines that an operator would want to see independently.
    pub(crate) fn due(&self, node_id: NodeId, reason: &'static str) -> bool {
        let now = tokio::time::Instant::now();
        match self.last_warned_at.entry((node_id, reason)) {
            dashmap::Entry::Occupied(mut entry) => {
                if now.duration_since(*entry.get()) >= self.interval {
                    entry.insert(now);
                    true
                } else {
                    false
                }
            }
            dashmap::Entry::Vacant(entry) => {
                entry.insert(now);
                true
            }
        }
    }

    /// Drop slots for nodes no longer in `live`, bounding the map by live node
    /// cardinality. Call once per tick with the phase scan the reconciler
    /// already did — see the module docs on why this isn't optional.
    ///
    /// Linear in `live` per tracked node. `live` is a reconciler's per-tick node
    /// list and the map is bounded by it, so both stay in the hundreds; a set
    /// would buy nothing measurable and cost an allocation per tick.
    pub(crate) fn retain_live(&self, live: &[NodeId]) {
        self.last_warned_at
            .retain(|(node_id, _), _| live.contains(node_id));
    }

    /// `(node, reason)` slots currently held. Tests assert pruning through this
    /// rather than reaching into the map.
    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.last_warned_at.len()
    }

    /// Whether `node_id` holds a slot for any reason.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, node_id: NodeId) -> bool {
        self.last_warned_at
            .iter()
            .any(|entry| entry.key().0 == node_id)
    }
}

#[cfg(test)]
mod tests;

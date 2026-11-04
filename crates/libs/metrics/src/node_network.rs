use std::fmt;

use crate::emit::{gauge, tag};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NetworkSlotState {
    Ready,
    Leased,
    Dirty,
    Resetting,
    Creating,
    Quarantined,
    Degraded,
}

impl NetworkSlotState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Leased => "leased",
            Self::Dirty => "dirty",
            Self::Resetting => "resetting",
            Self::Creating => "creating",
            Self::Quarantined => "quarantined",
            Self::Degraded => "degraded",
        }
    }
}

impl fmt::Display for NetworkSlotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn slots_max(count: u32) {
    gauge("node.network_slots.max", f64::from(count), &[]);
}

pub fn slots_available(count: u32) {
    gauge("node.network_slots.available", f64::from(count), &[]);
}

pub fn slots_by_state(state: NetworkSlotState, count: u32) {
    gauge(
        "node.network_slots",
        f64::from(count),
        &[tag("state", state)],
    );
}

/// A part of a sandbox namespace's nftables rules that node-manager installs
/// whole and stamps with its version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkRulePart {
    L2Ingress,
    PreTproxy,
    PackagedPolicy,
    EgressGuardrails,
    Nat,
    Forward,
    Proxy,
}

impl NetworkRulePart {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L2Ingress => "l2_ingress",
            Self::PreTproxy => "pre_tproxy",
            Self::PackagedPolicy => "packaged_policy",
            Self::EgressGuardrails => "egress_guardrails",
            Self::Nat => "nat",
            Self::Forward => "forward",
            Self::Proxy => "proxy",
        }
    }
}

impl fmt::Display for NetworkRulePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Slots in `state` with at least one part of their namespace rules older or
/// newer than this node-manager installs, each slot counted once.
pub fn slots_stale(state: NetworkSlotState, count: u32) {
    gauge(
        "node.network_slots.stale",
        f64::from(count),
        &[tag("state", state)],
    );
}

/// Slots in `state` whose `part` differs from what this node-manager
/// installs. A slot with several stale parts counts once for each.
pub fn slots_stale_parts(state: NetworkSlotState, part: NetworkRulePart, count: u32) {
    gauge(
        "node.network_slots.stale_parts",
        f64::from(count),
        &[tag("state", state), tag("part", part)],
    );
}

/// Slots in `state` with no stale part but at least one part whose version is
/// not known: it carries no stamp, or its last install failed.
pub fn slots_stale_unknown(state: NetworkSlotState, count: u32) {
    gauge(
        "node.network_slots.stale_unknown",
        f64::from(count),
        &[tag("state", state)],
    );
}

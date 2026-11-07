use std::fmt;

use pplx_space_metrics_emit::{gauge, tag};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

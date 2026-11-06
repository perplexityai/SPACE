//! Node-local `/space/cache` disk occupancy as seen by the node-ctl
//! SSD-headroom tier reconcile (the signal behind the `ssd-headroom-tier`
//! Kubernetes node label that biases sandbox placement).

use pplx_space_metrics_emit::gauge;

/// Free fraction of the `/space/cache` filesystem in `[0.0, 1.0]`,
/// measured once per node-ctl reconcile tick.
pub fn ssd_free_fraction(value: f64) {
    gauge("node.ssd_free_fraction", value, &[]);
}

/// Usage-keyed headroom tier (`0..=3`) advertised on the Kubernetes Node via
/// the `space.pplx.net/ssd-headroom-tier` label.
pub fn ssd_headroom_tier(tier: u8) {
    gauge("node.ssd_headroom_tier", f64::from(tier), &[]);
}

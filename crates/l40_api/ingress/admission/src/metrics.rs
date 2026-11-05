//! Ingress admission waiter metrics.

use pplx_space_metrics_emit::counter;

pub fn ingress_waiter_rejected() {
    counter("data_plane.ingress.waiter_rejected", 1, &[]);
}

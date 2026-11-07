//! Data-plane session retention, which the cluster controller sweeps.

use pplx_space_metrics_emit::counter;

pub fn session_retention_deleted(count: u64) {
    if count == 0 {
        return;
    }
    counter("data_plane.session.retention_deleted", count, &[]);
}

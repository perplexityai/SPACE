use pplx_space_metrics_emit::{counter, gauge, tag};

pub fn retention_deleted(kind: &str, count: u64) {
    if count == 0 {
        return;
    }
    counter("tombstone.retention.deleted", count, &[tag("kind", kind)]);
}

/// Snapshot storage locations the sandbox tombstone prune detached for the
/// snapshot-storage sweeper.
pub fn retention_storage_locations_detached(count: u64) {
    if count == 0 {
        return;
    }
    counter("tombstone.retention.storage_locations_detached", count, &[]);
}

pub fn retained_rows(kind: &str, count: u64) {
    gauge(
        "tombstone.retention.rows",
        count as f64,
        &[tag("kind", kind)],
    );
}

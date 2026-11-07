use pplx_space_metrics_emit::{counter, gauge};

pub fn audit_retention_deleted(count: u64) {
    if count == 0 {
        return;
    }
    counter("admin.audit.retention_deleted", count, &[]);
}

pub fn audit_retained_rows(count: u64) {
    gauge("admin.audit.rows", count as f64, &[]);
}

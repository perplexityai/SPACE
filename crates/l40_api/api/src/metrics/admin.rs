//! The admin audit metric the API server records.

use pplx_space_metrics_emit::{counter, tag};

pub fn audit_persist_failed(method: &str, mode: &str) {
    counter(
        "admin.audit.persist_failed",
        1,
        &[tag("method", method), tag("mode", mode)],
    );
}

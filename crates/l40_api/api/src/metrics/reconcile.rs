//! Reconcile queue requests the API server records.

use pplx_space_metrics_emit::{counter, tag};

pub fn queued(facet: &str, result: &str) {
    queued_count(facet, result, 1);
}

pub fn queued_count(facet: &str, result: &str, count: u64) {
    counter(
        "reconcile.queue.request",
        count,
        &[tag("facet", facet), tag("result", result)],
    );
}

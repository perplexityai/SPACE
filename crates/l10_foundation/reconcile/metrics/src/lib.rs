//! Reconcile-pipeline metrics keyed by facet: queue depth and dead letters,
//! dirty marks, and runs with their items and duration.
//!
//! They are their own crate so that every reconciler can emit them the same
//! way without depending on another's crate; the facet-specific families live
//! with the crate that records them.

use std::time::Instant;

use pplx_space_metrics_emit::{counter, elapsed_ms, gauge, tag, timing_ms};

pub fn queue_depth(facet: &str, state: &str, count: u64) {
    gauge(
        "reconcile.queue.depth",
        count as f64,
        &[tag("facet", facet), tag("state", state)],
    );
}

pub fn queue_dead_letter(facet: &str) {
    counter("reconcile.queue.dead_letter", 1, &[tag("facet", facet)]);
}

pub fn dirty_mark(facet: &str, result: &str) {
    counter(
        "reconcile.dirty.mark",
        1,
        &[tag("facet", facet), tag("result", result)],
    );
}

pub fn run_duration(
    facet: &str,
    direction: &str,
    result: &str,
    trigger: &str,
    started_at: Instant,
) {
    let tags = [
        tag("facet", facet),
        tag("direction", direction),
        tag("result", result),
        tag("trigger", trigger),
    ];
    counter("reconcile.run", 1, &tags);
    timing_ms("reconcile.run.duration_ms", elapsed_ms(started_at), &tags);
}

pub fn run_items(facet: &str, direction: &str, result: &str, count: u64) {
    counter(
        "reconcile.run.items",
        count,
        &[
            tag("facet", facet),
            tag("direction", direction),
            tag("result", result),
        ],
    );
}

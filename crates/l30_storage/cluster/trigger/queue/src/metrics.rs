//! Metrics of the reconcile trigger queue.

use pplx_space_metrics_emit::counter;

pub fn trigger_dead_letter() {
    counter("reconcile.trigger.dead_letter", 1, &[]);
}

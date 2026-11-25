//! Metrics of the reconcile trigger queue.

use pplx_space_metrics_emit::counter;

pub(crate) fn trigger_dead_letter() {
    counter("reconcile.trigger.dead_letter", 1, &[]);
}

//! The template build routing metric the API server records.

use pplx_space_metrics_emit::{counter, tag};

pub fn build_routing_fallback_missing_identity() {
    counter(
        "template.build.routing_fallback",
        1,
        &[tag("reason", "missing_builder_identity")],
    );
}

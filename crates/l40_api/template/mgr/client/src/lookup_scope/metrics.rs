//! Template-lookup scope compatibility metrics.

use pplx_space_metrics_emit::{counter, tag};

/// Per-hop scope protocol support; a receipt does not certify authorization.
pub fn template_lookup_compatibility(result: &str) {
    counter(
        "auth.template_lookup.compatibility",
        1,
        &[tag("result", result)],
    );
}

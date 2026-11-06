//! Resources a running workload uses, as its guest reports them.
//!
//! `sandbox.resource.memory.guest_used_bytes` is a distribution of the guest's
//! `MemTotal - MemAvailable`, in bytes, tagged only by `template`; deployment
//! dimensions come from the sink. Sandbox-manager's health probe records one
//! sample per accepted probe of a `started` sandbox that is not an idle
//! warm-pool member. Do not attach sandbox IDs or other per-sandbox
//! identifiers.

use super::*;

/// One guest memory reading of a running workload, in bytes.
pub fn guest_memory_used(template: &str, used_bytes: u64) {
    distribution(
        "sandbox.resource.memory.guest_used_bytes",
        used_bytes as f64,
        &[tag("template", template)],
    );
}

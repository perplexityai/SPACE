//! Host resource use for running sandboxes, independent of guest health.

use super::*;

/// One interval average for one running VM, with no per-sandbox metric tags.
/// The denominator is the VM's own CPU quota, not the node's shared ceiling.
pub fn sandbox_cpu_used(template: &str, used_cores: f64, pct_of_requested: Option<f64>) {
    let tags = &[tag("template", template)];
    distribution("sandbox.resource.cpu.used_cores", used_cores, tags);
    if let Some(percent) = pct_of_requested {
        distribution("sandbox.resource.cpu.used_pct_of_requested", percent, tags);
    }
}

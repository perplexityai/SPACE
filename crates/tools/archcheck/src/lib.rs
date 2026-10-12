//! Repository structure checks driven by `ARCHITECTURE.toml`.
//!
//! The binary loads a [`Context`] once, runs every rule in
//! [`rules::registered`], and reports [`Finding`]s. Each finding carries the
//! mode of the behaviour that produced it, so a rule can report on every pull
//! request (`advisory`) long before it is allowed to fail one (`required`).

pub mod baseline;
pub mod config;
pub mod context;
pub mod finding;
pub mod report;
pub mod rules;

pub use baseline::{BASELINE_FILE, Baseline, WriteBaselineError};
pub use config::{ArchitectureConfig, CONFIG_FILE, Mode};
pub use context::{Context, LoadError, ProductionFile};
pub use finding::{Finding, Location};

use std::path::Path;

/// Runs every registered rule over `ctx`, in registration order.
pub fn check(ctx: &Context) -> Vec<Finding> {
    rules::registered()
        .iter()
        .flat_map(|rule| rule.check(ctx))
        .collect()
}

/// Regenerates `architecture-baseline.toml` from the current tree.
///
/// The existing baseline is neither read nor required, so this also recovers
/// from a baseline that no longer parses. Returns the baseline it wrote.
pub fn write_baseline(root: &Path) -> Result<Baseline, WriteBaselineError> {
    let ctx = Context::load_with_baseline(root, Baseline::default())?;
    let mut baseline = Baseline::default();
    for rule in rules::registered() {
        rule.update_baseline(&ctx, &mut baseline);
    }
    baseline.write(&root.join(BASELINE_FILE))?;
    Ok(baseline)
}

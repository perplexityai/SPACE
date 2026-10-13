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

pub use baseline::{BASELINE_FILE, BaseBaseline, Baseline, WriteBaselineError};
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

/// One line per behaviour [`check`] skipped for lack of an input.
pub fn skipped(ctx: &Context) -> Vec<String> {
    rules::registered()
        .iter()
        .flat_map(|rule| rule.skipped(ctx))
        .collect()
}

/// What [`write_baseline`] wrote, and the violations it left out.
#[derive(Debug)]
pub struct WrittenBaseline {
    pub baseline: Baseline,
    /// One line per violation without an entry, such as `not added: …`.
    pub not_added: Vec<String>,
}

/// Rewrites `architecture-baseline.toml` from the current tree: entries
/// still in violation are kept and updated, the rest are removed, and none
/// are added.
pub fn write_baseline(root: &Path) -> Result<WrittenBaseline, WriteBaselineError> {
    let ctx = Context::load(root)?;
    let mut baseline = Baseline::default();
    let mut not_added = Vec::new();
    for rule in rules::registered() {
        not_added.extend(rule.update_baseline(&ctx, &mut baseline));
    }
    baseline.write(&root.join(BASELINE_FILE))?;
    Ok(WrittenBaseline {
        baseline,
        not_added,
    })
}

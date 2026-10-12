//! The rule interface and the registry every check runs from.

mod file_size;

use crate::{baseline::Baseline, context::Context, finding::Finding};

/// A rule reads its `[rules.<name>]` table from `ctx.config` and names
/// itself in each [`Finding`] it returns.
pub trait Rule {
    fn check(&self, ctx: &Context) -> Vec<Finding>;

    /// Fills this rule's table for `--write-baseline`. Rules without a
    /// baseline keep the default.
    fn update_baseline(&self, _ctx: &Context, _baseline: &mut Baseline) {}
}

/// Every rule, in reporting order. Register a new rule here.
pub fn registered() -> Vec<Box<dyn Rule>> {
    vec![Box::new(file_size::FileSize)]
}

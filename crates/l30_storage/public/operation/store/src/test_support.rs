//! This crate's production statements, exposed to other crates' plan-cost
//! tests so they `EXPLAIN` the text the verbs run.

/// The statement [`crate::operations::inline_org_at_limit`] runs.
pub const INLINE_ORG_AT_LIMIT: &str = crate::operations::INLINE_ORG_AT_LIMIT;

/// The statement [`crate::quota::sandboxes_held`] runs.
pub const SANDBOXES_HELD: &str = crate::quota::SANDBOXES_HELD;

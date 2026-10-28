//! Pure conversions of node-supplied publication evidence.
//!
//! The staged extraction preserves the existing rejection codes and messages.
//! SQL and publication diagnostics stay at their existing call sites until the
//! complete validated publication can replace the legacy input boundary.

pub(crate) mod fields;
pub(crate) mod quarantine;
pub(crate) mod templates;

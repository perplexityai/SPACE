//! Node registry vocabulary, node groups and the template ↔ node-group
//! compatibility relation.
//!
//! They are their own crate so that code can name them without depending on
//! `pplx-space-common-core`.

mod node;
mod node_group;
#[cfg(feature = "sqlx")]
mod postgres;

pub use node::*;
pub use node_group::*;

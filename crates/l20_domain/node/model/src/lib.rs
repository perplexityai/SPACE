//! Node registry vocabulary, node groups and the template ↔ node-group
//! compatibility relation.

mod node;
mod node_group;
#[cfg(feature = "sqlx")]
mod postgres;

pub use node::*;
pub use node_group::*;

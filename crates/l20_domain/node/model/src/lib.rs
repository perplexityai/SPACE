//! Node registry vocabulary, node groups and the template ↔ node-group
//! compatibility relation.

mod node;
mod node_group;

pub use node::*;
pub use node_group::*;

#[cfg(feature = "sqlx")]
pplx_space_job_id::postgres_uuid!(NodeGroupId);

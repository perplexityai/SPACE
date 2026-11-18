//! Node filesystem layout, object-store keys and storage URIs for template
//! and snapshot artifacts, and the sandbox artifact metadata document.
//!
//! These types are their own crate so that code can name them without
//! depending on `pplx-space-common-core`.

mod node_fs;
mod object_store;
#[cfg(feature = "serde")]
mod sandbox_artifact;

pub use node_fs::*;
pub use object_store::*;
#[cfg(feature = "serde")]
pub use sandbox_artifact::*;

#[cfg(test)]
mod runtime_contract_tests;

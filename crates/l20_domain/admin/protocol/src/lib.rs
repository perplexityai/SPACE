//! Wire types for the `/admin/api/v1` operator surface, shared by the
//! api-server and the admin CLI. They exist only with the `serde` feature.
//!
//! This is a protocol crate, like `pplx-space-public-api`: it holds wire
//! schemas built from the model crates' types, so no model depends on it.

mod admin;

#[cfg(feature = "serde")]
pub use admin::*;

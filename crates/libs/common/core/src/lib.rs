//! Dependency-light shared primitives for SPACE services.
//!
//! The crate contains dependency-light shared helpers:
//!
//! - [`types`] holds the sandbox, template, snapshot and node domain types, so
//!   raw strings, UUIDs and IDs are parsed once at the boundary. Value types
//!   that have their own `pplx-space-*` crate are imported from that crate,
//!   not through `types`.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_sandbox_model::*;
//! let id = SandboxId::new();
//! let selector = SandboxSelector::parse(id.label().to_string())?;
//!
//! assert!(selector.matches_id(id));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
pub mod types;

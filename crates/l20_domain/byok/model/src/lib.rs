//! Key Encryption Key (KEK) provider vocabulary for BYOK organizations, and
//! the AAD bound into a sandbox's node-local volume DEK.
//!
//! These types are their own crate so that code can name them without
//! depending on `pplx-space-common-core`.

mod kek;

pub use kek::*;

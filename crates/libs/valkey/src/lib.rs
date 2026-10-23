//! Redis cluster client construction and the shared-Valkey bootstrap
//! configuration for SPACE services.
//!
//! These depend only on the redis and thiserror crates, so they live in their
//! own crate and services can use them without the rest of the service host.

pub mod redis;
pub mod shared_valkey;

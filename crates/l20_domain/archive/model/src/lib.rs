//! Stable archive envelope and verification boundary, independent of storage
//! I/O.
//!
//! These types are their own crate so that code can name them without
//! depending on `pplx-space-common-core`.

mod archive;

pub use archive::*;

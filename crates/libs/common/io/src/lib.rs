//! Reusable I/O mechanics shared across SPACE services.
//!
//! This crate is the home for portable stream, compression, and
//! encryption building blocks that operate on bytes independently of
//! any product domain — Volume Manager remains the orchestrator and
//! authority that composes them into snapshot/artifact transfers, but
//! the mechanics themselves (and future btrfs full/diff stream
//! handling) live here so other consumers can reuse them without
//! depending on vol-mgr.
//!
//! The [`fs`] module provides streaming AES-256-GCM file/byte encryption,
//! while [`sparse`] provides a byte-oriented sparse-file writer. The crate
//! deliberately holds **no** BYOK/snapshot domain policy (which key, which
//! artifacts should be sparse, which AAD, where in S3): callers provide those
//! decisions and compose the shared mechanics.

pub mod fs;
#[cfg(all(unix, feature = "reflink"))]
pub mod reflink;
pub mod sparse;

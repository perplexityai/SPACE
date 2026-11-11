//! Single integration-test binary for `pplx-space-spaced-guest-server`.
//!
//! `filesystem`, `meta`, and `process` were previously three standalone
//! `tests/*.rs` binaries, each linking the full dependency graph
//! independently. Folding them into one binary with submodules cuts that
//! link cost to once per `cargo test` invocation. `support` holds the shared
//! fixtures they all previously included per-binary via `#[path]`; it is
//! declared once here instead of once per consumer.

#![cfg(test)]

#[cfg(unix)]
#[path = "support/guest_meta.rs"]
mod guest_meta;
#[cfg(unix)]
#[path = "support/guest_service.rs"]
mod guest_service;
#[cfg(unix)]
#[path = "support/process_events.rs"]
mod process_events;

#[cfg(unix)]
#[path = "it/filesystem.rs"]
mod filesystem;
#[cfg(unix)]
#[path = "it/meta.rs"]
mod meta;
#[cfg(unix)]
#[path = "it/process.rs"]
mod process;

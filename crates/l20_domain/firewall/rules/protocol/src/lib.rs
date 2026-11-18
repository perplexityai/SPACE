//! The versioned JSON document for
//! [`pplx_space_firewall_rules_model::FirewallRuleRevision`]: a strict decoder
//! and the canonical encoder, shared by the CLI and the Admin API.
//! [`encode_revision`] writes the only canonical bytes; the model types
//! implement no `Serialize`, so nothing else can write a second form.
//!
//! ```text
//! {"schema_version": 1, "rules": [{"id": ID, "all_of": [COMPARISON, ...]}, ...]}
//! COMPARISON = {"left": READ, "op": "==", "right": VALUE}
//! ```
//!
//! | read | JSON |
//! | --- | --- |
//! | payload header | `{"payload": {"protocol": P, "field": F}}` |
//! | TCP option | `{"tcp option": {"name": N}}` |
//! | masked payload header | `{"&": [{"payload": {"protocol": P, "field": F}}, MASK]}` |
//!
//! | value | JSON |
//! | --- | --- |
//! | integer | `443` |
//! | IPv4 address | `"192.0.2.1"` |
//! | literal true | `true` |
//! | IPv4 prefix | `{"prefix": {"addr": "192.0.2.0", "len": 24}}` |
//! | integer range | `{"range": [1024, 65535]}` |
//!
//! [`decode_revision`] takes text; a caller holding bytes runs
//! [`str::from_utf8`] first. It reads `schema_version` before anything else, so
//! a newer document fails as [`DecodeError::UnsupportedSchemaVersion`]. Within
//! a version, unknown fields are rejected at every level, so any change needs a
//! new version, and readers of a version must deploy before any writer emits
//! it. This crate sets no limit on document size; the caller that accepts a
//! document enforces one.

mod decode;
mod encode;
mod wire;

#[cfg(test)]
mod tests;

pub use decode::{DecodeError, decode_revision};
pub use encode::encode_revision;

/// The only schema version this crate reads and writes.
pub const SCHEMA_VERSION: u64 = 1;

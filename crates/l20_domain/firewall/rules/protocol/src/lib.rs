//! The versioned JSON document for
//! [`FirewallRuleRevision`](pplx_space_firewall_rules_model::FirewallRuleRevision);
//! this crate's tests show example documents. [`encode_revision`] is the only
//! writer of its canonical bytes. [`decode_revision`] reads `schema_version`
//! first and rejects unknown fields, so any change needs a new version.

mod decode;
mod encode;
mod wire;

#[cfg(test)]
mod tests;

pub use decode::{DecodeError, decode_revision};
pub use encode::encode_revision;

/// The only schema version this crate reads and writes.
pub const SCHEMA_VERSION: u64 = 1;

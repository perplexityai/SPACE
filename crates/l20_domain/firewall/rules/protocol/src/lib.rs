//! The JSON document for the runtime drop-only firewall rules: a strict,
//! versioned decoder and the canonical encoder for
//! [`pplx_space_firewall_rules::FirewallRuleRevision`].
//!
//! The CLI and the Admin API share this crate so that both read and write a
//! revision the same way. It does no I/O.
//!
//! # Document
//!
//! A revision is one complete snapshot:
//!
//! ```json
//! {
//!   "schema_version": 1,
//!   "rules": [
//!     {
//!       "id": "dns",
//!       "all_of": [
//!         { "left": { "payload": { "protocol": "udp", "field": "dport" } }, "op": "==", "right": 53 }
//!       ]
//!     }
//!   ]
//! }
//! ```
//!
//! Each comparison is `{"left": <read>, "op": "==", "right": <value>}`.
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
//! # Decoding
//!
//! [`decode_revision`] reads `schema_version` first and decodes the rest with
//! that version's grammar. It rejects, with the JSON path of the offending
//! element: a missing, `null`, non-integer or repeated `schema_version`, any
//! version other than 1, a missing or `null` required field, a field not listed
//! above at any level, a key repeated within one object, an object naming two
//! variants, a tuple of the wrong length, an operator other than `==`, an empty
//! `all_of`, a zero mask, a malformed or non-IPv4 address, a prefix length
//! above 32, a reversed range, a non-integer where an integer is required, a
//! rule ID or name that breaks its syntax rule (see the model crate), and a
//! repeated rule ID. A missing revision, `null`, or a document without `rules`
//! is never read as an empty revision; `{"schema_version": 1, "rules": []}` is.
//! Integers, masks and range endpoints are unsigned 64-bit; a larger or
//! negative number is rejected, never wrapped or truncated.
//!
//! Malformed JSON, including a number too large for a 64-bit float such as
//! `1e400`, input nested more deeply than `serde_json`'s recursion limit, and
//! trailing data after the document, fails while the text is tokenized, before
//! any element has a path: its [`DecodeErrorKind::Syntax`] error is at the root
//! and reports the line and column instead.
//!
//! The decoder returns a typed [`DecodeError`] and nothing else; what to do
//! with a revision that does not decode is the caller's decision. This crate
//! sets no limit on document size; the caller that accepts a document enforces
//! one.
//!
//! # Encoding
//!
//! [`encode_revision`] writes the canonical form, the only byte form this
//! crate produces: fields in the order shown above, rules and comparisons in
//! their own order, and no whitespace. The model types implement no
//! `Serialize`, so no other serializer can produce a second form.
//!
//! # Evolving the schema
//!
//! Within a schema version, unknown fields are rejected at every level, so any
//! change, additive or not, needs a new `schema_version`. Readers that
//! understand a version must be deployed before any writer emits it. A reader
//! given a newer version returns [`DecodeErrorKind::UnsupportedSchemaVersion`],
//! whatever else the document holds.

mod decode;
mod encode;

#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod tests;

pub use decode::{
    DecodeError, DecodeErrorKind, JsonKind, JsonPath, PathSegment, decode_revision,
    decode_revision_bytes,
};
pub use encode::encode_revision;

/// The only schema version this crate reads and writes.
pub const SCHEMA_VERSION: u64 = 1;

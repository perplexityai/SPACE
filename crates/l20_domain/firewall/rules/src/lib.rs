//! The runtime drop-only firewall rules: their typed model, the strict JSON
//! decoder, one atomic edit operation, and the canonical diff between two
//! revisions.
//!
//! The CLI and the Admin API share this crate so that both read a revision,
//! apply an edit batch, and show the resulting change the same way. It does no
//! I/O.
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
//! A rule drops a packet when every comparison in its nonempty `all_of`
//! matches. The caller supplies packet matches only: the table, chain, hook,
//! priority, interface and verdict belong to node-manager, and no field here
//! carries them.
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
//! [`FirewallRuleRevision::from_json_str`] and [`Rule::from_json_str`] reject,
//! with the JSON path of the offending element: a schema version other than the
//! integer 1, a missing or `null` required field, a field not listed above at
//! any level, a key repeated within one object, an object naming two variants,
//! a tuple of the wrong length, an operator other than `==`, an empty `all_of`,
//! a zero mask, a malformed or non-IPv4 address, a prefix length above 32, a
//! reversed range, a non-integer where an integer is required, and a repeated
//! rule ID. A missing revision, `null`, or a document without `rules` is never
//! read as an empty revision; `{"schema_version": 1, "rules": []}` is.
//! Malformed JSON, including a number too large for a 64-bit float such as
//! `1e400`, fails while the text is tokenized, before any element has a path:
//! its [`DecodeErrorKind::Syntax`] error reports the line and column instead.
//!
//! # Shape is not runtime qualification
//!
//! A decoded revision is structurally valid. It is not qualified to run: this
//! crate keeps no list of nft header fields or TCP option names, and does not
//! check that a value fits the field it is compared with. Protocol, field and
//! option names and rule IDs are kept as given, byte for byte, with no syntax
//! rule. Integers, masks and range endpoints are unsigned 64-bit; a larger or
//! negative number is rejected, never wrapped or truncated. An IPv4 prefix
//! keeps its address as written, including host bits. The node that programs
//! the firewall decides whether a structurally valid revision can run there.
//! This crate sets no limit on document size, rule count, `all_of` length or
//! name length; the caller that accepts a document enforces those.
//!
//! # Edits
//!
//! [`FirewallRuleRevision::apply`] applies a batch of [`Edit`]s to an explicit
//! base as one operation. `Add` requires an absent ID; `Replace` and `Remove`
//! require a present one; no two edits in a batch may name the same ID. On any
//! failure it returns an [`EditError`] and no candidate or partial diff.
//!
//! The candidate's order is:
//!
//! 1. a replaced rule keeps its position in the base;
//! 2. removing rules keeps the relative order of the rest;
//! 3. added rules follow, in batch order.
//!
//! Rules are never sorted or moved, and each rule's `all_of` keeps its order.
//!
//! # Diff
//!
//! [`RuleRevisionDiff`] lists one [`RuleChange`] per rule that differs: first
//! the base's replaced and removed rules in base order, then the added rules in
//! candidate order. A rule replaced with identical content, and every rule the
//! batch did not name, is absent from the diff.

mod diff;
mod edit;
mod model;
mod wire;

#[cfg(test)]
mod test_fixtures;

pub use diff::{RuleChange, RuleRevisionDiff};
pub use edit::{Edit, EditError, EditKind, RuleRevisionChange};
pub use model::{
    EmptyAllOfError, FirewallRuleRevision, HeaderField, HeaderProtocol, IntegerRange, Ipv4Prefix,
    Match, PayloadHeader, PrefixLengthError, Read, ReversedRangeError, Rule, RuleId,
    RuleRevisionError, TcpOptionName, Value,
};
pub use wire::{DecodeError, DecodeErrorKind, JsonKind, JsonPath, PathSegment, SCHEMA_VERSION};

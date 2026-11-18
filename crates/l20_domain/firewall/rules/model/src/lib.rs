//! The runtime drop-only firewall rules: their typed model, one atomic edit
//! operation, and the canonical diff between two revisions.
//!
//! The CLI and the Admin API share this crate so that both apply an edit batch
//! and show the resulting change the same way. It does no I/O and knows no
//! wire format: `pplx-space-firewall-rules-protocol` reads and writes the JSON
//! document and converts it to and from these types.
//!
//! # Revisions and rules
//!
//! A [`FirewallRuleRevision`] is one complete snapshot: an ordered list of
//! [`Rule`]s with unique IDs. An empty list is a real snapshot; there is no
//! default, because an absent revision is not an empty one.
//!
//! A rule drops a packet when every [`Match`] in its nonempty `all_of`
//! matches. A match compares a [`Read`] (a payload header field, a TCP option,
//! or a payload header field ANDed with a nonzero mask) with a [`Value`] (an
//! unsigned 64-bit integer, an IPv4 address, `true`, an IPv4 prefix or an
//! inclusive integer range), and equality is the only comparison. The caller
//! supplies packet matches only: the table, chain, hook, priority, interface
//! and verdict belong to node-manager, and no type here carries them.
//!
//! Values that match the same packets stay distinct: `Integer(80)` is not the
//! range 80 to 80, and an address is not its `/32` prefix. Re-encoding a rule
//! from one form to the other, as a migration might, shows in a diff as a
//! replacement.
//!
//! # Names
//!
//! A [`RuleId`] is 1 to [`MAX_NAME_LEN`] (64) bytes, each printable ASCII other
//! than space (0x21 to 0x7E). A protocol, field or TCP option name is 1 to 64
//! bytes of printable ASCII, space included (0x20 to 0x7E). Control
//! characters, escape sequences, and non-ASCII characters, including invisible
//! ones, are rejected, so a name displays as exactly what it is. A name that
//! passes is kept byte for byte. Loosening these rules later accepts every
//! document accepted today; tightening them would reject stored revisions, so
//! it is not a compatible change.
//!
//! # Shape is not runtime qualification
//!
//! A valid revision is structurally valid. It is not qualified to run: this
//! crate keeps no list of nft header fields or TCP option names, and does not
//! check that a value fits the field it is compared with. An IPv4 prefix keeps
//! its address as given, including host bits. The node that programs the
//! firewall decides whether a structurally valid revision can run there. This
//! crate sets no limit on rule count or `all_of` length; the caller that
//! accepts a document enforces those.
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
//! batch did not name, is absent from the diff. A diff carries no identity of
//! its base; the caller binds it to the base it was computed from.

mod diff;
mod edit;
mod model;

#[cfg(test)]
mod test_fixtures;

pub use diff::{RuleChange, RuleRevisionDiff};
pub use edit::{Edit, EditError, EditKind, RuleRevisionChange};
pub use model::{
    EmptyAllOfError, FirewallRuleRevision, HeaderField, HeaderProtocol, IntegerRange, Ipv4Prefix,
    MAX_NAME_LEN, Match, NameError, NameKind, PayloadHeader, PrefixLengthError, Read,
    ReversedRangeError, Rule, RuleId, RuleRevisionError, TcpOptionName, Value,
};

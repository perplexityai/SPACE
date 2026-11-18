//! The runtime drop-only firewall rules: their typed model and the [`diff`]
//! between two revisions, shared by the CLI and the Admin API.
//!
//! This crate has no wire format; `pplx-space-firewall-rules-protocol` owns the
//! only canonical bytes. Validation checks shape only: there is no catalog of
//! nft header fields or TCP options. The node that programs the firewall
//! decides whether a revision can run, and the caller that accepts a document
//! limits rule count and `all_of` length.
//!
//! Loosening the [`RuleId`] and [`Name`] rules later accepts every stored
//! revision; tightening them would reject some, so it is not a compatible
//! change.
//!
//! A [`RuleRevisionDiff`] lists the changed rules and reports whether the rules
//! both revisions hold changed order. It carries no identity of its base; the
//! caller binds it to the base it was computed from.

mod diff;
mod model;

#[cfg(test)]
mod test_fixtures;

pub use diff::{RuleChange, RuleRevisionDiff, diff};
pub use model::{
    EmptyAllOfError, FirewallRuleRevision, IntegerRange, Ipv4Prefix, Match, Name, NameError,
    PayloadHeader, PrefixLengthError, Read, ReversedRangeError, Rule, RuleId, RuleRevisionError,
    Value,
};

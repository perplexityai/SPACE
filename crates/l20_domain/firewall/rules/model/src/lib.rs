//! The typed runtime drop-only firewall rules and the [`diff`] between two
//! revisions. [`Match`] and its reads and values serialize as nft JSON `match`
//! operands and deserialize only into validated values;
//! `pplx-space-firewall-rules-protocol` adds the versioned document.
//! Rule IDs and names are limited to the sets on [`RuleId`] and [`Name`].
//! The caller supplies the comparisons; SPACE owns placement, the interface
//! guard and the verdict, and must render nft input as structured JSON with
//! namespaced rule IDs in comments. A revision that decodes may still be one
//! Linux cannot install or enforce.

mod diff;
mod model;
mod shape;

#[cfg(test)]
mod test_fixtures;

pub use diff::{RuleChange, RuleRevisionDiff, diff};
pub use model::{
    EmptyAllOfError, FirewallRuleRevision, IntegerRange, Ipv4Prefix, Match, Name, NameError,
    PayloadHeader, PrefixLengthError, Read, ReversedRangeError, Rule, RuleId, RuleRevisionError,
    Value,
};

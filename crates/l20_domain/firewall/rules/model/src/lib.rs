//! The typed runtime drop-only firewall rules and the [`diff`] between two
//! revisions; `pplx-space-firewall-rules-protocol` owns the wire format.
//! Rule IDs and names are limited to the sets on [`RuleId`] and [`Name`].
//! The node renderer must build nft input as structured JSON and namespace the
//! rule IDs it puts in nft comments.
//! Decoding checks shape only; the rollout's canary phase and failure budget
//! are meant to contain a revision nft rejects.
//! The renderer must add runtime rules at the end of
//! `ip sandbox pre_tproxy_guardrails`, after its established/related return;
//! they only add drops.

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

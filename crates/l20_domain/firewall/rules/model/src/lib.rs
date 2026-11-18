//! The runtime drop-only firewall rules: their typed model and the [`diff`]
//! between two revisions, shared by the CLI and the Admin API.
//!
//! This crate has no wire format; `pplx-space-firewall-rules-protocol` owns the
//! only canonical bytes. The caller that accepts a document limits rule count
//! and `all_of` length.
//!
//! # Names
//!
//! [`RuleId`] and [`Name`] admit only ASCII letters, digits and a few
//! separators, so they hold no space, quote, backslash, `;`, `{`, `}` or `#`.
//! Loosening these rules later accepts every stored revision; tightening them
//! would reject some, so it is not a compatible change. The node renderer must
//! still build nft input as structured JSON values, never by formatting
//! strings. It also namespaces a rule ID before using it in an nft comment. In
//! the chain these rules use (see Placement), node-manager finds its own rules
//! by exact comment, such as `execution-dhcp-bootstrap` and
//! `platform-egress-pre-tproxy-guardrail-quic-udp443`; elsewhere it also
//! matches comments by prefix (`space-cfg:`, `space-dnat-`, `dnat-`) and by
//! substring of a chain listing (`allow-tcp-`, `allow-udp-`). The renderer adds
//! a prefix no node-owned comment uses and recognizes runtime rules by that
//! prefix alone.
//!
//! # Validation
//!
//! Decoding checks shape only: there is no catalog of nft header fields or TCP
//! options, and no check that a value fits its field. A revision that decodes
//! but that nft rejects fails that node's rollout item. The rollout is meant to
//! stop such a revision before it spreads: it starts with a canary phase and
//! pauses once failed items reach its failure budget. The Admin API may also
//! check a candidate against the real renderer before storing it.
//! Node-manager applies nft JSON input as one atomic transaction, so a rejected
//! one changes nothing; what the node does next belongs to the node consumer.
//!
//! # Placement
//!
//! The node renderer installs runtime rules at the end of the guest
//! namespace's `ip sandbox pre_tproxy_guardrails` chain (prerouting, priority
//! -175), after the packaged `pre-tproxy-allow-established-related` return and
//! the packaged new-connection guardrails, and matches only packets from the
//! guest's TAP, as those rules do. A runtime rule therefore sees no established
//! or related traffic, and no DHCP request the bootstrap exception at the head
//! of the chain returns. Every rule drops, so a runtime rule adds drops and
//! never lifts one: the packaged guardrails and the platform hard-deny CIDRs in
//! the namespace's forward `guardrails` chain still apply.
//!
//! # Diff
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

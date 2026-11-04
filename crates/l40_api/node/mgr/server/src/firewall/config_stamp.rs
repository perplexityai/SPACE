//! Configuration stamps: inert marker rules that record which version of a
//! namespace rule part node-manager installed.
//!
//! A stamp is a rule whose only statement is a counter and whose comment
//! starts with [`PREFIX`]. It has no match and no verdict, so it never changes
//! how a packet is handled. Checks of listed namespace rules skip stamps so
//! they see only the rules that filter traffic. A rule that carries the prefix
//! with any other statement is not a stamp and is checked like any other rule.
//!
//! Each builder that installs a whole part appends that part's stamp, in the
//! same nft transaction and after the part's rules in its chain, so a stamp
//! in the kernel proves that version was installed. Partial repairs and
//! lease-scoped rules never write one.

use std::fmt;

use aws_lc_rs::digest::{SHA256, digest};
use serde_json::Value;

pub(super) const PREFIX: &str = "space-cfg:";

/// A part of a sandbox namespace's rules that node-manager installs whole.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Part {
    L2Ingress,
    PreTproxy,
    PackagedPolicy,
    EgressGuardrails,
    Nat,
    Forward,
    Proxy,
}

impl Part {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::L2Ingress => "l2_ingress",
            Self::PreTproxy => "pre_tproxy",
            Self::PackagedPolicy => "packaged_policy",
            Self::EgressGuardrails => "egress_guardrails",
            Self::Nat => "nat",
            Self::Forward => "forward",
            Self::Proxy => "proxy",
        }
    }

    /// The family, table and chain that carry this part's stamp.
    pub(super) const fn stamp_chain(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::L2Ingress => ("netdev", super::SANDBOX_L2_TABLE, "ingress"),
            Self::PreTproxy | Self::PackagedPolicy => (
                "ip",
                super::SANDBOX_TABLE,
                super::PRE_TPROXY_GUARDRAILS_CHAIN,
            ),
            Self::EgressGuardrails => ("ip", super::SANDBOX_TABLE, "guardrails"),
            Self::Nat => ("ip", super::SANDBOX_TABLE, "postrouting"),
            Self::Forward => ("ip", super::SANDBOX_TABLE, "forward"),
            Self::Proxy => ("ip", super::PROXY_TABLE, "prerouting"),
        }
    }
}

impl fmt::Display for Part {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The version of a part's rules: the first 32 bits of the SHA-256 of its
/// normalized rule template, written as eight hex digits.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PartVersion(u32);

impl PartVersion {
    /// The version of a part installed as nft JSON commands. Object keys are
    /// sorted and whitespace is not significant, so formatting-only edits
    /// keep the version.
    pub(super) fn of_json(commands: &[Value]) -> Self {
        let canonical = commands.iter().map(sorted).collect::<Vec<_>>();
        Self::of_bytes(Value::Array(canonical).to_string().as_bytes())
    }

    /// The version of a part installed as a native nft script. Runs of
    /// whitespace count as one space and blank lines do not count.
    pub(super) fn of_script(script: &str) -> Self {
        let lines = script
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>();
        Self::of_bytes(lines.join("\n").as_bytes())
    }

    fn of_bytes(bytes: &[u8]) -> Self {
        let digest = digest(&SHA256, bytes);
        let (prefix, _) = digest
            .as_ref()
            .split_first_chunk::<4>()
            .expect("SHA-256 digests are 32 bytes");
        Self(u32::from_be_bytes(*prefix))
    }
}

impl fmt::Display for PartVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

/// A JSON value whose objects list their keys in order, whichever map
/// representation serde_json was built with.
fn sorted(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), sorted(value)))
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
}

fn comment(part: Part, version: PartVersion) -> String {
    format!("{PREFIX}{part}={version}")
}

/// The nft JSON command that appends a part's stamp to its chain.
pub(super) fn json_stamp(part: Part, version: PartVersion) -> Value {
    let (family, table, chain) = part.stamp_chain();
    serde_json::json!({ "add": { "rule": {
        "family": family, "table": table, "chain": chain,
        "comment": comment(part, version),
        "expr": [{ "counter": null }]
    } } })
}

/// The native nft script line that appends a part's stamp to its chain.
pub(super) fn script_stamp(part: Part, version: PartVersion) -> String {
    let (family, table, chain) = part.stamp_chain();
    format!(
        "add rule {family} {table} {chain} counter comment \"{}\"\n",
        comment(part, version)
    )
}

/// Whether a rule object from an `nft -j` listing is a stamp.
pub(super) fn is_stamp_rule(rule: &Value) -> bool {
    let comment = rule.get("comment").and_then(Value::as_str);
    let exprs = rule.get("expr").and_then(Value::as_array);
    comment.is_some_and(|comment| comment.starts_with(PREFIX))
        && matches!(exprs.map(Vec::as_slice), Some([expr]) if is_counter(expr))
}

fn is_counter(expr: &Value) -> bool {
    expr.as_object()
        .is_some_and(|expr| expr.len() == 1 && expr.contains_key("counter"))
}

#[cfg(test)]
mod tests;

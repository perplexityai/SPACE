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
use std::str::FromStr;

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
    pub(crate) const ALL: [Self; 7] = [
        Self::L2Ingress,
        Self::PreTproxy,
        Self::PackagedPolicy,
        Self::EgressGuardrails,
        Self::Nat,
        Self::Forward,
        Self::Proxy,
    ];

    /// This part's position in [`Part::ALL`].
    const fn index(self) -> usize {
        match self {
            Self::L2Ingress => 0,
            Self::PreTproxy => 1,
            Self::PackagedPolicy => 2,
            Self::EgressGuardrails => 3,
            Self::Nat => 4,
            Self::Forward => 5,
            Self::Proxy => 6,
        }
    }

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

#[derive(Debug, thiserror::Error)]
#[error("unknown namespace rule part {0:?}")]
pub(crate) struct UnknownPart(String);

impl FromStr for Part {
    type Err = UnknownPart;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|part| part.as_str() == name)
            .ok_or_else(|| UnknownPart(name.to_string()))
    }
}

const INDEX_IN_RANGE: &str = "every part's index is below Part::ALL.len()";

/// One value for each part.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PerPart<T>([T; Part::ALL.len()]);

impl<T> PerPart<T> {
    pub(crate) fn from_fn(value: impl FnMut(Part) -> T) -> Self {
        Self(Part::ALL.map(value))
    }

    pub(crate) fn get(&self, part: Part) -> &T {
        self.0.get(part.index()).expect(INDEX_IN_RANGE)
    }

    pub(crate) fn set(&mut self, part: Part, value: T) {
        *self.0.get_mut(part.index()).expect(INDEX_IN_RANGE) = value;
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Part, &T)> {
        Part::ALL.into_iter().zip(&self.0)
    }
}

/// How a part of a namespace's rules compares with what this binary installs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartState {
    /// The part carries this binary's version.
    Current,
    /// The part carries another version.
    Stale,
    /// The part carries no stamp, or its install failed.
    Unknown,
}

/// What a namespace's listing shows of one part's stamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StampReading {
    /// The table that holds the part does not exist, so neither do its rules.
    NoTable,
    /// The table exists, but the part's chain holds no stamp, more than one,
    /// or a malformed one.
    NoStamp,
    /// The part's single stamp names this version.
    Version(PartVersion),
}

impl StampReading {
    pub(crate) fn version(self) -> Option<PartVersion> {
        match self {
            Self::Version(version) => Some(version),
            Self::NoTable | Self::NoStamp => None,
        }
    }
}

impl PartState {
    /// Compare a part's stamp, if it has one, with this binary's version.
    pub(crate) fn of(stamp: Option<PartVersion>, current: PartVersion) -> Self {
        match stamp {
            Some(stamp) if stamp == current => Self::Current,
            Some(_) => Self::Stale,
            None => Self::Unknown,
        }
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

impl FromStr for PartVersion {
    type Err = InvalidPartVersion;

    /// Exactly the eight lowercase hex digits a stamp carries.
    fn from_str(digits: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidPartVersion(digits.to_string());
        let lowercase_hex = digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if digits.len() != 8 || !lowercase_hex {
            return Err(invalid());
        }
        u32::from_str_radix(digits, 16)
            .map(Self)
            .map_err(|_| invalid())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid namespace rule part version {0:?}")]
pub(crate) struct InvalidPartVersion(String);

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

/// The part and version a stamp's comment names, or `None` when the comment
/// is not one this binary writes.
pub(super) fn parse_comment(comment: &str) -> Option<(Part, PartVersion)> {
    let (part, version) = comment.strip_prefix(PREFIX)?.split_once('=')?;
    Some((part.parse().ok()?, version.parse().ok()?))
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

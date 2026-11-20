//! Reads the owned table back from Linux and checks it against the reviewed
//! revision. Expectations come from the revision and the fixed placement, never
//! from the rendered batch, so a renderer mistake cannot verify itself.

use std::fs::File;
use std::io;
use std::process::Output;

use pplx_space_firewall_rules_model::{FirewallRuleRevision, Match, Rule};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{CHAIN, FAMILY, Nft, PRIORITY, TABLE, nft, rule_comment};

/// What fresh listings of the owned table showed, for this namespace only.
#[derive(Debug, PartialEq, Eq)]
pub enum Verification {
    Match,
    /// Linux listed no owned table, or one that breaks the contract.
    Mismatch(String),
    /// The table could not be read or understood, so it may or may not match.
    Inconclusive(String),
}

pub fn verify(revision: &FirewallRuleRevision, netns: &File) -> Verification {
    verify_with(revision, netns, &nft)
}

/// [`verify`] through a substitute for nft.
pub(crate) fn verify_with(
    revision: &FirewallRuleRevision,
    netns: &File,
    nft: &Nft<'_>,
) -> Verification {
    let list = |args: &[&str]| nft(netns, args, &[], crate::host_command::PROBE_BUDGET);
    observed(revision, list(&["-j", "list", "tables", FAMILY]), || {
        list(&["-j", "list", "table", FAMILY, TABLE])
    })
}

/// Lists the table only once the family's tables include it, so an absent
/// table is told apart from a failed command without reading nft's errors.
fn observed(
    revision: &FirewallRuleRevision,
    tables: io::Result<Output>,
    table: impl FnOnce() -> io::Result<Output>,
) -> Verification {
    let checked = stdout(tables)
        .and_then(|tables| owned_table_listed(&tables))
        .and_then(|listed| {
            if listed {
                stdout(table())
            } else {
                Err(mismatch(format!("no table {FAMILY} {TABLE}")))
            }
        })
        .and_then(|table| check(revision, &table));
    match checked {
        Ok(()) => Verification::Match,
        Err(verification) => verification,
    }
}

fn stdout(listed: io::Result<Output>) -> Result<Vec<u8>, Verification> {
    let output = listed.map_err(|error| inconclusive(format!("nft did not complete: {error}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(inconclusive(format!(
            "nft {}: {}",
            output.status,
            stderr.trim()
        )));
    }
    Ok(output.stdout)
}

fn owned_table_listed(stdout: &[u8]) -> Result<bool, Verification> {
    let mut listed = false;
    for object in listed_objects(stdout)? {
        let Some(("table", body)) = object.as_object().and_then(single) else {
            return Err(inconclusive(format!("{object} is not a table")));
        };
        let (Some(family), Some(name)) = (body["family"].as_str(), body["name"].as_str()) else {
            return Err(inconclusive(format!("table {body} has no family or name")));
        };
        listed |= (family, name) == (FAMILY, TABLE);
    }
    Ok(listed)
}

fn mismatch(detail: impl Into<String>) -> Verification {
    Verification::Mismatch(detail.into())
}

fn inconclusive(detail: impl Into<String>) -> Verification {
    Verification::Inconclusive(detail.into())
}

/// The objects one nft JSON listing holds after its schema version 1 metainfo.
fn listed_objects(stdout: &[u8]) -> Result<Vec<Value>, Verification> {
    let listing: Value = serde_json::from_slice(stdout)
        .map_err(|error| inconclusive(format!("listing is not JSON: {error}")))?;
    let objects = match listing.as_object().and_then(single) {
        Some(("nftables", Value::Array(objects))) => objects,
        _ => return Err(inconclusive("listing is not one nftables array")),
    };
    let Some((metainfo, objects)) = objects.split_first() else {
        return Err(inconclusive("listing is empty"));
    };
    match metainfo.as_object().and_then(single) {
        Some(("metainfo", body)) if body.get("json_schema_version") == Some(&json!(1)) => {}
        _ => return Err(inconclusive("no metainfo for JSON schema version 1")),
    }
    Ok(objects.to_vec())
}

fn check(revision: &FirewallRuleRevision, stdout: &[u8]) -> Result<(), Verification> {
    let (mut tables, mut chains, mut rules) = (Vec::new(), Vec::new(), Vec::new());
    for object in &listed_objects(stdout)? {
        let Some((kind, body)) = object.as_object().and_then(single) else {
            return Err(inconclusive(format!("{object} is not a single object")));
        };
        match kind {
            "metainfo" => return Err(inconclusive("metainfo listed twice")),
            "table" => tables.push(known(body, &["family", "name"])?),
            "chain" => chains.push(known(
                body,
                &["family", "table", "name", "type", "hook", "prio", "policy"],
            )?),
            "rule" => rules.push(known(
                body,
                &["family", "table", "chain", "comment", "expr"],
            )?),
            other => return Err(mismatch(format!("unexpected {other} object"))),
        }
    }

    if tables != [json!({"family": FAMILY, "name": TABLE})] {
        return Err(mismatch(format!("tables {tables:?}")));
    }
    let chain = json!({
        "family": FAMILY, "table": TABLE, "name": CHAIN,
        "type": "filter", "hook": "prerouting", "prio": PRIORITY, "policy": "accept",
    });
    if chains != [chain] {
        return Err(mismatch(format!("chains {chains:?}")));
    }
    let reviewed = revision.rules();
    if rules.len() != reviewed.len() {
        let (listed, expected) = (rules.len(), reviewed.len());
        return Err(mismatch(format!(
            "{listed} rules listed, {expected} reviewed"
        )));
    }
    rules
        .iter()
        .zip(reviewed)
        .try_for_each(|(listed, rule)| check_rule(listed, rule))
}

fn check_rule(listed: &Value, rule: &Rule) -> Result<(), Verification> {
    let id = rule.id();
    let placement = [("family", FAMILY), ("table", TABLE), ("chain", CHAIN)];
    if placement.iter().any(|(key, value)| listed[key] != *value) {
        return Err(mismatch(format!("rule {id} is outside the owned chain")));
    }
    if listed["comment"] != rule_comment(id) {
        return Err(mismatch(format!(
            "rule {id} is listed as {}",
            listed["comment"]
        )));
    }
    let Some(expr) = listed["expr"].as_array() else {
        return Err(inconclusive(format!("rule {id} has no expression array")));
    };
    // The SPACE guard, each reviewed comparison, then the drop; nothing else.
    let (guard, comparisons, verdict) = match expr.as_slice() {
        [guard, comparisons @ .., verdict] if comparisons.len() == rule.all_of().len() => {
            (guard, comparisons, verdict)
        }
        _ => {
            return Err(mismatch(format!(
                "rule {id} has {} expressions",
                expr.len()
            )));
        }
    };
    let expected_guard = json!({"match": {
        "op": "==", "left": {"meta": {"key": "iifname"}}, "right": crate::net::TAP_DEVICE,
    }});
    if *guard != expected_guard {
        return Err(mismatch(format!("rule {id} is guarded by {guard}")));
    }
    for (expression, reviewed) in comparisons.iter().zip(rule.all_of()) {
        let Some(("match", operand)) = expression.as_object().and_then(single) else {
            return Err(mismatch(format!("rule {id} holds {expression}")));
        };
        // Read with the revision's own strict grammar: a listing shape it
        // cannot read is unsupported, not a difference.
        let observed = Match::deserialize(operand)
            .map_err(|error| inconclusive(format!("rule {id}: {operand}: {error}")))?;
        if observed != *reviewed {
            return Err(mismatch(format!("rule {id} compares {operand}")));
        }
    }
    if *verdict != json!({"drop": null}) {
        return Err(mismatch(format!("rule {id} ends with {verdict}")));
    }
    Ok(())
}

fn single(object: &Map<String, Value>) -> Option<(&str, &Value)> {
    let mut entries = object.iter();
    match (entries.next(), entries.next()) {
        (Some((key, value)), None) => Some((key.as_str(), value)),
        _ => None,
    }
}

/// The object without its kernel-assigned handle, the only listing metadata
/// ignored. A key outside `keys` has semantics this check does not know.
fn known(body: &Value, keys: &[&str]) -> Result<Value, Verification> {
    let Some(body) = body.as_object() else {
        return Err(inconclusive(format!("{body} is not an object")));
    };
    let mut normalized = body.clone();
    normalized.remove("handle");
    match normalized.keys().find(|key| !keys.contains(&key.as_str())) {
        Some(key) => Err(inconclusive(format!("unrecognized listing key {key:?}"))),
        None => Ok(Value::Object(normalized)),
    }
}

#[cfg(test)]
mod tests;

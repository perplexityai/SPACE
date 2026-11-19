//! The nft JSON that builds SPACE's owned table for a reviewed firewall rule
//! revision. Construction only: replacing an installed table is the caller's
//! job, and nothing here touches the baseline tables.

use pplx_space_firewall_rules_model::{FirewallRuleRevision, RuleId};
use serde_json::{Value, json};

const FAMILY: &str = "ip";
const TABLE: &str = "space_firewall_rules";
const CHAIN: &str = "guest_drop";
// After conntrack (-200) and just ahead of the baseline pre-TPROXY guardrails.
const PRIORITY: i32 = super::PRE_TPROXY_GUARDRAILS_PRIO - 1;

/// The comment that ties an installed rule to its reviewed ID. `RuleId` admits
/// no `:`, so the prefix never collides with an ID.
fn rule_comment(id: &RuleId) -> String {
    format!("space-firewall-rule:{id}")
}

/// The ordered nft JSON commands that construct the owned table. Built only by
/// [`render`], so no caller can pass raw JSON in its place.
#[derive(Debug)]
pub struct TableConstruction(Vec<Value>);

impl TableConstruction {
    pub fn commands(&self) -> &[Value] {
        &self.0
    }
}

/// Renders every rule once, in revision order, from its borrowed comparisons.
pub fn render(revision: &FirewallRuleRevision) -> TableConstruction {
    let table = json!({"add": {"table": {"family": FAMILY, "name": TABLE}}});
    let chain = json!({"add": {"chain": {
        "family": FAMILY, "table": TABLE, "name": CHAIN,
        "type": "filter", "hook": "prerouting", "prio": PRIORITY, "policy": "accept",
    }}});
    let rules = revision.rules().iter().map(|rule| {
        let guard = json!({"match": {
            "left": {"meta": {"key": "iifname"}}, "op": "==", "right": crate::net::TAP_DEVICE,
        }});
        let comparisons = rule
            .all_of()
            .iter()
            .map(|comparison| json!({"match": comparison}));
        let expr: Vec<Value> = std::iter::once(guard)
            .chain(comparisons)
            .chain([json!({"drop": null})])
            .collect();
        json!({"add": {"rule": {
            "family": FAMILY, "table": TABLE, "chain": CHAIN,
            "comment": rule_comment(rule.id()), "expr": expr,
        }}})
    });
    TableConstruction([table, chain].into_iter().chain(rules).collect())
}

#[cfg(test)]
mod tests;

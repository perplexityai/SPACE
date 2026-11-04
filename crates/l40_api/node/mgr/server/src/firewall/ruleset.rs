//! A sandbox namespace's whole nftables ruleset, read chain by chain.

use serde_json::Value;

use super::{FirewallError, FirewallResult};

/// One `nft -j list ruleset` of a namespace. Startup reads several chains of
/// every slot namespace before the node accepts work; one listing replaces a
/// listing per chain.
pub(crate) struct NamespaceRuleset {
    entries: Vec<Value>,
}

#[derive(serde::Deserialize)]
struct Listing {
    nftables: Vec<Value>,
}

impl NamespaceRuleset {
    pub(crate) fn list(ns: &str) -> FirewallResult<Self> {
        let output = super::nft_output(Some(ns), &["-j", "list", "ruleset"])
            .map_err(FirewallError::ListStart)?;
        if !output.status.success() {
            return Err(FirewallError::ListFailed {
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Self::parse(&output.stdout)
    }

    fn parse(stdout: &[u8]) -> FirewallResult<Self> {
        let Listing { nftables } =
            serde_json::from_slice(stdout).map_err(FirewallError::ListJson)?;
        Ok(Self { entries: nftables })
    }

    /// A chain exactly as `nft -j list chain` lists it: the listing's
    /// metainfo, the chain, then its rules in order. `None` when the chain
    /// does not exist, as for a chain listing that reports it missing.
    pub(super) fn chain(&self, family: &str, table: &str, name: &str) -> Option<Value> {
        let names = |object: &Value, key: &str| {
            object["family"] == family && object["table"] == table && object[key] == name
        };
        let chain = self
            .entries
            .iter()
            .find(|entry| entry.get("chain").is_some_and(|chain| names(chain, "name")))?;
        let metainfo = self
            .entries
            .iter()
            .filter(|entry| entry.get("metainfo").is_some());
        let rules = self
            .entries
            .iter()
            .filter(|entry| entry.get("rule").is_some_and(|rule| names(rule, "chain")));
        let nftables = metainfo
            .chain(std::iter::once(chain))
            .chain(rules)
            .cloned()
            .collect::<Vec<_>>();
        Some(serde_json::json!({ "nftables": nftables }))
    }
}

#[cfg(test)]
mod tests;

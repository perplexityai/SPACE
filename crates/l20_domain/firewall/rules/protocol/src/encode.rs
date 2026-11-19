//! The canonical encoder: the version envelope around the model's own
//! serialization.

use pplx_space_firewall_rules_model::FirewallRuleRevision;

use crate::SCHEMA_VERSION;
use crate::wire::{RevisionV1, RuleV1};

/// Writes `revision` in its canonical form: fields in grammar order, rules and
/// comparisons in their own order, and no whitespace. Equal revisions encode
/// to equal bytes. A revision [`decode_revision`](crate::decode_revision)
/// returned decodes back to an equal one; one built with the model's
/// constructors may break nft's limits and not decode.
pub fn encode_revision(revision: &FirewallRuleRevision) -> String {
    let document = RevisionV1 {
        schema_version: SCHEMA_VERSION,
        rules: revision
            .rules()
            .iter()
            .map(|rule| RuleV1 {
                id: rule.id().as_str(),
                all_of: rule.all_of(),
            })
            .collect::<Vec<_>>(),
    };
    match serde_json::to_string(&document) {
        Ok(text) => text,
        // Every map key is a string and no type here returns an error, so
        // writing to a String cannot fail.
        Err(error) => unreachable!("revision serialization failed: {error}"),
    }
}

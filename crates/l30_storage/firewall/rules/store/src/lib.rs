//! Owning store for firewall-rules rollouts in control-plane Postgres: the
//! rollout header, its per-node work items, and their supporting records.
//!
//! * `firewall_rules_rollouts`: the operator's rollout header.
//! * `firewall_rules_rollout_items`: the fixed set of nodes a rollout targets,
//!   and their work state.
//! * `firewall_rules_revisions`: immutable rulesets.
//! * `firewall_rules_control`: the default revision for newly enrolled nodes.
//! * `firewall_rules_node_assignments`: each node's current intent.
//! * `firewall_rules_node_observations`: each node's accepted observations.
//! * `firewall_rules_mutation_receipts`: idempotent responses to operator
//!   requests.

mod fingerprint;
mod receipt;

pub use fingerprint::{MutationRequest, RequestFingerprint};
pub use receipt::{Claim, ReceiptRequest, claim_receipt, insert_receipt};

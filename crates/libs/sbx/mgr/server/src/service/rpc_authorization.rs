//! RPC caller roles and request scope normalization.

use super::*;

pub(super) fn request_sandbox_ids(sandbox_ids: &[String]) -> Vec<String> {
    let mut ids = Vec::with_capacity(sandbox_ids.len());
    let mut seen = BTreeSet::new();
    for sandbox_id in sandbox_ids.iter().map(|id| id.trim()) {
        if !sandbox_id.is_empty() && seen.insert(sandbox_id.to_owned()) {
            ids.push(sandbox_id.to_owned());
        }
    }
    ids
}

pub(super) const SPACE_TMPLMGR_ACCOUNT: &str = user_names::SPACE_TMPLMGR;

pub(super) const ALLOW_SANDBOX_OBSERVER: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];

pub(super) const ALLOW_SANDBOX_OBSERVER_OR_TMPLMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];

// Direct sandbox/snapshot import and export streams require root or a local
// operator; node-api retains separate authority for lifecycle and backup RPCs.
pub(super) const ALLOW_LOCAL_ARTIFACT_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
];

pub(super) const ALLOW_SANDBOX_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
];

pub(super) const ALLOW_SANDBOX_AUTO_LIFECYCLE_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

pub(super) const ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

pub(super) const ALLOW_SANDBOX_SNAPSHOT_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

pub(super) const ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];

pub(super) const ALLOW_AUTO_LIFECYCLE_DELETE_RECEIPTS: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

// The pending-backup inventory is the node's own answer to "what on this
// disk has no durable copy". Its only caller is node-ctl, which publishes it
// in the Full census; root covers break-glass inspection. Deliberately not
// the general observer set, which would put a node's data-loss exposure in
// front of every local reader.
pub(super) const ALLOW_LOCAL_BACKUP_INVENTORY: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

pub(super) const ALLOW_SANDBOX_ADMIN_OR_TMPLMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
];

pub(super) const ALLOW_SANDBOX_POOL_ADOPT: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
];

pub(super) const ALLOW_SANDBOX_START_CONTEXT: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
];

/// The callers that run lifecycle work for the control plane. Every one is
/// also allowed Start and Stop, which an upgrade runs as its caller.
pub(super) const ALLOW_SANDBOX_RUNTIME_UPGRADE: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
];

// Cluster-deleted evictions are driven by node-ctl's desired-state facet
// sync, which carries the authoritative "no cluster row" signal. Root and
// the local operator role cover break-glass tooling; deliberately not the
// general sandbox-operator set.
pub(super) const ALLOW_SANDBOX_CLUSTER_DELETED_EVICT: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

//! Resolves the node-group candidate set for one sandbox pod build.
//!
//! [`NodeGroupPlacement`] is the renderer: given a candidate set it emits the
//! required nodeAffinity term over
//! `node-restriction.kubernetes.io/space-node-group`, the preferred bias toward
//! the caller's dedicated groups, and the dedicated-org toleration. This module
//! is the half that computes the set, so every pod-build path can stop passing
//! `node_group_placement: None`:
//!
//! ```text
//! templates row + node_groups rows --> NodeGroup::compatible --> NodeGroupPlacement
//! ```
//!
//! [`NodeGroup::compatible`] is the whole definition of the set and this module
//! adds nothing to it. In particular the query is unfiltered: a generic
//! template is compatible with **every** generic-policy group, not just the
//! `is_default` one, and a caller that owns dedicated capacity gets
//! `{all generic groups} ∪ {its own compatible dedicated groups}`. Nothing
//! downstream would catch a narrower query — `compatible` never reads
//! `is_default`, so "all generic groups" and "the default generic group" both
//! satisfy the predicate — which is why this module's test module pins a
//! non-default generic group's presence in the set directly.
//!
//! # The gate
//!
//! Every path resolves through here, and with
//! `SPACE_SANDBOX_NODE_GROUP_PLACEMENT` unset (the shipped default) `resolve`
//! returns [`ResolvedPlacement::Disabled`] before it touches the database.
//! Rendered pods are then byte-identical to pre-node-group pods. See
//! [`SANDBOX_NODE_GROUP_PLACEMENT_ENV`] for why the ordering that gate protects
//! is not a preference.
//!
//! # An empty candidate set means two different things
//!
//! The distinction is what keeps the flag safe to flip:
//!
//! * For a **generic** template an empty set can only mean the cluster has no
//!   generic node group at all. That is provable from the predicate rather than
//!   assumed: for a generic group `compatible` reduces to
//!   `template.visibility == Generic` — its `Generic` arm tests nothing else,
//!   and the cross-org guard ahead of that arm (`launchable_by`) is
//!   unconditionally true for a generic template, whatever the caller org. So
//!   every generic group admits every generic template, and the set is empty
//!   only when there is no generic group to admit it. The feature is
//!   unprovisioned here, so placement is inert —
//!   [`ResolvedPlacement::Unprovisioned`], no constraints rendered. Enabling
//!   the flag before provisioning a generic group must be a no-op rather than
//!   a cluster-wide create failure.
//! * For an **org-specific** template it means the owning org has no compatible
//!   dedicated group. That fails closed with
//!   [`NodeGroupPlacementResolveError::NoCompatibleNodeGroup`]: there is no
//!   shared fallback, because a born-private template never touches shared
//!   hardware. It is the data-isolation guarantee, and it is per (org,
//!   template) rather than cluster-wide.
//!
//! [`SANDBOX_NODE_GROUP_PLACEMENT_ENV`]: pplx_space_common_core::kubernetes::SANDBOX_NODE_GROUP_PLACEMENT_ENV

use pplx_space_common_core::types::SandboxId;

use std::sync::atomic::{AtomicBool, Ordering};

use k8s_openapi::api::core::v1::Node;
use pplx_space_common_core::kubernetes::{
    NodeGroupPlacement, NodeGroupPlacementError, SANDBOX_NODE_GROUP_PLACEMENT_ENV, node_group_label,
};
use pplx_space_common_core::types::{NodeGroup, OrgId, TemplateVisibility};
use pplx_space_metrics::node_group_placement as metrics;
use sqlx::PgPool;

mod store;

// Only the error is public: it is the payload of a public variant below. The
// catalog read and its record stay private — the resolver owns that read, and a
// second public entry point to it would be a second way to compute a candidate
// set.
pub use store::NodeGroupCatalogError;
use store::{NodeGroupCatalog, catalog_for_template};

use pplx_space_metrics::node_group_placement::LocalRestorePreflight;
pub use pplx_space_metrics::node_group_placement::ResolvePath;

/// Who the candidate set is being resolved for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementSubject {
    /// A sandbox owned by an org. On create that is the requesting org; on
    /// restore, evacuation, and drain-migrate it is the **sandbox's owning
    /// org**, never the triggering actor's — those paths often have no end-user
    /// caller, and feeding an operator's org into `compatible` would reject the
    /// owner org's own template as cross-org.
    Sandbox(OrgId),
    /// A warm-pool slot: created before anyone claims it, so it has no org
    /// identity at all. `compatible` then matches no dedicated group — a
    /// dedicated group's owner is `Some(org)` and this caller is `None` — so
    /// the set can only ever name generic groups, which is what the pod builder
    /// requires of an org-less pod.
    WarmPool,
}

impl PlacementSubject {
    /// The org the compatibility predicate is evaluated for. `None` is not a
    /// missing argument: it is the warm pool's actual identity, and
    /// [`NodeGroup::compatible`] gives it generic templates on generic groups
    /// and nothing else.
    fn caller_org(self) -> Option<OrgId> {
        match self {
            Self::Sandbox(org_id) => Some(org_id),
            Self::WarmPool => None,
        }
    }
}

/// What one resolution produced. The two non-`Placed` variants both render no
/// node-group constraints, but they are different conditions and only one of
/// them is a misconfiguration, so they stay distinguishable rather than
/// collapsing into `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedPlacement {
    /// `SPACE_SANDBOX_NODE_GROUP_PLACEMENT` is off in this process. No query
    /// was issued.
    Disabled,
    /// No **generic** node group admits this template, so no boundary is
    /// rendered — preserving placement without node-group constraints.
    ///
    /// Not the same as "the cluster has no node groups", and the difference
    /// matters: a dedicated group can exist while no generic one does, and in
    /// that state an org without dedicated capacity resolves an empty set,
    /// lands here, and renders nothing — held off the dedicated nodes only by
    /// the taint. Rendering nothing there is byte-identical to pre-feature
    /// behavior rather than a regression, and `create_node_group` now refuses
    /// to create the first node group as a dedicated one, which is what makes
    /// the state unreachable. See the module docs.
    Unprovisioned,
    /// The candidate set, ready to render.
    Placed(NodeGroupPlacement),
}

impl ResolvedPlacement {
    /// The placement to hand to `build_sandbox_pod`, or `None` when nothing is
    /// to be rendered.
    pub fn placement(&self) -> Option<&NodeGroupPlacement> {
        match self {
            Self::Disabled | Self::Unprovisioned => None,
            Self::Placed(placement) => Some(placement),
        }
    }
}

/// Failure resolving a candidate set. Every variant fails the pod build closed:
/// a boundary that cannot be computed is not a boundary to guess at.
#[derive(Debug, thiserror::Error)]
pub enum NodeGroupPlacementResolveError {
    /// No node group admits this (org, template) pair. Reached only for an
    /// **org-specific** template, whose org has no compatible dedicated group;
    /// a generic template's empty set reports [`ResolvedPlacement::Unprovisioned`]
    /// instead.
    ///
    /// Note what it does *not* establish: nothing here checks whether generic
    /// capacity exists, so an org-specific template on a cluster whose
    /// `node_groups` table is empty lands here as well,
    /// beside the `Unprovisioned` its generic templates report. Triage the two
    /// together rather than reading this one as a per-org capacity gap.
    ///
    /// User-facing on create and on restore, both as a 409.
    #[error("no node group in this cluster admits template '{template}' for this organization")]
    NoCompatibleNodeGroup {
        template: String,
        /// `None` for a warm-pool slot, which has no org identity. Kept off the
        /// message: an org UUID says nothing to the caller who receives the
        /// 409, and the request log and
        /// `node_group_placement.resolved{outcome:no_compatible_group}` carry
        /// it for the operator who acts on it.
        caller_org: Option<OrgId>,
    },
    /// The `templates` row is gone, so the template's visibility — and with it
    /// the entire compatibility relation — is unknowable. Reachable for a
    /// suspended sandbox whose template was deleted and then reaped by
    /// tombstone retention. Fails closed rather than assuming `generic`,
    /// because assuming generic on an org-specific rootfs is precisely the
    /// data-isolation violation the boundary exists to prevent.
    #[error(
        "template '{template}' has no templates row, so its node-group compatibility cannot be \
         resolved"
    )]
    TemplateAbsent { template: String },
    #[error(transparent)]
    Catalog(#[from] NodeGroupCatalogError),
    /// [`NodeGroupPlacement::for_caller`] rejected a candidate this module had
    /// already filtered with the same predicate, so the two disagree.
    /// Unreachable by construction and fatal on purpose: the constructor is the
    /// authority on what a renderable set is.
    #[error(transparent)]
    Invariant(#[from] NodeGroupPlacementError),
}

impl NodeGroupPlacementResolveError {
    fn outcome(&self) -> metrics::ResolveOutcome {
        match self {
            Self::NoCompatibleNodeGroup { .. } => metrics::ResolveOutcome::NoCompatibleGroup,
            Self::TemplateAbsent { .. } => metrics::ResolveOutcome::TemplateAbsent,
            Self::Catalog(_) | Self::Invariant(_) => metrics::ResolveOutcome::Error,
        }
    }
}

/// A local-only restore's source node is outside the candidate set.
///
/// Its own type rather than a variant of [`NodeGroupPlacementResolveError`]:
/// the two are produced by different functions, and folding them together makes
/// each one's `Result` advertise a failure it cannot return — visibly so in the
/// metric mapping, where this condition is booked on
/// `local_restore_preflight{verdict}` and never on `resolved{outcome}`.
///
/// The pin is **not** dropped in response. `direct_node_name` is populated only
/// when there is no portable snapshot, so the sandbox's memory and disk exist on
/// that node and nowhere else: any other node in the set has neither the local
/// bytes nor a storage URI to restore from. Relaxing the pin would trade a clear
/// failure for a pod that schedules, consumes a network slot, and then cannot
/// restore.
///
/// The repair is to make the snapshot portable, and it does **not** require
/// restoring first — which matters, because "suspend it to a portable snapshot"
/// is not a thing an already-suspended sandbox can do. The snapshot files stay
/// on the node after a normal suspend (`CleanupCompletion::MarkSuspended` runs
/// with `keep_filesystem = true`), so sbx-mgr can upload them straight from the
/// `Suspended` state. Once the upload registers, the sandbox has a storage URI,
/// `direct_node_name` is no longer set, and the restore is placed by the
/// scheduler inside the boundary.
///
/// The message names **draining the node** rather than the upload itself,
/// because a drain is the entry point to the upload either way. Two things
/// perform it: the drain reconciler's promotion step, which walks exactly this
/// population — suspended sandboxes pinned to a node whose snapshot is not yet
/// registered — and backs each one up (`promote_local_snapshot`,
/// `node_drain/store.rs`); and the admin action
/// `POST /admin/api/v1/nodes/{id}:force-upload-local-snapshots`, which reaches
/// the same end state through `enqueue_artifact_backup`: the admin action
/// selects the current instance, while drain pins the exact local artifact
/// when its identity is recorded. That
/// action also refuses unless the node is already draining in a mode whose gate
/// blocks concurrent wakes (`force_upload_gate_open_message`,
/// `routes/admin/nodes.rs`), because it reads the live on-disk snapshot
/// directory and a concurrent wake plus re-suspend would race it. Naming the
/// drain therefore covers both without sending the reader at an endpoint that
/// will refuse them. Returning the node to a
/// compatible group is the other way out, and the message names both.
#[derive(Debug, thiserror::Error)]
#[error(
    "sandbox {sandbox_id} can only restore from node {node_name}, which is outside the \
     candidate set for template '{template}' — {source_placement}; drain that node to publish \
     this sandbox's snapshot to remote storage, after which it can restore anywhere in the \
     set, or return the node to a compatible group"
)]
pub struct LocalRestoreSourceError {
    pub sandbox_id: SandboxId,
    pub node_name: String,
    /// Why the node is outside the set, as a clause the message reads inline.
    ///
    /// A rendered phrase rather than a group name, because the two reasons do
    /// not share a shape: a foreign label has a group to name, and an absent
    /// one does not. Substituting a sentinel into "whose node group {…} is
    /// outside the set" told the caller their node belonged to a group called
    /// `<unlabeled>`.
    pub source_placement: String,
    pub template: String,
}

/// Pool-bound resolver, constructed once per process.
///
/// Holds the gate as a value read at construction rather than per call:
/// `SPACE_SANDBOX_NODE_GROUP_PLACEMENT` is a process env var, so a flip is a
/// deploy and reaching for `std::env::var` on every create would only add a
/// syscall to the hot path.
#[derive(Clone)]
pub struct NodeGroupPlacementResolver {
    pool: PgPool,
    enabled: bool,
}

/// Latches the first `Unprovisioned` warn so enabling the gate before a generic
/// group is provisioned logs once per process instead of once per sandbox
/// create. The metric counts every occurrence.
///
/// Process-global rather than a field, because a process can hold more than one
/// resolver: `space-cluster-controller` builds sandbox pods from two places —
/// the substrate's `ensure_pod_exists` and the warm-pool reconciler — and a
/// per-resolver latch would make "logged once per process" false there, at one
/// line per resolver.
///
/// One-way, and never reset: a cluster that is provisioned and later loses its
/// generic group (a stage-4 rollback that deletes the default) logs nothing at
/// all for that second era. The counter is the signal there, which is why the
/// warn points at it.
static UNPROVISIONED_WARNED: AtomicBool = AtomicBool::new(false);

impl NodeGroupPlacementResolver {
    /// Explicit gate, for tests and for callers that already hold the config.
    pub fn new(pool: PgPool, enabled: bool) -> Self {
        Self { pool, enabled }
    }

    /// Resolve the candidate set for one pod build.
    ///
    /// `template_name` must be the *same* string the pod will render in its
    /// `template` field: the pod builder cross-checks the placement's template
    /// against the pod's and renders an unsatisfiable boundary on a mismatch.
    pub async fn resolve(
        &self,
        path: ResolvePath,
        subject: PlacementSubject,
        template_name: &str,
    ) -> Result<ResolvedPlacement, NodeGroupPlacementResolveError> {
        if !self.enabled {
            return Ok(ResolvedPlacement::Disabled);
        }
        // `resolve_enabled` returns `Option`, so `Disabled` is unrepresentable
        // below the gate rather than an unreachable arm this match carries.
        let resolved = self.resolve_enabled(path, subject, template_name).await;
        match &resolved {
            Ok(None) => metrics::resolved(path, metrics::ResolveOutcome::Unprovisioned),
            Ok(Some(placement)) => metrics::resolved(
                path,
                if placement.targets_dedicated_capacity() {
                    metrics::ResolveOutcome::Dedicated
                } else {
                    metrics::ResolveOutcome::Shared
                },
            ),
            Err(error) => metrics::resolved(path, error.outcome()),
        }
        resolved.map(|placement| match placement {
            Some(placement) => ResolvedPlacement::Placed(placement),
            None => ResolvedPlacement::Unprovisioned,
        })
    }

    /// The gate-open path. `Ok(None)` is the unprovisioned cluster.
    async fn resolve_enabled(
        &self,
        path: ResolvePath,
        subject: PlacementSubject,
        template_name: &str,
    ) -> Result<Option<NodeGroupPlacement>, NodeGroupPlacementResolveError> {
        let caller_org = subject.caller_org();
        let Some(catalog) = catalog_for_template(&self.pool, template_name).await? else {
            return Err(NodeGroupPlacementResolveError::TemplateAbsent {
                template: template_name.to_string(),
            });
        };
        let NodeGroupCatalog { template, groups } = catalog;
        let candidates: Vec<NodeGroup> = groups
            .into_iter()
            .filter(|group| group.compatible(caller_org, &template))
            .collect();
        if candidates.is_empty() {
            return Self::empty_candidate_set(path, caller_org, template.visibility, template_name);
        }
        Ok(Some(NodeGroupPlacement::for_caller(
            caller_org,
            &template,
            &candidates,
        )?))
    }

    fn empty_candidate_set(
        path: ResolvePath,
        caller_org: Option<OrgId>,
        visibility: TemplateVisibility,
        template_name: &str,
    ) -> Result<Option<NodeGroupPlacement>, NodeGroupPlacementResolveError> {
        match visibility {
            // Provable, not assumed: see the module docs.
            TemplateVisibility::Generic => {
                if !UNPROVISIONED_WARNED.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        %path,
                        template = template_name,
                        gate = SANDBOX_NODE_GROUP_PLACEMENT_ENV,
                        "node-group placement is enabled but this cluster has no generic node \
                         group, so no node-group constraints are rendered; create the default \
                         generic group or disable the gate. Logged once \
                         per process; the resolved counter's unprovisioned outcome carries the \
                         rate"
                    );
                }
                Ok(None)
            }
            TemplateVisibility::OrgSpecific => {
                // Warn once per *operation*, not once per resolve. Two callers
                // ask this question and would otherwise both answer it:
                //
                // * An org-less caller — the warm-pool fill — reaching an
                //   org-specific template is the routine, expected decline.
                //   Nothing failed closed; that config simply never fills.
                // * `PoolClaim` is a probe on the create path, asking whether a
                //   pooled pod could serve this sandbox. Its refusal is not the
                //   outcome: the create continues, builds cold, and resolves
                //   again on `path:create`, which is where the warn belongs.
                //   Warning here too would put two identical unlatched lines in
                //   front of every such create.
                if caller_org.is_some() && path != ResolvePath::PoolClaim {
                    tracing::warn!(
                        %path,
                        template = template_name,
                        ?caller_org,
                        "no node group admits this org-specific template for its org; failing \
                         closed with no shared fallback"
                    );
                }
                Err(NodeGroupPlacementResolveError::NoCompatibleNodeGroup {
                    template: template_name.to_string(),
                    caller_org,
                })
            }
        }
    }
}

/// What the node reflector could say about a restore's source node.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RestoreSourceNode<'a> {
    /// The reflector holds the node object.
    Resolved(&'a Node),
    /// A **warm** reflector does not hold it: the node is gone.
    Absent,
    /// The reflector has not completed its initial `LIST`, so an absent node
    /// proves nothing.
    CacheCold,
}

/// Pre-flight for a local-only restore that is about to pin `spec.nodeName`.
///
/// The pin skips kube-scheduler, so nothing evaluates the rendered boundary
/// until kubelet admission — and a rejection there is terminal for the restore
/// rather than a cross-node re-drive. Checking the source node's live label
/// first converts that into an actionable error before a pod is created and a
/// network slot consumed.
///
/// [`RestoreSourceNode::Absent`] and [`RestoreSourceNode::CacheCold`] both keep
/// the pin: a cold cache is a control-plane state that must not convert into a
/// restore failure, and a vanished node fails the restore at binding with its
/// existing shape. Kubelet admission stays the backstop in both cases.
pub(crate) fn check_local_restore_source(
    placement: &NodeGroupPlacement,
    sandbox_id: SandboxId,
    node_name: &str,
    source: RestoreSourceNode<'_>,
) -> Result<(), LocalRestoreSourceError> {
    // One arm per variant, no `_` catch-all: a future `RestoreSourceNode` state
    // must pick its own verdict rather than silently reporting `cache_cold`,
    // which is the one verdict that means "keep the pin, nothing is wrong".
    let node = match source {
        RestoreSourceNode::Resolved(node) => node,
        RestoreSourceNode::Absent => {
            return keep_pin(LocalRestorePreflight::NodeAbsent, sandbox_id, node_name);
        }
        RestoreSourceNode::CacheCold => {
            return keep_pin(LocalRestorePreflight::CacheCold, sandbox_id, node_name);
        }
    };
    // An `In (…)` term does not match a node that lacks the key, so an
    // unlabeled source node is outside the set exactly as a foreign-labeled
    // one is — kubelet would reject the pod either way. The verdict tag is
    // what separates "mid-rollout worker" from "reassigned group" in triage.
    let (verdict, source_placement) = match node_group_label(node) {
        None => (
            LocalRestorePreflight::LabelAbsent,
            "it carries no node-group label".to_string(),
        ),
        Some(group) if !placement.admits_group(group) => (
            LocalRestorePreflight::OutOfSet,
            format!("its node group is {group}"),
        ),
        Some(_) => {
            metrics::local_restore_preflight(LocalRestorePreflight::InSet);
            return Ok(());
        }
    };
    metrics::local_restore_preflight(verdict);
    Err(LocalRestoreSourceError {
        sandbox_id,
        node_name: node_name.to_string(),
        source_placement,
        template: placement.template().as_str().to_string(),
    })
}

/// Books a verdict that leaves the source-node pin in place. Kubelet admission
/// stays the backstop: not knowing where a node sits is not the same as knowing
/// it sits outside the set.
fn keep_pin(
    verdict: LocalRestorePreflight,
    sandbox_id: SandboxId,
    node_name: &str,
) -> Result<(), LocalRestoreSourceError> {
    metrics::local_restore_preflight(verdict);
    tracing::warn!(
        sandbox_id = %sandbox_id,
        node_name,
        %verdict,
        "could not read the restore source node's live node-group label; keeping the \
         source-node pin with kubelet admission as the only check"
    );
    Ok(())
}

#[cfg(test)]
mod tests;

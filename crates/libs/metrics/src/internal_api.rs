use crate::emit::{counter, distribution, tag};
use pplx_space_common_core::types::QuarantineRecoveryPlanRejectionCause;

/// Records a designed `FAILED_PRECONDITION` from the legacy quarantine-recovery
/// plan endpoint. The cause is bounded; sandbox and node identities belong in
/// the caller's structured log rather than metric tags.
pub fn quarantine_recovery_plan_rejected(cause: QuarantineRecoveryPlanRejectionCause) {
    counter(
        "internal_api.quarantine_recovery_plan.rejected",
        1,
        &[tag("cause", cause)],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EcwCredentialVerificationResult {
    V2Current,
    V2Previous,
    Invalid,
    ConfigurationError,
}

impl EcwCredentialVerificationResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V2Current => "v2_current",
            Self::V2Previous => "v2_previous",
            Self::Invalid => "invalid",
            Self::ConfigurationError => "configuration_error",
        }
    }
}

impl std::fmt::Display for EcwCredentialVerificationResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn ecw_credential_verification(kind: &str, result: EcwCredentialVerificationResult) {
    counter(
        "internal_api.ecw_credential.verification",
        1,
        &[tag("kind", kind), tag("result", result.as_str())],
    );
}

metric_vocabulary!(
    /// What one node census did to that node's `node_local_backup_pending`
    /// mirror.
    ///
    /// Exactly one value is recorded per `PublishNodeState` call — every
    /// call, not only the ones carrying a report. Most publications are
    /// facet ticks that structurally cannot carry one: the report rides the
    /// Full census only, so the three node-ctl publish sites that send
    /// `sandbox_inventory_complete: false` never carry it — including
    /// `publish_network_policy`, which does ship sandbox rows, just not a
    /// complete inventory. `no_report` therefore dominates by construction
    /// and the raw `applied` share is **not** a readiness number.
    ///
    /// Read it as a partition of the failure modes instead: among the
    /// publications that did carry a report, `applied` against
    /// `census_rejected` / `malformed_report` / `undescribed_sandbox` is what
    /// says whether a node's mirror is keeping up, and only `applied`
    /// advances the freshness gate the drain reads. A per-environment cutover
    /// wants that restricted comparison, or the gate column's own freshness
    /// across live nodes — not this counter's overall ratio.
    LocalBackupQueueOutcome {
        /// The node's set replaced the mirror and the freshness gate advanced.
        Applied => "applied",
        /// The publication carried no complete report: a delta/`Items` tick,
        /// or a node image that predates the field. The node stays on the
        /// drain's history path.
        NoReport => "no_report",
        /// The server did not accept the sandbox census this publication
        /// claimed, so it cannot certify the pending set that came with it
        /// either.
        CensusRejected => "census_rejected",
        /// The publication took some sandbox only as lifecycle state, and
        /// named no pending artifact for it — so the node could not fully
        /// describe a sandbox and the report may be missing that sandbox's
        /// entry, which a set-replace would read as "no longer pending".
        UndescribedSandbox => "undescribed_sandbox",
        /// The report itself was unusable: an unparseable identity, or two
        /// entries naming the same sandbox, which contradicts the
        /// one-row-per-`(node, sandbox)` contract.
        MalformedReport => "malformed_report",
    }
);

metric_vocabulary!(
    /// Why a mirrored pending-backup row left the queue.
    LocalBackupQueueRemoval {
        /// The census stopped reporting the artifact, and the control plane
        /// confirmed either a durable copy of it or that its sandbox is
        /// restorable without this node at all.
        CensusDurable => "census_durable",
        /// `FinalizeArtifactBackup` flipped exactly
        /// this artifact's snapshot to `ready`.
        Finalized => "finalized",
    }
);

pub fn local_backup_queue_reconcile(outcome: LocalBackupQueueOutcome) {
    counter(
        "internal_api.local_backup_queue.reconcile",
        1,
        &[tag("outcome", outcome)],
    );
}

/// One node's queue depth, sampled at the end of its census reconcile.
///
/// A distribution rather than a gauge because the emitter is ClusterApi, which
/// serves every node's census from several replicas: an untagged gauge would
/// be last-writer-wins across the fleet, and a `node`-tagged one would mint a
/// series per worker node. As a distribution the fleet's p50/max are readable
/// without either, which is what "queue depth tracks in-flight backups, not
/// history" needs to be checkable. Per-node depth for one specific node is a
/// database question, not a metric one.
pub fn local_backup_queue_depth(rows: u64) {
    distribution("internal_api.local_backup_queue.depth", rows as f64, &[]);
}

/// One node's standing count of rows kept because the node stopped reporting
/// them and nothing could confirm them recoverable.
///
/// This is the casualty signal, and the reason the set-replace is not a plain
/// delete-then-insert: a row here names an artifact the node has stopped
/// accounting for and which the control plane cannot prove is recoverable
/// without that node's disk.
///
/// A **distribution of a stock**, not a counter of arrivals, and the
/// distinction is the difference between a usable alert and a pager that never
/// stops. Every census recounts the node's whole standing absent population,
/// so a single permanently stuck row added to a counter would read as one
/// casualty per minute forever. Sampled with zero included, so the maximum is
/// readable and the series does not go stale while the fleet is healthy; a
/// distribution rather than a gauge for the same reason
/// [`local_backup_queue_depth`] is one.
///
/// **A non-zero maximum is not by itself a casualty**, so alert on it rising
/// or on it persisting, not on the first sample above zero. Several
/// populations land here legitimately.
/// `queued_artifact_recoverable_without_node` documents two of them as
/// accepted: a rollout-skew row that never had an artifact marker, and a
/// sandbox the drain's own recovery path deliberately made markerless via
/// `adopt_remote_snapshot_for_drain_query`. Two more follow from the guard's
/// shape rather than from that list: a sandbox wedged in `deleting`, which
/// the census prune never tombstones and whose bytes really are still on the
/// disk, and a deleted sandbox's row, which stays counted until its
/// `sandboxes` row is hard-deleted and the foreign key cascades.
///
/// **Sampled only on a census the mirror accepted**, not on every census. A
/// declined one does no set-replace and so has no standing count to report,
/// which means a node that starts declining every tick — one chronically
/// degraded sandbox, or a writer rolled back to a build that does not mirror
/// — keeps whatever rows it had and stops reporting them. Its casualties
/// leave this series rather than rising in it. Alert on this beside
/// [`local_backup_queue_reconcile`] with `outcome != applied`; neither is
/// sufficient alone, because the failure that hides casualties here is
/// exactly the one that shows up there.
pub fn local_backup_queue_absent_not_durable(rows: u64) {
    distribution(
        "internal_api.local_backup_queue.absent_not_durable",
        rows as f64,
        &[],
    );
}

/// Rows the census could not mirror because the reported sandbox has no
/// `sandboxes` row. The queue's `sandbox_id` foreign key would reject them, so
/// they are skipped rather than allowed to abort the whole publication.
pub fn local_backup_queue_unknown_sandbox(rows: u64) {
    counter("internal_api.local_backup_queue.unknown_sandbox", rows, &[]);
}

pub fn local_backup_queue_removed(reason: LocalBackupQueueRemoval, rows: u64) {
    counter(
        "internal_api.local_backup_queue.removed",
        rows,
        &[tag("reason", reason)],
    );
}

/// Access preparation separates observing an existing restore owner from
/// entering the existing activation path. No per-sandbox metric labels.
pub fn ingress_preparation_path(observing: bool) {
    counter(
        "internal_api.ingress.preparation_path",
        1,
        &[tag(
            "path",
            if observing {
                "observe_owner"
            } else {
                "access_activation"
            },
        )],
    );
}

metric_vocabulary!(
    /// Which catalog a node's provisioning recipe is drawn from.
    RecipeCatalogScope {
        /// The unretired environments the node's build reports it knows.
        Known => "known",
        /// Every unretired environment: the node reports no known set, so its
        /// recipe is the whole catalog, every architecture included.
        Full => "full",
    }
);

/// How many environments one node's provisioning recipe holds, sampled from
/// each complete inventory the node publishes, whether or not recipes are
/// served. node-ctl refuses a recipe over `MAX_RECIPE_ENVIRONMENTS` (32)
/// whole, and the cluster API then omits it, so past the limit that node
/// installs nothing new until environments are retired. Alert on the fleet
/// maximum approaching the limit, before it's reached.
///
/// A distribution for the reason [`local_backup_queue_depth`] is one: several
/// replicas serve every node, so a gauge would be last-writer-wins.
pub fn runtime_provisioning_recipe_environments(environments: usize, catalog: RecipeCatalogScope) {
    distribution(
        "internal_api.runtime_provisioning.recipe_environments",
        environments as f64,
        &[tag("catalog", catalog)],
    );
}

//! Narrow reads of one sandbox, or of a batch of sandboxes, that keep working
//! when the whole row would not decode or when there is no row at all.
//!
//! A view names its sandbox and reads nothing until an accessor is called.
//! Each accessor is one call to its old reader, preserving that reader's
//! statement and transaction sequence, missing-row rule and strictness; the
//! one exception is `delete_receipts().execution_covers`, which owns the
//! statement the old reader now calls. Most accessors read only the columns
//! or child rows they answer from, so a malformed column elsewhere does not
//! fail them. `restore_scope().reclaim_attempt_is_current` decodes the whole
//! row strictly before it reads the scope, and the teardown `status` and
//! `release_evidence` decode the row's state and artifact pin. These also read
//! the `sandboxes` row: `cleanup_episode().current`,
//! `teardown().retry_admissible`, `quarantine().is_orphan_recovery_state`,
//! `quarantine().claim_is_current` and the typed view's
//! `delete_receipts().pending_matches_observed_execution`.
//!
//! A view is not a snapshot: two accessors are two reads. Callers that compare
//! their answers hold the connection guard, or a transaction, across both.

use pplx_space_common_core::types::{
    ExecutionGeneration, LocalArtifactId, SandboxActivationAttemptId, SandboxCleanupEpisodeId,
    SandboxId, SandboxRuntimeLabel, TransferScopeId,
};
use pplx_space_node_mgr_protocol::execution_fence::ExecutionFence;
use rusqlite::params;

use crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt;
use crate::db::guest_health_scan::StateLabelRead;
use crate::db::inventory_read::InventoryReadError;
use crate::db::local_artifact_leases::NamedArtifactPublication;
use crate::db::route_activation_claims::OpenRouteActivationClaim;
use crate::db::timestamps::EpochSeconds;
use crate::db::{
    ActivationRouteProvenance, ClaimedQuarantineRecoveryCandidate, CleanupCompletion,
    CleanupEpisode, CleanupTarget, Connection, Error, LocalArtifactRow, OptionalExtension as _,
    RestoreTransferScopeRow, Result, TeardownReleaseEvidence, TeardownSuspendOperation,
    TeardownSuspendStatus,
};
use crate::state::SandboxState;

/// A narrow view of the sandbox stored under `id`. Reads nothing until asked.
pub fn sandbox_view<'c>(conn: &'c Connection, id: &'c str) -> SandboxView<'c> {
    SandboxView { conn, id }
}

/// The receipt reads of `sandbox_id`. The delete-receipt outbox is keyed by
/// the typed sandbox id, so only this view reaches them.
pub fn typed_sandbox_view(conn: &Connection, sandbox_id: SandboxId) -> TypedSandboxView<'_> {
    TypedSandboxView { conn, sandbox_id }
}

/// A narrow view of a batch of sandboxes. Reads nothing until asked.
pub fn sandbox_views<'c>(conn: &'c Connection, ids: &'c [String]) -> SandboxViews<'c> {
    SandboxViews { conn, ids }
}

/// A record that may be present but undecodable.
///
/// Presence is a fact in its own right: a row written by a newer binary still
/// claims what it claims, whether or not this one can read it.
#[derive(Debug)]
pub enum Decoded<T> {
    Absent,
    Present(T),
    /// The row exists; decoding it failed with this row-decode error.
    Undecodable(Error),
}

impl<T> Decoded<T> {
    /// Whether a row exists, decodable or not.
    pub fn is_present(&self) -> bool {
        match self {
            Self::Absent => false,
            Self::Present(_) | Self::Undecodable(_) => true,
        }
    }

    /// The record, failing on a row that does not decode.
    pub fn into_strict(self) -> Result<Option<T>> {
        match self {
            Self::Absent => Ok(None),
            Self::Present(value) => Ok(Some(value)),
            Self::Undecodable(error) => Err(error),
        }
    }
}

pub struct SandboxView<'c> {
    conn: &'c Connection,
    id: &'c str,
}

/// What every facet binds: the connection and the view's one id.
#[derive(Clone, Copy)]
struct Key<'v> {
    conn: &'v Connection,
    id: &'v str,
}

impl SandboxView<'_> {
    fn key(&self) -> Key<'_> {
        Key {
            conn: self.conn,
            id: self.id,
        }
    }

    /// The `state` label as stored, `None` without a row. The label is not
    /// parsed, so a state this binary does not know is still returned.
    pub fn current_state_label(&self) -> Result<Option<String>> {
        crate::db::current_state_label(self.conn, self.id)
    }

    /// The route's execution generation, `None` without a row or without a
    /// generation; a stored generation that is not positive fails the read.
    pub fn runtime_execution_generation(&self) -> Result<Option<ExecutionGeneration>> {
        crate::db::runtime_execution_generation(self.conn, self.id)
    }

    /// The node-manager execution fence, including the previous execution.
    ///
    /// Unlike [`crate::db::SandboxRow::node_execution_fence`], which carries
    /// only the current execution, this reads the previous one too. A missing
    /// row is an error; a row with no current execution is `None`, and a row
    /// with a previous execution but no current one is an invalid-query error.
    pub fn node_execution_fence(&self) -> Result<Option<ExecutionFence>> {
        crate::db::node_execution_fence(self.conn, self.id)
    }

    /// What cleanup names the sandbox by: its runtime label, template and
    /// owner. Lenient: only those three columns decode. `None` without a row.
    pub fn cleanup_target(&self) -> Result<Option<CleanupTarget>> {
        crate::db::cleanup_target(self.conn, self.id)
    }

    /// The runtime label alone, `None` without a row.
    pub fn cleanup_runtime_label(&self) -> Result<Option<SandboxRuntimeLabel>> {
        crate::db::cleanup_runtime_label(self.conn, self.id)
    }

    /// Whether `completion` owns the row's cleanup intent. `false` without a
    /// row or with a state label this binary does not know; an intent that
    /// does not decode fails the read.
    pub fn cleanup_intent_current(&self, completion: CleanupCompletion) -> Result<bool> {
        crate::db::cleanup_intent_current(self.conn, self.id, completion)
    }

    /// The current and last-observed route provenance. A missing row is an
    /// error.
    pub fn activation_route_provenance(&self) -> Result<ActivationRouteProvenance> {
        crate::db::activation_route_provenance(self.conn, self.id)
    }

    /// Whether a row exists, whatever it holds.
    pub fn exists(&self) -> Result<bool> {
        crate::db::sandbox_exists(self.conn, self.id)
    }

    /// Whether the row has a live network-policy gateway to reconcile: a row
    /// with `netgtw_pid IS NOT NULL AND pool_template IS NULL`. `false`
    /// without a row.
    pub fn live_network_policy_gateway(&self) -> Result<bool> {
        crate::db::has_live_network_policy_gateway(self.conn, self.id)
    }

    /// Whether the row is `suspended` with `local_artifact_id` pinned.
    ///
    /// One `EXISTS` over two columns. It is only as current as the caller's
    /// guard: a caller that acts on the answer must hold the connection guard
    /// that every state transition serializes on until it has acted.
    pub fn suspended_with_pinned_artifact(&self, local_artifact_id: &str) -> Result<bool> {
        crate::db::sandbox_suspended_with_pinned_artifact(self.conn, self.id, local_artifact_id)
    }

    /// The sandbox's cleanup episode.
    pub fn cleanup_episode(&self) -> CleanupEpisodeFacet<'_> {
        CleanupEpisodeFacet(self.key())
    }

    /// The sandbox's restore transfer scope. Scopes outlive their row.
    pub fn restore_scope(&self) -> RestoreScopeFacet<'_> {
        RestoreScopeFacet(self.key())
    }

    /// The sandbox's execution delete receipt. Its auto-lifecycle delete
    /// receipts are read through [`typed_sandbox_view`].
    pub fn delete_receipts(&self) -> DeleteReceiptsFacet<'_> {
        DeleteReceiptsFacet(self.key())
    }

    /// The sandbox's teardown-suspend jobs and the state they publish into.
    pub fn teardown(&self) -> TeardownFacet<'_> {
        TeardownFacet(self.key())
    }

    /// The sandbox's node-local quarantine state.
    pub fn quarantine(&self) -> QuarantineFacet<'_> {
        QuarantineFacet(self.key())
    }

    /// The sandbox's open Kubernetes route activation claim.
    pub fn route_claim(&self) -> RouteClaimFacet<'_> {
        RouteClaimFacet(self.key())
    }

    /// The artifact the row pins and the evidence for it.
    pub fn pinned_artifact(&self) -> PinnedArtifactFacet<'_> {
        PinnedArtifactFacet(self.key())
    }

    /// The payload `snapshot_id` and `created_at` of the sandbox's newest
    /// snapshot-upload job, whatever its status. Raw: neither is parsed.
    pub fn latest_upload(&self) -> Result<Option<(Option<String>, String)>> {
        crate::db::eviction_scan::latest_upload_job(self.conn, self.id)
    }

    /// The sandbox's `local_artifacts` rows.
    pub fn artifacts(&self) -> ArtifactsFacet<'_> {
        ArtifactsFacet(self.key())
    }
}

#[derive(Clone, Copy)]
pub struct CleanupEpisodeFacet<'v>(Key<'v>);

impl CleanupEpisodeFacet<'_> {
    /// Whether an episode exists. Authoritative even for an episode a newer
    /// binary wrote that this one cannot decode.
    pub fn present(self) -> Result<bool> {
        crate::db::has_cleanup_episode(self.0.conn, self.0.id)
    }

    /// The decoded episode; one that does not decode fails the read.
    pub fn strict(self) -> Result<Option<CleanupEpisode>> {
        crate::db::cleanup_episode(self.0.conn, self.0.id)
    }

    /// The episode, keeping one that does not decode as `Undecodable`.
    /// Statement failures still fail the read.
    pub fn decoded(self) -> Result<Decoded<CleanupEpisode>> {
        match crate::db::cleanup_episode(self.0.conn, self.0.id) {
            Ok(None) => Ok(Decoded::Absent),
            Ok(Some(episode)) => Ok(Decoded::Present(episode)),
            Err(error) if crate::db::is_row_decode_error(error.raw()) => {
                Ok(Decoded::Undecodable(error))
            }
            Err(error) => Err(error),
        }
    }

    /// Whether `cleanup_id` is the requested episode and the row's cleanup
    /// intent still matches it. Joins the row; `false` without one.
    pub fn current(self, cleanup_id: SandboxCleanupEpisodeId) -> Result<bool> {
        crate::db::cleanup_episode_current(self.0.conn, self.0.id, cleanup_id)
    }
}

#[derive(Clone, Copy)]
pub struct RestoreScopeFacet<'v>(Key<'v>);

impl RestoreScopeFacet<'_> {
    /// The scope, with or without a `sandboxes` row; one that does not decode
    /// fails the read.
    pub fn scope(self) -> Result<Option<RestoreTransferScopeRow>> {
        crate::db::restore_transfer_scope(self.0.conn, self.0.id)
    }

    /// Whether a reclaim of the exact scope attempt may still proceed: the
    /// row is a retryable interrupted restore and the scope is that attempt's.
    ///
    /// Strict: the whole row is decoded first, and a row that does not decode
    /// fails the read; then the scope is read. `false` without a row.
    pub fn reclaim_attempt_is_current(
        self,
        transfer_scope_id: TransferScopeId,
        execution_generation: ExecutionGeneration,
        activation_attempt_id: SandboxActivationAttemptId,
    ) -> Result<bool> {
        crate::db::canonical_restore_reclaim_attempt_is_current(
            self.0.conn,
            self.0.id,
            transfer_scope_id,
            execution_generation,
            activation_attempt_id,
        )
    }
}

#[derive(Clone, Copy)]
pub struct DeleteReceiptsFacet<'v>(Key<'v>);

impl DeleteReceiptsFacet<'_> {
    /// Whether a retained execution delete receipt covers `execution_generation`
    /// at `now_epoch_secs`.
    ///
    /// The caller supplies the clock: a receipt covers only while it is inside
    /// its retention window, and the window is measured from the caller's
    /// `now`. Read the clock with [`crate::db::now_epoch_i64`] before calling.
    pub fn execution_covers(
        self,
        execution_generation: ExecutionGeneration,
        now_epoch_secs: i64,
    ) -> Result<bool> {
        let minimum_deleted_at =
            now_epoch_secs.saturating_sub(crate::db::EXECUTION_DELETE_RECEIPT_RETENTION_SECS);
        self.0
            .conn
            .query_row(
                "SELECT execution_generation >= ?1
         FROM sandbox_execution_delete_receipts
         WHERE sandbox_id = ?2
           AND deleted_at >= ?3",
                params![execution_generation.get(), self.0.id, minimum_deleted_at],
                |row| row.get(0),
            )
            .optional()
            .map(|covers| covers.unwrap_or(false))
    }
}

/// A view keyed by the typed sandbox id, for the auto-lifecycle delete
/// receipts the outbox stores under it.
#[derive(Clone, Copy)]
pub struct TypedSandboxView<'c> {
    conn: &'c Connection,
    sandbox_id: SandboxId,
}

impl<'c> TypedSandboxView<'c> {
    /// The sandbox's auto-lifecycle delete receipts.
    pub fn delete_receipts(self) -> PendingReceiptsFacet<'c> {
        PendingReceiptsFacet(self)
    }
}

#[derive(Clone, Copy)]
pub struct PendingReceiptsFacet<'c>(TypedSandboxView<'c>);

impl PendingReceiptsFacet<'_> {
    /// The pending auto-lifecycle delete receipt.
    pub fn pending(self) -> Result<Option<AutoLifecycleDeleteReceipt>> {
        crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
            self.0.conn,
            self.0.sandbox_id,
        )
    }

    /// Whether every pending receipt still describes every execution the row
    /// has observed. Reads the `sandboxes` row; `true` without one, since no
    /// pending receipt then disagrees with it.
    pub fn pending_matches_observed_execution(self) -> Result<bool> {
        crate::db::auto_lifecycle_delete_receipts::pending_receipt_matches_observed_execution(
            self.0.conn,
            self.0.sandbox_id,
        )
    }
}

#[derive(Clone, Copy)]
pub struct TeardownFacet<'v>(Key<'v>);

impl TeardownFacet<'_> {
    /// The newest teardown-suspend job, whatever its status. A job that does
    /// not decode fails the read.
    pub fn latest(self) -> Result<Option<TeardownSuspendOperation>> {
        crate::db::latest_teardown_suspend_operation(self.0.conn, self.0.id)
    }

    /// The row's state and suspend-artifact pin with the newest job. `None`
    /// without a row; a state, pin or job that does not decode fails the read.
    pub fn status(self) -> Result<Option<TeardownSuspendStatus>> {
        crate::db::teardown_suspend_status(self.0.conn, self.0.id)
    }

    /// Whether `execution_generation`'s active teardown-suspend job is
    /// running. More than one active job, or one that does not decode, fails
    /// the read.
    pub fn running_for(self, execution_generation: ExecutionGeneration) -> Result<bool> {
        crate::db::teardown_suspend_operation_running_for(
            self.0.conn,
            self.0.id,
            execution_generation,
        )
    }

    /// The release evidence for `execution_generation`, read in one deferred
    /// transaction.
    pub fn release_evidence(
        self,
        execution_generation: ExecutionGeneration,
    ) -> Result<TeardownReleaseEvidence> {
        crate::db::teardown_release_evidence(self.0.conn, self.0.id, execution_generation)
    }

    /// Whether a retry of `generation`'s teardown suspend is due: no active
    /// job for it, or one whose backoff has elapsed by the database clock.
    pub fn retry_due(self, generation: ExecutionGeneration) -> Result<bool> {
        crate::db::teardown_suspend_retry_due(self.0.conn, self.0.id, generation)
    }

    /// Whether a recovered resident may start a fresh teardown-suspend
    /// attempt for `generation`. Reads the `sandboxes` row and its cleanup
    /// episode; `false` without a row.
    pub fn retry_admissible(self, generation: ExecutionGeneration) -> Result<bool> {
        crate::db::teardown_suspend_retry_admissible(self.0.conn, self.0.id, generation)
    }
}

#[derive(Clone, Copy)]
pub struct QuarantineFacet<'v>(Key<'v>);

impl QuarantineFacet<'_> {
    /// Whether the row is `expected_state` and quarantined by sbx-manager as
    /// an orphan whose row went missing, with no artifact pinned. Joins the
    /// row; `false` without one.
    pub fn is_orphan_recovery_state(self, expected_state: SandboxState) -> Result<bool> {
        crate::db::is_orphan_quarantine_recovery_state(self.0.conn, self.0.id, expected_state)
    }

    /// Whether `claimed` still holds its recovery claim, which names its own
    /// sandbox: open the view on `claimed.candidate.sandbox_id`. Also reads
    /// that sandbox's state and cleanup episode; `false` without a row.
    pub fn claim_is_current(self, claimed: &ClaimedQuarantineRecoveryCandidate) -> Result<bool> {
        crate::db::quarantine_recovery_claim_is_current(self.0.conn, claimed)
    }
}

#[derive(Clone, Copy)]
pub struct RouteClaimFacet<'v>(Key<'v>);

impl RouteClaimFacet<'_> {
    /// Whether the open claim is exactly `claim`.
    pub fn matches(self, claim: &OpenRouteActivationClaim) -> Result<bool> {
        crate::db::route_activation_claims::claim_matches(self.0.conn, self.0.id, claim)
    }

    /// Whether this node rolled back the activation of `claim`, already
    /// matched as the open claim (see
    /// [`crate::db::route_activation_claims::claim_rolled_back`]).
    pub fn rolled_back(self, claim: &OpenRouteActivationClaim) -> Result<bool> {
        crate::db::route_activation_claims::claim_rolled_back(self.0.conn, self.0.id, claim)
    }
}

#[derive(Clone, Copy)]
pub struct PinnedArtifactFacet<'v>(Key<'v>);

impl PinnedArtifactFacet<'_> {
    /// The pinned artifact id, raw, and the row's `updated_at`, `None`
    /// without a row.
    pub fn pin_and_updated_at(self) -> Result<Option<(Option<String>, EpochSeconds)>> {
        crate::db::eviction_scan::artifact_pin_and_updated_at(self.0.conn, self.0.id)
    }

    /// `(artifact_kind, local_artifact_id, backed up)` of the artifact `pin`
    /// names, raw, only if this sandbox owns it.
    pub fn owned_evidence(self, pin: &str) -> Result<Option<(String, String, bool)>> {
        crate::db::eviction_scan::owned_artifact_evidence(self.0.conn, pin, self.0.id)
    }
}

#[derive(Clone, Copy)]
pub struct ArtifactsFacet<'v>(Key<'v>);

impl ArtifactsFacet<'_> {
    /// The named-snapshot artifacts, within the inventory budget.
    pub fn named(self) -> Result<Vec<NamedArtifactPublication>, InventoryReadError> {
        crate::db::local_artifact_leases::list_named_publications(self.0.conn, self.0.id)
    }

    /// The artifacts other than `staged_artifact` without a backup-completed
    /// stamp.
    pub fn others_awaiting_backup(
        self,
        staged_artifact: LocalArtifactId,
    ) -> Result<Vec<LocalArtifactRow>> {
        crate::db::other_local_artifacts_awaiting_backup(self.0.conn, self.0.id, staged_artifact)
    }
}

#[derive(Clone, Copy)]
pub struct SandboxViews<'c> {
    conn: &'c Connection,
    ids: &'c [String],
}

impl<'c> SandboxViews<'c> {
    /// Each sandbox's `state` label as stored, `None` without a row, through
    /// one prepared statement. Only preparing it fails the whole read; each
    /// id's read fails on its own.
    pub fn state_labels(self) -> Result<Vec<(&'c String, StateLabelRead)>> {
        crate::db::guest_health_scan::state_labels_for_ids(self.conn, self.ids)
    }
}

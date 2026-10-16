//! The control plane's mirror of each node's pending local-backup set.
//!
//! A draining node has to answer one question: will destroying this disk lose
//! the only copy of anything? The node holds the answer — in its
//! sandbox-manager SQLite, every sandbox whose current pin has no
//! `local_artifacts` row yet, or one whose `backup_completed_at` is still
//! NULL — and carries it in the Full census as `pending_local_backups`. This
//! module writes that answer into `node_local_backup_pending` so the drain can
//! read a bounded, node-scoped queue instead of deriving the set from
//! `sandboxes` history.
//!
//! ## Written from the first census, read by nothing yet
//!
//! The node-side producer of `pending_local_backups` ships alongside this, so
//! the queue starts filling as soon as both are deployed — this is not a
//! dormant path waiting on a flag. Nothing reads it: no consumer of
//! `node_local_backup_pending` or of
//! `node_status_snapshots.local_backup_inventory_reported_at` exists outside
//! this module and its tests. The node-drain reader is a separate change and
//! is expected to carry its own default-off flag, but that reader and that
//! flag are **not in this tree** — nothing here gates them, and this
//! paragraph is not evidence that a cutover control exists. Writing early and
//! reading late is deliberate: the mirror has to have been correct for a
//! while before a drain is allowed to trust it.
//!
//! ## Why a set-replace and not a truncate-and-insert
//!
//! The failure direction is asymmetric. A queue row that should not be there
//! costs a wasted durability probe. A missing row lets a node terminate while
//! it still holds the only copy of a sandbox. So the census never deletes a
//! row just because the node stopped naming it. Two arms release one, and
//! both ask about durability: the control plane can confirm from `snapshots`
//! that the queued artifact's own bytes are durable, or that the sandbox has
//! left this node *and* the artifact it carries there is durable.
//!
//! A tombstoned sandbox is deliberately not a third arm, even though
//! `lifecycle = 'deleted'` looks like a safe short-circuit.
//! `prune_absent_node_sandboxes` *mints* tombstones from census absence, so
//! a machine replaced under the same kube node name would have its
//! predecessor's rows released by its very first census with no durability
//! test at all. See `queued_artifact_recoverable_without_node` for the full
//! argument and the metric noise accepted in exchange. Rows that are absent
//! and
//! not provably recoverable are **kept** and counted into
//! [`local_backup_queue_absent_not_durable`], because that population is the
//! casualty signal: artifacts the node has stopped accounting for and for
//! which the control plane can find no durable copy.
//!
//! It is a signal rather than a proven loss list. The queue holds one row per
//! `(node, sandbox)` — that sandbox's current lifecycle artifact, whether that
//! is a suspended VM's `full` artifact or a stopped sandbox's `disk` one — so
//! a row can go absent because the pin moved rather than because the bytes
//! did. What absence never is, on its own, is grounds to delete.
//!
//! ## `created_at` is not load-bearing, whatever the migration says
//!
//! The historical migration states that `created_at`
//! means "when *this* artifact entered the queue", that the delete guard
//! treats an artifact as superseded when a newer full snapshot's `timestamp`
//! is at or after it, and that the upsert therefore re-stamps the column on
//! identity change. **None of that is true of this module, deliberately.**
//!
//! That design was written and then withdrawn: `snapshots.timestamp` is
//! `DEFAULT NOW()` and no writer binds it, so it is the row's *insert* time.
//! A deferred or retried backup of an **older** artifact inserts a row stamped
//! later than a **newer** artifact's queue entry, and the comparison would
//! read that as the newer artifact being superseded by older bytes — deleting
//! the queue row for the only copy of the newer state. Supersession is keyed
//! on artifact identity instead; see
//! `pplx_space_sandbox_portability::queued_artifact_recoverable_without_node`.
//!
//! `created_at` is therefore ordinary row-insert provenance: nothing reads it,
//! and the upsert leaves it alone. The migration's paragraph cannot be edited
//! — sqlx checksums the file and it is applied — so this is the correction.
//!
//! ## What the gate certifies
//!
//! `node_status_snapshots.local_backup_inventory_reported_at` is stamped only
//! by a census that reached [`reconcile`], and it is permission to believe an
//! **empty** queue — nothing more. A present row is self-evidencing and needs
//! no gate. It is stamped on every accepted census rather than latched once,
//! so a reader applying a freshness window sees the stamp stop advancing
//! whenever mirroring stops.
//!
//! Stopping is not the only way it ends. A census that *claimed* a complete
//! inventory and then could not mirror **clears** the column outright rather
//! than letting it age: the queue is known un-refreshed at that point, and a
//! recent timestamp over it would certify an emptiness the server did not
//! check. So a node whose censuses start being declined loses the gate on
//! the first one, not after a window; a writer rolled back to a build that
//! does not mirror at all loses it the same way. Only a publication that
//! could not have carried a report — a facet tick — leaves it alone to age.
//! The window itself, and the fallback to the history predicate when the
//! gate is stale or NULL, belong to the reader.
//!
//! Aging is not what covers a **same-name node replacement** — a new machine
//! registering under an existing kube node name, and so reusing `nodes.id`.
//! Its first accepted census re-stamps the gate immediately, so the gate
//! never goes stale to be caught by a window. What covers that case is the
//! set-replace: the successor's report names only its own artifacts, and the
//! predecessor's rows are removed only where the control plane can prove
//! them recoverable. Anything it cannot prove survives into the successor's
//! queue as a casualty, which is the answer a drain needs, and is why the
//! table carries no foreign key on `node_id`.
//!
//! The migration says otherwise — "a writer rollback or a same-name node
//! replacement ... ages out instead of staying trusted". The rollback half
//! is right and the replacement half is not, for the reason above. Historical migrations remain immutable, so as with `created_at` the
//! correction lives here.
//!
//! ## What this module must not touch
//!
//! `nodes`. `persist_pod_node_placement` holds that row's lock through commit,
//! and the contention this whole queue exists to relieve was that lock under a
//! slow drain query. It is why the gate column lives on the
//! `node_status_snapshots` satellite the publish transaction already upserts.
//! `node_local_backup_pending` carries no foreign key on `node_id` — see the
//! migration for why the casualty list must outlive the node row — so nothing
//! this module writes takes a `nodes` row lock, not even through referential
//! integrity.
//!
//! `sandboxes` is a different matter and is *not* claimed to be untouched.
//! `sandbox_id` **is** a foreign key, so inserting a queue row takes
//! `FOR KEY SHARE` on the referenced `sandboxes` row — a lock this
//! transaction did not previously hold for a sandbox the census itself did
//! not carry. It is the weakest row lock Postgres has: it conflicts only with
//! `FOR UPDATE` and with deletion of that row, which is exactly the conflict
//! the foreign key exists to create: a sandbox must not be hard-deleted out
//! from under a row that references it. It does **not** conflict with
//! `FOR NO KEY UPDATE` — that non-conflict is why the "no key" modes exist —
//! so a bare `UPDATE sandboxes SET active_node_id = ...`, which takes that
//! mode because no key column is touched, runs alongside this one.
//!
//! That is a claim about that statement, not about placement writers as a
//! class. One that takes an explicit `FOR UPDATE` on the row before writing
//! does conflict, and waits for this transaction to commit:
//! `persist_owned_restore_route_identity_receipt` and
//! `lock_sandbox_for_execution_mutation_in` both do exactly that. The bound
//! on that wait is this transaction's length, which is the thing the rest of
//! this section is about.
//!
//! That is a claim about this module, not about the whole publication. A
//! node's *first* census inserts its `node_status_snapshots` row, and that
//! table does have a `nodes` foreign key, so the insert takes `FOR KEY SHARE`
//! on the node row once. Every census after it takes the `ON CONFLICT DO
//! UPDATE` arm, where Postgres skips the referential check because the key is
//! unchanged. `tests::the_publish_transaction_takes_no_nodes_row_lock` covers
//! that steady state — the one a draining node is in — by holding the node row
//! `FOR UPDATE` from a second connection and requiring the publish to complete
//! anyway.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::LazyLock;

use pplx_space_common_core::{
    sql,
    types::{
        ArtifactKind, LocalArtifactId, LocalArtifactIdParseError, NodeId, SandboxId,
        SandboxIdParseError,
    },
};
use pplx_space_metrics::internal_api::{
    LocalBackupQueueOutcome, LocalBackupQueueRemoval, local_backup_queue_absent_not_durable,
    local_backup_queue_depth, local_backup_queue_reconcile, local_backup_queue_removed,
    local_backup_queue_unknown_sandbox,
};
use pplx_space_sandbox_portability::{
    artifact_has_durable_copy, queued_artifact_recoverable_without_node,
};
use tonic::Status;

use crate::metadata::artifact_kind_from_proto;
use crate::proto::PendingLocalBackupReport;
use crate::status::SandboxInventoryPrune;
use pplx_space_sbx_mgr_client::proto::PendingLocalBackup;

/// The artifact identity one pending-backup entry carries. The sandbox it
/// belongs to is the map key in [`ParsedPendingLocalBackups`], so it is not
/// repeated here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingLocalBackupEntry {
    artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
}

/// A node's complete answer to "which of my local artifacts have no durable
/// copy yet", parsed from one Full census.
///
/// Construction is the validation: a report that exists is one the mirror may
/// act on, including deleting rows from the queue. Entries are keyed by
/// sandbox, which is the queue's own primary-key prefix — two entries naming
/// one sandbox contradict the one-row-per-`(node, sandbox)` contract and are
/// rejected rather than silently collapsed, because collapsing would pick one
/// of two artifacts arbitrarily and the loser's row would then look absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedPendingLocalBackups {
    entries: BTreeMap<SandboxId, PendingLocalBackupEntry>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PendingLocalBackupParseError {
    #[error("invalid pending-backup sandbox_id {value:?}: {source}")]
    SandboxId {
        value: String,
        #[source]
        source: SandboxIdParseError,
    },
    #[error("invalid pending-backup local_artifact_id {value:?}: {source}")]
    LocalArtifactId {
        value: String,
        #[source]
        source: LocalArtifactIdParseError,
    },
    #[error("pending-backup artifact_kind must be DISK or FULL, got {0}")]
    ArtifactKind(i32),
    #[error("sandbox {0} appears more than once in the pending-backup report")]
    DuplicateSandbox(SandboxId),
}

impl ParsedPendingLocalBackups {
    fn parse(entries: &[PendingLocalBackup]) -> Result<Self, PendingLocalBackupParseError> {
        let mut parsed = BTreeMap::new();
        for entry in entries {
            let sandbox_id = SandboxId::from_str(entry.sandbox_id.trim()).map_err(|source| {
                PendingLocalBackupParseError::SandboxId {
                    value: entry.sandbox_id.trim().to_owned(),
                    source,
                }
            })?;
            let artifact_id =
                LocalArtifactId::from_str(entry.local_artifact_id.trim()).map_err(|source| {
                    PendingLocalBackupParseError::LocalArtifactId {
                        value: entry.local_artifact_id.trim().to_owned(),
                        source,
                    }
                })?;
            let artifact_kind = artifact_kind_from_proto(entry.artifact_kind).ok_or(
                PendingLocalBackupParseError::ArtifactKind(entry.artifact_kind),
            )?;
            if parsed
                .insert(
                    sandbox_id,
                    PendingLocalBackupEntry {
                        artifact_id,
                        artifact_kind,
                    },
                )
                .is_some()
            {
                return Err(PendingLocalBackupParseError::DuplicateSandbox(sandbox_id));
            }
        }
        Ok(Self { entries: parsed })
    }

    fn names(&self, sandbox_id: SandboxId) -> bool {
        self.entries.contains_key(&sandbox_id)
    }

    /// The three parallel arrays the set-replace's `UNNEST` takes, built in one
    /// pass so "these stay index-aligned" is a property of one expression
    /// rather than of three separate walks staying in step.
    fn unnest_columns(&self) -> (Vec<SandboxId>, Vec<LocalArtifactId>, Vec<&'static str>) {
        self.entries
            .iter()
            .map(|(sandbox_id, entry)| {
                (*sandbox_id, entry.artifact_id, entry.artifact_kind.as_str())
            })
            .collect()
    }
}

/// What one publication is allowed to do to the node's mirror.
#[derive(Debug)]
pub(super) enum PendingLocalBackupCensus {
    /// Set-replace the node's rows against this report and advance the gate.
    Report(ParsedPendingLocalBackups),
    /// Leave the rows exactly as the last census left them. The gate is a
    /// separate decision made by the caller: a publication that could not
    /// have mirrored preserves it, one that claimed a complete inventory and
    /// still could not mirror withdraws it. See `publish_node_state`.
    Declined(LocalBackupQueueOutcome),
}

/// Decide whether this publication carries a pending-backup report the mirror
/// may act on.
///
/// Four conditions, all of which must hold, and each of which exists for a
/// different failure:
///
/// 1. The report is **present**. A node image that predates the field, a
///    delta (`Items`) publish, and a tick where sandbox-manager could not
///    answer all send no message at all, and none of them means "nothing is
///    pending". An empty `pending` inside a present report does mean exactly
///    that. The wire type carries the distinction through presence rather
///    than a sibling flag, so this condition is the only way to reach the
///    list and cannot be skipped.
/// 2. The server accepted the sandbox census in the same publication
///    ([`SandboxInventoryPrune::Applied`]). The pending set and the sandbox
///    census come from the same node-side enumeration, so certifying one while
///    rejecting the other would let a census the server refused to prune
///    against still advance the drain's permission to trust an empty queue.
///    This is the server's verdict rather than the wire flag, but the only
///    thing it adds over the node's claim is "every sandbox identity parsed" —
///    the rest of the trust rests on condition 1, and nothing here corroborates
///    a node that says `complete` when it is not. The node is the only party
///    that can see its own disk; the freshness window on the gate, not this
///    condition, is what bounds how long a wrong answer is believed.
/// 3. The report parses, with one entry per sandbox.
/// 4. Every sandbox the control plane knows to be holding a local artifact,
///    and whose observation this publication could take only as lifecycle
///    state, is named in the report. A set-replace reads a missing entry as
///    "no longer pending", so an omission has to be trustworthy before it is
///    acted on.
///
///    This is defense in depth, not a response to a known producer gap. The
///    report cannot today be short by exactly one entry: both
///    `collect_pending_local_backups` (`node/ctl/server/src/facets.rs`) and
///    `list_pending_backups` (`sbx/mgr/server/src/local_artifacts.rs`) are
///    all-or-nothing — every failure, down to a single pin that will not
///    decode, yields no report at all rather than a partial one. What the
///    condition buys is independence from that: the report and the sandbox
///    census are separate collections, so the omission is being trusted on
///    the producer's structure rather than on anything this publication
///    shows, and a sandbox this publication could only take as lifecycle
///    state is one the server has no fresh corroboration for. Declining
///    costs the node its fast path until the next clean census.
///
/// Condition 4 asks the control plane rather than reading the wire status, and
/// that is the point. `sandboxes.current_local_artifact_id IS NOT NULL` *is*
/// "this sandbox has local bytes somewhere", held by the server from the last
/// publication that described the sandbox fully. Every status-keyed spelling
/// of the same question is an enumeration that has to be kept complete by
/// hand, and the set of node-side states that end up publishing a marker is
/// wider than the obvious two — sandbox-manager pins its own artifact while
/// setting `cleanup_required`
/// (`sbx/mgr/store/src/db/local_artifacts.rs`'s `pin_suspended_artifact_for_cleanup`, a
/// SQLite writer), and the teardown-suspend path pins one mid-transition,
/// and each of those eventually reaches this column through
/// `publish_clear_or_preserve_current_local_artifact_marker_assignments`. A
/// hand-kept status list that missed one would accept an incomplete report
/// and certify an empty queue. Reading the marker the server already stores
/// is closed by construction and fails safe for states nobody has thought of
/// yet.
///
/// It is still scoped to sandboxes the report **omits**, because one the node
/// described poorly but did name an artifact for is not at risk — the entry
/// the degradation might have dropped is demonstrably there. Without that
/// half, one chronically degraded sandbox would keep a node's mirror dark.
///
/// The extra read costs a statement only on publications that actually carry
/// a degraded observation, which is not the steady state.
pub(super) async fn decide(
    conn: &mut sqlx::PgConnection,
    pending_local_backups: Option<&PendingLocalBackupReport>,
    inventory_prune: SandboxInventoryPrune,
    state_only_sandbox_ids: &BTreeSet<SandboxId>,
    node_id: NodeId,
) -> Result<PendingLocalBackupCensus, Status> {
    let Some(reported) = pending_local_backups else {
        return Ok(PendingLocalBackupCensus::Declined(
            LocalBackupQueueOutcome::NoReport,
        ));
    };
    if !inventory_prune.stamps_pruned_at() {
        return Ok(PendingLocalBackupCensus::Declined(
            LocalBackupQueueOutcome::CensusRejected,
        ));
    }
    let report = match ParsedPendingLocalBackups::parse(&reported.pending) {
        Ok(report) => report,
        Err(error) => {
            tracing::warn!(
                %node_id,
                %error,
                "declining an unusable pending-backup report"
            );
            return Ok(PendingLocalBackupCensus::Declined(
                LocalBackupQueueOutcome::MalformedReport,
            ));
        }
    };
    if let Some(sandbox_id) =
        undescribed_artifact_owner(conn, state_only_sandbox_ids, &report, node_id).await?
    {
        tracing::warn!(
            %node_id,
            %sandbox_id,
            "declining a pending-backup report that names no artifact for a sandbox the \
             publication could take only as lifecycle state"
        );
        return Ok(PendingLocalBackupCensus::Declined(
            LocalBackupQueueOutcome::UndescribedSandbox,
        ));
    }
    Ok(PendingLocalBackupCensus::Report(report))
}

/// One sandbox, if any, that this publication could take only as lifecycle
/// state, that the report does not name, and that the control plane believes
/// is holding a local artifact.
///
/// `state_only_sandbox_ids` covers both origins of degradation —
/// `publish_sandbox_states` records the id under the node's own
/// `state_degraded` flag and again when an optional field fails to parse. A
/// sandbox whose identity did not parse at all is in neither that set nor the
/// report, but it also forces the census prune to be skipped, so condition 2
/// of [`decide`] already rejects that publication.
///
/// The marker is read from the database rather than taken from this
/// publication precisely because a state-only row is one whose optional fields
/// were dropped: its wire marker may be absent exactly when it matters. The
/// stored one survives, since the publish path preserves it unless the node
/// sends a new artifact or the row becomes eligible to clear it.
///
/// **Scoped to sandboxes this node could actually be holding.** A node can
/// report a degraded observation for a sandbox that has since moved
/// elsewhere; `publish_sandbox_states` scopes its observed-state write to
/// `s.id IS NULL OR COALESCE(direct_route.current_node_id,
/// s.active_node_id) = $1`, so a moved-away row is dropped there — note it
/// keeps a row whose sandbox the control plane has never seen. The id still
/// lands in `state_only` regardless, because that set is built in the Rust
/// loop before any ownership is consulted. Its marker then names the *other*
/// node's artifact, which this node is right not to describe — so without
/// this conjunct one stale foreign row would decline every report and, since
/// a declined census now withdraws the gate, hold the node on the history
/// predicate indefinitely.
///
/// This guard consults the same two ownership signals publish does — a
/// direct-route sandbox's `current_node_id` and `active_node_id` — plus
/// `target_node_id`, but unions them instead of preferring one, so a
/// disagreement keeps the sandbox in scope rather than resolving it away.
/// `pplx_space_sandbox_store::any_undescribed_artifact_owner`, which owns
/// the statement, records why the two paths differ.
///
/// The exclusion is "demonstrably attached elsewhere", not "not attached
/// here": a sandbox attached to no node at all is still included, because
/// nothing rules out its artifact sitting on this disk and the guard's job
/// is to be conservative about exactly that.
///
/// **Also scoped to markers that could still be pending.** A marker whose
/// artifact already has a durable copy is one `list_pending_backups` is
/// right to omit — its `backup_completed_at` is set — so a report that does
/// not name it has no gap. Without this the guard reads any long-since
/// backed-up marker as undescribed, and because a declined census now
/// withdraws the gate, a single chronically degraded sandbox would hold its
/// node on the history predicate forever. That is the same failure
/// [`decide`] avoids by asking about artifact evidence rather than about
/// degradation, applied one level down to the evidence itself.
async fn undescribed_artifact_owner(
    conn: &mut sqlx::PgConnection,
    state_only_sandbox_ids: &BTreeSet<SandboxId>,
    report: &ParsedPendingLocalBackups,
    node_id: NodeId,
) -> Result<Option<SandboxId>, Status> {
    let omitted = state_only_sandbox_ids
        .iter()
        .copied()
        .filter(|sandbox_id| !report.names(*sandbox_id))
        .collect::<Vec<_>>();
    pplx_space_sandbox_store::any_undescribed_artifact_owner(conn, node_id, &omitted)
        .await
        .map_err(|err| {
            Status::internal(format!("look up undescribed pending-backup owners: {err}"))
        })
}

/// Counts from one set-replace, emitted after the publish transaction commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingLocalBackupReconcile {
    /// Reported entries with no `sandboxes` row, which the queue's foreign key
    /// would reject.
    ///
    /// Dropping them does not make the mirror unsafe to certify, which is why
    /// a census that skips some still stamps the gate. The migration settles
    /// it: `sandbox_id` cascades precisely because "a deleted sandbox has
    /// nothing left to rescue, so its outstanding backup work is moot". Had
    /// such a row been mirrored a moment before the `sandboxes` row went
    /// away, the cascade would have removed it without consulting durability
    /// either. This filter is that same policy applied at insert time rather
    /// than a gap in it, and it holds whichever delete path removed the row.
    /// The other way to reach it — an id the control plane has never held —
    /// lands in the same place from the other side: there is no sandbox for
    /// the artifact to be restored into, so nothing is given up by not
    /// tracking it. The count is drift telemetry either way: the node is
    /// reporting work for a sandbox the control plane does not have.
    pub(super) unknown_sandboxes: u64,
    /// Rows removed because the node stopped reporting them and the control
    /// plane confirmed a durable copy.
    pub(super) deleted_durable: u64,
    /// Rows the node stopped reporting that could **not** be confirmed
    /// durable, and were therefore kept. The casualty signal.
    pub(super) retained_absent: u64,
    /// The node's queue depth once the replace finished.
    pub(super) depth: u64,
}

impl PendingLocalBackupReconcile {
    pub(super) fn emit_metrics(self) {
        local_backup_queue_reconcile(LocalBackupQueueOutcome::Applied);
        local_backup_queue_depth(self.depth);
        if self.unknown_sandboxes > 0 {
            local_backup_queue_unknown_sandbox(self.unknown_sandboxes);
        }
        if self.deleted_durable > 0 {
            local_backup_queue_removed(
                LocalBackupQueueRemoval::CensusDurable,
                self.deleted_durable,
            );
        }
        // Emitted on every census, zero included, unlike the counters above:
        // it is a standing population, so a series that only appears when it is
        // non-zero cannot be read as a maximum.
        local_backup_queue_absent_not_durable(self.retained_absent);
    }
}

/// Replace one node's queue rows with the set it just reported.
///
/// Runs inside the caller's publish transaction, so these rows and the gate
/// `upsert_node_status_from_publish` stamps commit together or not at all.
pub(super) async fn reconcile(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    report: &ParsedPendingLocalBackups,
) -> Result<PendingLocalBackupReconcile, Status> {
    let (reported_sandbox_ids, reported_artifact_ids, reported_artifact_kinds) =
        report.unnest_columns();

    // On conflict this *replaces* the tracked artifact without asking whether
    // the one it displaces was durable — the only write here that removes
    // tracking without the delete guard's proof. Its warrant is narrower than
    // the guard's and does not depend on it: the queue holds one row per
    // `(node, sandbox)`, that row is the sandbox's current lifecycle artifact
    // *on this node*, and the replacement comes from this node's own report.
    // The node is the authority on its own disk, so a report naming a
    // different artifact for the same sandbox is first-hand evidence that the
    // pin moved there — not an inference from control-plane state that may be
    // stale.
    //
    // That distinction matters because the delete guard deliberately does not
    // trust the marker alone for the same conclusion: a marker can lag behind
    // the artifact a node is reporting, which is why the guard's second
    // disjunct additionally requires the sandbox to have left this node. No
    // such lag is possible here, because the evidence *is* the node's report.
    //
    // The `sandboxes` existence test is not an optimization: `sandbox_id` is a
    // foreign key, so an entry naming a sandbox the control plane has never
    // heard of would abort the whole publication rather than skip one row. It
    // narrows the window rather than closing it — the row can still be deleted
    // between this `EXISTS` and the foreign-key check, which aborts the
    // publication and is recovered by the next census.
    //
    // It deliberately does **not** also exclude tombstoned sandboxes.
    // `prune_absent_node_sandboxes` can mark a space-managed sandbox
    // `deleted` earlier in this same transaction while the node still reports
    // a pending artifact for it, and skipping those inserts looks tidier: it
    // avoids a row the delete guard would release once the node stops
    // reporting it. The cost is worse than the tidiness. `unknown_sandboxes`
    // is derived as `reported - upserted`, so every skipped tombstone would
    // read as a sandbox the control plane has never heard of and raise an
    // inventory-drift counter every minute; and because the row is still
    // *reported*, the census-absent delete never considers it, so a row
    // already present would keep a stale `artifact_id` for as long as the
    // tombstone survives retention. Letting the upsert run keeps the identity
    // current and the counter honest, at the price of the row sitting in
    // `depth` — a stock, where a constant offset is readable — until the node
    // stops reporting it.
    //
    // Skipped entirely when the node reports nothing pending, which once the
    // node side lands is the common case fleet-wide: `UNNEST` over three empty
    // arrays produces no rows, so the statement's only effect would be a round
    // trip inside a transaction whose length is the thing this work exists to
    // contain.
    let upserted = if reported_sandbox_ids.is_empty() {
        0
    } else {
        sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending (node_id, sandbox_id, artifact_id, artifact_kind)
         SELECT $1, reported.sandbox_id, reported.artifact_id, reported.artifact_kind
           FROM UNNEST($2::UUID[], $3::UUID[], $4::TEXT[])
                AS reported(sandbox_id, artifact_id, artifact_kind)
          WHERE EXISTS (
                    SELECT 1 FROM sandboxes WHERE sandboxes.id = reported.sandbox_id
                )
         ON CONFLICT (node_id, sandbox_id) DO UPDATE
            SET artifact_id = EXCLUDED.artifact_id,
                artifact_kind = EXCLUDED.artifact_kind"
    ))
        .bind(node_id.as_uuid())
        .bind(&reported_sandbox_ids)
        .bind(&reported_artifact_ids)
        .bind(&reported_artifact_kinds)
        .execute(&mut *conn)
        .await
        .map_err(|err| Status::internal(format!("mirror reported pending local backups: {err}")))?
        .rows_affected()
    };

    let deleted_durable = delete_durable_absent(conn, node_id, &reported_sandbox_ids).await?;

    // Counted after the delete, so what remains absent is exactly the
    // population that could not be confirmed durable.
    let (depth, retained_absent): (i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*),
                COUNT(*) FILTER (WHERE NOT (sandbox_id = ANY($2)))
           FROM node_local_backup_pending
          WHERE node_id = $1"
    ))
    .bind(node_id.as_uuid())
    .bind(&reported_sandbox_ids)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("count mirrored pending local backups: {err}")))?;

    // Every reported entry whose sandbox exists is affected by the upsert,
    // including one whose stored values did not change, so the shortfall is
    // exactly the entries the `EXISTS` filtered out.
    let reported = report.entries.len() as u64;
    Ok(PendingLocalBackupReconcile {
        unknown_sandboxes: reported.saturating_sub(upserted),
        deleted_durable,
        retained_absent: retained_absent.max(0).cast_unsigned(),
        depth: depth.max(0).cast_unsigned(),
    })
}

/// Built once: every input is a literal, so the composed text is identical
/// on every census, and this runs for each node each minute. Same shape as
/// `sandbox_quarantine_store`'s recovery queries, and the `line!()` the
/// `sql!` locator captures is fixed either way.
///
/// At module scope rather than inside [`delete_durable_absent`] so
/// `tests::the_delete_guard_resolves_through_indexes_not_a_history_scan` can
/// EXPLAIN the statement this function executes instead of a copy of it: a
/// copy pins the shared predicate but not the `USING`/`WHERE` scaffolding
/// around it.
static DELETE_DURABLE_ABSENT: LazyLock<String> = LazyLock::new(|| {
    let recoverable = queued_artifact_recoverable_without_node(
        "sandbox",
        "queued.artifact_id",
        "queued.artifact_kind",
        "queued.node_id",
    );
    sql!(
        format!(
            "DELETE FROM node_local_backup_pending queued
              USING sandboxes sandbox
              WHERE queued.node_id = $1
                AND sandbox.id = queued.sandbox_id
                AND NOT (queued.sandbox_id = ANY($2))
                AND {recoverable}"
        )
        .as_str()
    )
});

/// Remove the rows this node no longer reports **and** which the control plane
/// can prove are recoverable without that node's disk.
///
/// The proof is [`queued_artifact_recoverable_without_node`], which lives in
/// `pplx-space-sandbox-portability` rather than here because the drain that
/// will read this queue has to agree with the census that wrote it about what
/// "recoverable" means, and a second spelling is how the two drift. Its two
/// durability arms are identity-tied and neither is timestamped: whether the
/// queue row's own artifact has a durable copy, and whether the sandbox's
/// *current* artifact does once it has left this node. There is deliberately
/// no tombstone short-circuit: the census prune mints `deleted` from
/// absence, so one would erase a replaced node's casualty rows.
///
/// Everything else stays. A row that is absent and not provably recoverable is
/// the closest thing the control plane has to "what would this node's disk take
/// with it", so it is kept and alerted rather than deleted on the node's
/// silence alone.
async fn delete_durable_absent(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    reported_sandbox_ids: &[SandboxId],
) -> Result<u64, Status> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(DELETE_DURABLE_ABSENT.as_str()))
            .bind(node_id.as_uuid())
            .bind(reported_sandbox_ids)
            .execute(conn)
            .await
            .map_err(|err| {
                Status::internal(format!("prune durable absent pending local backups: {err}"))
            })?
            .rows_affected(),
    )
}

/// The artifact identity a finalize just made durable.
///
/// All four parts are required, which is why the `snapshots` row that produces
/// it — `FinalizedSnapshotRow`, beside the statement that selects it — can
/// only offer an `Option`: a delete that dropped `source_node_id` or
/// `artifact_id` from its `WHERE` would key on `(node, sandbox)` alone, and
/// that is exactly what lets finalizing an *old* artifact remove a *newer*
/// artifact's row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FinalizedArtifact {
    pub(super) source_node_id: NodeId,
    pub(super) sandbox_id: SandboxId,
    pub(super) artifact_id: LocalArtifactId,
    pub(super) artifact_kind: ArtifactKind,
}

/// Delete the mirrored queue row for exactly the artifact a finalize made
/// durable.
///
/// The fast path, not the only exit: the census's set-replace heals a row
/// whose finalize raced its insert, or whose backup completed before the queue
/// existed. Returns whether a row was removed, so the caller can keep the
/// metric off the overwhelmingly common no-op.
///
/// It carries the census guard's durability test rather than trusting the
/// finalize alone, because a `ready` state is not by itself proof that a live
/// copy exists: the snapshot's only storage location can already have been
/// orphaned when the finalize flips the row. Requiring
/// [`artifact_has_durable_copy`] makes the two exits agree on what "durable"
/// means, which is why that proof lives in `pplx-space-sandbox-portability`
/// rather than being spelled out twice.
///
/// For the common class there is no census backstop, which is what makes the
/// conjunct load-bearing rather than defensive. When the artifact has a
/// `local_artifacts` row, the node stamps its `backup_completed_at`
/// immediately after `finalize_artifact_backup` returns `ready`, and
/// `list_pending_backups` filters on that column being NULL, so by the next
/// census the node has stopped reporting the artifact and a row deleted here
/// is not restored.
///
/// One class does get re-reported: `list_pending_backups` also selects pins
/// with **no** `local_artifacts` row at all (`a.local_artifact_id IS NULL`),
/// covering both a missing row and one owned by another sandbox. There is no
/// `backup_completed_at` to stamp, so those keep being reported and the
/// census re-inserts them. That class is rare — every publish path writes the
/// artifact row and the pin in one transaction, and the snapshot GC excludes
/// pinned artifacts — so it is not a backstop to rely on, and the conjunct is
/// still required for the ordinary class above.
///
/// A restored artifact is **not** in that class, despite having no completed
/// backup: `publish_restored_suspended_artifact` inserts its row, so it is
/// reported through the `backup_completed_at IS NULL` arm and stamped as soon
/// as a backup of it finalizes.
pub(super) async fn clear_finalized(
    conn: &mut sqlx::PgConnection,
    finalized: FinalizedArtifact,
) -> Result<bool, Status> {
    /// Built once: every input is a bind placeholder, so the composed text is
    /// identical on every finalize. Same shape as `DELETE_DURABLE_ABSENT`.
    static DELETE_FINALIZED: LazyLock<String> = LazyLock::new(|| {
        let durable = artifact_has_durable_copy("$2", "$3", "$4");
        sql!(
            format!(
                "DELETE FROM node_local_backup_pending
                  WHERE node_id = $1
                    AND sandbox_id = $2
                    AND artifact_id = $3
                    AND artifact_kind = $4
                    AND {durable}"
            )
            .as_str()
        )
    });
    let removed = sqlx::query(sqlx::AssertSqlSafe(DELETE_FINALIZED.as_str()))
        .bind(finalized.source_node_id.as_uuid())
        .bind(finalized.sandbox_id.as_uuid())
        .bind(finalized.artifact_id.as_uuid())
        .bind(finalized.artifact_kind.as_str())
        .execute(conn)
        .await
        .map_err(|err| Status::internal(format!("clear finalized pending local backup: {err}")))?
        .rows_affected()
        > 0;
    Ok(removed)
}

#[cfg(test)]
mod tests;

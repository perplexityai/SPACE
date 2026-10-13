//! Shared SQL fragments for the sandbox "is this snapshot portable?" decision.
//!
//! A SPACE sandbox with `current_local_artifact_id` is **portable** when a
//! `state='ready'` full backup for that exact artifact has been registered in
//! `snapshot_storage_locations`. Nullable rollout-skew rows use the legacy
//! timestamp/freshness rules below. A portable suspended sandbox can be
//! restored on any healthy node; the source node is then free to drain.
//!
//! The same freshness fragment shows up in two decision-points with subtly
//! different lifecycle scopes:
//!
//! * **Drain-completion** — admin `:terminate` guard, drain reconciler
//!   blocker count, drain reconciler mark-failed atomic re-check. Only
//!   `lifecycle = 'suspended'` rows can be portable here: `suspending` /
//!   `restoring` rows are mid-flight and don't yet have a snapshot to
//!   promote, so they always block drain.
//! * **Restore fallback selection** — the API drain gate's portable-bypass and
//!   the initial Kubernetes user-restore fallback selection. The scope widens
//!   to `lifecycle IN ('suspended', 'restoring')` because
//!   `begin_restore_lifecycle` flips the row from `suspended` to `restoring`
//!   *before* pod creation, and the snapshot is still canonical mid-restore.
//!   Retry selection after that claim uses the pre-restore source window so a
//!   failed restore placement bump does not hide the source snapshot, and a
//!   later failed-branch artifact is not mistaken for the source. For
//!   `started` / `paused` the live VM memory on the source node is the
//!   canonical copy; a ready snapshot from a prior suspend cycle is stale.
//!
//! Centralising the artifact/freshness fragment here keeps the admin counter, the
//! drain gate, the restore-affinity short-circuits, and the drained-node
//! auto-terminator from drifting against each other: each callsite is one
//! "is this sandbox portable?" question, and the answer must be identical
//! everywhere it's asked.
//!
//! ## A third question: is *this row* the pinned artifact?
//!
//! [`snapshot_is_current_local_artifact`] answers the per-snapshot inverse of
//! the marker branch above, and its consumers are **deletion** paths rather
//! than portability decisions — the snapshot-retention sweeper uses it to
//! refuse to prune a sandbox's tracked restore target. It lives here for the
//! same anti-drift reason: the sweep's "never delete this" and the restore
//! selector's "this is the one to load" must be the same predicate, or the
//! sweep can delete exactly what restore would have selected. **Five** callers
//! build from it today — five *production* call sites, plus a test that builds
//! it directly. Enumerated rather than counted, because an earlier revision of
//! this paragraph undercounted them:
//!
//! 1. [`matching_ready_backup_exists`], in this file.
//! 2. [`snapshot_is_live_restore_source`], in this file — the deletion guard.
//! 3. The snapshot-retention store's marked-restore-breach reporting query,
//!    which composes the fragment directly rather than through (2).
//! 4. The Kubernetes substrate's marked suspend restore-source selector.
//! 5. Its disk-only sibling, for an evacuated `stopped` sandbox. That one
//!    exists because (4) filters `sn.type = 'full'`; both resolve their
//!    source through this fragment, so one guard covers both.
//!
//! Narrowing the fragment therefore touches a *reporting* query and a
//! portability probe, not just the two selectors and the guard.
//!
//! It is not yet the *only* spelling of that pair. Six other fragments in this
//! file still inline `sn.local_artifact_id = {alias}.current_local_artifact_id
//! AND sn.artifact_kind = {alias}.current_local_artifact_kind` directly. Each
//! is equivalent today — they all sit inside an `EXISTS`, where the
//! fragment's explicit `FALSE` and a bare comparison's `NULL` filter the row
//! the same way — so this is a consolidation debt, not a live divergence.
//! Switching them is a drain/portability plan change and is deliberately not
//! bundled with a retention-sweeper flip.
//!
//! If you change portability semantics in this file, a deletion path depends
//! on you.
//!
//! ## NULL-safety of the freshness clause
//!
//! `sn.timestamp >= s.placement_changed_at` is wrapped in
//! `s.placement_changed_at IS NULL OR ...` so rows that predate the
//! `placement_changed_at` column (or have never re-placed since creation)
//! still treat any `state='ready'` snapshot as fresh. A plain comparison
//! returns NULL when the right-hand side is NULL, which would silently flip
//! such rows to "local-only" and block drain on legacy data.
//!
//! ## What `snapshots.timestamp` actually is
//!
//! `snapshots.timestamp` is the row's immutable **creation** time: set to
//! `NOW()` when `BeginArtifactBackup` inserts the row in `state='creating'`
//! and never updated thereafter (the terminal `FinalizeArtifactBackup`
//! transitions `state` and `size_bytes` but deliberately leaves `timestamp`
//! alone). Every suspend attempt allocates a fresh row id and S3 prefix, so
//! a late-finalizing stale upload produces its own row with its own
//! creation timestamp — earlier than the wake that bumped
//! `placement_changed_at`, which classifies it correctly as stale. The
//! `wake → modify → re-suspend` race and the residual late-register race
//! that earlier versions of this module documented are both closed by the
//! immutable-row design.

use pplx_space_common_core::types::{ArtifactOrigin, SandboxLifecycle};

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the sandbox row aliased as `sandbox_alias` has a `state='ready'` full
/// snapshot of its **current** artifact registered in
/// `snapshot_storage_locations`.
///
/// Freshness here is the marker, not a timestamp: the snapshot has to carry
/// the artifact identity the sandbox currently points at. Whatever covers
/// markerless rows is supplied by the legacy arm each composer pairs this
/// with, and the three composers differ —
/// [`fresh_ready_snapshot_exists`] compares against `placement_changed_at`,
/// [`latest_full_snapshot_ready`] uses [`no_newer_full_snapshot`] and no
/// timestamp at all, and [`current_suspend_snapshot_ready`] compares against
/// `lifecycle_changed_at`. This fragment makes no claim about which.
///
/// `sandbox_alias` is the SQL alias for the `sandboxes` row in the calling
/// query — pass the bare table name (`"sandboxes"`) when no alias is set.
/// The fragment is safe to compose with `NOT`, `AND`, `OR`, etc.
fn matching_ready_backup_exists(sandbox_alias: &str) -> String {
    let pinned = snapshot_is_current_local_artifact(sandbox_alias, "sn");
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND {pinned}\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
         )"
    )
}

/// Composes the exact-artifact-identity branch (used once a row carries an
/// immutable `current_local_artifact_id`) with the nullable rollout-skew
/// `legacy` branch, keyed on whether the row has an artifact identity yet.
/// `exact` and `legacy` are each self-contained boolean SQL fragments.
/// ## Only safe for a caller that reads this positively
///
/// The two branches become sibling `EXISTS` expressions, so a caller that
/// *negates* the result gets `NOT (A OR B)`, which Postgres cannot flatten into
/// an anti-join — each side becomes a `SubPlan`, and a branch whose correlation
/// includes an inequality (the placement-freshness comparison, in every
/// snapshot predicate here) cannot be hashed either, so it re-executes once per
/// outer row, multiplying the work by the number of pinned sandboxes.
///
/// Every current caller of this helper reads it positively, which is why they
/// still use it. A caller that needs to negate one must nest both branches
/// inside a single correlated `EXISTS` instead — see
/// [`inflight_suspend_upload_exists`], which was moved to that shape for
/// exactly this reason.
fn artifact_identity_or_legacy(sandbox_alias: &str, exact: &str, legacy: &str) -> String {
    format!(
        "(\n\
             ({sandbox_alias}.current_local_artifact_id IS NOT NULL AND {exact})\n\
             OR ({sandbox_alias}.current_local_artifact_id IS NULL AND {legacy})\n\
         )"
    )
}

pub fn fresh_ready_snapshot_exists(sandbox_alias: &str) -> String {
    let exact = matching_ready_backup_exists(sandbox_alias);
    let legacy = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND (\n\
                    {sandbox_alias}.placement_changed_at IS NULL\n\
                    OR sn.timestamp >= {sandbox_alias}.placement_changed_at\n\
                )\n\
         )"
    );
    artifact_identity_or_legacy(sandbox_alias, &exact, &legacy)
}

/// Returns a SQL fragment evaluating to TRUE iff the snapshot aliased as
/// `snapshot_alias` **is** the sandbox's tracked current local artifact — the
/// exact artifact identity a marked row's restore resolves its source by.
///
/// This is the per-snapshot counterpart of the per-sandbox marker branch in
/// [`fresh_ready_snapshot_exists`]: that one asks "does this sandbox still
/// have its pinned artifact?", this one asks "is *this row* the pinned
/// artifact?". Retention and GC paths need the latter, so they can exclude a
/// live restore target from deletion. Both match on `(local_artifact_id,
/// artifact_kind)`, so a sweep and the restore selector cannot drift on what
/// "the current artifact" means.
///
/// Compose it into a query that already joins both a `sandboxes` row and a
/// `snapshots` row.
///
/// ## NULL-safety
///
/// The fragment evaluates to a non-NULL boolean, which matters because callers
/// negate it (`AND NOT (...)`): a NULL there filters the row out instead of
/// keeping it — the exact opposite of the intended "not pinned, therefore
/// eligible". Both `IS NOT NULL` guards are load-bearing. A markerless sandbox
/// short-circuits to FALSE instead of yielding `uuid = NULL` → NULL, and so
/// does a legacy snapshot carrying no artifact identity. Given those guards the
/// remaining comparisons cannot be NULL, because the schema's
/// `sandboxes_current_local_artifact_identity_check` and
/// `snapshots_artifact_identity_check` make each `kind` non-NULL exactly when
/// its `id` is.
pub fn snapshot_is_current_local_artifact(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let matches_marker = snapshot_matches_artifact(
        snapshot_alias,
        &format!("{sandbox_alias}.current_local_artifact_id"),
        &format!("{sandbox_alias}.current_local_artifact_kind"),
    );
    format!(
        "(\n\
             {sandbox_alias}.current_local_artifact_id IS NOT NULL\n\
             AND {snapshot_alias}.local_artifact_id IS NOT NULL\n\
             AND {matches_marker}\n\
         )"
    )
}

/// The artifact-identity half of the expression-keyed fragments below: does
/// the snapshot aliased as `snapshot_alias` carry exactly this artifact?
///
/// The alias-keyed spelling of the same pair is
/// [`snapshot_is_current_local_artifact`], which reads the identity off a
/// `sandboxes` row's marker. These callers hold an artifact of their own and
/// must not consult the marker, because it may already name a later one.
fn snapshot_matches_artifact(
    snapshot_alias: &str,
    artifact_id: &str,
    artifact_kind: &str,
) -> String {
    format!(
        "(\n\
             {snapshot_alias}.local_artifact_id = {artifact_id}\n\
             AND {snapshot_alias}.artifact_kind = {artifact_kind}\n\
         )"
    )
}

/// Returns an `EXISTS(...)` SQL fragment evaluating to TRUE iff the exact
/// local artifact named by the three supplied expressions already has a
/// durable copy: a `state='ready'` snapshot of that artifact identity with a
/// storage location that has not been orphaned.
///
/// Keyed on an **artifact** rather than on a `sandboxes` row: like
/// [`deleting_snapshot_matches_artifact`] and
/// [`queued_artifact_recoverable_without_node`] below, and unlike the
/// alias-keyed fragments elsewhere in this file, the arguments are arbitrary
/// SQL expressions — bind placeholders, or columns of whatever the caller has
/// joined — not a table alias.
///
/// This answers only "are *these bytes* off the disk". It is deliberately
/// narrower than [`queued_artifact_recoverable_without_node`], which is the
/// question a caller deciding whether to stop tracking an artifact has to ask.
pub fn artifact_has_durable_copy(
    sandbox_id: &str,
    artifact_id: &str,
    artifact_kind: &str,
) -> String {
    let matches_artifact = snapshot_matches_artifact("ready_snapshot", artifact_id, artifact_kind);
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots ready_snapshot\n\
               JOIN snapshot_storage_locations ready_storage\n\
                 ON ready_storage.snapshot_id = ready_snapshot.id\n\
                AND ready_storage.orphaned_at IS NULL\n\
              WHERE ready_snapshot.sandbox_id = {sandbox_id}\n\
                AND ready_snapshot.state = 'ready'\n\
                AND {matches_artifact}\n\
         )"
    )
}

/// Returns a boolean SQL fragment evaluating to TRUE iff the sandbox aliased
/// as `sandbox_alias` is still attached to the node named by `node_id`.
///
/// Attachment is `active_node_id` **or** `target_node_id`, which is what the
/// node drain already means by "attached to this node"
/// (`node_drain/store.rs:1197`'s `(active_node_id = $1 OR target_node_id = $1)`
/// is the executable one; `node_drain/mod.rs:876` restates it in prose).
/// That spells it with `=`; this
/// uses `IS NOT DISTINCT FROM`, which agrees for every non-NULL `node_id`
/// and additionally gives a total answer rather than NULL, so a caller may
/// safely negate it.
///
/// **Only safe as a filter on an already-joined row.** `orphaned_nodes`
/// deliberately does *not* use this shape: its comment
/// (`orphaned_nodes/store.rs`) explains that driving a scan from
/// `(active_node_id = n.id OR target_node_id = n.id)` can make the planner
/// BitmapOr both partial indexes and materialize every sandbox pinned to the
/// node, so it splits the question into two `NOT EXISTS` probes. Use this
/// fragment where the `sandboxes` row is already in hand, as `delete_durable_absent` does by
/// joining on the queue row's `sandbox_id`; do not make it the access path.
///
/// Named here so a caller can write `NOT {attached}` instead of restating
/// the pair — the definition of "attached" is exactly the kind that drifts
/// when it is spelled out per call site. Callers today are
/// [`queued_artifact_recoverable_without_node`] in this file and
/// `pplx-space-sandbox-store`'s `any_undescribed_artifact_owner`, which wraps
/// it in an extra direct-route disjunct. Grep before changing the semantics:
/// this list has already gone stale once.
///
/// **A NULL is not an answer.** Both columns NULL means detached from every
/// node, which is neither attached here nor attached elsewhere, so this
/// returns FALSE and a caller that wants "demonstrably somewhere else" must
/// also require [`sandbox_attached_anywhere`]. That holds for a **non-NULL**
/// `node_id`; pass a nullable expression and `NULL IS NOT DISTINCT FROM NULL`
/// makes a detached sandbox read as attached. Callers bind a node id, so this
/// is a constraint on the argument rather than a live case. `suspended_execution_reaper`
/// unplaces with `target_node_id = COALESCE(target_node_id, active_node_id)`
/// and keeps the node in `target_node_id`, so a reaped sandbox is still
/// attached; `clear_pod_tracking_in` NULLs both, so an orphan-recovered one
/// is not.
pub fn sandbox_attached_to_node(sandbox_alias: &str, node_id: &str) -> String {
    format!(
        "(\n\
             {sandbox_alias}.active_node_id IS NOT DISTINCT FROM {node_id}\n\
             OR {sandbox_alias}.target_node_id IS NOT DISTINCT FROM {node_id}\n\
         )"
    )
}

/// Returns a boolean SQL fragment evaluating to TRUE iff the sandbox aliased
/// as `sandbox_alias` is attached to *some* node.
///
/// The companion [`sandbox_attached_to_node`] needs: "not attached here" and
/// "attached somewhere else" differ exactly on the detached sandbox, and for
/// a caller deciding whether state on one node is still reachable the
/// difference is the whole question.
pub fn sandbox_attached_anywhere(sandbox_alias: &str) -> String {
    format!(
        "(\n\
             {sandbox_alias}.active_node_id IS NOT NULL\n\
             OR {sandbox_alias}.target_node_id IS NOT NULL\n\
         )"
    )
}

/// Returns a boolean SQL fragment evaluating to TRUE iff a queued local
/// artifact can be given up without losing state — the whole question a
/// per-node pending-backup queue's writer and its reader both have to answer,
/// kept in one place so they cannot answer it differently.
///
/// Two arms, named rather than numbered — an arm was once added at the front
/// and every ordinal reference elsewhere silently became off-by-one.
/// **Neither involves a timestamp**:
///
/// A tombstoned sandbox is **not** an arm, deliberately. `lifecycle =
/// 'deleted'` looks like a safe short-circuit — nothing will restore it, so
/// its bytes are not state anyone can lose — and releasing on it does clear
/// the noise where an ordinary space-managed delete otherwise leaves a row
/// counted into [`local_backup_queue_absent_not_durable`] until tombstone
/// retention hard-deletes the `sandboxes` row and the foreign key cascades.
///
/// It is still wrong, because `prune_absent_node_sandboxes` *mints*
/// tombstones from census absence: it marks `deleted` every space-managed
/// sandbox with `active_node_id = <this node>` that the census omits, in the
/// same transaction and before this predicate runs. A machine replaced under
/// the same kube node name reuses `nodes.id`, and its first census omits
/// every predecessor sandbox — so the arm would release exactly the rows
/// recording what the predecessor's disk took with it, with no durability
/// test at all, and the successor's gate would then certify the empty queue.
/// The one moment the casualty list exists for is the one it would be erased
/// in.
///
/// Distinguishing a user-initiated delete from an absence-inferred tombstone
/// needs a signal the schema does not carry, so the metric noise is accepted
/// instead: a deleted sandbox's row stays counted until its `sandboxes` row
/// is hard-deleted and the cascade takes it. Wrong in the direction that
/// costs a probe rather than a record.
///
/// * **Own-bytes arm** — this artifact's bytes are durable:
///   [`artifact_has_durable_copy`] on
///   the queued `(artifact_id, artifact_kind)`, not on the sandbox's marker,
///   which may already name a later artifact. The ordinary exit.
/// * **Departed-marker arm** — the sandbox left this node and its current
///   artifact is durable: the
///   same fragment again, applied to `current_local_artifact_id` /
///   `current_local_artifact_kind`, **and** conjoined with the sandbox no
///   longer being attached to the queue row's node. This is what lets a row go
///   once the pin has moved on: a sandbox that suspends here, restores
///   elsewhere and re-suspends there leaves behind an artifact that will never
///   gain a snapshot of its own, and without this disjunct those rows
///   accumulate as false casualties forever.
///
///   The residency conjunct is what makes "the pin moved on" true rather than
///   assumed. Durability of the marker alone does not order the two
///   artifacts: the marker can be *behind* the queued artifact.
///   `collect_sandbox_states` runs before `collect_pending_local_backups`, so
///   a suspend completing between the two round trips ships a report naming
///   an artifact the sandbox census has not caught up to yet; and a
///   `state_degraded` sandbox skips the marker assignment entirely, leaving
///   the marker behind for as long as it stays degraded. Both arise
///   same-node, but that is a fact about how lag *starts*, not about how long
///   it lasts, and a lagged sandbox can then be restored elsewhere.
///
///   So the conjunct is not justified by "lag is same-node". It is justified
///   by where the marker can go next.
///   [`publish_clear_or_preserve_current_local_artifact_marker_assignments`]
///   moves the marker only for a sandbox some node is publishing a non-NULL
///   artifact for. Reaching this delete means the node's *pending-backup
///   report* no longer names the sandbox, which is weaker than the node
///   having stopped publishing it — so the residency conjunct is what
///   carries the argument, not the report: once the sandbox is attached to a
///   different node, the marker can only ever advance to that node's
///   artifact and can never return to the one this row tracks. Those bytes can no longer become anything's restore
///   source, which is what makes the row stale rather than a casualty.
///
///   While the sandbox is still attached here, none of that holds — the
///   marker may yet catch up to the queued artifact — so the row stays. That
///   is the whole asymmetry the conjunct encodes.
///
///   Which is why the arm spells this as [`sandbox_attached_anywhere`] **and
///   not** [`sandbox_attached_to_node`], rather than as a bare inequality:
///   "not here" and "somewhere else" differ exactly on the detached sandbox,
///   and only the second licenses the delete. Those two fragments carry the
///   column detail and the NULL behaviour. Placement writes both columns on
///   a real relocation (`target_node_id = placed.node_id`), so a genuine
///   restore elsewhere satisfies them.
///
///   Deliberately **not** [`matching_ready_backup_exists`], which looks like
///   exactly this and filters `sn.type = 'full'`. `begin_artifact_backup`
///   derives `snapshots.type` from the artifact kind, so that filter means
///   `artifact_kind = 'full'`, and a `stopped` sandbox's marker is a `disk`
///   artifact — every such sandbox would be permanently unable to satisfy this
///   disjunct. The identity comparison already pins the kind to whatever the
///   marker names, so no `type` filter is wanted on top of it.
///
/// ## Why no timestamp, and why not the full source-release predicate
///
/// The obvious spellings are both unsound, for one reason:
/// `snapshots.timestamp` is `DEFAULT NOW()` and no writer binds it, so it is
/// the row's **insert** time, written by `BeginArtifactBackup`. A deferred or retried backup of an
/// **older** artifact therefore inserts a row stamped later than a **newer**
/// artifact's, so any "newer than X" test can read older bytes as superseding
/// newer ones and delete the queue row for the only copy of the newer state.
///
/// That rules out a direct comparison against the queue row, and it also rules
/// out [`snapshot_durable_for_source_release`], which looks like the right
/// predicate to delegate to and is not. Its marker-carrying arm is an
/// artifact-identity comparison, but note it sits under that predicate's
/// outer `sn.type = 'full'` — the same filter rejected above — so it cannot
/// serve a `disk` marker either. Its **markerless** arm — the one
/// taken when `current_local_artifact_id IS NULL` — splits again, and the
/// split is why delegating would be wrong:
///
/// * `evacuation_restore_target IS NULL`: accepts any ready full snapshot
///   stamped at or after `placement_changed_at`. The same insert-time
///   timestamp, so the same hole.
/// * `evacuation_restore_target IS NOT NULL`: uses
///   [`no_newer_full_snapshot`] instead, which is stronger — but it is still
///   an ordering over snapshots that **have rows**, and an artifact whose
///   backup was never begun has none.
///
/// Releasing a *source node* on either is a different and weaker claim than
/// saying a specific artifact's bytes are expendable, so this predicate takes
/// only the identity-tied half.
///
/// ## The cost, which is deliberate
///
/// A markerless sandbox never satisfies the departed-marker arm, so its absent
/// rows stay queued and counted as casualties. Two populations land there, and
/// the second is a real false positive:
///
/// * Rollout-skew rows that never had a marker. Legacy, shrinking.
/// * Sandboxes the drain's own recovery path **deliberately** made markerless.
///   `adopt_remote_snapshot_for_drain_query` NULLs the marker, the evacuation
///   target and `placement_changed_at` together, precisely so a stuck row
///   stops blocking a drain, and it does so only after checking
///   [`latest_full_snapshot_ready`]. Such a sandbox is recoverable and this
///   predicate will still call its absent queue rows casualties. A NULL marker
///   makes the identity comparison NULL, so the disjunct is false rather than
///   erroring.
///
/// Admitting the second population would mean accepting
/// [`latest_full_snapshot_ready`] as supersession proof, and it is not one for
/// *this* question. It asks whether the newest full snapshot is ready, which
/// is an ordering over snapshots that **have rows**; an artifact whose backup
/// was never begun has no row, so it does not make
/// [`no_newer_full_snapshot`] false and the predicate can be true while that
/// artifact's bytes are the only copy. That is the same blind spot as the
/// timestamp comparison above, reached from a different direction.
///
/// So the false casualties stay. A kept row costs the reader a re-probe and
/// this metric some noise; a wrongly deleted one costs a sandbox. Whether the
/// drain's reader should discount adopted rows is a question for the reader,
/// which re-applies its own predicates per row and is where the release
/// decision actually lives.
///
/// **Residency here reads the two `sandboxes` columns only.** It does not
/// read `sandbox_direct_route_identities.current_node_id`, which the census
/// decline guard — `pplx-space-sandbox-store`'s
/// `any_undescribed_artifact_owner` — does union in, so the two answer
/// "has this sandbox left this node" from different inputs.
///
/// The difference shows only when the route disagrees with both `sandboxes`
/// columns. Unioning it would hold the departed-marker arm shut for a sandbox
/// the route places on this node, and would open it for one the `sandboxes`
/// columns place nowhere while the route names somewhere else.
///
/// Three of the paths that write a route pin it to the placement columns,
/// each differently. All live in
/// `crates/libs/api/src/controllers/substrate/direct.rs`:
///
/// * `persist_route_identity_receipt` moves `current_node_id` to a different
///   node only under an `EXISTS` requiring `sandboxes.active_node_id` to
///   already name that node.
/// * `persist_owned_restore_route_identity_receipt` writes the route and both
///   placement columns in one transaction that rolls back unless the
///   placement update matches a row.
/// * The pooled-adoption path takes a receipt only when its
///   `current_node_id` equals the candidate node and builds the claim's
///   `active_node_id` from that same candidate, so the row
///   `commit_pooled_sandbox_claim` inserts names the node the route names.
///
/// The initial insert pins nothing, and the ordinary create sequence is not
/// atomic across the two: `controllers/sandbox/queries.rs` inserts the
/// sandbox `'unplaced'`, the route is persisted while creating the execution,
/// and `active_node_id` is set by a separate later `UPDATE` that can fail on
/// its own. A failure there leaves a route naming a node while both placement
/// columns are still NULL. It is not a divergence this predicate can act on:
/// the sandbox id is freshly minted, so any queue row for it belongs to the
/// node that created it — the node the route names — and unioning the column
/// would find `sandbox_attached_to_node` true and change nothing.
///
/// So the column is omitted because no reachable state needs it, not because
/// omitting it is safer. What would change that is a path establishing a
/// route for a sandbox the placement columns already place on a different
/// node; that is the thing to look for before trusting this paragraph.
///
/// Read positively. Both halves are self-contained `EXISTS` expressions, so
/// the composition warning on `artifact_identity_or_legacy` applies to a
/// caller that negates the result.
pub fn queued_artifact_recoverable_without_node(
    sandbox_alias: &str,
    artifact_id: &str,
    artifact_kind: &str,
    node_id: &str,
) -> String {
    let durable =
        artifact_has_durable_copy(&format!("{sandbox_alias}.id"), artifact_id, artifact_kind);
    let current_artifact_durable = artifact_has_durable_copy(
        &format!("{sandbox_alias}.id"),
        &format!("{sandbox_alias}.current_local_artifact_id"),
        &format!("{sandbox_alias}.current_local_artifact_kind"),
    );
    let attached_here = sandbox_attached_to_node(sandbox_alias, node_id);
    let attached_anywhere = sandbox_attached_anywhere(sandbox_alias);
    format!(
        "(\n\
             {durable}\n\
             OR (\n\
                  {attached_anywhere}\n\
                  AND NOT {attached_here}\n\
                  AND {current_artifact_durable}\n\
                )\n\
         )"
    )
}

/// Returns a SQL fragment evaluating to TRUE iff a deleting snapshot matches
/// the supplied sandbox and local-artifact identity expressions without a
/// non-orphaned ready copy of the same immutable artifact.
///
/// The "without a ready copy" half is [`artifact_has_durable_copy`], shared
/// with the ClusterApi census mirror's delete guard so the two cannot come to
/// disagree about what makes an artifact safe to stop tracking.
pub fn deleting_snapshot_matches_artifact(
    sandbox_id: &str,
    artifact_id: &str,
    artifact_kind: &str,
) -> String {
    let durable = artifact_has_durable_copy(sandbox_id, artifact_id, artifact_kind);
    let matches_artifact =
        snapshot_matches_artifact("deleting_snapshot", artifact_id, artifact_kind);
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots deleting_snapshot\n\
              WHERE deleting_snapshot.sandbox_id = {sandbox_id}\n\
                AND deleting_snapshot.state = 'deleting'\n\
                AND {matches_artifact}\n\
                AND NOT {durable}\n\
         )"
    )
}

/// Returns a SQL fragment evaluating to TRUE iff the snapshot aliased as
/// `snapshot_alias` is a snapshot some restore path would currently load for
/// the sandbox aliased as `sandbox_alias` — i.e. deleting it would strand a
/// live sandbox.
///
/// This is the deletion-side counterpart to the restore *selector*, and it
/// has to cover **both** shapes that selector can take, because which one
/// applies depends on the row's lifecycle:
///
/// 1. A marked row restores by exact artifact identity
///    ([`snapshot_is_current_local_artifact`]).
/// 2. A row already in `restoring` retries from the pre-restore source window
///    ([`restore_retry_source_snapshot_window`]) instead, deliberately
///    ignoring the marker — a failed restore branch can publish a *later*
///    artifact after the row entered `restoring`, and the retry must load the
///    state restore originally started from, not that later one.
///
/// Covering only (1) leaves a real hole: those later failed-branch artifacts
/// still count toward the retention rank, so once `keep_n` of them pile up,
/// the retry's actual source falls outside the keep-set and becomes
/// prune-eligible while the sandbox is still trying to restore from it.
///
/// The `restoring` arm is deliberately wider than the selector: the selector
/// takes the newest row in the window, this shields every row in it.
/// The excess is temporary: `restoring` is a transient lifecycle the sweeper
/// bounds, so those rows become reclaimable again as soon as the row leaves
/// it — and erring wide costs deferred disk while erring narrow costs an
/// unrestorable sandbox.
///
/// NULL-safe for the same reason [`snapshot_is_current_local_artifact`] is,
/// and callers negate this too: `sandboxes.lifecycle` is `NOT NULL`, and
/// [`restore_retry_source_snapshot_window`] is built entirely from
/// `IS NULL`-guarded disjunctions, so the composed expression is never NULL.
///
/// ## Why neither arm narrows to "the row the selector would pick"
///
/// Both arms match a *set* of snapshots while the selector takes one row
/// from it (`ORDER BY timestamp DESC LIMIT 1`), so both shield more than
/// strictly necessary. That is deliberate.
///
/// For the marker arm, narrowing is not even expressible here:
/// [`snapshot_is_current_local_artifact`] has five consumers (listed in this
/// module's docs) and "newest matching" is meaningful only for the two
/// selectors. The portability check asks whether *any* matching ready backup
/// exists; the retention store's reporting query asks whether one specific row
/// is the marked one; and each selector's branch is a filter it then orders and
/// limits itself. Narrowing the shared fragment would break all of them; adding
/// a second near-identical fragment would reintroduce exactly the drift this
/// file exists to prevent.
///
/// Narrowing this safety guard is dangerous: the suspend selector's `ORDER BY`
/// has no id tiebreak, so on equal timestamps its choice is not deterministic.
/// A "newest by `(timestamp, id)`" guard could shield a different row than
/// restore actually loads, making the sandbox unrestorable. Its disk-only
/// sibling does order by `(timestamp, id)`, but the shared guard must protect
/// both selectors.
pub fn snapshot_is_live_restore_source(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let marked = snapshot_is_current_local_artifact(sandbox_alias, snapshot_alias);
    let retry_window = restore_retry_source_snapshot_window(sandbox_alias, snapshot_alias);
    let restoring = SandboxLifecycle::Restoring.as_str();
    format!(
        "(\n\
             {marked}\n\
             OR ({sandbox_alias}.lifecycle = '{restoring}' AND {retry_window})\n\
         )"
    )
}

/// Returns a correlated `NOT EXISTS(...)` SQL fragment that is TRUE iff the
/// snapshot aliased as `snapshot_alias` is the **newest** `type='full'`
/// snapshot for the sandbox aliased as `sandbox_alias` — i.e. no later
/// (`timestamp >`) full snapshot of any state exists. Compose it into a query
/// that already joins a `snapshots` row.
///
/// A newer `creating` (upload in flight) or `failed` row both count as
/// "newer," so they both
/// make the older row non-newest. This is the safe direction: the newest
/// suspend is authoritative, so an older `ready` snapshot must never be treated
/// as current while a newer attempt exists. If the predicate ignored newer
/// `failed` rows and the *current* suspend's own snapshot is the failed one,
/// an older `ready` snapshot would become "newest ready" and a restore from it
/// would load stale bytes (silent data loss) — strictly worse than waiting.
/// When the newest full snapshot is `failed` (the current evacuation upload
/// failed), this makes [`latest_full_snapshot_ready`] false, so the sandbox is
/// not restore-pending and keeps blocking source release. It does **not**
/// wedge: the node-drain reupload queue (`pinned_local_only_sandboxes_query`)
/// uses [`snapshot_durable_for_source_release`] — also marker-aware — so a
/// marked sandbox whose newest snapshot is `failed` is enumerated, reuploaded
/// from the on-disk artifacts (still present, since the source can't terminate
/// while blocked), and a new `ready` snapshot re-drives the restore. If the
/// artifacts are gone the reupload returns `ArtifactMissing` and the give-up
/// path marks it `failed` — a terminal outcome, never a restore from the stale
/// older snapshot. A `deleting` row remains a blocker even though it cannot be
/// selected: falling back past a claimed newest artifact before its catalog
/// row is finalized could restore stale state after a floor breach.
pub fn no_newer_full_snapshot(sandbox_alias: &str, snapshot_alias: &str) -> String {
    format!(
        "NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND newer.timestamp > {snapshot_alias}.timestamp\n\
         )"
    )
}

/// Like [`no_newer_full_snapshot`], except that a newer failed retry of the
/// same immutable artifact does not invalidate an already-ready snapshot.
///
/// A non-null local artifact ID is required to prove identity. Markerless
/// legacy snapshots keep the strict newest-full-snapshot rule.
pub fn no_newer_full_snapshot_ignoring_failed_retry(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    format!(
        "NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND newer.timestamp > {snapshot_alias}.timestamp\n\
                AND NOT (\n\
                    {snapshot_alias}.local_artifact_id IS NOT NULL\n\
                    AND newer.state = 'failed'\n\
                    AND newer.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                    AND newer.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                    AND COALESCE(newer.artifact_origin, '{suspend}') =\n\
                        COALESCE({snapshot_alias}.artifact_origin, '{suspend}')\n\
                )\n\
         )"
    )
}

/// A ready suspend artifact is current recovery evidence only when it covers
/// all recorded activity and is still the newest full snapshot.
pub fn quarantine_recovery_suspend_snapshot_current(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let no_newer = no_newer_full_snapshot_ignoring_failed_retry(sandbox_alias, snapshot_alias);
    format!(
        "{snapshot_alias}.type = 'full'\n\
         AND {snapshot_alias}.state = 'ready'\n\
         AND ({snapshot_alias}.artifact_origin IS NULL\n\
              OR {snapshot_alias}.artifact_origin = '{suspend}')\n\
         AND ({sandbox_alias}.last_command_at IS NULL\n\
              OR {snapshot_alias}.timestamp >= {sandbox_alias}.last_command_at)\n\
         AND ({sandbox_alias}.last_access_at IS NULL\n\
              OR {snapshot_alias}.timestamp >= {sandbox_alias}.last_access_at)\n\
         AND {no_newer}"
    )
}

/// The orphan fallback additionally requires evidence that predates the
/// quarantine episode it is intended to recover.
pub fn pre_quarantine_suspend_snapshot_current(
    sandbox_alias: &str,
    snapshot_alias: &str,
    quarantine_entered_at: &str,
) -> String {
    let current = quarantine_recovery_suspend_snapshot_current(sandbox_alias, snapshot_alias);
    format!(
        "{current}\n\
         AND {snapshot_alias}.timestamp <= {quarantine_entered_at}"
    )
}

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the **newest** `type='full'` snapshot for the sandbox aliased as
/// `sandbox_alias` is `state='ready'` and durably registered in
/// `snapshot_storage_locations`.
///
/// This is the correct portability gate for the node-drain cohort restore, and
/// it differs from [`fresh_ready_snapshot_exists`] in *which* snapshot it
/// trusts: rather than "a ready snapshot newer than the last placement," it
/// asks "is the latest suspend's snapshot ready?" That distinction matters in
/// two scenarios the freshness gate gets wrong on the evacuation path:
///
/// * **Stale prior-cycle snapshot.** After `wake → modify → evacuation-suspend`
///   the sandbox has an old `ready` snapshot (the previous cycle) *and* a fresh
///   `creating` upload (the current suspend). A plain "any ready snapshot
///   exists" check would select the old one and restore stale bytes — silent
///   data loss. Gating on the *newest* full snapshot being ready waits for the
///   current upload to finalize before the sandbox is restorable.
/// * **Failed-restore placement bump.** A failed cohort-restore attempt places
///   a rebuilt pod, bumping `placement_changed_at` past the snapshot's
///   `timestamp`. [`fresh_ready_snapshot_exists`] would then classify the
///   sandbox's own canonical snapshot as stale and strand the retry. This gate
///   references no placement timestamp, so the retry stays eligible.
///
/// Keep using [`fresh_ready_snapshot_exists`] for the unmarked / general
/// portability questions (admin `:terminate` guard, reupload/give-up queue,
/// source-drain blocker, initial user restore fallback): there the
/// last-placement freshness comparison is the right "is this sandbox portable
/// now?" question. After a restore claim has moved a sandbox to `restoring`,
/// use [`restore_retry_source_snapshot_window`] for user-restore retries so the
/// restore placement itself cannot make the selected snapshot appear stale,
/// while later artifacts from a failed branch stay out of the source set.
///
/// `sandbox_alias` is the SQL alias for the `sandboxes` row in the calling
/// query — pass the bare table name (`"sandboxes"`) when no alias is set.
/// The fragment is safe to compose with `NOT`, `AND`, `OR`, etc.
pub fn latest_full_snapshot_ready(sandbox_alias: &str) -> String {
    let no_newer = no_newer_full_snapshot(sandbox_alias, "sn");
    let exact = matching_ready_backup_exists(sandbox_alias);
    let legacy = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND {no_newer}\n\
         )"
    );
    artifact_identity_or_legacy(sandbox_alias, &exact, &legacy)
}

/// Returns a boolean SQL fragment for a candidate snapshot row from the source
/// window of a `restoring` retry.
///
/// A failed restore branch can publish a later artifact after the sandbox row
/// entered `restoring`. Retry pod creation must ignore those later rows and
/// use the source state that restore originally started from. If placement
/// moved before restore started, it still bounds stale prior-cycle snapshots;
/// if placement moved during restore, it belongs to the failed branch and must
/// not strand the original source snapshot. When the sandbox currently carries
/// a local artifact id, markerless snapshots are not strong enough evidence for
/// this restore source.
pub fn restore_retry_source_snapshot_window(sandbox_alias: &str, snapshot_alias: &str) -> String {
    format!(
        "(\n\
             ({sandbox_alias}.lifecycle_changed_at IS NULL\n\
              OR {snapshot_alias}.timestamp < {sandbox_alias}.lifecycle_changed_at)\n\
             AND (\n\
                 {sandbox_alias}.placement_changed_at IS NULL\n\
                 OR {sandbox_alias}.lifecycle_changed_at IS NULL\n\
                 OR {sandbox_alias}.placement_changed_at >= {sandbox_alias}.lifecycle_changed_at\n\
                 OR {snapshot_alias}.timestamp >= {sandbox_alias}.placement_changed_at\n\
             )\n\
             AND (\n\
                 {sandbox_alias}.current_local_artifact_id IS NULL\n\
                 OR ({snapshot_alias}.local_artifact_id IS NOT NULL\n\
                     AND {snapshot_alias}.artifact_kind = 'full')\n\
             )\n\
         )"
    )
}

/// Returns TRUE when `snapshot_alias` is the newest full snapshot in the
/// restore attempt's pre-claim source window. Newer rows created after the
/// restore claim are ignored. A newer pre-claim `creating` row or `failed` row
/// for a different artifact still blocks stale fallback. A `deleting` row also
/// blocks fallback until finalization; only failed retries of the same
/// immutable artifact are ignored.
pub fn restore_retry_source_snapshot_is_current(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let source_window = restore_retry_source_snapshot_window(sandbox_alias, snapshot_alias);
    let newer_source_window = restore_retry_source_snapshot_window(sandbox_alias, "newer");
    format!(
        "({source_window}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND newer.timestamp > {snapshot_alias}.timestamp\n\
                AND {newer_source_window}\n\
                AND NOT (\n\
                    {snapshot_alias}.local_artifact_id IS NOT NULL\n\
                    AND newer.state = 'failed'\n\
                    AND newer.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                    AND newer.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                    AND COALESCE(newer.artifact_origin, '{suspend}') =\n\
                        COALESCE({snapshot_alias}.artifact_origin, '{suspend}')\n\
                )\n\
         ))"
    )
}

/// Returns a SQL fragment evaluating to TRUE iff the sandbox has a durable
/// snapshot that the `restoring` retry path can use as its source.
pub fn restore_retry_source_snapshot_exists(sandbox_alias: &str) -> String {
    let source_is_current = restore_retry_source_snapshot_is_current(sandbox_alias, "sn");
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND {source_is_current}\n\
         )"
    )
}

/// Returns a SQL fragment evaluating to TRUE iff a `restoring` row can safely be
/// reverted to `suspended`.
///
/// This is intentionally stricter than [`restore_retry_source_snapshot_exists`].
/// Once a row is `suspended`, restore selection no longer has the `restoring`
/// source-window guard. Reverting is safe only when the suspended selector will
/// still choose the same source: either the current artifact identity itself has
/// a ready pre-restore snapshot, or a markerless row's newest full snapshot is
/// the pre-restore source.
pub fn restore_retry_suspended_revert_snapshot_exists(sandbox_alias: &str) -> String {
    let exact = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.local_artifact_id = {sandbox_alias}.current_local_artifact_id\n\
                AND sn.artifact_kind = {sandbox_alias}.current_local_artifact_kind\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND ({sandbox_alias}.lifecycle_changed_at IS NULL\n\
                     OR sn.timestamp < {sandbox_alias}.lifecycle_changed_at)\n\
         )"
    );
    let no_newer = no_newer_full_snapshot(sandbox_alias, "sn");
    let source_window = restore_retry_source_snapshot_window(sandbox_alias, "sn");
    let legacy = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND {source_window}\n\
                AND {no_newer}\n\
         )"
    );
    artifact_identity_or_legacy(sandbox_alias, &exact, &legacy)
}

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the sandbox aliased as `sandbox_alias` has a `type='full'` suspend snapshot
/// from the **current** suspend cycle that is durably registered and
/// `state='ready'` — where "current cycle" means the snapshot was created at or
/// after the row entered its current lifecycle
/// (`sn.timestamp >= {alias}.lifecycle_changed_at`).
///
/// This is the crash-recovery redrive's "did *this* suspend finalize a durable
/// snapshot?" gate, and it differs from [`latest_full_snapshot_ready`] in the one
/// place that matters for a row stranded in `suspending`: the
/// `lifecycle_changed_at` anchor. If a controller crash strands the row in
/// `suspending` **before** `BeginArtifactBackup` inserts this cycle's row, a
/// stale `ready` snapshot left by a prior `wake → modify → re-suspend` cycle is
/// the *newest* full snapshot — so [`latest_full_snapshot_ready`] would report
/// ready and the redrive would advance the row, letting the restore load
/// pre-wake bytes (silent data loss). The current snapshot is always created
/// strictly after the `suspending` claim that set `lifecycle_changed_at`, so
/// gating on `sn.timestamp >= {alias}.lifecycle_changed_at` accepts this
/// suspend's snapshot and rejects every prior-cycle one. `lifecycle_changed_at`
/// (not `placement_changed_at`) is the right anchor: it is set exactly when the
/// row entered `suspending` and does not move while it stays there, whereas
/// `placement_changed_at` is bumped by `finish_suspended_execution` mid-suspend.
pub fn current_suspend_snapshot_ready(sandbox_alias: &str) -> String {
    let exact = matching_ready_backup_exists(sandbox_alias);
    let legacy = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND sn.timestamp >= {sandbox_alias}.lifecycle_changed_at\n\
         )"
    );
    artifact_identity_or_legacy(sandbox_alias, &exact, &legacy)
}

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the sandbox aliased as `sandbox_alias` has a durable `state='ready'` full
/// suspend snapshot that exactly matches its current artifact marker, taken at
/// or after the last lifecycle transition.
///
/// Use it for code paths that must distinguish "the marker is current" from
/// "the row has no marker, so restore will choose by newest snapshot". The
/// `lifecycle_changed_at` bound is deliberate for the always-on suspend
/// projections (node-publish suspended projection, snapshot-ready projection,
/// pod-teardown started-row recovery): those run concurrently with in-flight
/// lifecycle RPCs, and a marker branch that trusts a pre-transition snapshot
/// lets a projection finalize a suspend out from under the API's own suspend
/// call during rapid suspend/wake cycles. Code paths that have separately
/// proven the row inactive should use
/// [`exact_marker_snapshot_backup_current`] instead.
fn current_suspend_origin_marker_snapshot_ready_with_guards(
    sandbox_alias: &str,
    additional_guards: &str,
) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.local_artifact_id = {sandbox_alias}.current_local_artifact_id\n\
                AND sn.artifact_kind = {sandbox_alias}.current_local_artifact_kind\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND {sandbox_alias}.current_local_artifact_origin = '{suspend}'\n\
                AND (sn.artifact_origin IS NULL OR sn.artifact_origin = '{suspend}')\n\
                AND sn.timestamp >= {sandbox_alias}.lifecycle_changed_at\n\
                {additional_guards}\n\
         )"
    )
}

pub fn current_suspend_origin_marker_snapshot_ready(sandbox_alias: &str) -> String {
    current_suspend_origin_marker_snapshot_ready_with_guards(sandbox_alias, "")
}

/// Returns the lifecycle gate shared by suspended-state projection paths.
///
/// Stable active lifecycles can accept the caller's ordinary portability
/// proof. A `suspending` row additionally requires the active Kubernetes
/// execution to have reached `suspended` and caller-supplied evidence that the
/// publication identifies the exact committed suspend artifact.
pub fn suspended_projection_lifecycle_eligible(
    sandbox_alias: &str,
    committed_suspend_evidence: &str,
) -> String {
    suspended_projection_lifecycle_eligible_with_node_evidence(
        sandbox_alias,
        committed_suspend_evidence,
        None,
    )
}

/// Returns the suspended projection lifecycle gate with an optional exact
/// node-publication authority proof. The node proof is used only for a
/// `suspending` row whose active execution still says `running`: the node's
/// exact execution identity and durable suspend artifact are then sufficient
/// to close a driver-crashed suspend without trusting a stale pod.
pub fn suspended_projection_lifecycle_eligible_with_node_evidence(
    sandbox_alias: &str,
    committed_suspend_evidence: &str,
    node_suspend_evidence: Option<&str>,
) -> String {
    let node_suspend_branch = node_suspend_evidence
        .map(|evidence| format!("OR ({evidence})"))
        .unwrap_or_default();
    format!(
        "(\n\
             LOWER(TRIM({sandbox_alias}.lifecycle)) IN ('started', 'paused', 'restoring')\n\
             OR (\n\
                 LOWER(TRIM({sandbox_alias}.lifecycle)) = 'suspending'\n\
                 AND EXISTS (\n\
                     SELECT 1\n\
                       FROM sandbox_k8s_executions execution\n\
                      WHERE execution.sandbox_id = {sandbox_alias}.id\n\
                        AND execution.active\n\
                        AND execution.state = 'suspended'\n\
                 )\n\
                 AND ({committed_suspend_evidence})\n\
             )\n\
             {node_suspend_branch}\n\
         )"
    )
}

/// Node authority for finalizing a suspend whose API-side driver did not
/// commit the active execution. Generation is always exact; pod UID is exact
/// when retained, with NULL accepted only because terminal service-route
/// repair historically cleared that field. The durable artifact still has to
/// match the active node and postdate the suspend lifecycle transition. Every
/// comparison is against the same sandbox row lock held by the projection
/// transaction.
pub fn node_published_execution_identity_matches(
    execution_alias: &str,
    generation_expression: &str,
    pod_uid_expression: &str,
    pod_uid_missing_authority: &str,
) -> String {
    format!(
        "{execution_alias}.generation = {generation_expression}\n\
         AND (\n\
             {execution_alias}.pod_uid = {pod_uid_expression}\n\
             OR ({pod_uid_missing_authority} AND {execution_alias}.pod_uid IS NULL)\n\
         )"
    )
}

pub fn node_published_suspend_evidence(sandbox_alias: &str, input_alias: &str) -> String {
    let identity_match = node_published_execution_identity_matches(
        "execution",
        &format!("{input_alias}.execution_generation"),
        &format!("{input_alias}.pod_uid"),
        "TRUE",
    );
    format!(
        "LOWER(TRIM({sandbox_alias}.lifecycle)) = 'suspending'\n\
         AND {input_alias}.execution_generation IS NOT NULL\n\
         AND {input_alias}.pod_uid IS NOT NULL\n\
         AND EXISTS (\n\
             SELECT 1\n\
               FROM sandbox_k8s_executions execution\n\
              WHERE execution.sandbox_id = {sandbox_alias}.id\n\
                AND execution.active\n\
                AND execution.state = 'running'\n\
                AND {identity_match}\n\
         )\n\
         AND {input_alias}.current_local_artifact_id IS NOT NULL\n\
         AND {input_alias}.current_local_artifact_kind = 'full'\n\
         AND {input_alias}.current_local_artifact_origin = 'suspend'\n\
         AND EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.source_node_id = {sandbox_alias}.active_node_id\n\
                AND sn.local_artifact_id = {input_alias}.current_local_artifact_id\n\
                AND sn.artifact_kind = 'full'\n\
                AND sn.artifact_origin = 'suspend'\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.timestamp >= {sandbox_alias}.lifecycle_changed_at\n\
         )"
    )
}

/// Like [`current_suspend_origin_marker_snapshot_ready`], but trusts the exact
/// marker's durable backup even when the snapshot predates
/// `lifecycle_changed_at`.
///
/// Exact artifact identity is stronger than the lifecycle timestamp: the
/// marker names immutable suspend bytes, and a later stuck `started`
/// projection can move `lifecycle_changed_at` past the snapshot without
/// changing those bytes. But recorded activity after the marker means restore
/// would roll back observable work, so the marker is current only when it also
/// covers the last command/access clocks. A newer full snapshot still makes
/// the marker stale, except for a failed retry that names the same immutable
/// artifact identity. That retry contains no newer bytes and must not hide an
/// already-ready durable copy of the exact marker.
///
/// Only for callers that separately prove no runtime can modify the artifact:
/// either an aged inactive repair candidate, or the exact terminal recovery
/// owner with no pod materialization or placement while holding the sandbox
/// lock and retiring that owner in the same transaction. Always-on projections
/// must keep the lifecycle-bounded variant, or they race in-flight lifecycle
/// RPCs.
pub fn exact_marker_snapshot_backup_current(sandbox_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let current = quarantine_recovery_suspend_snapshot_current(sandbox_alias, "sn");
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.local_artifact_id = {sandbox_alias}.current_local_artifact_id\n\
                AND sn.artifact_kind = {sandbox_alias}.current_local_artifact_kind\n\
                AND {sandbox_alias}.current_local_artifact_origin = '{suspend}'\n\
                AND {current}\n\
         )"
    )
}

/// Paired SQL fragments for a revert-to-`suspended` repair decision:
/// `eligible` gates whether the row may be reverted at all, and
/// `clear_marker` says whether the revert must drop a stale artifact marker
/// so the restore selector picks the newest snapshot instead.
pub struct SuspendRevertEligibility {
    pub clear_marker: String,
    pub eligible: String,
}

fn suspend_revert_eligibility(
    sandbox_alias: &str,
    marker_current: String,
    newest_current: String,
) -> SuspendRevertEligibility {
    SuspendRevertEligibility {
        clear_marker: format!(
            "({sandbox_alias}.current_local_artifact_id IS NOT NULL AND NOT ({marker_current}))"
        ),
        eligible: format!("(({marker_current}) OR ({newest_current}))"),
    }
}

/// Returns SQL fragments for the shared started-suspend recovery decision.
///
/// `clear_marker` is TRUE when the sandbox carries a marker, but that marker is
/// not backed by its exact current suspend snapshot. `eligible` is TRUE when
/// either that exact marker is current, or markerless restore would select a
/// newest current suspend snapshot. Row-state guards such as lifecycle, active
/// executions, pod IP evidence, and age remain caller-owned.
pub fn started_suspend_revert_eligibility(sandbox_alias: &str) -> SuspendRevertEligibility {
    let marker_current = current_suspend_origin_marker_snapshot_ready(sandbox_alias);
    let newest_current = newest_suspend_snapshot_current(sandbox_alias);
    suspend_revert_eligibility(sandbox_alias, marker_current, newest_current)
}

/// Returns eligibility for finalizing a `suspending` sandbox whose active pod
/// disappeared after publishing a durable snapshot.
///
/// The snapshot must come from `active_node_id` because pod absence proves only
/// that execution's handoff completed; a snapshot from another node cannot
/// close it. It must also cover the last command and access clocks so recovery
/// cannot discard activity observed after publication. For markerless rows, the
/// proof selects the same unique newest durable ready snapshot that wake will
/// restore. Newer `creating` and `failed` attempts do not hide that snapshot
/// because wake cannot select them.
pub fn missing_suspending_pod_revert_eligibility(sandbox_alias: &str) -> SuspendRevertEligibility {
    let marker_guards = format!(
        "AND sn.source_node_id = {sandbox_alias}.active_node_id\n\
         AND ({sandbox_alias}.last_command_at IS NULL\n\
              OR sn.timestamp >= {sandbox_alias}.last_command_at)\n\
         AND ({sandbox_alias}.last_access_at IS NULL\n\
              OR sn.timestamp >= {sandbox_alias}.last_access_at)"
    );
    let marker_current =
        current_suspend_origin_marker_snapshot_ready_with_guards(sandbox_alias, &marker_guards);
    let newest_current = newest_suspend_snapshot_current_from_active_node(sandbox_alias);
    suspend_revert_eligibility(sandbox_alias, marker_current, newest_current)
}

/// Repair-loop variant of [`started_suspend_revert_eligibility`]: the marker
/// branch trusts an exact durable backup even when the snapshot predates
/// `lifecycle_changed_at` (see [`exact_marker_snapshot_backup_current`]).
///
/// Only for the started-sandbox repair reconciler, which lists rows already
/// proven inactive (no active execution, no pod evidence) and aged past its
/// repair minimum before this eligibility runs. Always-on projections must use
/// the default variant.
pub fn started_repair_revert_eligibility(sandbox_alias: &str) -> SuspendRevertEligibility {
    let marker_current = exact_marker_snapshot_backup_current(sandbox_alias);
    let newest_current = newest_suspend_snapshot_current(sandbox_alias);
    suspend_revert_eligibility(sandbox_alias, marker_current, newest_current)
}

/// Like [`newest_suspend_snapshot_current`], but anchored on the activity
/// clocks alone — the snapshot is not required to postdate
/// `lifecycle_changed_at`.
///
/// It exists for `failed` rows recoverable from a durable suspend snapshot:
/// a restore interrupted mid-staging leaves artifacts on disk, the node's
/// inventory reconcile materializes them into a `failed` row, and the control
/// plane adopts that failure over a sandbox whose canonical suspend snapshot
/// is intact in S3. For that cohort the snapshot always *predates* the failed
/// transition (`lifecycle_changed_at` moved when the row failed and the VM
/// never reactivated), so the lifecycle-anchored variant rejects exactly the
/// rows a repair loop exists to recover. The same is true of
/// `placement_changed_at`: the failed restore's own placement bump postdates
/// the snapshot, so placement-freshness gates are equally poisoned.
///
/// The no-rollback proof carries over unchanged from
/// [`newest_suspend_snapshot_current`]: the snapshot must cover the last
/// recorded command/access clocks, and no concretely-non-suspend full-ready
/// snapshot may tie or beat it by timestamp (faithfulness to the markerless
/// restore selector, which picks the newest full-ready snapshot with no
/// origin filter).
///
/// Only for callers that separately prove no runtime can modify the artifact:
/// either an aged inactive repair candidate, or the exact terminal recovery
/// owner with no pod materialization or placement while holding the sandbox
/// lock and retiring that owner in the same transaction. Always-on projections
/// must use the lifecycle-bounded variant.
pub fn newest_suspend_snapshot_activity_current(sandbox_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND (sn.artifact_origin IS NULL OR sn.artifact_origin = '{suspend}')\n\
                AND ({sandbox_alias}.last_command_at IS NULL\n\
                     OR sn.timestamp >= {sandbox_alias}.last_command_at)\n\
                AND ({sandbox_alias}.last_access_at IS NULL\n\
                     OR sn.timestamp >= {sandbox_alias}.last_access_at)\n\
                AND NOT EXISTS (\n\
                    SELECT 1\n\
                      FROM snapshots sn2\n\
                      JOIN snapshot_storage_locations ssl2 ON ssl2.snapshot_id = sn2.id\n\
                     WHERE sn2.sandbox_id = {sandbox_alias}.id\n\
                       AND ssl2.orphaned_at IS NULL\n\
                       AND sn2.type = 'full'\n\
                       AND sn2.state = 'ready'\n\
                       AND sn2.artifact_origin IS NOT NULL\n\
                       AND sn2.artifact_origin <> '{suspend}'\n\
                       AND sn2.timestamp >= sn.timestamp\n\
                )\n\
         )"
    )
}

/// Repair eligibility for terminally failed restore/recovery attempts that are
/// recoverable from a durable suspend snapshot.
///
/// The marker branch trusts an exact durable backup regardless of the
/// lifecycle/placement clocks ([`exact_marker_snapshot_backup_current`] — the
/// marker names immutable suspend bytes). The markerless branch requires the
/// activity-anchored newest-snapshot proof
/// ([`newest_suspend_snapshot_activity_current`]); the lifecycle-anchored
/// variant would reject the whole cohort, whose snapshot predates the failed
/// transition by construction.
///
/// Callers own the row-state guards. The repair reconciler requires
/// `lifecycle = 'failed'`, no live execution evidence, and a minimum age. The
/// synchronous started-recovery path instead requires its exact terminal owner,
/// no pod UID/IP/node placement, and retires that owner under the same lock and
/// transaction. A compatibility-Pod fence is caller policy.
pub fn failed_repair_revert_eligibility(sandbox_alias: &str) -> SuspendRevertEligibility {
    let marker_current = exact_marker_snapshot_backup_current(sandbox_alias);
    let newest_current = newest_suspend_snapshot_activity_current(sandbox_alias);
    suspend_revert_eligibility(sandbox_alias, marker_current, newest_current)
}

/// Returns the shared SET-list fragment for clearing or preserving the current
/// local artifact marker while reverting a started sandbox back to suspended.
///
/// `clear_marker_expr` must evaluate to TRUE only when the current marker is
/// stale and safe to clear; when it is FALSE, all marker columns are preserved.
pub fn clear_current_local_artifact_marker_assignments(
    sandbox_alias: &str,
    clear_marker_expr: &str,
) -> String {
    format!(
        "current_local_artifact_id = CASE WHEN {clear_marker_expr} THEN NULL ELSE {sandbox_alias}.current_local_artifact_id END,\n\
         current_local_artifact_kind = CASE WHEN {clear_marker_expr} THEN NULL ELSE {sandbox_alias}.current_local_artifact_kind END,\n\
         current_local_artifact_origin = CASE WHEN {clear_marker_expr} THEN NULL ELSE {sandbox_alias}.current_local_artifact_origin END"
    )
}

pub fn write_current_local_artifact_marker_assignments(
    artifact_id_expr: &str,
    artifact_kind_expr: &str,
    artifact_origin_expr: &str,
) -> String {
    format!(
        "current_local_artifact_id = {artifact_id_expr},\n\
         current_local_artifact_kind = {artifact_kind_expr},\n\
         current_local_artifact_origin = {artifact_origin_expr}"
    )
}

/// Returns the shared SET-list fragment for choosing between a newly
/// published artifact marker, clearing a stale marker, and preserving the
/// current marker.
pub fn publish_clear_or_preserve_current_local_artifact_marker_assignments(
    sandbox_alias: &str,
    published_artifact_id_expr: &str,
    published_artifact_kind_expr: &str,
    published_artifact_origin_expr: &str,
    clear_marker_expr: &str,
) -> String {
    format!(
        "current_local_artifact_id = CASE
             WHEN {published_artifact_id_expr} IS NOT NULL THEN {published_artifact_id_expr}
             WHEN {clear_marker_expr} THEN NULL
             ELSE {sandbox_alias}.current_local_artifact_id
         END,
         current_local_artifact_kind = CASE
             WHEN {published_artifact_id_expr} IS NOT NULL THEN {published_artifact_kind_expr}
             WHEN {clear_marker_expr} THEN NULL
             ELSE {sandbox_alias}.current_local_artifact_kind
         END,
         current_local_artifact_origin = CASE
             WHEN {published_artifact_id_expr} IS NOT NULL THEN {published_artifact_origin_expr}
             WHEN {clear_marker_expr} THEN NULL
             ELSE {sandbox_alias}.current_local_artifact_origin
         END"
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SuspendedTargetNodeAssignment {
    Preserve,
    PreserveOrBackfillActive,
    Clear,
}

pub struct SuspendedK8sFinalizationSql {
    sandbox_alias: String,
    candidate_alias: String,
    marker_assignments: String,
    target_node_assignment: SuspendedTargetNodeAssignment,
    lifecycle: SandboxLifecycle,
}

impl SuspendedK8sFinalizationSql {
    pub fn active_execution_assignments(&self, execution_alias: &str) -> String {
        active_execution_assignments(execution_alias, self.lifecycle)
    }

    pub fn open_port_exposure_assignments(&self) -> &'static str {
        OPEN_PORT_EXPOSURE_ASSIGNMENTS
    }

    pub fn sandbox_assignments(&self) -> String {
        sandbox_assignments(
            &self.sandbox_alias,
            &self.marker_assignments,
            self.target_node_assignment,
            self.lifecycle,
        )
    }

    pub fn retire_active_execution_cte(&self) -> String {
        let active_execution_assignments = self.active_execution_assignments("e");
        let candidate_alias = &self.candidate_alias;
        format!(
            "retired_execution AS (\n\
                 UPDATE sandbox_k8s_executions e\n\
                    SET {active_execution_assignments}\n\
                   FROM {candidate_alias}\n\
                  WHERE e.sandbox_id = {candidate_alias}.id\n\
                    AND e.active\n\
                  RETURNING e.sandbox_id\n\
             )"
        )
    }

    pub fn invalidate_open_port_exposures_cte(&self) -> String {
        let open_port_exposure_assignments = self.open_port_exposure_assignments();
        let candidate_alias = &self.candidate_alias;
        format!(
            "reset_port_exposures AS (\n\
                 UPDATE sandbox_port_exposures spe\n\
                    SET {open_port_exposure_assignments}\n\
                   FROM {candidate_alias}\n\
                  WHERE spe.sandbox_id = {candidate_alias}.id\n\
                    AND spe.desired_state = 'open'\n\
                  RETURNING spe.sandbox_id\n\
             )"
        )
    }
}

/// Returns shared SQL fragments for projecting a Kubernetes sandbox as
/// suspended after a durable suspend handoff.
///
/// `marker_assignments` supplies the caller-specific current artifact marker
/// policy: explicit suspend writes the just-published marker, while recovery
/// paths may preserve or clear an existing marker.
pub fn suspended_k8s_finalization_sql(
    sandbox_alias: &str,
    candidate_alias: &str,
    marker_assignments: &str,
    target_node_assignment: SuspendedTargetNodeAssignment,
) -> SuspendedK8sFinalizationSql {
    k8s_finalization_sql(
        sandbox_alias,
        candidate_alias,
        marker_assignments,
        target_node_assignment,
        SandboxLifecycle::Suspended,
    )
}

/// Like [`suspended_k8s_finalization_sql`] but lands the sandbox in
/// `quarantined`, for finalizations that unplace a sandbox after authoritative
/// pod absence with no recoverable artifact.
pub fn quarantined_k8s_finalization_sql(
    sandbox_alias: &str,
    candidate_alias: &str,
    marker_assignments: &str,
    target_node_assignment: SuspendedTargetNodeAssignment,
) -> SuspendedK8sFinalizationSql {
    k8s_finalization_sql(
        sandbox_alias,
        candidate_alias,
        marker_assignments,
        target_node_assignment,
        SandboxLifecycle::Quarantined,
    )
}

/// Private renderer shared by the two public constructors. The lifecycle is
/// deliberately not part of any public signature: only `suspended` and
/// `quarantined` are valid unplaced-terminal states, so exposing the full
/// `SandboxLifecycle` would let a caller render a contradictory row (e.g.
/// `started` with `placement = 'unplaced'`). The constructors fix the two
/// legal modes; the lifecycle string still comes from the shared
/// `SandboxLifecycle::as_str()` (no parallel vocabulary).
fn k8s_finalization_sql(
    sandbox_alias: &str,
    candidate_alias: &str,
    marker_assignments: &str,
    target_node_assignment: SuspendedTargetNodeAssignment,
    lifecycle: SandboxLifecycle,
) -> SuspendedK8sFinalizationSql {
    SuspendedK8sFinalizationSql {
        sandbox_alias: sandbox_alias.to_string(),
        candidate_alias: candidate_alias.to_string(),
        marker_assignments: marker_assignments.to_string(),
        target_node_assignment,
        lifecycle,
    }
}

fn sandbox_assignments(
    sandbox_alias: &str,
    marker_assignments: &str,
    target_node_assignment: SuspendedTargetNodeAssignment,
    lifecycle: SandboxLifecycle,
) -> String {
    let target_node_assignment = match target_node_assignment {
        SuspendedTargetNodeAssignment::Preserve => None,
        SuspendedTargetNodeAssignment::PreserveOrBackfillActive => Some(format!(
            "target_node_id = COALESCE({sandbox_alias}.target_node_id, {sandbox_alias}.active_node_id)"
        )),
        SuspendedTargetNodeAssignment::Clear => Some(
            "target_node_id = NULL,\n                    placement_changed_at = NULL".to_string(),
        ),
    };
    let mut sandbox_assignments = vec![
        format!("lifecycle = '{}'", lifecycle.as_str()),
        "placement = 'unplaced'".to_string(),
        "active_node_id = NULL".to_string(),
        "command_ready_pending = FALSE".to_string(),
    ];
    if let Some(target_node_assignment) = target_node_assignment {
        sandbox_assignments.push(target_node_assignment);
    }
    if !marker_assignments.is_empty() {
        sandbox_assignments.push(marker_assignments.to_string());
    }
    sandbox_assignments.join(",\n                    ")
}

fn active_execution_assignments(execution_alias: &str, lifecycle: SandboxLifecycle) -> String {
    format!(
        "active = FALSE,\n\
                        state = '{}',\n\
                        pod_ip = NULL,\n\
                        terminated_at = COALESCE({execution_alias}.terminated_at, NOW()),\n\
                        updated_at = NOW()",
        lifecycle.as_str()
    )
}

const OPEN_PORT_EXPOSURE_ASSIGNMENTS: &str = "apply_state = 'pending',\n\
                        last_applied_node_id = NULL,\n\
                        last_error = NULL,\n\
                        last_bridge_status = NULL,\n\
                        last_applied_bridge_generation = NULL,\n\
                        last_observed_at = NULL,\n\
                        updated_at = NOW()";

/// Returns a SQL fragment evaluating to TRUE iff the snapshot the markerless
/// restore selector would actually load is a ready, durable full `suspend`
/// snapshot that provably loses no writes.
///
/// Unlike [`current_suspend_origin_marker_snapshot_ready`], this does NOT
/// require the snapshot to match `current_local_artifact_id`. It exists for the
/// stranded `started` rows whose marker points at the pre-restore (source)
/// artifact while a newer suspend snapshot from this cycle was published but
/// never advanced the marker. Recovering those clears the marker so restore
/// picks the newest snapshot — which is sound only when (a) that newest snapshot
/// is itself a suspend artifact and (b) nothing ran after it.
///
/// **Faithfulness to the restore selector.** After the marker is cleared, the
/// markerless suspend-restore selector picks the newest `type='full'
/// state='ready'` snapshot **with no `artifact_origin` filter** (`ORDER BY
/// sn.timestamp DESC LIMIT 1` in `kubernetes/store/restore_source.rs`). So
/// this predicate must vouch for *that* snapshot, not merely for the existence
/// of some current suspend snapshot: it requires a current suspend/null-origin
/// snapshot that no concretely-non-suspend full-ready snapshot ties or beats by
/// timestamp. If a newer (or equal-timestamp) `stop`/`checkpoint`/other-origin
/// full snapshot exists, restore would load that instead, so recovery must
/// refuse.
///
/// The `timestamp >= GREATEST(lifecycle_changed_at, last_command_at,
/// last_access_at)` guard proves the snapshot post-dates this lifecycle cycle and
/// the last recorded command/access, so a markerless restore cannot roll back
/// user state. It deliberately does not anchor on `placement_changed_at`: the
/// reaper cohort that uses this predicate is a leaked auto-suspend projection
/// whose placement clock still names the pre-suspend placement. If that cohort
/// invariant is ever violated, the repaired row may be restore-ineligible, but
/// not restored from stale bytes.
pub fn newest_suspend_snapshot_current(sandbox_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let selection_guard = format!(
        "AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn2\n\
               JOIN snapshot_storage_locations ssl2 ON ssl2.snapshot_id = sn2.id\n\
              WHERE sn2.sandbox_id = {sandbox_alias}.id\n\
                AND ssl2.orphaned_at IS NULL\n\
                AND sn2.type = 'full'\n\
                AND sn2.state = 'ready'\n\
                AND sn2.artifact_origin IS NOT NULL\n\
                AND sn2.artifact_origin <> '{suspend}'\n\
                AND sn2.timestamp >= sn.timestamp\n\
         )"
    );
    newest_suspend_snapshot_current_with_guards(sandbox_alias, "", &selection_guard)
}

fn newest_suspend_snapshot_current_from_active_node(sandbox_alias: &str) -> String {
    let source_guard = format!("AND sn.source_node_id = {sandbox_alias}.active_node_id");
    let selection_guard = format!(
        "AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn2\n\
               JOIN snapshot_storage_locations ssl2 ON ssl2.snapshot_id = sn2.id\n\
              WHERE sn2.sandbox_id = {sandbox_alias}.id\n\
                AND ssl2.orphaned_at IS NULL\n\
                AND sn2.type = 'full'\n\
                AND sn2.state = 'ready'\n\
                AND sn2.id <> sn.id\n\
                AND sn2.timestamp >= sn.timestamp\n\
         )"
    );
    newest_suspend_snapshot_current_with_guards(sandbox_alias, &source_guard, &selection_guard)
}

fn newest_suspend_snapshot_current_with_guards(
    sandbox_alias: &str,
    source_guard: &str,
    selection_guard: &str,
) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND (sn.artifact_origin IS NULL OR sn.artifact_origin = '{suspend}')\n\
                AND sn.timestamp >= GREATEST(\n\
                        {sandbox_alias}.lifecycle_changed_at,\n\
                        COALESCE({sandbox_alias}.last_command_at, {sandbox_alias}.lifecycle_changed_at),\n\
                        COALESCE({sandbox_alias}.last_access_at, {sandbox_alias}.lifecycle_changed_at))\n\
                {source_guard}\n\
                {selection_guard}\n\
         )"
    )
}

/// Returns a SQL fragment evaluating to TRUE iff a stranded suspend still has
/// state that the node-drain redrive can recover.
///
/// For new rows, a non-NULL `current_local_artifact_id` plus cleared
/// `active_node_id` means local publication and source execution retirement
/// completed; backup may not have been enqueued yet, and failing that row would
/// discard the source of the pending exact-artifact backup. A stale artifact
/// identity left from a prior suspend while `active_node_id` is still set is
/// not recoverable evidence for the current suspend. For nullable rollout-skew
/// rows, a current-cycle `creating` or `ready` full snapshot remains the only
/// recoverable evidence. `creating` rows have no storage-location row yet, so
/// the legacy branch deliberately omits that join.
pub fn recoverable_suspend_snapshot_exists(sandbox_alias: &str) -> String {
    let legacy = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.type = 'full'\n\
                AND sn.state IN ('creating', 'ready')\n\
                AND sn.timestamp >= {sandbox_alias}.lifecycle_changed_at\n\
         )"
    );
    format!(
        "(\n\
             (\n\
                 {sandbox_alias}.current_local_artifact_id IS NOT NULL\n\
                 AND {sandbox_alias}.active_node_id IS NULL\n\
             )\n\
             OR ({sandbox_alias}.current_local_artifact_id IS NULL AND {legacy})\n\
         )"
    )
}

/// Returns a boolean SQL fragment that is TRUE iff the sandbox aliased as
/// `sandbox_alias` is durably restorable from S3 — the question that releases
/// the **source node** for drain / terminate. Marker-aware, so the
/// source-release guards (the node-drain `draining → drained` blocker and the
/// `drained_node_terminator` `drained → terminating` guard) stay in lockstep
/// with the cohort-restore predicate:
///
/// * **Evacuation-marked** (`evacuation_restore_target IS NOT NULL`): the
///   *newest* full snapshot must be `ready` ([`latest_full_snapshot_ready`]) —
///   the exact condition the cohort restore waits on. Releasing the source on
///   the older [`fresh_ready_snapshot_exists`] instead would let the node
///   terminate while the current evacuation upload is still `creating`, killing
///   the only in-flight copy and stranding the marked sandbox (the restore
///   refuses the stale prior snapshot). Gating both on the same predicate makes
///   "source safe to release" ⟺ "restore can proceed" hold **by construction**,
///   not by a subtle reachability argument that future placement-timestamp
///   changes could quietly break.
/// * **Unmarked** (plain user-suspended): placement-freshness
///   ([`fresh_ready_snapshot_exists`]) is the right portability question — a
///   `wake → modify → re-suspend` cycle must not release the source on a stale
///   prior-cycle snapshot, and there is no cohort restore to keep in step with.
pub fn snapshot_durable_for_source_release(sandbox_alias: &str) -> String {
    let pinned = snapshot_is_current_local_artifact(sandbox_alias, "sn");
    let no_newer = no_newer_full_snapshot(sandbox_alias, "sn");
    // Keep the sandbox correlation in a single EXISTS so a fleet-wide caller
    // can use a hash semi/anti join. OR-ing complete EXISTS expressions makes
    // Postgres probe snapshots and storage locations separately for each row.
    format!(
        "EXISTS (
             SELECT 1
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE sn.sandbox_id = {sandbox_alias}.id
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND ssl.orphaned_at IS NULL
                AND (
                    {pinned}
                    OR (
                        {sandbox_alias}.current_local_artifact_id IS NULL
                        AND (
                            ({sandbox_alias}.evacuation_restore_target IS NOT NULL AND {no_newer})
                            OR (
                                {sandbox_alias}.evacuation_restore_target IS NULL
                                AND (
                                    {sandbox_alias}.placement_changed_at IS NULL
                                    OR sn.timestamp >= {sandbox_alias}.placement_changed_at
                                )
                            )
                        )
                    )
                )
         )"
    )
}

/// The in-flight gate's per-snapshot test: is this `creating` full snapshot the
/// upload that protects the sandbox's *current* suspend?
///
/// Shared verbatim by all three `inflight_suspend_upload_*_exists` fragments,
/// which differ only in how they scope `sn.source_node_id`. Factored because
/// this is the part that *decides* — the artifact-identity branch for a
/// marker-carrying row, placement-freshness for a rollout-skew one — and a
/// change to either rule has to reach all three at once or the three drift.
/// The scoping conjunct is deliberately *not* factored with it: each caller
/// keeps its own complete `EXISTS`, so no conditional SQL is stitched in and
/// every query site stays readable as one statement.
///
/// The identity branch is [`snapshot_is_current_local_artifact`] rather than a
/// fourth hand-written copy of those columns, so "the current artifact" has one
/// definition across this crate.
fn inflight_snapshot_matches_current_suspend(sandbox_alias: &str) -> String {
    let pinned = snapshot_is_current_local_artifact(sandbox_alias, "sn");
    format!(
        "(\n\
             {pinned}\n\
             OR (\n\
                 {sandbox_alias}.current_local_artifact_id IS NULL\n\
                 AND (\n\
                     {sandbox_alias}.placement_changed_at IS NULL\n\
                     OR sn.timestamp >= {sandbox_alias}.placement_changed_at\n\
                 )\n\
             )\n\
         )"
    )
}

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the sandbox row aliased as `sandbox_alias` has **any** suspend snapshot
/// whose durable upload is still in flight — a `type='full'` snapshot in
/// `state='creating'`, regardless of which node it is uploading from.
///
/// Use this for the **give-up / mark-lost** decision, which is global to the
/// sandbox: marking a sandbox `failed` (or scheduling a redundant reupload)
/// must never race an in-flight upload *anywhere*. If the sandbox's
/// `target_node_id` has drifted to a different node than the one its upload
/// started on, a node-scoped check
/// ([`inflight_suspend_upload_from_node_exists`]) would miss the upload and
/// let the give-up path mark a still-recoverable sandbox `failed`.
///
/// **Gated on state + freshness, not absolute age.** A `creating` row protects
/// the sandbox until the upload worker terminalizes it — `state='ready'`
/// (durable; the sandbox becomes portable and stops blocking) or
/// `state='failed'` (the worker exhausted its retries; the give-up path may
/// then proceed). It is **not** bounded by an arbitrary age cutoff — that would
/// re-introduce the race this gate closes (a slow-but-alive upload past the
/// cutoff marked `failed` while it was still going to finalize). It **is**
/// freshness-gated against `placement_changed_at` exactly like
/// [`fresh_ready_snapshot_exists`]: the live upload's `creating` row is always
/// fresh (its `timestamp` is set at `BeginArtifactBackup`, after the last
/// placement, and no placement event bumps `placement_changed_at` while it is
/// `creating` — suspend leaves the sandbox unplaced and evacuation-restore
/// needs a *ready* snapshot), whereas a stale prior-cycle `creating` remnant
/// (a dead earlier attempt left behind across a later wake/re-place) has
/// `timestamp < placement_changed_at` and is correctly ignored so it can't
/// suppress reupload/give-up or block drain for the current copy.
///
/// The one residual case — a `creating` row whose worker died *and* whose
/// best-effort `FinalizeArtifactBackup(FAILED)` also failed, with no later
/// re-place to make it stale — never terminalizes; it surfaces as a visible
/// stuck-drain (alertable) rather than silent data loss, and is bounded by a
/// dedicated stuck-`creating` reaper (tracked separately).
///
/// For the inverse question — "does terminating *this specific* node strand
/// data?" — use [`inflight_suspend_upload_from_node_exists`], which is
/// node-scoped because terminating a node only strands an upload physically
/// running on it.
///
/// ## Why one `EXISTS` and not one per artifact-identity branch
///
/// The two branches are nested *inside* a single correlated `EXISTS`, the same
/// shape [`snapshot_durable_for_source_release`] uses, rather than composed
/// through [`artifact_identity_or_legacy`] as two sibling `EXISTS`
/// expressions. The two forms are logically identical — the branch key
/// `current_local_artifact_id IS [NOT] NULL` reads only the outer row, so
/// distributing it inside the `EXISTS` preserves truth for every row — but they
/// plan differently, and the sibling form is unusable at production scale.
///
/// A caller that negates this fragment over many rows gets `NOT (A OR B)`,
/// which Postgres cannot flatten into an anti-join; each side becomes a
/// `SubPlan`. The legacy side correlates on `sn.timestamp >=
/// {alias}.placement_changed_at`, an inequality, so unlike the exact side it
/// cannot be turned into a hashed subplan either, and re-executes per outer
/// row. Its own quals (`type='full' AND state='creating'`) then steer the
/// planner to an index on `(state, type)` whose index condition carries no
/// `sandbox_id`, so the correlation lands in a filter and every outer row
/// rescans every `creating` full snapshot in the cluster.
///
/// Written as one `EXISTS`, the planner can reach an anti-join and
/// materialise the creating-snapshot set once.
pub fn inflight_suspend_upload_exists(sandbox_alias: &str) -> String {
    let matches = inflight_snapshot_matches_current_suspend(sandbox_alias);
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
                AND {matches}\n\
         )"
    )
}

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the sandbox row aliased as `sandbox_alias` has a suspend snapshot whose
/// **durable upload is still in flight from `node_placeholder`** — i.e. a
/// `type='full'` snapshot in `state='creating'` whose `source_node_id` is the
/// node in question.
///
/// This is the "don't reclaim the node out from under an in-flight upload"
/// signal. A suspend registers the snapshot via `BeginArtifactBackup`
/// (`state='creating'`, `source_node_id = <source node>`) and only flips it to
/// `state='ready'` via `FinalizeArtifactBackup` **after** the volume manager
/// confirms every artifact is durably stored. Between those two events the
/// snapshot lives only on the source node's disk and in the in-flight upload
/// worker; terminating the node destroys both. Gating drain/terminate on the
/// **absence** of such a row (and excluding such sandboxes from the
/// promote/reupload/mark-lost path) keeps the node alive until the upload
/// finalizes (`ready` → portable, drops out) or fails (`failed` → genuinely
/// lost, reupload/give-up then applies).
///
/// Gated on state + freshness, not absolute age (same rationale as
/// [`inflight_suspend_upload_exists`]): a *fresh* `creating` row (timestamp at
/// or after the sandbox's last placement) blocks until the worker terminalizes
/// it to `ready`/`failed`; a stale prior-cycle remnant is ignored.
///
/// `node_placeholder` is the SQL placeholder (e.g. `"$1"`) bound to the node
/// id in the calling query. `sandbox_alias` correlates the snapshot to the
/// sandbox row so callers can count or filter per sandbox. Safe to compose
/// with `NOT`, `AND`, `OR`.
///
/// **One `EXISTS`, not one per artifact-identity branch** — the shape and its
/// reason are documented on [`inflight_suspend_upload_exists`]. A negating
/// caller over many rows gets `NOT (A OR B)`, which cannot be planned as an
/// anti-join, and the legacy branch's inequality correlation stops it being
/// hashed too, so it re-executes per outer row against an index that carries no
/// `sandbox_id`. This fragment appears twice in the `draining -> drained`
/// blocker count, so the sibling form would rescan every cluster-wide
/// `state='creating'` full snapshot once per pinned row.
pub fn inflight_suspend_upload_from_node_exists(
    sandbox_alias: &str,
    node_placeholder: &str,
) -> String {
    let matches = inflight_snapshot_matches_current_suspend(sandbox_alias);
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.source_node_id = {node_placeholder}\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
                AND {matches}\n\
         )"
    )
}

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the sandbox row aliased as `sandbox_alias` has a suspend snapshot whose
/// durable upload is still in flight from a node **other than**
/// `node_placeholder` — i.e. a `type='full'` snapshot in `state='creating'`
/// whose `source_node_id` differs from the node in question.
///
/// Use this for a give-up / mark-lost decision made because a *specific* node
/// is confirmed destroyed (its EC2 instance is terminated): an upload still
/// `creating` *from that node* can never finalize — the upload worker and its
/// staged copies died with the box — so it must not veto the give-up the way
/// the node-agnostic [`inflight_suspend_upload_exists`] would (that would
/// re-park the sandbox forever on evidence that is already dead). An upload in
/// flight from any **other** node may still finalize `ready` and make the
/// sandbox durable on its own, so it still blocks. `IS DISTINCT FROM` keeps a
/// NULL `source_node_id` blocking: an unattributable upload can't be proven to
/// have died with the node, so the fail-safe direction is to wait for it to
/// terminalize.
///
/// Same state + freshness gating as [`inflight_suspend_upload_exists`] — a
/// `creating` row counts only while placement-fresh, never bounded by an
/// absolute age cutoff; see that function's docs for the rationale.
///
/// **One `EXISTS`, not one per artifact-identity branch** — the shape and its
/// reason are documented on [`inflight_suspend_upload_exists`]. A negating
/// caller over many rows gets `NOT (A OR B)`, which cannot be planned as an
/// anti-join, and the legacy branch's inequality correlation stops it being
/// hashed too, so it re-executes per outer row against an index that carries no
/// `sandbox_id`. This fragment appears twice in the `draining -> drained`
/// blocker count, so the sibling form would rescan every cluster-wide
/// `state='creating'` full snapshot once per pinned row.
pub fn inflight_suspend_upload_from_other_node_exists(
    sandbox_alias: &str,
    node_placeholder: &str,
) -> String {
    let matches = inflight_snapshot_matches_current_suspend(sandbox_alias);
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.source_node_id IS DISTINCT FROM {node_placeholder}\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
                AND {matches}\n\
         )"
    )
}

/// SQL fragment evaluating to TRUE iff the sandbox aliased as `sandbox_alias`
/// is **portable for drain-completion**:
///
/// `{alias}.lifecycle = 'suspended' AND <fresh ready snapshot exists>`.
///
/// Use this at decision points that gate the source node's drain on the
/// absence of local-only snapshots. `suspending` / `restoring` rows are
/// deliberately excluded — their state isn't yet uploadable so they always
/// block drain regardless of any prior snapshot.
pub fn portable_for_drain_completion(sandbox_alias: &str) -> String {
    let snapshot_exists = fresh_ready_snapshot_exists(sandbox_alias);
    format!("{sandbox_alias}.lifecycle = 'suspended' AND {snapshot_exists}")
}

/// SQL fragment evaluating to TRUE iff the sandbox aliased as `sandbox_alias`
/// is **portable for restore-affinity drop**:
///
/// `{alias}.lifecycle IN ('suspended', 'restoring') AND <fresh ready snapshot exists>`.
///
/// Use this at the API drain gate's portable-bypass and when evaluating a
/// portable remote restore fallback. The
/// scope widens beyond `suspended` because `begin_restore_lifecycle` flips
/// the row to `restoring` before pod creation, and the snapshot is still
/// canonical mid-restore. `started` / `paused` are excluded because the live
/// VM memory on the source node is canonical for those lifecycles — a stale
/// prior-cycle snapshot must not release source-node affinity.
pub fn portable_for_restore_affinity(sandbox_alias: &str) -> String {
    let snapshot_exists = fresh_ready_snapshot_exists(sandbox_alias);
    format!("{sandbox_alias}.lifecycle IN ('suspended', 'restoring') AND {snapshot_exists}")
}

/// Boolean SQL fragment: TRUE iff the sandbox row aliased `sandbox_alias`
/// blocks the node named by `node_expr` from completing a drain — the exact
/// `draining → drained` gate the node-drain reconciler counts.
///
/// The drain completes once the node's sandboxes are durably restorable from
/// S3, **not** once they are restored onto the target cohort: the data is
/// durable in S3, so the source node is free to terminate. The cohort restore
/// is decoupled, driven by the cluster-wide restore reconciler.
///
/// Three target-pinned states block, and the reasons differ:
///
/// - `suspending` — the suspend RPC is mid-flight, so the local artifact is not
///   yet published.
/// - `restoring` **only for non-evacuation restores**
///   (`evacuation_restore_target IS NULL`). An evacuation restore reads from the
///   durable S3 backup and briefly sits in `restoring` with `target_node_id`
///   still equal to the source, so counting it would re-couple the source drain
///   to restore completion.
/// - `suspended` whose newest full snapshot is not yet durably restorable, via
///   the marker-aware [`snapshot_durable_for_source_release`] — the same
///   predicate the cohort restore waits on, so the source cannot terminate
///   while the current evacuation backup is still `creating`.
///
/// Plus any sandbox whose suspend-snapshot upload is still in flight *from*
/// this node: terminating mid-upload kills the worker holding the only durable
/// copy.
///
/// Note that two of these states pin through `target_node_id` while
/// `active_node_id` may be **NULL** — an in-flight user restore of a suspended
/// sandbox is the common case. A caller that counts residency through
/// `active_node_id` alone therefore sees none of them, and would report a node
/// as empty while its drain gate is held open. That asymmetry is why this
/// predicate is shared rather than restated: the reconciler that enforces the
/// gate and any tool that predicts it must count the same rows.
///
/// `node_expr` is the SQL expression for the node — a bound placeholder
/// (`"$1"`) or a correlated column from an enclosing row (`"scope.node_id"`).
///
/// ## No longer executed — this is the reference definition
///
/// Nothing in production runs this form any more. Both gates execute
/// [`drain_gate_blockers_for_node`], because this predicate's disjuncts
/// attribute to different columns and so give the planner no indexable driver
/// over `sandboxes`, forcing a full table scan for each node.
///
/// It is deliberately kept: this is where the gate's *meaning* is written down
/// — `pplx_space_common_svc`'s residency docs point here for it — and it is the
/// oracle the executed forms are differentially tested against in
/// `drain_gate_differential_tests`. Neither of those needs `pub`, since that
/// test is in this crate; what does is `pplx-space-api`'s
/// `live_residents_tests`, which uses it cross-crate as the same oracle. So it
/// stays the definition of the question; it is just not how the question gets
/// asked. Reach for [`drain_gate_blockers_for_node`] instead, always: this
/// form's access path does not survive a production-sized `sandboxes`.
pub fn drain_gate_blocks_source_release(sandbox_alias: &str, node_expr: &str) -> String {
    let target_pinned = drain_gate_target_pinned_blocks(sandbox_alias, node_expr);
    let durable = snapshot_durable_for_source_release(sandbox_alias);
    let inflight_upload = inflight_suspend_upload_from_node_exists(sandbox_alias, node_expr);
    format!(
        "(\n\
             {target_pinned}\n\
             OR ({inflight_upload} AND NOT {durable})\n\
         )"
    )
}

/// The `target_node_id`-attributed half of [`drain_gate_blocks_source_release`]:
/// the mid-flight and not-yet-portable lifecycles pinned to the node, without
/// the in-flight-upload disjunct.
///
/// Split out for [`drain_gate_blocker_pairs`], whose first branch needs exactly
/// this half. It is not a second definition of the gate — the authoritative
/// predicate above is built from it — and it must not be used as one: on its own
/// it does not hold a node whose only blocker is an upload in flight from it,
/// which is the case that strands the sole durable copy. Use it only where a
/// sibling branch supplies that attribution.
fn drain_gate_target_pinned_blocks(sandbox_alias: &str, node_expr: &str) -> String {
    let durable = snapshot_durable_for_source_release(sandbox_alias);
    format!(
        "(\n\
             {sandbox_alias}.target_node_id = {node_expr}\n\
             AND (\n\
                 {sandbox_alias}.lifecycle = 'suspending'\n\
                 OR ({sandbox_alias}.lifecycle = 'restoring'\n\
                     AND {sandbox_alias}.evacuation_restore_target IS NULL)\n\
                 OR ({sandbox_alias}.lifecycle = 'suspended' AND NOT {durable})\n\
             )\n\
         )"
    )
}

/// Batched counterpart of [`drain_gate_blocks_source_release`]: a complete
/// `SELECT` yielding one `(node_id, sandbox_id)` pair per sandbox that blocks a
/// node's drain gate, for an entire set of nodes in one pass.
///
/// The single-node predicate cannot be reused per node at fleet scale. Its two
/// disjuncts are attributed to *different* columns — one to the sandbox's
/// `target_node_id`, one to a snapshot's `source_node_id` — so an `OR` over
/// both leaves the planner no indexable driver on `sandboxes`, and a correlated
/// evaluation degrades to a full scan of that table per node. This form drives
/// each attribution from its own indexable column instead, so the work is
/// proportional to the blocking rows rather than to nodes × sandboxes.
///
/// Equivalence with the single-node form is a property, not a coincidence, and
/// is what this module's `drain_gate_differential_tests` assert against a live
/// Postgres:
///
/// * The first branch applies [`drain_gate_target_pinned_blocks`] with the node
///   expression bound to `s.target_node_id`, so the lifecycle vocabulary and the
///   durability rules are never restated here — only pre-filtered by an
///   indexable equality so the planner has a driver. It carries the
///   `target_node_id`-attributed half only, because the second branch already
///   covers every in-flight attribution: branch 1's in-flight disjunct could
///   fire only for a sandbox with a `creating` full snapshot whose
///   `source_node_id` equals its own `target_node_id`, and that row is exactly a
///   branch-2 driver. Shedding it is not an optimisation of a cheap conjunct —
///   the disjunct puts the in-flight `EXISTS` inside a top-level `OR`, where
///   Postgres cannot flatten it to a join, so it becomes a per-row subplan
///   steered onto an index whose condition carries no `sandbox_id`. Removing
///   the redundant disjunct avoids that repeated cluster-wide scan.
/// * The second branch covers the one case the first cannot see: an upload still
///   in flight from a node that is *not* the sandbox's current target. It drives
///   off `snapshots` (`state = 'creating'` is selective and indexed) and then
///   re-applies the shared in-flight fragment for the artifact-identity and
///   freshness nuances.
/// * A sandbox whose in-flight source *is* its target satisfies both branches;
///   `UNION` (not `UNION ALL`) collapses the duplicate, matching the single-node
///   form's `COUNT(*)` over one `sandboxes` scan, which counts such a row once.
///
/// `node_ids_param` is the placeholder bound to a `UUID[]` of the nodes in
/// question (e.g. `"$1::UUID[]"`).
pub fn drain_gate_blocker_pairs(node_ids_param: &str) -> String {
    let target_pinned = drain_gate_target_pinned_blocks("s", "s.target_node_id");
    // `driver` rather than `sn`: the shared in-flight fragment uses `sn` for its
    // own subquery, so an outer `sn` would be shadowed and its node comparison
    // would collapse to a tautology.
    let inflight_from_driver =
        inflight_suspend_upload_from_node_exists("s", "driver.source_node_id");
    let durable = snapshot_durable_for_source_release("s");
    format!(
        "SELECT s.target_node_id AS node_id, s.id AS sandbox_id\n\
           FROM sandboxes s\n\
          WHERE s.target_node_id = ANY({node_ids_param})\n\
            AND {target_pinned}\n\
         UNION\n\
         SELECT driver.source_node_id AS node_id, s.id AS sandbox_id\n\
           FROM snapshots driver\n\
           JOIN sandboxes s ON s.id = driver.sandbox_id\n\
          WHERE driver.source_node_id = ANY({node_ids_param})\n\
            AND driver.type = 'full'\n\
            AND driver.state = 'creating'\n\
            AND {inflight_from_driver}\n\
            AND NOT {durable}"
    )
}

/// Complete `SELECT` of the sandbox ids blocking **one** node's drain gate.
///
/// Same rows as [`drain_gate_blocks_source_release`] evaluated for that node,
/// but reached through [`drain_gate_blocker_pairs`] so each of the gate's two
/// attributions drives off its own indexable column. Use this anywhere a
/// statement needs one node's blockers — as a `COUNT(*)` source, or inside a
/// `NOT EXISTS` guard — and never the single-node predicate over a bare
/// `sandboxes` scan.
///
/// The predicate itself cannot drive such a scan: its disjuncts attribute to
/// `sandboxes.target_node_id` and to `snapshots.source_node_id`, so an `OR`
/// over both leaves the planner nothing indexable and it reads the whole table.
/// This form keeps the node filter visible to the planner so it can avoid
/// a cluster-wide lifecycle bitmap.
///
/// `node_param` is the placeholder bound to a single node id (e.g. `"$1"`). The
/// scoping is stated twice — once to pre-filter each branch, once as an outer
/// `WHERE` — so a reader of the outer statement can see that the result is one
/// node's. It is the same placeholder in both places, so it re-states the
/// scoping rather than checking it: it cannot catch a caller that binds the
/// wrong node.
pub fn drain_gate_blockers_for_node(node_param: &str) -> String {
    let pairs = drain_gate_blocker_pairs(&format!("ARRAY[{node_param}]::UUID[]"));
    format!(
        "SELECT gate_blockers.sandbox_id
           FROM (
         {pairs}
               ) gate_blockers
          WHERE gate_blockers.node_id = {node_param}"
    )
}

#[cfg(test)]
mod drain_gate_differential_tests;
#[cfg(test)]
mod inflight_upload_shape_differential_tests;
#[cfg(test)]
mod queued_artifact_recoverable_tests;

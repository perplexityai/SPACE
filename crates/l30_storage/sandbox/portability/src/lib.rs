//! Shared SQL fragments for the sandbox "is this snapshot portable?" decision,
//! plus [`execution_supersession`], the Rust predicate `BeginArtifactBackup`'s
//! generation fence and the orphaned-restore-pod GC both decide with.
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
//! The same module answers a related question for absence-driven deletion:
//! [`stopped_artifact_durable`] keeps the internal API from deleting a stopped
//! sandbox, released from its node once its pinned artifact was durable, when
//! that node stops reporting it.
//!
//! ## A third question: is *this row* the pinned artifact?
//!
//! [`snapshot_is_current_local_artifact`] answers the per-snapshot inverse of
//! the marker branch above, and its consumers are **deletion** paths rather
//! than portability decisions — the snapshot-retention sweeper uses it to
//! refuse to prune a sandbox's tracked restore target. It lives here for the
//! same anti-drift reason: the sweep's "never delete this" and the restore
//! selector's "this is the one to load" must be the same predicate, or the
//! sweep can delete exactly what restore would have selected. Its consumers
//! include:
//!
//! 1. [`matching_ready_backup_exists`], in this file.
//! 2. [`snapshot_is_live_restore_source`], in this file — the deletion guard.
//! 3. The snapshot-retention store's marked-restore-breach reporting query,
//!    which composes the fragment directly rather than through (2).
//! 4. The shared restore-source selector
//!    (`pplx_space_sandbox_store::restore_source::select_restore_source`),
//!    which every substrate's suspend restore and the node's
//!    `GetSuspendSnapshotLocation` lookup call.
//! 5. The Kubernetes substrate's disk-only sibling, for an evacuated `stopped`
//!    sandbox. That one exists because (4) filters `sn.type = 'full'`; both
//!    resolve their source through this fragment, so one guard covers both.
//!
//! The drain, occupancy and placement reads and the admin runtime population
//! build from it too; `grep` for the rest rather than trusting a count.
//! Narrowing the fragment therefore touches *reporting* queries and
//! portability probes, not just the selectors and the guard.
//!
//! It is not yet the *only* spelling of that pair. Four other fragments in this
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
//!
//! ## Capture generation
//!
//! `timestamp` orders *registrations*, not captures. A re-upload of an older
//! capture (the drain reupload queue, a disk eviction's upload before delete)
//! registers a fresh attempt row whose `timestamp` is later than every
//! capture taken since. `snapshots.generation` is the generation of the
//! execution that captured the bytes; every backup row of one artifact
//! that records one records the same one (`pplx_space_snapshot_store::insert`
//! says how it's recorded). Executions' generations only grow, so it orders captures taken
//! by different executions whatever the order they registered in. Captures
//! one execution took record the same generation, and among them the
//! captures' first registrations decide.
//!
//! The "is this the newest full snapshot?" fragments
//! ([`no_newer_full_snapshot`], [`no_newer_full_snapshot_ignoring_failed_retry`]
//! and [`restore_retry_source_snapshot_is_current`]) and the restore-source
//! selector (`pplx_space_sandbox_store::restore_source::select_restore_source`)
//! rank by it first ([`capture_rank`]). Between rows of one rank they order
//! by the capture's first registration ([`capture_registered_at`]), not the
//! row's own, so a re-upload of a capture's artifact is never newer than the
//! capture. A pinned row restores its pinned artifact whatever the
//! generations say: the pin names the bytes exactly.
//!
//! A row that records no generation, from before it was recorded or from a
//! capture the node couldn't vouch for, ranks below every capture with a
//! generation only when a later execution's capture provably follows it: the
//! execution that took a capture with a generation started
//! (`sandbox_k8s_executions.created_at`) after this capture first registered,
//! so this capture was taken before that execution existed. Both clocks are
//! the control plane's. A first registration can be pruned, or can come long
//! after its capture, so the registration order of the two captures alone
//! proves nothing. A Direct sandbox keeps no execution history; there a
//! later-registered capture with a higher generation than every capture
//! registered before this one still ranks it below. In a history where no
//! capture has a generation, the rows keep registration order. A capture without a generation registered after one with a
//! generation, with no later execution's capture after it, can't be ordered
//! against it: it may be a re-upload of an older capture the
//! node couldn't vouch for. That history is unordered
//! ([`restore_order_unordered`]): the restore refuses it explicitly rather
//! than guess, unless the row is pinned.
//!
//! Comparisons of a snapshot against a sandbox clock (`placement_changed_at`,
//! `lifecycle_changed_at`, the activity clocks) stay on `timestamp`, since a
//! generation can't be compared with a wall-clock bound.

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
    matching_ready_backup_of_type_exists(sandbox_alias, "full")
}

/// The kind-parameterized body behind [`matching_ready_backup_exists`] and
/// [`current_disk_artifact_backup_ready`].
///
/// Only `snapshots.type` differs between the suspend path's durability proof
/// and the stopped-evacuation path's, and both are the predicate that
/// authorizes releasing a node — one after a memory snapshot, one after a disk
/// artifact. Keeping them as one body means a correction to the shared shape
/// (an extra `ssl` condition, a change to how orphaned locations count) cannot
/// land on one and miss the other, which two visually identical copies invite:
/// a diff touching either would read as complete.
///
/// `snapshot_type` is an in-crate literal, never caller data.
fn matching_ready_backup_of_type_exists(sandbox_alias: &str, snapshot_type: &str) -> String {
    let pinned = snapshot_is_current_local_artifact(sandbox_alias, "sn");
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND {pinned}\n\
                AND sn.type = '{snapshot_type}'\n\
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

/// Returns a self-contained boolean SQL fragment: the sandbox aliased as
/// `sandbox_alias` has a durable copy a user restore on its own node would
/// load. A pinned row needs a ready backup of its pinned artifact. An
/// unpinned row needs a ready full snapshot with a live location that
/// registered at or after the last placement, and that no full snapshot in
/// any state outranks by capture generation: the restore loads the latest
/// capture, and a re-upload of an older one registered since the placement
/// isn't it, nor is one of the previous capture while the latest's upload is
/// in flight or failed. An unordered history ([`restore_order_unordered`])
/// has none: its restore refuses.
pub fn fresh_ready_snapshot_exists(sandbox_alias: &str) -> String {
    let exact = matching_ready_backup_exists(sandbox_alias);
    let captured = capture_registered_at("sn");
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, "sn");
    let unordered = restore_order_unordered(sandbox_alias);
    let legacy = format!(
        "(NOT {unordered} AND EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND (\n\
                    {sandbox_alias}.placement_changed_at IS NULL\n\
                    OR {captured} >= {sandbox_alias}.placement_changed_at\n\
                )\n\
                AND NOT {outranked}\n\
         ))"
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
    let durable = snapshot_has_durable_copy("ready_snapshot");
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots ready_snapshot\n\
              WHERE ready_snapshot.sandbox_id = {sandbox_id}\n\
                AND {matches_artifact}\n\
                AND {durable}\n\
         )"
    )
}

/// Returns a boolean SQL fragment evaluating to TRUE iff the snapshot aliased
/// as `snapshot_alias` has a durable copy: it is `state='ready'` and has a
/// storage location that has not been orphaned.
///
/// The per-snapshot half of [`artifact_has_durable_copy`], which asks it of
/// any snapshot of one artifact. Compose it into a query that already has the
/// `snapshots` row; it is never NULL.
pub fn snapshot_has_durable_copy(snapshot_alias: &str) -> String {
    format!(
        "(\n\
             {snapshot_alias}.state = 'ready'\n\
             AND EXISTS (\n\
                 SELECT 1\n\
                   FROM snapshot_storage_locations durable_storage\n\
                  WHERE durable_storage.snapshot_id = {snapshot_alias}.id\n\
                    AND durable_storage.orphaned_at IS NULL\n\
             )\n\
         )"
    )
}

/// Returns a boolean SQL fragment evaluating to TRUE iff the sandbox aliased
/// as `sandbox_alias` is still attached to the node named by `node_id`.
///
/// Attachment is `active_node_id` **or** `target_node_id`, which is what the
/// node drain already means by "attached to this node" (the
/// `(active_node_id = $1 OR target_node_id = $1)` of
/// `pplx_space_sandbox_store::drain::abandon_stale_resident_sandboxes_query`
/// is the executable one; the cluster controller's
/// `NodeDrainSink::abandon_stale_resident_sandboxes` docs restate it in
/// prose). That spells it with `=`; this uses `IS NOT DISTINCT FROM`, which
/// agrees for every non-NULL `node_id` and additionally gives a total answer
/// rather than NULL, so a caller may safely negate it.
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
/// counted into
/// `pplx_space_internal_api_server::metrics::internal_api::local_backup_queue_absent_not_durable`
/// until tombstone retention hard-deletes the `sandboxes` row and the foreign
/// key cascades.
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
///   by where the marker can go next. The node-published suspended
///   finalization (`pplx_space_sandbox_store::ingest::finalize_suspended`)
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
/// `crates/l40_api/api/src/controllers/substrate/direct.rs`:
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
/// has to cover **every** shape that selector can take, because which one
/// applies depends on the row's lifecycle and pin:
///
/// 1. A marked row restores by exact artifact identity
///    ([`snapshot_is_current_local_artifact`]).
/// 2. A row already in `restoring` retries from the pre-restore source window
///    ([`restore_retry_source_snapshot_window`]) instead, deliberately
///    ignoring the marker — a failed restore branch can publish a *later*
///    artifact after the row entered `restoring`, and the retry must load the
///    state restore originally started from, not that later one.
///
/// 3. An unpinned row restores its latest capture by [`capture_rank`], then
///    by the capture's first registration ([`capture_registered_at`]), while
///    the retention rank counts each row's own registration. That's a
///    `suspended` or `restoring` row, and also a `started` one whose runtime
///    a node recovers through `GetSuspendSnapshotLocation`. Otherwise `keep_n`
///    later re-uploads of older captures would push the selector's pick out
///    of the keep-set, and once it was pruned restore would load older bytes.
///    So every full snapshot of such a row that no candidate the selector
///    could load (a `ready` full snapshot with a live location) is newer
///    than is shielded too: a higher generation than its rank, or the same
///    rank and a later first registration, a tie broken by registration and
///    then id as the selector breaks it. That's the selector's possible
///    picks and no more: every backup of the latest capture, and, while the
///    history is unordered ([`restore_order_unordered`]), the latest capture
///    of each side, which an operator chooses between. An outranked capture,
///    with a generation or without, prunes by rank. A failed or unbacked upload of a later
///    capture doesn't lift the shield: the selector won't load the capture
///    while that row outranks it, and nothing removes a failed row, so the
///    shield can keep a capture no selector loads until a later capture or a
///    backup of the failed one lands. Unlike the `restoring` arm, this one
///    isn't bounded by a transient lifecycle: it holds in every lifecycle
///    while the row stays unpinned, and a suspend that publishes its artifact
///    pins the row and ends it.
///
/// 4. While the history is unordered ([`restore_order_unordered`]), every
///    full snapshot registered at or before a capture that makes it so: one
///    without a generation and without a rank ([`capture_rank`]). Pruning one
///    of them could turn the refused history into a chosen one. It holds
///    pinned rows too, since a pin can be cleared later. Rows registered
///    after it prune by rank, since pruning them can only leave the history
///    more ambiguous, which refuses. It lifts once a later execution's
///    capture ranks that capture below it. In an ordered history, pinned or
///    not, it holds nothing.
///
/// 5. The first backup of an artifact that has later backups. It anchors
///    the capture's first registration ([`capture_registered_at`]): pruned,
///    a re-upload of an older capture would take its place in the order,
///    registered late, and could outrank the latest capture. It's held only
///    while another backup of the artifact remains, and only while no
///    restorable capture outranks it: then its registration can't decide
///    anything. Except for a capture without a generation that ranks 0
///    ([`capture_rank`]): its rank rests on its first registration preceding
///    a later execution's start, and pruned, its registration could move past
///    that start and make the history unordered, so it's held while it ranks
///    0. So is the artifact's first backup that records a generation while
///    one that records none remains: that one takes its rank from it.
///
/// Covering only (1) leaves a real hole: those later failed-branch artifacts
/// still count toward the retention rank, so once `keep_n` of them pile up,
/// the retry's actual source falls outside the keep-set and becomes
/// prune-eligible while the sandbox is still trying to restore from it.
///
/// The `restoring` arm is deliberately wider than the selector: the selector
/// takes the newest row in the window, this shields every row in it, and
/// every later backup of an artifact in it, which a node's
/// `GetSuspendSnapshotLocation` can restore when the claim's source was
/// re-uploaded after the claim.
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
/// from it (the latest capture), so both shield more than strictly
/// necessary. That is deliberate.
///
/// For the marker arm, narrowing is not even expressible here:
/// [`snapshot_is_current_local_artifact`] has many consumers (see this
/// module's docs) and "newest matching" is meaningful only for the
/// selectors. The portability check asks whether *any* matching ready backup
/// exists; the retention store's reporting query asks whether one specific row
/// is the marked one; and each selector's branch is a filter it then orders and
/// limits itself. Narrowing the shared fragment would break all of them; adding
/// a second near-identical fragment would reintroduce exactly the drift this
/// file exists to prevent.
///
/// Narrowing this safety guard is dangerous: a guard that re-derived "the row
/// the selector picks" could shield a different row than restore actually
/// loads, making the sandbox unrestorable, and both selectors, the suspend
/// one and its disk-only sibling, would have to be re-derived exactly.
pub fn snapshot_is_live_restore_source(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let marked = snapshot_is_current_local_artifact(sandbox_alias, snapshot_alias);
    let retry_window = restore_retry_source_snapshot_window(sandbox_alias, snapshot_alias);
    let source_window = restore_retry_source_snapshot_window(sandbox_alias, "source");
    let restoring = SandboxLifecycle::Restoring.as_str();
    let rank = capture_rank(snapshot_alias);
    let captured = capture_registered_at(snapshot_alias);
    let shadow_rank = capture_rank("shadow");
    let shadow_captured = capture_registered_at("shadow");
    let frozen_rank = capture_rank("frozen_by");
    let suspend = ArtifactOrigin::Suspend.as_str();
    format!(
        "(\n\
             {marked}\n\
             OR ({snapshot_alias}.type = 'full'\n\
                 AND EXISTS (\n\
                     SELECT 1\n\
                       FROM snapshots frozen_era\n\
                      WHERE frozen_era.sandbox_id = {sandbox_alias}.id\n\
                        AND frozen_era.type = 'full'\n\
                        AND frozen_era.generation IS NOT NULL\n\
                 )\n\
                 AND EXISTS (\n\
                     SELECT 1\n\
                       FROM snapshots frozen_by\n\
                      WHERE frozen_by.sandbox_id = {sandbox_alias}.id\n\
                        AND frozen_by.type = 'full'\n\
                        AND frozen_by.generation IS NULL\n\
                        AND frozen_by.timestamp >= {snapshot_alias}.timestamp\n\
                        AND {frozen_rank} IS NULL\n\
                 ))\n\
             OR ({snapshot_alias}.type = 'full'\n\
                 AND {snapshot_alias}.local_artifact_id IS NOT NULL\n\
                 AND NOT EXISTS (\n\
                     SELECT 1\n\
                       FROM snapshots anchor\n\
                      WHERE anchor.sandbox_id = {sandbox_alias}.id\n\
                        AND anchor.type = 'full'\n\
                        AND anchor.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                        AND anchor.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                        AND (anchor.timestamp, anchor.id) < ({snapshot_alias}.timestamp, {snapshot_alias}.id)\n\
                 )\n\
                 AND EXISTS (\n\
                     SELECT 1\n\
                       FROM snapshots anchor\n\
                      WHERE anchor.sandbox_id = {sandbox_alias}.id\n\
                        AND anchor.type = 'full'\n\
                        AND anchor.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                        AND anchor.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                        AND anchor.id <> {snapshot_alias}.id\n\
                 )\n\
                 AND (\n\
                     COALESCE({rank} = 0, FALSE)\n\
                     OR NOT EXISTS (\n\
                         SELECT 1\n\
                           FROM snapshots shadow\n\
                           JOIN snapshot_storage_locations shadow_location\n\
                             ON shadow_location.snapshot_id = shadow.id\n\
                            AND shadow_location.orphaned_at IS NULL\n\
                          WHERE shadow.sandbox_id = {sandbox_alias}.id\n\
                            AND shadow.type = 'full'\n\
                            AND shadow.state = 'ready'\n\
                            AND shadow.generation IS NOT NULL\n\
                            AND shadow.generation > {rank}\n\
                     )))\n\
             OR ({snapshot_alias}.type = 'full'\n\
                 AND {snapshot_alias}.local_artifact_id IS NOT NULL\n\
                 AND {snapshot_alias}.generation IS NOT NULL\n\
                 AND NOT EXISTS (\n\
                     SELECT 1\n\
                       FROM snapshots anchor\n\
                      WHERE anchor.sandbox_id = {sandbox_alias}.id\n\
                        AND anchor.type = 'full'\n\
                        AND anchor.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                        AND anchor.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                        AND anchor.generation IS NOT NULL\n\
                        AND (anchor.timestamp, anchor.id) < ({snapshot_alias}.timestamp, {snapshot_alias}.id)\n\
                 )\n\
                 AND EXISTS (\n\
                     SELECT 1\n\
                       FROM snapshots anchor\n\
                      WHERE anchor.sandbox_id = {sandbox_alias}.id\n\
                        AND anchor.type = 'full'\n\
                        AND anchor.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                        AND anchor.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                        AND anchor.generation IS NULL\n\
                 ))\n\
             OR ({sandbox_alias}.lifecycle = '{restoring}'\n\
                 AND ({retry_window}\n\
                      OR ({snapshot_alias}.local_artifact_id IS NOT NULL\n\
                          AND EXISTS (\n\
                              SELECT 1\n\
                                FROM snapshots source\n\
                               WHERE source.sandbox_id = {sandbox_alias}.id\n\
                                 AND source.type = 'full'\n\
                                 AND source.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                                 AND source.artifact_kind = {snapshot_alias}.artifact_kind\n\
                                 AND {source_window}\n\
                          ))))\n\
             OR ({sandbox_alias}.current_local_artifact_id IS NULL\n\
                 AND {snapshot_alias}.type = 'full'\n\
                 AND NOT EXISTS (\n\
                     SELECT 1\n\
                       FROM snapshots shadow\n\
                       JOIN snapshot_storage_locations shadow_location\n\
                         ON shadow_location.snapshot_id = shadow.id\n\
                        AND shadow_location.orphaned_at IS NULL\n\
                      WHERE shadow.sandbox_id = {sandbox_alias}.id\n\
                        AND shadow.type = 'full'\n\
                        AND shadow.state = 'ready'\n\
                        AND COALESCE(shadow.artifact_origin, '{suspend}') = '{suspend}'\n\
                        AND (shadow.generation > {rank}\n\
                             OR ({shadow_rank} IS NOT DISTINCT FROM {rank}\n\
                                 AND ({shadow_captured} > {captured}\n\
                                      OR ({shadow_captured} = {captured}\n\
                                          AND NOT ({snapshot_alias}.local_artifact_id IS NOT NULL\n\
                                                   AND shadow.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                                                   AND shadow.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind)\n\
                                          AND (shadow.timestamp, shadow.id)\n\
                                              > ({snapshot_alias}.timestamp, {snapshot_alias}.id)))))\n\
                 ))\n\
         )"
    )
}

/// Returns a boolean SQL fragment that is TRUE iff the snapshot aliased as
/// `snapshot_alias` is the **newest** `type='full'` snapshot for the sandbox
/// aliased as `sandbox_alias`: no full snapshot of any state was captured
/// after it (see "Newest by capture, not by registration" below). Compose it
/// into a query that already joins a `snapshots` row.
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
///
/// ## Newest by capture, not by registration
///
/// "Newer" follows the capture rank first (see "Capture generation" in the
/// module docs): the row must not be outranked by a full snapshot with a
/// higher `generation` than its [`capture_rank`], nor have its capture
/// registered before the capture of a full snapshot nothing outranks. Rows
/// with no rank outrank nothing and are outranked by nothing, so among them,
/// and among rows of one rank, the captures' first registrations decide. A
/// re-upload of an old capture registers last, but its capture registered
/// with its first backup, and its inherited generation is outranked, so it
/// can't make the latest capture look stale.
///
/// The two clauses are deliberately not a pairwise "rank, else timestamp"
/// comparison: that order isn't transitive once ranked and unranked rows
/// mix. Generation 5 at t1, an unranked row at t2 and a generation 3
/// re-upload at t3 form a cycle (t2 beats t1, t3 beats t2, 5 beats 3) in
/// which every row has a newer one, so no row would ever be newest. Here the
/// unranked row is: nothing outranks it, and the only row registered after
/// it is outranked. Such a history is unordered
/// ([`restore_order_unordered`]), and the callers that restore refuse it.
pub fn no_newer_full_snapshot(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_generation(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
         ))"
    )
}

/// [`no_newer_full_snapshot`] among suspend captures: a newer full snapshot of
/// another origin, a named snapshot's or a stop's backup, doesn't make the
/// snapshot aliased as `snapshot_alias` stale unless it outranks it by capture
/// generation, since a later registration of the same execution's named
/// snapshot was taken before the suspend. For the restore selector, which
/// restores an unpinned row only from suspend captures.
pub fn no_newer_suspend_capture(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_generation(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND COALESCE(newer.artifact_origin, '{suspend}') = '{suspend}'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
         ))"
    )
}

/// [`full_snapshot_outranked_by_generation`], counting only outranking rows
/// that haven't failed: a capture that is uploading or ready. A failed upload
/// won't land, and nothing retries one for an unpinned row on a gentle drain,
/// so a hold on it would never end.
pub fn full_snapshot_outranked_by_live_capture(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let rank = capture_rank(snapshot_alias);
    full_snapshot_outranked_where(sandbox_alias, &rank, "dom.state <> 'failed'")
}

/// [`no_newer_suspend_capture`], counting only later captures that haven't
/// failed ([`full_snapshot_outranked_by_live_capture`]). An upload in flight
/// ends: the node's worker finalizes it, or the stuck-snapshot reaper fails
/// it. So a hold on one is bounded.
pub fn no_newer_live_suspend_capture(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let outranked = full_snapshot_outranked_by_live_capture(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_live_capture(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND newer.state <> 'failed'\n\
                AND COALESCE(newer.artifact_origin, '{suspend}') = '{suspend}'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
         ))"
    )
}

/// Returns a timestamp SQL expression: when the capture of the snapshot
/// aliased as `snapshot_alias` first registered. That's the earliest
/// registration of any backup row of its artifact (`local_artifact_id`,
/// `artifact_kind`), in any state, or the row's own for a row with no
/// artifact id. A re-upload of a capture registers late, but its capture
/// registered when its first backup did. Never NULL.
pub fn capture_registered_at(snapshot_alias: &str) -> String {
    format!(
        "COALESCE((\n\
             SELECT MIN(first_backup.timestamp)\n\
               FROM snapshots first_backup\n\
              WHERE first_backup.sandbox_id = {snapshot_alias}.sandbox_id\n\
                AND first_backup.type = {snapshot_alias}.type\n\
                AND first_backup.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                AND first_backup.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
         ), {snapshot_alias}.timestamp)"
    )
}

/// Returns a `BIGINT` SQL expression: the capture rank of the full snapshot
/// aliased as `snapshot_alias`. A row with a generation ranks by its
/// `generation`, and so does a backup without one of an artifact another
/// backup records one for: the same capture. Any other row without a
/// generation ranks 0, below every generation, when a full snapshot of the
/// sandbox with a generation, in any state, is a later execution's capture,
/// taken after this one:
///
/// - the sandbox's execution of that generation started after this capture
///   first registered, so it couldn't have taken a capture before this one.
///   Neither a pruned first backup nor a capture first uploaded long after
///   it was taken can fake that;
/// - or, when the sandbox has no execution of that generation (Direct keeps
///   no execution history), that capture first registered after this one did
///   and records a higher generation than every capture with a generation
///   first registered before it.
///
/// Otherwise NULL: in a history where no capture has a generation, and for a
/// capture without one that no such capture follows
/// ([`restore_order_unordered`]).
pub fn capture_rank(snapshot_alias: &str) -> String {
    capture_rank_where(snapshot_alias, "TRUE")
}

/// [`capture_rank`] for a restore retry, as the retry's pre-claim source
/// window ([`restore_retry_source_snapshot_window`]) sees it: only a later
/// execution's capture in the window can rank a capture without a generation
/// below the ones with one. A capture the failed branch registered after the claim says
/// nothing about the order of the captures the retry chooses between.
pub fn capture_rank_in_retry_window(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let window = restore_retry_source_snapshot_window(sandbox_alias, "rank_later");
    capture_rank_where(snapshot_alias, &window)
}

fn capture_rank_where(snapshot_alias: &str, later_condition: &str) -> String {
    let later = capture_registered_at("rank_later");
    let before = capture_registered_at("rank_before");
    let this = capture_registered_at(snapshot_alias);
    format!(
        "COALESCE({snapshot_alias}.generation, (\n\
             SELECT MIN(same_artifact.generation)\n\
               FROM snapshots same_artifact\n\
              WHERE same_artifact.sandbox_id = {snapshot_alias}.sandbox_id\n\
                AND same_artifact.type = {snapshot_alias}.type\n\
                AND same_artifact.local_artifact_id = {snapshot_alias}.local_artifact_id\n\
                AND same_artifact.artifact_kind IS NOT DISTINCT FROM {snapshot_alias}.artifact_kind\n\
                AND same_artifact.generation IS NOT NULL\n\
         ), CASE WHEN EXISTS (\n\
             SELECT 1\n\
               FROM snapshots rank_later\n\
              WHERE rank_later.sandbox_id = {snapshot_alias}.sandbox_id\n\
                AND rank_later.type = 'full'\n\
                AND rank_later.generation IS NOT NULL\n\
                AND {later_condition}\n\
                AND CASE WHEN EXISTS (\n\
                         SELECT 1\n\
                           FROM sandbox_k8s_executions rank_execution\n\
                          WHERE rank_execution.sandbox_id = {snapshot_alias}.sandbox_id\n\
                            AND rank_execution.generation = rank_later.generation\n\
                     )\n\
                     THEN NOT EXISTS (\n\
                         SELECT 1\n\
                           FROM sandbox_k8s_executions rank_execution\n\
                          WHERE rank_execution.sandbox_id = {snapshot_alias}.sandbox_id\n\
                            AND rank_execution.generation = rank_later.generation\n\
                            AND rank_execution.created_at <= {this}\n\
                     )\n\
                     ELSE {later} > {this}\n\
                          AND NOT EXISTS (\n\
                              SELECT 1\n\
                                FROM snapshots rank_before\n\
                               WHERE rank_before.sandbox_id = {snapshot_alias}.sandbox_id\n\
                                 AND rank_before.type = 'full'\n\
                                 AND rank_before.generation >= rank_later.generation\n\
                                 AND {before} <= {this}\n\
                          )\n\
                     END\n\
         ) THEN 0 END)"
    )
}

/// Returns an `EXISTS(...)` SQL fragment: the sandbox aliased as
/// `sandbox_alias` has a full snapshot, in any state, that records no
/// generation, registered after one with a generation, and no later
/// execution's capture registered after it ([`capture_rank`] is NULL).
/// Nothing orders it against the captures with a generation: it
/// may be the latest capture, from a node that couldn't vouch for it, or a
/// re-upload of an older one. An unpinned restore refuses such a history
/// explicitly instead of guessing, and the gates and retention treat it as
/// having no single source.
pub fn restore_order_unordered(sandbox_alias: &str) -> String {
    restore_order_unordered_where(sandbox_alias, &capture_rank("unordered"), "TRUE", "TRUE")
}

/// [`restore_order_unordered`] within a restore retry's pre-claim source
/// window ([`restore_retry_source_snapshot_window`]): a row the failed
/// branch registered after the claim can't make the retry's source
/// ambiguous, since the retry never loads it.
pub fn restore_order_unordered_in_retry_window(sandbox_alias: &str) -> String {
    let window = restore_retry_source_snapshot_window(sandbox_alias, "unordered");
    let rank = capture_rank_in_retry_window(sandbox_alias, "unordered");
    let era_window = restore_retry_source_snapshot_window(sandbox_alias, "era");
    restore_order_unordered_where(sandbox_alias, &rank, &window, &era_window)
}

fn restore_order_unordered_where(
    sandbox_alias: &str,
    rank: &str,
    condition: &str,
    era_condition: &str,
) -> String {
    // The probe for a capture with a generation goes first: a sandbox with
    // none, nearly every one, never ranks its rows without one.
    format!(
        "(EXISTS (\n\
             SELECT 1\n\
               FROM snapshots era\n\
              WHERE era.sandbox_id = {sandbox_alias}.id\n\
                AND era.type = 'full'\n\
                AND era.generation IS NOT NULL\n\
                AND {era_condition}\n\
         )\n\
         AND EXISTS (\n\
             SELECT 1\n\
               FROM snapshots unordered\n\
              WHERE unordered.sandbox_id = {sandbox_alias}.id\n\
                AND unordered.type = 'full'\n\
                AND unordered.generation IS NULL\n\
                AND {rank} IS NULL\n\
                AND {condition}\n\
         ))"
    )
}

/// Returns an `EXISTS(...)` SQL fragment: some full snapshot of the sandbox
/// aliased as `sandbox_alias`, in any state, records a higher capture
/// `generation` than the [`capture_rank`] of the snapshot aliased as
/// `snapshot_alias`. Never TRUE when that snapshot has no rank. The fragment's own alias is `dom`, so
/// `snapshot_alias` must be another one.
pub fn full_snapshot_outranked_by_generation(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let rank = capture_rank(snapshot_alias);
    full_snapshot_outranked_where(sandbox_alias, &rank, "TRUE")
}

/// [`full_snapshot_outranked_by_generation`], counting only the outranking
/// rows `dom` for which `condition` holds.
fn full_snapshot_outranked_where(sandbox_alias: &str, rank: &str, condition: &str) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots dom\n\
              WHERE dom.sandbox_id = {sandbox_alias}.id\n\
                AND dom.type = 'full'\n\
                -- First, so a row without a generation never computes the rank.\n\
                AND dom.generation IS NOT NULL\n\
                AND dom.generation > {rank}\n\
                AND {condition}\n\
         )"
    )
}

/// Like [`no_newer_full_snapshot`], newest by capture generation first, except
/// that a newer failed retry of the same immutable artifact does not
/// invalidate an already-ready snapshot.
///
/// A non-null local artifact ID is required to prove identity. Markerless
/// legacy snapshots keep the strict newest-full-snapshot rule.
pub fn no_newer_full_snapshot_ignoring_failed_retry(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let suspend = ArtifactOrigin::Suspend.as_str();
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_by_generation(sandbox_alias, "newer");
    format!(
        "(NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND {newer_captured} > {captured}\n\
                AND NOT {newer_outranked}\n\
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
/// quarantine episode it is intended to recover. It names no artifact, so it
/// chooses by capture order, and like the restore selector it refuses an
/// unordered history ([`restore_order_unordered`]): either capture could be
/// the latest.
pub fn pre_quarantine_suspend_snapshot_current(
    sandbox_alias: &str,
    snapshot_alias: &str,
    quarantine_entered_at: &str,
) -> String {
    let current = quarantine_recovery_suspend_snapshot_current(sandbox_alias, snapshot_alias);
    let unordered = restore_order_unordered(sandbox_alias);
    format!(
        "{current}\n\
         AND {snapshot_alias}.timestamp <= {quarantine_entered_at}\n\
         AND NOT {unordered}"
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
    let unordered = restore_order_unordered(sandbox_alias);
    let legacy = format!(
        "(NOT {unordered} AND EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND {no_newer}\n\
         ))"
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
/// this restore source, and only a suspend capture is: the retry ignores the
/// pin, which names a suspend's artifact. Either way a backup of a full named
/// snapshot, or of a stop, registered before the claim isn't the state the
/// restore started from: a named snapshot is taken while the sandbox runs, so
/// one registered after a suspend capture of the same execution was taken
/// before it. The window admits only suspend captures (and rows from before
/// origins were recorded), as an unpinned first attempt does.
pub fn restore_retry_source_snapshot_window(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
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
             AND COALESCE({snapshot_alias}.artifact_origin, '{suspend}') = '{suspend}'\n\
             AND (\n\
                 {sandbox_alias}.current_local_artifact_id IS NULL\n\
                 OR ({snapshot_alias}.local_artifact_id IS NOT NULL\n\
                     AND {snapshot_alias}.artifact_kind = 'full')\n\
             )\n\
         )"
    )
}

/// Returns a boolean SQL fragment: the snapshot aliased as `snapshot_alias`
/// registered before the restore of the sandbox aliased as `sandbox_alias`
/// claimed it, the one bound a retry applies to its pinned artifact. The pin
/// names the bytes exactly, so the placement bounds of
/// [`restore_retry_source_snapshot_window`], which keep a markerless retry off
/// stale captures, don't apply: a failed restore can leave the placement clock
/// newer than the pin's backup.
pub fn registered_before_restore_claim(sandbox_alias: &str, snapshot_alias: &str) -> String {
    format!(
        "({sandbox_alias}.lifecycle_changed_at IS NULL\n\
          OR {snapshot_alias}.timestamp < {sandbox_alias}.lifecycle_changed_at)"
    )
}

/// Returns TRUE when `snapshot_alias` is the newest full snapshot in the
/// restore attempt's pre-claim source window. Newer rows created after the
/// restore claim are ignored. A newer pre-claim `creating` row or `failed` row
/// for a different artifact still blocks stale fallback. A `deleting` row also
/// blocks fallback until finalization; only failed retries of the same
/// immutable artifact are ignored.
///
/// Newest is by capture generation first, as in [`no_newer_full_snapshot`],
/// but only rows in the window can outrank one: a capture the failed restore
/// branch registered after the claim carries a later generation than the
/// source, and must not strand it.
pub fn restore_retry_source_snapshot_is_current(
    sandbox_alias: &str,
    snapshot_alias: &str,
) -> String {
    let captured = capture_registered_at(snapshot_alias);
    let newer_captured = capture_registered_at("newer");
    let suspend = ArtifactOrigin::Suspend.as_str();
    let source_window = restore_retry_source_snapshot_window(sandbox_alias, snapshot_alias);
    let newer_source_window = restore_retry_source_snapshot_window(sandbox_alias, "newer");
    let outranked = full_snapshot_outranked_in_retry_window(sandbox_alias, snapshot_alias);
    let newer_outranked = full_snapshot_outranked_in_retry_window(sandbox_alias, "newer");
    format!(
        "({source_window}\n\
         AND NOT {outranked}\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots newer\n\
              WHERE newer.sandbox_id = {sandbox_alias}.id\n\
                AND newer.type = 'full'\n\
                AND {newer_captured} > {captured}\n\
                AND {newer_source_window}\n\
                AND NOT {newer_outranked}\n\
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

/// [`full_snapshot_outranked_by_generation`] restricted to the restore retry's
/// pre-claim source window.
fn full_snapshot_outranked_in_retry_window(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let window = restore_retry_source_snapshot_window(sandbox_alias, "dom");
    let rank = capture_rank_in_retry_window(sandbox_alias, snapshot_alias);
    full_snapshot_outranked_where(sandbox_alias, &rank, &window)
}

/// Returns a SQL fragment evaluating to TRUE iff the sandbox has a durable
/// snapshot that the `restoring` retry path can use as its source: its
/// pinned artifact when the window holds it, else the window's newest
/// capture of a history that isn't unordered ([`restore_order_unordered`]).
pub fn restore_retry_source_snapshot_exists(sandbox_alias: &str) -> String {
    let source_is_current = restore_retry_source_snapshot_is_current(sandbox_alias, "sn");
    let pinned = snapshot_is_current_local_artifact(sandbox_alias, "sn");
    let window = registered_before_restore_claim(sandbox_alias, "sn");
    let unordered = restore_order_unordered_in_retry_window(sandbox_alias);
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND ssl.orphaned_at IS NULL\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND (({source_is_current} AND ({pinned} OR NOT {unordered}))\n\
                     OR ({pinned} AND {window}))\n\
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

/// [`started_suspend_revert_eligibility`] for finalizing a node-local suspend
/// the node reports: the node keeps the sandbox, suspended, on its own disk,
/// and the row only records that. Its markerless arm doesn't refuse an
/// unordered history ([`restore_order_unordered`]): finalizing chooses no
/// restore source, and refusing it would leave the row `started` with an
/// execution while the node holds the sandbox suspended. The restore still
/// refuses that history off the sandbox's node until it's pinned. Reverts that
/// unplace the row keep the refusal.
pub fn started_suspend_finalize_eligibility(sandbox_alias: &str) -> SuspendRevertEligibility {
    let marker_current = current_suspend_origin_marker_snapshot_ready(sandbox_alias);
    let newest_current = newest_suspend_snapshot_published(sandbox_alias);
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

/// Orphaned-sandbox variant of [`started_suspend_revert_eligibility`]: the
/// marker branch trusts an exact durable backup even when the snapshot
/// predates `lifecycle_changed_at` (see
/// [`exact_marker_snapshot_backup_current`]). Use it only for a row whose pod
/// is confirmed missing, terminal or unready past a grace period, or whose
/// latest execution is `forgotten`. Always-on projections must use the default
/// variant.
pub fn orphan_recovery_revert_eligibility(sandbox_alias: &str) -> SuspendRevertEligibility {
    let marker_current = exact_marker_snapshot_backup_current(sandbox_alias);
    let newest_current = newest_suspend_snapshot_current(sandbox_alias);
    suspend_revert_eligibility(sandbox_alias, marker_current, newest_current)
}

/// TRUE when reverting the row to `suspended` and unplacing it can't lose
/// bytes newer than the snapshot the revert restores, as far as the control
/// plane's records go. Three things could hold such bytes:
///
/// - the marked local artifact, unless it's absent or has a durable copy;
/// - any full snapshot still uploading. This is wider than
///   [`inflight_suspend_upload_exists`], which only sees an upload of the
///   marked artifact (or, markerless, one registered since placement), while a
///   newer artifact's upload can be in flight before the marker moves. An
///   upload whose worker died holds the row until the stuck-snapshot reaper
///   finalizes it;
/// - a full snapshot whose upload failed after the newest ready one. Its local
///   artifact may still be on the node, and the markerless eligibility only
///   reads ready snapshots. A failed retry of an artifact that already has a
///   ready copy carries nothing newer and doesn't count.
///
/// Necessary, not sufficient. A rootfs written after the newest snapshot
/// leaves no record here, so node drain's pod-less give-up conjoins this with
/// [`newest_full_snapshot_unambiguous`] and with the node's own answer for
/// the generation that ran there: a local delete.
pub fn pod_less_drain_revert_loses_no_newer_bytes(sandbox_alias: &str) -> String {
    let marker_durable = artifact_has_durable_copy(
        &format!("{sandbox_alias}.id"),
        &format!("{sandbox_alias}.current_local_artifact_id"),
        &format!("{sandbox_alias}.current_local_artifact_kind"),
    );
    format!(
        "(({sandbox_alias}.current_local_artifact_id IS NULL OR {marker_durable})
          AND NOT EXISTS (
              SELECT 1
                FROM snapshots uploading
               WHERE uploading.sandbox_id = {sandbox_alias}.id
                 AND uploading.type = 'full'
                 AND uploading.state = 'creating'
          )
          AND NOT EXISTS (
              SELECT 1
                FROM snapshots failed
               WHERE failed.sandbox_id = {sandbox_alias}.id
                 AND failed.type = 'full'
                 AND failed.state = 'failed'
                 AND failed.timestamp > COALESCE(
                     (
                         SELECT MAX(ready.timestamp)
                           FROM snapshots ready
                          WHERE ready.sandbox_id = {sandbox_alias}.id
                            AND ready.type = 'full'
                            AND ready.state = 'ready'
                     ),
                     '-infinity'::timestamptz
                 )
                 AND NOT EXISTS (
                     SELECT 1
                       FROM snapshots copy
                      WHERE copy.sandbox_id = {sandbox_alias}.id
                        AND copy.state = 'ready'
                        AND copy.local_artifact_id = failed.local_artifact_id
                 )
          ))"
    )
}

/// TRUE when the snapshot a markerless suspend restore would select is
/// unambiguous: one ready full snapshot with storage that isn't orphaned, no
/// newer full snapshot in any state other than a failed retry of the same
/// artifact, and no other ready full snapshot at the same timestamp unless
/// it holds the same artifact.
///
/// Among ready full snapshots of one capture rank the restore orders by their
/// captures' first registrations, then their own; two artifacts' rows
/// registered at the same instant leave that to the snapshot id, so such a tie
/// counts as ambiguous. A newer snapshot still `deleting` (or in any state
/// other than ready) names bytes the activity-anchored eligibility, which reads
/// only ready snapshots, never compares against. Node drain's pod-less
/// give-up conjoins this with that eligibility before unplacing a row.
pub fn newest_full_snapshot_unambiguous(sandbox_alias: &str) -> String {
    let newest = no_newer_full_snapshot_ignoring_failed_retry(sandbox_alias, "source");
    let unordered = restore_order_unordered(sandbox_alias);
    format!(
        "(NOT {unordered} AND EXISTS (\n\
             SELECT 1\n\
               FROM snapshots source\n\
               JOIN snapshot_storage_locations source_storage\n\
                 ON source_storage.snapshot_id = source.id\n\
                AND source_storage.orphaned_at IS NULL\n\
              WHERE source.sandbox_id = {sandbox_alias}.id\n\
                AND source.type = 'full'\n\
                AND source.state = 'ready'\n\
                AND {newest}\n\
                AND NOT EXISTS (\n\
                    SELECT 1\n\
                      FROM snapshots tied\n\
                     WHERE tied.sandbox_id = {sandbox_alias}.id\n\
                       AND tied.type = 'full'\n\
                       AND tied.state = 'ready'\n\
                       AND tied.timestamp = source.timestamp\n\
                       AND tied.id <> source.id\n\
                       AND NOT (\n\
                           source.local_artifact_id IS NOT NULL\n\
                           AND tied.local_artifact_id = source.local_artifact_id\n\
                           AND tied.artifact_kind IS NOT DISTINCT FROM source.artifact_kind\n\
                       )\n\
                )\n\
         ))"
    )
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
/// recorded command/access clocks, no full snapshot may outrank it by capture
/// generation, and no concretely-non-suspend full-ready snapshot may tie or
/// beat it by timestamp (faithfulness to the markerless restore selector; see
/// that function).
///
/// Only for callers that separately prove no runtime can modify the artifact:
/// either an aged inactive repair candidate, or the exact terminal recovery
/// owner with no pod materialization or placement while holding the sandbox
/// lock and retiring that owner in the same transaction. Always-on projections
/// must use the lifecycle-bounded variant.
pub fn newest_suspend_snapshot_activity_current(sandbox_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, "sn");
    let unordered = restore_order_unordered(sandbox_alias);
    format!(
        "(NOT {unordered} AND EXISTS (\n\
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
                AND NOT {outranked}\n\
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
         ))"
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

/// Returns a boolean SQL fragment: the snapshot aliased as `snapshot_alias` is
/// the capture the sandbox aliased as `sandbox_alias` is being suspended with,
/// so the row can be pinned to its artifact. It's a `ready` full backup of a
/// suspend artifact (`artifact_kind = 'full'`, origin `suspend`) with a storage
/// location that isn't orphaned, whose capture first registered at or after
/// the row's last lifecycle change (so not a re-upload of a capture from
/// before it, which one execution can also own after a reverted suspend);
/// and it records the capture generation of the sandbox's
/// active execution, so that execution, the one being suspended, captured it.
/// And no full snapshot of the sandbox, in any state, is newer by capture
/// ([`no_newer_full_snapshot`]): one execution can own several captures, a
/// reverted suspend's and the next one, and only the latest is the state.
///
/// The generation is the proof. Registration time alone can't tell this
/// suspend's capture from a re-upload of an older one that registered while
/// the suspend ran, and pinning that re-upload would restore the older state.
/// A capture the node didn't vouch for isn't pinned.
///
/// A node-local suspend finalizes the row without the API's suspend handoff,
/// which is what pins an API suspend. Pinning the capture there gives the
/// restore the exact artifact, so a later re-upload of an older capture can't
/// be selected in its place. Never NULL.
pub fn suspend_snapshot_is_current_capture(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let captured = capture_registered_at(snapshot_alias);
    let no_newer = no_newer_full_snapshot(sandbox_alias, snapshot_alias);
    format!(
        "({snapshot_alias}.type = 'full'\n\
          AND {snapshot_alias}.state = 'ready'\n\
          AND {snapshot_alias}.local_artifact_id IS NOT NULL\n\
          AND {snapshot_alias}.artifact_kind IS NOT DISTINCT FROM 'full'\n\
          AND {snapshot_alias}.artifact_origin IS NOT DISTINCT FROM '{suspend}'\n\
          AND EXISTS (\n\
              SELECT 1\n\
                FROM snapshot_storage_locations capture_ssl\n\
               WHERE capture_ssl.snapshot_id = {snapshot_alias}.id\n\
                 AND capture_ssl.orphaned_at IS NULL\n\
          )\n\
          AND {captured} >= {sandbox_alias}.lifecycle_changed_at\n\
          AND EXISTS (\n\
              SELECT 1\n\
                FROM sandbox_k8s_executions capture_execution\n\
               WHERE capture_execution.sandbox_id = {sandbox_alias}.id\n\
                 AND capture_execution.active\n\
                 AND capture_execution.generation = {snapshot_alias}.generation\n\
          )\n\
          AND {no_newer})"
    )
}

/// Returns a boolean SQL fragment: the snapshot aliased as `snapshot_alias`
/// is a `ready` full backup, with a live location, of the full suspend
/// artifact `artifact_expr` that node `node_expr` reports holding for the
/// sandbox aliased as `sandbox_alias`, uploaded from that node, and it's
/// still the sandbox's latest capture as far as the control plane can tell:
/// no full snapshot in any state outranks it by generation, and no suspend
/// capture of another artifact, from any node, first registered after it
/// did, which would mean the sandbox ran since, or at the same instant, which
/// capture order can't break. A named snapshot's or a
/// stop's backup isn't a capture the sandbox resumes from, so it doesn't
/// count.
///
/// The node's report is the evidence, not the generation, so this pins a
/// capture without one too: the node holds the bytes it suspended. The
/// restore-pin repair and a node-local suspend's finalization use it to pin
/// the row.
pub fn node_held_capture_is_latest(
    sandbox_alias: &str,
    snapshot_alias: &str,
    artifact_expr: &str,
    node_expr: &str,
) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, snapshot_alias);
    let captured = capture_registered_at(snapshot_alias);
    let later_captured = capture_registered_at("elsewhere");
    format!(
        "({snapshot_alias}.sandbox_id = {sandbox_alias}.id\n\
          AND {snapshot_alias}.type = 'full'\n\
          AND {snapshot_alias}.state = 'ready'\n\
          AND {snapshot_alias}.local_artifact_id = {artifact_expr}\n\
          AND {snapshot_alias}.artifact_kind = 'full'\n\
          AND {snapshot_alias}.artifact_origin = '{suspend}'\n\
          AND {snapshot_alias}.source_node_id = {node_expr}\n\
          AND EXISTS (\n\
              SELECT 1\n\
                FROM snapshot_storage_locations held_ssl\n\
               WHERE held_ssl.snapshot_id = {snapshot_alias}.id\n\
                 AND held_ssl.orphaned_at IS NULL\n\
          )\n\
          AND NOT {outranked}\n\
          AND NOT EXISTS (\n\
              SELECT 1\n\
                FROM snapshots elsewhere\n\
               WHERE elsewhere.sandbox_id = {sandbox_alias}.id\n\
                 AND elsewhere.type = 'full'\n\
                 AND (elsewhere.artifact_origin IS NULL\n\
                      OR elsewhere.artifact_origin = '{suspend}')\n\
                 AND elsewhere.local_artifact_id IS DISTINCT FROM {artifact_expr}\n\
                 AND {later_captured} >= {captured}\n\
          ))"
    )
}

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
/// markerless restore selector
/// (`pplx_space_sandbox_store::restore_source::select_restore_source`) picks
/// the latest capture among ready full suspend (or null-origin) snapshots:
/// no candidate outranks it by capture generation, and it registered last
/// among those. So this predicate must vouch for *that* snapshot, not merely
/// for the existence of some current suspend snapshot: it requires a current
/// suspend/null-origin snapshot that no full snapshot outranks by generation.
/// It also refuses when a concretely-non-suspend full-ready snapshot ties or
/// beats it by timestamp, which the selector no longer loads: stricter than
/// the selector, so it can only leave a row unrecovered, never recover one
/// the selector would restore differently.
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
    let unordered = restore_order_unordered(sandbox_alias);
    let newest = newest_suspend_snapshot_published(sandbox_alias);
    format!("(NOT {unordered} AND {newest})")
}

/// [`newest_suspend_snapshot_current`] without its refusal of an unordered
/// history: the snapshot is the newest current suspend capture as far as
/// capture order can tell. Only for [`started_suspend_finalize_eligibility`].
fn newest_suspend_snapshot_published(sandbox_alias: &str) -> String {
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
    let unordered = restore_order_unordered(sandbox_alias);
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
    let newest =
        newest_suspend_snapshot_current_with_guards(sandbox_alias, &source_guard, &selection_guard);
    format!("(NOT {unordered} AND {newest})")
}

fn newest_suspend_snapshot_current_with_guards(
    sandbox_alias: &str,
    source_guard: &str,
    selection_guard: &str,
) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, "sn");
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
                AND NOT {outranked}\n\
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
///   ([`fresh_ready_snapshot_exists`]) is the right portability question, and
///   the capture must also be newest among suspend captures, as for a marked
///   row: a later capture still uploading or failed holds the source — a
///   `wake → modify → re-suspend` cycle must not release the source on a stale
///   prior-cycle snapshot, and there is no cohort restore to keep in step with.
///   As there, the fresh snapshot must be the latest capture: a re-upload of an
///   older one registered since the placement doesn't release the source, nor
///   does one of the previous capture while the latest's upload is in flight
///   or failed.
/// * An unpinned row releases its source only on a suspend capture: the
///   restore never loads a named snapshot's or a stop's backup for it.
/// * Neither releases the source of an unpinned row whose history is
///   unordered ([`restore_order_unordered`]): its restore refuses, and the
///   source may hold the only copy of the latest state.
pub fn snapshot_durable_for_source_release(sandbox_alias: &str) -> String {
    let unordered = restore_order_unordered(sandbox_alias);
    // An unpinned row releases its source only on a copy the restore would
    // load: a suspend capture, by the rules below. A named snapshot's or a
    // stop's backup isn't one, and while it outranks the suspend capture,
    // nothing is.
    let copy = durable_copy_exists_where(sandbox_alias, &released_unpinned_copy(sandbox_alias));
    // The unordered check is per sandbox, so it's ANDed outside the copy's
    // EXISTS, once.
    format!(
        "(({sandbox_alias}.current_local_artifact_id IS NOT NULL OR NOT {unordered})
         AND {copy})"
    )
}

/// [`snapshot_durable_for_source_release`] without its refusal of an
/// unpinned unordered history: a durable copy exists that the sandbox could
/// still restore from. For the give-ups that fail a sandbox with no durable
/// copy (the drain's after `ArtifactMissing`, the drained-node terminator's).
/// An unordered history keeps every ready durable copy, evacuation-marked or
/// not, since its newest rows may be captures that never uploaded and an
/// operator chooses between them; failing it would end the sandbox's last
/// restore path. Such a row stays `suspended`, and its restore refuses until
/// it's pinned. A named snapshot's backup counts here too: a give-up keeps
/// the row rather than fail it. So does every copy
/// [`snapshot_durable_for_source_release`] accepts.
pub fn snapshot_durable_copy_exists(sandbox_alias: &str) -> String {
    let unordered = restore_order_unordered(sandbox_alias);
    let ordered =
        ordered_unpinned_copy(sandbox_alias, &no_newer_full_snapshot(sandbox_alias, "sn"));
    // Everything the source-release gate would accept counts too.
    let released = released_unpinned_copy(sandbox_alias);
    durable_copy_exists_where(
        sandbox_alias,
        &format!("({unordered} OR {ordered} OR {released})"),
    )
}

/// Whether the ready full snapshot aliased `sn` is the copy that releases an
/// unpinned row's source ([`snapshot_durable_for_source_release`]): a suspend
/// capture, newest among suspend captures, since a later capture may hold
/// bytes only the source node has and releasing it would leave the restore an
/// older capture. For an evacuation-marked row a failed later capture holds it
/// too, since the evacuating drain re-uploads it from the node, and fails the
/// row if its bytes are gone. For an unmarked row only an upload still in
/// flight holds it, which ends: nothing retries a failed one there, so a hold
/// on it would never end. An unmarked row also needs a capture fresher than
/// its last placement.
fn released_unpinned_copy(sandbox_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let newest = no_newer_suspend_capture(sandbox_alias, "sn");
    let newest_live = no_newer_live_suspend_capture(sandbox_alias, "sn");
    let captured = capture_registered_at("sn");
    format!(
        "((sn.artifact_origin IS NULL OR sn.artifact_origin = '{suspend}')
          AND (({sandbox_alias}.evacuation_restore_target IS NOT NULL AND {newest})
               OR ({sandbox_alias}.evacuation_restore_target IS NULL
                   AND {newest_live}
                   AND ({sandbox_alias}.placement_changed_at IS NULL
                        OR {captured} >= {sandbox_alias}.placement_changed_at))))"
    )
}

/// Whether the ready full snapshot aliased `sn` is the copy an unpinned row
/// with an ordered history restores from: for an evacuation-marked row, the
/// newest by `no_newer` ([`no_newer_full_snapshot`] or
/// [`no_newer_suspend_capture`]); otherwise one fresher than the last
/// placement that no capture outranks.
fn ordered_unpinned_copy(sandbox_alias: &str, no_newer: &str) -> String {
    let captured = capture_registered_at("sn");
    let outranked = full_snapshot_outranked_by_generation(sandbox_alias, "sn");
    format!(
        "(({sandbox_alias}.evacuation_restore_target IS NOT NULL AND {no_newer})
          OR (
              {sandbox_alias}.evacuation_restore_target IS NULL
              AND (
                  {sandbox_alias}.placement_changed_at IS NULL
                  OR {captured} >= {sandbox_alias}.placement_changed_at
              )
              AND NOT {outranked}
          ))"
    )
}

/// An `EXISTS` over the sandbox's ready full snapshots with a live location:
/// one of its pinned artifact, or, for an unpinned row, one for which
/// `unpinned` holds.
fn durable_copy_exists_where(sandbox_alias: &str, unpinned: &str) -> String {
    let pinned = snapshot_is_current_local_artifact(sandbox_alias, "sn");
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
                    OR ({sandbox_alias}.current_local_artifact_id IS NULL AND {unpinned})
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

/// Returns a boolean SQL fragment that is TRUE iff the row aliased as
/// `sandbox_alias` is a `stopped` sandbox whose pinned artifact is durable in
/// S3 — the question that decides whether a node's report of absence may
/// remove the row rather than merely forget the observation.
///
/// The internal API reads a node's absence as a deletion in two places, both
/// only sound while the node's local row is the sandbox's sole existence:
///
/// * `prune_absent_node_sandboxes` (`internal/api/server/src/status.rs`, through
///   `pplx_space_sandbox_store::ingest::prune_census_absent_sandboxes`): a
///   complete census that omits the sandbox drives that statement's
///   `marked_deleted` arm, which sets `lifecycle = 'deleted'`.
/// * `publish_sandbox_states` (`internal/api/server/src/published_state.rs`):
///   an explicit `present: false` for a stopped sandbox deletes its row and
///   detaches its snapshots' storage locations. A node reports that for any
///   requested sandbox it no longer holds.
///
/// This fragment names the case where the premise fails, so both can skip it.
///
/// Three conjuncts, each excluding a different row:
///
/// * `lifecycle = 'stopped'` — the cluster still intends the sandbox stopped.
///   A woken row carrying a stale stop pin does not buy an exemption; its live
///   instance really is gone when the census stops naming it.
/// * `current_local_artifact_origin = 'stop'` — the pinned bytes are a
///   *stopped* instance's, not a prior suspend's. `sandboxes`'
///   `current_local_artifact_origin` admits only `'stop'` and `'suspend'`
///   (`sandboxes_current_local_artifact_origin_check`), and a suspend pin
///   can outlive its cycle: `recoverable_suspend_snapshot_exists` documents
///   the stale-pin-with-`active_node_id`-set row this excludes.
/// * The exact pinned artifact has a durable copy —
///   [`artifact_has_durable_copy`] over the marker's
///   `(current_local_artifact_id, current_local_artifact_kind)`, so the
///   fragment vouches for whatever kind the pin names rather than hard-coding
///   one, and agrees with every other caller on what "durable" means. Hence
///   `stopped_artifact_durable` and not `..._disk_...`: a stop artifact is a
///   disk in practice, but nothing here requires it.
///
/// Deliberately **not** placement-gated, following the exact-identity branch
/// of [`fresh_ready_snapshot_exists`] — [`matching_ready_backup_exists`] omits
/// `placement_changed_at`, and only the markerless legacy branch beside it
/// compares timestamps. Matching the pin is this module's currency test, so a
/// re-place must not revoke the exemption.
///
/// ## NULL-safety
///
/// The caller negates this (`AND NOT …`), so a NULL result would spare a row
/// instead of reaping it — the opposite of the intended default. It cannot
/// be NULL. `lifecycle` is `NOT NULL`, and an `EXISTS` is never NULL, so
/// `current_local_artifact_origin` is the only term that can be.
/// `sandboxes_current_local_artifact_identity_check`
/// (`db/postgres/*_baseline.up.sql`) makes it NULL exactly when
/// `current_local_artifact_id` is, and a NULL id matches no snapshot, so the
/// `EXISTS` is FALSE and the conjunction is FALSE rather than NULL. That
/// constraint is load-bearing here, not just a value-domain check: an id
/// without an origin, over a durable artifact, would make the conjunction NULL
/// and spare the row.
///
/// ## What the producer owes this predicate
///
/// **Laxer than the source-release gate, deliberately.** This is a new
/// predicate, not a reuse of [`snapshot_durable_for_source_release`]. That
/// gate has no lifecycle or origin term, and for a row that carries a pin,
/// which is every row this one can match, its `EXISTS` differs from this one's
/// only by also requiring `sn.type = 'full'`; both match the snapshot's
/// `artifact_kind` against the pin. So a disk-kind stop pin can earn this
/// exemption without satisfying the drain gate, and the two are not
/// interchangeable.
///
/// That asymmetry is the right way round for the two questions. Releasing a
/// node is retryable — refuse it and the drain waits, visibly. Tombstoning is
/// not: it is an irreversible, user-visible write, so its guard should err
/// toward sparing. A predicate strict enough for a node release would tombstone
/// rows it merely cannot vouch for.
///
/// It ships ahead of its producer deliberately, and that only works if the
/// producer meets three obligations this fragment cannot enforce:
///
/// * **Re-mint the pin every stop.** Trusting the marker without a cycle
///   anchor is sound for suspend because "every suspend attempt allocates a
///   fresh row id and S3 prefix" (module header). A stop path that left a
///   prior cycle's marker in place after `stop → start → write → stop` would
///   have this fragment vouch for the *first* session's disk, and the row
///   would survive as restorable from stale bytes instead of being tombstoned
///   where the loss is visible.
/// * **Commit the catalog rows and the pin before the node can first report
///   the sandbox absent.** Both callers lock the rows they act on: the census arm is an
///   `UPDATE`, and the `present: false` delete selects its candidates
///   `FOR UPDATE`. Under Read Committed, a row updated after the statement's
///   snapshot is re-evaluated against its newest committed version: that
///   version supplies every reference to the row, including the `EXISTS`'s
///   correlation to its pin, but the `EXISTS`'s own scans of `snapshots` and
///   `snapshot_storage_locations` keep the statement's original snapshot. So
///   a pin committed after that snapshot is seen, while catalog rows committed
///   after it are not, in the pin's transaction or any other, and the
///   `EXISTS` is FALSE. The census arm then tombstones a durable row. The
///   `present: false` delete fails instead, because its cascade into
///   `snapshots` trips the `RESTRICT` foreign key of the storage location it
///   couldn't see, so the whole publication is refused. With both commits
///   ahead of the first absence report, every statement that reads the
///   absence starts after them.
/// * **Move the row off the node.** A spared row keeps `active_node_id`, and
///   the drained-node terminator's `active_resident_predicate` counts a
///   `stopped` row, so under the default `ResidencyEvidence::LifecycleOnly`
///   the node stays held. Under census-aware residency the drain discounts the
///   unobserved row and the node can terminate, leaving a row that names a
///   node that no longer exists, and nothing sweeps a stranded `stopped` row.
///
/// It is not a substitute for the evacuation's own durability gate either: a
/// caller that deletes a local row without establishing the pin gets no
/// protection here.
///
/// ## Reaches nothing today
///
/// One writer outside the suspend paths below now produces a `'stop'` marker:
/// node drain's `record_stopped_evacuation_artifact`, on a Kubernetes row it
/// has claimed for evacuation. Both callers of this fragment filter
/// `managed_by` to the Space substrate, so neither reaches those rows.
///
/// Every current Postgres writer of a non-NULL marker is a suspend path, and
/// four write `'suspend'` whenever they write an origin at all: `kubernetes/store/suspend.rs`'s
/// suspend-commit `UPDATE` and the cluster controller's quarantine recovery to
/// `suspended`
/// (`pplx_space_sandbox_store::repair::recover_quarantined_publication_to_suspended`,
/// which also nulls `active_node_id`) spell it in their SQL, while
/// `finalize_suspended_sandbox_in` (both callers bind `ArtifactOrigin::Suspend`)
/// and `persist_optional_current_local_artifact` (whose only caller, the Direct
/// substrate's `suspend_execution`, binds it too) take it as a parameter. The
/// fifth — the internal API's `apply_suspended_k8s_finalization` — mirrors
/// the artifact the node published, but only through
/// [`node_published_suspend_evidence`], which requires the published origin to
/// be `'suspend'` and its kind `'full'`, so it cannot carry a `'stop'` marker
/// either. Its statement (`pplx_space_sandbox_store::ingest::finalize_suspended`)
/// also nulls `active_node_id`, while this arm only reaches rows whose
/// `active_node_id` names the publishing node. So no current writer produces
/// a row this term spares. The schema has admitted a `'stop'` origin
/// since the marker columns were added, so a row an earlier writer left
/// pinned that way would be spared only if its pinned artifact is durable,
/// which is the outcome this term exists for.
pub fn stopped_artifact_durable(sandbox_alias: &str) -> String {
    let durable = artifact_has_durable_copy(
        &format!("{sandbox_alias}.id"),
        &format!("{sandbox_alias}.current_local_artifact_id"),
        &format!("{sandbox_alias}.current_local_artifact_kind"),
    );
    let stopped = SandboxLifecycle::Stopped.as_str();
    let stop = ArtifactOrigin::Stop.as_str();
    format!(
        "(\n\
             {sandbox_alias}.lifecycle = '{stopped}'\n\
             AND {sandbox_alias}.current_local_artifact_origin = '{stop}'\n\
             AND {durable}\n\
         )"
    )
}

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the **disk** artifact the sandbox row aliased as `sandbox_alias` currently
/// pins has a non-orphaned `state='ready'` backup in the catalog.
///
/// This is the durability proof a `stopped` evacuation releases its source node
/// on: the drain publishes the sandbox's disk artifact, waits for this to hold,
/// and only then releases the node, which the drain then terminates with the
/// rootfs on it. Nothing on the node objects — a `stopped` row's
/// `sandbox_erase_safety` is `Safe` (`crates/l40_api/sbx/mgr/server/src/db/
/// cleanup_episodes.rs`). This predicate is the drain's only check that the
/// bytes exist off the node; the drain's other checks before releasing the node
/// confirm the node still holds the copy that was published and that nothing
/// has started it since.
///
/// The row shape it matches is exactly the one `GetArtifactBackupLocation`
/// returns — same `(sandbox_id, local_artifact_id, artifact_kind, state =
/// 'ready')` key, via [`snapshot_is_current_local_artifact`] — so "the drain
/// believes the bytes are durable" and "a restore can resolve them to a URI"
/// cannot disagree. The `snapshot_storage_locations` join with
/// `orphaned_at IS NULL` is what makes the second half true rather than
/// assumed: a `ready` snapshot whose only storage location has been orphaned
/// has no URI left to restore from.
///
/// **No freshness term here, and that is a scoping decision rather than a
/// claim that the identity cannot go stale.** It can. `current_local_artifact_id`
/// names an immutable artifact, but a start that boots the guest and then
/// fails is rolled back to `stopped` keeping whatever pin was already on the
/// row — sandbox-manager's `apply_cleanup_state` only overwrites it when
/// publishing a suspend artifact, and only a successful start clears it — so
/// after `stop → mint → failed start that writes` the pin describes the disk as
/// it was before the failed start while the rootfs has moved on.
///
/// That matters to exactly one caller: the teardown, which gives up the rootfs
/// and must not do so on the strength of an artifact describing different
/// bytes. It cannot be fixed with a freshness term here, or anywhere —
/// `snapshots.timestamp` is *registration* time, the pin is reused rather than
/// re-minted, and an already-`ready` backup produces no new row at all, so no
/// timestamp the cluster holds is a proxy for when the snapshot was taken.
/// `pplx_space_sandbox_store::drain::claim_stopped_evacuation` instead establishes
/// **provenance**: it will only open an evacuation on a row with no pin, so the
/// artifact it goes on to publish is one it caused to be minted from the
/// current rootfs.
///
/// So read this as "the pinned disk artifact is durably restorable", not as
/// "the pinned disk artifact describes the current rootfs". A caller for which
/// those differ owns the difference.
///
/// The kind is pinned twice on purpose. `snapshot_is_current_local_artifact`
/// matches `sn.artifact_kind` against the sandbox's pinned kind, so a `full`
/// pin cannot satisfy this; `sn.type = 'disk'` additionally pins the catalog
/// row's own kind, which is what the disk-only restore-source selector reads.
/// A row where the two disagree is a catalog bug, and this refuses it rather
/// than releasing a node on it.
pub fn current_disk_artifact_backup_ready(sandbox_alias: &str) -> String {
    matching_ready_backup_of_type_exists(sandbox_alias, "disk")
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

/// Returns a self-contained SQL fragment that is TRUE when a `started`
/// sandbox aliased `sandbox_alias`, whose started recovery failed, has no
/// snapshot that could restore it without a rollback and none that might
/// become one. It gates the give-up that marks such a sandbox lost once the
/// node in `{sandbox_alias}.active_node_id` is gone. `settle_param` is the
/// bind placeholder for the dead-upload settle window in seconds.
///
/// Deliberately stricter than started recovery's own source lookup, so the
/// give-up can never fail a sandbox that recovery, or the failed-recovery
/// convergence to `suspended`, could still bring back. Either of these keeps
/// the sandbox:
///
/// - **Any ready full snapshot that covers the activity clocks**, whatever
///   its origin or artifact identity, with a live storage location. Started
///   recovery accepts only the current suspend snapshot.
/// - **Any `creating` full snapshot that may still finalize.** One from
///   another node, or from no recorded node, can still become `ready`. One
///   from the gone node can't, since its upload worker died with the
///   instance, but a finalize RPC that left the node first may still be in
///   flight, so it counts until it has been quiet for the settle window, the
///   rule the drained-node give-up applies to the same rows.
///
/// A ready snapshot older than the last command or access doesn't count:
/// restoring it would silently roll the user back, which is why started
/// recovery refuses it.
///
/// Two `NOT EXISTS` conjuncts rather than `NOT (A OR B)`, so a caller that
/// scans many rows can plan each as an anti-join.
pub fn started_restore_source_absent(sandbox_alias: &str, settle_param: &str) -> String {
    format!(
        "NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'ready'\n\
                AND ssl.orphaned_at IS NULL\n\
                AND ({sandbox_alias}.last_command_at IS NULL\n\
                     OR sn.timestamp >= {sandbox_alias}.last_command_at)\n\
                AND ({sandbox_alias}.last_access_at IS NULL\n\
                     OR sn.timestamp >= {sandbox_alias}.last_access_at)\n\
         )\n\
         AND NOT EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
                AND (\n\
                    sn.source_node_id IS DISTINCT FROM {sandbox_alias}.active_node_id\n\
                    OR COALESCE(sn.last_attempt_at, sn.timestamp)\n\
                       >= NOW() - make_interval(secs => {settle_param}::BIGINT)\n\
                )\n\
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
/// — `pplx_space_sandbox_residency`'s docs point here for it — and it is the
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

pub mod execution_supersession;

#[cfg(test)]
mod drain_gate_differential_tests;
#[cfg(test)]
mod inflight_upload_shape_differential_tests;
#[cfg(test)]
mod queued_artifact_recoverable_tests;

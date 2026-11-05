//! Writes for a node whose Kubernetes Node is gone and whose EC2 instance the
//! cloud lookup reports gone: the sandboxes it held, the uploads it was
//! running, and the bookkeeping they leave behind.
//!
//! # What the verdict proves
//!
//! **The instance is gone, and its bytes with it.** A running VM, a transition
//! in flight, a published local artifact without a backup, an upload worker
//! and the copies it staged all lived in the instance's memory and on its
//! disk. No probe of the node is needed to know they are gone, and none is
//! possible. Nothing that lived there resolves on its own, so nothing here
//! waits for it to, with one exception: a `FinalizeArtifactBackup(READY)` RPC
//! that left the node before the instance died may still be in the API path,
//! which is what the `dead_upload_settle` of [`write_off_on_gone_node`] waits
//! out.
//!
//! **Why every write re-checks the node's uid.** `nodes.id` is a v5 UUID of
//! the Kubernetes node name and role, so a replacement that registers under
//! the same name gets the same id. Its registration bumps
//! `node_k8s.k8s_node_uid`, and the inventory reflector then resets
//! `drain_phase`. Every write here re-checks both against what the caller
//! observed at the cloud cross-check ([`NodeIdentityGuard`]), so the dead
//! node's verdict never lands on the replacement's sandboxes or uploads.
//!
//! **Statement guards are snapshot reads.** An `EXISTS` guard over `nodes` and
//! `node_k8s` inside a statement is atomic with that statement's write, but it
//! reads the statement's snapshot and locks nothing, so a drain-phase change
//! or uid bump that commits while a longer transaction runs is invisible to
//! it. A one-statement write needs nothing more, and neither does a write of
//! the `nodes` row itself, which serializes on that row's lock.
//! [`write_off_on_gone_node`]
//! runs several statements, so it first reads both guard rows `FOR SHARE`:
//! the shared lock holds those writers off until it commits, or waits for
//! theirs and reads the committed rows.
//!
//! # Callers
//!
//! Node drain's recovery of a `draining` node calls [`fail_residents`],
//! [`fail_inflight_uploads`], [`adopt_remote_snapshot`] and
//! [`mark_suspended_lost`]. Each is one statement on the executor its caller
//! passes, guarded in the statement by `drain_phase = 'draining'` and the uid.
//! They write the sandbox or snapshot row and nothing beside it: executions,
//! placement and the backup queue are left as they are. The node's uploads
//! are failed at once, with a reason the caller supplies.
//!
//! The drained-node terminator's give-up ([`WriteOff::UnrecoverableSuspended`])
//! and the lost started-sandbox sweep ([`WriteOff::Started`]) call
//! [`write_off_on_gone_node`] in a transaction they begin and commit. It locks
//! the guard rows, claims and flips the casualties, and writes off what each
//! leaves behind: its dead uploads once quiet past the settle, the node's
//! backup queue row, its active execution and its placement.
//!
//! Each caller keeps its loop, its per-tick cap and the events it emits.

use pplx_space_common_core::types::{
    NodeId, PositiveDuration, SandboxId, SnapshotId, SubstrateKind,
};

use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    current_suspend_snapshot_ready, inflight_suspend_upload_exists,
    inflight_suspend_upload_from_other_node_exists, latest_full_snapshot_ready,
    recoverable_suspend_snapshot_exists, snapshot_durable_for_source_release,
    started_restore_source_absent,
};
use sqlx::{Postgres, Transaction};

use crate::drain::secs_bind;

/// The `nodes.drain_phase` a [`NodeIdentityGuard`] expects.
#[derive(Clone, Copy, Debug)]
pub enum ExpectedPhase<'a> {
    /// `drain_phase = '<phase>'`.
    Is(&'static str),
    /// `drain_phase IS NOT DISTINCT FROM <placeholder>`, where a NULL bind
    /// means outside any drain.
    Bound(&'a str),
}

/// The re-check that the node a write acts on is still the one the caller
/// observed: its `nodes` row in the expected drain phase, and its `node_k8s`
/// row carrying the observed uid (see the module docs for why).
///
/// Fields are bind placeholders, such as `"$1"`. Two renderings exist, one
/// per kind of read: [`Self::exists`] for a guard inside a statement's
/// `WHERE`, and [`Self::node_row`] and [`Self::uid_row`] for statements of
/// their own, such as a `FOR SHARE` read or a diagnosis.
#[derive(Clone, Copy, Debug)]
pub struct NodeIdentityGuard<'a> {
    /// The node id.
    pub node: &'a str,
    /// The `node_k8s.k8s_node_uid` the caller observed; NULL matches NULL.
    pub uid: &'a str,
    pub phase: ExpectedPhase<'a>,
    /// The node's substrate, bound to [`SubstrateKind::Kubernetes`], or
    /// `None` when the guard doesn't check it.
    pub managed_by: Option<&'a str>,
}

impl NodeIdentityGuard<'_> {
    /// The predicate over a `nodes` row: its id, drain phase and substrate.
    pub fn node_predicate(&self) -> String {
        let phase = match self.phase {
            ExpectedPhase::Is(phase) => format!("drain_phase = '{phase}'"),
            ExpectedPhase::Bound(placeholder) => {
                format!("drain_phase IS NOT DISTINCT FROM {placeholder}")
            }
        };
        let managed_by = self
            .managed_by
            .map(|placeholder| format!(" AND managed_by = {placeholder}"))
            .unwrap_or_default();
        format!("id = {node} AND {phase}{managed_by}", node = self.node)
    }

    /// `SELECT 1` of the node's `nodes` row while [`Self::node_predicate`]
    /// holds.
    pub fn node_row(&self) -> String {
        format!("SELECT 1 FROM nodes WHERE {}", self.node_predicate())
    }

    /// `SELECT 1` of the node's `node_k8s` row while it carries the observed
    /// uid.
    pub fn uid_row(&self) -> String {
        format!(
            "SELECT 1 FROM node_k8s WHERE node_id = {node} AND k8s_node_uid IS NOT DISTINCT FROM {uid}",
            node = self.node,
            uid = self.uid,
        )
    }

    /// Both rows as `AND EXISTS` conjuncts, to append to a `WHERE`. They are
    /// uncorrelated, so they compose under any alias the statement uses.
    pub fn exists(&self) -> String {
        format!(
            "AND EXISTS ({node_row})\n            AND EXISTS ({uid_row})",
            node_row = self.node_row(),
            uid_row = self.uid_row(),
        )
    }
}

/// Node drain's guard: the node is still `draining`, under the uid at `uid`.
fn draining<'a>(node: &'a str, uid: &'a str) -> NodeIdentityGuard<'a> {
    NodeIdentityGuard {
        node,
        uid,
        phase: ExpectedPhase::Is("draining"),
        managed_by: None,
    }
}

// ---------------------------------------------------------------------------
// Node drain: one statement each, guarded in the statement.
// ---------------------------------------------------------------------------

/// `$1` = node id, `$2` = the resident lifecycles as `text[]`, `$3` = the
/// observed uid. Flip every sandbox still resident on the draining node
/// (`active_node_id = $1`) in one of `$2` to `failed`, returning `(id,
/// pre-failure lifecycle)` per row.
///
/// Every resident lifecycle died with the instance, so unlike the drain's
/// age-gated abandon sweep there is nothing to wait for. `suspended` is not a
/// resident lifecycle: whether a suspended row is recoverable depends on its
/// remote snapshot, which [`adopt_remote_snapshot`] and
/// [`mark_suspended_lost`] decide per row.
///
/// Two kinds of row are spared because they are recoverable without the
/// node, the same two the drain's abandon sweep spares:
/// - A **marked `restoring`** row reads from the durable snapshot; the
///   cluster-wide restore sweep re-drives it onto another node.
/// - A **marked `suspending`** row whose current cycle has recoverable
///   evidence ([`recoverable_suspend_snapshot_exists`]). With `active_node_id
///   = $1`, only the legacy current-cycle `creating`/`ready` branch can match
///   (the exact-identity branch requires `active_node_id IS NULL`). A `ready`
///   backup means the bytes are durable, and the stuck-snapshot reaper
///   settles a `creating` one, after which the row is failed here on a later
///   tick. A row spared on `ready` evidence has no automatic way forward: the
///   crash redrive requires `active_node_id IS NULL`, which the dead node never
///   cleared, and the restore sweep lists only `suspended` and `restoring`
///   rows. It holds the drain, visibly, until an operator advances it.
///
/// The data-modifying CTE returns the lifecycle each row had before the flip,
/// which a bare `RETURNING` can't, for the caller's events. `FOR UPDATE SKIP
/// LOCKED` skips a row whose completion CAS holds its lock; a later tick
/// re-evaluates it. Public so the drain's Postgres tests can run it directly.
pub fn fail_residents_query() -> String {
    let recoverable = recoverable_suspend_snapshot_exists("sandboxes");
    let identity = draining("$1", "$3").exists();
    format!(
        "WITH claimed AS (
         SELECT id, lifecycle
           FROM sandboxes
          WHERE active_node_id = $1
            AND lifecycle = ANY($2)
            AND (evacuation_restore_target IS NULL OR lifecycle = 'suspending')
            AND NOT (
                lifecycle = 'suspending'
                AND evacuation_restore_target IS NOT NULL
                AND {recoverable}
            )
            {identity}
          FOR UPDATE SKIP LOCKED
     )
     UPDATE sandboxes s
        SET lifecycle = 'failed',
            evacuation_restore_target = NULL
       FROM claimed c
      WHERE s.id = c.id
      RETURNING s.id, c.lifecycle"
    )
}

/// Fail every sandbox resident on the draining node `node_id` in one of
/// `lifecycles` ([`fail_residents_query`]). `lifecycles` are wire strings;
/// `expected_uid` is the uid the caller observed. Returns `(id, pre-failure
/// lifecycle)` per row flipped.
///
/// Writes an **inference**: a drain give-up on the cloud provider's verdict.
pub async fn fail_residents(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    lifecycles: &[&str],
    expected_uid: Option<&str>,
) -> Result<Vec<(SandboxId, String)>, sqlx::Error> {
    let query = fail_residents_query();
    sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(lifecycles)
        .bind(expected_uid)
        .fetch_all(executor)
        .await
}

/// `$1` = node id, `$2` = the observed uid, `$3` = the failure reason. Fail
/// every `creating` full upload sourced from the draining node, at once:
/// its worker died with the instance, so no finalize can arrive. This is the
/// flip the stuck-snapshot reaper applies after its age window. The drain's
/// blocker count counts these rows whatever their sandbox's lifecycle, so
/// without it a failed resident's upload holds the node in `draining` until
/// the reaper runs, or forever where it is disabled. `failure_reason` is
/// stamped only when still NULL, keeping a reason the worker recorded, as the
/// reaper does. Returns `(snapshot_id, sandbox_id)` per row flipped. Public
/// so the drain's Postgres tests can run it directly.
pub fn fail_inflight_uploads_query() -> String {
    let identity = draining("$1", "$2").exists();
    format!(
        "UPDATE snapshots
            SET state = 'failed',
                failure_reason = COALESCE(failure_reason, $3)
          WHERE state = 'creating'
            AND type = 'full'
            AND source_node_id = $1
            {identity}
          RETURNING id, sandbox_id"
    )
}

/// Fail every in-flight full upload from the draining node `node_id`
/// ([`fail_inflight_uploads_query`]), recording `failure_reason` where none
/// is recorded. Returns `(snapshot_id, sandbox_id)` per row flipped.
pub async fn fail_inflight_uploads(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    expected_uid: Option<&str>,
    failure_reason: &str,
) -> Result<Vec<(SnapshotId, SandboxId)>, sqlx::Error> {
    let query = fail_inflight_uploads_query();
    sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(expected_uid)
        .bind(failure_reason)
        .fetch_all(executor)
        .await
}

/// `$1` = sandbox id, `$2` = node id, `$3` = the observed uid. Fail a
/// `suspended` sandbox pinned to the draining node.
///
/// **Any ready full snapshot refuses**, whatever its freshness or artifact
/// identity. The drain's artifact-missing give-up
/// ([`crate::drain::mark_sandbox_lost_query`]) refuses only on
/// [`snapshot_durable_for_source_release`], whose unmarked branch is anchored
/// to placement freshness, so a ready snapshot made placement-stale by a
/// stuck restore's pod bump doesn't refuse it. Here that would lose data: the
/// drain tries [`adopt_remote_snapshot`] first and falls back to this, and
/// the adopt returns 0 for such a row inside its settle window although it
/// would adopt it on a later tick. When a durable copy of any cycle exists,
/// the row stays parked, visible, for the adopt or an operator. With no ready
/// snapshot at all neither portability branch can hold, so this subsumes
/// `NOT durable`.
///
/// **An upload in flight from any node refuses** ([`inflight_suspend_upload_exists`]),
/// including one from the gone node: the stuck-snapshot reaper or
/// [`fail_inflight_uploads`] fails it first. Public so the drain's Postgres
/// tests can run it directly.
pub fn mark_suspended_lost_query() -> String {
    let inflight_upload = inflight_suspend_upload_exists("sandboxes");
    let identity_guards = draining("$2", "$3").exists();
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE id = $1
            AND lifecycle = 'suspended'
            AND target_node_id = $2
            AND NOT EXISTS (
                SELECT 1
                  FROM snapshots sn
                  JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                 WHERE sn.sandbox_id = sandboxes.id
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
            )
            AND NOT {inflight_upload}
            {identity_guards}"
    )
}

/// Fail a `suspended` sandbox pinned to the draining node `source_node_id`
/// ([`mark_suspended_lost_query`]). Returns the rows flipped, `0` or `1`.
///
/// Writes an **inference**: a drain give-up on the cloud provider's verdict.
pub async fn mark_suspended_lost(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    expected_uid: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let query = mark_suspended_lost_query();
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(expected_uid)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

/// `$1` = sandbox id, `$2` = node id, `$3` = the observed uid, `$4` = settle
/// seconds. Make a pinned local-only `suspended` row on the draining node
/// durable from its remote snapshot: the drain's reupload adopt
/// ([`crate::drain::adopt_remote_snapshot_for_drain_query`]) with the node
/// identity guard and a current-suspend-cycle anchor
/// ([`current_suspend_snapshot_ready`]).
///
/// The reupload adopt runs only with proof that the VM is restoring *from*
/// the snapshot, which makes the snapshot canonical, so
/// [`latest_full_snapshot_ready`] alone is safe there. This adopt runs for
/// every pinned local-only row of the node, without that proof. For a legacy
/// row whose last wake, modify and re-suspend never registered a snapshot,
/// the newest ready snapshot is the prior cycle's, and adopting it would
/// silently roll the sandbox back. The anchor (`sn.timestamp >=
/// lifecycle_changed_at`, the crash redrive's gate) refuses that: the row
/// stays parked, or with no ready snapshot at all [`mark_suspended_lost`]
/// fails it, an explicit failure in place of a silent rollback. For a row
/// that carries an artifact identity, the exact-artifact branch already pins
/// the snapshot, so the anchor only tightens the legacy branch. Public so the
/// drain's Postgres tests can run it directly.
pub fn adopt_remote_snapshot_query() -> String {
    let inflight_upload = inflight_suspend_upload_exists("s");
    let latest_ready = latest_full_snapshot_ready("s");
    let current_cycle_ready = current_suspend_snapshot_ready("s");
    let identity_guards = draining("$2", "$3").exists();
    format!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = NULL,
                current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL,
                evacuation_restore_target = NULL,
                placement_changed_at = NULL
          WHERE s.id = $1
            AND s.lifecycle = 'suspended'
            AND s.target_node_id = $2
            AND s.lifecycle_changed_at < NOW() - make_interval(secs => $4::BIGINT)
            AND NOT {inflight_upload}
            AND {latest_ready}
            AND {current_cycle_ready}
            {identity_guards}"
    )
}

/// Make a pinned local-only `suspended` sandbox on the draining node durable
/// from its remote snapshot ([`adopt_remote_snapshot_query`]), once it has
/// rested in `suspended` for `settle`. Returns the rows flipped, `0` or `1`.
///
/// Writes an **inference**: a drain give-up that adopts the remote snapshot in
/// place of the local artifact that died with the node. The lifecycle stays
/// `suspended`.
pub async fn adopt_remote_snapshot(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    settle: PositiveDuration,
    expected_uid: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(adopt_remote_snapshot_query(),)))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(expected_uid)
        .bind(secs_bind(settle))
        .execute(executor)
        .await?
        .rows_affected();
    Ok(flipped)
}

// ---------------------------------------------------------------------------
// The transactional write-off.
// ---------------------------------------------------------------------------

/// Which sandboxes a [`write_off_on_gone_node`] gives up, and how it knows the
/// node is gone.
#[derive(Clone, Copy, Debug)]
pub enum WriteOff<'a> {
    /// The drained-node terminator's give-up: every `suspended` row pinned to
    /// the `drained` Kubernetes node via `target_node_id` with no recoverable
    /// copy anywhere. The node row must still be `drained` and carry
    /// `expected_uid`.
    UnrecoverableSuspended { expected_uid: Option<&'a str> },
    /// The lost started-sandbox sweep's give-up: one `started` row whose
    /// started recovery failed at execution `generation`.
    Started {
        sandbox_id: SandboxId,
        generation: i64,
        source: StartedSource<'a>,
    },
}

/// How the lost started-sandbox sweep found the source node gone.
#[derive(Clone, Copy, Debug)]
pub enum StartedSource<'a> {
    /// The node row was deleted, and must still be absent.
    RowDeleted,
    /// The Kubernetes node row is still registered; it must still be outside
    /// any drain and carry `expected_uid`.
    Registered { expected_uid: Option<&'a str> },
}

/// Give up the sandboxes `write_off` names on the gone node `node_id`, and
/// write off what they leave behind. Returns the ids it flipped to `failed`;
/// empty when a guard refused. Runs on the caller's transaction, which the
/// caller commits; on a refusal nothing was written.
///
/// In order, each step under the previous one's locks:
///
/// 1. **The node.** Its guard rows are read `FOR SHARE` (see the module docs).
///    A [`StartedSource::RowDeleted`] node must have no row; `FOR SHARE` on a
///    missing row locks nothing, so a registration that commits after this
///    read isn't fenced, but such a row is a new instance under the old name
///    and can't make the flip wrong. Pod placement and the terminator lock the
///    node rows before sandbox rows too, so this order can't deadlock against
///    them.
/// 2. **The sandboxes.** [`WriteOff::UnrecoverableSuspended`] claims its
///    candidates with [`unrecoverable_suspended_claim_query`].
///    [`WriteOff::Started`] locks its row and requires it `started`: started
///    recovery locks the row before it re-arms the execution, so a wake either
///    commits first and the flip's execution check refuses, or waits and then
///    finds the row `failed`.
/// 3. **Their in-flight uploads**, `FOR UPDATE`, so a finalize that commits
///    first is visible to the flip and spares the sandbox. The terminator locks
///    only the node's own uploads of its candidates, the lost-started sweep
///    every upload of its sandbox. A finalize racing the transaction either
///    waits and then refuses on its own state guard, or deadlocks against it
///    and one of the two aborts; a flip retries next tick, a finalize from the
///    gone node doesn't, and its uploaded bytes are left for the storage
///    sweeper.
/// 4. **The flip**: [`mark_unrecoverable_suspended_lost_query`] or
///    [`mark_started_lost`], each re-checking every condition its caller
///    decided on.
/// 5. **The write-off** of each flipped sandbox:
///    - Its `creating` full uploads from the node, quiet past
///      `dead_upload_settle`, are failed. Left `creating`, one reads as a live
///      upload forever and the drained-node snapshot guard refuses the reap on
///      it. No failure reason is recorded.
///    - Its `node_local_backup_pending` row on the node is deleted. A gone
///      node sends no census to replace its set, and the table's only foreign
///      key is on `sandbox_id`. A row on another node is that node's pending
///      work and stays. This takes the queue row after the sandbox row, while
///      the census publish takes the queue row first; that can deadlock only
///      against a census from the gone node itself.
///    - Its active execution is retired `forgotten`, the pod-tracking
///      write-off state, with its pod IP cleared: the pod died with the node.
///      The suspended-execution reaper, which retires such rows otherwise,
///      scans only `suspended` rows, so it never would once the row is
///      `failed`. The flip's row lock serializes this against the reaper's
///      `FOR UPDATE` and lifecycle re-check and against wake's `suspended ->
///      restoring` CAS, so either order commutes.
///    - It is unplaced: `unplaced`, no `active_node_id`, so it stops pinning
///      the node row the terminator deletes next. `target_node_id` keeps the
///      last place it lived, and `placement_changed_at`, which tracks
///      placement for snapshot freshness, isn't bumped.
///
///    Port exposures are left alone: re-queueing them for a terminal sandbox
///    creates work that can never apply.
pub async fn write_off_on_gone_node(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    write_off: WriteOff<'_>,
    dead_upload_settle: Duration,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let node_gone = match write_off {
        WriteOff::UnrecoverableSuspended { expected_uid } => {
            node_identity_holds(tx, node_id, Some("drained"), expected_uid).await?
        }
        WriteOff::Started {
            source: StartedSource::Registered { expected_uid },
            ..
        } => node_identity_holds(tx, node_id, None, expected_uid).await?,
        WriteOff::Started {
            source: StartedSource::RowDeleted,
            ..
        } => node_row_absent(tx, node_id).await?,
    };
    if !node_gone {
        return Ok(Vec::new());
    }

    let ids = match write_off {
        WriteOff::UnrecoverableSuspended { expected_uid } => {
            let claim = unrecoverable_suspended_claim_query();
            let claimed: Vec<SandboxId> =
                sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(claim.as_str())))
                    .bind(node_id)
                    .fetch_all(&mut **tx)
                    .await?;
            if claimed.is_empty() {
                return Ok(Vec::new());
            }
            let _locked_dead_uploads: Vec<SnapshotId> = sqlx::query_scalar(sql!(
                "SELECT id FROM snapshots
              WHERE sandbox_id = ANY($1)
                AND source_node_id = $2
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
            ))
            .bind(&claimed)
            .bind(node_id)
            .fetch_all(&mut **tx)
            .await?;
            mark_unrecoverable_suspended_lost(
                tx,
                node_id,
                expected_uid,
                &claimed,
                dead_upload_settle,
            )
            .await?
        }
        WriteOff::Started {
            sandbox_id,
            generation,
            ..
        } => {
            let locked: Option<String> = sqlx::query_scalar(sql!(
                "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
            ))
            .bind(sandbox_id)
            .fetch_optional(&mut **tx)
            .await?;
            if locked.as_deref() != Some("started") {
                return Ok(Vec::new());
            }
            let _locked_uploads: Vec<SnapshotId> = sqlx::query_scalar(sql!(
                "SELECT id FROM snapshots
              WHERE sandbox_id = $1
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
            ))
            .bind(sandbox_id)
            .fetch_all(&mut **tx)
            .await?;
            if mark_started_lost(tx, sandbox_id, node_id, generation, dead_upload_settle).await? {
                vec![sandbox_id]
            } else {
                Vec::new()
            }
        }
    };
    if !ids.is_empty() {
        write_off_lost_sandboxes(tx, node_id, &ids, dead_upload_settle).await?;
    }
    Ok(ids)
}

/// The `FOR SHARE` reads of the node's identity guard: `$1` = node id, then
/// `$2` = the expected drain phase and `$3` = [`SubstrateKind::Kubernetes`]
/// in the `nodes` read, and `$2` = the observed uid in the `node_k8s` read.
/// Each read is its own statement, so `$2` binds a different value in each.
const LOCKED_IDENTITY: NodeIdentityGuard<'static> = NodeIdentityGuard {
    node: "$1",
    uid: "$2",
    phase: ExpectedPhase::Bound("$2"),
    managed_by: Some("$3"),
};

/// Whether the Kubernetes node row is still in `drain_phase` (`None`: outside
/// any drain) and still carries `expected_uid`, read `FOR SHARE`.
async fn node_identity_holds(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    drain_phase: Option<&str>,
    expected_uid: Option<&str>,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static LOCK_NODE_ROW: LazyLock<String> =
        LazyLock::new(|| sql!(format!("{} FOR SHARE", LOCKED_IDENTITY.node_row())));
    static LOCK_UID_ROW: LazyLock<String> =
        LazyLock::new(|| sql!(format!("{} FOR SHARE", LOCKED_IDENTITY.uid_row())));
    let node: Option<i32> = sqlx::query_scalar(sqlx::AssertSqlSafe(LOCK_NODE_ROW.as_str()))
        .bind(node_id)
        .bind(drain_phase)
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&mut **tx)
        .await?;
    if node.is_none() {
        return Ok(false);
    }
    let uid: Option<i32> = sqlx::query_scalar(sqlx::AssertSqlSafe(LOCK_UID_ROW.as_str()))
        .bind(node_id)
        .bind(expected_uid)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(uid.is_some())
}

/// Whether the node has no `nodes` row.
async fn node_row_absent(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
) -> Result<bool, sqlx::Error> {
    let row: Option<i32> = sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR SHARE"))
        .bind(node_id)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(row.is_none())
}

/// `$1` = node id. The drained-node give-up's claim: the node's pinned
/// `suspended` rows without a durable copy, locked `FOR UPDATE` in `id` order.
///
/// The claim comes before the flip's in-flight veto is evaluated. A
/// concurrent `BeginArtifactBackup` insert holds only a `KEY SHARE` foreign
/// key lock on the sandbox row, which doesn't conflict with the flip's own
/// `NO KEY UPDATE` lock, so without the claim the veto could run before the
/// insert commits and fail a sandbox whose durable copy is about to start
/// uploading. `FOR UPDATE` does conflict with `KEY SHARE`: the insert either
/// committed first, and the flip's fresh snapshot sees it, or waits for the
/// commit. `ORDER BY id` keeps the lock order deterministic against other
/// multi-row lockers. An insert that lands entirely after the commit is out of
/// reach, since `BeginArtifactBackup` doesn't check the lifecycle.
///
/// It claims only rows without a durable copy because those are the only rows
/// [`mark_unrecoverable_suspended_lost_query`] can flip: the flip re-checks
/// `AND NOT {durable}` on the claimed ids, so locking a durable row buys
/// nothing. Durability can change between the claim and the flip either way:
///
/// * Not durable at the claim, durable by the flip: claimed, and spared by
///   the flip's own `NOT {durable}`.
/// * Durable at the claim, not durable by the flip (its ready copy's storage
///   location was orphaned, say): not claimed, so this tick can't flip it. It
///   still blocks the reap, which counts a `suspended` row that is `NOT
///   {durable}` by the same predicate, so the next tick's refused delete runs
///   the give-up again and claims it. That one-tick miss is accepted over
///   locking every durable row on every tick.
///
/// The durability check is a scalar subquery, `NOT (SELECT …)`: a bare `NOT
/// EXISTS` is pullable, and the planner may realise it as a hash or merge
/// anti-join over the whole `snapshots` ⋈ `snapshot_storage_locations`
/// product. As a scalar subquery it stays a per-row `SubPlan` whose
/// `sn.sandbox_id = s.id` is an index probe. The statement locks `sandboxes`
/// rows and nothing else: `FOR UPDATE` locks the rows of its own `FROM` list,
/// and the durability check is a sublink. Public so the terminator's lock and
/// plan tests run this statement rather than a copy.
pub fn unrecoverable_suspended_claim_query() -> String {
    let durable = snapshot_durable_for_source_release("s");
    format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.lifecycle = 'suspended'
            AND s.target_node_id = $1
            AND NOT (SELECT {durable})
          ORDER BY s.id
          FOR UPDATE"
    )
}

/// `$1` = node id, `$2` = the observed uid, `$3` = the ids the claim locked,
/// `$4` = the dead-upload settle in seconds, `$5` =
/// [`SubstrateKind::Kubernetes`]. The drained-node give-up's flip: every
/// claimed `suspended` row pinned to the node with no recoverable copy to
/// `failed`, returning the ids flipped.
///
/// It differs from the drain's artifact-missing
/// [`crate::drain::mark_sandbox_lost_query`] in two ways, both because the
/// instance is gone:
///
/// * No node probe precedes it: "no durable snapshot" alone proves the
///   sandbox unrecoverable.
/// * Only an upload from **another** node vetoes
///   ([`inflight_suspend_upload_from_other_node_exists`]): one still
///   `creating` from the gone node can never finalize, and letting it veto
///   would park the sandbox forever. Its own uploads defer the flip only
///   until quiet past the settle.
///
/// The durability test is the marker-aware
/// [`snapshot_durable_for_source_release`] the terminator's snapshot guard
/// uses, so "blocks the reap" and "eligible for the give-up" can't drift: a
/// durably portable row neither blocks nor flips, a local-only one does both.
/// `suspending` and `restoring` rows are out of scope; they keep blocking and
/// wait for their own sweepers.
///
/// The node guards in it are snapshot reads; run it only in
/// [`write_off_on_gone_node`], after the `FOR SHARE` reads and the claim.
/// Public so the terminator's Postgres tests can run it directly.
pub fn mark_unrecoverable_suspended_lost_query() -> String {
    let durable = snapshot_durable_for_source_release("sandboxes");
    let inflight_from_other_node =
        inflight_suspend_upload_from_other_node_exists("sandboxes", "$1");
    let identity = NodeIdentityGuard {
        node: "$1",
        uid: "$2",
        phase: ExpectedPhase::Is("drained"),
        managed_by: Some("$5"),
    }
    .exists();
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE lifecycle = 'suspended'
            AND target_node_id = $1
            -- Settle guard on same-node creating uploads ($4 = quiet
            -- seconds, see DEAD_UPLOAD_SETTLE): EC2 'terminated' proves the
            -- worker is dead but not that a READY finalize for already-
            -- durable bytes isn't still in the API path. A row heartbeated
            -- (or created) inside the window defers the flip; the sandbox
            -- keeps blocking the reap and later ticks re-check.
            AND NOT EXISTS (
                SELECT 1 FROM snapshots sn
                 WHERE sn.sandbox_id = sandboxes.id
                   AND sn.source_node_id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'creating'
                   AND COALESCE(sn.last_attempt_at, sn.timestamp)
                       >= NOW() - make_interval(secs => $4::BIGINT)
            )
            -- Only rows the caller FOR UPDATE-claimed may flip. Under READ
            -- COMMITTED the claim only fences rows that matched at its own
            -- snapshot; without this restriction a row entering the
            -- candidate set between the claim and this statement would flip
            -- un-fenced, re-opening the FK-lock race with a concurrent
            -- BeginArtifactBackup insert. Late arrivals wait for the next
            -- tick's claim instead.
            AND id = ANY($3)
            AND NOT {durable}
            AND NOT {inflight_from_other_node}
            {identity}
          RETURNING id"
    )
}

async fn mark_unrecoverable_suspended_lost(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    expected_uid: Option<&str>,
    claimed: &[SandboxId],
    dead_upload_settle: Duration,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let query = mark_unrecoverable_suspended_lost_query();
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(expected_uid)
        .bind(claimed)
        .bind(dead_upload_settle.as_secs().cast_signed())
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&mut **tx)
        .await
}

/// The lost started-sandbox sweep's flip of `sandboxes` from `started` to
/// `failed`. Returns whether the row flipped.
///
/// Every condition the sweep decided on is re-checked in the `WHERE`: the row
/// is still a `started` Kubernetes sandbox on `source_node_id`, its active
/// execution is still the `recovery_failed` one at `generation` (a wake that
/// re-armed it to `recovering`, or replaced it, refuses) and still tracks no
/// pod, and no snapshot could restore it without a rollback
/// ([`started_restore_source_absent`], with `dead_upload_settle` as its settle
/// window).
///
/// Run it only in [`write_off_on_gone_node`], under the node and sandbox
/// locks; public so the sweep's Postgres tests can run it directly.
pub async fn mark_started_lost(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    generation: i64,
    dead_upload_settle: Duration,
) -> Result<bool, sqlx::Error> {
    /// Built once: every input is a literal or a bind placeholder.
    static MARK_STARTED_LOST: LazyLock<String> = LazyLock::new(|| {
        let no_restore_source = started_restore_source_absent("s", "$4");
        sql!(
            format!(
                "UPDATE sandboxes s
                    SET lifecycle = 'failed'
                  WHERE s.id = $1
                    AND s.managed_by = $5
                    AND s.lifecycle = 'started'
                    AND s.active_node_id = $2
                    AND EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.active
                           AND e.state = 'recovery_failed'
                           AND e.generation = $3
                           AND NULLIF(BTRIM(e.pod_uid), '') IS NULL
                           AND e.pod_ip IS NULL
                           AND e.k8s_node_name IS NULL
                    )
                    AND {no_restore_source}"
            )
            .as_str()
        )
    });
    let flipped = sqlx::query(sqlx::AssertSqlSafe(MARK_STARTED_LOST.as_str()))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(generation)
        .bind(dead_upload_settle.as_secs().cast_signed())
        .bind(SubstrateKind::Kubernetes)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    Ok(flipped == 1)
}

/// Step 5 of [`write_off_on_gone_node`] for `ids`, just flipped to `failed`.
async fn write_off_lost_sandboxes(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    ids: &[SandboxId],
    dead_upload_settle: Duration,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE snapshots
            SET state = 'failed'
          WHERE sandbox_id = ANY($1)
            AND source_node_id = $2
            AND type = 'full'
            AND state = 'creating'
            AND COALESCE(last_attempt_at, timestamp)
                < NOW() - make_interval(secs => $3::BIGINT)"
    ))
    .bind(ids)
    .bind(node_id)
    .bind(dead_upload_settle.as_secs().cast_signed())
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM node_local_backup_pending
          WHERE node_id = $1 AND sandbox_id = ANY($2)"
    ))
    .bind(node_id)
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'forgotten',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE sandbox_id = ANY($1)
            AND active"
    ))
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    // Guarded only by `lifecycle = 'failed'`: the rows were flipped in this
    // transaction, which holds their locks.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'unplaced',
                active_node_id = NULL
          WHERE id = ANY($1)
            AND lifecycle = 'failed'"
    ))
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

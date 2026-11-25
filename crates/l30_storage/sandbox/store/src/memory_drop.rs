//! The memory drop: a suspended Kubernetes sandbox whose node is gone keeps
//! its disk and loses its memory, done in Postgres and object storage alone.
//!
//! A suspended Kubernetes sandbox is unplaced, and its only copy is the
//! durable full suspend artifact its pin names. Its memory and VM state are
//! in the snapshot format of the runtime that captured it, so retiring that
//! runtime leaves them unloadable. Its rootfs isn't tied to the runtime. When
//! the node that last ran it is gone, no node holds a local copy, so the
//! control plane can write a disk-only artifact itself: seal the row, copy the
//! artifact's rootfs into a new disk prefix, then, in one transaction, re-pin
//! the row to that disk artifact and lift the seal. The row stays `suspended`
//! throughout, and its lifecycle clock (`lifecycle_changed_at`) doesn't move.
//!
//! The seal is the row's restore lease (`restore_lease_token`,
//! `restore_lease_expires_at`), taken without moving the lifecycle ([`seal`]),
//! renewed while the copy runs ([`renew`]) and cleared by [`commit`] or
//! [`release`]. The user restore claim already refuses a live lease, and the
//! `sandboxes_clear_restore_lease` trigger keeps a lease across writes that
//! keep the row `suspended`, so the seal holds until the drop lifts it, it
//! expires, or a write moves the row's lifecycle, which clears it. A sealed row
//! is one that's `suspended` with a live lease ([`sealed`]); fork admission,
//! the delete and the restore claims refuse it with a retryable error.
//!
//! The full artifact's snapshot rows and storage locations aren't touched:
//! they stay `ready` and attached, so retention keeps them and an operator
//! can point the row back at them. The candidate requires the pinned full
//! snapshot to be the sandbox's newest full snapshot, so after the drop the
//! retained artifact is the newest full snapshot of a row pinned to a disk
//! that [`dropped`] reports, and no restore claim takes such a row
//! (`restore_lease`), so no newer full snapshot appears while it's dropped.
//!
//! This module owns the reads and the guarded write on `sandboxes`. The
//! api's sandbox controller drives the copy between them and owns the
//! transaction; the snapshot rows belong to `pplx-space-snapshot-store`.

use std::time::Duration;

use pplx_space_accounts::OrgId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    no_newer_full_snapshot_ignoring_failed_retry, snapshot_is_current_local_artifact,
};
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use sqlx::{PgExecutor, Postgres, Transaction};
use uuid::Uuid;

/// The durable artifact a memory drop copies from, and the pin the commit
/// re-checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryDropCandidate {
    pub org_id: OrgId,
    /// The row's pin, a `full` `suspend` artifact.
    pub pinned_artifact_id: LocalArtifactId,
    /// The newest ready snapshot row of that artifact, which is also the
    /// sandbox's newest full snapshot.
    pub full_snapshot_id: SnapshotId,
    /// Its storage location's connection string, unvalidated.
    pub storage_connection_string: String,
}

/// Whether to read the candidate under a row lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateRead {
    /// A plain read, for deciding whether to start copying.
    Plain,
    /// `FOR UPDATE OF s`, inside the transaction that seals the row. Every
    /// activation claims the row with a `suspended` → `restoring` CAS, so
    /// holding the lock fences them until the seal is decided. A fork reads
    /// the full artifact without claiming the row; the caller fences forks
    /// with the fork-source lock (`crate::fork_source_lifecycle`) before this
    /// one.
    Locked,
}

/// Why a row isn't a memory-drop candidate, for the operator who asked for
/// one. The first condition that fails, in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryDropRefusal {
    /// No sandbox row has this id.
    NotFound,
    /// The row isn't on the Kubernetes substrate.
    NotKubernetes,
    /// The row's memory is already dropped: it's `suspended` and pinned to a
    /// disk.
    AlreadyDropped,
    /// The row isn't `suspended`.
    NotSuspended,
    /// Another memory drop holds the row's seal: it's `suspended` with a live
    /// restore lease. Retry once that drop finishes or its seal expires.
    Sealed,
    /// The row is placed, or a restore is about to take it: an active node,
    /// an evacuation claim or an active execution.
    Placed,
    /// The row is volume-encrypted.
    Encrypted,
    /// The pin isn't a `full` `suspend` artifact.
    NotFullSuspendPin,
    /// No ready, unorphaned snapshot row holds the pinned artifact.
    NoDurableCopy,
    /// A newer full snapshot of the sandbox exists, other than a failed retry
    /// of the pinned artifact, so the pinned one wouldn't be the newest full
    /// snapshot retention keeps.
    NewerFullSnapshot,
    /// The node the row last ran on is still registered, or no execution
    /// names one.
    NodeNotGone,
}

impl MemoryDropRefusal {
    /// A stable snake_case reason for the API and the audit record.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::NotKubernetes => "not_kubernetes",
            Self::AlreadyDropped => "already_dropped",
            Self::NotSuspended => "not_suspended",
            Self::Sealed => "memory_drop_in_progress",
            Self::Placed => "placed",
            Self::Encrypted => "encrypted",
            Self::NotFullSuspendPin => "not_full_suspend_pin",
            Self::NoDurableCopy => "no_durable_copy",
            Self::NewerFullSnapshot => "newer_full_snapshot",
            Self::NodeNotGone => "node_not_gone",
        }
    }
}

/// The conditions on `s`, the `sandboxes` row, that don't involve the pinned
/// artifact's snapshot rows. [`candidate`] and [`refusal`] both read them, so
/// the two can't drift.
fn row_conditions() -> [(&'static str, String); 7] {
    [
        ("kubernetes", "s.managed_by = $2".to_string()),
        (
            "dropped",
            "s.lifecycle = 'suspended' AND s.current_local_artifact_kind = 'disk'".to_string(),
        ),
        ("suspended", "s.lifecycle = 'suspended'".to_string()),
        // An expired seal is a drop that died mid-copy; a retry takes it over.
        ("unsealed", LEASE_EXPIRED.to_string()),
        (
            "unplaced",
            "s.active_node_id IS NULL
            AND s.evacuation_restore_target IS NULL
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_k8s_executions ske
                 WHERE ske.sandbox_id = s.id
                   AND ske.active
            )"
            .to_string(),
        ),
        (
            "unencrypted",
            "NOT COALESCE(s.volume_encryption, FALSE)".to_string(),
        ),
        (
            "full_suspend_pin",
            "s.current_local_artifact_kind = 'full'
            AND s.current_local_artifact_origin = 'suspend'"
                .to_string(),
        ),
    ]
}

/// No live restore lease on `s`: none, or one past its expiry.
const LEASE_EXPIRED: &str =
    "(s.restore_lease_expires_at IS NULL OR s.restore_lease_expires_at <= NOW())";

/// The node checks: neither the placement's node nor the newest execution's
/// node has a `node_k8s` mapping, and an execution names a node.
const NODE_GONE: &str = "NOT EXISTS (
                SELECT 1 FROM node_k8s nk WHERE nk.node_id = s.target_node_id
            )
            AND EXISTS (
                SELECT 1
                  FROM sandbox_k8s_executions ske
                 WHERE ske.sandbox_id = s.id
                   AND ske.k8s_node_name IS NOT NULL
            )
            AND NOT EXISTS (
                SELECT 1
                  FROM node_k8s nk
                 WHERE nk.k8s_node_name = (
                     SELECT ske.k8s_node_name
                       FROM sandbox_k8s_executions ske
                      WHERE ske.sandbox_id = s.id
                        AND ske.k8s_node_name IS NOT NULL
                      ORDER BY ske.generation DESC
                      LIMIT 1
                 )
            )";

/// The conditions on `sn` and `ssl`, a snapshot row of the pinned artifact
/// and its location: durable, and the newest full snapshot.
fn durable_copy() -> String {
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    format!(
        "{pinned}
            AND sn.type = 'full'
            AND sn.state = 'ready'
            AND ssl.orphaned_at IS NULL"
    )
}

/// Read the memory-drop candidate of one sandbox. `None` when the row isn't
/// eligible; [`refusal`] says why.
///
/// Eligible means all of:
///
/// * a Kubernetes row, `suspended`, with no live restore lease (no other drop
///   holds its seal), no active node, no evacuation claim and no active
///   execution: nothing is restoring it or about to;
/// * not volume-encrypted. A BYOK artifact is sealed to its role, so its
///   rootfs can't be relabelled as a disk artifact by copying it;
/// * pinned to a `full` `suspend` artifact with a ready, unorphaned snapshot
///   row: the bytes the pin names are durable;
/// * that snapshot row is the sandbox's newest full snapshot, ignoring a
///   failed retry of the same artifact. Retention's keep-N floor always keeps
///   the newest ready full snapshot, so the artifact this retains survives for
///   as long as the row stays dropped;
/// * the node the row last ran on is gone: neither its placement's node nor
///   the newest execution's node has a `node_k8s` mapping. The terminate path
///   deletes the `nodes` row, which cascades to `node_k8s`. With the node gone,
///   no node holds a local copy that a later restore could prefer over the
///   disk artifact this writes, and none can publish a newer one. An
///   execution must name a node: without one, the absence checks would pass
///   on no evidence.
///
/// Newest snapshot first, with an `sn.id` tiebreak: a retried backup of one
/// artifact mints a fresh `snapshots` row per attempt, so the pin can match
/// several rows.
pub async fn candidate(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    read: CandidateRead,
) -> Result<Option<MemoryDropCandidate>, sqlx::Error> {
    let rows = row_conditions()
        .into_iter()
        .filter(|(name, _)| *name != "dropped")
        .map(|(_, condition)| condition)
        .collect::<Vec<_>>()
        .join("\n            AND ");
    let durable = durable_copy();
    let newest = no_newer_full_snapshot_ignoring_failed_retry("s", "sn");
    // Only the trailing lock clause differs between the two reads.
    let lock = match read {
        CandidateRead::Plain => "",
        CandidateRead::Locked => "FOR UPDATE OF s",
    };
    let query = sql!(format!(
        "SELECT s.org_id, s.current_local_artifact_id, sn.id, ssl.storage_connection_string
           FROM sandboxes s
           JOIN snapshots sn ON sn.sandbox_id = s.id
           JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
          WHERE s.id = $1
            AND {rows}
            AND {durable}
            AND {newest}
            AND {NODE_GONE}
          ORDER BY sn.timestamp DESC, sn.id DESC
          LIMIT 1
          {lock}"
    ));
    type Row = (OrgId, LocalArtifactId, SnapshotId, String);
    let row: Option<Row> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .bind(sandbox_id)
        // A bind, not a `'k8s'` literal: the label's spelling stays with
        // `SubstrateKind`.
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(executor)
        .await?;
    Ok(row.map(
        |(org_id, pinned_artifact_id, full_snapshot_id, storage_connection_string)| {
            MemoryDropCandidate {
                org_id,
                pinned_artifact_id,
                full_snapshot_id,
                storage_connection_string,
            }
        },
    ))
}

/// Why the row isn't a [`candidate`], or `None` when it is one. A plain read:
/// the answer can change as soon as it returns, so the caller reports it and
/// decides nothing else from it.
pub async fn refusal(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<MemoryDropRefusal>, sqlx::Error> {
    let columns = row_conditions()
        .into_iter()
        .map(|(name, condition)| format!("COALESCE(({condition}), FALSE) AS {name}"))
        .collect::<Vec<_>>()
        .join(",\n                ");
    let durable = durable_copy();
    let newest = no_newer_full_snapshot_ignoring_failed_retry("s", "sn");
    let query = sql!(format!(
        "SELECT {columns},
                EXISTS (
                    SELECT 1
                      FROM snapshots sn
                      JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                     WHERE sn.sandbox_id = s.id
                       AND {durable}
                ) AS durable,
                EXISTS (
                    SELECT 1
                      FROM snapshots sn
                      JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                     WHERE sn.sandbox_id = s.id
                       AND {durable}
                       AND {newest}
                ) AS newest,
                COALESCE(({NODE_GONE}), FALSE) AS node_gone
           FROM sandboxes s
          WHERE s.id = $1"
    ));
    type Row = (bool, bool, bool, bool, bool, bool, bool, bool, bool, bool);
    let row: Option<Row> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .bind(sandbox_id)
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(executor)
        .await?;
    let Some((
        kubernetes,
        dropped,
        suspended,
        unsealed,
        unplaced,
        unencrypted,
        full_suspend_pin,
        durable,
        newest,
        node_gone,
    )) = row
    else {
        return Ok(Some(MemoryDropRefusal::NotFound));
    };
    // The first failing condition, most fundamental first.
    let checks = [
        (kubernetes, MemoryDropRefusal::NotKubernetes),
        (!dropped, MemoryDropRefusal::AlreadyDropped),
        (suspended, MemoryDropRefusal::NotSuspended),
        (unsealed, MemoryDropRefusal::Sealed),
        (unplaced, MemoryDropRefusal::Placed),
        (unencrypted, MemoryDropRefusal::Encrypted),
        (full_suspend_pin, MemoryDropRefusal::NotFullSuspendPin),
        (durable, MemoryDropRefusal::NoDurableCopy),
        (newest, MemoryDropRefusal::NewerFullSnapshot),
        (node_gone, MemoryDropRefusal::NodeNotGone),
    ];
    Ok(checks
        .into_iter()
        .find_map(|(holds, refusal)| (!holds).then_some(refusal)))
}

/// **Guarded transition.** Seal the row for a memory drop: read its
/// candidate under `FOR UPDATE OF s` and take its restore lease with `token`,
/// expiring `ttl` from now, without moving its lifecycle. Returns the
/// candidate, or `None` when the row isn't one and nothing was written;
/// [`refusal`] says why.
///
/// The caller runs this in a transaction that already holds the fork-source
/// lock (`crate::fork_source_lifecycle`) and found no active fork, the order a
/// delete takes them in, and commits it before copying. The candidate allows
/// an expired lease, which this replaces: a drop that died mid-copy leaves its
/// seal to expire, and a retry takes the row over with a new token.
pub async fn seal(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    token: Uuid,
    ttl: Duration,
) -> Result<Option<MemoryDropCandidate>, sqlx::Error> {
    let Some(candidate) = candidate(&mut **tx, sandbox_id, CandidateRead::Locked).await? else {
        return Ok(None);
    };
    let sealed = sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + ($3::bigint * INTERVAL '1 millisecond')
          WHERE id = $1
            AND lifecycle = 'suspended'"
    ))
    .bind(sandbox_id)
    .bind(token)
    .bind(millis(ttl))
    .execute(&mut **tx)
    .await?;
    // The row is locked and was just read as `suspended`, so the write lands.
    Ok((sealed.rows_affected() == 1).then_some(candidate))
}

/// **Guarded transition.** Push the seal's expiry to `ttl` from now, if the
/// row still holds `token` and is `suspended`. Returns whether it does;
/// `false` means the drop lost its seal and must not commit.
///
/// An expired seal that nothing replaced is still the drop's, so this renews
/// it too: the token, not the expiry, is the guard.
pub async fn renew(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
    ttl: Duration,
) -> Result<bool, sqlx::Error> {
    let renewed = sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + ($3::bigint * INTERVAL '1 millisecond')
          WHERE id = $1
            AND lifecycle = 'suspended'
            AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(token)
    .bind(millis(ttl))
    .execute(executor)
    .await?;
    Ok(renewed.rows_affected() == 1)
}

/// **Guarded transition.** Lift the seal without dropping anything, if the row
/// still holds `token`: the drop refused after sealing, or its copy failed.
/// Returns whether it did; `false` means the seal was already gone.
pub async fn release(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
) -> Result<bool, sqlx::Error> {
    let released = sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = NULL,
                restore_lease_expires_at = NULL
          WHERE id = $1
            AND lifecycle = 'suspended'
            AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(token)
    .execute(executor)
    .await?;
    Ok(released.rows_affected() == 1)
}

/// **Guarded transition.** Re-pin the row to the `disk` `stop` artifact
/// `disk_artifact_id` and lift the seal, if the row still holds `token` and
/// `candidate`'s pin. The lifecycle stays `suspended`, and the lifecycle clock
/// doesn't move. Returns whether the row was written; `false` is an outcome
/// (the drop lost its seal, or the pin moved), not an error.
///
/// The token proves nothing else claimed the row since [`seal`]: every
/// restore claim refuses a live lease or replaces it, and every write that
/// moves the lifecycle clears it. The node checks run again, so a node
/// registered under the gone node's name during the copy, which could hold a
/// local copy a restore would prefer, makes this refuse.
///
/// The caller runs this in the transaction that registers the disk snapshot
/// row.
pub async fn commit(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    token: Uuid,
    candidate: &MemoryDropCandidate,
    disk_artifact_id: LocalArtifactId,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop',
                restore_lease_token = NULL,
                restore_lease_expires_at = NULL
          WHERE s.id = $1
            AND s.lifecycle = 'suspended'
            AND s.active_node_id IS NULL
            AND s.evacuation_restore_target IS NULL
            AND s.restore_lease_token = $4
            AND s.current_local_artifact_id = $3
            AND s.current_local_artifact_kind = 'full'
            AND {NODE_GONE}"
    ))))
    .bind(sandbox_id)
    .bind(disk_artifact_id)
    .bind(candidate.pinned_artifact_id)
    .bind(token)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Whether a memory drop holds the row's seal: it's `suspended` with a live
/// restore lease. Only [`seal`] writes a lease on a `suspended` row.
///
/// Fork admission, the delete and the restore claims ask this to refuse such
/// a row with a retryable error rather than a final one.
pub async fn sealed(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes
              WHERE id = $1
                AND lifecycle = 'suspended'
                AND restore_lease_expires_at > NOW()
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(executor)
    .await
}

/// Whether the row's memory is dropped: `suspended` and pinned to a disk.
///
/// Only the memory drop writes that shape, and the restore claims refuse it
/// (`restore_lease`), so it holds until an operator points the row back at its
/// retained full artifact or deletes the sandbox.
pub async fn dropped(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes
              WHERE id = $1
                AND lifecycle = 'suspended'
                AND current_local_artifact_kind = 'disk'
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(executor)
    .await
}

/// Whether the row is still pinned to `candidate`'s artifact.
///
/// A caller that lost [`commit`] asks this before giving the disk prefix it
/// wrote to the storage sweeper. The prefix is named after the pinned
/// artifact alone, so a later drop of the same pin copies into the same
/// prefix again; handing it to the sweeper then would delete bytes that drop
/// registers. Once the pin moves, nothing drops that artifact again, so its
/// prefix is garbage.
pub async fn pin_held(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    candidate: &MemoryDropCandidate,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes
              WHERE id = $1
                AND current_local_artifact_id = $2
         )"
    ))
    .bind(sandbox_id)
    .bind(candidate.pinned_artifact_id)
    .fetch_one(executor)
    .await
}

/// `duration` in whole milliseconds for a SQL bind, saturating.
fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;

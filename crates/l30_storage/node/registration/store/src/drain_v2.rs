//! The v2 node drain's columns of `nodes`: `drain_engine`, and the
//! `drain_status` JSON a v2 drain keeps its generation, deadline, report and
//! fence in ([`StoredV2Status`], the one typed shape of its keys).
//! Every statement and SQL fragment over them is here, so the cluster
//! controller's v2 loop, the admin API, the drain gates and registration
//! read and write them one way.

use pplx_space_accounts::NodeId;
use pplx_space_node_drain::NodeDrainEngine;
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};

/// A v2 drain's `drain_status`, as the admin API starts it and the cluster
/// controller writes it, and as every reader reads it. Its keys are frozen:
/// a point in time ends in `_at` and is Unix seconds by the database clock.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Status {
    /// The drain's id: `drain_started_at` in microseconds. Every `DrainNode`
    /// call of the drain names it, and the node takes generations only
    /// forward.
    pub generation: u64,
    /// When holding ends: `drain_started_at` plus `:drain`'s
    /// `hold_seconds`, moved earlier only. From it on, the controller asks
    /// the node to evacuate.
    pub deadline_at: i64,
    /// A best-effort set of the sandboxes the drain has seen running, which
    /// only grows: what the still-suspended gauge counts and the console
    /// marks.
    #[serde(default)]
    pub observed_running: Vec<String>,
    /// Every sandbox the controller has recorded as evacuated, appended
    /// only, so the console's counts survive each sandbox's release from
    /// the node's report.
    #[serde(default)]
    pub evacuated_total: Vec<String>,
    /// The fence: the EC2 instance the drain terminates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminate_instance: Option<String>,
    /// When Auto Scaling accepted the termination of the fenced instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<i64>,
    /// When EC2 first listed the drain's instance (the fence once written,
    /// else the recorded one) `terminated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminated_observed_at: Option<i64>,
    /// The last report the controller stored under [`Self::generation`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<StoredV2Report>,
}

impl StoredV2Status {
    /// Read a `drain_status` column; `None` for NULL.
    pub fn from_column(
        value: Option<&serde_json::Value>,
    ) -> Result<Option<Self>, serde_json::Error> {
        value.map(Self::deserialize).transpose()
    }

    /// The drain fenced its instance for termination.
    pub const fn fenced(&self) -> bool {
        self.terminate_instance.is_some()
    }
}

/// The last `DrainReport` the controller stored for a drain's generation,
/// with what it recorded of it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Report {
    /// When the controller stored it.
    pub reported_at: i64,
    /// The one stall stamp: the last time the set of sandboxes the node
    /// reports evacuated grew, or the deadline once it has passed if the
    /// set hasn't grown since, so this plus the stall bound is when the
    /// drain stalls.
    pub evacuated_grew_at: i64,
    #[serde(default)]
    pub evacuated: Vec<StoredV2Evacuated>,
    #[serde(default)]
    pub pending: Vec<StoredV2Pending>,
    #[serde(default)]
    pub blocked: Vec<StoredV2Blocked>,
    /// The evacuations the controller hasn't recorded yet.
    #[serde(default)]
    pub unrecorded: Vec<StoredV2Unrecorded>,
}

impl StoredV2Report {
    /// The node reported nothing pending or blocked, and the controller
    /// recorded every evacuation: the drain is finished.
    pub fn finished(&self) -> bool {
        self.pending.is_empty() && self.blocked.is_empty() && self.unrecorded.is_empty()
    }
}

/// A sandbox the node reports durable elsewhere.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Evacuated {
    pub sandbox_id: String,
    pub artifact_kind: StoredV2ArtifactKind,
    pub artifact_id: String,
}

/// An evacuated sandbox's artifact: a suspended sandbox's full snapshot or a
/// stopped one's disk. A kind this build doesn't know reads as `unknown`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoredV2ArtifactKind {
    Full,
    Disk,
    #[serde(other)]
    Unknown,
}

/// A sandbox the node reports it can't move yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Pending {
    pub sandbox_id: String,
    pub wait: StoredV2Wait,
    /// A started VM at the node's planning read.
    #[serde(default)]
    pub running: bool,
    /// Human text: displayed, never parsed or asserted on.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// When the row entered its state, by the node's clock; display only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
    /// A warm-pool member, not a user's sandbox. Absent reads false.
    #[serde(default)]
    pub pool_member: bool,
}

/// What a pending sandbox waits on, as the node's `DrainWait` says: kept
/// running until the drain evacuates (`hold`), being made durable by the
/// drain (`moving`), or waiting on work the drain doesn't drive
/// (`external`). A wait this build doesn't know reads as `unspecified`,
/// still pending.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoredV2Wait {
    Hold,
    Moving,
    External,
    #[serde(other)]
    Unspecified,
}

/// A node-wide problem the node reports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Blocked {
    pub reason: String,
}

/// An evacuation the controller hasn't recorded yet, why, and since when.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Unrecorded {
    pub sandbox_id: String,
    pub reason: String,
    pub since: i64,
}

/// A node's drain v2 columns, read under the caller's row lock.
#[derive(Debug, Clone, PartialEq)]
pub struct DrainV2Columns {
    pub engine: NodeDrainEngine,
    pub status: Option<StoredV2Status>,
    /// Whether `node_k8s` records the node's EC2 instance.
    pub recorded_instance: bool,
}

/// The node's v2 drain columns; `None` when there is no such node. A
/// `drain_status` that doesn't read as [`StoredV2Status`] is a decode error.
pub async fn drain_v2_columns(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
) -> Result<Option<DrainV2Columns>, sqlx::Error> {
    type Row = (Option<String>, Option<serde_json::Value>, bool);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT n.drain_engine, n.drain_status, k.instance_id IS NOT NULL
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor)
    .await?;
    row.map(|(engine, status, recorded_instance)| {
        Ok(DrainV2Columns {
            engine: NodeDrainEngine::from_column(engine.as_deref()),
            status: StoredV2Status::from_column(status.as_ref())
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            recorded_instance,
        })
    })
    .transpose()
}

/// Start a v2 drain on a node `update_drain_phase` has just moved to
/// `draining` in this transaction: record the engine, clear the mode (a v2
/// drain takes none), and write the drain's `drain_status`: its generation,
/// the drain's start in microseconds, and its deadline, `hold_seconds` after
/// the drain's start in Unix seconds, by the database clock that stamped
/// `drain_started_at`. Until the deadline the node keeps its running
/// sandboxes running; a hold of 0 evacuates them at once. A later drain
/// starts later, so its generation is larger: the node takes generations
/// only forward.
///
/// `drain_engine` is bound from [`NodeDrainEngine`], the one closed set of
/// engines: the column has no CHECK, so no statement spells a value itself.
pub async fn start_v2_drain(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    hold_seconds: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_engine = $3::TEXT,
                drain_mode = NULL,
                drain_status = jsonb_build_object(
                    'generation', (EXTRACT(EPOCH FROM drain_started_at) * 1000000)::BIGINT,
                    'deadline_at', FLOOR(EXTRACT(EPOCH FROM drain_started_at))::BIGINT + $2::BIGINT
                )
          WHERE id = $1 AND drain_phase = 'draining' AND drain_started_at IS NOT NULL"
    ))
    .bind(node_id)
    .bind(hold_seconds)
    .bind(NodeDrainEngine::V2.column())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// What a repeat `:drain` with a hold did to the deadline of the v2 drain in
/// flight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldChange {
    /// The new deadline, `hold_seconds` after the drain's start, is earlier:
    /// stored.
    Shortened,
    /// The same deadline: nothing changed.
    Kept,
    /// Later than the stored deadline: nothing changed. A drain's deadline
    /// only moves earlier.
    Refused,
}

/// Move the deadline of the v2 drain in flight on `node_id`, whose row the
/// caller has locked, to `hold_seconds` after the drain's start, only if
/// that is earlier, by the clock [`start_v2_drain`] reads. A row with no
/// deadline is refused.
pub async fn shorten_v2_hold(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    hold_seconds: i64,
) -> Result<HoldChange, sqlx::Error> {
    let (requested, stored): (Option<i64>, Option<i64>) = sqlx::query_as(sql!(
        "SELECT FLOOR(EXTRACT(EPOCH FROM drain_started_at))::BIGINT + $2::BIGINT,
                (drain_status->>'deadline_at')::BIGINT
           FROM nodes
          WHERE id = $1 AND drain_engine = 'v2'"
    ))
    .bind(node_id)
    .bind(hold_seconds)
    .fetch_optional(&mut **tx)
    .await?
    .unwrap_or((None, None));
    let (Some(requested), Some(stored)) = (requested, stored) else {
        return Ok(HoldChange::Refused);
    };
    if requested > stored {
        return Ok(HoldChange::Refused);
    }
    if requested == stored {
        return Ok(HoldChange::Kept);
    }
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('deadline_at', $2::BIGINT)
          WHERE id = $1 AND drain_engine = 'v2'"
    ))
    .bind(node_id)
    .bind(requested)
    .execute(&mut **tx)
    .await?;
    Ok(HoldChange::Shortened)
}

/// What a v2 drain's start reads under the cluster's evacuation lock: the
/// node's heartbeat and how many other v2 drains hold an evacuation slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct V2StartAdmission {
    /// The node's status heartbeat is reachable and not stale.
    pub heartbeat_fresh: bool,
    /// The v2 drains of other nodes that hold an evacuation slot.
    pub evacuating: i64,
}

/// SQL predicate over the `nodes` row aliased `n`: its v2 drain holds one of
/// the cluster's evacuation slots. A drain takes one when the admin route
/// starts it, and keeps it for its whole drain, its hold included: while it
/// is `draining` and, once undrained (a v2 engine with no phase), until its
/// cancel finishes on the node. `drained`, `terminating` and a deleted row
/// hold none. A drain that only holds running sandboxes keeps its slot
/// because its node evacuates them all at its deadline: freeing it would
/// let any number of such drains evacuate at once. This predicate is the one
/// statement of the rule.
pub const HOLDS_EVACUATION_SLOT: &str = "(n.drain_engine = 'v2'
     AND (n.drain_phase = 'draining' OR n.drain_phase IS NULL))";

/// `$1` = node id. Read under the evacuation lock: the node's heartbeat,
/// and how many other nodes' v2 drains hold an evacuation slot
/// ([`HOLDS_EVACUATION_SLOT`]).
static V2_START_ADMISSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let fresh = pplx_space_sandbox_residency::node_heartbeat_fresh("$1");
    sql!(format!(
        "SELECT {fresh} AS heartbeat_fresh,
                (SELECT COUNT(*)
                   FROM nodes n
                  WHERE {HOLDS_EVACUATION_SLOT}
                    AND n.id <> $1) AS evacuating"
    ))
});

/// Serialize the start of v2 drains across the cluster and read what the
/// start of one on `node_id` needs: take the evacuation lock, a transaction
/// advisory lock every v2 start takes, held until the caller's transaction
/// ends, so two starts can't both see a free slot; then read the node's
/// heartbeat and count the slots other nodes hold ([`V2StartAdmission`]).
/// Only a v2 drain's start takes the lock; the caller writes the start in
/// the same transaction, so the lock covers the count and the write.
pub async fn admit_v2_start(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
) -> Result<V2StartAdmission, sqlx::Error> {
    const EVACUATION_LOCK: &str = "node_drain_v2_evacuation_slots";
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(EVACUATION_LOCK)
    .execute(&mut **tx)
    .await?;
    let (heartbeat_fresh, evacuating): (bool, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(V2_START_ADMISSION.as_str()))
            .bind(node_id)
            .fetch_one(&mut **tx)
            .await?;
    Ok(V2StartAdmission {
        heartbeat_fresh,
        evacuating,
    })
}

#[cfg(test)]
mod tests;
